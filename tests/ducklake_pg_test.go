// ducklake_pg_test.go is the production-shaped DuckLake proof: a real
// PostgreSQL catalog with two independent materializers (the multi-replica
// case) racing the same raw_events delta. Skips unless PG_CATALOG_DSN is set,
// e.g.:
//
//	PG_CATALOG_DSN='dbname=ducklake_test host=127.0.0.1 port=54329 user=postgres' \
//	  go test ./tests/ -run TestDuckLakePostgres -v
//
// File-catalog tests cover the logic everywhere; this covers cross-connection
// concurrency that a single-process file catalog cannot.
package tests

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pgCatalogDSN(t *testing.T) string {
	dsn := os.Getenv("PG_CATALOG_DSN")
	if dsn == "" {
		t.Skip("set PG_CATALOG_DSN to run the Postgres-catalog DuckLake test")
	}
	return dsn
}

// newPGLakeService opens a duck service on the shared Postgres catalog + data
// path. uniqueSchema is appended so parallel test runs don't collide; the
// first caller creates raw_events.
func newPGLakeService(t *testing.T, dsn, dataPath string) *duck.Service {
	t.Helper()
	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      dsn,
		DataPath:        dataPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func TestDuckLakePostgres_ConcurrentMaterializers(t *testing.T) {
	dsn := pgCatalogDSN(t)
	ctx := context.Background()
	// DuckLake binds DATA_PATH into the catalog permanently, so it must be
	// stable across the services in this run (and across reruns against the
	// same catalog DB). PG_DATA_PATH supplies a fixed, pre-cleaned dir.
	dataPath := os.Getenv("PG_DATA_PATH")
	if dataPath == "" {
		dataPath = t.TempDir()
	}
	subject := fmt.Sprintf("did:erc721:137:%s:55", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)

	// Fresh tables for this run.
	seed := newPGLakeService(t, dsn, dataPath)
	db := seed.DB()
	for _, q := range []string{
		"DROP TABLE IF EXISTS lake.signals", "DROP TABLE IF EXISTS lake.events",
		"DROP TABLE IF EXISTS lake.ingest_progress", "DROP TABLE IF EXISTS lake.raw_events",
		`CREATE TABLE lake.raw_events (subject VARCHAR, "time" TIMESTAMP WITH TIME ZONE, type VARCHAR,
			id VARCHAR, source VARCHAR, producer VARCHAR, data_content_type VARCHAR, data_version VARCHAR,
			extras VARCHAR, data VARCHAR, data_base64 BLOB, data_index_key VARCHAR, voids_id VARCHAR)`,
	} {
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}

	const events = 40
	for i := range events {
		seedRawStatus(t, db, fmt.Sprintf("pg-%d", i), subject,
			day.Add(time.Duration(i)*time.Minute), speedAt(day.Add(time.Duration(i)*time.Minute), float64(i)))
	}

	// Two independent materializers (separate connections = the multi-replica
	// shape) drain concurrently, racing the same snapshot deltas.
	run := func() {
		svc := newPGLakeService(t, dsn, dataPath)
		mat, err := materializer.NewDuckLakeMaterializer(ctx, svc.DB(), zerolog.Nop())
		require.NoError(t, err)
		runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
			WithDuckLake(mat)
		for {
			n, err := runner.RunOnce(ctx)
			require.NoError(t, err)
			if n == 0 {
				return
			}
		}
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); run() }()
	}
	wg.Wait()

	// Exactly-once across both writers: 40 decoded speed rows, no duplicates.
	var rows int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, events, rows, "every raw event decoded exactly once under concurrent materializers")

	var dupes int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT cloud_event_id, name, timestamp FROM lake.signals
		GROUP BY cloud_event_id, name, timestamp HAVING count(*) > 1)`).Scan(&dupes))
	assert.Zero(t, dupes, "no duplicate decoded rows")
}

// TestDuckLakePostgres_ConcurrentPaginatedFatSnapshot proves finding #1c under a real
// catalog: a SINGLE oversized snapshot (many rows, span can't be count-split) is drained
// by two independent materializers using tiny per-window bounds, so most rows flow
// through intermediate (non-cursor-advancing) windows while both writers race the final
// cursor CAS. Exactly-once must still hold — the intermediate-window idempotency + the
// cursor-coupled final commit are the #1c invariant, exercised across connections that a
// single-process file catalog cannot reproduce.
func TestDuckLakePostgres_ConcurrentPaginatedFatSnapshot(t *testing.T) {
	dsn := pgCatalogDSN(t)
	ctx := context.Background()
	dataPath := os.Getenv("PG_DATA_PATH")
	if dataPath == "" {
		dataPath = t.TempDir()
	}
	subject := fmt.Sprintf("did:erc721:137:%s:56", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)

	seed := newPGLakeService(t, dsn, dataPath)
	db := seed.DB()
	for _, q := range []string{
		"DROP TABLE IF EXISTS lake.signals", "DROP TABLE IF EXISTS lake.events",
		"DROP TABLE IF EXISTS lake.signals_latest", "DROP TABLE IF EXISTS lake.events_latest",
		"DROP TABLE IF EXISTS lake.ingest_progress", "DROP TABLE IF EXISTS lake.raw_events",
		`CREATE TABLE lake.raw_events (subject VARCHAR, "time" TIMESTAMP WITH TIME ZONE, type VARCHAR,
			id VARCHAR, source VARCHAR, producer VARCHAR, data_content_type VARCHAR, data_version VARCHAR,
			extras VARCHAR, data VARCHAR, data_base64 BLOB, data_index_key VARCHAR, voids_id VARCHAR)`,
	} {
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}

	// ONE snapshot, 50 rows: a single transaction commits them as one snapshot, so the
	// span bound cannot split it — only intra-snapshot pagination can bound the pass.
	const events = 50
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := range events {
		at := base.Add(time.Duration(i) * time.Second)
		ev := deviceStatus(fmt.Sprintf("pgpag-%d", i), subject, at, speedAt(at, float64(i)))
		_, err := tx.ExecContext(ctx,
			`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data)
			 VALUES (?, ?, ?, ?, ?, ?, '', ?, '{}', ?)`,
			ev.Subject, ev.Time.UTC(), ev.Type, ev.ID, ev.Source, ev.Producer, ev.DataVersion, string(ev.Data))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	run := func() {
		svc := newPGLakeService(t, dsn, dataPath)
		mat, err := materializer.NewDuckLakeMaterializer(ctx, svc.DB(), zerolog.Nop())
		require.NoError(t, err)
		mat.WithMaxRowsPerWindow(7) // force multi-window pagination of the fat snapshot
		runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
			WithDuckLake(mat)
		for {
			n, err := runner.RunOnce(ctx)
			require.NoError(t, err)
			if n == 0 {
				return
			}
		}
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); run() }()
	}
	wg.Wait()

	var rows int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, events, rows, "paginated fat snapshot decoded exactly once under concurrent materializers")

	var dupes int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT cloud_event_id, name, timestamp FROM lake.signals
		GROUP BY cloud_event_id, name, timestamp HAVING count(*) > 1)`).Scan(&dupes))
	assert.Zero(t, dupes, "no duplicate decoded rows across paginated windows")
}
