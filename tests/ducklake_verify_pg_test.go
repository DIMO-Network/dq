// ducklake_verify_pg_test.go — verification loops 9-10: the #1c pagination + #5b
// incremental rollup under real-Postgres concurrency with MORE than two writers and a
// mid-span crash of one writer while the others drain. Gated on PG_CATALOG_DSN.
package tests

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// V9 — three independent materializers drain the same paginated fat snapshot; exactly-once
// base rows AND an exact incremental rollup.
func TestVerify09_PG_ThreeConcurrentPaginated(t *testing.T) {
	dsn := pgCatalogDSN(t)
	ctx := context.Background()
	dataPath := os.Getenv("PG_DATA_PATH")
	if dataPath == "" {
		dataPath = t.TempDir()
	}
	subject := fmt.Sprintf("did:erc721:137:%s:57", vehicleNFT.Hex())
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
	const events = 60
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := range events {
		at := base.Add(time.Duration(i) * time.Second)
		ev := deviceStatus(fmt.Sprintf("v9-%d", i), subject, at, speedAt(at, float64(i)))
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
		mat.WithMaxRowsPerWindow(5)
		runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat)
		for {
			n, err := runner.RunOnce(ctx)
			require.NoError(t, err)
			if n == 0 {
				return
			}
		}
	}
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() { defer wg.Done(); run() }()
	}
	wg.Wait()

	var rows, dupes int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, events, rows, "three racing writers decode the paginated snapshot exactly once")
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT cloud_event_id, name, timestamp FROM lake.signals GROUP BY cloud_event_id, name, timestamp HAVING count(*) > 1)`).Scan(&dupes))
	assert.Zero(t, dupes)
	assert.EqualValues(t, events, dumpRollupMap(t, ctx, db)[subject+"|speed"].count, "incremental rollup exact under three writers")
}

// V10 — one writer crashes mid-span (its intermediate window errors) while two others drain;
// the idempotent windows + cursor-coupled final commit still yield exactly-once + an exact
// rollup.
func TestVerify10_PG_CrashOneWriterMidSpan(t *testing.T) {
	dsn := pgCatalogDSN(t)
	ctx := context.Background()
	dataPath := os.Getenv("PG_DATA_PATH")
	if dataPath == "" {
		dataPath = t.TempDir()
	}
	subject := fmt.Sprintf("did:erc721:137:%s:58", vehicleNFT.Hex())
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
	const events = 60
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := range events {
		at := base.Add(time.Duration(i) * time.Second)
		ev := deviceStatus(fmt.Sprintf("v10-%d", i), subject, at, speedAt(at, float64(i)))
		_, err := tx.ExecContext(ctx,
			`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data)
			 VALUES (?, ?, ?, ?, ?, ?, '', ?, '{}', ?)`,
			ev.Subject, ev.Time.UTC(), ev.Type, ev.ID, ev.Source, ev.Producer, ev.DataVersion, string(ev.Data))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	// The flaky writer crashes on its first intermediate window, then is restarted (a
	// real supervisor loop); the two healthy writers race it the whole time.
	flaky := func() {
		crashed := false
		for attempt := 0; attempt < 6; attempt++ {
			svc := newPGLakeService(t, dsn, dataPath)
			mat, err := materializer.NewDuckLakeMaterializer(ctx, svc.DB(), zerolog.Nop())
			require.NoError(t, err)
			mat.WithMaxRowsPerWindow(5)
			if !crashed {
				mat.WithWindowCommitHook(func(idx int) error {
					if idx == 0 {
						crashed = true
						return fmt.Errorf("injected mid-span crash")
					}
					return nil
				})
			}
			runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat)
			done := false
			for {
				n, err := runner.RunOnce(ctx)
				if err != nil {
					break // crashed; loop restarts a fresh materializer
				}
				if n == 0 {
					done = true
					break
				}
			}
			if done {
				return
			}
		}
	}
	healthy := func() {
		svc := newPGLakeService(t, dsn, dataPath)
		mat, err := materializer.NewDuckLakeMaterializer(ctx, svc.DB(), zerolog.Nop())
		require.NoError(t, err)
		mat.WithMaxRowsPerWindow(5)
		runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat)
		for {
			n, err := runner.RunOnce(ctx)
			require.NoError(t, err)
			if n == 0 {
				return
			}
		}
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); flaky() }()
	go func() { defer wg.Done(); healthy() }()
	go func() { defer wg.Done(); healthy() }()
	wg.Wait()

	var rows, dupes int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, events, rows, "exactly-once despite a writer crashing mid-span")
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT cloud_event_id, name, timestamp FROM lake.signals GROUP BY cloud_event_id, name, timestamp HAVING count(*) > 1)`).Scan(&dupes))
	assert.Zero(t, dupes)
	assert.EqualValues(t, events, dumpRollupMap(t, ctx, db)[subject+"|speed"].count, "incremental rollup exact after a mid-span crash under concurrency")
}
