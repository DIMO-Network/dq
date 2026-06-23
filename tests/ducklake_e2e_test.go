// ducklake_e2e_test.go proves the converged DuckLake materializer against a
// file catalog (identical DuckLake SQL to Postgres; the Postgres + concurrency
// case is the PG_CATALOG_DSN-gated test). It seeds lake.raw_events the way
// din's sink does, then asserts the materializer reads the snapshot delta,
// decodes, writes lake.signals, advances the snapshot cursor, and is
// exactly-once on a re-run.
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLakeService opens a DuckDB service with a file-backed DuckLake catalog,
// then creates the raw_events table din owns.
func newLakeService(t *testing.T, dir string) *duck.Service {
	t.Helper()
	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      dir + "/catalog.ducklake",
		DataPath:        dir + "/lakedata",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	_, err = svc.DB().ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS lake.raw_events (
		subject VARCHAR, "time" TIMESTAMP WITH TIME ZONE, type VARCHAR, id VARCHAR,
		source VARCHAR, producer VARCHAR, data_content_type VARCHAR, data_version VARCHAR,
		extras VARCHAR, data VARCHAR, data_base64 BLOB, data_index_key VARCHAR, voids_id VARCHAR)`)
	require.NoError(t, err)
	return svc
}

// seedRawStatus inserts one dimo.status raw event (the din sink's row shape)
// with a default-module signal payload, as its own snapshot.
func seedRawStatus(t *testing.T, db *sql.DB, id, subject string, ts time.Time, signals ...map[string]any) {
	t.Helper()
	ev := deviceStatus(id, subject, ts, signals...)
	// din's appender writes empty strings (not NULL) for header columns.
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data)
		 VALUES (?, ?, ?, ?, ?, ?, '', ?, '{}', ?)`,
		ev.Subject, ev.Time.UTC(), ev.Type, ev.ID, ev.Source, ev.Producer, ev.DataVersion, string(ev.Data))
	require.NoError(t, err)
}

func TestDuckLake_MaterializeFromRawEvents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)

	// din writes three status events as three snapshots.
	seedRawStatus(t, db, "dl-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 40))
	seedRawStatus(t, db, "dl-2", subject, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 80))
	seedRawStatus(t, db, "dl-3", subject, day.Add(3*time.Hour), speedAt(day.Add(3*time.Hour), 65))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	processed := drainRunner(t, ctx, runner)
	require.Equal(t, 3, processed, "all three raw events consumed")

	var rows int
	var maxSpeed float64
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*), max(value_number) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).
		Scan(&rows, &maxSpeed))
	assert.Equal(t, 3, rows)
	assert.Equal(t, 80.0, maxSpeed)

	// Snapshot cursor advanced and exactly-once on re-run.
	again := drainRunner(t, ctx, runner)
	assert.Zero(t, again, "caught-up decoder consumes nothing")
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.signals").Scan(&rows))
	assert.Equal(t, 3, rows, "no double-decode on re-run")

	// A new raw event becomes a new snapshot; the next pass picks up only it.
	seedRawStatus(t, db, "dl-4", subject, day.Add(4*time.Hour), speedAt(day.Add(4*time.Hour), 90))
	delta := drainRunner(t, ctx, runner)
	assert.Equal(t, 1, delta, "incremental snapshot diff picks up only the new event")
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.signals").Scan(&rows))
	assert.Equal(t, 4, rows)

	// dq reported its cursor into din's snapshot-expiry floor, and it equals
	// the processed snapshot id (the lake.ingest_progress cursor).
	var floor, cursor int64
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT snapshot_id FROM meta.din_consumer_progress WHERE consumer = 'dq'").Scan(&floor))
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT CAST(cursor AS BIGINT) FROM lake.ingest_progress WHERE partition = 'lake.raw_events#snapshot'").Scan(&cursor))
	assert.Equal(t, cursor, floor, "consumer floor matches the processed snapshot cursor")
}

func drainRunner(t *testing.T, ctx context.Context, r *materializer.Runner) int {
	t.Helper()
	total := 0
	for {
		n, err := r.RunOnce(ctx)
		require.NoError(t, err)
		total += n
		if n == 0 {
			return total
		}
	}
}
