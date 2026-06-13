// ducklake_e2e_test.go proves the DuckLake decoded-store write path against a
// file catalog (identical DuckLake SQL to a Postgres catalog; the Postgres
// case is exercised by the PG_CATALOG_DSN-gated concurrency test). It mirrors
// pipeline_e2e_test's seeding and asserts: rows land in lake.signals, the
// catalog cursor advances, a re-run is exactly-once (idempotent), the
// watermark.json projection is written for din, and the rows decode to the
// hand-computed values.
package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newDuckLakeService opens a DuckDB service with a file-backed DuckLake
// catalog rooted under dir.
func newDuckLakeService(t *testing.T, dir string) *duck.Service {
	t.Helper()
	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      dir + "/catalog.ducklake",
		DataPath:        dir + "/lakedata",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func duckLakeRunner(t *testing.T, store materializer.ObjectStore, db *sql.DB, root string) *materializer.Runner {
	t.Helper()
	w, err := materializer.NewDuckLakeWriter(context.Background(), db, store, "decoded/v1/")
	require.NoError(t, err)
	return materializer.New(materializer.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
		BatchMaxFiles:     1,
	}, store, zerolog.Nop()).WithDuckLake(w)
}

func TestDuckLake_WritePathEndToEnd(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := newFSStore(t, root)
	svc := newDuckLakeService(t, t.TempDir())
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())

	// Three bundles across two past days; speeds 40/80/65 → MAX 80, count 3.
	day1 := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	day2 := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	writeRawBundle(t, store, day1, 1, deviceStatus("dl-1", subject, day1.Add(time.Hour), speedAt(day1.Add(time.Hour), 40)))
	writeRawBundle(t, store, day1, 2, deviceStatus("dl-2", subject, day1.Add(2*time.Hour), speedAt(day1.Add(2*time.Hour), 80)))
	writeRawBundle(t, store, day2, 3, deviceStatus("dl-3", subject, day2.Add(time.Hour), speedAt(day2.Add(time.Hour), 65)))

	runner := duckLakeRunner(t, store, db, root)
	processed := drainRunner(t, ctx, runner)
	require.Equal(t, 3, processed, "all three bundles materialized")

	// Rows landed in the catalog table.
	var rows int
	var maxSpeed float64
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*), max(value_number) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).
		Scan(&rows, &maxSpeed))
	assert.Equal(t, 3, rows)
	assert.Equal(t, 80.0, maxSpeed)

	// Cursor advanced for both partitions.
	var cursorRows int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.ingest_progress").Scan(&cursorRows))
	assert.Equal(t, 2, cursorRows, "one cursor per date partition")

	// Watermark projection written for din's raw compactor.
	body, err := store.GetObject(ctx, "decoded/v1/_state/watermark.json")
	require.NoError(t, err)
	var wm map[string]string
	require.NoError(t, json.Unmarshal(body, &wm))
	assert.Len(t, wm, 2, "projection covers both partitions")

	// Exactly-once: a second drain inserts nothing more.
	again := drainRunner(t, ctx, runner)
	assert.Zero(t, again, "re-run processes no already-cursored files")
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.signals").Scan(&rows))
	assert.Equal(t, 3, rows, "no double-insert on re-run")
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
