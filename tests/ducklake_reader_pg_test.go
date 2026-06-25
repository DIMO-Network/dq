package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDuckLakePostgres_ReadDuringMaterialize is the multi-pod QUERY proof: several reader
// pods (separate DuckDB services on the shared Postgres catalog) query signals_latest
// WHILE one materializer pod commits batches. The rollup refresh is a DELETE+INSERT that
// runs inside the single commit transaction, so DuckLake snapshot isolation must give
// readers old-complete or new-complete — never the post-DELETE/pre-INSERT empty state. A
// reader that ever sees the subject's rollup drop back to empty after it was populated has
// caught a torn read (the single-transaction guarantee regressed). Runs under -race to
// also exercise concurrent reader+writer catalog access. Skips unless PG_CATALOG_DSN set.
func TestDuckLakePostgres_ReadDuringMaterialize(t *testing.T) {
	dsn := pgCatalogDSN(t)
	ctx := context.Background()
	dataPath := os.Getenv("PG_DATA_PATH")
	if dataPath == "" {
		dataPath = t.TempDir()
	}
	subject := fmt.Sprintf("did:erc721:137:%s:77", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)

	seed := newPGLakeService(t, dsn, dataPath)
	db := seed.DB()
	// Append-only (no DROP) + a unique subject, so this coexists with the other PG-catalog
	// tests on a shared catalog: a DROP+recreate of raw_events would leave a materializer's
	// delta read spanning a version where the table is gone. Clean up this subject's rows.
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS lake.raw_events (subject VARCHAR, "time" TIMESTAMP WITH TIME ZONE, type VARCHAR,
		id VARCHAR, source VARCHAR, producer VARCHAR, data_content_type VARCHAR, data_version VARCHAR,
		extras VARCHAR, data VARCHAR, data_base64 BLOB, data_index_key VARCHAR, voids_id VARCHAR)`)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, tbl := range []string{"raw_events", "signals", "signals_latest"} {
			_, _ = db.ExecContext(ctx, fmt.Sprintf("DELETE FROM lake.%s WHERE subject = ?", tbl), subject)
		}
	})

	const events = 200
	for i := range events {
		ts := day.Add(time.Duration(i) * time.Minute)
		seedRawStatus(t, db, fmt.Sprintf("rd-%d", i), subject, ts, speedAt(ts, float64(i)))
	}

	// Open all services on the test goroutine (the helpers use require, which is only
	// safe here) — the spawned goroutines just use the *sql.DB / runner.
	matSvc := newPGLakeService(t, dsn, dataPath)
	mat, err := materializer.NewDuckLakeMaterializer(ctx, matSvc.DB(), zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	const readers = 3
	readerDBs := make([]*sql.DB, readers)
	for i := range readerDBs {
		readerDBs[i] = newPGLakeService(t, dsn, dataPath).DB()
	}

	matDone := make(chan struct{})
	var matErr error
	go func() {
		defer close(matDone)
		for {
			n, e := runner.RunOnce(ctx)
			if e != nil {
				matErr = e
				return
			}
			if n == 0 {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	var tornReads int64
	for _, rdb := range readerDBs {
		wg.Add(1)
		go func(rdb *sql.DB) {
			defer wg.Done()
			sawPositive := false
			for {
				select {
				case <-matDone:
					return
				default:
				}
				var c int
				// A transient error during a concurrent commit/attach is not the
				// assertion — only a clean 0-after-positive is a torn read.
				if err := rdb.QueryRowContext(ctx,
					"SELECT count(*) FROM lake.signals_latest WHERE subject = ? AND name = 'speed'", subject).Scan(&c); err != nil {
					continue
				}
				if c > 0 {
					sawPositive = true
				} else if sawPositive {
					atomic.AddInt64(&tornReads, 1)
				}
			}
		}(rdb)
	}
	wg.Wait()

	require.NoError(t, matErr)
	assert.Zero(t, atomic.LoadInt64(&tornReads),
		"a reader saw signals_latest drop to empty mid-materialize — the rollup recompute is not atomic")

	var rows int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals_latest WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, 1, rows, "exactly one latest speed row after the materializer drains")
}
