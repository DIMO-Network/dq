// ducklake_latest_rollup_test.go covers SR-5: GetLatestSignals (named signals,
// no source filter, non-location) is served from the lake.signals_latest rollup
// in O(distinct-names) rather than a full-history dedup scan. Proven by pruning
// the base table (as retention does — the rollup is kept) and asserting the
// named-latest query still returns the value: only a rollup read can.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_GetLatestSignals_ServedFromRollup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)

	seedRawStatus(t, db, "lr-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 40))
	seedRawStatus(t, db, "lr-2", subject, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 65))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	// Drop the base history; the rollup is current state and stays (this is
	// exactly what PruneDecoded does at the retention boundary).
	_, err = db.ExecContext(ctx, "DELETE FROM lake.signals")
	require.NoError(t, err)

	q := duck.NewLakeQueries(svc)
	got, err := q.GetLatestSignals(ctx, subject, &model.LatestSignalsArgs{
		SignalNames: map[string]struct{}{"speed": {}},
	})
	require.NoError(t, err)

	var speed float64
	var found bool
	for _, s := range got {
		if s.Data.Name == "speed" {
			speed, found = s.Data.ValueNumber, true
		}
	}
	require.True(t, found, "speed returned from the rollup after base prune")
	assert.Equal(t, 65.0, speed, "latest speed is the newest reading, read from the rollup")
}

// TestDuckLake_FlushRollup_SubjectScoped pins B2: the decoupled rollup flush
// recomputes only the subjects a batch dirtied — across buckets, in one flush —
// and a later batch touching one subject refreshes that subject's rollup row
// without disturbing (or depending on re-scanning) the others. Bucket-scoped
// dirtiness saturated at trivial fleet activity and made every flush a
// full-table recompute on the decode goroutine.
func TestDuckLake_FlushRollup_SubjectScoped(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()

	subjA := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	subjB := fmt.Sprintf("did:erc721:137:%s:43", vehicleNFT.Hex())
	subjC := fmt.Sprintf("did:erc721:137:%s:44", vehicleNFT.Hex())
	require.NotEqual(t, duck.HashBucket(subjA), duck.HashBucket(subjB),
		"test wants subjects spanning buckets")
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	seedRawStatus(t, db, "ss-a1", subjA, day.Add(time.Hour), speedAt(day.Add(time.Hour), 40))
	seedRawStatus(t, db, "ss-b1", subjB, day.Add(time.Hour), speedAt(day.Add(time.Hour), 50))
	seedRawStatus(t, db, "ss-c1", subjC, day.Add(time.Hour), speedAt(day.Add(time.Hour), 60))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 3, drainRunner(t, ctx, runner))

	latestSpeed := func(subject string) float64 {
		var v float64
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT value_number FROM lake.signals_latest WHERE subject = ? AND name = 'speed'", subject).Scan(&v))
		return v
	}
	assert.Equal(t, 40.0, latestSpeed(subjA))
	assert.Equal(t, 50.0, latestSpeed(subjB))
	assert.Equal(t, 60.0, latestSpeed(subjC), "one flush covers all dirtied subjects across buckets")

	// A later batch touches ONLY subjA; its rollup row refreshes, the others stay
	// correct without being part of the flush.
	seedRawStatus(t, db, "ss-a2", subjA, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 90))
	require.Equal(t, 1, drainRunner(t, ctx, runner))
	assert.Equal(t, 90.0, latestSpeed(subjA), "dirty subject recomputed")
	assert.Equal(t, 50.0, latestSpeed(subjB), "untouched subject intact")
	assert.Equal(t, 60.0, latestSpeed(subjC), "untouched subject intact")

	// The subject-scoped flush must be byte-identical to the full disaster-recovery
	// rebuild — same aggregation, same dedup — for the whole table.
	var beforeRows int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.signals_latest").Scan(&beforeRows))
	require.NoError(t, mat.RecomputeRollup(ctx))
	var afterRows int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.signals_latest").Scan(&afterRows))
	assert.Equal(t, beforeRows, afterRows, "incremental flush and full rebuild agree on row count")
	assert.Equal(t, 90.0, latestSpeed(subjA))
	assert.Equal(t, 50.0, latestSpeed(subjB))
	assert.Equal(t, 60.0, latestSpeed(subjC))
}
