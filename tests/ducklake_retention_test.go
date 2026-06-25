// ducklake_retention_test.go covers the optional decoded-data retention prune
// (CHD-38): DuckLake snapshot expiry bounds history age, not data size, so the
// decoded tables grow unbounded. A configurable row-level TTL (default off)
// deletes decoded rows older than the window; this proves it removes old rows
// and keeps recent ones.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_PruneDecoded(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:11", vehicleNFT.Hex())
	now := time.Now().UTC()

	old := now.AddDate(0, 0, -30)
	recent := now.Add(-time.Hour)
	seedRawStatus(t, db, "old", subject, old, speedAt(old, 40))
	seedRawStatus(t, db, "recent", subject, recent, speedAt(recent, 80))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	deleted, err := mat.PruneDecoded(ctx, 7*24*time.Hour)
	require.NoError(t, err)
	assert.EqualValues(t, 1, deleted, "the 30-day-old signal row is pruned")

	var remaining int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&remaining))
	assert.Equal(t, 1, remaining, "only the recent row remains")
}

// TestDuckLake_PruneDecoded_RemovesOrphanRollup proves the prune cleans up the rollup in
// lockstep: a (subject,name) whose base rows are ALL pruned must not be left as a phantom
// "latest" in signals_latest (refreshRollup only touches live-batch subjects, so without
// this the no-source latest/summary reads would serve data that no longer exists).
func TestDuckLake_PruneDecoded_RemovesOrphanRollup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:12", vehicleNFT.Hex())
	old := time.Now().UTC().AddDate(0, 0, -30)
	// Only an OLD speed row — so a 7-day prune fully removes speed from lake.signals.
	seedRawStatus(t, db, "old-only", subject, old, speedAt(old, 40))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	rollupCount := func() int {
		var n int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT count(*) FROM lake.signals_latest WHERE subject = ? AND name = 'speed'", subject).Scan(&n))
		return n
	}
	require.Equal(t, 1, rollupCount(), "rollup has the speed latest before pruning")

	_, err = mat.PruneDecoded(ctx, 7*24*time.Hour)
	require.NoError(t, err)

	var base int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&base))
	assert.Equal(t, 0, base, "the old speed row is pruned from the base table")
	assert.Equal(t, 0, rollupCount(), "the orphaned rollup row is removed (no phantom latest)")
}
