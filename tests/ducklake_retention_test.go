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
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, nil, zerolog.Nop()).
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
