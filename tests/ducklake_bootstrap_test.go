// ducklake_bootstrap_test.go covers the idempotent snapshot-cursor bootstrap.
// The advance must be a compare-and-swap against a single pre-seeded row, not a
// guard-less INSERT that two concurrent first-writers could both perform and
// then both decode the same snapshot range (CHD-9). The full two-replica race
// is the PG_CATALOG_DSN-gated test; this pins the seed-once invariant locally.
package tests

import (
	"context"
	"testing"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_BootstrapSeedsCursorExactlyOnce(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()

	// Two bootstraps (a restart, or a second replica) must leave one cursor row.
	_, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	_, err = materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)

	var n int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.ingest_progress WHERE partition = 'lake.raw_events#snapshot'").Scan(&n))
	assert.Equal(t, 1, n, "the snapshot cursor is seeded exactly once across bootstraps")
}
