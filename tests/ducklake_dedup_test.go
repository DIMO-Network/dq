// ducklake_dedup_test.go pins the cross-batch dedup contract: the bounded
// anti-join still drops a redelivered cloudevent in steady state, and backfill
// mode skips the guard (a clean historical load carries no redeliveries; the
// read path dedups regardless).
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_DedupAcrossBatches(t *testing.T) {
	ctx := context.Background()
	subject := fmt.Sprintf("did:erc721:137:%s:88", vehicleNFT.Hex())
	ts := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)

	countSpeed := func(db *sql.DB) int {
		var n int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&n))
		return n
	}
	newRunner := func(t *testing.T, db *sql.DB, backfill bool) *materializer.Runner {
		mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
		require.NoError(t, err)
		mat = mat.WithBackfillMode(backfill)
		return materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
			WithDuckLake(mat)
	}

	// Steady state: the same cloudevent redelivered in a LATER snapshot (the cursor
	// already moved past the first) is dropped by the bounded anti-join.
	t.Run("steady mode dedups the redelivery", func(t *testing.T) {
		svc := newLakeService(t, t.TempDir())
		db := svc.DB()
		runner := newRunner(t, db, false)

		seedRawStatus(t, db, "dup", subject, ts, speedAt(ts, 50))
		require.Equal(t, 1, drainRunner(t, ctx, runner))
		seedRawStatus(t, db, "dup", subject, ts, speedAt(ts, 50)) // redelivery, new snapshot
		drainRunner(t, ctx, runner)

		assert.Equal(t, 1, countSpeed(db), "the cross-batch duplicate is deduped")
	})

	// Backfill mode: the anti-join is skipped, so the redelivery is stored twice.
	t.Run("backfill mode skips dedup", func(t *testing.T) {
		svc := newLakeService(t, t.TempDir())
		db := svc.DB()
		runner := newRunner(t, db, true)

		seedRawStatus(t, db, "dup", subject, ts, speedAt(ts, 50))
		require.Equal(t, 1, drainRunner(t, ctx, runner))
		seedRawStatus(t, db, "dup", subject, ts, speedAt(ts, 50))
		drainRunner(t, ctx, runner)

		assert.Equal(t, 2, countSpeed(db), "backfill mode skips the anti-join, storing the redelivery twice")
	})
}
