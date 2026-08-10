// ducklake_daily_rollup_test.go proves the daily watermarked refresh (dq#55)
// as the SOLE maintainer of lake.signals_latest (the per-pass fold was removed
// in step 5): the seed, the daily fold (new names, new subjects, collisions,
// redeliveries, location fixes, multi-day catch-up), refresh idempotency (an
// already-covered boundary must no-op, never double-fold), and the
// late-arrival path (rows stamped before the watermark arriving after it) —
// always column-identical to a full recompute over the deduped base
// (assertRollupMatchesOracle). The promote / fresh-install flip paths live in
// ducklake_daily_flip_test.go.
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func locFixAt(ts time.Time, lat, lon float64) map[string]any {
	return map[string]any{"name": "currentLocationCoordinates", "timestamp": ts.Format(time.RFC3339Nano),
		"value": map[string]any{"latitude": lat, "longitude": lon, "hdop": 1.5}}
}

func storedWatermark(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	var w string
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT cursor FROM lake.ingest_progress WHERE partition = 'lake.signals_latest#daily_watermark'").Scan(&w))
	return w
}

func TestDuckLake_DailyRollup_SeedFoldAndLateArrivals(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	s1 := fmt.Sprintf("did:erc721:137:%s:91", vehicleNFT.Hex())
	s2 := fmt.Sprintf("did:erc721:137:%s:92", vehicleNFT.Hex())
	s3 := fmt.Sprintf("did:erc721:137:%s:93", vehicleNFT.Hex())

	day0 := time.Now().UTC().AddDate(0, 0, -6).Truncate(24 * time.Hour)
	b1, b2, b4 := day0.AddDate(0, 0, 1), day0.AddDate(0, 0, 2), day0.AddDate(0, 0, 4)

	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupOn, 0)
	})
	require.NoError(t, mat.LoadDailyRollupState(ctx))

	// Day 0: two subjects, a location fix, a same-timestamp collision.
	collTS := day0.Add(40 * time.Minute)
	seedRawStatus(t, db, "d0-1", s1, day0.Add(10*time.Minute), speedAt(day0.Add(10*time.Minute), 10))
	seedRawStatus(t, db, "d0-2", s1, day0.Add(20*time.Minute),
		speedAt(day0.Add(20*time.Minute), 20), locFixAt(day0.Add(20*time.Minute), 42.33, -83.05))
	seedRawStatus(t, db, "d0-3", s1, collTS, speedAt(collTS, 30))
	seedRawStatus(t, db, "d0-4", s1, collTS, speedAt(collTS, 31)) // same (s,n,ts), different ceid
	seedRawStatus(t, db, "d0-5", s2, day0.Add(1*time.Hour), speedAt(day0.Add(1*time.Hour), 55))
	drainNoFlush(t, ctx, runner)
	assert.Empty(t, dumpRollupMap(t, ctx, db), "nothing maintains the rollup at decode time (the fold is gone)")

	// Seed: lake.signals_latest becomes the bounded recompute over ts < b1.
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b1))
	assertRollupMatchesOracle(t, ctx, db)
	assert.Equal(t, b1.Format(time.RFC3339), storedWatermark(t, ctx, db), "watermark persisted at the boundary")
	assert.EqualValues(t, 3, dumpRollupMap(t, ctx, db)[s1+"|speed"].count,
		"same-(subject,name,timestamp) collision is one distinct reading: 3 counted, not 4")

	// Guards: a non-midnight boundary is refused; an already-covered boundary
	// is a no-op that must not move the watermark backwards or double-fold.
	require.Error(t, mat.RunDailyRollupRefresh(ctx, b1.Add(90*time.Minute)))
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b1))
	assert.Equal(t, b1.Format(time.RFC3339), storedWatermark(t, ctx, db))
	assert.EqualValues(t, 3, dumpRollupMap(t, ctx, db)[s1+"|speed"].count, "re-running a covered boundary changes nothing")

	// Day 1: a new name for s1, a brand-new subject s3, a newer fix, and a
	// redelivery of a day-0 event (arrives late → dedup'd in base, late-marked,
	// recomputed to a no-op).
	seedRawStatus(t, db, "d1-1", s1, b1.Add(30*time.Minute),
		speedAt(b1.Add(30*time.Minute), 44), odoAt(b1.Add(30*time.Minute), 1200))
	seedRawStatus(t, db, "d1-2", s3, b1.Add(1*time.Hour), speedAt(b1.Add(1*time.Hour), 77))
	seedRawStatus(t, db, "d1-3", s1, b1.Add(2*time.Hour), locFixAt(b1.Add(2*time.Hour), 42.40, -83.10))
	seedRawStatus(t, db, "d0-1", s1, day0.Add(10*time.Minute), speedAt(day0.Add(10*time.Minute), 10)) // redelivery
	drainNoFlush(t, ctx, runner)

	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b2))
	assertRollupMatchesOracle(t, ctx, db)

	// Days 2-3, folded in ONE refresh (a missed boundary catches up), plus a
	// genuinely late NEW reading: s2 uploads a buffered day-0 row AFTER the
	// watermark has passed it. Without the late set this row's count/first_seen
	// contribution would be lost forever.
	seedRawStatus(t, db, "d2-1", s2, b2.Add(45*time.Minute), speedAt(b2.Add(45*time.Minute), 60))
	seedRawStatus(t, db, "late-1", s2, day0.Add(90*time.Minute), speedAt(day0.Add(90*time.Minute), 58)) // stamped day 0, arrives day 2
	seedRawStatus(t, db, "d3-1", s1, b2.AddDate(0, 0, 1).Add(15*time.Minute), speedAt(b2.AddDate(0, 0, 1).Add(15*time.Minute), 90))
	drainNoFlush(t, ctx, runner)

	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b4))
	assertRollupMatchesOracle(t, ctx, db)
	assert.EqualValues(t, 3, dumpRollupMap(t, ctx, db)[s2+"|speed"].count,
		"the late buffered reading is counted after the late-set recompute")
	var lateLeft int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.rollup_late_subjects").Scan(&lateLeft))
	assert.Zero(t, lateLeft, "late marks cleared by the recompute that covered them")

	// One row per (subject, name), always — the cardinality contract the
	// per-pass fold used to violate under compaction races.
	var keys, rows int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*), coalesce(sum(n), 0) FROM (SELECT count(*) AS n FROM lake.signals_latest GROUP BY subject, name)`).
		Scan(&keys, &rows))
	assert.Equal(t, keys, rows, "exactly one physical rollup row per key")
}

// TestDuckLake_DailyRollup_WatermarkSurvivesRestart proves the watermark (and
// with it the fold's induction base) is durable: a fresh materializer over the
// same catalog picks up where the old one stopped — an already-covered
// boundary no-ops, the next boundary folds incrementally rather than reseeding.
func TestDuckLake_DailyRollup_WatermarkSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:94", vehicleNFT.Hex())
	day0 := time.Now().UTC().AddDate(0, 0, -4).Truncate(24 * time.Hour)
	b1, b2 := day0.AddDate(0, 0, 1), day0.AddDate(0, 0, 2)

	runner1, mat1 := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupOn, 0)
	})
	require.NoError(t, mat1.LoadDailyRollupState(ctx))
	seedRawStatus(t, db, "w1", subj, day0.Add(5*time.Minute), speedAt(day0.Add(5*time.Minute), 12))
	drainNoFlush(t, ctx, runner1)
	require.NoError(t, mat1.RunDailyRollupRefresh(ctx, b1))

	// "Restart": a new materializer over the same catalog.
	runner2, mat2 := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupOn, 0)
	})
	require.NoError(t, mat2.LoadDailyRollupState(ctx))
	require.NoError(t, mat2.RunDailyRollupRefresh(ctx, b1), "already-covered boundary must no-op on the restarted writer")
	assert.Equal(t, b1.Format(time.RFC3339), storedWatermark(t, ctx, db))

	seedRawStatus(t, db, "w2", subj, b1.Add(10*time.Minute), speedAt(b1.Add(10*time.Minute), 24))
	drainNoFlush(t, ctx, runner2)
	require.NoError(t, mat2.RunDailyRollupRefresh(ctx, b2))
	assertRollupMatchesOracle(t, ctx, db)
	assert.EqualValues(t, 2, dumpRollupMap(t, ctx, db)[subj+"|speed"].count)
}
