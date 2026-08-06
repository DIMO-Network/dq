// ducklake_daily_rollup_test.go proves dq#55 step 1: the daily watermarked
// refresh maintains lake.signals_latest_daily EXACTLY equal to the
// incrementally-folded lake.signals_latest over settled data — across the
// seed, the daily fold (new names, new subjects, collisions, redeliveries,
// location fixes, multi-day catch-up), and the late-arrival path (rows
// stamped before the watermark arriving after it), with the shadow diff
// reporting zero. The live rollup is the oracle here; its own exactness
// against RecomputeRollup is proven by ducklake_incremental_rollup_test.go.
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

func dumpDailyMap(t *testing.T, ctx context.Context, db *sql.DB) map[string]rollupRow {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT subject, name, subject_bucket, "timestamp", value_number, value_string,
			loc_lat, loc_lon, loc_hdop, loc_heading, loc_ts, count, first_seen, last_seen
		 FROM lake.signals_latest_daily ORDER BY subject, name`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	out := map[string]rollupRow{}
	for rows.Next() {
		var r rollupRow
		require.NoError(t, rows.Scan(&r.subject, &r.name, &r.bucket, &r.timestamp, &r.valueNumber, &r.valueString,
			&r.locLat, &r.locLon, &r.locHdop, &r.locHeading, &r.locTS, &r.count, &r.firstSeen, &r.lastSeen))
		out[r.subject+"|"+r.name] = r
	}
	require.NoError(t, rows.Err())
	return out
}

// assertDailyMatchesLive compares the daily table against the live rollup,
// column by column. Valid whenever every seeded row's timestamp is before the
// last refreshed boundary (all data settled), which every scenario below
// arranges — then the two tables must be identical.
func assertDailyMatchesLive(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	live := dumpRollupMap(t, ctx, db)
	daily := dumpDailyMap(t, ctx, db)
	keys := map[string]bool{}
	for k := range live {
		keys[k] = true
	}
	for k := range daily {
		keys[k] = true
	}
	for k := range keys {
		l, okL := live[k]
		d, okD := daily[k]
		require.Truef(t, okL, "%s in the daily table but MISSING from the live rollup", k)
		require.Truef(t, okD, "%s in the live rollup but MISSING from the daily table", k)
		assert.Equalf(t, l.count, d.count, "%s count", k)
		assert.Truef(t, l.timestamp.Equal(d.timestamp), "%s timestamp: live=%s daily=%s", k, l.timestamp, d.timestamp)
		assert.Equalf(t, l.valueNumber, d.valueNumber, "%s value_number", k)
		assert.Equalf(t, l.valueString, d.valueString, "%s value_string", k)
		assert.Truef(t, l.firstSeen.Equal(d.firstSeen), "%s first_seen: live=%s daily=%s", k, l.firstSeen, d.firstSeen)
		assert.Truef(t, l.lastSeen.Equal(d.lastSeen), "%s last_seen: live=%s daily=%s", k, l.lastSeen, d.lastSeen)
		assert.InDeltaf(t, l.locLat, d.locLat, 1e-9, "%s loc_lat", k)
		assert.InDeltaf(t, l.locLon, d.locLon, 1e-9, "%s loc_lon", k)
		assert.InDeltaf(t, l.locHdop, d.locHdop, 1e-9, "%s loc_hdop", k)
		assert.Truef(t, l.locTS.Equal(d.locTS), "%s loc_ts: live=%s daily=%s", k, l.locTS, d.locTS)
	}
}

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

func requireZeroDiff(t *testing.T, counts map[string]int) {
	t.Helper()
	for class, n := range counts {
		assert.Zerof(t, n, "shadow diff class %s", class)
	}
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
		m.WithDailyRollup(materializer.DailyRollupShadow, 0)
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

	// Seed: the daily table becomes the bounded recompute over ts < b1.
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b1))
	assertDailyMatchesLive(t, ctx, db)
	assert.Equal(t, b1.Format(time.RFC3339), storedWatermark(t, ctx, db), "watermark persisted at the boundary")
	counts, err := mat.DailyRollupDiff(ctx)
	require.NoError(t, err)
	requireZeroDiff(t, counts)

	// Guards: a non-midnight boundary is refused; an already-covered boundary
	// is a no-op that must not move the watermark backwards.
	require.Error(t, mat.RunDailyRollupRefresh(ctx, b1.Add(90*time.Minute)))
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b1))
	assert.Equal(t, b1.Format(time.RFC3339), storedWatermark(t, ctx, db))

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
	assertDailyMatchesLive(t, ctx, db)
	counts, err = mat.DailyRollupDiff(ctx)
	require.NoError(t, err)
	requireZeroDiff(t, counts)

	// Days 2-3, folded in ONE refresh (a missed boundary catches up), plus a
	// genuinely late NEW reading: s2 uploads a buffered day-0 row AFTER the
	// watermark has passed it. Without the late set this row's count/first_seen
	// contribution would be lost forever.
	seedRawStatus(t, db, "d2-1", s2, b2.Add(45*time.Minute), speedAt(b2.Add(45*time.Minute), 60))
	seedRawStatus(t, db, "late-1", s2, day0.Add(90*time.Minute), speedAt(day0.Add(90*time.Minute), 58)) // stamped day 0, arrives day 2
	seedRawStatus(t, db, "d3-1", s1, b2.AddDate(0, 0, 1).Add(15*time.Minute), speedAt(b2.AddDate(0, 0, 1).Add(15*time.Minute), 90))
	drainNoFlush(t, ctx, runner)

	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b4))
	assertDailyMatchesLive(t, ctx, db)
	counts, err = mat.DailyRollupDiff(ctx)
	require.NoError(t, err)
	requireZeroDiff(t, counts)
	assert.EqualValues(t, 3, dumpDailyMap(t, ctx, db)[s2+"|speed"].count,
		"the late buffered reading is counted after the late-set recompute")
	var lateLeft int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.rollup_late_subjects").Scan(&lateLeft))
	assert.Zero(t, lateLeft, "late marks cleared by the recompute that covered them")

	// The diff actually detects divergence: corrupt one daily row and expect a
	// mismatch, proving zero-diff above is a real assertion, not vacuity.
	_, err = db.ExecContext(ctx,
		"UPDATE lake.signals_latest_daily SET count = count + 1 WHERE subject = ? AND name = 'speed'", s1)
	require.NoError(t, err)
	counts, err = mat.DailyRollupDiff(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, counts["mismatch"], "a corrupted daily row must surface as a mismatch")
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
		m.WithDailyRollup(materializer.DailyRollupShadow, 0)
	})
	require.NoError(t, mat1.LoadDailyRollupState(ctx))
	seedRawStatus(t, db, "w1", subj, day0.Add(5*time.Minute), speedAt(day0.Add(5*time.Minute), 12))
	drainNoFlush(t, ctx, runner1)
	require.NoError(t, mat1.RunDailyRollupRefresh(ctx, b1))

	// "Restart": a new materializer over the same catalog.
	runner2, mat2 := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupShadow, 0)
	})
	require.NoError(t, mat2.LoadDailyRollupState(ctx))
	require.NoError(t, mat2.RunDailyRollupRefresh(ctx, b1), "already-covered boundary must no-op on the restarted writer")
	assert.Equal(t, b1.Format(time.RFC3339), storedWatermark(t, ctx, db))

	seedRawStatus(t, db, "w2", subj, b1.Add(10*time.Minute), speedAt(b1.Add(10*time.Minute), 24))
	drainNoFlush(t, ctx, runner2)
	require.NoError(t, mat2.RunDailyRollupRefresh(ctx, b2))
	assertDailyMatchesLive(t, ctx, db)
	assert.EqualValues(t, 2, dumpDailyMap(t, ctx, db)[subj+"|speed"].count)
}
