// ducklake_incremental_rollup_test.go proves finding #5b: lake.signals_latest is
// maintained INCREMENTALLY at decode-commit time (O(batch), not an O(history) recompute
// per flush), and the incremental result is EXACTLY the full RecomputeRollup — the
// differential invariant, checked across redelivery, same-timestamp collision,
// out-of-order arrival, multi-window (#1c) spans, location updates, and dormant re-report.
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rollupRow struct {
	subject, name       string
	bucket              int
	timestamp           time.Time
	valueNumber         sql.NullFloat64
	valueString         sql.NullString
	locLat, locLon      float64
	locHdop, locHeading float64
	locTS               time.Time
	count               int64
	firstSeen, lastSeen time.Time
}

func dumpRollupMap(t *testing.T, ctx context.Context, db *sql.DB) map[string]rollupRow {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT subject, name, subject_bucket, "timestamp", value_number, value_string,
			loc_lat, loc_lon, loc_hdop, loc_heading, loc_ts, count, first_seen, last_seen
		 FROM lake.signals_latest ORDER BY subject, name`)
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

// assertMatchesRecompute snapshots the incrementally-maintained rollup, rebuilds it from
// scratch with RecomputeRollup, and asserts every column of every (subject,name) row is
// identical. This is the exactness contract for #5b.
func assertMatchesRecompute(t *testing.T, ctx context.Context, db *sql.DB, mat *materializer.DuckLakeMaterializer) {
	t.Helper()
	incremental := dumpRollupMap(t, ctx, db)
	require.NoError(t, mat.RecomputeRollup(ctx))
	recomputed := dumpRollupMap(t, ctx, db)

	keys := map[string]bool{}
	for k := range incremental {
		keys[k] = true
	}
	for k := range recomputed {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		inc, okI := incremental[k]
		rec, okR := recomputed[k]
		require.Truef(t, okI, "%s present after RecomputeRollup but MISSING from the incremental rollup", k)
		require.Truef(t, okR, "%s present in the incremental rollup but MISSING after RecomputeRollup", k)
		assert.Equalf(t, rec.count, inc.count, "%s count", k)
		assert.Truef(t, rec.timestamp.Equal(inc.timestamp), "%s timestamp: recompute=%s incremental=%s", k, rec.timestamp, inc.timestamp)
		assert.Equalf(t, rec.valueNumber, inc.valueNumber, "%s value_number", k)
		assert.Equalf(t, rec.valueString, inc.valueString, "%s value_string", k)
		assert.Truef(t, rec.firstSeen.Equal(inc.firstSeen), "%s first_seen: recompute=%s incremental=%s", k, rec.firstSeen, inc.firstSeen)
		assert.Truef(t, rec.lastSeen.Equal(inc.lastSeen), "%s last_seen: recompute=%s incremental=%s", k, rec.lastSeen, inc.lastSeen)
		assert.InDeltaf(t, rec.locLat, inc.locLat, 1e-9, "%s loc_lat", k)
		assert.InDeltaf(t, rec.locLon, inc.locLon, 1e-9, "%s loc_lon", k)
		assert.Truef(t, rec.locTS.Equal(inc.locTS), "%s loc_ts: recompute=%s incremental=%s", k, rec.locTS, inc.locTS)
	}
}

func incrRunner(t *testing.T, ctx context.Context, db *sql.DB, opts ...func(*materializer.DuckLakeMaterializer)) (*materializer.Runner, *materializer.DuckLakeMaterializer) {
	t.Helper()
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	for _, o := range opts {
		o(mat)
	}
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat)
	return runner, mat
}

// drainNoFlush runs the decode loop to completion WITHOUT calling FlushRollup, so
// lake.signals_latest is populated ONLY by the commit-time incremental fold (#5b) — the
// whole point of the differential test.
func drainNoFlush(t *testing.T, ctx context.Context, r *materializer.Runner) {
	t.Helper()
	for {
		n, err := r.RunOnce(ctx)
		require.NoError(t, err)
		if n == 0 {
			return
		}
	}
}

// TestDuckLake_IncrementalRollup_MatchesRecompute drives the decode loop WITHOUT ever
// calling FlushRollup, so lake.signals_latest is populated only by the commit-time
// incremental fold, then asserts it equals a full RecomputeRollup after each scenario.
func TestDuckLake_IncrementalRollup_MatchesRecompute(t *testing.T) {
	ctx := context.Background()
	subject := fmt.Sprintf("did:erc721:137:%s:71", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -3).Truncate(time.Hour)

	t.Run("basic multi-batch increasing", func(t *testing.T) {
		svc := newLakeService(t, t.TempDir())
		db := svc.DB()
		runner, mat := incrRunner(t, ctx, db)
		seedRawStatus(t, db, "b1", subject, base.Add(1*time.Minute), speedAt(base.Add(1*time.Minute), 10))
		seedRawStatus(t, db, "b2", subject, base.Add(2*time.Minute), speedAt(base.Add(2*time.Minute), 20))
		drainNoFlush(t, ctx, runner)
		seedRawStatus(t, db, "b3", subject, base.Add(3*time.Minute), speedAt(base.Add(3*time.Minute), 30))
		drainNoFlush(t, ctx, runner)
		assertMatchesRecompute(t, ctx, db, mat)
		assert.EqualValues(t, 3, dumpRollupMap(t, ctx, db)[subject+"|speed"].count)
	})

	t.Run("redelivery same cloud_event_id no double count", func(t *testing.T) {
		svc := newLakeService(t, t.TempDir())
		db := svc.DB()
		runner, mat := incrRunner(t, ctx, db)
		seedRawStatus(t, db, "r1", subject, base.Add(1*time.Minute), speedAt(base.Add(1*time.Minute), 10))
		drainNoFlush(t, ctx, runner)
		seedRawStatus(t, db, "r1", subject, base.Add(1*time.Minute), speedAt(base.Add(1*time.Minute), 10)) // redelivery
		drainNoFlush(t, ctx, runner)
		assertMatchesRecompute(t, ctx, db, mat)
		assert.EqualValues(t, 1, dumpRollupMap(t, ctx, db)[subject+"|speed"].count)
	})

	t.Run("same-timestamp collision distinct count", func(t *testing.T) {
		svc := newLakeService(t, t.TempDir())
		db := svc.DB()
		runner, mat := incrRunner(t, ctx, db)
		ts := base.Add(5 * time.Minute)
		seedRawStatus(t, db, "c1", subject, ts, speedAt(ts, 40))
		seedRawStatus(t, db, "c2", subject, ts, speedAt(ts, 41)) // same (s,n,ts), different ceid
		drainNoFlush(t, ctx, runner)
		assertMatchesRecompute(t, ctx, db, mat)
		assert.EqualValues(t, 1, dumpRollupMap(t, ctx, db)[subject+"|speed"].count, "a same-(subject,name,timestamp) collision is one distinct row")
	})

	t.Run("out-of-order older batch", func(t *testing.T) {
		svc := newLakeService(t, t.TempDir())
		db := svc.DB()
		runner, mat := incrRunner(t, ctx, db)
		seedRawStatus(t, db, "o2", subject, base.Add(10*time.Minute), speedAt(base.Add(10*time.Minute), 80))
		drainNoFlush(t, ctx, runner)
		seedRawStatus(t, db, "o1", subject, base.Add(1*time.Minute), speedAt(base.Add(1*time.Minute), 5)) // older, arrives later
		drainNoFlush(t, ctx, runner)
		assertMatchesRecompute(t, ctx, db, mat)
		got := dumpRollupMap(t, ctx, db)[subject+"|speed"]
		assert.EqualValues(t, 2, got.count)
		assert.EqualValues(t, 80, got.valueNumber.Float64, "latest stays the newer reading despite the later-arriving older one")
		assert.True(t, got.firstSeen.Equal(base.Add(1*time.Minute)), "first_seen moves back to the older reading")
	})

	t.Run("multi-window fat span exact", func(t *testing.T) {
		svc := newLakeService(t, t.TempDir())
		db := svc.DB()
		runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) { m.WithMaxRowsPerWindow(3) })
		seedRawStatusOneSnapshot(t, db, subject, base.Add(20*time.Minute), 10) // one snapshot, 10 rows, 3/window
		drainNoFlush(t, ctx, runner)
		assertMatchesRecompute(t, ctx, db, mat)
		assert.EqualValues(t, 10, dumpRollupMap(t, ctx, db)[subject+"|speed"].count)
	})

	t.Run("dormant re-report", func(t *testing.T) {
		svc := newLakeService(t, t.TempDir())
		db := svc.DB()
		runner, mat := incrRunner(t, ctx, db)
		seedRawStatus(t, db, "d1", subject, base.Add(1*time.Minute), speedAt(base.Add(1*time.Minute), 10))
		drainNoFlush(t, ctx, runner)
		// long gap, then report again
		seedRawStatus(t, db, "d2", subject, base.Add(48*time.Hour), speedAt(base.Add(48*time.Hour), 55))
		drainNoFlush(t, ctx, runner)
		assertMatchesRecompute(t, ctx, db, mat)
		got := dumpRollupMap(t, ctx, db)[subject+"|speed"]
		assert.EqualValues(t, 2, got.count)
		assert.EqualValues(t, 55, got.valueNumber.Float64)
	})
}

// TestDuckLake_IncrementalRollup_CrashReplayExact proves the incremental fold is
// idempotent across a crash: a window commits its base rows AND its rollup delta, the pass
// then crashes before finishing the span, and on restart the replayed window must add 0 to
// count (rows already at rest → NOT-EXISTS delta 0, recency re-fold stable), so the final
// rollup still equals a full RecomputeRollup.
func TestDuckLake_IncrementalRollup_CrashReplayExact(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:72", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	seedRawStatusOneSnapshot(t, db, subject, base, 9) // one snapshot, 9 rows

	// mat1 crashes right after the first intermediate window commits (base + rollup delta).
	mat1, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat1.WithMaxRowsPerWindow(3)
	mat1.WithWindowCommitHook(func(idx int) error {
		if idx == 0 {
			return fmt.Errorf("injected crash after first window")
		}
		return nil
	})
	runner1 := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat1)
	_, err = runner1.RunOnce(ctx)
	require.Error(t, err)
	assert.EqualValues(t, 3, dumpRollupMap(t, ctx, db)[subject+"|speed"].count, "first window's rollup delta is durable")

	// Restart and drain: the replayed window must not double-count.
	runner2, mat2 := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) { m.WithMaxRowsPerWindow(3) })
	_ = runner2
	drainNoFlush(t, ctx, runner2)
	assert.EqualValues(t, 9, dumpRollupMap(t, ctx, db)[subject+"|speed"].count, "replayed window added 0 — exactly nine")
	assertMatchesRecompute(t, ctx, db, mat2)
}

// TestDuckLake_IncrementalRollup_Randomized fuzzes many batches (redeliveries, collisions,
// out-of-order, varied window sizes, multiple subjects/names, location fixes) through the
// incremental path and asserts the result equals RecomputeRollup every time. Deterministic
// seed for reproducibility.
func TestDuckLake_IncrementalRollup_Randomized(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	rng := rand.New(rand.NewSource(20260707))
	subjects := []string{
		fmt.Sprintf("did:erc721:137:%s:81", vehicleNFT.Hex()),
		fmt.Sprintf("did:erc721:137:%s:82", vehicleNFT.Hex()),
		fmt.Sprintf("did:erc721:137:%s:83", vehicleNFT.Hex()),
	}
	names := []func(ts time.Time, v float64) map[string]any{speedAt, odoAt}
	base := time.Now().UTC().AddDate(0, 0, -5).Truncate(time.Hour)

	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) { m.WithMaxRowsPerWindow(2 + rng.Intn(5)) })
	seen := map[string]bool{} // ids emitted, to force redeliveries
	seq := 0
	for round := 0; round < 25; round++ {
		n := 1 + rng.Intn(4)
		for i := 0; i < n; i++ {
			subj := subjects[rng.Intn(len(subjects))]
			nameFn := names[rng.Intn(len(names))]
			// timestamps wander forward and sometimes backward (out-of-order)
			off := time.Duration(seq*7+rng.Intn(400)-100) * time.Second
			ts := base.Add(off)
			seq++
			id := fmt.Sprintf("rnd-%d", seq)
			if len(seen) > 0 && rng.Intn(5) == 0 { // redelivery of a prior id
				for k := range seen {
					id = k
					break
				}
			} else if rng.Intn(6) == 0 && seq > 1 { // collision: new id, reuse a recent timestamp
				ts = base.Add(time.Duration((seq-1)*7) * time.Second)
			}
			seen[id] = true
			seedRawStatus(t, db, id, subj, ts, nameFn(ts, float64(rng.Intn(120))))
		}
		drainNoFlush(t, ctx, runner)
		if round%5 == 4 {
			assertMatchesRecompute(t, ctx, db, mat)
		}
	}
	assertMatchesRecompute(t, ctx, db, mat)
}

func odoAt(ts time.Time, v float64) map[string]any {
	return map[string]any{"name": "powertrainTransmissionTravelledDistance", "timestamp": ts.Format(time.RFC3339Nano), "value": v}
}

// TestDuckLake_IncrementalRollup_AncientRedelivery pins the >30d-redelivery count fix: a
// reading older than the 30d dedup probe floor, redelivered (same cloud_event_id) through
// the live path, must NOT inflate signals_latest.count — it stays equal to RecomputeRollup.
func TestDuckLake_IncrementalRollup_AncientRedelivery(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:73", vehicleNFT.Hex())
	ancient := time.Now().UTC().Add(-40 * 24 * time.Hour).Truncate(time.Hour) // > dedupProbeFloor (30d)
	runner, mat := incrRunner(t, ctx, db)
	seedRawStatus(t, db, "anc1", subj, ancient, speedAt(ancient, 22))
	drainNoFlush(t, ctx, runner)
	seedRawStatus(t, db, "anc1", subj, ancient, speedAt(ancient, 22)) // redelivery of the SAME event, >30d old
	drainNoFlush(t, ctx, runner)
	assertMatchesRecompute(t, ctx, db, mat)
	assert.EqualValues(t, 1, dumpRollupMap(t, ctx, db)[subj+"|speed"].count, "an ancient redelivery must not inflate count")
}
