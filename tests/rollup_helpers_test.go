// rollup_helpers_test.go — shared fixtures for the lake.signals_latest rollup
// tests: the row shape + dump, the materializer/runner constructor, the
// no-flush drain, and the full-recompute oracle. Extracted from the (deleted)
// incremental-fold differential test when dq#55 step 5 removed the per-pass
// fold; the daily refresh (daily_rollup.go) is now the rollup's only
// steady-state writer, and these helpers are how its output is checked.
package tests

import (
	"context"
	"database/sql"
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

// drainNoFlush runs the decode loop to completion WITHOUT calling FlushRollup.
// Since dq#55 step 5 nothing maintains lake.signals_latest at decode time, so
// after this drain the rollup holds whatever the last daily refresh (or
// recompute) left — tests asserting rollup content must run
// RunDailyRollupRefresh (mode on) or RecomputeRollup explicitly first.
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

// oracleRecompute rebuilds lake.signals_latest IN PLACE from the full base
// with a fresh default-mode materializer (RecomputeRollup is deliberately
// disabled under mode on) and returns the rows — the exactness oracle for the
// daily-refresh tests, valid when every seeded row is stamped before the last
// refreshed boundary. Capture the refresh's answer BEFORE calling this: the
// rebuild overwrites the table being checked.
func oracleRecompute(t *testing.T, ctx context.Context, db *sql.DB) map[string]rollupRow {
	t.Helper()
	oracle, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, oracle.RecomputeRollup(ctx))
	return dumpRollupMap(t, ctx, db)
}

// assertRollupMatchesOracle asserts the current lake.signals_latest (as the
// daily refresh left it) is column-for-column identical to a full recompute
// over the deduped base. Valid whenever every seeded row's timestamp is before
// the last refreshed boundary (all data settled), which callers arrange.
func assertRollupMatchesOracle(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	got := dumpRollupMap(t, ctx, db)
	want := oracleRecompute(t, ctx, db)
	keys := map[string]bool{}
	for k := range got {
		keys[k] = true
	}
	for k := range want {
		keys[k] = true
	}
	for k := range keys {
		g, okG := got[k]
		w, okW := want[k]
		require.Truef(t, okG, "%s present after the oracle recompute but MISSING from the daily-refreshed rollup", k)
		require.Truef(t, okW, "%s present in the daily-refreshed rollup but MISSING after the oracle recompute", k)
		assert.Equalf(t, w.count, g.count, "%s count", k)
		assert.Truef(t, w.timestamp.Equal(g.timestamp), "%s timestamp: oracle=%s refresh=%s", k, w.timestamp, g.timestamp)
		assert.Equalf(t, w.valueNumber, g.valueNumber, "%s value_number", k)
		assert.Equalf(t, w.valueString, g.valueString, "%s value_string", k)
		assert.Truef(t, w.firstSeen.Equal(g.firstSeen), "%s first_seen: oracle=%s refresh=%s", k, w.firstSeen, g.firstSeen)
		assert.Truef(t, w.lastSeen.Equal(g.lastSeen), "%s last_seen: oracle=%s refresh=%s", k, w.lastSeen, g.lastSeen)
		assert.InDeltaf(t, w.locLat, g.locLat, 1e-9, "%s loc_lat", k)
		assert.InDeltaf(t, w.locLon, g.locLon, 1e-9, "%s loc_lon", k)
		assert.InDeltaf(t, w.locHdop, g.locHdop, 1e-9, "%s loc_hdop", k)
		assert.Truef(t, w.locTS.Equal(g.locTS), "%s loc_ts: oracle=%s refresh=%s", k, w.locTS, g.locTS)
	}
}

// refreshRollup makes lake.signals_latest current through boundary via the
// PRODUCTION maintenance path: it enables the daily rollup on mat and runs one
// refresh. Since dq#55 step 5 nothing maintains the rollup at decode time, so
// any test that drains and then reads rollup-served answers calls this first.
// boundary must be a UTC midnight covering every seeded timestamp; readings
// stamped before an already-set watermark are healed via the late-subject path
// (they were marked during the drain), so successive drain+refresh phases with
// increasing boundaries stay exact. Note mat is mode-on afterwards
// (RecomputeRollup on it is refused — use oracleRecompute for a DR rebuild).
func refreshRollup(t *testing.T, ctx context.Context, mat *materializer.DuckLakeMaterializer, boundary time.Time) {
	t.Helper()
	mat.WithDailyRollup(materializer.DailyRollupOn, 0)
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, boundary))
}

func odoAt(ts time.Time, v float64) map[string]any {
	return map[string]any{"name": "powertrainTransmissionTravelledDistance", "timestamp": ts.Format(time.RFC3339Nano), "value": v}
}
