// ducklake_daily_flip_test.go proves dq#55 step 4: the shadow→on promote
// (including remediation of a duplicate-corrupted live table), the per-pass
// fold going quiet under mode on, and the daily refresh maintaining
// lake.signals_latest itself — still exactly equal to a full recompute over
// settled data.
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

// oracleRecompute rebuilds lake.signals_latest from the full base with a
// mode-off materializer (RecomputeRollup is deliberately disabled under mode
// on) and returns the rows — the exactness oracle for flip tests, valid when
// every seeded row is stamped before the last refreshed boundary.
func oracleRecompute(t *testing.T, ctx context.Context, db *sql.DB) map[string]rollupRow {
	t.Helper()
	oracle, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, oracle.RecomputeRollup(ctx))
	return dumpRollupMap(t, ctx, db)
}

func TestDuckLake_DailyFlip_PromoteHealsAndServes(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:121", vehicleNFT.Hex())
	day0 := time.Now().UTC().AddDate(0, 0, -4).Truncate(24 * time.Hour)
	b1, b2 := day0.AddDate(0, 0, 1), day0.AddDate(0, 0, 2)

	// Shadow era: fold on, shadow table seeded and folded once.
	shadowRunner, shadowMat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupShadow, 0)
	})
	require.NoError(t, shadowMat.LoadDailyRollupState(ctx))
	seedRawStatus(t, db, "fl-1", subj, day0.Add(time.Hour), speedAt(day0.Add(time.Hour), 30))
	drainNoFlush(t, ctx, shadowRunner)
	require.NoError(t, shadowMat.RunDailyRollupRefresh(ctx, b1))
	seedRawStatus(t, db, "fl-2", subj, b1.Add(time.Hour), speedAt(b1.Add(time.Hour), 44), odoAt(b1.Add(time.Hour), 500))
	drainNoFlush(t, ctx, shadowRunner)
	require.NoError(t, shadowMat.RunDailyRollupRefresh(ctx, b2))

	// Manufacture the production corruption: visible duplicate rows on the
	// live table (the per-pass-fold-vs-compaction race residue).
	for i := 0; i < 5; i++ {
		_, err := db.ExecContext(ctx,
			`INSERT INTO lake.signals_latest SELECT * FROM lake.signals_latest WHERE subject = ? AND name = 'speed' LIMIT 1`, subj)
		require.NoError(t, err)
	}
	var liveRows int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.signals_latest WHERE subject = ? AND name = 'speed'", subj).Scan(&liveRows))
	require.Greater(t, liveRows, 1, "corruption manufactured")

	// The flip: a new materializer in mode on. Loading state promotes the
	// shadow table — the corrupted live content is discarded wholesale.
	flipRunner, flipMat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupOn, 0)
	})
	require.NoError(t, flipMat.LoadDailyRollupState(ctx))
	live := dumpRollupMap(t, ctx, db)
	assert.EqualValues(t, 2, live[subj+"|speed"].count, "promoted content is the shadow table's (exact through b2)")
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.signals_latest WHERE subject = ? AND name = 'speed'", subj).Scan(&liveRows))
	assert.Equal(t, 1, liveRows, "promote healed the duplicates")
	var shadowExists int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'signals_latest_daily'`).Scan(&shadowExists))
	assert.Zero(t, shadowExists, "shadow table dropped after verified promote")

	// Fold-off proof: new readings decode into the base but must NOT touch
	// lake.signals_latest until the next refresh.
	newTS := b2.Add(2 * time.Hour)
	seedRawStatus(t, db, "fl-3", subj, newTS, speedAt(newTS, 77))
	drainNoFlush(t, ctx, flipRunner)
	afterDecode := dumpRollupMap(t, ctx, db)
	assert.EqualValues(t, 2, afterDecode[subj+"|speed"].count, "per-pass fold is off: the rollup is untouched by decode")
	assert.EqualValues(t, 44, afterDecode[subj+"|speed"].valueNumber.Float64)

	// The next refresh folds the tail into the LIVE table; the result must
	// equal a full recompute (all data settled < b3).
	b3 := day0.AddDate(0, 0, 3)
	require.NoError(t, flipMat.RunDailyRollupRefresh(ctx, b3))
	got := dumpRollupMap(t, ctx, db)
	assert.EqualValues(t, 3, got[subj+"|speed"].count)
	assert.EqualValues(t, 77, got[subj+"|speed"].valueNumber.Float64)
	want := oracleRecompute(t, ctx, db)
	require.Equal(t, len(want), len(got))
	for k, w := range want {
		g := got[k]
		assert.Equalf(t, w.count, g.count, "%s count", k)
		assert.Truef(t, w.timestamp.Equal(g.timestamp), "%s timestamp", k)
		assert.Truef(t, w.firstSeen.Equal(g.firstSeen), "%s first_seen", k)
		assert.Truef(t, w.lastSeen.Equal(g.lastSeen), "%s last_seen", k)
	}
}

// TestDuckLake_DailyFlip_FreshInstallSeedsLive covers mode on with no shadow
// era at all: the first refresh seeds lake.signals_latest directly (bounded
// recompute, no DROP of the serving table), and subsequent folds maintain it.
func TestDuckLake_DailyFlip_FreshInstallSeedsLive(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:122", vehicleNFT.Hex())
	day0 := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	b1, b2 := day0.AddDate(0, 0, 1), day0.AddDate(0, 0, 2)

	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupOn, 0)
	})
	require.NoError(t, mat.LoadDailyRollupState(ctx))
	seedRawStatus(t, db, "fs-1", subj, day0.Add(time.Hour), speedAt(day0.Add(time.Hour), 25))
	drainNoFlush(t, ctx, runner)
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b1))
	assert.EqualValues(t, 1, dumpRollupMap(t, ctx, db)[subj+"|speed"].count, "seed populated the live table")

	seedRawStatus(t, db, "fs-2", subj, b1.Add(time.Hour), speedAt(b1.Add(time.Hour), 50))
	drainNoFlush(t, ctx, runner)
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b2))
	got := dumpRollupMap(t, ctx, db)
	assert.EqualValues(t, 2, got[subj+"|speed"].count)
	want := oracleRecompute(t, ctx, db)
	for k, w := range want {
		assert.Equalf(t, w.count, got[k].count, "%s count", k)
		assert.Truef(t, w.lastSeen.Equal(got[k].lastSeen), "%s last_seen", k)
	}
}
