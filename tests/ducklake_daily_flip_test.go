// ducklake_daily_flip_test.go proves the dq#55 boot transitions of the daily
// refresh: the shadow→on promote (a leftover shadow-era
// lake.signals_latest_daily table found at boot under mode on is swapped into
// lake.signals_latest — including remediation of a duplicate-corrupted live
// table — then dropped forever), decode leaving lake.signals_latest untouched
// (the per-pass fold is gone, step 5), and the daily refresh maintaining the
// live table itself — still exactly equal to a full recompute over settled
// data. Shadow MODE no longer exists, so the shadow-era state is manufactured
// directly: the table, its content (a bounded recompute), and the watermark
// row — exactly what a node upgrading straight from mode=shadow boots with.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_DailyFlip_PromoteHealsAndServes(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:121", vehicleNFT.Hex())
	day0 := time.Now().UTC().AddDate(0, 0, -4).Truncate(24 * time.Hour)
	b1, b2 := day0.AddDate(0, 0, 1), day0.AddDate(0, 0, 2)

	// Base rows across two settled days, decoded by a default-mode writer.
	preRunner, preMat := incrRunner(t, ctx, db)
	seedRawStatus(t, db, "fl-1", subj, day0.Add(time.Hour), speedAt(day0.Add(time.Hour), 30))
	seedRawStatus(t, db, "fl-2", subj, b1.Add(time.Hour), speedAt(b1.Add(time.Hour), 44), odoAt(b1.Add(time.Hour), 500))
	drainNoFlush(t, ctx, preRunner)

	// Manufacture the shadow-era leftovers a straight-from-shadow upgrade boots
	// with: a validated one-row-per-key shadow table exact through b2 (all data
	// is < b2, so a full recompute IS the bounded one) and its watermark row.
	require.NoError(t, preMat.RecomputeRollup(ctx))
	_, err := db.ExecContext(ctx, "CREATE TABLE lake.signals_latest_daily AS SELECT * FROM lake.signals_latest")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO lake.ingest_progress (partition, cursor) VALUES (?, ?)",
		"lake.signals_latest#daily_watermark", b2.Format(time.RFC3339))
	require.NoError(t, err)

	// Manufacture the production corruption on the live table: visible
	// duplicate rows (the per-pass-fold-vs-compaction race residue).
	for i := 0; i < 5; i++ {
		_, err := db.ExecContext(ctx,
			`INSERT INTO lake.signals_latest SELECT * FROM lake.signals_latest WHERE subject = ? AND name = 'speed' LIMIT 1`, subj)
		require.NoError(t, err)
	}
	var liveRows int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.signals_latest WHERE subject = ? AND name = 'speed'", subj).Scan(&liveRows))
	require.Greater(t, liveRows, 1, "corruption manufactured")

	// The flip boot: a new materializer in mode on. Loading state promotes the
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
	assert.EqualValues(t, 2, afterDecode[subj+"|speed"].count, "per-pass fold is gone: the rollup is untouched by decode")
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

// TestDuckLake_DailyFlip_AbortedShadowSeedIsDropped covers the degenerate
// leftover: a shadow table WITHOUT a watermark (an aborted shadow-era seed) is
// worthless as a promote source — boot under mode on must drop it, leave the
// live table alone, and let the first refresh seed lake.signals_latest.
func TestDuckLake_DailyFlip_AbortedShadowSeedIsDropped(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:123", vehicleNFT.Hex())
	day0 := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	b1 := day0.AddDate(0, 0, 1)

	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupOn, 0)
	})
	seedRawStatus(t, db, "as-1", subj, day0.Add(time.Hour), speedAt(day0.Add(time.Hour), 41))
	drainNoFlush(t, ctx, runner)
	// A shadow table with no watermark row: the aborted-seed leftover.
	_, err := db.ExecContext(ctx, "CREATE TABLE lake.signals_latest_daily AS SELECT * FROM lake.signals_latest WHERE false")
	require.NoError(t, err)

	require.NoError(t, mat.LoadDailyRollupState(ctx))
	var shadowExists int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'signals_latest_daily'`).Scan(&shadowExists))
	assert.Zero(t, shadowExists, "unwatermarked shadow table dropped, not promoted")

	require.NoError(t, mat.RunDailyRollupRefresh(ctx, b1))
	assert.EqualValues(t, 1, dumpRollupMap(t, ctx, db)[subj+"|speed"].count, "first refresh seeds the live table")
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
