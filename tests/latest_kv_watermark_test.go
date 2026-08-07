// latest_kv_watermark_test.go proves dq#55 step 2: once the rollup is
// refreshed from a daily watermark instead of per-pass, the KV
// bootstrap/reconcile merge must ALSO replay the lake.signals tail since that
// watermark — otherwise a subject whose first-ever reading landed after the
// watermark is invisible to the completeness proof, and a reconcile after a
// lost publish would certify a bucket with a real hole (an authoritative
// wrong "no data" under LATEST_KV_NEGATIVE=serve).
//
// The watermark row is created by the REAL writer (RunDailyRollupRefresh) and
// read by latestkv — so these tests are also the cross-package contract that
// the two packages' (deliberately unshared) watermark keys stay equal.
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

// TestLatestKV_ReconcileReplaysTailSinceWatermark manufactures the exact hole
// the tail replay closes: subject B's first-ever reading lands AFTER the
// watermark, its publish is "lost" (the bucket never saw it), and the rollup
// is day-stale (B absent — simulated by deleting B's rollup rows, which is
// what a daily-refreshed rollup looks like before its next fold). The
// reconcile must still produce B's key, or negative serving would lie.
func TestLatestKV_ReconcileReplaysTailSinceWatermark(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "tail-reconcile")
	subjA := fmt.Sprintf("did:erc721:137:%s:101", vehicleNFT.Hex())
	subjB := fmt.Sprintf("did:erc721:137:%s:102", vehicleNFT.Hex())

	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	watermark := day.AddDate(0, 0, 2) // two settled days, then the boundary
	oldTS := day.Add(2 * time.Hour)   // A: before the watermark
	newTS := watermark.Add(90 * time.Minute)

	// No KV publisher wired: every "publish" is lost, the bucket starts empty.
	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupShadow, 0)
	})
	seedRawStatus(t, db, "wmA", subjA, oldTS, speedAt(oldTS, 33))
	drainNoFlush(t, ctx, runner)
	// The real writer establishes the watermark row latestkv must find.
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, watermark))

	// B is born after the watermark, with a same-timestamp collision (the
	// smaller cloud_event_id must win, matching the rollup's recency order)
	// and a location fix.
	seedRawStatus(t, db, "wmB-2", subjB, newTS, speedAt(newTS, 71))
	seedRawStatus(t, db, "wmB-1", subjB, newTS, speedAt(newTS, 70)) // ceid "wmB-1" < "wmB-2": wins the tie
	locTS := newTS.Add(10 * time.Minute)
	seedRawStatus(t, db, "wmB-3", subjB, locTS, locFixAt(locTS, 42.33, -83.05))
	drainNoFlush(t, ctx, runner)

	// Simulate the post-flip world: the rollup is day-stale and has never seen
	// B. (Pre-flip the per-pass fold keeps it fresh, so delete B's rows.)
	_, err := db.ExecContext(ctx, "DELETE FROM lake.signals_latest WHERE subject = ?", subjB)
	require.NoError(t, err)

	require.NoError(t, store.ReconcileFromRollup(ctx, db))

	// A: restored from the rollup pass, as before.
	entryA, err := store.GetEntry(ctx, subjA)
	require.NoError(t, err)
	require.NotNil(t, entryA, "pre-watermark subject restored from the rollup")
	assert.Equal(t, 33.0, entryA.Signals["speed"].Num)

	// B: only the tail replay can know about it.
	entryB, err := store.GetEntry(ctx, subjB)
	require.NoError(t, err)
	require.NotNil(t, entryB, "post-watermark subject must be covered by the tail replay — a miss here becomes a confident wrong 'no data' under negative serving")
	speed := entryB.Signals["speed"]
	assert.Equal(t, 70.0, speed.Num, "same-timestamp collision resolved by cloud_event_id ASC, matching the rollup's recency order")
	assert.True(t, speed.TS.Equal(newTS), "tail value carries the reading's timestamp")
	loc := entryB.Signals["currentLocationCoordinates"]
	require.NotNil(t, loc.Loc, "tail replay carries the location part")
	assert.Equal(t, 42.33, loc.Loc.Lat)
	assert.True(t, loc.Loc.TS.Equal(locTS), "location part stamped with the fix time")

	// The replay is merge-only: running it again changes nothing (no CAS
	// churn, no regression) — same idempotency contract as live publishes.
	require.NoError(t, store.ReconcileFromRollup(ctx, db))
	again, err := store.GetEntry(ctx, subjB)
	require.NoError(t, err)
	assert.Equal(t, entryB.Signals["speed"], again.Signals["speed"])
}

// TestLatestKV_BootstrapReplaysTail proves the bootstrap path gets the same
// tail: a fresh bucket filled by BootstrapFromRollup covers a subject the
// day-stale rollup has never seen. This is the "bootstrap from rollup (≤24h
// stale), then fold signals since watermark" recovery bound from dq#55 —
// O(1–2 partitions), never O(history).
func TestLatestKV_BootstrapReplaysTail(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "tail-bootstrap")
	subj := fmt.Sprintf("did:erc721:137:%s:103", vehicleNFT.Hex())

	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	watermark := day.AddDate(0, 0, 1)
	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupShadow, 0)
	})
	// Establish the watermark first, over an empty pre-boundary lake.
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, watermark))

	newTS := watermark.Add(45 * time.Minute)
	seedRawStatus(t, db, "wmboot-1", subj, newTS, speedAt(newTS, 88))
	drainNoFlush(t, ctx, runner)
	_, err := db.ExecContext(ctx, "DELETE FROM lake.signals_latest WHERE subject = ?", subj)
	require.NoError(t, err)

	require.NoError(t, store.BootstrapFromRollup(ctx, db, false))
	entry, err := store.GetEntry(ctx, subj)
	require.NoError(t, err)
	require.NotNil(t, entry, "bootstrap must cover post-watermark subjects via the tail replay")
	assert.Equal(t, 88.0, entry.Signals["speed"].Num)
}
