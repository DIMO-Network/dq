// latest_kv_extended_test.go covers dq#55 step 3: allLatest and
// availableSignals served from the signals-latest KV entry (same Entry the
// proven signalsLatest path reads — these tests pin the CONVERSION to the
// rollup SQL's answers), and summaries as the exact (rollup ∪
// tail-since-watermark) union once the rollup is daily-refreshed.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/app"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/latestkv"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLatestKVExt_AllLatestAndAvailableServeMatchRollup(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "ext-serve")
	subject := fmt.Sprintf("did:erc721:137:%s:111", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1, t2, fixT := day.Add(time.Hour), day.Add(2*time.Hour), day.Add(90*time.Minute)

	seedRawStatus(t, db, "ext-1", subject, t1, speedAt(t1, 40), odoAt(t1, 1200))
	seedRawStatus(t, db, "ext-2", subject, fixT, locFixAt(fixT, 42.33, -83.05))
	seedRawStatus(t, db, "ext-3", subject, t2, speedAt(t2, 65))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithLatestPublisher(app.NewLatestKVPublisher(store, nil, zerolog.Nop()))
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 3, drainRunner(t, ctx, runner))

	rollupQ := duck.NewLakeQueries(svc)
	serveQ := duck.NewLakeQueries(svc).
		WithLatestKV(store, duck.KVReadServe, zerolog.Nop()).
		WithLatestKVExtended(duck.KVReadServe)

	// allLatest: the KV answer must be the rollup answer, row for row —
	// including the location fix winning its row's timestamp and the virtual
	// lastSeen row.
	want, err := rollupQ.GetAllLatestSignals(ctx, subject, nil)
	require.NoError(t, err)
	got, err := serveQ.GetAllLatestSignals(ctx, subject, nil)
	require.NoError(t, err)
	require.Len(t, got, len(want), "same row count as the rollup path")
	for i := range want {
		assert.Equal(t, want[i].Data.Name, got[i].Data.Name)
		assert.Truef(t, want[i].Data.Timestamp.Equal(got[i].Data.Timestamp), "%s timestamp: rollup=%s kv=%s",
			want[i].Data.Name, want[i].Data.Timestamp, got[i].Data.Timestamp)
		assert.Equal(t, want[i].Data.ValueNumber, got[i].Data.ValueNumber, want[i].Data.Name)
		assert.Equal(t, want[i].Data.ValueString, got[i].Data.ValueString, want[i].Data.Name)
		assert.Equal(t, want[i].Data.ValueLocation, got[i].Data.ValueLocation, want[i].Data.Name)
	}

	// availableSignals: the entry's key set equals the rollup's DISTINCT names.
	wantNames, err := rollupQ.GetAvailableSignals(ctx, subject, nil)
	require.NoError(t, err)
	gotNames, err := serveQ.GetAvailableSignals(ctx, subject, nil)
	require.NoError(t, err)
	assert.Equal(t, wantNames, gotNames)

	// A subject the cache has never seen falls back to the rollup path.
	missing := fmt.Sprintf("did:erc721:137:%s:404", vehicleNFT.Hex())
	fbAll, err := serveQ.GetAllLatestSignals(ctx, missing, nil)
	require.NoError(t, err)
	rbAll, err := rollupQ.GetAllLatestSignals(ctx, missing, nil)
	require.NoError(t, err)
	assert.Equal(t, len(rbAll), len(fbAll), "miss without negative serving must serve the rollup answer")

	// Shadow mode never changes what is served.
	shadowQ := duck.NewLakeQueries(svc).
		WithLatestKV(store, duck.KVReadServe, zerolog.Nop()).
		WithLatestKVExtended(duck.KVReadShadow)
	shAll, err := shadowQ.GetAllLatestSignals(ctx, subject, nil)
	require.NoError(t, err)
	assert.Equal(t, len(want), len(shAll))
	shNames, err := shadowQ.GetAvailableSignals(ctx, subject, nil)
	require.NoError(t, err)
	assert.Equal(t, wantNames, shNames)

	// Authoritative miss under live coverage: the empty answers, with no lake
	// read — allLatest is exactly the rollup's empty shape (the virtual
	// lastSeen row at epoch), availableSignals is nil.
	require.NoError(t, store.PutCoverage(ctx, latestkv.Coverage{VerifiedThrough: time.Now()}))
	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	watcher, err := store.WatchCoverage(watchCtx)
	require.NoError(t, err)
	negQ := duck.NewLakeQueries(svc).
		WithLatestKV(store, duck.KVReadServe, zerolog.Nop()).
		WithLatestKVExtended(duck.KVReadServe).
		WithLatestKVNegative(watcher, duck.KVNegativeServe)
	negAll, err := negQ.GetAllLatestSignals(ctx, missing, nil)
	require.NoError(t, err)
	require.Len(t, negAll, 1, "authoritative miss serves only the virtual lastSeen row")
	assert.Equal(t, model.LastSeenField, negAll[0].Data.Name)
	assert.Equal(t, time.Unix(0, 0).UTC(), negAll[0].Data.Timestamp.UTC(), "epoch lastSeen — the rollup's empty answer")
	negNames, err := negQ.GetAvailableSignals(ctx, missing, nil)
	require.NoError(t, err)
	assert.Nil(t, negNames, "authoritative miss serves the rollup's nil answer")
}

// TestSignalSummaries_DailyServingUnionExact pins the (rollup ∪ tail) union:
// against a day-stale rollup (simulated by swapping in the shadow table the
// daily refresh maintains), summaries under LAKE_ROLLUP_DAILY_SERVING must
// equal the answers the fresh rollup gave — counts, first/last seen, and a
// name first seen only AFTER the watermark (which exists in no rollup row at
// all and only the tail can count).
func TestSignalSummaries_DailyServingUnionExact(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:112", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	watermark := day.AddDate(0, 0, 2)

	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithDailyRollup(materializer.DailyRollupShadow, 0)
	})
	// Pre-watermark history: two names, multiple readings, a redelivery.
	seedRawStatus(t, db, "su-1", subject, day.Add(1*time.Hour), speedAt(day.Add(1*time.Hour), 10))
	seedRawStatus(t, db, "su-2", subject, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 20), odoAt(day.Add(2*time.Hour), 900))
	seedRawStatus(t, db, "su-1", subject, day.Add(1*time.Hour), speedAt(day.Add(1*time.Hour), 10)) // redelivery: no double count
	drainNoFlush(t, ctx, runner)
	require.NoError(t, mat.RunDailyRollupRefresh(ctx, watermark))

	// Post-watermark tail: more speed readings and a name BORN after W.
	newTS := watermark.Add(30 * time.Minute)
	seedRawStatus(t, db, "su-3", subject, newTS, speedAt(newTS, 30),
		map[string]any{"name": "powertrainRange", "timestamp": newTS.Format(time.RFC3339Nano), "value": 250.0})
	drainNoFlush(t, ctx, runner)

	// Expected: the answers off the FRESH per-pass rollup (exact today).
	expected, err := duck.NewLakeQueries(svc).GetSignalSummaries(ctx, subject, nil)
	require.NoError(t, err)
	require.NotEmpty(t, expected)

	// Simulate the step-4 flip: signals_latest becomes the daily table's
	// content — exact as of the watermark, blind to the tail.
	_, err = db.ExecContext(ctx, "DELETE FROM lake.signals_latest")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO lake.signals_latest SELECT * FROM lake.signals_latest_daily")
	require.NoError(t, err)

	// Sanity: the plain rollup read is now WRONG (missing the tail) — the
	// union assertion below is not vacuous.
	stale, err := duck.NewLakeQueries(svc).GetSignalSummaries(ctx, subject, nil)
	require.NoError(t, err)
	require.NotEqual(t, len(expected), len(stale), "day-stale rollup must be missing the tail-born name")

	got, err := duck.NewLakeQueries(svc).WithDailyServingRollup(true).GetSignalSummaries(ctx, subject, nil)
	require.NoError(t, err)
	require.Len(t, got, len(expected))
	for i := range expected {
		assert.Equal(t, expected[i].Name, got[i].Name)
		assert.Equalf(t, expected[i].NumberOfSignals, got[i].NumberOfSignals, "%s count", expected[i].Name)
		assert.Truef(t, expected[i].FirstSeen.Equal(got[i].FirstSeen), "%s first_seen", expected[i].Name)
		assert.Truef(t, expected[i].LastSeen.Equal(got[i].LastSeen), "%s last_seen", expected[i].Name)
	}
}
