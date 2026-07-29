// latest_kv_negative_test.go covers dq#42: answering a cache MISS as an
// authoritative "no data" instead of falling back to the rollup. The property
// under test is agreement — the answer served from a miss must be
// indistinguishable from the ~795ms rollup read it replaces — plus the gate
// that makes it admissible: a miss is only an answer while the writer's
// coverage assertion is live.
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// negativeQueries builds a serve-mode reader with authoritative-miss serving in
// mode, after asserting coverage on the bucket.
func negativeQueries(t *testing.T, svc *duck.Service, store *latestkv.Store, mode duck.KVNegativeMode) *duck.Queries {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.PutCoverage(ctx, latestkv.Coverage{VerifiedThrough: time.Now()}))
	watcher, err := store.WatchCoverage(ctx)
	require.NoError(t, err)
	require.True(t, watcher.Trusted(), "coverage should be trusted right after it is asserted")
	return duck.NewLakeQueries(svc).
		WithLatestKV(store, duck.KVReadServe, zerolog.Nop()).
		WithLatestKVNegative(watcher, mode)
}

// TestLatestKVNegative_MissMatchesRollupAnswer is the equivalence the whole
// change rests on: for a subject with no data, the answer built from a missing
// key must equal the answer the rollup produces by scanning and finding nothing.
func TestLatestKVNegative_MissMatchesRollupAnswer(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "neg-equiv")
	subject := fmt.Sprintf("did:erc721:137:%s:31", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1 := day.Add(time.Hour)

	// Seed one subject so the rollup table exists and is populated; the subject
	// under test is a different one that never reported.
	seedRawStatus(t, db, "neg-1", subject, t1, speedAt(t1, 40))
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithLatestPublisher(app.NewLatestKVPublisher(store, nil, zerolog.Nop()))
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	rollupQ := duck.NewLakeQueries(svc)
	negQ := negativeQueries(t, svc, store, duck.KVNegativeServe)
	missing := fmt.Sprintf("did:erc721:137:%s:999", vehicleNFT.Hex())

	// Every request shape a no-data subject can arrive in, including the mixed
	// one: the epoch lastSeen row and the absence of value/location rows are
	// both part of the contract.
	shapes := map[string]*model.LatestSignalsArgs{
		"named + lastSeen": latestArgsFor("speed"),
		"named only": func() *model.LatestSignalsArgs {
			a := latestArgsFor("speed", "odometer")
			a.IncludeLastSeen = false
			return a
		}(),
		"lastSeen only": func() *model.LatestSignalsArgs {
			a := latestArgsFor()
			return a
		}(),
		"location": func() *model.LatestSignalsArgs {
			a := latestArgsFor("speed")
			a.LocationSignalNames["currentLocationCoordinates"] = struct{}{}
			return a
		}(),
	}
	for name, args := range shapes {
		t.Run(name, func(t *testing.T) {
			want, err := rollupQ.GetLatestSignals(ctx, missing, args)
			require.NoError(t, err)
			got, err := negQ.GetLatestSignals(ctx, missing, args)
			require.NoError(t, err)
			require.Len(t, got, len(want), "authoritative miss must return the rollup's row count")
			for i := range want {
				assert.Equal(t, want[i].Data.Name, got[i].Data.Name)
				assert.Equal(t, want[i].Data.Timestamp, got[i].Data.Timestamp, want[i].Data.Name)
				assert.Equal(t, want[i].Data.ValueNumber, got[i].Data.ValueNumber, want[i].Data.Name)
				assert.Equal(t, want[i].Data.ValueLocation, got[i].Data.ValueLocation, want[i].Data.Name)
			}
		})
	}
}

// TestLatestKVNegative_HitsStillServeRealData guards against the negative path
// swallowing subjects that DO have data — the mistake that would matter most.
func TestLatestKVNegative_HitsStillServeRealData(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "neg-hit")
	subject := fmt.Sprintf("did:erc721:137:%s:32", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1, t2 := day.Add(time.Hour), day.Add(2*time.Hour)

	seedRawStatus(t, db, "neg-h1", subject, t1, speedAt(t1, 40))
	seedRawStatus(t, db, "neg-h2", subject, t2, speedAt(t2, 65))
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithLatestPublisher(app.NewLatestKVPublisher(store, nil, zerolog.Nop()))
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	negQ := negativeQueries(t, svc, store, duck.KVNegativeServe)
	got, err := negQ.GetLatestSignals(ctx, subject, latestArgsFor("speed"))
	require.NoError(t, err)
	var speed float64
	for _, s := range got {
		if s.Data.Name == "speed" {
			speed = s.Data.ValueNumber
		}
	}
	assert.Equal(t, 65.0, speed, "a subject with data must still be served its data")
}

// TestLatestKVNegative_UntrustedCoverageFallsBack: when the writer's assertion
// lapses, the reader must return to reading the lake. This is the safety
// property — every failure of the contract has to degrade to today's behavior,
// never to a wrong answer.
func TestLatestKVNegative_UntrustedCoverageFallsBack(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "neg-untrusted")
	subject := fmt.Sprintf("did:erc721:137:%s:33", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1 := day.Add(time.Hour)

	seedRawStatus(t, db, "neg-u1", subject, t1, speedAt(t1, 40))
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithLatestPublisher(app.NewLatestKVPublisher(store, nil, zerolog.Nop()))
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	// A stale assertion: the writer stopped heartbeating long enough ago that no
	// reader should still believe it.
	require.NoError(t, store.PutCoverage(ctx, latestkv.Coverage{
		VerifiedThrough: time.Now().Add(-2 * latestkv.CoverageMaxStaleness),
	}))
	watcher, err := store.WatchCoverage(ctx)
	require.NoError(t, err)
	require.False(t, watcher.Trusted(), "a stale assertion must not be trusted")

	negQ := duck.NewLakeQueries(svc).
		WithLatestKV(store, duck.KVReadServe, zerolog.Nop()).
		WithLatestKVNegative(watcher, duck.KVNegativeServe)
	rollupQ := duck.NewLakeQueries(svc)

	// The KV entry for a subject that HAS data is still served from the cache;
	// only the miss path is gated. Both must agree with the rollup.
	missing := fmt.Sprintf("did:erc721:137:%s:998", vehicleNFT.Hex())
	want, err := rollupQ.GetLatestSignals(ctx, missing, latestArgsFor("speed"))
	require.NoError(t, err)
	got, err := negQ.GetLatestSignals(ctx, missing, latestArgsFor("speed"))
	require.NoError(t, err)
	assert.Equal(t, len(want), len(got), "an untrusted miss falls back to the rollup and still answers")
}

// TestLatestKVNegative_ShadowServesRollupResult: shadow mode must change no
// answer at all — it only observes what serving would have gotten right.
func TestLatestKVNegative_ShadowServesRollupResult(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "neg-shadow")
	subject := fmt.Sprintf("did:erc721:137:%s:34", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1 := day.Add(time.Hour)

	seedRawStatus(t, db, "neg-s1", subject, t1, speedAt(t1, 40))
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithLatestPublisher(app.NewLatestKVPublisher(store, nil, zerolog.Nop()))
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	shadowQ := negativeQueries(t, svc, store, duck.KVNegativeShadow)
	rollupQ := duck.NewLakeQueries(svc)
	missing := fmt.Sprintf("did:erc721:137:%s:997", vehicleNFT.Hex())

	want, err := rollupQ.GetLatestSignals(ctx, missing, latestArgsFor("speed"))
	require.NoError(t, err)
	before := counterValue(t, "dq_lake_latest_kv_false_negative_total")
	got, err := shadowQ.GetLatestSignals(ctx, missing, latestArgsFor("speed"))
	require.NoError(t, err)
	assert.Equal(t, len(want), len(got), "shadow must serve the rollup answer unchanged")
	assert.Equal(t, before, counterValue(t, "dq_lake_latest_kv_false_negative_total"),
		"a genuinely empty subject is a TRUE negative, not a false one")

	// Now break the invariant the way a lost publish would: the lake has rows for
	// this subject, the bucket has no key for it. Shadow must catch it — this
	// counter is the whole reason shadow mode exists, so a test that only ever
	// exercised the healthy path would prove nothing about the gate.
	require.NoError(t, store.DeleteSubject(ctx, subject))
	_, err = shadowQ.GetLatestSignals(ctx, subject, latestArgsFor("speed"))
	require.NoError(t, err)
	assert.Equal(t, before+1, counterValue(t, "dq_lake_latest_kv_false_negative_total"),
		"a subject the lake has and the cache lost must be counted as a false negative")
}

// TestLatestKVNegative_TrustGaugeReadableWithoutTraffic is the regression this
// gauge was reshaped for. It used to be written by the read path, so a pod that
// had served no eligible query reported 0 — indistinguishable from "coverage is
// not trusted". On a node whose load is mirrored from a partner oracle that is
// the normal state for most of the day, so the series was uninformative exactly
// when it was most wanted: after a deploy, before traffic arrives.
func TestLatestKVNegative_TrustGaugeReadableWithoutTraffic(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	store := newLatestKVStore(t, "neg-gauge")

	// Live assertion, and NOT ONE query served against it.
	negativeQueries(t, svc, store, duck.KVNegativeShadow)
	assert.Equal(t, 1.0, gaugeValue(t, "dq_lake_latest_kv_coverage_trusted"),
		"trust must be observable with zero traffic")

	// Let the assertion lapse: the scrape must follow it down without a query
	// having to notice first.
	require.NoError(t, store.PutCoverage(ctx, latestkv.Coverage{
		VerifiedThrough: time.Now().Add(-2 * latestkv.CoverageMaxStaleness),
	}))
	require.Eventually(t, func() bool {
		return gaugeValue(t, "dq_lake_latest_kv_coverage_trusted") == 0
	}, 5*time.Second, 20*time.Millisecond, "a stale assertion must show as untrusted at scrape time")
}

// gaugeValue reads a registered gauge's current value by name. Returns -1 when
// the series is absent, which is distinct from a present 0 — the whole point of
// registering the trust gauge lazily.
func gaugeValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			return m.GetGauge().GetValue()
		}
	}
	return -1
}

// counterValue reads a registered counter's current value by name.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		var total float64
		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
		return total
	}
	return 0
}
