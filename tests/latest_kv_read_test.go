// latest_kv_read_test.go covers phase 2 of the signals-latest cache: the
// query-side read modes. Serve mode must return exactly what the rollup path
// returns (same decode pipeline feeding both), fall back on a cache miss, and
// bypass the cache for source-filtered queries; shadow mode must leave the
// served result untouched.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/app"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func latestArgsFor(names ...string) *model.LatestSignalsArgs {
	a := &model.LatestSignalsArgs{SignalNames: map[string]struct{}{}, LocationSignalNames: map[string]struct{}{}, IncludeLastSeen: true}
	for _, n := range names {
		a.SignalNames[n] = struct{}{}
	}
	return a
}

func TestLatestKV_ServeMatchesRollupAndFallsBack(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "read-serve")
	subject := fmt.Sprintf("did:erc721:137:%s:21", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1, t2 := day.Add(time.Hour), day.Add(2*time.Hour)

	seedRawStatus(t, db, "kvr-1", subject, t1, speedAt(t1, 40))
	seedRawStatus(t, db, "kvr-2", subject, t2, speedAt(t2, 65))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithLatestPublisher(app.NewLatestKVPublisher(store, zerolog.Nop()))
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	rollupQ := duck.NewLakeQueries(svc)
	serveQ := duck.NewLakeQueries(svc).WithLatestKV(store, duck.KVReadServe, zerolog.Nop())

	// Serve mode returns exactly what the rollup path returns.
	want, err := rollupQ.GetLatestSignals(ctx, subject, latestArgsFor("speed"))
	require.NoError(t, err)
	got, err := serveQ.GetLatestSignals(ctx, subject, latestArgsFor("speed"))
	require.NoError(t, err)
	require.Len(t, got, len(want), "same row count as the rollup path")
	for i := range want {
		assert.Equal(t, want[i].Data.Name, got[i].Data.Name)
		assert.Equal(t, want[i].Data.Timestamp, got[i].Data.Timestamp, want[i].Data.Name)
		assert.Equal(t, want[i].Data.ValueNumber, got[i].Data.ValueNumber, want[i].Data.Name)
		assert.Equal(t, want[i].Data.ValueLocation, got[i].Data.ValueLocation, want[i].Data.Name)
	}

	// A subject the cache has never seen falls back to the rollup path and
	// still answers (the virtual lastSeen row at epoch, exactly like rollup).
	missing := fmt.Sprintf("did:erc721:137:%s:404", vehicleNFT.Hex())
	fromKV, err := serveQ.GetLatestSignals(ctx, missing, latestArgsFor("speed"))
	require.NoError(t, err)
	fromRollup, err := rollupQ.GetLatestSignals(ctx, missing, latestArgsFor("speed"))
	require.NoError(t, err)
	assert.Equal(t, len(fromRollup), len(fromKV), "cache miss must serve the rollup answer")

	// Source-filtered queries bypass the cache (it folds sources), landing on
	// the deduped scan exactly as before.
	src := "0xConnLicense"
	filtered := latestArgsFor("speed")
	filtered.Filter = &model.SignalFilter{Source: &src}
	scan, err := serveQ.GetLatestSignals(ctx, subject, filtered)
	require.NoError(t, err)
	var speed float64
	for _, s := range scan {
		if s.Data.Name == "speed" {
			speed = s.Data.ValueNumber
		}
	}
	assert.Equal(t, 65.0, speed, "source filter served from the lake scan")
}

func TestLatestKV_ShadowServesRollupResult(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "read-shadow")
	subject := fmt.Sprintf("did:erc721:137:%s:22", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1 := day.Add(time.Hour)

	seedRawStatus(t, db, "kvs-1", subject, t1, speedAt(t1, 40))
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithLatestPublisher(app.NewLatestKVPublisher(store, zerolog.Nop()))
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	shadowQ := duck.NewLakeQueries(svc).WithLatestKV(store, duck.KVReadShadow, zerolog.Nop())
	got, err := shadowQ.GetLatestSignals(ctx, subject, latestArgsFor("speed"))
	require.NoError(t, err)
	var found *vss.Signal
	for _, s := range got {
		if s.Data.Name == "speed" {
			found = s
		}
	}
	require.NotNil(t, found, "shadow mode serves the rollup result")
	assert.Equal(t, 40.0, found.Data.ValueNumber)
}
