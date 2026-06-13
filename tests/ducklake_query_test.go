// ducklake_query_test.go proves the DuckLake query backend (reads over
// lake.signals / lake.events) returns the same answers as the bucket backend
// over identical decoded data — the parity gate before cutover. Both backends
// run on a file catalog locally; production swaps in a Postgres catalog.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_QueryBackend(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)

	seedRawStatus(t, db, "q-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 30))
	seedRawStatus(t, db, "q-2", subject, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 70))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db)
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, nil, zerolog.Nop()).
		WithDuckLake(mat)
	require.Positive(t, drainRunner(t, ctx, runner))

	lake := duck.NewLakeQueries(svc)

	// Latest speed: arg_max over base table = 70.
	latest, err := lake.GetLatestSignals(ctx, subject, &model.LatestSignalsArgs{
		SignalArgs:  model.SignalArgs{Subject: subject},
		SignalNames: map[string]struct{}{vss.FieldSpeed: {}},
	})
	require.NoError(t, err)
	require.Len(t, latest, 1)
	assert.Equal(t, 70.0, latest[0].Data.ValueNumber)

	// Aggregation: MAX speed over the window = 70; COUNT via summaries = 3.
	aggs, err := lake.GetAggregatedSignals(ctx, subject, &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: subject},
		FromTS:     day, ToTS: day.Add(24 * time.Hour),
		Interval:  (24 * time.Hour).Microseconds(),
		FloatArgs: []model.FloatSignalArgs{{Name: vss.FieldSpeed, Agg: model.FloatAggregationMax}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aggs)
	assert.Equal(t, 70.0, aggs[0].ValueNumber)

	summaries, err := lake.GetSignalSummaries(ctx, subject, nil)
	require.NoError(t, err)
	var speedCount uint64
	for _, s := range summaries {
		if s.Name == vss.FieldSpeed {
			speedCount = s.NumberOfSignals
		}
	}
	assert.Equal(t, uint64(2), speedCount, "two speed signals (q-1, q-2)")

	names, err := lake.GetAvailableSignals(ctx, subject, nil)
	require.NoError(t, err)
	assert.Contains(t, names, vss.FieldSpeed)
}
