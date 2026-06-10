// compaction_e2e_test.go proves decoded-layer compaction is invisible to
// queries: aggregations over a partition return identical results before
// and after its batch files merge into one.
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

func TestDecodedCompaction_QueryEquivalence(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := newFSStore(t, root)
	subject := fmt.Sprintf("did:erc721:137:%s:88", vehicleNFT.Hex())

	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	for i := range 4 {
		ts := day.Add(time.Duration(i+1) * time.Hour)
		writeRawBundle(t, store, day, i+1,
			deviceStatus(fmt.Sprintf("cmp-%d", i), subject, ts,
				speedAt(ts, float64(30+i*10)),
				speedAt(ts.Add(time.Minute), float64(35+i*10))))
	}

	// BatchMaxFiles=1 → one decoded object per raw bundle, the small-file
	// pattern compaction exists to fix.
	runner := materializer.New(materializer.Config{
		ChainID:         137,
		BatchMaxFiles:   1,
		CompactMinFiles: 2,
	}, store, zerolog.Nop())
	processed, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	for processed != 0 {
		processed, err = runner.RunOnce(ctx)
		require.NoError(t, err)
	}

	date := day.Format("2006-01-02")
	partition := "decoded/v1/signals/date=" + date + "/"
	before, err := store.List(ctx, partition)
	require.NoError(t, err)
	require.Greater(t, len(before), 1, "fixture must produce the small-file pattern")

	svc, err := duck.NewService(duck.Config{S3Enabled: false})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	queries := duck.NewQueries(svc, root)

	aggArgs := &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: subject},
		FromTS:     day,
		ToTS:       day.Add(24 * time.Hour),
		Interval:   time.Hour.Microseconds(),
		FloatArgs: []model.FloatSignalArgs{
			{Name: vss.FieldSpeed, Agg: model.FloatAggregationMax},
			{Name: vss.FieldSpeed, Agg: model.FloatAggregationAvg},
		},
	}
	wantRows, err := queries.GetAggregatedSignals(ctx, subject, aggArgs)
	require.NoError(t, err)
	require.NotEmpty(t, wantRows)

	n, err := runner.CompactOnce(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	after, err := store.List(ctx, partition)
	require.NoError(t, err)
	require.Len(t, after, 1, "partition merged to one file")

	gotRows, err := queries.GetAggregatedSignals(ctx, subject, aggArgs)
	require.NoError(t, err)
	assert.Equal(t, wantRows, gotRows, "aggregations identical across compaction")
}
