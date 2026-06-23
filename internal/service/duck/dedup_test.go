package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLakeQueries_DedupOverCount proves that at-rest duplicate
// (subject,name,timestamp) rows — which at-least-once ingest (device retry,
// sink redelivery, cross-batch) can store with different cloud_event_id — do
// not over-count aggregations or summaries. The segments path already dedups;
// the aggregation/latest/summary paths did not (CHD-2 / R1-C1). One canonical
// row per key (lowest cloud_event_id) is kept by collapsing duplicates.
func TestLakeQueries_DedupOverCount(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	q := NewLakeQueries(svc)
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:1"
	ts1 := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	ts2 := ts1.Add(time.Minute)

	// The same (subject,name,timestamp) reading was stored twice with a
	// different cloud_event_id; ts2 is a distinct reading.
	insertSignal(t, svc, subject, "speed", "ce-a", ts1, 60)
	insertSignal(t, svc, subject, "speed", "ce-b", ts1, 60)
	insertSignal(t, svc, subject, "speed", "ce-c", ts2, 80)

	// Summary count (lake_latest path) collapses the duplicate.
	sums, err := q.getSignalSummariesLake(ctx, subject, nil)
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.EqualValues(t, 2, sums[0].NumberOfSignals, "duplicate reading collapses to one count")

	// avg (aggregations → signalTable path) is over (60, 80), not (60, 60, 80).
	from := ts1.Add(-time.Minute)
	to := ts2.Add(time.Minute)
	agg, err := q.GetAggregatedSignals(ctx, subject, &model.AggregatedSignalArgs{
		FromTS:    from,
		ToTS:      to,
		Interval:  int64(24 * time.Hour / time.Microsecond), // one bucket spanning the window
		FloatArgs: []model.FloatSignalArgs{{Name: "speed", Agg: model.FloatAggregationAvg}},
	})
	require.NoError(t, err)
	require.Len(t, agg, 1)
	assert.Equal(t, 70.0, agg[0].ValueNumber, "avg deduped to (60+80)/2, not (60+60+80)/3")
}
