package duck

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLakeAggregations_EngineParity locks DuckDB aggregate semantics on the
// lake (target) backend against golden vectors (CHD-33). These are the
// agreed expected values;
// the aggregate behavior is documented in aggregations.go
// (exact median, mode for TOP, string_agg DISTINCT for UNIQUE, and the
// coalesce(…,0) empty-aggregate default). The bucket
// path exercises the same expressions in aggregations_test.go.
func TestLakeAggregations_EngineParity(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	q := NewLakeQueries(svc)
	subj := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:1"
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	for i, v := range []float64{10, 20, 30} {
		insertSignal(t, svc, subj, "speed", fmt.Sprintf("m-%d", i), base.Add(time.Duration(i)*time.Second), v)
	}

	from, to := base.Add(-time.Minute), base.Add(time.Minute)
	oneBucket := int64(24 * time.Hour / time.Microsecond)

	med, err := q.GetAggregatedSignals(ctx, subj, &model.AggregatedSignalArgs{
		FromTS: from, ToTS: to, Interval: oneBucket,
		FloatArgs: []model.FloatSignalArgs{{Name: "speed", Agg: model.FloatAggregationMed}},
	})
	require.NoError(t, err)
	require.Len(t, med, 1)
	assert.Equal(t, 20.0, med[0].ValueNumber, "exact median([10,20,30]) == 20")

	// A requested signal with no rows in the window yields no row (not a spurious
	// coalesced 0), matching CH's GROUP BY: aggregating an absent name is empty.
	empty, err := q.GetAggregatedSignals(ctx, subj, &model.AggregatedSignalArgs{
		FromTS: from, ToTS: to, Interval: oneBucket,
		FloatArgs: []model.FloatSignalArgs{{Name: "nonexistent", Agg: model.FloatAggregationMax}},
	})
	require.NoError(t, err)
	assert.Empty(t, empty, "absent signal aggregates to no rows")
}
