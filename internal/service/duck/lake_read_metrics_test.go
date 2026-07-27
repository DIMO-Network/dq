package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lakeReadCount returns how many observations dq_lake_read_seconds holds for op.
// GetMetricWithLabelValues creates the child if absent, so an untouched op reads
// 0 rather than erroring.
func lakeReadCount(t *testing.T, op string) uint64 {
	t.Helper()
	obs, err := lakeReadSeconds.GetMetricWithLabelValues(op)
	require.NoError(t, err)
	m := &dto.Metric{}
	require.NoError(t, obs.(prometheus.Metric).Write(m))
	return m.GetHistogram().GetSampleCount()
}

// TestFetchPayloadTimesOnlyBlobResolution pins where resolvePayload's
// observeLakeRead sits. fetchPayload exists to measure S3 blob resolution — the
// one read op whose cost is off-lake — so the two early returns that do no S3
// work must NOT be counted. Timing from function entry instead would flood the
// histogram with ~0s samples (inline payloads are the common case) and pin p50
// at zero while real downloads hid in the tail.
func TestFetchPayloadTimesOnlyBlobResolution(t *testing.T) {
	ctx := context.Background()
	l := &LakeEventService{} // every case below returns before touching svc/getter

	t.Run("inline payload is not counted", func(t *testing.T) {
		before := lakeReadCount(t, "fetchPayload")
		ev := cloudevent.StoredEvent{
			RawEvent: cloudevent.RawEvent{
				CloudEventHeader: cloudevent.CloudEventHeader{Subject: testSubject1, ID: "inline-1"},
				Data:             []byte(`{"speed":42}`),
			},
		}
		raw, err := l.resolvePayload(ctx, ev)
		require.NoError(t, err)
		assert.NotEmpty(t, raw.Data, "the inline payload is served as-is")
		assert.Equal(t, before, lakeReadCount(t, "fetchPayload"),
			"an inline payload does no blob resolution and must not be timed")
	})

	t.Run("no blob reference is not counted", func(t *testing.T) {
		before := lakeReadCount(t, "fetchPayload")
		ev := cloudevent.StoredEvent{
			RawEvent: cloudevent.RawEvent{CloudEventHeader: cloudevent.CloudEventHeader{Subject: testSubject1, ID: "empty-1"}},
		}
		_, err := l.resolvePayload(ctx, ev)
		require.NoError(t, err)
		assert.Equal(t, before, lakeReadCount(t, "fetchPayload"),
			"a genuinely empty payload does no blob resolution and must not be timed")
	})

	t.Run("blob resolution is counted", func(t *testing.T) {
		before := lakeReadCount(t, "fetchPayload")
		ev := cloudevent.StoredEvent{
			RawEvent:     cloudevent.RawEvent{CloudEventHeader: cloudevent.CloudEventHeader{Subject: testSubject1, ID: "blob-1"}},
			DataIndexKey: eventrepo.BlobKeyPrefix + "payload.bin",
		}
		// getter is nil, so this errors out — but the op was entered, and the
		// failure path is exactly the latency a blob-resolution panel must show.
		_, err := l.resolvePayload(ctx, ev)
		require.Error(t, err)
		assert.Equal(t, before+1, lakeReadCount(t, "fetchPayload"),
			"a payload under BlobKeyPrefix reaches blob resolution and must be timed")
	})
}

// TestAggregationArgGuardsNotTimed pins the same rule on the aggregation ops:
// the arg-validation short circuits return without building or running SQL, so
// they must not land in dq_lake_read_seconds.
func TestAggregationArgGuardsNotTimed(t *testing.T) {
	ctx := context.Background()
	q := &Queries{}

	t.Run("empty agg args", func(t *testing.T) {
		before := lakeReadCount(t, "signalsRange")
		got, err := q.GetAggregatedSignals(ctx, testSubject1, nil)
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Equal(t, before, lakeReadCount(t, "signalsRange"),
			"a no-op arg set never reaches the lake and must not be timed")
	})

	t.Run("no ranges", func(t *testing.T) {
		before := lakeReadCount(t, "signalsRanges")
		got, err := q.GetAggregatedSignalsForRanges(ctx, testSubject1, nil, time.Time{}, time.Time{}, nil, nil)
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Equal(t, before, lakeReadCount(t, "signalsRanges"),
			"an empty range list never reaches the lake and must not be timed")
	})
}
