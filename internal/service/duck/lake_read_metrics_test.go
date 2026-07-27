package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
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

// TestFetchOpsAreDistinctShapes pins the fetch-path op split. All four callers
// funnel through queryLakeRawCols, but a bounded index search, a LIMIT 1 latest
// lookup, a single by-id point read and a chunked by-id batch are four different
// queries. One shared label would make dq_lake_read_seconds multi-modal and its
// quantiles meaningless — the reason the op is passed in rather than derived
// from the column projection (fetchIndexSearch and fetchLatestIndex share one).
func TestFetchOpsAreDistinctShapes(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)

	ts := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	insertRawEvent(t, svc, mkStoredEvent("ev-1", "dimo.status", lakeRawSubj, ts))
	opts := &grpc.AdvancedSearchOptions{Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}}}

	ops := []string{"fetchIndexSearch", "fetchLatestIndex", "fetchByID", "fetchByIDBatch"}
	before := map[string]uint64{}
	for _, op := range ops {
		before[op] = lakeReadCount(t, op)
	}

	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1)

	_, err = lsvc.GetLatestIndexAdvanced(ctx, opts)
	require.NoError(t, err)

	_, err = lsvc.GetCloudEventFromIndex(ctx, &indexes[0])
	require.NoError(t, err)

	_, err = lsvc.ListCloudEventsFromIndexes(ctx, indexes)
	require.NoError(t, err)

	for _, op := range ops {
		assert.Equal(t, before[op]+1, lakeReadCount(t, op),
			"%s must be timed under its own op — one observation per shape, no sharing", op)
	}
}

// TestEventSummariesPathsTimedSeparately pins the summaries split. The rollup
// read is O(distinct-names); the fallback is a full-history scan. They are
// separate ops because a single request can perform BOTH — an existing but
// empty rollup falls through to the scan — so one blended op would mix two
// shapes and their sum.
func TestEventSummariesPathsTimedSeparately(t *testing.T) {
	ctx := context.Background()
	_, svc, q := newQueriesHarness(t)

	// No events_latest table yet: eventsRollupAvailable is false, so this is the
	// pure pre-rollup path — scan only, rollup untouched.
	beforeScan, beforeRollup := lakeReadCount(t, "eventSummariesScan"), lakeReadCount(t, "eventSummariesRollup")
	_, err := q.GetEventSummaries(ctx, testSubject1)
	require.NoError(t, err)
	assert.Equal(t, beforeScan+1, lakeReadCount(t, "eventSummariesScan"), "the base scan is timed")
	assert.Equal(t, beforeRollup, lakeReadCount(t, "eventSummariesRollup"), "no rollup read happened")

	// An empty events_latest is the ambiguous case: the rollup is consulted and
	// then falls back to the scan, so the request records one of each.
	_, err = svc.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS lake.events_latest (
		subject VARCHAR, subject_bucket INTEGER, name VARCHAR,
		count BIGINT, first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`)
	require.NoError(t, err)
	// No cache reset needed: eventsRollupReady is one-way false→true and is
	// re-probed on every call while false, so the new table is picked up.

	beforeScan, beforeRollup = lakeReadCount(t, "eventSummariesScan"), lakeReadCount(t, "eventSummariesRollup")
	_, err = q.GetEventSummaries(ctx, testSubject1)
	require.NoError(t, err)
	assert.Equal(t, beforeRollup+1, lakeReadCount(t, "eventSummariesRollup"), "the rollup read is timed")
	assert.Equal(t, beforeScan+1, lakeReadCount(t, "eventSummariesScan"),
		"the empty-rollup fallback scan is timed separately, not folded into the rollup op")
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
