package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestItem1_SourceFilteredDedupKeepsRequestedSource proves the Item 1 fix: when a
// source filter is present, the source predicate lives INSIDE the dedup subquery,
// so a two-source collision on (subject,name,timestamp) — where the OTHER
// source's cloud_event_id sorts lower and would win the dedup, then be removed by
// an OUTER source filter — no longer drops the requested source's genuine reading.
// The unfiltered path still collapses to exactly one canonical row (lowest id).
func TestItem1_SourceFilteredDedupKeepsRequestedSource(t *testing.T) {
	ctx := context.Background()
	_, svc, q := newQueriesHarness(t)
	subject := testSubject1
	ts := mkts(t, "2026-06-01T00:00:00Z")
	wanted, other := "wanted-src", "other-src"

	// Both sources report the same (subject, speed, ts). The OTHER row is inserted
	// first so its cloud_event_id (ce-sig-0) sorts BELOW the wanted row (ce-sig-1)
	// and wins the (subject,name,timestamp) dedup.
	insertSignalRows(t, svc, []sigFixture{
		{subject: subject, source: other, name: "speed", ts: ts, num: 999},
		{subject: subject, source: wanted, name: "speed", ts: ts, num: 42},
	})
	wantedFilter := &model.SignalFilter{Source: &wanted}

	// GetLatestSignals (source-filtered) must return the wanted source's reading.
	latest, err := q.GetLatestSignals(ctx, subject, &model.LatestSignalsArgs{
		SignalArgs:  model.SignalArgs{Subject: subject, Filter: wantedFilter},
		SignalNames: map[string]struct{}{"speed": {}},
	})
	require.NoError(t, err)
	require.Len(t, latest, 1, "source-filtered latest must not drop the requested source's row")
	assert.Equal(t, 42.0, latest[0].Data.ValueNumber)

	// GetAggregatedSignals (source-filtered).
	agg, err := q.GetAggregatedSignals(ctx, subject, &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: subject, Filter: wantedFilter},
		FromTS:     ts.Add(-time.Hour),
		ToTS:       ts.Add(time.Hour),
		Interval:   int64(24 * time.Hour / time.Microsecond),
		FloatArgs:  []model.FloatSignalArgs{{Name: "speed", Agg: model.FloatAggregationMax}},
	})
	require.NoError(t, err)
	require.Len(t, agg, 1, "source-filtered aggregation must not drop the requested source's row")
	assert.Equal(t, 42.0, agg[0].ValueNumber)

	// GetAvailableSignals (source-filtered) — the wanted source does report speed.
	avail, err := q.GetAvailableSignals(ctx, subject, wantedFilter)
	require.NoError(t, err)
	assert.Equal(t, []string{"speed"}, avail)

	// GetSignalSummaries (source-filtered) — one row for the wanted source.
	sums, err := q.GetSignalSummaries(ctx, subject, wantedFilter)
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.EqualValues(t, 1, sums[0].NumberOfSignals)

	// Unfiltered lake path still collapses to ONE canonical row: lowest
	// cloud_event_id wins, i.e. the OTHER source's 999.
	unAgg, err := q.GetAggregatedSignals(ctx, subject, &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: subject},
		FromTS:     ts.Add(-time.Hour),
		ToTS:       ts.Add(time.Hour),
		Interval:   int64(24 * time.Hour / time.Microsecond),
		FloatArgs:  []model.FloatSignalArgs{{Name: "speed", Agg: model.FloatAggregationMax}},
	})
	require.NoError(t, err)
	require.Len(t, unAgg, 1)
	assert.Equal(t, 999.0, unAgg[0].ValueNumber, "unfiltered dedup keeps the lowest cloud_event_id (other source)")

	unSums, err := q.getSignalSummariesLake(ctx, subject, nil)
	require.NoError(t, err)
	require.Len(t, unSums, 1)
	assert.EqualValues(t, 1, unSums[0].NumberOfSignals, "unfiltered dedup collapses the collision to one canonical row")
}

// TestItem2_AllLatestLocationUsesFixTime proves the Item 2 fix: GetAllLatestSignals
// stamps the location VALUE with the (0,0)-filtered fix time (loc_ts), not the
// unfiltered max(timestamp). A trailing (0,0) reading must not report the last
// real fix at a later instant, and ALL-latest must agree with named GetLatestSignals.
func TestItem2_AllLatestLocationUsesFixTime(t *testing.T) {
	ctx := context.Background()
	_, svc, q := newQueriesHarness(t)
	subject := testSubject1
	t1 := mkts(t, "2026-06-01T00:00:00Z")
	t2 := t1.Add(time.Hour)

	// A real fix at T1, then a (0,0) "no GPS" reading at T2 for the same signal.
	insertSignalRows(t, svc, []sigFixture{
		{subject: subject, source: "src", name: vss.FieldCurrentLocationCoordinates, ts: t1, lat: 12.5, lon: -34.5, hdop: 0.9},
		{subject: subject, source: "src", name: vss.FieldCurrentLocationCoordinates, ts: t2, lat: 0, lon: 0},
	})

	// Named latest already uses the fix time — establish the reference.
	named, err := q.getLatestSignalsLake(ctx, subject, &model.LatestSignalsArgs{
		SignalArgs:          model.SignalArgs{Subject: subject},
		LocationSignalNames: map[string]struct{}{vss.FieldCurrentLocationCoordinates: {}},
	})
	require.NoError(t, err)
	require.Len(t, named, 1)
	require.True(t, named[0].Data.Timestamp.Equal(t1), "named latest stamps the location value with the fix time T1")

	// ALL-latest must carry the T1 fix value stamped at T1, matching named latest —
	// not the T2 (0,0) reading's timestamp.
	all, err := q.getAllLatestSignalsLake(ctx, subject, nil)
	require.NoError(t, err)
	var loc *vss.Signal
	for _, s := range all {
		if s.Data.Name == vss.FieldCurrentLocationCoordinates {
			loc = s
		}
	}
	require.NotNil(t, loc, "ALL-latest must include the location signal")
	assert.InDelta(t, 12.5, loc.Data.ValueLocation.Latitude, 1e-9, "ALL-latest carries the T1 fix value")
	assert.True(t, loc.Data.Timestamp.Equal(t1),
		"ALL-latest location timestamp must be the fix time T1, matching named latest (not the trailing (0,0) T2)")
	assert.True(t, loc.Data.Timestamp.Equal(named[0].Data.Timestamp), "ALL-latest agrees with named latest")
}

// TestItem5_FetchPrefixAnomalyCountedNotEmptied proves the Item 5 fetch-path
// guard: a raw_events row whose data_index_key is non-empty but NOT under
// BlobKeyPrefix (a din BLOB_PREFIX misconfig) is counted on
// dq_blob_prefix_anomaly_total and served as an empty payload, rather than
// silently lost with no signal.
func TestItem5_FetchPrefixAnomalyCountedNotEmptied(t *testing.T) {
	ctx := context.Background()
	l := &LakeEventService{} // resolvePayload short-circuits before touching svc/getter here

	before := testutil.ToFloat64(blobPrefixAnomalyTotal)
	ev := cloudevent.StoredEvent{
		RawEvent:     cloudevent.RawEvent{CloudEventHeader: cloudevent.CloudEventHeader{Subject: "did:erc721:137:0x0:1", ID: "anom-1"}},
		DataIndexKey: "wrong-prefix/payload.json", // set, but not under BlobKeyPrefix
	}
	raw, err := l.resolvePayload(ctx, ev)
	require.NoError(t, err, "a prefix anomaly is logged/counted, not a hard failure")
	assert.Empty(t, raw.Data, "the payload is served empty")
	assert.Equal(t, before+1, testutil.ToFloat64(blobPrefixAnomalyTotal), "the anomaly is counted")
}

// TestItem2_RollupAllLatestLocationUsesLocTS pins the ROLLUP half of Item 2: the
// no-source GetAllLatestSignals path reads lake.signals_latest, whose stored
// timestamp is the unfiltered latest but whose loc_ts is the (0,0)-filtered fix
// time. The location value must be stamped with loc_ts, not the timestamp.
func TestItem2_RollupAllLatestLocationUsesLocTS(t *testing.T) {
	ctx := context.Background()
	_, svc, q := newQueriesHarness(t)
	subject := testSubject1
	t1 := mkts(t, "2026-06-01T00:00:00Z") // real fix time (loc_ts)
	t2 := t1.Add(time.Hour)               // trailing (0,0) reading time == unfiltered latest

	_, err := svc.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS lake.signals_latest (
		subject VARCHAR, subject_bucket INTEGER, name VARCHAR, "timestamp" TIMESTAMP WITH TIME ZONE,
		value_number DOUBLE, value_string VARCHAR,
		loc_lat DOUBLE, loc_lon DOUBLE, loc_hdop DOUBLE, loc_heading DOUBLE,
		loc_ts TIMESTAMP WITH TIME ZONE, count BIGINT,
		first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`)
	require.NoError(t, err)
	_, err = svc.db.ExecContext(ctx, `INSERT INTO lake.signals_latest VALUES
		(?, ?, ?, ?, 0, '', 12.5, -34.5, 0.9, 0, ?, 2, ?, ?)`,
		subject, HashBucket(subject), vss.FieldCurrentLocationCoordinates, t2.UTC(), t1.UTC(), t1.UTC(), t2.UTC())
	require.NoError(t, err)

	all, err := q.GetAllLatestSignals(ctx, subject, nil) // no source filter → rollup path
	require.NoError(t, err)
	var loc *vss.Signal
	for _, s := range all {
		if s.Data.Name == vss.FieldCurrentLocationCoordinates {
			loc = s
		}
	}
	require.NotNil(t, loc)
	assert.InDelta(t, 12.5, loc.Data.ValueLocation.Latitude, 1e-9)
	assert.True(t, loc.Data.Timestamp.Equal(t1), "rollup location value stamped with loc_ts (T1), not the unfiltered timestamp (T2)")
}
