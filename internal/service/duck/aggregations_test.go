package duck

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/ch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	srcOne = "src-1"
	srcTwo = "src-2"

	sigSpeed = "speed"
	sigPower = "powertrainType"
	sigLoc   = "currentLocationCoordinates"
)

func d1(t *testing.T, hms string) time.Time { return mkts(t, "2026-06-01T"+hms+"Z") }
func d2(t *testing.T, hms string) time.Time { return mkts(t, "2026-06-02T"+hms+"Z") }

// setupAggFixtures writes 2 days of decoded signals for 2 vehicles.
// Day 2026-06-03 deliberately has no partition so day-glob pruning is
// exercised by the multi-day tests.
func setupAggFixtures(t *testing.T) *Queries {
	t.Helper()
	root, svc, q := newQueriesHarness(t)

	writeSignalsFixture(t, svc, root, "2026-06-01", []sigFixture{
		{subject: testSubject1, source: srcOne, name: sigSpeed, ts: d1(t, "00:00:10"), num: 10},
		{subject: testSubject1, source: srcOne, name: sigSpeed, ts: d1(t, "00:00:50"), num: 30},
		{subject: testSubject1, source: srcOne, name: sigSpeed, ts: d1(t, "00:01:10"), num: 50},
		{subject: testSubject1, source: srcOne, name: sigSpeed, ts: d1(t, "00:02:30"), num: 20},
		{subject: testSubject1, source: srcTwo, name: sigSpeed, ts: d1(t, "00:00:20"), num: 80},

		{subject: testSubject1, source: srcOne, name: sigPower, ts: d1(t, "00:00:05"), str: "HEV"},
		{subject: testSubject1, source: srcOne, name: sigPower, ts: d1(t, "00:00:45"), str: "EV"},
		{subject: testSubject1, source: srcOne, name: sigPower, ts: d1(t, "00:00:55"), str: "EV"},
		{subject: testSubject1, source: srcOne, name: sigPower, ts: d1(t, "00:01:05"), str: "EV"},

		{subject: testSubject1, source: srcOne, name: sigLoc, ts: d1(t, "00:00:20"), lat: 1, lon: 2, hdop: 0.5, heading: 90},
		{subject: testSubject1, source: srcOne, name: sigLoc, ts: d1(t, "00:00:40"), lat: 3, lon: 4, hdop: 0.7, heading: 180},
		{subject: testSubject1, source: srcOne, name: sigLoc, ts: d1(t, "00:01:20")}, // (0, 0): excluded from interval aggs
		{subject: testSubject1, source: srcOne, name: sigLoc, ts: d1(t, "00:01:40"), lat: 5, lon: 6, hdop: 0.8, heading: 270},

		{subject: testSubject2, source: srcOne, name: sigSpeed, ts: d1(t, "00:00:30"), num: 999},
	})
	writeSignalsFixture(t, svc, root, "2026-06-02", []sigFixture{
		{subject: testSubject1, source: srcOne, name: sigSpeed, ts: d2(t, "12:00:00"), num: 100},
	})
	return q
}

type aggKey struct {
	ts    time.Time
	stype ch.FieldType
	index uint16
}

func aggsByKey(signals []*ch.AggSignal) map[aggKey]*ch.AggSignal {
	m := make(map[aggKey]*ch.AggSignal, len(signals))
	for _, s := range signals {
		m[aggKey{ts: s.Timestamp, stype: s.SignalType, index: s.SignalIndex}] = s
	}
	return m
}

func floatArgsAggArgs(t *testing.T, from, to time.Time, interval time.Duration, source *string, floatArgs []model.FloatSignalArgs) *model.AggregatedSignalArgs {
	t.Helper()
	args := &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: testSubject1},
		FromTS:     from,
		ToTS:       to,
		Interval:   interval.Microseconds(),
		FloatArgs:  floatArgs,
	}
	if source != nil {
		args.Filter = &model.SignalFilter{Source: source}
	}
	return args
}

func TestGetAggregatedSignalsFloatAggregations(t *testing.T) {
	q := setupAggFixtures(t)
	src := srcOne
	aggArgs := floatArgsAggArgs(t, d1(t, "00:00:00"), d1(t, "00:05:00"), time.Minute, &src, []model.FloatSignalArgs{
		{Name: sigSpeed, Agg: model.FloatAggregationAvg},
		{Name: sigSpeed, Agg: model.FloatAggregationMin},
		{Name: sigSpeed, Agg: model.FloatAggregationMax},
		{Name: sigSpeed, Agg: model.FloatAggregationFirst},
		{Name: sigSpeed, Agg: model.FloatAggregationLast},
		{Name: sigSpeed, Agg: model.FloatAggregationMed},
	})

	signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
	require.NoError(t, err)
	require.Len(t, signals, 18, "3 buckets with data x 6 aggregations; empty buckets must not produce rows")

	m := aggsByKey(signals)
	minute := func(n int) time.Time { return d1(t, "00:00:00").Add(time.Duration(n) * time.Minute) }
	// minute 0: values 10, 30
	want0 := []float64{20, 10, 30, 10, 30, 20} // avg, min, max, first, last, med
	for i, want := range want0 {
		row := m[aggKey{ts: minute(0), stype: ch.FloatType, index: uint16(i)}]
		require.NotNil(t, row, "missing minute-0 row for agg index %d", i)
		assert.InDelta(t, want, row.ValueNumber, 1e-9, "agg index %d", i)
	}
	// minute 1: single value 50; minute 2: single value 20
	for i := range want0 {
		assert.InDelta(t, 50, m[aggKey{ts: minute(1), stype: ch.FloatType, index: uint16(i)}].ValueNumber, 1e-9)
		assert.InDelta(t, 20, m[aggKey{ts: minute(2), stype: ch.FloatType, index: uint16(i)}].ValueNumber, 1e-9)
	}

	// rows are ordered by bucket timestamp ascending
	for i := 1; i < len(signals); i++ {
		assert.False(t, signals[i].Timestamp.Before(signals[i-1].Timestamp))
	}
}

func TestGetAggregatedSignalsNoSourceFilterIncludesAllSources(t *testing.T) {
	q := setupAggFixtures(t)
	aggArgs := floatArgsAggArgs(t, d1(t, "00:00:00"), d1(t, "00:01:00"), time.Minute, nil, []model.FloatSignalArgs{
		{Name: sigSpeed, Agg: model.FloatAggregationMax},
	})
	signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
	require.NoError(t, err)
	require.Len(t, signals, 1)
	assert.InDelta(t, 80, signals[0].ValueNumber, 1e-9, "src-2 value must be included without a source filter")
}

func TestGetAggregatedSignalsNonMidnightOrigin(t *testing.T) {
	q := setupAggFixtures(t)
	src := srcOne
	from := d1(t, "00:00:30")
	aggArgs := floatArgsAggArgs(t, from, d1(t, "00:03:30"), time.Minute, &src, []model.FloatSignalArgs{
		{Name: sigSpeed, Agg: model.FloatAggregationAvg},
	})
	signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
	require.NoError(t, err)
	require.Len(t, signals, 2)

	// Buckets are aligned to the :30 origin, not to the minute.
	assert.Equal(t, d1(t, "00:00:30"), signals[0].Timestamp, "bucket must start at the origin")
	assert.InDelta(t, 40, signals[0].ValueNumber, 1e-9, "values at 00:00:50 and 00:01:10 share the [00:00:30, 00:01:30) bucket")
	assert.Equal(t, d1(t, "00:02:30"), signals[1].Timestamp)
	assert.InDelta(t, 20, signals[1].ValueNumber, 1e-9)
}

func TestGetAggregatedSignalsStringAggregations(t *testing.T) {
	q := setupAggFixtures(t)
	src := srcOne
	aggArgs := &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: testSubject1, Filter: &model.SignalFilter{Source: &src}},
		FromTS:     d1(t, "00:00:00"),
		ToTS:       d1(t, "00:02:00"),
		Interval:   time.Minute.Microseconds(),
		StringArgs: []model.StringSignalArgs{
			{Name: sigPower, Agg: model.StringAggregationFirst},
			{Name: sigPower, Agg: model.StringAggregationLast},
			{Name: sigPower, Agg: model.StringAggregationUnique},
			{Name: sigPower, Agg: model.StringAggregationTop},
		},
	}
	signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
	require.NoError(t, err)
	require.Len(t, signals, 8, "2 buckets x 4 aggregations")

	m := aggsByKey(signals)
	minute0 := d1(t, "00:00:00")
	assert.Equal(t, "HEV", m[aggKey{ts: minute0, stype: ch.StringType, index: 0}].ValueString)
	assert.Equal(t, "EV", m[aggKey{ts: minute0, stype: ch.StringType, index: 1}].ValueString)
	uniq := strings.Split(m[aggKey{ts: minute0, stype: ch.StringType, index: 2}].ValueString, ",")
	sort.Strings(uniq)
	assert.Equal(t, []string{"EV", "HEV"}, uniq)
	assert.Equal(t, "EV", m[aggKey{ts: minute0, stype: ch.StringType, index: 3}].ValueString, "EV appears 2 of 3 times in minute 0")

	minute1 := d1(t, "00:01:00")
	for i := range 4 {
		assert.Equal(t, "EV", m[aggKey{ts: minute1, stype: ch.StringType, index: uint16(i)}].ValueString)
	}
}

func TestGetAggregatedSignalsLocationAggregations(t *testing.T) {
	q := setupAggFixtures(t)
	src := srcOne
	aggArgs := &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: testSubject1, Filter: &model.SignalFilter{Source: &src}},
		FromTS:     d1(t, "00:00:00"),
		ToTS:       d1(t, "00:02:00"),
		Interval:   time.Minute.Microseconds(),
		LocationArgs: []model.LocationSignalArgs{
			{Name: sigLoc, Agg: model.LocationAggregationFirst},
			{Name: sigLoc, Agg: model.LocationAggregationLast},
			{Name: sigLoc, Agg: model.LocationAggregationAvg},
		},
	}
	signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
	require.NoError(t, err)
	require.Len(t, signals, 6, "2 buckets x 3 aggregations")

	m := aggsByKey(signals)
	minute0, minute1 := d1(t, "00:00:00"), d1(t, "00:01:00")

	first := m[aggKey{ts: minute0, stype: ch.LocType, index: 0}].ValueLocation
	assert.InDelta(t, 1, first.Latitude, 1e-9)
	assert.InDelta(t, 2, first.Longitude, 1e-9)
	assert.InDelta(t, 0.5, first.HDOP, 1e-9)
	assert.InDelta(t, 90, first.Heading, 1e-9)

	last := m[aggKey{ts: minute0, stype: ch.LocType, index: 1}].ValueLocation
	assert.InDelta(t, 3, last.Latitude, 1e-9)
	assert.InDelta(t, 4, last.Longitude, 1e-9)

	avg := m[aggKey{ts: minute0, stype: ch.LocType, index: 2}].ValueLocation
	assert.InDelta(t, 2, avg.Latitude, 1e-9)
	assert.InDelta(t, 3, avg.Longitude, 1e-9)
	assert.InDelta(t, 0.6, avg.HDOP, 1e-9)
	assert.InDelta(t, 135, avg.Heading, 1e-9)

	// minute 1 contains a (0, 0) row that interval aggregations exclude.
	for i := range 3 {
		loc := m[aggKey{ts: minute1, stype: ch.LocType, index: uint16(i)}].ValueLocation
		assert.InDelta(t, 5, loc.Latitude, 1e-9, "(0,0) row must be excluded, agg %d", i)
		assert.InDelta(t, 6, loc.Longitude, 1e-9, "(0,0) row must be excluded, agg %d", i)
	}
}

func TestGetAggregatedSignalsFloatValueFilter(t *testing.T) {
	q := setupAggFixtures(t)
	src := srcOne
	gt := 15.0

	aggArgs := floatArgsAggArgs(t, d1(t, "00:00:00"), d1(t, "00:03:00"), time.Minute, &src, []model.FloatSignalArgs{
		{Name: sigSpeed, Agg: model.FloatAggregationAvg, Filter: &model.SignalFloatFilter{Gt: &gt}},
	})
	signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
	require.NoError(t, err)
	require.Len(t, signals, 3)
	assert.InDelta(t, 30, signals[0].ValueNumber, 1e-9, "value 10 must be filtered out of minute 0")

	t.Run("or filter", func(t *testing.T) {
		lt, gt := 15.0, 45.0
		aggArgs := floatArgsAggArgs(t, d1(t, "00:00:00"), d1(t, "00:03:00"), time.Minute, &src, []model.FloatSignalArgs{
			{Name: sigSpeed, Agg: model.FloatAggregationAvg, Filter: &model.SignalFloatFilter{
				Or: []*model.SignalFloatFilter{{Lt: &lt}, {Gt: &gt}},
			}},
		})
		signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
		require.NoError(t, err)
		require.Len(t, signals, 2, "minute 2 (value 20) matches neither OR branch")
		assert.InDelta(t, 10, signals[0].ValueNumber, 1e-9)
		assert.InDelta(t, 50, signals[1].ValueNumber, 1e-9)
	})
}

func TestGetAggregatedSignalsRand(t *testing.T) {
	q := setupAggFixtures(t)
	src := srcOne
	aggArgs := floatArgsAggArgs(t, d1(t, "00:00:00"), d1(t, "00:01:00"), time.Minute, &src, []model.FloatSignalArgs{
		{Name: sigSpeed, Agg: model.FloatAggregationRand},
	})
	signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
	require.NoError(t, err)
	require.Len(t, signals, 1)
	assert.Contains(t, []float64{10, 30}, signals[0].ValueNumber, "RAND must pick one of the bucket's values")
}

func TestGetAggregatedSignalsMultiDayWithMissingPartition(t *testing.T) {
	q := setupAggFixtures(t)
	src := srcOne
	// 2026-06-03 has no parquet partition; the day must be pruned, not error.
	aggArgs := floatArgsAggArgs(t, d1(t, "00:00:00"), mkts(t, "2026-06-04T00:00:00Z"), 24*time.Hour, &src, []model.FloatSignalArgs{
		{Name: sigSpeed, Agg: model.FloatAggregationAvg},
	})
	signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
	require.NoError(t, err)
	require.Len(t, signals, 2)
	assert.Equal(t, d1(t, "00:00:00"), signals[0].Timestamp)
	assert.InDelta(t, 27.5, signals[0].ValueNumber, 1e-9, "avg of 10, 30, 50, 20")
	assert.Equal(t, d2(t, "00:00:00"), signals[1].Timestamp)
	assert.InDelta(t, 100, signals[1].ValueNumber, 1e-9)
}

func TestGetAggregatedSignalsEdgeCases(t *testing.T) {
	q := setupAggFixtures(t)

	t.Run("unknown subject yields empty", func(t *testing.T) {
		aggArgs := floatArgsAggArgs(t, d1(t, "00:00:00"), d1(t, "01:00:00"), time.Minute, nil, []model.FloatSignalArgs{
			{Name: sigSpeed, Agg: model.FloatAggregationAvg},
		})
		signals, err := q.GetAggregatedSignals(context.Background(), "did:erc721:1:0x0:404", aggArgs)
		require.NoError(t, err)
		assert.Empty(t, signals)
	})

	t.Run("no aggregations yields empty", func(t *testing.T) {
		signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, &model.AggregatedSignalArgs{
			FromTS: d1(t, "00:00:00"), ToTS: d1(t, "01:00:00"), Interval: time.Minute.Microseconds(),
		})
		require.NoError(t, err)
		assert.Empty(t, signals)
	})

	// minute 0 holds two location points: (1,2)@:20 and (3,4)@:40. A spatial
	// filter that encloses exactly one of them must select that one (SR review
	// #10 — replaces the old "location filters are rejected").
	t.Run("inCircle filter selects only points within the radius", func(t *testing.T) {
		aggArgs := &model.AggregatedSignalArgs{
			SignalArgs: model.SignalArgs{Subject: testSubject1},
			FromTS:     d1(t, "00:00:00"),
			ToTS:       d1(t, "00:01:00"),
			Interval:   time.Minute.Microseconds(),
			LocationArgs: []model.LocationSignalArgs{{
				Name: sigLoc,
				Agg:  model.LocationAggregationFirst,
				Filter: &model.SignalLocationFilter{
					// ~5 km around (3,4); (1,2) is ~314 km away and excluded.
					InCircle: &model.InCircleFilter{Center: &model.FilterLocation{Latitude: 3, Longitude: 4}, Radius: 5},
				},
			}},
		}
		signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
		require.NoError(t, err)
		require.Len(t, signals, 1)
		assert.InDelta(t, 3, signals[0].ValueLocation.Latitude, 1e-9)
		assert.InDelta(t, 4, signals[0].ValueLocation.Longitude, 1e-9)
	})

	t.Run("inPolygon filter selects only points inside the polygon", func(t *testing.T) {
		aggArgs := &model.AggregatedSignalArgs{
			SignalArgs: model.SignalArgs{Subject: testSubject1},
			FromTS:     d1(t, "00:00:00"),
			ToTS:       d1(t, "00:01:00"),
			Interval:   time.Minute.Microseconds(),
			LocationArgs: []model.LocationSignalArgs{{
				Name: sigLoc,
				Agg:  model.LocationAggregationFirst,
				Filter: &model.SignalLocationFilter{
					// A box around (1,2) only; (3,4) is outside.
					InPolygon: []*model.FilterLocation{
						{Latitude: 0.5, Longitude: 1.5},
						{Latitude: 0.5, Longitude: 2.5},
						{Latitude: 1.5, Longitude: 2.5},
						{Latitude: 1.5, Longitude: 1.5},
					},
				},
			}},
		}
		signals, err := q.GetAggregatedSignals(context.Background(), testSubject1, aggArgs)
		require.NoError(t, err)
		require.Len(t, signals, 1)
		assert.InDelta(t, 1, signals[0].ValueLocation.Latitude, 1e-9)
		assert.InDelta(t, 2, signals[0].ValueLocation.Longitude, 1e-9)
	})
}

func TestGetAggregatedSignalsForRanges(t *testing.T) {
	q := setupAggFixtures(t)
	ranges := []ch.TimeRange{
		{From: d1(t, "00:00:00"), To: d1(t, "00:01:00")},
		{From: d1(t, "00:01:00"), To: d1(t, "00:02:00")},
	}
	floatArgs := []model.FloatSignalArgs{
		{Name: sigSpeed, Agg: model.FloatAggregationAvg},
		{Name: sigSpeed, Agg: model.FloatAggregationMax},
	}
	locationArgs := []model.LocationSignalArgs{
		{Name: sigLoc, Agg: model.LocationAggregationFirst},
	}

	rows, err := q.GetAggregatedSignalsForRanges(context.Background(), testSubject1,
		ranges, d1(t, "00:00:00"), d1(t, "00:02:00"), floatArgs, locationArgs)
	require.NoError(t, err)
	require.Len(t, rows, 6, "2 segments x (2 float + 1 location)")

	byKey := map[[3]int]*ch.AggSignalForRange{}
	for _, r := range rows {
		byKey[[3]int{r.SegIndex, int(r.SignalType), int(r.SignalIndex)}] = r
	}

	// seg 0: speed values 10, 30 and 80 (no source filter in batch agg)
	assert.InDelta(t, 40, byKey[[3]int{0, int(ch.FloatType), 0}].ValueNumber, 1e-9)
	assert.InDelta(t, 80, byKey[[3]int{0, int(ch.FloatType), 1}].ValueNumber, 1e-9)
	// seg 1: speed value 50
	assert.InDelta(t, 50, byKey[[3]int{1, int(ch.FloatType), 0}].ValueNumber, 1e-9)
	assert.InDelta(t, 50, byKey[[3]int{1, int(ch.FloatType), 1}].ValueNumber, 1e-9)

	// seg 0 first location is (1, 2)
	loc0 := byKey[[3]int{0, int(ch.LocType), 0}].ValueLocation
	assert.InDelta(t, 1, loc0.Latitude, 1e-9)
	assert.InDelta(t, 2, loc0.Longitude, 1e-9)
	// seg 1: batch agg does NOT exclude (0, 0) rows, mirroring ClickHouse.
	loc1 := byKey[[3]int{1, int(ch.LocType), 0}].ValueLocation
	assert.InDelta(t, 0, loc1.Latitude, 1e-9)
	assert.InDelta(t, 0, loc1.Longitude, 1e-9)

	t.Run("empty ranges", func(t *testing.T) {
		rows, err := q.GetAggregatedSignalsForRanges(context.Background(), testSubject1, nil, d1(t, "00:00:00"), d1(t, "00:02:00"), floatArgs, nil)
		require.NoError(t, err)
		assert.Nil(t, rows)
	})
	t.Run("no aggregations", func(t *testing.T) {
		rows, err := q.GetAggregatedSignalsForRanges(context.Background(), testSubject1, ranges, d1(t, "00:00:00"), d1(t, "00:02:00"), nil, nil)
		require.NoError(t, err)
		assert.Empty(t, rows)
	})
}
