package duck

import (
	"context"
	"testing"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/qtypes"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	evTripStart  = "trip.start"
	evTripEnd    = "trip.end"
	evHarshBrake = "harsh.brake"
	evNoTags     = "no.tags"

	srcA = "src-a"
	srcB = "src-b"
)

func setupEventFixtures(t *testing.T) *Queries {
	t.Helper()
	root, svc, q := newQueriesHarness(t)

	writeEventsFixture(t, svc, root, "2026-06-01", []eventFixture{
		{subject: testSubject1, source: srcA, name: evTripStart, ts: d1(t, "10:00:00"), metadata: `{"a":1}`, tags: []string{"trip"}},
		{subject: testSubject1, source: srcB, name: evHarshBrake, ts: d1(t, "11:00:00"), durNs: 5000000000, tags: []string{"driving", "safety"}},
		{subject: testSubject1, source: srcA, name: evTripEnd, ts: d1(t, "12:00:00"), tags: []string{"trip"}},
		{subject: testSubject2, source: srcA, name: evTripStart, ts: d1(t, "10:30:00"), tags: []string{"trip"}},
	})
	writeEventsFixture(t, svc, root, "2026-06-02", []eventFixture{
		{subject: testSubject1, source: srcB, name: evHarshBrake, ts: d2(t, "09:00:00"), tags: []string{"safety"}},
		{subject: testSubject1, source: srcA, name: evNoTags, ts: d2(t, "10:00:00")},
	})
	return q
}

func eventNames(events []*vss.Event) []string {
	names := make([]string, len(events))
	for i, e := range events {
		names[i] = e.Data.Name
	}
	return names
}

func TestGetEvents(t *testing.T) {
	q := setupEventFixtures(t)
	from, to := d1(t, "00:00:00"), mkts(t, "2026-06-03T00:00:00Z")

	events, err := q.GetEvents(context.Background(), testSubject1, from, to, nil)
	require.NoError(t, err)
	require.Equal(t, []string{evNoTags, evHarshBrake, evTripEnd, evHarshBrake, evTripStart}, eventNames(events),
		"events must be newest-first and exclude other subjects")

	brake := events[3]
	assert.Equal(t, srcB, brake.Source)
	assert.Equal(t, d1(t, "11:00:00"), brake.Data.Timestamp)
	assert.EqualValues(t, 5000000000, brake.Data.DurationNs)
	assert.Equal(t, []string{"driving", "safety"}, brake.Tags)

	start := events[4]
	assert.Equal(t, `{"a":1}`, start.Data.Metadata)
	assert.Equal(t, []string{"trip"}, start.Tags)

	assert.Empty(t, events[0].Tags, "empty tag lists must round-trip as empty")

	t.Run("time range bounds", func(t *testing.T) {
		events, err := q.GetEvents(context.Background(), testSubject1, d1(t, "11:00:00"), d2(t, "09:00:00"), nil)
		require.NoError(t, err)
		assert.Equal(t, []string{evTripEnd, evHarshBrake}, eventNames(events), "from inclusive, to exclusive")
	})

	t.Run("missing day partitions", func(t *testing.T) {
		events, err := q.GetEvents(context.Background(), testSubject1, mkts(t, "2026-06-03T00:00:00Z"), mkts(t, "2026-06-05T00:00:00Z"), nil)
		require.NoError(t, err)
		assert.Empty(t, events)
	})
}

func TestGetEventsTagFilters(t *testing.T) {
	q := setupEventFixtures(t)
	from, to := d1(t, "00:00:00"), mkts(t, "2026-06-03T00:00:00Z")
	get := func(t *testing.T, filter *model.EventFilter) []string {
		t.Helper()
		events, err := q.GetEvents(context.Background(), testSubject1, from, to, filter)
		require.NoError(t, err)
		return eventNames(events)
	}

	t.Run("containsAny", func(t *testing.T) {
		names := get(t, &model.EventFilter{Tags: &model.StringArrayFilter{ContainsAny: []string{"safety"}}})
		assert.Equal(t, []string{evHarshBrake, evHarshBrake}, names)
	})
	t.Run("containsAll", func(t *testing.T) {
		names := get(t, &model.EventFilter{Tags: &model.StringArrayFilter{ContainsAll: []string{"driving", "safety"}}})
		assert.Equal(t, []string{evHarshBrake}, names)
	})
	t.Run("notContainsAny", func(t *testing.T) {
		names := get(t, &model.EventFilter{Tags: &model.StringArrayFilter{NotContainsAny: []string{"trip"}}})
		assert.Equal(t, []string{evNoTags, evHarshBrake, evHarshBrake}, names)
	})
	t.Run("or", func(t *testing.T) {
		names := get(t, &model.EventFilter{Tags: &model.StringArrayFilter{
			ContainsAny: []string{"driving"},
			Or:          []*model.StringArrayFilter{{ContainsAny: []string{"trip"}}},
		}})
		assert.Equal(t, []string{evTripEnd, evHarshBrake, evTripStart}, names)
	})
}

func TestGetEventsNameAndSourceFilters(t *testing.T) {
	q := setupEventFixtures(t)
	from, to := d1(t, "00:00:00"), mkts(t, "2026-06-03T00:00:00Z")
	get := func(t *testing.T, filter *model.EventFilter) []string {
		t.Helper()
		events, err := q.GetEvents(context.Background(), testSubject1, from, to, filter)
		require.NoError(t, err)
		return eventNames(events)
	}

	brake := evHarshBrake
	assert.Equal(t, []string{evHarshBrake, evHarshBrake}, get(t, &model.EventFilter{Name: &model.StringValueFilter{Eq: &brake}}))

	prefix := "trip"
	assert.Equal(t, []string{evTripEnd, evTripStart}, get(t, &model.EventFilter{Name: &model.StringValueFilter{StartsWith: &prefix}}))

	src := srcA
	assert.Equal(t, []string{evNoTags, evTripEnd, evTripStart}, get(t, &model.EventFilter{Source: &model.StringValueFilter{Eq: &src}}))

	start, end := evTripStart, evTripEnd
	assert.Equal(t, []string{evTripEnd, evTripStart}, get(t, &model.EventFilter{Name: &model.StringValueFilter{
		Eq: &start,
		Or: []*model.StringValueFilter{{Eq: &end}},
	}}))
}

func TestGetEventCounts(t *testing.T) {
	q := setupEventFixtures(t)
	from, to := d1(t, "00:00:00"), mkts(t, "2026-06-03T00:00:00Z")

	counts, err := q.GetEventCounts(context.Background(), testSubject1, from, to, nil)
	require.NoError(t, err)
	require.Len(t, counts, 4)
	want := map[string]int{evHarshBrake: 2, evNoTags: 1, evTripEnd: 1, evTripStart: 1}
	for _, c := range counts {
		assert.Equal(t, want[c.Name], c.Count, c.Name)
	}

	t.Run("name subset", func(t *testing.T) {
		counts, err := q.GetEventCounts(context.Background(), testSubject1, from, to, []string{evHarshBrake})
		require.NoError(t, err)
		require.Len(t, counts, 1)
		assert.Equal(t, &qtypes.EventCount{Name: evHarshBrake, Count: 2}, counts[0])
	})

	t.Run("empty range", func(t *testing.T) {
		counts, err := q.GetEventCounts(context.Background(), testSubject1, mkts(t, "2026-06-04T00:00:00Z"), mkts(t, "2026-06-05T00:00:00Z"), nil)
		require.NoError(t, err)
		assert.Empty(t, counts)
	})
}

func TestGetEventCountsForRanges(t *testing.T) {
	q := setupEventFixtures(t)
	ranges := []qtypes.TimeRange{
		{From: d1(t, "00:00:00"), To: d2(t, "00:00:00")},
		{From: d2(t, "00:00:00"), To: mkts(t, "2026-06-03T00:00:00Z")},
	}

	counts, err := q.GetEventCountsForRanges(context.Background(), testSubject1, ranges, nil)
	require.NoError(t, err)
	want := []*qtypes.EventCountForRange{
		{SegIndex: 0, Name: evHarshBrake, Count: 1},
		{SegIndex: 0, Name: evTripEnd, Count: 1},
		{SegIndex: 0, Name: evTripStart, Count: 1},
		{SegIndex: 1, Name: evHarshBrake, Count: 1},
		{SegIndex: 1, Name: evNoTags, Count: 1},
	}
	assert.Equal(t, want, counts)

	t.Run("name subset", func(t *testing.T) {
		counts, err := q.GetEventCountsForRanges(context.Background(), testSubject1, ranges, []string{evHarshBrake})
		require.NoError(t, err)
		want := []*qtypes.EventCountForRange{
			{SegIndex: 0, Name: evHarshBrake, Count: 1},
			{SegIndex: 1, Name: evHarshBrake, Count: 1},
		}
		assert.Equal(t, want, counts)
	})

	t.Run("empty ranges", func(t *testing.T) {
		counts, err := q.GetEventCountsForRanges(context.Background(), testSubject1, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, counts)
	})
}

func TestGetEventSummaries(t *testing.T) {
	q := setupEventFixtures(t)

	summaries, err := q.GetEventSummaries(context.Background(), testSubject1)
	require.NoError(t, err)
	require.Len(t, summaries, 4)

	assert.Equal(t, evHarshBrake, summaries[0].Name)
	assert.EqualValues(t, 2, summaries[0].Count)
	assert.Equal(t, d1(t, "11:00:00"), summaries[0].FirstSeen)
	assert.Equal(t, d2(t, "09:00:00"), summaries[0].LastSeen)

	assert.Equal(t, evNoTags, summaries[1].Name)
	assert.Equal(t, evTripEnd, summaries[2].Name)
	assert.Equal(t, evTripStart, summaries[3].Name)

	t.Run("unknown subject", func(t *testing.T) {
		summaries, err := q.GetEventSummaries(context.Background(), "did:erc721:1:0x0:404")
		require.NoError(t, err)
		assert.Empty(t, summaries)
	})
}
