package repositories

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSegmentsStartingAfter pins the cursor filter that replaced the buggy `from`
// advance. A segment whose (clipped) start is at or before the cursor was already
// returned on a prior page and must be dropped; only strictly-later segments survive.
func TestSegmentsStartingAfter(t *testing.T) {
	ts := func(s int) time.Time { return time.Unix(int64(s), 0).UTC() }
	seg := func(start int) *model.Segment {
		return &model.Segment{Start: &model.SignalLocation{Timestamp: ts(start)}}
	}

	out := segmentsStartingAfter([]*model.Segment{seg(10), seg(20), seg(30)}, ts(20))
	require.Len(t, out, 1, "cursor=20 retires the 10 and 20 segments; only 30 survives")
	assert.True(t, out[0].Start.Timestamp.Equal(ts(30)))

	require.NotPanics(t, func() {
		segmentsStartingAfter([]*model.Segment{{Start: nil}}, ts(0))
	}, "a nil Start must be skipped, not panic")
}

// TestGetDailyActivity_DSTCalendarDays pins calendar-day iteration across a DST
// transition. A civil day is 23h/25h on a transition, so iterating by a flat 24h drifts
// off local midnight and terminates a day early, dropping the last calendar day. NY
// spring-forward is 2025-03-09; a 2025-03-07 → 2025-03-11 range must yield 5 daily
// records (the flat-24h bug returned 4 and mis-aligned 03-10).
func TestGetDailyActivity_DSTCalendarDays(t *testing.T) {
	r, err := NewRepository(&dailyFake{})
	require.NoError(t, err)

	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	from := time.Date(2025, 3, 7, 12, 0, 0, 0, loc)
	to := time.Date(2025, 3, 11, 12, 0, 0, 0, loc)
	tz := "America/New_York"

	days, err := r.GetDailyActivity(context.Background(), "did:erc721:137:0xabc:1", from, to,
		model.DetectionMechanismIgnitionDetection, nil, nil, nil, &tz)
	require.NoError(t, err)
	require.Len(t, days, 5,
		"5 calendar days 03-07..03-11; a flat-24h iterator drops 03-11 across the 03-09 DST transition")

	// Day starts are local midnight in UTC; the boundary shifts 05:00Z (EST) → 04:00Z
	// (EDT) across 03-09, proving calendar (not 24h) stepping.
	wantStartsUTC := []time.Time{
		time.Date(2025, 3, 7, 5, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 8, 5, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 9, 5, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 10, 4, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 11, 4, 0, 0, 0, time.UTC),
	}
	for i, d := range days {
		require.NotNil(t, d.Start, "day %d", i)
		assert.Truef(t, d.Start.Timestamp.Equal(wantStartsUTC[i]),
			"day %d start: want %s got %s", i, wantStartsUTC[i], d.Start.Timestamp)
	}
}
