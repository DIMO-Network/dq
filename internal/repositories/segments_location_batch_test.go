package repositories

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// locSeg builds a complete (start+end) segment with no aggregate-supplied location,
// so both boundaries need gap-fill.
func locSeg(start, end time.Time) *model.Segment {
	return &model.Segment{
		Start: &model.SignalLocation{Timestamp: start},
		End:   &model.SignalLocation{Timestamp: end},
	}
}

// TestGetSegments_BatchesLocationGapFill proves finding #8: segment start/end
// location gap-fill fans out to ONE batched LocationsAt query for the whole page —
// not one LocationAt point query per boundary (which was O(2·segments)) — and the
// batched results scatter back to the correct boundary by index. The legacy per-point
// LocationAt path must never be touched.
func TestGetSegments_BatchesLocationGapFill(t *testing.T) {
	base := time.Now().UTC().Add(-24 * time.Hour)
	ts := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	const n = 25
	segs := make([]*model.Segment, n)
	for i := 0; i < n; i++ {
		segs[i] = locSeg(ts(i*10), ts(i*10+5))
	}

	// Each probe resolves to a location encoding its own timestamp, so a mis-scatter
	// (wrong boundary getting another boundary's fix) is caught.
	fake := &locationCountingFake{
		segments: segs,
		locFn: func(t time.Time) *model.Location {
			return &model.Location{Latitude: float64(t.Unix()), Longitude: -float64(t.Unix()), Hdop: 1}
		},
	}
	r, err := NewRepository(fake)
	require.NoError(t, err)

	from, to := ts(0), ts(n*10+10)
	// A signal request forces wantSummary so enrichment (and thus gap-fill) runs.
	got, err := r.GetSegments(context.Background(), "did:erc721:137:0xabc:1", from, to,
		model.DetectionMechanismIgnitionDetection, nil,
		[]*model.SegmentSignalRequest{sigSpeed}, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, got, n)

	// O(1) location queries: exactly one batched call, zero point calls, covering all
	// 2·n boundaries.
	assert.Equal(t, int32(1), atomic.LoadInt32(&fake.batchCalls), "gap-fill must issue exactly one batched LocationsAt")
	assert.Zero(t, atomic.LoadInt32(&fake.pointCalls), "batched path must never fall back to per-point LocationAt")
	assert.Equal(t, int32(2*n), atomic.LoadInt32(&fake.lastBatchSize), "one probe per start+end boundary")

	// Correct scatter: each boundary carries the fix for ITS OWN timestamp.
	for i, seg := range got {
		require.NotNil(t, seg.Start.Value, "seg %d start", i)
		require.NotNil(t, seg.End.Value, "seg %d end", i)
		assert.Equalf(t, float64(seg.Start.Timestamp.Unix()), seg.Start.Value.Latitude, "seg %d start scatter", i)
		assert.Equalf(t, float64(seg.End.Timestamp.Unix()), seg.End.Value.Latitude, "seg %d end scatter", i)
	}
}

// TestGetSegments_GapFillNoFixFallsBackToOrigin proves a boundary with no prior fix
// still resolves to the (0,0) no-data sentinel through the batched path, preserving
// the prior per-point fallback chain.
func TestGetSegments_GapFillNoFixFallsBackToOrigin(t *testing.T) {
	base := time.Now().UTC().Add(-time.Hour)
	fake := &locationCountingFake{
		segments: []*model.Segment{locSeg(base, base.Add(5*time.Minute))},
		locFn:    func(time.Time) *model.Location { return nil }, // vehicle has no fix ever
	}
	r, err := NewRepository(fake)
	require.NoError(t, err)

	got, err := r.GetSegments(context.Background(), "did:erc721:137:0xabc:1", base.Add(-time.Minute), base.Add(time.Hour),
		model.DetectionMechanismIgnitionDetection, nil,
		[]*model.SegmentSignalRequest{sigSpeed}, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].Start.Value)
	require.NotNil(t, got[0].End.Value)
	assert.Equal(t, noDataLocation(), got[0].Start.Value, "no fix → (0,0) sentinel")
	assert.Equal(t, noDataLocation(), got[0].End.Value)
	assert.Equal(t, int32(1), atomic.LoadInt32(&fake.batchCalls))
	assert.Zero(t, atomic.LoadInt32(&fake.pointCalls))
}

// TestGetDailyActivity_BatchesLocationGapFill proves the day-boundary gap-fill is
// also batched (finding #8): GetDailyActivity issues O(1) location queries — one
// batched call for the inner GetSegments page and one for the day boundaries — and
// never a per-day point query, regardless of the number of days.
func TestGetDailyActivity_BatchesLocationGapFill(t *testing.T) {
	fake := &locationCountingFake{
		locFn: func(t time.Time) *model.Location {
			return &model.Location{Latitude: float64(t.Unix()), Longitude: 1, Hdop: 1}
		},
	}
	r, err := NewRepository(fake)
	require.NoError(t, err)

	now := time.Now().UTC()
	from := now.AddDate(0, 0, -20) // 21 calendar days
	days, err := r.GetDailyActivity(context.Background(), "did:erc721:137:0xabc:1", from, now,
		model.DetectionMechanismIgnitionDetection, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, days)

	// No per-point LocationAt regardless of day count; batched calls are O(1) (segments
	// page + day boundaries), not O(days).
	assert.Zero(t, atomic.LoadInt32(&fake.pointCalls), "day gap-fill must never use per-point LocationAt")
	assert.LessOrEqual(t, atomic.LoadInt32(&fake.batchCalls), int32(2),
		"at most one batched call for the inner segments page and one for day boundaries")

	// Every day boundary is resolved (no nil Values leak out).
	for i, d := range days {
		require.NotNilf(t, d.Start.Value, "day %d start", i)
		require.NotNilf(t, d.End.Value, "day %d end", i)
	}
}
