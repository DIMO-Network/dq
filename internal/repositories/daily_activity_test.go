package repositories

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/qtypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dailyFake is a full QueryService whose unused methods panic (the embedded nil
// interface), so the test fails loudly if GetDailyActivity reaches a method it
// should not. The day-summary methods count their calls.
type dailyFake struct {
	QueryService
	perDayAgg    int32
	perDayEvents int32
}

func (f *dailyFake) GetSegments(context.Context, string, time.Time, time.Time, model.DetectionMechanism, *model.SegmentConfig) ([]*model.Segment, error) {
	return nil, nil
}

func (f *dailyFake) GetAggregatedSignalsForRanges(context.Context, string, []qtypes.TimeRange, time.Time, time.Time, []model.FloatSignalArgs, []model.LocationSignalArgs) ([]*qtypes.AggSignalForRange, error) {
	return nil, nil
}

func (f *dailyFake) GetEventCountsForRanges(context.Context, string, []qtypes.TimeRange, []string) ([]*qtypes.EventCountForRange, error) {
	return nil, nil
}

func (f *dailyFake) GetAggregatedSignals(context.Context, string, *model.AggregatedSignalArgs) ([]*qtypes.AggSignal, error) {
	atomic.AddInt32(&f.perDayAgg, 1)
	return nil, nil
}

func (f *dailyFake) GetEventCounts(context.Context, string, time.Time, time.Time, []string) ([]*qtypes.EventCount, error) {
	atomic.AddInt32(&f.perDayEvents, 1)
	return nil, nil
}

// GetDailyActivity must summarize all calendar days in one batched ForRanges
// call, not one GetAggregatedSignals + GetEventCounts per day. The per-day form
// fired up to 64 serialized round-trips for a 32-day window, blowing the request
// timeout (SR-3). Asserted by counting the non-batched calls: they must be zero.
func TestGetDailyActivity_BatchesDaySummaries(t *testing.T) {
	fb := &dailyFake{}
	r, err := NewRepository(fb)
	require.NoError(t, err)

	now := time.Now().UTC()
	from := now.AddDate(0, 0, -3)
	_, err = r.GetDailyActivity(context.Background(), "did:erc721:137:0xabc:1", from, now,
		model.DetectionMechanismIgnitionDetection, nil, nil, nil, nil)
	require.NoError(t, err)

	assert.Zero(t, atomic.LoadInt32(&fb.perDayAgg), "day summaries must be batched, not one GetAggregatedSignals per day")
	assert.Zero(t, atomic.LoadInt32(&fb.perDayEvents), "day event counts must be batched, not one GetEventCounts per day")
}
