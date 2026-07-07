package repositories

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/qtypes"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

type fakeBackend struct {
	getAggregatedSignals func(ctx context.Context, subject string, aggArgs *model.AggregatedSignalArgs) ([]*qtypes.AggSignal, error)
	getEventCounts       func(ctx context.Context, subject string, from, to time.Time, eventNames []string) ([]*qtypes.EventCount, error)
}

func (f *fakeBackend) GetAggregatedSignals(ctx context.Context, subject string, aggArgs *model.AggregatedSignalArgs) ([]*qtypes.AggSignal, error) {
	if f.getAggregatedSignals != nil {
		return f.getAggregatedSignals(ctx, subject, aggArgs)
	}
	return nil, nil
}

func (f *fakeBackend) GetAggregatedSignalsForRanges(context.Context, string, []qtypes.TimeRange, time.Time, time.Time, []model.FloatSignalArgs, []model.LocationSignalArgs) ([]*qtypes.AggSignalForRange, error) {
	return nil, nil
}

func (f *fakeBackend) GetLatestSignals(context.Context, string, *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	return nil, nil
}

func (f *fakeBackend) GetAllLatestSignals(context.Context, string, *model.SignalFilter) ([]*vss.Signal, error) {
	return nil, nil
}

func (f *fakeBackend) GetAvailableSignals(context.Context, string, *model.SignalFilter) ([]string, error) {
	return nil, nil
}

func (f *fakeBackend) GetSignalSummaries(context.Context, string, *model.SignalFilter) ([]*model.SignalDataSummary, error) {
	return nil, nil
}

func (f *fakeBackend) GetEvents(context.Context, string, time.Time, time.Time, *model.EventFilter) ([]*vss.Event, error) {
	return nil, nil
}

func (f *fakeBackend) GetEventCounts(ctx context.Context, subject string, from, to time.Time, eventNames []string) ([]*qtypes.EventCount, error) {
	if f.getEventCounts != nil {
		return f.getEventCounts(ctx, subject, from, to, eventNames)
	}
	return nil, nil
}

func (f *fakeBackend) GetEventCountsForRanges(context.Context, string, []qtypes.TimeRange, []string) ([]*qtypes.EventCountForRange, error) {
	return nil, nil
}

func (f *fakeBackend) GetEventSummaries(context.Context, string) ([]*qtypes.EventSummary, error) {
	return nil, nil
}

// fakePrimary adds the segments surface so it satisfies QueryService.
type fakePrimary struct {
	fakeBackend
	segments []*model.Segment
}

func (f *fakePrimary) GetSegments(context.Context, string, time.Time, time.Time, model.DetectionMechanism, *model.SegmentConfig) ([]*model.Segment, error) {
	return f.segments, nil
}

// locationCountingFake is a QueryService that counts location gap-fill queries: the
// batched LocationsAt path (finding #8) vs. the legacy per-point LocationAt path. The
// batched call resolves each probe via locFn so a test can assert the scattered
// results as well as the O(1) query count. Unimplemented methods panic through the
// embedded nil QueryService, so an unexpected call fails loudly.
type locationCountingFake struct {
	QueryService
	segments      []*model.Segment
	batchCalls    int32
	pointCalls    int32
	lastBatchSize int32
	locFn         func(ts time.Time) *model.Location
}

func (f *locationCountingFake) GetSegments(context.Context, string, time.Time, time.Time, model.DetectionMechanism, *model.SegmentConfig) ([]*model.Segment, error) {
	return f.segments, nil
}

func (f *locationCountingFake) GetAggregatedSignalsForRanges(context.Context, string, []qtypes.TimeRange, time.Time, time.Time, []model.FloatSignalArgs, []model.LocationSignalArgs) ([]*qtypes.AggSignalForRange, error) {
	return nil, nil // no aggregate locations → every boundary needs gap-fill
}

func (f *locationCountingFake) GetEventCountsForRanges(context.Context, string, []qtypes.TimeRange, []string) ([]*qtypes.EventCountForRange, error) {
	return nil, nil
}

func (f *locationCountingFake) GetAggregatedSignals(context.Context, string, *model.AggregatedSignalArgs) ([]*qtypes.AggSignal, error) {
	return nil, nil
}

func (f *locationCountingFake) GetEventCounts(context.Context, string, time.Time, time.Time, []string) ([]*qtypes.EventCount, error) {
	return nil, nil
}

// LocationAt is the legacy per-point path; a batched implementation must never call it.
func (f *locationCountingFake) LocationAt(_ context.Context, _ string, ts time.Time) (*model.Location, error) {
	atomic.AddInt32(&f.pointCalls, 1)
	if f.locFn != nil {
		return f.locFn(ts), nil
	}
	return nil, nil
}

// LocationsAt is the batched path: one call resolves every probe.
func (f *locationCountingFake) LocationsAt(_ context.Context, _ string, tss []time.Time) ([]*model.Location, error) {
	atomic.AddInt32(&f.batchCalls, 1)
	atomic.StoreInt32(&f.lastBatchSize, int32(len(tss)))
	out := make([]*model.Location, len(tss))
	for i, ts := range tss {
		if f.locFn != nil {
			out[i] = f.locFn(ts)
		}
	}
	return out, nil
}
