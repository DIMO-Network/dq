package repositories

import (
	"context"
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
