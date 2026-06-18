package repositories

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/ch"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBackend implements Backend with overridable funcs for the methods the
// tests exercise; everything else returns zero values.
type fakeBackend struct {
	getAggregatedSignals func(ctx context.Context, subject string, aggArgs *model.AggregatedSignalArgs) ([]*ch.AggSignal, error)
	getEventCounts       func(ctx context.Context, subject string, from, to time.Time, eventNames []string) ([]*ch.EventCount, error)
}

func (f *fakeBackend) GetAggregatedSignals(ctx context.Context, subject string, aggArgs *model.AggregatedSignalArgs) ([]*ch.AggSignal, error) {
	if f.getAggregatedSignals != nil {
		return f.getAggregatedSignals(ctx, subject, aggArgs)
	}
	return nil, nil
}

func (f *fakeBackend) GetAggregatedSignalsForRanges(context.Context, string, []ch.TimeRange, time.Time, time.Time, []model.FloatSignalArgs, []model.LocationSignalArgs) ([]*ch.AggSignalForRange, error) {
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

func (f *fakeBackend) GetEventCounts(ctx context.Context, subject string, from, to time.Time, eventNames []string) ([]*ch.EventCount, error) {
	if f.getEventCounts != nil {
		return f.getEventCounts(ctx, subject, from, to, eventNames)
	}
	return nil, nil
}

func (f *fakeBackend) GetEventCountsForRanges(context.Context, string, []ch.TimeRange, []string) ([]*ch.EventCountForRange, error) {
	return nil, nil
}

func (f *fakeBackend) GetEventSummaries(context.Context, string) ([]*ch.EventSummary, error) {
	return nil, nil
}

// fakePrimary adds the segments surface so it satisfies CHService.
type fakePrimary struct {
	fakeBackend
	segments []*model.Segment
}

func (f *fakePrimary) GetSegments(context.Context, string, time.Time, time.Time, model.DetectionMechanism, *model.SegmentConfig) ([]*model.Segment, error) {
	return f.segments, nil
}

func diffValuesAny(a, b any) (string, bool) {
	return diffValues(reflect.ValueOf(a), reflect.ValueOf(b), "")
}

func counterValue(t *testing.T, vec *prometheus.CounterVec, method string) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, vec.WithLabelValues(method).Write(m))
	return m.GetCounter().GetValue()
}

func newTestShadow(primary CHService, secondary Backend, logOut *bytes.Buffer) *ShadowBackend {
	logger := zerolog.Nop()
	if logOut != nil {
		logger = zerolog.New(logOut)
	}
	// nil secondarySegment: segment shadow tests use a dedicated constructor
	// or pass nil when they only care about the signal/event shadow surface.
	return NewShadowBackend(primary, secondary, nil, logger)
}

func TestShadowMatchNoMismatch(t *testing.T) {
	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rows := func() []*ch.AggSignal {
		return []*ch.AggSignal{
			{SignalType: ch.FloatType, SignalIndex: 0, Timestamp: ts, ValueNumber: 42.5},
			{SignalType: ch.StringType, SignalIndex: 1, Timestamp: ts, ValueString: "on"},
		}
	}
	primary := &fakePrimary{fakeBackend: fakeBackend{
		getAggregatedSignals: func(context.Context, string, *model.AggregatedSignalArgs) ([]*ch.AggSignal, error) {
			return rows(), nil
		},
	}}
	secondary := &fakeBackend{
		getAggregatedSignals: func(context.Context, string, *model.AggregatedSignalArgs) ([]*ch.AggSignal, error) {
			out := rows()
			// Within epsilon and a different (but equal) time representation:
			// neither should count as a mismatch.
			out[0].ValueNumber += 1e-12
			out[0].Timestamp = ts.In(time.FixedZone("EST", -5*3600))
			return out, nil
		},
	}

	var logOut bytes.Buffer
	shadow := newTestShadow(primary, secondary, &logOut)

	mismatchBefore := counterValue(t, shadowMismatchTotal, "GetAggregatedSignals")
	errorBefore := counterValue(t, shadowErrorTotal, "GetAggregatedSignals")

	res, err := shadow.GetAggregatedSignals(context.Background(), "did:test:1", &model.AggregatedSignalArgs{})
	require.NoError(t, err)
	require.Len(t, res, 2)
	shadow.Wait()

	assert.Equal(t, mismatchBefore, counterValue(t, shadowMismatchTotal, "GetAggregatedSignals"), "matching results must not bump the mismatch counter")
	assert.Equal(t, errorBefore, counterValue(t, shadowErrorTotal, "GetAggregatedSignals"))
	assert.NotContains(t, logOut.String(), "shadow result mismatch")
}

func TestShadowMismatchCountedAndLogged(t *testing.T) {
	primary := &fakePrimary{fakeBackend: fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*ch.EventCount, error) {
			return []*ch.EventCount{{Name: "harshBraking", Count: 3}}, nil
		},
	}}
	secondary := &fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*ch.EventCount, error) {
			return []*ch.EventCount{{Name: "harshBraking", Count: 4}}, nil
		},
	}

	var logOut bytes.Buffer
	shadow := newTestShadow(primary, secondary, &logOut)

	mismatchBefore := counterValue(t, shadowMismatchTotal, "GetEventCounts")

	res, err := shadow.GetEventCounts(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), nil)
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, 3, res[0].Count, "primary result must be served untouched")
	shadow.Wait()

	assert.Equal(t, mismatchBefore+1, counterValue(t, shadowMismatchTotal, "GetEventCounts"))
	log := logOut.String()
	assert.Contains(t, log, "shadow result mismatch")
	assert.Contains(t, log, "GetEventCounts")
	assert.Contains(t, log, "Count", "diff sample should point at the differing field")
}

func TestShadowSecondaryPanicRecovered(t *testing.T) {
	primary := &fakePrimary{fakeBackend: fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*ch.EventCount, error) {
			return []*ch.EventCount{{Name: "ignitionOn", Count: 7}}, nil
		},
	}}
	secondary := &fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*ch.EventCount, error) {
			panic("duckdb exploded")
		},
	}

	var logOut bytes.Buffer
	shadow := newTestShadow(primary, secondary, &logOut)

	errorBefore := counterValue(t, shadowErrorTotal, "GetEventCounts")
	mismatchBefore := counterValue(t, shadowMismatchTotal, "GetEventCounts")

	res, err := shadow.GetEventCounts(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), nil)
	require.NoError(t, err, "secondary panic must not affect the primary result")
	require.Len(t, res, 1)
	assert.Equal(t, 7, res[0].Count)
	shadow.Wait()

	assert.Equal(t, errorBefore+1, counterValue(t, shadowErrorTotal, "GetEventCounts"))
	assert.Equal(t, mismatchBefore, counterValue(t, shadowMismatchTotal, "GetEventCounts"))
	assert.Contains(t, logOut.String(), "shadow call panicked")
}

func TestShadowSecondaryErrorCounted(t *testing.T) {
	primary := &fakePrimary{fakeBackend: fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*ch.EventCount, error) {
			return nil, nil
		},
	}}
	secondary := &fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*ch.EventCount, error) {
			return nil, errors.New("read_parquet failed")
		},
	}

	var logOut bytes.Buffer
	shadow := newTestShadow(primary, secondary, &logOut)

	errorBefore := counterValue(t, shadowErrorTotal, "GetEventCounts")
	_, err := shadow.GetEventCounts(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), nil)
	require.NoError(t, err)
	shadow.Wait()

	assert.Equal(t, errorBefore+1, counterValue(t, shadowErrorTotal, "GetEventCounts"))
	assert.Contains(t, logOut.String(), "shadow call failed")
}

func TestShadowPrimaryErrorSkipsSecondary(t *testing.T) {
	t.Parallel()
	primaryErr := errors.New("clickhouse down")
	primary := &fakePrimary{fakeBackend: fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*ch.EventCount, error) {
			return nil, primaryErr
		},
	}}
	secondaryCalled := make(chan struct{}, 1)
	secondary := &fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*ch.EventCount, error) {
			secondaryCalled <- struct{}{}
			return nil, nil
		},
	}

	shadow := newTestShadow(primary, secondary, nil)
	_, err := shadow.GetEventCounts(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), nil)
	require.ErrorIs(t, err, primaryErr)
	shadow.Wait()
	select {
	case <-secondaryCalled:
		t.Fatal("secondary must not be called when the primary errors")
	default:
	}
}

func TestShadowGetSegmentsPrimaryOnly(t *testing.T) {
	t.Parallel()
	segs := []*model.Segment{{}}
	primary := &fakePrimary{segments: segs}
	// nil secondarySegment: shadow skips comparison, returns primary result.
	shadow := newTestShadow(primary, &fakeBackend{}, nil)

	res, err := shadow.GetSegments(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), model.DetectionMechanismIdling, nil)
	require.NoError(t, err)
	assert.Equal(t, segs, res)
}

func TestShadowGetSegmentsWithSecondaryMatch(t *testing.T) {
	// Not parallel: reads a global Prometheus counter; parallel runs would race
	// with TestShadowGetSegmentsWithSecondaryMismatch which bumps the same counter.
	segs := []*model.Segment{{Duration: 300}}
	primary := &fakePrimary{segments: segs}
	secondary := &fakePrimary{segments: segs} // identical result
	logger := zerolog.Nop()
	shadow := NewShadowBackend(primary, &fakeBackend{}, secondary, logger)

	mismatchBefore := counterValue(t, shadowMismatchTotal, "GetSegments")
	res, err := shadow.GetSegments(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), model.DetectionMechanismIdling, nil)
	require.NoError(t, err)
	assert.Equal(t, segs, res)
	shadow.Wait()

	assert.Equal(t, mismatchBefore, counterValue(t, shadowMismatchTotal, "GetSegments"), "identical segment results must not bump mismatch counter")
}

func TestShadowGetSegmentsWithSecondaryMismatch(t *testing.T) {
	// Not parallel: see TestShadowGetSegmentsWithSecondaryMatch comment.
	primary := &fakePrimary{segments: []*model.Segment{{Duration: 300}}}
	secondary := &fakePrimary{segments: []*model.Segment{{Duration: 600}}} // different duration
	var logOut bytes.Buffer
	logger := zerolog.New(&logOut)
	shadow := NewShadowBackend(primary, &fakeBackend{}, secondary, logger)

	mismatchBefore := counterValue(t, shadowMismatchTotal, "GetSegments")
	res, err := shadow.GetSegments(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), model.DetectionMechanismIdling, nil)
	require.NoError(t, err)
	assert.Equal(t, 300, res[0].Duration, "primary result must be served untouched")
	shadow.Wait()

	assert.Equal(t, mismatchBefore+1, counterValue(t, shadowMismatchTotal, "GetSegments"), "differing durations must bump mismatch counter")
	assert.Contains(t, logOut.String(), "shadow result mismatch")
}

func TestDiffValuesFloatEpsilon(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		a, b  any
		equal bool
	}{
		{"exact", 1.5, 1.5, true},
		{"within epsilon", 1.5, 1.5 + 1e-10, true},
		{"outside epsilon", 1.5, 1.5 + 1e-8, false},
		{"nil vs empty slice", []string(nil), []string{}, true},
		{"nested struct diff", &ch.EventSummary{Name: "a", Count: 1}, &ch.EventSummary{Name: "a", Count: 2}, false},
		{"map values", map[string]float64{"x": 1}, map[string]float64{"x": 1 + 1e-12}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			diff, ok := diffValuesAny(tc.a, tc.b)
			assert.Equal(t, tc.equal, ok, "diff: %s", diff)
			if !tc.equal {
				assert.NotEmpty(t, diff)
			}
		})
	}
}
