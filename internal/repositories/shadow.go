package repositories

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/ch"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

var (
	shadowMismatchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dq_shadow_mismatch_total",
			Help: "Number of shadow (DuckDB) query results that did not match the primary (ClickHouse) result.",
		},
		[]string{"method"},
	)
	shadowErrorTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dq_shadow_error_total",
			Help: "Number of shadow (DuckDB) queries that errored or panicked.",
		},
		[]string{"method"},
	)
	// shadowDroppedTotal is separate from errors so a saturated shadow can be
	// distinguished from a failing one: a high drop ratio means "clean" only
	// means "didn't look", and the cutover gate must not trust it (CHD-15).
	shadowDroppedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dq_shadow_dropped_total",
			Help: "Number of shadow (DuckDB) queries skipped because the concurrency limit was saturated (not compared).",
		},
		[]string{"method"},
	)
)

const (
	defaultShadowTimeout        = 10 * time.Second
	defaultShadowMaxConcurrency = 4
	// floatEpsilon is the absolute tolerance used when comparing float values
	// between the two backends.
	floatEpsilon = 1e-9
	// diffSampleLen caps the rendered diff sample in mismatch logs.
	diffSampleLen = 512
)

// ShadowBackend serves every query from the primary backend while replaying
// the signal/latest/summary/event/segment queries against the secondary
// backends in the background and comparing results. Mismatches and secondary
// errors are logged and counted; they never affect the primary response.
type ShadowBackend struct {
	primary          CHService
	secondary        Backend
	secondarySegment SegmentsBackend
	log              zerolog.Logger
	sem              chan struct{}
	timeout          time.Duration
	wg               sync.WaitGroup
}

var _ CHService = (*ShadowBackend)(nil)

// NewShadowBackend creates a ShadowBackend with bounded shadow concurrency
// and a per-call timeout for the secondary backend. secondarySegment may be
// nil, in which case GetSegments shadows are skipped (treated as match).
func NewShadowBackend(primary CHService, secondary Backend, secondarySegment SegmentsBackend, log zerolog.Logger) *ShadowBackend {
	return &ShadowBackend{
		primary:          primary,
		secondary:        secondary,
		secondarySegment: secondarySegment,
		log:              log.With().Str("component", "shadow").Logger(),
		sem:              make(chan struct{}, defaultShadowMaxConcurrency),
		timeout:          defaultShadowTimeout,
	}
}

// Wait blocks until all in-flight shadow calls have finished. Intended for
// shutdown and tests.
func (s *ShadowBackend) Wait() {
	s.wg.Wait()
}

// shadow fires call against the secondary backend in a goroutine and compares
// its result to the primary result. When the primary itself errored the
// comparison is skipped — there is nothing trustworthy to compare against.
// Slots are bounded by the semaphore; when saturated the shadow call is
// dropped (and counted) rather than delaying or piling up goroutines.
func (s *ShadowBackend) shadow(method, argsSummary string, primaryRes any, primaryErr error, call func(ctx context.Context) (any, error)) {
	if primaryErr != nil {
		return
	}
	select {
	case s.sem <- struct{}{}:
	default:
		shadowDroppedTotal.WithLabelValues(method).Inc()
		s.log.Warn().Str("method", method).Msg("shadow call dropped: concurrency limit reached")
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-s.sem }()
		defer func() {
			if r := recover(); r != nil {
				shadowErrorTotal.WithLabelValues(method).Inc()
				s.log.Error().Str("method", method).Str("args", argsSummary).
					Interface("panic", r).Msg("shadow call panicked")
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		defer cancel()
		secondaryRes, err := call(ctx)
		if err != nil {
			shadowErrorTotal.WithLabelValues(method).Inc()
			s.log.Error().Err(err).Str("method", method).Str("args", argsSummary).
				Msg("shadow call failed")
			return
		}
		if diff, ok := diffValues(reflect.ValueOf(primaryRes), reflect.ValueOf(secondaryRes), ""); !ok {
			shadowMismatchTotal.WithLabelValues(method).Inc()
			s.log.Warn().Str("method", method).Str("args", argsSummary).
				Str("diff", truncate(diff, diffSampleLen)).
				Msg("shadow result mismatch")
		}
	}()
}

func subjectRange(subject string, from, to time.Time) string {
	return fmt.Sprintf("subject=%s from=%s to=%s", subject, from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339))
}

// GetAggregatedSignals serves from primary and shadows to secondary.
func (s *ShadowBackend) GetAggregatedSignals(ctx context.Context, subject string, aggArgs *model.AggregatedSignalArgs) ([]*ch.AggSignal, error) {
	res, err := s.primary.GetAggregatedSignals(ctx, subject, aggArgs)
	args := fmt.Sprintf("subject=%s", subject)
	if aggArgs != nil {
		args = fmt.Sprintf("%s floatArgs=%d stringArgs=%d locationArgs=%d", subjectRange(subject, aggArgs.FromTS, aggArgs.ToTS), len(aggArgs.FloatArgs), len(aggArgs.StringArgs), len(aggArgs.LocationArgs))
	}
	s.shadow("GetAggregatedSignals", args, res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetAggregatedSignals(ctx, subject, aggArgs)
	})
	return res, err
}

// GetAggregatedSignalsForRanges serves from primary and shadows to secondary.
func (s *ShadowBackend) GetAggregatedSignalsForRanges(ctx context.Context, subject string, ranges []ch.TimeRange, globalFrom, globalTo time.Time, floatArgs []model.FloatSignalArgs, locationArgs []model.LocationSignalArgs) ([]*ch.AggSignalForRange, error) {
	res, err := s.primary.GetAggregatedSignalsForRanges(ctx, subject, ranges, globalFrom, globalTo, floatArgs, locationArgs)
	args := fmt.Sprintf("%s ranges=%d floatArgs=%d locationArgs=%d", subjectRange(subject, globalFrom, globalTo), len(ranges), len(floatArgs), len(locationArgs))
	s.shadow("GetAggregatedSignalsForRanges", args, res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetAggregatedSignalsForRanges(ctx, subject, ranges, globalFrom, globalTo, floatArgs, locationArgs)
	})
	return res, err
}

// GetLatestSignals serves from primary and shadows to secondary.
func (s *ShadowBackend) GetLatestSignals(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	res, err := s.primary.GetLatestSignals(ctx, subject, latestArgs)
	args := fmt.Sprintf("subject=%s", subject)
	if latestArgs != nil {
		args = fmt.Sprintf("subject=%s signals=%d", subject, len(latestArgs.SignalNames))
	}
	s.shadow("GetLatestSignals", args, res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetLatestSignals(ctx, subject, latestArgs)
	})
	return res, err
}

// GetAllLatestSignals serves from primary and shadows to secondary.
func (s *ShadowBackend) GetAllLatestSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]*vss.Signal, error) {
	res, err := s.primary.GetAllLatestSignals(ctx, subject, filter)
	s.shadow("GetAllLatestSignals", "subject="+subject, res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetAllLatestSignals(ctx, subject, filter)
	})
	return res, err
}

// GetAvailableSignals serves from primary and shadows to secondary.
func (s *ShadowBackend) GetAvailableSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]string, error) {
	res, err := s.primary.GetAvailableSignals(ctx, subject, filter)
	s.shadow("GetAvailableSignals", "subject="+subject, res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetAvailableSignals(ctx, subject, filter)
	})
	return res, err
}

// GetSignalSummaries serves from primary and shadows to secondary.
func (s *ShadowBackend) GetSignalSummaries(ctx context.Context, subject string, filter *model.SignalFilter) ([]*model.SignalDataSummary, error) {
	res, err := s.primary.GetSignalSummaries(ctx, subject, filter)
	s.shadow("GetSignalSummaries", "subject="+subject, res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetSignalSummaries(ctx, subject, filter)
	})
	return res, err
}

// GetEvents serves from primary and shadows to secondary.
func (s *ShadowBackend) GetEvents(ctx context.Context, subject string, from, to time.Time, filter *model.EventFilter) ([]*vss.Event, error) {
	res, err := s.primary.GetEvents(ctx, subject, from, to, filter)
	s.shadow("GetEvents", subjectRange(subject, from, to), res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetEvents(ctx, subject, from, to, filter)
	})
	return res, err
}

// GetEventCounts serves from primary and shadows to secondary.
func (s *ShadowBackend) GetEventCounts(ctx context.Context, subject string, from, to time.Time, eventNames []string) ([]*ch.EventCount, error) {
	res, err := s.primary.GetEventCounts(ctx, subject, from, to, eventNames)
	s.shadow("GetEventCounts", subjectRange(subject, from, to), res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetEventCounts(ctx, subject, from, to, eventNames)
	})
	return res, err
}

// GetEventCountsForRanges serves from primary and shadows to secondary.
func (s *ShadowBackend) GetEventCountsForRanges(ctx context.Context, subject string, ranges []ch.TimeRange, eventNames []string) ([]*ch.EventCountForRange, error) {
	res, err := s.primary.GetEventCountsForRanges(ctx, subject, ranges, eventNames)
	args := fmt.Sprintf("subject=%s ranges=%d events=%d", subject, len(ranges), len(eventNames))
	s.shadow("GetEventCountsForRanges", args, res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetEventCountsForRanges(ctx, subject, ranges, eventNames)
	})
	return res, err
}

// GetEventSummaries serves from primary and shadows to secondary.
func (s *ShadowBackend) GetEventSummaries(ctx context.Context, subject string) ([]*ch.EventSummary, error) {
	res, err := s.primary.GetEventSummaries(ctx, subject)
	s.shadow("GetEventSummaries", "subject="+subject, res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetEventSummaries(ctx, subject)
	})
	return res, err
}

// GetSegments serves from the primary and shadows to secondarySegment when
// one is configured. A nil secondarySegment skips the shadow (treated as
// match).
func (s *ShadowBackend) GetSegments(ctx context.Context, subject string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig) ([]*model.Segment, error) {
	res, err := s.primary.GetSegments(ctx, subject, from, to, mechanism, config)
	args := fmt.Sprintf("%s mechanism=%s", subjectRange(subject, from, to), mechanism)
	s.shadow("GetSegments", args, res, err, func(ctx context.Context) (any, error) {
		if s.secondarySegment == nil {
			return res, nil // no secondary configured; treat as match
		}
		return s.secondarySegment.GetSegments(ctx, subject, from, to, mechanism, config)
	})
	return res, err
}

var (
	timeType        = reflect.TypeOf(time.Time{})
	signalSliceType = reflect.TypeOf([]*vss.Signal(nil))
)

// sortedSignals returns a copy ordered by (name, timestamp) with nils first,
// giving diffValues a canonical order for slices that have none.
func sortedSignals(in []*vss.Signal) []*vss.Signal {
	out := slices.Clone(in)
	slices.SortStableFunc(out, func(x, y *vss.Signal) int {
		if x == nil || y == nil {
			return boolToInt(y == nil) - boolToInt(x == nil)
		}
		if c := strings.Compare(x.Data.Name, y.Data.Name); c != 0 {
			return c
		}
		return x.Data.Timestamp.Compare(y.Data.Timestamp)
	})
	return out
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// diffValues deep-compares two values like reflect.DeepEqual, with two
// telemetry-specific relaxations: floats are equal within floatEpsilon (NaN
// equals NaN), and time.Time is compared with Equal so wall-clock-identical
// instants match regardless of monotonic reading or location. It returns a
// human-readable sample describing the first difference found.
func diffValues(a, b reflect.Value, path string) (string, bool) {
	if !a.IsValid() || !b.IsValid() {
		if a.IsValid() == b.IsValid() {
			return "", true
		}
		return fmt.Sprintf("%s: validity mismatch (primary valid=%t, secondary valid=%t)", path, a.IsValid(), b.IsValid()), false
	}
	if a.Type() != b.Type() {
		return fmt.Sprintf("%s: type mismatch (%s vs %s)", path, a.Type(), b.Type()), false
	}

	// time.Time gets instant comparison (Equal) so wall-clock-identical times
	// match regardless of monotonic reading or location. A time.Time buried in
	// an unexported field (never the case for query row types) falls through
	// to the strict struct walk below.
	if a.Type() == timeType && a.CanInterface() {
		ta, tb := a.Interface().(time.Time), b.Interface().(time.Time)
		if !ta.Equal(tb) {
			return fmt.Sprintf("%s: %s != %s", path, ta, tb), false
		}
		return "", true
	}

	switch a.Kind() {
	case reflect.Float32, reflect.Float64:
		fa, fb := a.Float(), b.Float()
		if math.IsNaN(fa) && math.IsNaN(fb) {
			return "", true
		}
		if math.Abs(fa-fb) > floatEpsilon {
			return fmt.Sprintf("%s: %v != %v", path, fa, fb), false
		}
		return "", true
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			if a.IsNil() == b.IsNil() {
				return "", true
			}
			return fmt.Sprintf("%s: nil mismatch (primary nil=%t, secondary nil=%t)", path, a.IsNil(), b.IsNil()), false
		}
		return diffValues(a.Elem(), b.Elem(), path)
	case reflect.Slice, reflect.Array:
		if a.Kind() == reflect.Slice && a.IsNil() != b.IsNil() && a.Len() == 0 && b.Len() == 0 {
			return "", true // treat nil and empty slices as equal
		}
		// Latest-signal results carry no ordering guarantee from either
		// backend (GROUP BY output order is engine-specific), so compare
		// them as sets keyed by (name, timestamp) rather than by position.
		if a.Type() == signalSliceType && a.CanInterface() && b.CanInterface() {
			a = reflect.ValueOf(sortedSignals(a.Interface().([]*vss.Signal)))
			b = reflect.ValueOf(sortedSignals(b.Interface().([]*vss.Signal)))
		}
		if a.Len() != b.Len() {
			return fmt.Sprintf("%s: length %d != %d", path, a.Len(), b.Len()), false
		}
		for i := range a.Len() {
			if diff, ok := diffValues(a.Index(i), b.Index(i), fmt.Sprintf("%s[%d]", path, i)); !ok {
				return diff, false
			}
		}
		return "", true
	case reflect.Map:
		if a.Len() != b.Len() {
			return fmt.Sprintf("%s: map length %d != %d", path, a.Len(), b.Len()), false
		}
		iter := a.MapRange()
		for iter.Next() {
			bv := b.MapIndex(iter.Key())
			if !bv.IsValid() {
				return fmt.Sprintf("%s[%v]: missing in secondary", path, iter.Key()), false
			}
			if diff, ok := diffValues(iter.Value(), bv, fmt.Sprintf("%s[%v]", path, iter.Key())); !ok {
				return diff, false
			}
		}
		return "", true
	case reflect.Struct:
		for i := range a.NumField() {
			if diff, ok := diffValues(a.Field(i), b.Field(i), path+"."+a.Type().Field(i).Name); !ok {
				return diff, false
			}
		}
		return "", true
	case reflect.String:
		if a.String() != b.String() {
			return fmt.Sprintf("%s: %q != %q", path, a.String(), b.String()), false
		}
		return "", true
	case reflect.Bool:
		if a.Bool() != b.Bool() {
			return fmt.Sprintf("%s: %t != %t", path, a.Bool(), b.Bool()), false
		}
		return "", true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if a.Int() != b.Int() {
			return fmt.Sprintf("%s: %d != %d", path, a.Int(), b.Int()), false
		}
		return "", true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if a.Uint() != b.Uint() {
			return fmt.Sprintf("%s: %d != %d", path, a.Uint(), b.Uint()), false
		}
		return "", true
	default:
		// Channels, funcs, complex numbers, unsafe pointers: none appear in
		// query row types; fall back to strict DeepEqual semantics.
		if a.CanInterface() && b.CanInterface() && reflect.DeepEqual(a.Interface(), b.Interface()) {
			return "", true
		}
		return fmt.Sprintf("%s: values of kind %s differ", path, a.Kind()), false
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
