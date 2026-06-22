package eventrepo

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

var (
	fetchShadowMismatchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dq_fetch_shadow_mismatch_total",
			Help: "Number of fetch shadow (lake) index results that did not match the primary (ClickHouse) result.",
		},
		[]string{"method"},
	)
	fetchShadowErrorTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dq_fetch_shadow_error_total",
			Help: "Number of fetch shadow (lake) index queries that errored or panicked.",
		},
		[]string{"method"},
	)
	// fetchShadowDroppedTotal is separate from errors so a saturated fetch
	// shadow is distinguishable from a failing one (CHD-15) — a high drop ratio
	// means comparisons were skipped, not that the lake matched.
	fetchShadowDroppedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dq_fetch_shadow_dropped_total",
			Help: "Number of fetch shadow (lake) index queries skipped because the concurrency limit was saturated (not compared).",
		},
		[]string{"method"},
	)
)

const (
	defaultFetchShadowTimeout = 10 * time.Second
	// 16, not 4: at 4 the shadow drops the great majority of comparisons under
	// even moderate QPS (CH p99 is 50–200ms), so a clean mismatch counter meant
	// "didn't look", not "matched" — undermining the cutover gate (SR-13).
	defaultFetchShadowMaxConcurrency = 16
	fetchShadowDiffSampleLen         = 512
)

// ShadowEventService serves fetch from a ClickHouse primary while replaying
// the three index/summary methods against a lake secondary and counting
// metadata mismatches. Payload methods (GetCloudEventFromIndex,
// ListCloudEventsFromIndexes, PresignBlobURL) are served from primary only —
// byte-level payload comparison is out of scope.
type ShadowEventService struct {
	primary   EventService
	secondary EventService
	log       zerolog.Logger
	sem       chan struct{}
	timeout   time.Duration
	wg        sync.WaitGroup
}

var _ EventService = (*ShadowEventService)(nil)

// NewShadowEventService creates a ShadowEventService with bounded shadow
// concurrency. primary (ClickHouse) always serves responses; secondary (lake)
// is compared in the background.
func NewShadowEventService(primary, secondary EventService, log zerolog.Logger) *ShadowEventService {
	return &ShadowEventService{
		primary:   primary,
		secondary: secondary,
		log:       log,
		sem:       make(chan struct{}, defaultFetchShadowMaxConcurrency),
		timeout:   defaultFetchShadowTimeout,
	}
}

// Wait blocks until all in-flight shadow goroutines have finished.
func (s *ShadowEventService) Wait() {
	s.wg.Wait()
}

// BatchesAllIndexes follows the primary, since payloads are served from it.
func (s *ShadowEventService) BatchesAllIndexes() bool { return s.primary.BatchesAllIndexes() }

// shadow fires call against the secondary in a goroutine and compares its
// result to primaryRes. When primaryErr is non-nil the shadow is skipped.
// Slots are bounded by the semaphore; when saturated the call is dropped.
func (s *ShadowEventService) shadow(method string, primaryRes any, primaryErr error, call func(ctx context.Context) (any, error)) {
	if primaryErr != nil {
		return
	}
	select {
	case s.sem <- struct{}{}:
	default:
		fetchShadowDroppedTotal.WithLabelValues(method).Inc()
		s.log.Warn().Str("method", method).Msg("fetch shadow call dropped: concurrency limit reached")
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-s.sem }()
		defer func() {
			if r := recover(); r != nil {
				fetchShadowErrorTotal.WithLabelValues(method).Inc()
				s.log.Error().Str("method", method).Interface("panic", r).Msg("fetch shadow call panicked")
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		defer cancel()
		secondaryRes, err := call(ctx)
		if err != nil {
			fetchShadowErrorTotal.WithLabelValues(method).Inc()
			s.log.Error().Err(err).Str("method", method).Msg("fetch shadow call failed")
			return
		}
		if diff, ok := diffEventValues(reflect.ValueOf(primaryRes), reflect.ValueOf(secondaryRes), ""); !ok {
			fetchShadowMismatchTotal.WithLabelValues(method).Inc()
			s.log.Warn().Str("method", method).
				Str("diff", truncateStr(diff, fetchShadowDiffSampleLen)).
				Msg("fetch shadow result mismatch")
		}
	}()
}

// --- Index methods (shadowed) ---

// ListIndexesAdvanced serves from primary and shadows to secondary.
func (s *ShadowEventService) ListIndexesAdvanced(ctx context.Context, limit int, opts *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error) {
	res, err := s.primary.ListIndexesAdvanced(ctx, limit, opts)
	s.shadow("ListIndexesAdvanced", res, err, func(ctx context.Context) (any, error) {
		return s.secondary.ListIndexesAdvanced(ctx, limit, opts)
	})
	return res, err
}

// GetLatestIndexAdvanced serves from primary and shadows to secondary.
func (s *ShadowEventService) GetLatestIndexAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[ObjectInfo], error) {
	res, err := s.primary.GetLatestIndexAdvanced(ctx, opts)
	s.shadow("GetLatestIndexAdvanced", res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetLatestIndexAdvanced(ctx, opts)
	})
	return res, err
}

// GetCloudEventTypeSummariesAdvanced serves from primary and shadows to secondary.
func (s *ShadowEventService) GetCloudEventTypeSummariesAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) ([]CloudEventTypeSummary, error) {
	res, err := s.primary.GetCloudEventTypeSummariesAdvanced(ctx, opts)
	s.shadow("GetCloudEventTypeSummariesAdvanced", res, err, func(ctx context.Context) (any, error) {
		return s.secondary.GetCloudEventTypeSummariesAdvanced(ctx, opts)
	})
	return res, err
}

// --- Non-Advanced index methods (delegate to Advanced) ---

// ListIndexes serves from primary only (non-Advanced variant).
func (s *ShadowEventService) ListIndexes(ctx context.Context, limit int, opts *grpc.SearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error) {
	return s.primary.ListIndexes(ctx, limit, opts)
}

// GetLatestIndex serves from primary only (non-Advanced variant).
func (s *ShadowEventService) GetLatestIndex(ctx context.Context, opts *grpc.SearchOptions) (cloudevent.CloudEvent[ObjectInfo], error) {
	return s.primary.GetLatestIndex(ctx, opts)
}

// --- Payload methods (primary only; no secondary comparison) ---

// GetCloudEventFromIndex serves from primary only.
func (s *ShadowEventService) GetCloudEventFromIndex(ctx context.Context, index *cloudevent.CloudEvent[ObjectInfo], bucketName string) (cloudevent.RawEvent, error) {
	return s.primary.GetCloudEventFromIndex(ctx, index, bucketName)
}

// ListCloudEventsFromIndexes serves from primary only.
func (s *ShadowEventService) ListCloudEventsFromIndexes(ctx context.Context, indexes []cloudevent.CloudEvent[ObjectInfo], bucketName string) ([]cloudevent.RawEvent, error) {
	return s.primary.ListCloudEventsFromIndexes(ctx, indexes, bucketName)
}

// PresignBlobURL serves from primary only.
func (s *ShadowEventService) PresignBlobURL(ctx context.Context, key string) (string, error) {
	return s.primary.PresignBlobURL(ctx, key)
}

// diffEventValues deep-compares two CloudEvent index result values, treating
// time.Time with Equal (ignores monotonic clock), nil and empty slices as
// equal, and truncating the diff sample for logging.
func diffEventValues(a, b reflect.Value, path string) (string, bool) {
	if !a.IsValid() || !b.IsValid() {
		if a.IsValid() == b.IsValid() {
			return "", true
		}
		return fmt.Sprintf("%s: validity mismatch (primary valid=%t, secondary valid=%t)", path, a.IsValid(), b.IsValid()), false
	}
	if a.Type() != b.Type() {
		return fmt.Sprintf("%s: type mismatch (%s vs %s)", path, a.Type(), b.Type()), false
	}

	var timeType = reflect.TypeOf(time.Time{})
	if a.Type() == timeType && a.CanInterface() {
		ta, tb := a.Interface().(time.Time), b.Interface().(time.Time)
		if !ta.Equal(tb) {
			return fmt.Sprintf("%s: %s != %s", path, ta, tb), false
		}
		return "", true
	}

	switch a.Kind() {
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			if a.IsNil() == b.IsNil() {
				return "", true
			}
			return fmt.Sprintf("%s: nil mismatch (primary nil=%t, secondary nil=%t)", path, a.IsNil(), b.IsNil()), false
		}
		return diffEventValues(a.Elem(), b.Elem(), path)
	case reflect.Slice, reflect.Array:
		if a.Kind() == reflect.Slice && a.IsNil() != b.IsNil() && a.Len() == 0 && b.Len() == 0 {
			return "", true // treat nil and empty slices as equal
		}
		if a.Len() != b.Len() {
			return fmt.Sprintf("%s: length %d != %d", path, a.Len(), b.Len()), false
		}
		for i := range a.Len() {
			if diff, ok := diffEventValues(a.Index(i), b.Index(i), fmt.Sprintf("%s[%d]", path, i)); !ok {
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
			if diff, ok := diffEventValues(iter.Value(), bv, fmt.Sprintf("%s[%v]", path, iter.Key())); !ok {
				return diff, false
			}
		}
		return "", true
	case reflect.Struct:
		for i := range a.NumField() {
			if diff, ok := diffEventValues(a.Field(i), b.Field(i), path+"."+a.Type().Field(i).Name); !ok {
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
		if a.CanInterface() && b.CanInterface() && reflect.DeepEqual(a.Interface(), b.Interface()) {
			return "", true
		}
		return fmt.Sprintf("%s: values of kind %s differ", path, a.Kind()), false
	}
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
