package eventrepo

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// counterFor returns the current value of dq_fetch_shadow_mismatch_total for
// the given method label, or 0 if the label combination has not been observed.
func counterFor(method string) float64 {
	c, err := fetchShadowMismatchTotal.GetMetricWithLabelValues(method)
	if err != nil {
		return 0
	}
	return testutil.ToFloat64(c)
}

// fakeWhiteBoxService is a minimal EventService for white-box counter tests.
// It is separate from the external fakeEventService to avoid a naming clash.
type fakeWhiteBoxService struct {
	listResult []cloudevent.CloudEvent[ObjectInfo]
	listErr    error
	latestResult cloudevent.CloudEvent[ObjectInfo]
	latestErr  error
}

var _ EventService = (*fakeWhiteBoxService)(nil)

func (f *fakeWhiteBoxService) ListIndexesAdvanced(_ context.Context, _ int, _ *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error) {
	return f.listResult, f.listErr
}

func (f *fakeWhiteBoxService) GetLatestIndexAdvanced(_ context.Context, _ *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[ObjectInfo], error) {
	return f.latestResult, f.latestErr
}

func (f *fakeWhiteBoxService) GetCloudEventTypeSummariesAdvanced(_ context.Context, _ *grpc.AdvancedSearchOptions) ([]CloudEventTypeSummary, error) {
	return nil, nil
}

func (f *fakeWhiteBoxService) ListIndexes(_ context.Context, _ int, _ *grpc.SearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error) {
	return f.listResult, f.listErr
}

func (f *fakeWhiteBoxService) GetLatestIndex(_ context.Context, _ *grpc.SearchOptions) (cloudevent.CloudEvent[ObjectInfo], error) {
	return f.latestResult, f.latestErr
}

func (f *fakeWhiteBoxService) GetCloudEventFromIndex(_ context.Context, _ *cloudevent.CloudEvent[ObjectInfo], _ string) (cloudevent.RawEvent, error) {
	return cloudevent.RawEvent{}, nil
}

func (f *fakeWhiteBoxService) ListCloudEventsFromIndexes(_ context.Context, _ []cloudevent.CloudEvent[ObjectInfo], _ string) ([]cloudevent.RawEvent, error) {
	return nil, nil
}

func (f *fakeWhiteBoxService) PresignBlobURL(_ context.Context, _ string) (string, error) {
	return "", nil
}

// TestShadowMismatchCounter_IncrementOnMismatch asserts that
// dq_fetch_shadow_mismatch_total{method="ListIndexesAdvanced"} increments by
// exactly 1 when primary and secondary return different results, and does NOT
// increment when results match.
func TestShadowMismatchCounter_IncrementOnMismatch(t *testing.T) {
	const method = "ListIndexesAdvanced"
	ts := time.Now().UTC()

	primary := &fakeWhiteBoxService{
		listResult: []cloudevent.CloudEvent[ObjectInfo]{
			{CloudEventHeader: cloudevent.CloudEventHeader{ID: "primary-id", Subject: "s", Time: ts}},
		},
	}
	secondary := &fakeWhiteBoxService{
		listResult: []cloudevent.CloudEvent[ObjectInfo]{
			{CloudEventHeader: cloudevent.CloudEventHeader{ID: "different-id", Subject: "s", Time: ts}},
		},
	}

	shadow := NewShadowEventService(primary, secondary, zerolog.Nop())

	before := counterFor(method)

	list, err := shadow.ListIndexesAdvanced(context.Background(), 10, nil)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "primary-id", list[0].ID)

	shadow.Wait()

	assert.Equal(t, before+1, counterFor(method), "mismatch counter must increment by 1")
}

// TestShadowMismatchCounter_NoIncrementOnMatch asserts that the counter does
// NOT increment when primary and secondary return identical results.
func TestShadowMismatchCounter_NoIncrementOnMatch(t *testing.T) {
	const method = "GetLatestIndexAdvanced"
	ts := time.Now().UTC()

	event := cloudevent.CloudEvent[ObjectInfo]{
		CloudEventHeader: cloudevent.CloudEventHeader{ID: "same-id", Subject: "s", Time: ts},
		Data:             ObjectInfo{Key: "k"},
	}

	primary := &fakeWhiteBoxService{latestResult: event}
	secondary := &fakeWhiteBoxService{latestResult: event}

	shadow := NewShadowEventService(primary, secondary, zerolog.Nop())

	before := counterFor(method)

	got, err := shadow.GetLatestIndexAdvanced(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "same-id", got.ID)

	shadow.Wait()

	assert.Equal(t, before, counterFor(method), "mismatch counter must not increment on match")
}
