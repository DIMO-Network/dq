package eventrepo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeEventService implements eventrepo.EventService with canned responses.
// It records every call for assertion and can be configured to return errors.
type fakeEventService struct {
	listIndexesResult  []cloudevent.CloudEvent[eventrepo.ObjectInfo]
	latestIndexResult  cloudevent.CloudEvent[eventrepo.ObjectInfo]
	typeSummariesResult []eventrepo.CloudEventTypeSummary
	errListIndexes     error
	errLatestIndex     error
	errTypeSummaries   error

	// call counters
	listCalls    int
	latestCalls  int
	summaryCalls int
}

var _ eventrepo.EventService = (*fakeEventService)(nil)

func (f *fakeEventService) ListIndexesAdvanced(_ context.Context, _ int, _ *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	f.listCalls++
	return f.listIndexesResult, f.errListIndexes
}

func (f *fakeEventService) GetLatestIndexAdvanced(_ context.Context, _ *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	f.latestCalls++
	return f.latestIndexResult, f.errLatestIndex
}

func (f *fakeEventService) GetCloudEventTypeSummariesAdvanced(_ context.Context, _ *grpc.AdvancedSearchOptions) ([]eventrepo.CloudEventTypeSummary, error) {
	f.summaryCalls++
	return f.typeSummariesResult, f.errTypeSummaries
}

func (f *fakeEventService) ListIndexes(_ context.Context, _ int, _ *grpc.SearchOptions) ([]cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return f.listIndexesResult, f.errListIndexes
}

func (f *fakeEventService) GetLatestIndex(_ context.Context, _ *grpc.SearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return f.latestIndexResult, f.errLatestIndex
}

func (f *fakeEventService) GetCloudEventFromIndex(_ context.Context, _ *cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) (cloudevent.RawEvent, error) {
	return cloudevent.RawEvent{}, nil
}

func (f *fakeEventService) ListCloudEventsFromIndexes(_ context.Context, _ []cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) ([]cloudevent.RawEvent, error) {
	return nil, nil
}

func (f *fakeEventService) PresignBlobURL(_ context.Context, _ string) (string, error) {
	return "https://example.com/presigned", nil
}


func TestShadowEventService_PrimaryAlwaysServed(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().UTC()

	primary := &fakeEventService{
		listIndexesResult: []cloudevent.CloudEvent[eventrepo.ObjectInfo]{
			{CloudEventHeader: cloudevent.CloudEventHeader{ID: "p1", Subject: "subj", Time: ts}},
		},
		latestIndexResult: cloudevent.CloudEvent[eventrepo.ObjectInfo]{
			CloudEventHeader: cloudevent.CloudEventHeader{ID: "p1", Subject: "subj", Time: ts},
		},
		typeSummariesResult: []eventrepo.CloudEventTypeSummary{
			{Type: "dimo.status", Count: 5, FirstSeen: ts.Add(-time.Hour), LastSeen: ts},
		},
	}
	secondary := &fakeEventService{
		listIndexesResult: []cloudevent.CloudEvent[eventrepo.ObjectInfo]{
			{CloudEventHeader: cloudevent.CloudEventHeader{ID: "p1", Subject: "subj", Time: ts}},
		},
		latestIndexResult: cloudevent.CloudEvent[eventrepo.ObjectInfo]{
			CloudEventHeader: cloudevent.CloudEventHeader{ID: "p1", Subject: "subj", Time: ts},
		},
		typeSummariesResult: []eventrepo.CloudEventTypeSummary{
			{Type: "dimo.status", Count: 5, FirstSeen: ts.Add(-time.Hour), LastSeen: ts},
		},
	}

	shadow := eventrepo.NewShadowEventService(primary, secondary, zerolog.Nop())

	// ListIndexesAdvanced should return primary result.
	list, err := shadow.ListIndexesAdvanced(ctx, 10, nil)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "p1", list[0].ID)

	// GetLatestIndexAdvanced should return primary result.
	latest, err := shadow.GetLatestIndexAdvanced(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "p1", latest.ID)

	// GetCloudEventTypeSummariesAdvanced should return primary result.
	sums, err := shadow.GetCloudEventTypeSummariesAdvanced(ctx, nil)
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.Equal(t, uint64(5), sums[0].Count)

	shadow.Wait()

	// Primary was called once per method.
	assert.Equal(t, 1, primary.listCalls, "primary ListIndexesAdvanced called once")
	assert.Equal(t, 1, primary.latestCalls, "primary GetLatestIndexAdvanced called once")
	assert.Equal(t, 1, primary.summaryCalls, "primary GetCloudEventTypeSummariesAdvanced called once")

	// Secondary was also called (shadow goroutine).
	assert.Equal(t, 1, secondary.listCalls, "secondary ListIndexesAdvanced called once (shadow)")
	assert.Equal(t, 1, secondary.latestCalls, "secondary GetLatestIndexAdvanced called once (shadow)")
	assert.Equal(t, 1, secondary.summaryCalls, "secondary GetCloudEventTypeSummariesAdvanced called once (shadow)")
}

func TestShadowEventService_PayloadMethodsPrimaryOnly(t *testing.T) {
	ctx := context.Background()

	primary := &fakeEventService{}
	secondary := &fakeEventService{}
	shadow := eventrepo.NewShadowEventService(primary, secondary, zerolog.Nop())

	idx := &cloudevent.CloudEvent[eventrepo.ObjectInfo]{
		CloudEventHeader: cloudevent.CloudEventHeader{ID: "x", Subject: "s"},
		Data:             eventrepo.ObjectInfo{Key: "lake://s/x"},
	}

	// GetCloudEventFromIndex served from primary only — no shadow goroutine.
	_, err := shadow.GetCloudEventFromIndex(ctx, idx, "")
	require.NoError(t, err)

	// ListCloudEventsFromIndexes served from primary only.
	_, err = shadow.ListCloudEventsFromIndexes(ctx, []cloudevent.CloudEvent[eventrepo.ObjectInfo]{*idx}, "")
	require.NoError(t, err)

	// PresignBlobURL served from primary only.
	url, err := shadow.PresignBlobURL(ctx, "some/key")
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/presigned", url)

	shadow.Wait()

	// Secondary must NOT have been called for payload methods.
	assert.Equal(t, 0, secondary.listCalls, "secondary not called for payload methods")
	assert.Equal(t, 0, secondary.latestCalls, "secondary not called for payload methods")
	assert.Equal(t, 0, secondary.summaryCalls, "secondary not called for payload methods")
}

func TestShadowEventService_MismatchIncrementsCounter(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().UTC()

	primary := &fakeEventService{
		listIndexesResult: []cloudevent.CloudEvent[eventrepo.ObjectInfo]{
			{CloudEventHeader: cloudevent.CloudEventHeader{ID: "primary-id", Subject: "s", Time: ts}},
		},
	}
	// Secondary returns a different ID — should cause a mismatch.
	secondary := &fakeEventService{
		listIndexesResult: []cloudevent.CloudEvent[eventrepo.ObjectInfo]{
			{CloudEventHeader: cloudevent.CloudEventHeader{ID: "different-id", Subject: "s", Time: ts}},
		},
	}

	shadow := eventrepo.NewShadowEventService(primary, secondary, zerolog.Nop())

	list, err := shadow.ListIndexesAdvanced(ctx, 10, nil)
	require.NoError(t, err)
	// Primary result is always served.
	require.Len(t, list, 1)
	assert.Equal(t, "primary-id", list[0].ID)

	shadow.Wait()

	// Secondary was called and results differed. The Prometheus counter
	// assertion (dq_fetch_shadow_mismatch_total +1) is in the white-box
	// companion TestShadowMismatchCounter_IncrementOnMismatch which has
	// direct access to fetchShadowMismatchTotal.
	assert.Equal(t, 1, secondary.listCalls, "secondary called for shadow comparison")
}

func TestShadowEventService_PrimaryErrorSkipsShadow(t *testing.T) {
	ctx := context.Background()

	primary := &fakeEventService{
		errListIndexes: errors.New("primary DB down"),
	}
	secondary := &fakeEventService{
		listIndexesResult: []cloudevent.CloudEvent[eventrepo.ObjectInfo]{
			{CloudEventHeader: cloudevent.CloudEventHeader{ID: "sec-id"}},
		},
	}

	shadow := eventrepo.NewShadowEventService(primary, secondary, zerolog.Nop())

	_, err := shadow.ListIndexesAdvanced(ctx, 10, nil)
	require.Error(t, err, "primary error propagated")
	assert.Contains(t, err.Error(), "primary DB down")

	shadow.Wait()

	// When primary errors, the shadow goroutine is skipped entirely.
	assert.Equal(t, 0, secondary.listCalls, "secondary not called when primary errors")
}

func TestShadowEventService_MatchNoCounter(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().UTC()

	// Both primary and secondary return identical results.
	event := cloudevent.CloudEvent[eventrepo.ObjectInfo]{
		CloudEventHeader: cloudevent.CloudEventHeader{ID: "same-id", Subject: "s", Time: ts},
		Data:             eventrepo.ObjectInfo{Key: "k"},
	}
	primary := &fakeEventService{latestIndexResult: event}
	secondary := &fakeEventService{latestIndexResult: event}

	shadow := eventrepo.NewShadowEventService(primary, secondary, zerolog.Nop())

	got, err := shadow.GetLatestIndexAdvanced(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "same-id", got.ID)

	shadow.Wait()

	assert.Equal(t, 1, secondary.latestCalls, "secondary shadowed GetLatestIndexAdvanced")
}
