package rpc

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// emptyEventService returns empty results from every list/index method — the
// shape the lake backend returns when nothing matches.
type emptyEventService struct{}

var _ eventrepo.EventService = (*emptyEventService)(nil)

func (emptyEventService) GetLatestIndex(context.Context, *grpc.SearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, sql.ErrNoRows
}
func (emptyEventService) GetLatestIndexAdvanced(context.Context, *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, sql.ErrNoRows
}
func (emptyEventService) ListIndexes(context.Context, int, *grpc.SearchOptions) ([]cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return nil, nil
}
func (emptyEventService) ListIndexesAdvanced(context.Context, int, *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return nil, nil
}
func (emptyEventService) GetCloudEventTypeSummariesAdvanced(context.Context, *grpc.AdvancedSearchOptions) ([]eventrepo.CloudEventTypeSummary, error) {
	return nil, nil
}
func (emptyEventService) GetCloudEventFromIndex(context.Context, *cloudevent.CloudEvent[eventrepo.ObjectInfo], string) (cloudevent.RawEvent, error) {
	return cloudevent.RawEvent{}, sql.ErrNoRows
}
func (emptyEventService) ListCloudEventsFromIndexes(context.Context, []cloudevent.CloudEvent[eventrepo.ObjectInfo], string) ([]cloudevent.RawEvent, error) {
	return nil, nil
}
func (emptyEventService) PresignBlobURL(context.Context, string) (string, error) { return "", nil }

// TestListCloudEventsFromIndex_RejectsTraversalKey proves a client-supplied
// index key containing a path-traversal sequence is rejected before it is
// dereferenced (CHD-22 defense-in-depth).
func TestListCloudEventsFromIndex_RejectsTraversalKey(t *testing.T) {
	s := NewServer(emptyEventService{})
	_, err := s.ListCloudEventsFromIndex(context.Background(), &grpc.ListCloudEventsFromKeysRequest{
		Indexes: []*grpc.CloudEventIndex{
			{Data: &grpc.ObjectInfo{Key: "cloudevent/../../etc/secret"}},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestListCloudEventsFromIndex_RejectsTooManyKeys bounds the request: an
// oversized index list would fan out into that many fetches from one call
// (SR-4). It must be rejected, not processed.
func TestListCloudEventsFromIndex_RejectsTooManyKeys(t *testing.T) {
	s := NewServer(emptyEventService{})
	idxs := make([]*grpc.CloudEventIndex, maxIndexKeysPerRequest+1)
	for i := range idxs {
		idxs[i] = &grpc.CloudEventIndex{Data: &grpc.ObjectInfo{Key: "cloudevent/blobs/x"}}
	}
	_, err := s.ListCloudEventsFromIndex(context.Background(), &grpc.ListCloudEventsFromKeysRequest{Indexes: idxs})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestListIndexes_EmptyReturnsNotFound pins the gRPC contract: an empty result
// is NotFound, not OK+empty (CHD-22). The lake backend
// returns an empty slice with no error, which silently broke clients expecting
// NotFound.
func TestListIndexes_EmptyReturnsNotFound(t *testing.T) {
	s := NewServer(emptyEventService{})
	_, err := s.ListIndexes(context.Background(), &grpc.ListIndexesRequest{
		Options: &grpc.SearchOptions{},
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err), "empty list result maps to NotFound")
}
