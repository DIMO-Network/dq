package fetch

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/stretchr/testify/require"
)

// recordingService counts which payload path ListCloudEventsFromIndexes takes.
// The embedded nil interface satisfies EventService; only the methods the two
// paths actually call are overridden.
type recordingService struct {
	eventrepo.EventService
	batches     bool
	listCalls   atomic.Int64 // the non-batching path fans GetCloudEventFromIndex out concurrently
	perKeyCalls atomic.Int64
}

func (r *recordingService) BatchesAllIndexes() bool { return r.batches }

func (r *recordingService) ListCloudEventsFromIndexes(_ context.Context, indexes []cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) ([]cloudevent.RawEvent, error) {
	r.listCalls.Add(1)
	return make([]cloudevent.RawEvent, len(indexes)), nil
}

func (r *recordingService) GetCloudEventFromIndex(_ context.Context, _ *cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) (cloudevent.RawEvent, error) {
	r.perKeyCalls.Add(1)
	return cloudevent.RawEvent{}, nil
}

// A batching backend (the lake) resolves every index in one call; a non-batching
// backend falls back to one query per key.
func TestListCloudEventsFromIndexes_Routing(t *testing.T) {
	idx := make([]cloudevent.CloudEvent[eventrepo.ObjectInfo], 5)
	for i := range idx {
		idx[i].Subject = "did:1"
		idx[i].ID = fmt.Sprint(i)
		idx[i].Data.Key = "lake://did:1/" + fmt.Sprint(i) // not a parquet ref
	}

	batched := &recordingService{batches: true}
	_, err := ListCloudEventsFromIndexes(context.Background(), batched, idx, []string{"b"})
	require.NoError(t, err)
	require.Equal(t, int64(1), batched.listCalls.Load(), "a batching backend resolves all indexes in one call")
	require.Equal(t, int64(0), batched.perKeyCalls.Load(), "no per-key fallback for a batching backend")

	perKey := &recordingService{batches: false}
	_, err = ListCloudEventsFromIndexes(context.Background(), perKey, idx, []string{"b"})
	require.NoError(t, err)
	require.Equal(t, int64(0), perKey.listCalls.Load(), "non-parquet keys don't hit the batched path on a non-batching backend")
	require.Equal(t, int64(len(idx)), perKey.perKeyCalls.Load(), "a non-batching backend fetches one key at a time")
}
