package fetch

import (
	"context"
	"fmt"
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
	listCalls   int
	perKeyCalls int
}

func (r *recordingService) BatchesAllIndexes() bool { return r.batches }

func (r *recordingService) ListCloudEventsFromIndexes(_ context.Context, indexes []cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) ([]cloudevent.RawEvent, error) {
	r.listCalls++
	return make([]cloudevent.RawEvent, len(indexes)), nil
}

func (r *recordingService) GetCloudEventFromIndex(_ context.Context, _ *cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) (cloudevent.RawEvent, error) {
	r.perKeyCalls++
	return cloudevent.RawEvent{}, nil
}

// A batching backend (the lake) resolves every index in one call; a non-batching
// backend (ClickHouse) falls back to one query per key.
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
	require.Equal(t, 1, batched.listCalls, "a batching backend resolves all indexes in one call")
	require.Equal(t, 0, batched.perKeyCalls, "no per-key fallback for a batching backend")

	perKey := &recordingService{batches: false}
	_, err = ListCloudEventsFromIndexes(context.Background(), perKey, idx, []string{"b"})
	require.NoError(t, err)
	require.Equal(t, 0, perKey.listCalls, "non-parquet keys don't hit the batched path on a non-batching backend")
	require.Equal(t, len(idx), perKey.perKeyCalls, "a non-batching backend fetches one key at a time")
}
