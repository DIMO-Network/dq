package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ListCloudEventsFromIndexes must return one payload per requested index in the
// exact input order, even when the indexes span multiple subjects — the SR-4
// batching groups ids by subject into one query per subject, so order
// reassembly is the property that can regress.
func TestLakeEventService_ListCloudEventsFromIndexes_OrderAcrossSubjects(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	subjA := lakeRawSubj
	subjB := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:202"

	a1 := mkStoredEvent("a1", "dimo.status", subjA, now.Add(-3*time.Hour))
	a2 := mkStoredEvent("a2", "dimo.status", subjA, now.Add(-1*time.Hour))
	b1 := mkStoredEvent("b1", "dimo.status", subjB, now.Add(-2*time.Hour))
	for _, e := range []cloudevent.StoredEvent{a1, b1, a2} {
		insertRawEvent(t, svc, e)
	}

	idx := func(ev cloudevent.StoredEvent) cloudevent.CloudEvent[eventrepo.ObjectInfo] {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{CloudEventHeader: ev.CloudEventHeader}
	}
	in := []cloudevent.CloudEvent[eventrepo.ObjectInfo]{idx(a1), idx(b1), idx(a2)}

	out, err := lsvc.ListCloudEventsFromIndexes(ctx, in)
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, "a1", out[0].ID, "input order preserved across subjects")
	assert.Equal(t, "b1", out[1].ID)
	assert.Equal(t, "a2", out[2].ID)
}

// A requested index with no matching row yields ErrNotFound (unchanged from the
// per-index path).
func TestLakeEventService_ListCloudEventsFromIndexes_MissingIsNotFound(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	a1 := mkStoredEvent("a1", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	insertRawEvent(t, svc, a1)

	idx := func(subject, id string) cloudevent.CloudEvent[eventrepo.ObjectInfo] {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{
			CloudEventHeader: cloudevent.CloudEventHeader{Subject: subject, ID: id},
		}
	}
	in := []cloudevent.CloudEvent[eventrepo.ObjectInfo]{idx(lakeRawSubj, "a1"), idx(lakeRawSubj, "does-not-exist")}

	_, err := lsvc.ListCloudEventsFromIndexes(ctx, in)
	require.ErrorIs(t, err, ErrNotFound)
}
