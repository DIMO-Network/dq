package duck

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mkTypeVariant builds the row din writes when one upload carries two payloads:
// same subject, same id, same instant, different type. The payload embeds the
// type so a test can tell which variant it got back.
func mkTypeVariant(id, ceType, subject string, ts time.Time) cloudevent.StoredEvent {
	ev := mkStoredEvent(id, ceType, subject, ts)
	ev.Data = json.RawMessage(fmt.Sprintf(`{"variant":%q}`, ceType))
	return ev
}

func idxOf(ev cloudevent.StoredEvent) cloudevent.CloudEvent[eventrepo.ObjectInfo] {
	return cloudevent.CloudEvent[eventrepo.ObjectInfo]{CloudEventHeader: ev.CloudEventHeader}
}

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

// One upload emits several raw_events rows sharing a cloudevent id under
// different types, so (subject, id) does not identify one event. Both variants
// must come back, each carrying its OWN payload — keying the resolution map on
// (subject, id) collapsed them onto whichever row the query happened to yield
// last, so one of the two got the other's payload (#50).
func TestLakeEventService_ListCloudEventsFromIndexes_SameIDAcrossTypes(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	status := mkTypeVariant("shared-id", "dimo.status", lakeRawSubj, now)
	finger := mkTypeVariant("shared-id", "dimo.fingerprint", lakeRawSubj, now)
	insertRawEvent(t, svc, status)
	insertRawEvent(t, svc, finger)

	out, err := lsvc.ListCloudEventsFromIndexes(ctx, []cloudevent.CloudEvent[eventrepo.ObjectInfo]{idxOf(status), idxOf(finger)})
	require.NoError(t, err)
	require.Len(t, out, 2, "neither type variant is dropped")

	assert.Equal(t, "dimo.status", out[0].Type)
	assert.JSONEq(t, `{"variant":"dimo.status"}`, string(out[0].Data), "each variant keeps its own payload")
	assert.Equal(t, "dimo.fingerprint", out[1].Type)
	assert.JSONEq(t, `{"variant":"dimo.fingerprint"}`, string(out[1].Data))
}

// The production failure: a page of N indexes whose ids expand to more than N
// rows. The re-read's LIMIT used to be len(ids), so the expansion pushed
// requested rows past the cut and the id vanished — failing the ENTIRE batch
// with ErrNotFound, not just the one event.
//
// Requesting the fingerprint variants makes the drop deterministic: every id's
// status row sorts ahead of its fingerprint row, so a len(ids) LIMIT fills up
// entirely with rows nobody asked for.
func TestLakeEventService_ListCloudEventsFromIndexes_IDExpansionDoesNotTruncate(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	const n = 40
	want := make([]cloudevent.CloudEvent[eventrepo.ObjectInfo], 0, n)
	for i := range n {
		id := fmt.Sprintf("id-%02d", i)
		ts := now.Add(-time.Duration(i) * time.Minute)
		insertRawEvent(t, svc, mkTypeVariant(id, "dimo.status", lakeRawSubj, ts))
		finger := mkTypeVariant(id, "dimo.fingerprint", lakeRawSubj, ts)
		insertRawEvent(t, svc, finger)
		want = append(want, idxOf(finger))
	}

	out, err := lsvc.ListCloudEventsFromIndexes(ctx, want)
	require.NoError(t, err, "no requested id may be truncated out of the re-read")
	require.Len(t, out, n)
	for i, ev := range out {
		assert.Equal(t, "dimo.fingerprint", ev.Type, "index %d resolved to the wrong type variant", i)
		assert.Equal(t, want[i].ID, ev.ID, "output stays in input order")
	}
}

// A page may list the same (subject, id) twice — once per type variant — so the
// deduped IN-list must not shorten or reorder the result.
func TestLakeEventService_ListCloudEventsFromIndexes_RepeatedIDs(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	status := mkTypeVariant("dup-id", "dimo.status", lakeRawSubj, now)
	finger := mkTypeVariant("dup-id", "dimo.fingerprint", lakeRawSubj, now)
	insertRawEvent(t, svc, status)
	insertRawEvent(t, svc, finger)

	in := []cloudevent.CloudEvent[eventrepo.ObjectInfo]{idxOf(status), idxOf(finger), idxOf(status)}
	out, err := lsvc.ListCloudEventsFromIndexes(ctx, in)
	require.NoError(t, err)
	require.Len(t, out, len(in), "same length as the input even when ids repeat")
	assert.Equal(t, []string{"dimo.status", "dimo.fingerprint", "dimo.status"},
		[]string{out[0].Type, out[1].Type, out[2].Type})
}

// GetCloudEventFromIndex must return the row its index header names. The old
// LIMIT 1 re-read picked arbitrarily among an id's type variants, so the header
// came from the list query and the payload from an unrelated row — currently
// masked by din writing identical payloads across variants, but nothing
// enforces that (#50).
func TestLakeEventService_GetCloudEventFromIndex_PicksHeaderMatch(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	status := mkTypeVariant("one-id", "dimo.status", lakeRawSubj, now)
	finger := mkTypeVariant("one-id", "dimo.fingerprint", lakeRawSubj, now)
	insertRawEvent(t, svc, status)
	insertRawEvent(t, svc, finger)

	for _, ev := range []cloudevent.StoredEvent{status, finger} {
		idx := idxOf(ev)
		got, err := lsvc.GetCloudEventFromIndex(ctx, &idx)
		require.NoError(t, err)
		assert.Equal(t, ev.Type, got.Type)
		assert.JSONEq(t, string(ev.Data), string(got.Data))
	}
}

// The gRPC ListCloudEventsFromIndex path lets a caller supply an index with only
// subject+id and no time/type/source, which the header key cannot match. Those
// must still resolve — to the newest row for the id, as before.
func TestLakeEventService_ListCloudEventsFromIndexes_PartialHeaderFallback(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	older := mkTypeVariant("part-id", "dimo.fingerprint", lakeRawSubj, now.Add(-time.Hour))
	newer := mkTypeVariant("part-id", "dimo.status", lakeRawSubj, now)
	insertRawEvent(t, svc, older)
	insertRawEvent(t, svc, newer)

	partial := cloudevent.CloudEvent[eventrepo.ObjectInfo]{
		CloudEventHeader: cloudevent.CloudEventHeader{Subject: lakeRawSubj, ID: "part-id"},
	}
	out, err := lsvc.ListCloudEventsFromIndexes(ctx, []cloudevent.CloudEvent[eventrepo.ObjectInfo]{partial})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "dimo.status", out[0].Type, "partial header falls back to the newest row for the id")
}
