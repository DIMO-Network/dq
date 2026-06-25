package duck

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// fakeBlobGetter is a minimal eventrepo.ObjectGetter for blob-fetch tests: it
// serves byte payloads from an in-memory map keyed by S3 object key.
type fakeBlobGetter struct {
	objects map[string][]byte
}

func (f *fakeBlobGetter) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	b, ok := f.objects[*in.Key]
	if !ok {
		return nil, fmt.Errorf("no such key: %s", *in.Key)
	}
	n := int64(len(b))
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b)), ContentLength: &n}, nil
}

// newLakeEventServiceForTest opens a DuckLake file catalog, creates the
// lake.raw_events table, and returns a LakeEventService ready for testing.
func newLakeEventServiceForTest(t *testing.T) (*LakeEventService, *Service) {
	t.Helper()
	svc := newLakeServiceForTest(t) // creates lake-attached Service

	_, err := svc.db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS lake.raw_events (
			subject          VARCHAR,
			time             TIMESTAMPTZ,
			type             VARCHAR,
			id               VARCHAR,
			source           VARCHAR,
			producer         VARCHAR,
			data_content_type VARCHAR,
			data_version     VARCHAR,
			extras           VARCHAR,
			data             VARCHAR,
			data_base64      BLOB,
			data_index_key   VARCHAR,
			voids_id         VARCHAR
		)`)
	require.NoError(t, err)

	return NewLakeEventService(svc, nil, nil, ""), svc
}

// lakeRawSubj is the test subject DID used across lake-fetch tests.
const lakeRawSubj = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:101"

// insertRawEvent inserts a row directly into lake.raw_events.
func insertRawEvent(t *testing.T, svc *Service, ev cloudevent.StoredEvent) {
	t.Helper()
	var extrasJSON *string
	if len(ev.Extras) > 0 || ev.Signature != "" || len(ev.Tags) > 0 {
		// Store non-column fields (signature, tags) into extras before serialising,
		// mirroring cloudevent.StoreNonColumnFields.
		m := make(map[string]any, len(ev.Extras))
		for k, v := range ev.Extras {
			m[k] = v
		}
		if ev.Signature != "" {
			m["signature"] = ev.Signature
		}
		if len(ev.Tags) > 0 {
			m["tags"] = ev.Tags
		}
		b, err := json.Marshal(m)
		require.NoError(t, err)
		s := string(b)
		extrasJSON = &s
	}

	var dataStr *string
	if len(ev.Data) > 0 {
		s := string(ev.Data)
		dataStr = &s
	}

	var dataBase64 []byte
	if ev.DataBase64 != "" {
		dataBase64 = []byte(ev.DataBase64)
	}

	var dataIndexKey *string
	if ev.DataIndexKey != "" {
		dataIndexKey = &ev.DataIndexKey
	}

	var voidsID *string
	if ev.VoidsID != "" {
		voidsID = &ev.VoidsID
	}

	_, err := svc.db.ExecContext(context.Background(),
		`INSERT INTO lake.raw_events
			(subject, time, type, id, source, producer, data_content_type, data_version,
			 extras, data, data_base64, data_index_key, voids_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.Subject, ev.Time.UTC(),
		ev.Type, ev.ID, ev.Source, ev.Producer,
		ev.DataContentType, ev.DataVersion,
		extrasJSON, dataStr, dataBase64, dataIndexKey, voidsID)
	require.NoError(t, err)
}

// mkStoredEvent builds a minimal StoredEvent for test insertion.
func mkStoredEvent(id, ceType, subject string, ts time.Time) cloudevent.StoredEvent {
	return cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion,
				Type:        ceType,
				Subject:     subject,
				Source:      "src-test",
				Producer:    subject,
				ID:          id,
				Time:        ts,
			},
			Data: json.RawMessage(`{"v":1}`),
		},
	}
}

// TestLakeEventService_ListIndexesAdvanced verifies newest-first ordering,
// deduplication, and that voided events and tombstones are excluded.
func TestLakeEventService_ListIndexesAdvanced(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	// Two event types.
	ev1 := mkStoredEvent("ev-status-1", "dimo.status", lakeRawSubj, now.Add(-3*time.Hour))
	ev2 := mkStoredEvent("ev-status-2", "dimo.status", lakeRawSubj, now.Add(-2*time.Hour))
	ev3 := mkStoredEvent("ev-fp-1", "dimo.fingerprint", lakeRawSubj, now.Add(-1*time.Hour))

	// ev4 will be voided by tombstone ev5.
	ev4 := mkStoredEvent("ev-to-void", "dimo.status", lakeRawSubj, now.Add(-90*time.Minute))
	ev5 := cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion,
				Type:        "dimo.status.tombstone",
				Subject:     lakeRawSubj,
				Source:      "src-test",
				ID:          "tombstone-1",
				Time:        now.Add(-80 * time.Minute),
			},
		},
		VoidsID: "ev-to-void",
	}

	for _, e := range []cloudevent.StoredEvent{ev1, ev2, ev3, ev4, ev5} {
		insertRawEvent(t, svc, e)
	}

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	}

	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)

	// Expected: ev3 (newest), ev2, ev1 — ev4 voided, ev5 tombstone excluded.
	require.Len(t, indexes, 3, "voided event and tombstone must be excluded")
	assert.Equal(t, "ev-fp-1", indexes[0].ID, "newest first")
	assert.Equal(t, "ev-status-2", indexes[1].ID)
	assert.Equal(t, "ev-status-1", indexes[2].ID)

	for _, idx := range indexes {
		assert.NotEmpty(t, idx.Data.Key, "ObjectInfo.Key must be set")
	}
}

// TestLakeEventService_DedupOnKey verifies that duplicate rows collapse on
// the header key (subject+time+type+source+id).
func TestLakeEventService_DedupOnKey(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	dup := mkStoredEvent("dup-1", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	// Insert the same event twice (simulates at-least-once ingest).
	insertRawEvent(t, svc, dup)
	insertRawEvent(t, svc, dup)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "duplicate rows collapse on header key")
}

// TestLakeEventService_GetLatestIndexAdvanced verifies newest-non-voided return
// and ErrNotFound on empty.
func TestLakeEventService_GetLatestIndexAdvanced(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	older := mkStoredEvent("ev-older", "dimo.status", lakeRawSubj, now.Add(-2*time.Hour))
	newer := mkStoredEvent("ev-newer", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	insertRawEvent(t, svc, older)
	insertRawEvent(t, svc, newer)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	}
	idx, err := lsvc.GetLatestIndexAdvanced(ctx, opts)
	require.NoError(t, err)
	assert.Equal(t, "ev-newer", idx.ID, "latest must be newest non-voided event")

	// Empty subject returns ErrNotFound.
	emptyOpts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{"did:nobody"}},
	}
	_, err = lsvc.GetLatestIndexAdvanced(ctx, emptyOpts)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound), "expected ErrNotFound, got: %v", err)
}

// TestLakeEventService_GetCloudEventTypeSummariesAdvanced verifies per-type
// aggregation, excluding voided events.
func TestLakeEventService_GetCloudEventTypeSummariesAdvanced(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	// 2 status events, 1 fingerprint. One status (ev-void) will be voided.
	ev1 := mkStoredEvent("ev-s1", "dimo.status", lakeRawSubj, now.Add(-4*time.Hour))
	ev2 := mkStoredEvent("ev-s2", "dimo.status", lakeRawSubj, now.Add(-3*time.Hour))
	evVoid := mkStoredEvent("ev-void", "dimo.status", lakeRawSubj, now.Add(-2*time.Hour))
	evFP := mkStoredEvent("ev-fp", "dimo.fingerprint", lakeRawSubj, now.Add(-time.Hour))
	tomb := cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion,
				Type:        "dimo.status.tombstone",
				Subject:     lakeRawSubj,
				Source:      "src-test",
				ID:          "tomb-2",
				Time:        now.Add(-90 * time.Minute),
			},
		},
		VoidsID: "ev-void",
	}

	for _, e := range []cloudevent.StoredEvent{ev1, ev2, evVoid, evFP, tomb} {
		insertRawEvent(t, svc, e)
	}

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	}
	summaries, err := lsvc.GetCloudEventTypeSummariesAdvanced(ctx, opts)
	require.NoError(t, err)

	// Only dimo.status (2, excluding voided) and dimo.fingerprint (1).
	// tombstone type itself is also excluded (it's a tombstone with voids_id set).
	require.Len(t, summaries, 2, "tombstone type excluded; voided event excluded from count")

	fpIdx := -1
	stIdx := -1
	for i, s := range summaries {
		switch s.Type {
		case "dimo.fingerprint":
			fpIdx = i
		case "dimo.status":
			stIdx = i
		}
	}
	require.NotEqual(t, -1, stIdx, "dimo.status must appear")
	require.NotEqual(t, -1, fpIdx, "dimo.fingerprint must appear")

	assert.EqualValues(t, 2, summaries[stIdx].Count, "voided event excluded from count")
	assert.EqualValues(t, 1, summaries[fpIdx].Count)
	assert.False(t, summaries[stIdx].FirstSeen.IsZero())
	assert.False(t, summaries[stIdx].LastSeen.IsZero())
}

// TestLakeEventService_DataVersionFilter verifies DataVersion narrowing.
func TestLakeEventService_DataVersionFilter(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	ev1 := mkStoredEvent("ev-v1", "dimo.status", lakeRawSubj, now.Add(-2*time.Hour))
	ev1.DataVersion = "1.0"
	ev2 := mkStoredEvent("ev-v2", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	ev2.DataVersion = "2.0"
	insertRawEvent(t, svc, ev1)
	insertRawEvent(t, svc, ev2)

	opts := &grpc.AdvancedSearchOptions{
		Subject:     &grpc.StringFilterOption{In: []string{lakeRawSubj}},
		DataVersion: &grpc.StringFilterOption{In: []string{"1.0"}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1)
	assert.Equal(t, "ev-v1", indexes[0].ID)
}

// TestLakeEventService_TagsFilter verifies that tags stored in extras.tags
// are correctly matched via list_has_any.
func TestLakeEventService_TagsFilter(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	evTagged := mkStoredEvent("ev-tagged", "dimo.status", lakeRawSubj, now.Add(-2*time.Hour))
	evTagged.Tags = []string{"trip", "safety"}
	evUntagged := mkStoredEvent("ev-untagged", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	insertRawEvent(t, svc, evTagged)
	insertRawEvent(t, svc, evUntagged)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
		Tags:    &grpc.ArrayFilterOption{ContainsAny: []string{"safety"}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "only tagged event matches")
	assert.Equal(t, "ev-tagged", indexes[0].ID)
}

// TestLakeEventService_GetCloudEventFromIndex verifies inline-data payload
// resolution.
func TestLakeEventService_GetCloudEventFromIndex(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	ev := mkStoredEvent("ev-inline", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	ev.Data = json.RawMessage(`{"speed":42}`)
	insertRawEvent(t, svc, ev)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 1, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1)

	raw, err := lsvc.GetCloudEventFromIndex(ctx, &indexes[0])
	require.NoError(t, err)
	assert.JSONEq(t, `{"speed":42}`, string(raw.Data))
	assert.Equal(t, "ev-inline", raw.ID)
}

// TestLakeEventService_BlobIndexKey verifies that the ObjectInfo.Key is set
// to the blob key (data_index_key) when the event references a large payload.
func TestLakeEventService_BlobIndexKey(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	blobKey := eventrepo.BlobKeyPrefix + "test-subject/2026/06/blob1"
	ev := mkStoredEvent("ev-blob", "dimo.attestation", lakeRawSubj, now.Add(-time.Hour))
	ev.DataIndexKey = blobKey
	ev.Data = nil // large payload, data is in S3
	insertRawEvent(t, svc, ev)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 1, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1)

	// The ObjectInfo.Key must be the blob key so the resolver can presign it.
	assert.Equal(t, blobKey, indexes[0].Data.Key,
		"blob event: ObjectInfo.Key must be the data_index_key for presign routing")
}

// TestLakeEventService_VoidingExcludes verifies that a voided event and its
// tombstone are both absent from all index/summary results.
func TestLakeEventService_VoidingExcludes(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	good := mkStoredEvent("ev-good", "dimo.status", lakeRawSubj, now.Add(-2*time.Hour))
	voided := mkStoredEvent("ev-voided", "dimo.status", lakeRawSubj, now.Add(-1*time.Hour))
	tomb := cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion,
				Type:        "dimo.tombstone",
				Subject:     lakeRawSubj,
				Source:      "src-test",
				ID:          "tomb-void",
				Time:        now.Add(-30 * time.Minute),
			},
		},
		VoidsID: "ev-voided",
	}

	for _, e := range []cloudevent.StoredEvent{good, voided, tomb} {
		insertRawEvent(t, svc, e)
	}

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	}

	// ListIndexesAdvanced: only good event visible.
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	ids := make([]string, len(indexes))
	for i, idx := range indexes {
		ids[i] = idx.ID
	}
	assert.Equal(t, []string{"ev-good"}, ids, "voided + tombstone excluded from list")

	// GetLatestIndexAdvanced: returns the good event, not the (newer) voided one.
	latest, err := lsvc.GetLatestIndexAdvanced(ctx, opts)
	require.NoError(t, err)
	assert.Equal(t, "ev-good", latest.ID)

	// Type summaries: count excludes voided.
	summaries, err := lsvc.GetCloudEventTypeSummariesAdvanced(ctx, opts)
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	assert.Equal(t, "dimo.status", summaries[0].Type)
	assert.EqualValues(t, 1, summaries[0].Count)
}

// TestQueryLakeRaw_BeforeOnlyAnchorsLookbackFloor pins the default-window guard:
// a subject-less, id-less search bounded only by an old Before must anchor its
// lookback floor to Before, not to now. Before the fix the floor was now-window
// (later than Before), so the window was empty and the query silently returned
// nothing.
func TestQueryLakeRaw_BeforeOnlyAnchorsLookbackFloor(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)

	old := time.Now().UTC().Add(-2 * defaultFetchScanWindow).Truncate(time.Millisecond)
	insertRawEvent(t, svc, mkStoredEvent("old-1", "dimo.status", lakeRawSubj, old))

	// Upper bound just after the event; no subject, no ids → only the default
	// window guard applies. The floor must follow Before so the event is in range.
	evs, err := lsvc.queryLakeRaw(ctx, RawFilter{Before: old.Add(time.Hour), ExcludeVoided: true}, 10)
	require.NoError(t, err)
	require.Len(t, evs, 1, "event before the requested upper bound must be returned, not clamped out")
	assert.Equal(t, "old-1", evs[0].ID)
}

// TestLakeEventService_GetCloudEventFromIndex_ErrNotFound checks that
// fetching by a non-existent ID returns ErrNotFound.
func TestLakeEventService_GetCloudEventFromIndex_ErrNotFound(t *testing.T) {
	ctx := context.Background()
	lsvc, _ := newLakeEventServiceForTest(t)

	ghost := &cloudevent.CloudEvent[eventrepo.ObjectInfo]{
		CloudEventHeader: cloudevent.CloudEventHeader{
			Subject: lakeRawSubj,
			ID:      "does-not-exist",
		},
		Data: eventrepo.ObjectInfo{Key: "lake://" + lakeRawSubj + "/does-not-exist"},
	}
	_, err := lsvc.GetCloudEventFromIndex(ctx, ghost)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
}

// --- Fix 1 tests ---

// TestErrNotFoundWrapsSQLErrNoRows verifies Fix 1: ErrNotFound wraps
// sql.ErrNoRows so that the gRPC layer can map it to codes.NotFound.
func TestErrNotFoundWrapsSQLErrNoRows(t *testing.T) {
	require.ErrorIs(t, ErrNotFound, sql.ErrNoRows,
		"ErrNotFound must wrap sql.ErrNoRows for gRPC NotFound mapping")
}

// TestGetLatestIndexAdvanced_ErrNotFound_WrapsErrNoRows verifies that an empty
// subject triggers ErrNotFound that satisfies both errors.Is(err, ErrNotFound)
// AND errors.Is(err, sql.ErrNoRows).
func TestGetLatestIndexAdvanced_ErrNotFound_WrapsErrNoRows(t *testing.T) {
	ctx := context.Background()
	lsvc, _ := newLakeEventServiceForTest(t)

	_, err := lsvc.GetLatestIndexAdvanced(ctx, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{"did:nobody"}},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound, "empty subject must return ErrNotFound")
	require.ErrorIs(t, err, sql.ErrNoRows,
		"ErrNotFound must satisfy errors.Is(err, sql.ErrNoRows) for gRPC layer")
}

// timestampProto converts a time.Time to a *timestamppb.Timestamp for test helpers.
func timestampProto(_ *testing.T, ts time.Time) *timestamppb.Timestamp {
	return timestamppb.New(ts)
}

// --- Fix 2 tests ---

// TestAfterBoundaryIsStrict verifies Fix 2: an event whose timestamp equals
// filter.After is excluded (strict >).
func TestAfterBoundaryIsStrict(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	// ev-exact: timestamp == After boundary → must NOT appear.
	// ev-after: timestamp is 1ms after boundary → must appear.
	boundary := now.Add(-time.Hour)
	evExact := mkStoredEvent("ev-at-boundary", "dimo.status", lakeRawSubj, boundary)
	evAfter := mkStoredEvent("ev-after-boundary", "dimo.status", lakeRawSubj, boundary.Add(time.Millisecond))
	insertRawEvent(t, svc, evExact)
	insertRawEvent(t, svc, evAfter)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
		After:   timestampProto(t, boundary),
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "event at boundary must be excluded (strict >)")
	assert.Equal(t, "ev-after-boundary", indexes[0].ID)
}

// --- Fix 3 tests ---

// TestMultiSubjectIN verifies that multiple subjects in Subject.In are all
// returned (multi-value IN, not just In[0]).
func TestMultiSubjectIN(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	subj1 := "did:erc721:137:0xAAA:1"
	subj2 := "did:erc721:137:0xBBB:2"
	subj3 := "did:erc721:137:0xCCC:3"

	insertRawEvent(t, svc, mkStoredEvent("ev-s1", "dimo.status", subj1, now.Add(-3*time.Hour)))
	insertRawEvent(t, svc, mkStoredEvent("ev-s2", "dimo.status", subj2, now.Add(-2*time.Hour)))
	insertRawEvent(t, svc, mkStoredEvent("ev-s3", "dimo.status", subj3, now.Add(-time.Hour)))

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subj1, subj2}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 2, "multi-subject IN must return all matching subjects")
	ids := []string{indexes[0].ID, indexes[1].ID}
	assert.ElementsMatch(t, []string{"ev-s1", "ev-s2"}, ids)
}

// TestStringNotIn verifies that NotIn on a string field (Type used as example)
// correctly excludes matching events.
func TestStringNotIn(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	subj := "did:erc721:137:0xNOTIN:1"
	insertRawEvent(t, svc, mkStoredEvent("ev-status", "dimo.status", subj, now.Add(-2*time.Hour)))
	insertRawEvent(t, svc, mkStoredEvent("ev-fp", "dimo.fingerprint", subj, now.Add(-time.Hour)))

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subj}},
		Type:    &grpc.StringFilterOption{NotIn: []string{"dimo.status"}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "NotIn must exclude dimo.status")
	assert.Equal(t, "ev-fp", indexes[0].ID)
}

// TestTagsContainsAll verifies that ContainsAll requires all tags to be present.
func TestTagsContainsAll(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	subj := "did:erc721:137:0xTAGS:1"
	evBoth := mkStoredEvent("ev-both", "dimo.status", subj, now.Add(-3*time.Hour))
	evBoth.Tags = []string{"trip", "safety"}
	evOne := mkStoredEvent("ev-one", "dimo.status", subj, now.Add(-2*time.Hour))
	evOne.Tags = []string{"trip"}
	evNone := mkStoredEvent("ev-none", "dimo.status", subj, now.Add(-time.Hour))
	insertRawEvent(t, svc, evBoth)
	insertRawEvent(t, svc, evOne)
	insertRawEvent(t, svc, evNone)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subj}},
		Tags:    &grpc.ArrayFilterOption{ContainsAll: []string{"trip", "safety"}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "ContainsAll must require both tags")
	assert.Equal(t, "ev-both", indexes[0].ID)
}

// TestTagsNotContainsAny verifies that NotContainsAny excludes events that
// have any of the specified tags.
func TestTagsNotContainsAny(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	subj := "did:erc721:137:0xNCA:1"
	evTagged := mkStoredEvent("ev-tagged", "dimo.status", subj, now.Add(-2*time.Hour))
	evTagged.Tags = []string{"dangerous"}
	evClean := mkStoredEvent("ev-clean", "dimo.status", subj, now.Add(-time.Hour))
	insertRawEvent(t, svc, evTagged)
	insertRawEvent(t, svc, evClean)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subj}},
		Tags:    &grpc.ArrayFilterOption{NotContainsAny: []string{"dangerous"}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "NotContainsAny must exclude tagged event")
	assert.Equal(t, "ev-clean", indexes[0].ID)
}

// TestExtrasFilter verifies IN / NOT IN filtering on the raw extras column.
func TestExtrasFilter(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	subj := "did:erc721:137:0xEXT:1"
	evA := mkStoredEvent("ev-extras-a", "dimo.status", subj, now.Add(-2*time.Hour))
	evA.Extras = map[string]any{"region": "us-east"}
	evB := mkStoredEvent("ev-extras-b", "dimo.status", subj, now.Add(-time.Hour))
	evB.Extras = map[string]any{"region": "eu-west"}
	insertRawEvent(t, svc, evA)
	insertRawEvent(t, svc, evB)

	// Compute the expected extras JSON strings so we can filter by exact value.
	extrasA, err := json.Marshal(evA.Extras)
	require.NoError(t, err)
	extrasB, err := json.Marshal(evB.Extras)
	require.NoError(t, err)

	// IN filter: only us-east event.
	inOpts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subj}},
		Extras:  &grpc.StringFilterOption{In: []string{string(extrasA)}},
	}
	inIndexes, err := lsvc.ListIndexesAdvanced(ctx, 10, inOpts)
	require.NoError(t, err)
	require.Len(t, inIndexes, 1, "Extras IN must match only the us-east event")
	assert.Equal(t, "ev-extras-a", inIndexes[0].ID)

	// NOT IN filter: exclude eu-west, only us-east remains.
	notInOpts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subj}},
		Extras:  &grpc.StringFilterOption{NotIn: []string{string(extrasB)}},
	}
	notInIndexes, err := lsvc.ListIndexesAdvanced(ctx, 10, notInOpts)
	require.NoError(t, err)
	require.Len(t, notInIndexes, 1, "Extras NOT IN must exclude eu-west event")
	assert.Equal(t, "ev-extras-a", notInIndexes[0].ID)
}

// TestOrClauseReturnsError verifies that an Or clause in an advanced filter
// returns errOrClauseUnsupported rather than silently over-returning.
func TestOrClauseReturnsError(t *testing.T) {
	ctx := context.Background()
	lsvc, _ := newLakeEventServiceForTest(t)

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{
			In: []string{lakeRawSubj},
			Or: []*grpc.StringFilterOption{
				{In: []string{"did:other"}},
			},
		},
	}
	_, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.Error(t, err, "Or clause must return an error, not silently over-return")
	require.ErrorIs(t, err, errOrClauseUnsupported)
}

// TestTimestampAsc_ListIndexesAdvanced verifies that TimestampAsc=true returns
// events oldest-first and TimestampAsc=false (or unset) returns newest-first,
// matching eventrepo.ListIndexesAdvanced semantics exactly.
func TestTimestampAsc_ListIndexesAdvanced(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	subj := "did:erc721:137:0xASC:1"
	ev1 := mkStoredEvent("ev-oldest", "dimo.status", subj, now.Add(-3*time.Hour))
	ev2 := mkStoredEvent("ev-middle", "dimo.status", subj, now.Add(-2*time.Hour))
	ev3 := mkStoredEvent("ev-newest", "dimo.status", subj, now.Add(-1*time.Hour))
	for _, e := range []cloudevent.StoredEvent{ev1, ev2, ev3} {
		insertRawEvent(t, svc, e)
	}

	// DESC (default / TimestampAsc unset) → newest first, matching CH.
	descOpts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subj}},
	}
	descIdxs, err := lsvc.ListIndexesAdvanced(ctx, 10, descOpts)
	require.NoError(t, err)
	require.Len(t, descIdxs, 3)
	assert.Equal(t, "ev-newest", descIdxs[0].ID, "DESC: first result must be newest")
	assert.Equal(t, "ev-middle", descIdxs[1].ID)
	assert.Equal(t, "ev-oldest", descIdxs[2].ID, "DESC: last result must be oldest")

	// ASC (TimestampAsc=true) → oldest first, matching CH.
	ascOpts := &grpc.AdvancedSearchOptions{
		Subject:      &grpc.StringFilterOption{In: []string{subj}},
		TimestampAsc: wrapperspb.Bool(true),
	}
	ascIdxs, err := lsvc.ListIndexesAdvanced(ctx, 10, ascOpts)
	require.NoError(t, err)
	require.Len(t, ascIdxs, 3)
	assert.Equal(t, "ev-oldest", ascIdxs[0].ID, "ASC: first result must be oldest")
	assert.Equal(t, "ev-middle", ascIdxs[1].ID)
	assert.Equal(t, "ev-newest", ascIdxs[2].ID, "ASC: last result must be newest")
}

// TestTimestampAsc_GetLatestIndexAdvanced verifies that GetLatestIndexAdvanced
// always returns the newest event even when the caller passes TimestampAsc=true,
// because it forces DESC before calling
// ListIndexesAdvanced.
func TestTimestampAsc_GetLatestIndexAdvanced(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	subj := "did:erc721:137:0xLATEST:1"
	older := mkStoredEvent("ev-older", "dimo.status", subj, now.Add(-2*time.Hour))
	newer := mkStoredEvent("ev-newer", "dimo.status", subj, now.Add(-1*time.Hour))
	insertRawEvent(t, svc, older)
	insertRawEvent(t, svc, newer)

	// With TimestampAsc=true, GetLatestIndexAdvanced must still return newest.
	opts := &grpc.AdvancedSearchOptions{
		Subject:      &grpc.StringFilterOption{In: []string{subj}},
		TimestampAsc: wrapperspb.Bool(true),
	}
	idx, err := lsvc.GetLatestIndexAdvanced(ctx, opts)
	require.NoError(t, err)
	assert.Equal(t, "ev-newer", idx.ID,
		"GetLatestIndexAdvanced must return newest event even when TimestampAsc=true")
}

// TestLakeEventService_GetCloudEventFromIndex_BlobFetchesFromS3 verifies the
// gRPC blob gap fix: a blob-backed event (data_index_key set, inline data
// empty) must have its raw payload bytes fetched from S3 into RawEvent.Data,
// because the gRPC proto carries only Data (grpc.CloudEventToProto drops
// DataBase64) and din stores the raw decoded payload at the blob key.
func TestLakeEventService_GetCloudEventFromIndex_BlobFetchesFromS3(t *testing.T) {
	ctx := context.Background()
	_, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	blobKey := eventrepo.BlobKeyPrefix + lakeRawSubj + "/2026/06/blob-1"
	blobBytes := []byte(`{"image":"big-binary-payload","frames":12345}`)
	getter := &fakeBlobGetter{objects: map[string][]byte{blobKey: blobBytes}}
	lsvc := NewLakeEventService(svc, getter, nil, "test-bucket")

	ev := mkStoredEvent("ev-blob-fetch", "dimo.attestation", lakeRawSubj, now.Add(-time.Hour))
	ev.DataIndexKey = blobKey
	ev.Data = nil // large payload: bytes live in S3, not inline
	insertRawEvent(t, svc, ev)

	indexes, err := lsvc.ListIndexesAdvanced(ctx, 1, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	})
	require.NoError(t, err)
	require.Len(t, indexes, 1)
	require.Equal(t, blobKey, indexes[0].Data.Key, "blob index key must route to the blob object")

	raw, err := lsvc.GetCloudEventFromIndex(ctx, &indexes[0])
	require.NoError(t, err)
	require.Equal(t, blobBytes, []byte(raw.Data),
		"blob payload must be downloaded from S3 into Data (proto carries Data only)")
}

// TestLakeEventService_GetCloudEventFromIndex_BlobNoGetter verifies that a blob
// event with no object store configured fails loudly rather than silently
// returning an empty payload (the original bug).
func TestLakeEventService_GetCloudEventFromIndex_BlobNoGetter(t *testing.T) {
	ctx := context.Background()
	_, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	blobKey := eventrepo.BlobKeyPrefix + lakeRawSubj + "/2026/06/blob-2"
	lsvc := NewLakeEventService(svc, nil, nil, "") // no getter

	ev := mkStoredEvent("ev-blob-nogetter", "dimo.attestation", lakeRawSubj, now.Add(-time.Hour))
	ev.DataIndexKey = blobKey
	ev.Data = nil
	insertRawEvent(t, svc, ev)

	indexes, err := lsvc.ListIndexesAdvanced(ctx, 1, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	})
	require.NoError(t, err)
	require.Len(t, indexes, 1)

	_, err = lsvc.GetCloudEventFromIndex(ctx, &indexes[0])
	require.Error(t, err, "blob event with no object store must error, not return empty data")
}

// TestLakeEventService_ListIndexesAdvanced_CapsLimit verifies that an
// over-large caller limit is clamped to maxLakeQueryLimit (1000). Without the
// cap, a single gRPC caller could force an
// unbounded scan + Go-side dedup (memory/latency DoS).
func TestLakeEventService_ListIndexesAdvanced_CapsLimit(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	subj := "did:erc721:137:0xCAP:1"

	// Insert 1100 distinct events (> the 1000 cap) in one statement.
	_, err := svc.db.ExecContext(ctx, `
		INSERT INTO lake.raw_events
			(subject, time, type, id, source, producer, data_content_type, data_version, extras, data, data_base64, data_index_key, voids_id)
		SELECT ?, now() - (i * INTERVAL 1 SECOND), 'dimo.status', 'ev-' || i, 'src-test', ?, '', '', NULL, '{"v":1}', NULL, NULL, NULL
		FROM generate_series(1, 1100) AS t(i)`, subj, subj)
	require.NoError(t, err)

	indexes, err := lsvc.ListIndexesAdvanced(ctx, 100000, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subj}},
	})
	require.NoError(t, err)
	require.Len(t, indexes, 1000, "limit must be capped at maxLakeQueryLimit (1000)")
}
