package duck

// pr3_pr4_parity_test.go pins the functionality of two superseded PRs whose
// features were reimplemented natively in the DuckLake layer:
//
//   - PR #3 "Handle presigned links to data blobs": externalized (>1MB) blob
//     payloads are served as short-lived presigned S3 GET URLs instead of being
//     inlined. The query resolver routes any index whose key carries
//     eventrepo.BlobKeyPrefix through LakeEventService.PresignBlobURL.
//   - PR #4 "Hide events based on new tombstone rows": a tombstone (voids_id set)
//     and the event it voids are both excluded from every read path.
//
// Existing coverage: TestLakeEventService_BlobIndexKey (blob event → ObjectInfo.Key
// is the blob key, so the resolver's HasPrefix(key, BlobKeyPrefix) branch fires)
// and TestLakeEventService_VoidingExcludes (tombstone-after-event). These tests
// add the missing halves: the presign mechanism itself, and order-independent
// voiding (tombstone ingested BEFORE the event it voids).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePresigner implements eventrepo.Presigner: it returns a canned URL (or
// error) and records the bucket/key it was asked to presign.
type fakePresigner struct {
	url       string
	err       error
	gotBucket string
	gotKey    string
}

func (f *fakePresigner) PresignGetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	if f.err != nil {
		return nil, f.err
	}
	if in.Bucket != nil {
		f.gotBucket = *in.Bucket
	}
	if in.Key != nil {
		f.gotKey = *in.Key
	}
	return &v4.PresignedHTTPRequest{URL: f.url}, nil
}

// TestLakeEventService_PresignBlobURL pins PR #3's mechanism: a blob key is
// turned into a presigned GET URL for the configured blob bucket, and the
// missing-dependency cases fail loudly instead of panicking or leaking inline.
func TestLakeEventService_PresignBlobURL(t *testing.T) {
	ctx := context.Background()
	_, svc := newLakeEventServiceForTest(t)
	blobKey := eventrepo.BlobKeyPrefix + "did:erc721:137:0xabc:101/2026/06/blob1"

	pre := &fakePresigner{url: "https://blob-bucket.s3.amazonaws.com/" + blobKey + "?X-Amz-Signature=deadbeef"}
	lsvc := NewLakeEventService(svc, nil, pre, "blob-bucket")

	url, err := lsvc.PresignBlobURL(ctx, blobKey)
	require.NoError(t, err)
	assert.Equal(t, pre.url, url, "returns the presigned URL verbatim")
	assert.Equal(t, "blob-bucket", pre.gotBucket, "presigns against the configured blob bucket")
	assert.Equal(t, blobKey, pre.gotKey, "presigns the exact blob key")

	// Missing presigner / bucket must error, never silently inline or panic.
	noPre := NewLakeEventService(svc, nil, nil, "blob-bucket")
	_, err = noPre.PresignBlobURL(ctx, blobKey)
	require.Error(t, err, "nil presigner must error")

	noBucket := NewLakeEventService(svc, nil, pre, "")
	_, err = noBucket.PresignBlobURL(ctx, blobKey)
	require.Error(t, err, "empty bucket must error")

	// A presigner failure is wrapped and surfaced, not swallowed.
	boom := &fakePresigner{err: errors.New("sts denied")}
	failSvc := NewLakeEventService(svc, nil, boom, "blob-bucket")
	_, err = failSvc.PresignBlobURL(ctx, blobKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sts denied")

	// Guard (defense in depth): a key outside BlobKeyPrefix is refused without
	// ever calling the presigner, so no caller can presign arbitrary objects
	// (e.g. raw parquet) in the bucket.
	guardPre := &fakePresigner{url: "https://should-not-be-reached"}
	guardSvc := NewLakeEventService(svc, nil, guardPre, "blob-bucket")
	_, err = guardSvc.PresignBlobURL(ctx, "lake/parquet/not-a-blob.parquet")
	require.Error(t, err, "non-blob key must be rejected")
	assert.Empty(t, guardPre.gotKey, "presigner must not be invoked for a non-blob key")
}

// TestLakeEventService_VoidingTombstoneBeforeEvent pins PR #4's tombstone
// semantics under reverse ingest order: a tombstone whose timestamp PRECEDES the
// event it voids must still hide that event. The voiding anti-join keys on
// voids_id == id (not on time), so ordering must not matter — this guards against
// a regression to a time-windowed implementation.
func TestLakeEventService_VoidingTombstoneBeforeEvent(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	// Tombstone is the OLDEST row; the event it voids arrives later.
	tomb := cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion,
				Type:        "dimo.tombstone",
				Subject:     lakeRawSubj,
				Source:      "src-test",
				ID:          "tomb-early",
				Time:        now.Add(-2 * time.Hour),
			},
		},
		VoidsID: "ev-late-voided",
	}
	voided := mkStoredEvent("ev-late-voided", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	good := mkStoredEvent("ev-keep", "dimo.status", lakeRawSubj, now.Add(-30*time.Minute))

	for _, e := range []cloudevent.StoredEvent{tomb, voided, good} {
		insertRawEvent(t, svc, e)
	}

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	}

	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	ids := make([]string, len(indexes))
	for i, idx := range indexes {
		ids[i] = idx.ID
	}
	assert.Equal(t, []string{"ev-keep"}, ids,
		"event voided by an earlier-timestamped tombstone is still excluded; the tombstone itself is never listed")

	// Latest must skip the voided event even though it is newer than the tombstone.
	latest, err := lsvc.GetLatestIndexAdvanced(ctx, opts)
	require.NoError(t, err)
	assert.Equal(t, "ev-keep", latest.ID)
}
