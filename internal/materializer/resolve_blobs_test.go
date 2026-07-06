package materializer

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/blobcrypt"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubGetter is an eventrepo.ObjectGetter serving in-memory blobs, with per-key
// oversize (huge ContentLength) and transient-failure injection.
type stubGetter struct {
	objects  map[string][]byte
	oversize map[string]bool
	fail     map[string]error
}

func (g *stubGetter) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := *in.Key
	if err := g.fail[key]; err != nil {
		return nil, err
	}
	if g.oversize[key] {
		big := int64(64 << 20) // > eventrepo maxObjectSize (50 MiB): the size check fires before any read
		return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte("x"))), ContentLength: &big}, nil
	}
	b, ok := g.objects[key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	n := int64(len(b))
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b)), ContentLength: &n}, nil
}

func blobKey(s string) string { return eventrepo.BlobKeyPrefix + s }

func rawWithID(id string) cloudevent.RawEvent {
	return cloudevent.RawEvent{CloudEventHeader: cloudevent.CloudEventHeader{ID: id}}
}

// sealedGarbage is a blob that carries the blobcrypt magic (IsSealed) but whose
// ciphertext will never authenticate — used for sealed_no_key and decrypt cases.
var sealedGarbage = append([]byte("DBE1"), bytes.Repeat([]byte{0}, 12+16)...)

// TestResolveBlobs_ContainsDeterministicPoison proves Items 3/4/5: an oversize
// object and a sealed blob with no key are DETERMINISTIC poison — skipped and
// counted, never aborting the pass — while a genuinely-missing object is a 404
// skip and a data_index_key not under the blob prefix is a prefix anomaly (kept,
// decoded empty). Only these deterministic conditions are contained.
func TestResolveBlobs_ContainsDeterministicPoison(t *testing.T) {
	registerMetrics()
	ctx := context.Background()

	oversizeKey, sealedKey, missingKey, goodKey := blobKey("big"), blobKey("sealed"), blobKey("gone"), blobKey("good")
	anomalyKey := "wrong-prefix/x.json"

	m := &DuckLakeMaterializer{
		blobs:      &stubGetter{objects: map[string][]byte{goodKey: []byte(`{"ok":true}`), sealedKey: sealedGarbage}, oversize: map[string]bool{oversizeKey: true}},
		blobBucket: "b",
		log:        zerolog.Nop(),
	}
	events := []cloudevent.RawEvent{
		rawWithID("e-good"), rawWithID("e-oversize"), rawWithID("e-sealed"), rawWithID("e-missing"), rawWithID("e-anomaly"),
	}
	blobKeys := []string{goodKey, oversizeKey, sealedKey, missingKey, anomalyKey}

	oversizeBefore := testutil.ToFloat64(blobPoisonTotal.WithLabelValues("oversize"))
	sealedBefore := testutil.ToFloat64(blobPoisonTotal.WithLabelValues("sealed_no_key"))
	missingBefore := testutil.ToFloat64(blobMissingTotal)

	kept, err := m.resolveBlobs(ctx, events, blobKeys)
	require.NoError(t, err, "deterministic poison must be contained, not abort the pass")

	ids := map[string]string{}
	for _, e := range kept {
		ids[e.ID] = string(e.Data)
	}
	assert.Len(t, kept, 2, "only the good and prefix-anomaly rows survive")
	assert.Equal(t, `{"ok":true}`, ids["e-good"], "good blob payload resolved")
	if _, ok := ids["e-anomaly"]; !ok {
		t.Fatal("prefix-anomaly row must be kept (decoded empty), not skipped")
	}
	assert.Empty(t, ids["e-anomaly"], "prefix-anomaly row keeps an empty payload")
	assert.NotContains(t, ids, "e-oversize")
	assert.NotContains(t, ids, "e-sealed")
	assert.NotContains(t, ids, "e-missing")

	assert.Equal(t, oversizeBefore+1, testutil.ToFloat64(blobPoisonTotal.WithLabelValues("oversize")))
	assert.Equal(t, sealedBefore+1, testutil.ToFloat64(blobPoisonTotal.WithLabelValues("sealed_no_key")))
	assert.Equal(t, missingBefore+1, testutil.ToFloat64(blobMissingTotal))
}

// TestResolveBlobs_DecryptPoison proves the decrypt-failure poison path (Item 3):
// with a cipher configured, a sealed blob that fails to authenticate is skipped
// and counted, not aborted.
func TestResolveBlobs_DecryptPoison(t *testing.T) {
	registerMetrics()
	ctx := context.Background()

	cipher, err := blobcrypt.NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	require.NoError(t, err)

	badKey := blobKey("badct")
	m := &DuckLakeMaterializer{
		blobs:      &stubGetter{objects: map[string][]byte{badKey: sealedGarbage}},
		blobBucket: "b",
		blobCipher: cipher,
		log:        zerolog.Nop(),
	}
	before := testutil.ToFloat64(blobPoisonTotal.WithLabelValues("decrypt"))
	kept, err := m.resolveBlobs(ctx, []cloudevent.RawEvent{rawWithID("e-decrypt")}, []string{badKey})
	require.NoError(t, err, "an undecryptable blob is deterministic poison, not a retry")
	assert.Empty(t, kept, "the poison row is skipped")
	assert.Equal(t, before+1, testutil.ToFloat64(blobPoisonTotal.WithLabelValues("decrypt")))
}

// TestResolveBlobs_TransientAborts proves a TRANSIENT fetch error still aborts the
// pass (returns an error) so the same delta is retried — the poison classification
// must not swallow retryable failures.
func TestResolveBlobs_TransientAborts(t *testing.T) {
	registerMetrics()
	ctx := context.Background()

	key := blobKey("flaky")
	m := &DuckLakeMaterializer{
		blobs:      &stubGetter{fail: map[string]error{key: errors.New("connection reset by peer")}},
		blobBucket: "b",
		log:        zerolog.Nop(),
	}
	_, err := m.resolveBlobs(ctx, []cloudevent.RawEvent{rawWithID("e-flaky")}, []string{key})
	require.Error(t, err, "a transient S3 error must abort the pass for retry, not be contained as poison")
}
