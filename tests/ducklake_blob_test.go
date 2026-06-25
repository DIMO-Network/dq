// ducklake_blob_test.go proves the DuckLake materializer resolves externalized
// blob payloads. din writes payloads larger than the inline threshold to S3 and
// leaves only a data_index_key (under BlobKeyPrefix) on the raw_events row; the
// materializer must download the blob before decoding or every large payload
// decodes to nothing (CHD-8 — permanent decoded-data loss).
package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBlobStore is a minimal eventrepo.ObjectGetter serving byte payloads from
// an in-memory map keyed by S3 object key.
type fakeBlobStore struct {
	objects map[string][]byte
}

func (f *fakeBlobStore) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	b, ok := f.objects[*in.Key]
	if !ok {
		return nil, fmt.Errorf("no such key: %s", *in.Key)
	}
	n := int64(len(b))
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b)), ContentLength: &n}, nil
}

func TestDuckLake_MaterializeBlobPayload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:9", vehicleNFT.Hex())
	ts := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)

	// din externalized this payload to S3: the raw_events row carries an empty
	// data column and only a data_index_key under BlobKeyPrefix.
	ev := deviceStatus("blob-1", subject, ts, speedAt(ts, 55))
	blobKey := eventrepo.BlobKeyPrefix + "blob-1.json"
	getter := &fakeBlobStore{objects: map[string][]byte{blobKey: ev.Data}}

	_, err := db.ExecContext(ctx,
		`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data, data_index_key)
		 VALUES (?, ?, ?, ?, ?, ?, '', ?, '{}', '', ?)`,
		ev.Subject, ev.Time.UTC(), ev.Type, ev.ID, ev.Source, ev.Producer, ev.DataVersion, blobKey)
	require.NoError(t, err)

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat = mat.WithBlobStore(getter, "test-bucket")
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	processed := drainRunner(t, ctx, runner)
	require.Equal(t, 1, processed, "the blob-backed raw event is consumed")

	var rows int
	var speed float64
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*), coalesce(max(value_number), 0) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).
		Scan(&rows, &speed))
	assert.Equal(t, 1, rows, "the externalized blob payload decoded into a signal row")
	assert.Equal(t, 55.0, speed)
}
