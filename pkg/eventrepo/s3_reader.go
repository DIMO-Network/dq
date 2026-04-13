package eventrepo

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// downloadS3Object fetches an S3 object into memory, rejecting anything larger than maxSize bytes.
func downloadS3Object(ctx context.Context, client ObjectGetter, bucket, key string, maxSize int64) ([]byte, error) {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s/%s: %w", bucket, key, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.ContentLength != nil && *resp.ContentLength > 0 {
		size := *resp.ContentLength
		if size > maxSize {
			return nil, fmt.Errorf("object %s/%s exceeds max size of %d bytes", bucket, key, maxSize)
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(resp.Body, data); err != nil {
			return nil, fmt.Errorf("read object %s/%s: %w", bucket, key, err)
		}
		return data, nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSize+1))
	if err != nil {
		return nil, fmt.Errorf("read object %s/%s: %w", bucket, key, err)
	}
	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("object %s/%s exceeds max size of %d bytes", bucket, key, maxSize)
	}
	return data, nil
}

// S3ReaderAt implements io.ReaderAt by downloading the entire S3 object into
// memory on creation. This avoids multiple round-trips (HeadObject + range
// GETs) which is optimal for the expected file sizes (< 100 KB).
type S3ReaderAt struct {
	data []byte
}

// NewS3ReaderAt downloads the S3 object into memory and returns a ReaderAt.
// Rejects objects larger than maxObjectSize.
func NewS3ReaderAt(ctx context.Context, client ObjectGetter, bucket, key string) (*S3ReaderAt, error) {
	data, err := downloadS3Object(ctx, client, bucket, key, maxObjectSize)
	if err != nil {
		return nil, err
	}
	return &S3ReaderAt{data: data}, nil
}

// ReadAt implements io.ReaderAt by reading from the in-memory buffer.
func (r *S3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Size returns the total size of the S3 object in bytes.
func (r *S3ReaderAt) Size() int64 { return int64(len(r.data)) }
