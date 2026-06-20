package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3ObjectStore adapts the AWS S3 client to the materializer.ObjectStore
// interface for a single bucket.
type s3ObjectStore struct {
	client *s3.Client
	bucket string
}

var (
	_ materializer.ObjectStore  = (*s3ObjectStore)(nil)
	_ materializer.CompactStore = (*s3ObjectStore)(nil)
)

func newS3ObjectStore(client *s3.Client, bucket string) *s3ObjectStore {
	return &s3ObjectStore{client: client, bucket: bucket}
}

// List returns every object under prefix, in lexicographic key order.
func (s *s3ObjectStore) List(ctx context.Context, prefix string) ([]materializer.ObjectInfo, error) {
	var infos []materializer.ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing s3://%s/%s: %w", s.bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			infos = append(infos, materializer.ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			})
		}
	}
	return infos, nil
}

// GetObject downloads one object, translating S3 not-found errors to
// materializer.ErrNotFound as the store contract requires.
func (s *s3ObjectStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			return nil, fmt.Errorf("s3://%s/%s: %w", s.bucket, key, materializer.ErrNotFound)
		}
		return nil, fmt.Errorf("getting s3://%s/%s: %w", s.bucket, key, err)
	}
	defer out.Body.Close() //nolint:errcheck
	// Fail fast on a pathologically large object rather than reading it whole
	// into memory and risking OOM (SR-22). The cap is generous — well above any
	// legitimate raw bundle or compacted partition — so it only catches
	// corrupt/runaway objects.
	if out.ContentLength != nil && *out.ContentLength > maxStoreObjectBytes {
		return nil, fmt.Errorf("s3://%s/%s is %d bytes, over the %d-byte store object limit", s.bucket, key, *out.ContentLength, maxStoreObjectBytes)
	}
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("reading s3://%s/%s: %w", s.bucket, key, err)
	}
	return body, nil
}

// maxStoreObjectBytes bounds a single object read into memory by the materializer
// object store. Generous (2 GiB) so it never trips on a normal large compacted
// partition — it exists only to stop a corrupt/runaway object from OOMing.
const maxStoreObjectBytes = 2 << 30

// PutObject uploads one object.
func (s *s3ObjectStore) PutObject(ctx context.Context, key string, body []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("putting s3://%s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// DeleteObject removes one object. AWS S3 deletes are quiet on missing
// keys, but S3-compatible stores (MinIO) can 404 — treat that as done so
// compaction recovery stays idempotent.
func (s *s3ObjectStore) DeleteObject(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			return nil
		}
		return fmt.Errorf("deleting s3://%s/%s: %w", s.bucket, key, err)
	}
	return nil
}
