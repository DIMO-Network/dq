package eventrepo

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

// IsObjectNotFound reports whether err indicates the S3 object does not exist
// (NoSuchKey / NotFound / 404) — a permanent condition — as opposed to a
// transient fetch failure (timeout, throttle, 5xx). Callers use it to skip a
// permanently-missing payload (or return a clean NotFound) instead of wedging.
func IsObjectNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound", "NotFoundException", "404":
			return true
		}
	}
	return false
}

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

// DownloadObject fetches an entire S3 object into memory, rejecting objects
// larger than the maximum single-object size (maxObjectSize). It is the
// blob-payload resolution primitive used by the DuckLake LakeEventService.
func DownloadObject(ctx context.Context, client ObjectGetter, bucket, key string) ([]byte, error) {
	return downloadS3Object(ctx, client, bucket, key, maxObjectSize)
}
