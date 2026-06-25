// Package eventrepo holds the shared cloudevent object-storage types and S3
// helpers. The event fetch surface (EventService) is served by the DuckLake
// backend (internal/service/duck.LakeEventService).
package eventrepo

import (
	"context"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ObjectInfo is the information about the object in S3.
type ObjectInfo struct {
	Key string
}

// ObjectGetter is an interface for getting an object from S3.
type ObjectGetter interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Presigner generates presigned S3 GET URLs.
type Presigner interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

// BlobKeyPrefix is the S3 key prefix used for large binary blob objects.
// Keys with this prefix are served via presigned URL instead of inline in the response.
const BlobKeyPrefix = "cloudevent/blobs/"

// maxObjectSize is the maximum size of a single S3 object we'll read (50 MiB).
// Objects larger than this are rejected to prevent OOM from corrupted or
// malicious index keys pointing to oversized objects.
const maxObjectSize = 50 << 20

// CloudEventTypeSummary holds per-type aggregate metadata for a subject.
type CloudEventTypeSummary struct {
	Type      string
	Count     uint64
	FirstSeen time.Time
	LastSeen  time.Time
}
