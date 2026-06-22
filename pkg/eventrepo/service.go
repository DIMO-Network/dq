package eventrepo

import (
	"context"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/grpc"
)

// EventService is the cloudevent fetch surface consumed by the GraphQL
// resolver, the gRPC FetchService, and internal/fetch. ClickHouse (*Service)
// and the DuckLake-backed duck.LakeEventService both implement it, selected
// by QUERY_BACKEND.
type EventService interface {
	// Index lookups (metadata + object locator, no payload).
	GetLatestIndex(ctx context.Context, opts *grpc.SearchOptions) (cloudevent.CloudEvent[ObjectInfo], error)
	GetLatestIndexAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[ObjectInfo], error)
	ListIndexes(ctx context.Context, limit int, opts *grpc.SearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error)
	ListIndexesAdvanced(ctx context.Context, limit int, opts *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error)

	// Aggregation.
	GetCloudEventTypeSummariesAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) ([]CloudEventTypeSummary, error)

	// Payload resolution from an index entry.
	GetCloudEventFromIndex(ctx context.Context, index *cloudevent.CloudEvent[ObjectInfo], bucketName string) (cloudevent.RawEvent, error)
	ListCloudEventsFromIndexes(ctx context.Context, indexes []cloudevent.CloudEvent[ObjectInfo], bucketName string) ([]cloudevent.RawEvent, error)

	// BatchesAllIndexes reports whether ListCloudEventsFromIndexes resolves ANY
	// index efficiently in one batched call (e.g. the lake backend groups by
	// subject → one query per subject). When true, internal/fetch routes every
	// index through it instead of the per-key fallback. The ClickHouse backend
	// returns false: its ListCloudEventsFromIndexes only batches parquet refs, and
	// other keys are individual S3 objects fetched per key.
	BatchesAllIndexes() bool

	// Blob payloads served as presigned URLs.
	PresignBlobURL(ctx context.Context, key string) (string, error)
}

var _ EventService = (*Service)(nil)
