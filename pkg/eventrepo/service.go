package eventrepo

import (
	"context"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/grpc"
)

// EventService is the cloudevent fetch surface consumed by the GraphQL
// resolver, the gRPC FetchService, and internal/fetch. The DuckLake-backed
// duck.LakeEventService implements it (see the assertion in lake_fetch.go).
type EventService interface {
	// Index lookups (metadata + object locator, no payload).
	GetLatestIndex(ctx context.Context, opts *grpc.SearchOptions) (cloudevent.CloudEvent[ObjectInfo], error)
	GetLatestIndexAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[ObjectInfo], error)
	ListIndexes(ctx context.Context, limit int, opts *grpc.SearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error)
	ListIndexesAdvanced(ctx context.Context, limit int, opts *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error)

	// Aggregation.
	GetCloudEventTypeSummariesAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) ([]CloudEventTypeSummary, error)

	// Payload resolution from an index entry.
	GetCloudEventFromIndex(ctx context.Context, index *cloudevent.CloudEvent[ObjectInfo]) (cloudevent.RawEvent, error)
	ListCloudEventsFromIndexes(ctx context.Context, indexes []cloudevent.CloudEvent[ObjectInfo]) ([]cloudevent.RawEvent, error)

	// Blob payloads served as presigned URLs.
	PresignBlobURL(ctx context.Context, key string) (string, error)

	// BlobsMaybeSealed reports whether stored blobs may be at-rest encrypted
	// (a blob cipher is configured). When true, presigned URLs are WRONG for
	// blob payloads — the customer would download DBE1 ciphertext (H16) — and
	// callers must resolve the payload inline (download + decrypt) instead.
	BlobsMaybeSealed() bool
}
