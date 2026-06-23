package repositories

import (
	"context"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/ch"
	"github.com/DIMO-Network/dq/internal/service/duck"
)

// LocationAtSource is an optional backend capability: the nearest non-origin
// location fix at or before a timestamp, reaching back before any window. The lake
// backend implements it (duck.Queries.LocationAt); ClickHouse does not, so the
// segment enrichment only gap-fills trip start/end locations on the lake path.
type LocationAtSource interface {
	LocationAt(ctx context.Context, subject string, ts time.Time) (*model.Location, error)
}

// Compile-time interface assertions: both query engines must satisfy the
// shared Backend surface, and ClickHouse additionally the segments surface.
var (
	_ Backend         = (*ch.Service)(nil)
	_ Backend         = (*duck.Queries)(nil)
	_ CHService       = (*ch.Service)(nil)
	_ SegmentsBackend = (*ch.Service)(nil)
)

// composedBackend pairs a primary signal/event Backend with a separate
// SegmentsBackend, forming the full CHService surface. Used for the duckdb
// query backend, where segment detection still runs on ClickHouse.
type composedBackend struct {
	Backend
	SegmentsBackend
}

// ComposeBackend returns a CHService that serves signal/latest/summary/event
// queries from backend and segment detection from segments.
func ComposeBackend(backend Backend, segments SegmentsBackend) CHService {
	return composedBackend{Backend: backend, SegmentsBackend: segments}
}

// LocationAt promotes the underlying Backend's nearest-fix lookup (the lake
// backend's) so the composed CHService satisfies LocationAtSource; returns nil when
// the Backend doesn't support it (ClickHouse-backed compose).
func (b composedBackend) LocationAt(ctx context.Context, subject string, ts time.Time) (*model.Location, error) {
	if las, ok := b.Backend.(LocationAtSource); ok {
		return las.LocationAt(ctx, subject, ts)
	}
	return nil, nil
}
