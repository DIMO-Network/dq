package repositories

import (
	"context"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/duck"
)

// LocationAtSource is an optional backend capability: the nearest non-origin
// location fix at or before a timestamp, reaching back before any window. The lake
// backend implements it (duck.Queries.LocationAt) for trip start/end gap-fill.
type LocationAtSource interface {
	LocationAt(ctx context.Context, subject string, ts time.Time) (*model.Location, error)
}

// Compile-time interface assertions for the DuckLake backend (the only backend).
var (
	_ Backend         = (*duck.Queries)(nil)
	_ SegmentsBackend = (*duck.LakeSegments)(nil)
	_ QueryService    = composedBackend{}
)

// composedBackend pairs the signal/event Backend with a separate SegmentsBackend,
// forming the full query-service surface. The DuckLake backend supplies both
// (NewLakeQueries + NewLakeSegments).
type composedBackend struct {
	Backend
	SegmentsBackend
}

// ComposeBackend returns a QueryService that serves signal/latest/summary/event
// queries from backend and segment detection from segments.
func ComposeBackend(backend Backend, segments SegmentsBackend) QueryService {
	return composedBackend{Backend: backend, SegmentsBackend: segments}
}

// LocationAt promotes the underlying Backend's nearest-fix lookup so the composed
// service satisfies LocationAtSource; returns nil when the Backend lacks it.
func (b composedBackend) LocationAt(ctx context.Context, subject string, ts time.Time) (*model.Location, error) {
	if las, ok := b.Backend.(LocationAtSource); ok {
		return las.LocationAt(ctx, subject, ts)
	}
	return nil, nil
}
