package repositories

import (
	"github.com/DIMO-Network/dq/internal/service/ch"
	"github.com/DIMO-Network/dq/internal/service/duck"
)

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
