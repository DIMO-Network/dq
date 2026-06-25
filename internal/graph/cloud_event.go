package graph

import (
	"context"
	"encoding/json"
	"io"

	"github.com/99designs/gqlgen/graphql"
	"github.com/DIMO-Network/cloudevent"
)

// CloudEventWrapper holds a pointer to a RawEvent so resolvers can expose
// header, data, dataBase64, and dataUrl without copying the underlying event.
type CloudEventWrapper struct {
	Raw *cloudevent.RawEvent
	// DataURL is the presigned URL for a blob payload, or nil for inline events.
	// A pointer (not a bare string) so the dataUrl GraphQL field serializes as
	// null rather than "" when the payload is inline (MCP auto-selects dataUrl).
	DataURL *string
}

// RawJSON is the raw bytes of a JSON value. It implements graphql.Marshaler by
// writing the bytes directly so the payload appears as unescaped JSON (object/array)
// in the GraphQL response instead of an escaped string.
type RawJSON []byte

// MarshalGQL writes the raw JSON bytes to w so the response contains unescaped JSON.
// Stored raw_events.data is device-supplied (validated at ingest, but legacy or
// corrupt rows are possible); invalid bytes written verbatim would break the whole
// response body — one poison row would corrupt every event in the same query — so
// fall back to null. json.Valid is O(len) but inline data is small (large blobs go
// via dataUrl).
func (r RawJSON) MarshalGQL(w io.Writer) {
	if len(r) == 0 || !json.Valid(r) {
		_, _ = w.Write([]byte("null"))
		return
	}
	_, _ = w.Write(r)
}

// UnmarshalGQL satisfies the graphql.Unmarshaler interface (e.g. for variables).
// Scalar is primarily used for output (CloudEvent.data); input is not used.
func (r *RawJSON) UnmarshalGQL(v interface{}) error {
	*r = nil
	return nil
}

// dataFieldsRequested returns true if the current GraphQL selection set includes
// data or dataBase64. When neither is selected there is no need to fetch the
// event payload from S3.
func dataFieldsRequested(ctx context.Context) bool {
	for _, f := range graphql.CollectFieldsCtx(ctx, nil) {
		if f.Name == "data" || f.Name == "dataBase64" {
			return true
		}
	}
	return false
}
