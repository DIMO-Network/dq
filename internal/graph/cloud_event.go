package graph

import (
	"context"
	"io"

	"github.com/99designs/gqlgen/graphql"
	"github.com/DIMO-Network/cloudevent"
)

// CloudEventWrapper holds a pointer to a RawEvent so resolvers can expose
// header, data, dataBase64, and dataUrl without copying the underlying event.
type CloudEventWrapper struct {
	Raw     *cloudevent.RawEvent
	DataURL string // non-empty when the payload is a blob served via presigned URL
}

// RawJSON is the raw bytes of a JSON value. It implements graphql.Marshaler by
// writing the bytes directly so the payload appears as unescaped JSON (object/array)
// in the GraphQL response instead of an escaped string.
type RawJSON []byte

// MarshalGQL writes the raw JSON bytes to w so the response contains unescaped JSON.
func (r RawJSON) MarshalGQL(w io.Writer) {
	if len(r) == 0 {
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
