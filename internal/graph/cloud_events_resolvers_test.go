package graph

import (
	"context"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
)

// ctxWithSelectedFields builds a synthetic GraphQL context that reports the
// named fields as selected on the current object. This lets us test
// dataFieldsRequested without a running server.
func ctxWithSelectedFields(names ...string) context.Context {
	sel := make(ast.SelectionSet, len(names))
	for i, name := range names {
		sel[i] = &ast.Field{Name: name}
	}
	ctx := graphql.WithOperationContext(context.Background(), &graphql.OperationContext{})
	ctx = graphql.WithFieldContext(ctx, &graphql.FieldContext{
		Field: graphql.CollectedField{
			Selections: sel,
		},
	})
	return ctx
}

func TestDataFieldsRequested(t *testing.T) {
	tests := []struct {
		name   string
		fields []string
		want   bool
	}{
		{
			name:   "no fields selected",
			fields: nil,
			want:   false,
		},
		{
			name:   "header only",
			fields: []string{"header"},
			want:   false,
		},
		{
			name:   "dataUrl only",
			fields: []string{"dataUrl"},
			want:   false,
		},
		{
			name:   "header and dataUrl",
			fields: []string{"header", "dataUrl"},
			want:   false,
		},
		{
			name:   "data selected",
			fields: []string{"header", "data"},
			want:   true,
		},
		{
			name:   "dataBase64 selected",
			fields: []string{"header", "dataBase64"},
			want:   true,
		},
		{
			name:   "both data fields selected",
			fields: []string{"data", "dataBase64"},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := ctxWithSelectedFields(tt.fields...)
			if got := dataFieldsRequested(ctx); got != tt.want {
				t.Errorf("dataFieldsRequested() = %v, want %v (fields: %v)", got, tt.want, tt.fields)
			}
		})
	}
}
