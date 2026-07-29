package app

import (
	"context"
	"errors"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

func fieldCount(t *testing.T, field, status string) uint64 {
	t.Helper()
	o, err := rootFieldSeconds.GetMetricWithLabelValues(field, status)
	require.NoError(t, err)
	m, ok := o.(prometheus.Metric)
	require.True(t, ok)
	var pb dto.Metric
	require.NoError(t, m.Write(&pb))
	return pb.GetHistogram().GetSampleCount()
}

// fieldCtx builds the resolver context gqlgen would supply for a field on object.
func fieldCtx(object, name string) context.Context {
	return graphql.WithFieldContext(context.Background(), &graphql.FieldContext{
		Object: object,
		Field:  graphql.CollectedField{Field: &ast.Field{Name: name}},
	})
}

func TestRootFieldMetrics_RecordsQueryFields(t *testing.T) {
	before := fieldCount(t, "availableCloudEventTypes", "success")

	_, err := rootFieldMetrics{}.InterceptField(
		fieldCtx("Query", "availableCloudEventTypes"),
		func(context.Context) (any, error) { return "ok", nil })

	require.NoError(t, err)
	require.Equal(t, before+1, fieldCount(t, "availableCloudEventTypes", "success"))
}

func TestRootFieldMetrics_LabelsErrors(t *testing.T) {
	before := fieldCount(t, "signalsLatest", "error")

	_, err := rootFieldMetrics{}.InterceptField(
		fieldCtx("Query", "signalsLatest"),
		func(context.Context) (any, error) { return nil, errors.New("boom") })

	require.Error(t, err)
	require.Equal(t, before+1, fieldCount(t, "signalsLatest", "error"))
}

// Nested and leaf fields must not be timed: the signal selection set alone would
// otherwise mint ~117 series per status.
func TestRootFieldMetrics_SkipsNonRootFields(t *testing.T) {
	before := fieldCount(t, "speed", "success")

	_, err := rootFieldMetrics{}.InterceptField(
		fieldCtx("SignalCollection", "speed"),
		func(context.Context) (any, error) { return 1.0, nil })

	require.NoError(t, err)
	require.Equal(t, before, fieldCount(t, "speed", "success"))
}

// A missing FieldContext must pass through rather than panic — the interceptor
// runs on every field in every request.
func TestRootFieldMetrics_NoFieldContext(t *testing.T) {
	got, err := rootFieldMetrics{}.InterceptField(
		context.Background(),
		func(context.Context) (any, error) { return "passthrough", nil })

	require.NoError(t, err)
	require.Equal(t, "passthrough", got)
}
