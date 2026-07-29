package app

import (
	"context"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// rootFieldSeconds is the per-query-field latency and call count of the GraphQL
// surface.
//
// server-garage's shared Tracer already exports graphql_request_total, but it is
// labelled by response size, complexity bucket and status — nothing that
// identifies WHICH query ran. That makes two ordinary questions unanswerable:
// "how often is availableCloudEventTypes actually called?" and "what latency
// does a caller of it see?". The lake-side dq_lake_read_seconds answers neither,
// because one GraphQL field can issue several lake reads (or none, on a cache
// hit) and carries resolver-side work the lake metrics never see.
//
// Cardinality is bounded by the schema: the label is a root Query field name, of
// which there are ~10, times two status values.
var rootFieldSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "dq_graphql_field_seconds",
	Help:    "Wall-clock of each root Query field resolver, by field and status. The user-visible latency dq_lake_read_seconds cannot express.",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
}, []string{"field", "status"})

// rootFieldMetrics times root Query field resolvers. Nested and leaf fields are
// passed through untouched: timing them would multiply the series count by the
// ~117-field signal selection set for no diagnostic gain, and their durations
// are already inside the root field's.
type rootFieldMetrics struct{}

var _ interface {
	graphql.HandlerExtension
	graphql.FieldInterceptor
} = rootFieldMetrics{}

// ExtensionName returns the name of this extension.
func (rootFieldMetrics) ExtensionName() string { return "RootFieldMetrics" }

// Validate accepts any schema; the extension reads only the resolver context.
func (rootFieldMetrics) Validate(graphql.ExecutableSchema) error { return nil }

// InterceptField times root Query fields and records the outcome.
func (rootFieldMetrics) InterceptField(ctx context.Context, next graphql.Resolver) (any, error) {
	fc := graphql.GetFieldContext(ctx)
	if fc == nil || fc.Object != "Query" {
		return next(ctx)
	}
	start := time.Now()
	res, err := next(ctx)
	status := "success"
	if err != nil {
		status = "error"
	}
	rootFieldSeconds.WithLabelValues(fc.Field.Name, status).Observe(time.Since(start).Seconds())
	return res, err
}
