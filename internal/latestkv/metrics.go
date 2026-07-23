package latestkv

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Publish-side health for the signals-latest KV cache. NOT promauto, for the
// same reason as dq_materializer_* (H2): only the process that actually
// publishes (the materializer release) may export these — a query-fleet pod
// exporting flat zeros would defeat absent()-based alerting. Registration
// happens in registerMetrics, called from the Store constructor only.
var (
	// publishErrorsTotal is the alerting signal: a sustained increase means the
	// cache is going stale (entries heal per-subject on the next reading, or via
	// BootstrapFromRollup) while decode itself keeps succeeding.
	publishErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_latest_kv_publish_errors_total",
		Help: "Per-subject latest-KV publishes that failed; a sustained increase means the signals-latest cache is going stale.",
	})
	subjectsPublishedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_latest_kv_subjects_published_total",
		Help: "Subjects successfully folded into the signals-latest KV bucket.",
	})
	publishSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "dq_latest_kv_publish_seconds",
		Help: "Wall-clock of one decoded batch's KV publish (all touched subjects). Runs before the catalog commit, so growth here directly stretches the decode cycle.",
		// Sub-ms per-subject round trips at batch fan-out: 1ms..~16s.
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})
	bootstrapTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dq_latest_kv_bootstrap_timestamp_seconds",
		Help: "Unix time of the last completed BootstrapFromRollup on this process; 0 when none ran (normal after the first-ever bootstrap wrote its marker).",
	})
)

var registerMetricsOnce sync.Once

func registerMetrics() {
	registerMetricsOnce.Do(func() {
		prometheus.MustRegister(publishErrorsTotal, subjectsPublishedTotal, publishSeconds, bootstrapTimestamp)
	})
}
