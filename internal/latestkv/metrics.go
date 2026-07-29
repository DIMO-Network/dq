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
	// casRetriesTotal counts entry writes that lost a compare-and-swap and were
	// re-folded. A low rate is normal whenever two writers touch the same subject
	// at once (a coverage reconcile alongside live publishing, or a RunBackfill);
	// a sustained high rate means they are contending hard enough to be worth
	// separating in time.
	casRetriesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_latest_kv_cas_retries_total",
		Help: "Entry writes that lost a compare-and-swap and were retried against the newer value.",
	})
	bootstrapTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dq_latest_kv_bootstrap_timestamp_seconds",
		Help: "Unix time of the last completed BootstrapFromRollup on this process; 0 when none ran (normal after the first-ever bootstrap wrote its marker).",
	})
	// coverageProven is the writer's half of the negative-serving contract
	// (coverage.go): 1 while this process holds a proof that the bucket mirrors
	// lake.signals_latest and is heartbeating it, 0 otherwise. It going to 0 is
	// the leading indicator that query pods are about to stop answering "no
	// data" from the cache and fall back to ~795ms rollup reads.
	coverageProven = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dq_latest_kv_coverage_proven",
		Help: "1 when the writer holds a proof of signals-latest coverage and is asserting it; 0 when unproven or degraded.",
	})
	// coverageVerifiedTimestamp is the last successful heartbeat WRITE. Readers
	// stop trusting the assertion CoverageMaxStaleness after this, so alert on
	// its age rather than on coverageProven alone: a proven writer that cannot
	// reach NATS reads as healthy here and still loses its readers.
	coverageVerifiedTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dq_latest_kv_coverage_verified_timestamp_seconds",
		Help: "Unix time of the last successful coverage heartbeat write.",
	})
	reconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dq_latest_kv_reconcile_total",
		Help: "Coverage reconcile passes over lake.signals_latest by result (ok|error). Expected to be rare: boot after an unclean exit, or recovery from a publish failure.",
	}, []string{"result"})
	reconcileSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "dq_latest_kv_reconcile_seconds",
		Help: "Wall-clock of one coverage reconcile pass (full lake.signals_latest scan plus per-subject folds).",
		// A full pass is tens of seconds at 256 bucket partitions: 0.1s..~55m.
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 16),
	})
)

var registerMetricsOnce sync.Once

func registerMetrics() {
	registerMetricsOnce.Do(func() {
		prometheus.MustRegister(publishErrorsTotal, subjectsPublishedTotal, publishSeconds, bootstrapTimestamp, casRetriesTotal)
	})
}

var registerCoverageMetricsOnce sync.Once

// registerCoverageMetrics is deliberately NOT part of registerMetrics: the
// query fleet constructs a Store too (to read the cache), so anything
// registered there appears on query pods as flat zeros. A permanently-zero
// dq_latest_kv_coverage_proven on every query pod would make the obvious alert
// on it useless. Only a process that constructs a CoverageReporter — the live
// materializer — asserts coverage, so only it exports these.
func registerCoverageMetrics() {
	registerCoverageMetricsOnce.Do(func() {
		prometheus.MustRegister(coverageProven, coverageVerifiedTimestamp, reconcileTotal, reconcileSeconds)
	})
}
