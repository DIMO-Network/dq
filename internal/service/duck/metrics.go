package duck

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DIMO-Network/dq/internal/latestkv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// poolStats exports database/sql pool statistics for every duck.Service in
// the process (H4). WaitCount/WaitDuration are THE saturation signal for the
// small DuckDB connection pool — a rising wait rate means queries are queueing
// on the pool and the global admission cap / replica count needs attention.
// One collector, pools labeled, because a materializer pod runs two services
// (query backend + decode loop) and duplicate plain gauges would panic.
var poolStats = &poolStatsCollector{pools: map[string]*sql.DB{}}

// Registered in init(), not in the var initializer: Register calls Describe,
// which reads the pool*Desc vars — inside a var initializer those may not be
// initialized yet (dependency-order analysis can't see through the method).
// The collector emits no series until a Service tracks a pool, so this is
// safe in every binary.
func init() { prometheus.MustRegister(poolStats) }

type poolStatsCollector struct {
	mu    sync.Mutex
	pools map[string]*sql.DB
}

func (c *poolStatsCollector) track(name string, db *sql.DB) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pools[name] = db
}

var (
	poolOpenDesc  = prometheus.NewDesc("dq_db_pool_open_connections", "Open connections in the DuckDB pool.", []string{"pool"}, nil)
	poolInUseDesc = prometheus.NewDesc("dq_db_pool_in_use_connections", "Connections currently executing queries.", []string{"pool"}, nil)
	poolIdleDesc  = prometheus.NewDesc("dq_db_pool_idle_connections", "Idle connections in the pool.", []string{"pool"}, nil)
	poolMaxDesc   = prometheus.NewDesc("dq_db_pool_max_open_connections", "Configured pool ceiling (DUCKDB_MAX_CONNS).", []string{"pool"}, nil)
	poolWaitDesc  = prometheus.NewDesc("dq_db_pool_wait_count_total", "Requests that had to wait for a connection.", []string{"pool"}, nil)
	poolWaitSDesc = prometheus.NewDesc("dq_db_pool_wait_duration_seconds_total", "Cumulative time requests spent waiting for a connection.", []string{"pool"}, nil)
)

func (c *poolStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- poolOpenDesc
	ch <- poolInUseDesc
	ch <- poolIdleDesc
	ch <- poolMaxDesc
	ch <- poolWaitDesc
	ch <- poolWaitSDesc
}

func (c *poolStatsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, db := range c.pools {
		s := db.Stats()
		ch <- prometheus.MustNewConstMetric(poolOpenDesc, prometheus.GaugeValue, float64(s.OpenConnections), name)
		ch <- prometheus.MustNewConstMetric(poolInUseDesc, prometheus.GaugeValue, float64(s.InUse), name)
		ch <- prometheus.MustNewConstMetric(poolIdleDesc, prometheus.GaugeValue, float64(s.Idle), name)
		ch <- prometheus.MustNewConstMetric(poolMaxDesc, prometheus.GaugeValue, float64(s.MaxOpenConnections), name)
		ch <- prometheus.MustNewConstMetric(poolWaitDesc, prometheus.CounterValue, float64(s.WaitCount), name)
		ch <- prometheus.MustNewConstMetric(poolWaitSDesc, prometheus.CounterValue, s.WaitDuration.Seconds(), name)
	}
}

// lakeLatestServedTotal counts how latest/summary/availableSignals reads were
// answered: from the precomputed signals_latest rollup (O(distinct-names)) or by
// a full deduped-history scan. A high "scan" rate means source-filtered or
// location queries are bypassing the rollup — visibility for the SR-7 / SR-1
// rollup-coverage gap, which is otherwise invisible.
var lakeLatestServedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "dq_lake_latest_served_total",
	Help: "Latest/summary/available reads by serving path: rollup or scan.",
}, []string{"path"})

func observeLakePath(rollup bool) {
	if rollup {
		lakeLatestServedTotal.WithLabelValues("rollup").Inc()
		return
	}
	lakeLatestServedTotal.WithLabelValues("scan").Inc()
}

// lakeReadSeconds is the range/fetch counterpart of lakeLatestQuerySeconds:
// per-op wall-clock for the lake reads the signals-latest cache does NOT
// serve. These are what remains of dq's read latency after the KV flip
// (2026-07-27) — the oracle's mirrored-read panel blends them with the
// now-sub-ms latest reads, so this is the op-level split that says whether a
// slow read is the shared DuckLake planning floor or genuine scan volume.
//
// One op per query shape, never per entry point: an op that blends shapes has
// a multi-modal histogram, and its quantiles then describe no query anyone
// actually ran. Ops:
//
//   - signalsRange, signalsRanges — the aggregation reads.
//   - events, eventCounts, eventCountsRanges — the lake.events reads.
//   - eventSummariesRollup, eventSummariesScan — the O(distinct-names) rollup
//     read and the full-history scan. Split because one request can do both:
//     an empty rollup falls back to the scan, so a scan observation appearing
//     at all is itself the signal that events_latest is missing or unpopulated.
//   - fetchIndexSearch, fetchLatestIndex — bounded index scan vs LIMIT 1. Same
//     projection, different shape.
//   - fetchByID, fetchByIDBatch — single point lookup vs a chunked IN-list of
//     up to maxLakeQueryLimit ids.
//   - fetchTypeSummary — the per-type aggregate.
//   - fetchPayload — per-event S3 blob resolution, the only op here whose cost
//     is off-lake.
var lakeReadSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "dq_lake_read_seconds",
	Help:    "Wall-clock of non-latest lake reads by op (range aggregations, event queries, cloudevent fetch scans and payload resolution).",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
}, []string{"op"})

// observeLakeRead records one non-latest lake read. Call deferred with
// time.Now() evaluated at the start:
//
//	defer observeLakeRead("signalsRange", time.Now())
func observeLakeRead(op string, start time.Time) {
	lakeReadSeconds.WithLabelValues(op).Observe(time.Since(start).Seconds())
}

// kvReadTotal counts signals-latest cache reads by outcome. In serve mode
// every non-hit outcome is a rollup fallback, so a rising miss/error/version
// rate under KVReadServe means the cache is silently degrading — the read-side
// analogue of dq_latest_kv_publish_errors_total.
var kvReadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "dq_lake_latest_kv_read_total",
	Help: "signals-latest KV cache reads by outcome (hit|miss|error|version); non-hits fall back to the rollup path.",
}, []string{"outcome"})

// kvFalseNegativeTotal / kvTrueNegativeTotal are the dark launch for
// authoritative-miss serving (LATEST_KV_NEGATIVE=shadow, dq#42): of the misses
// the reader would have answered "no data", how many did the rollup actually
// have rows for. false_negative must be flat at ZERO before flipping to serve —
// it is the invariant "every subject in lake.signals_latest has a key in the
// bucket" failing on real traffic, and each one would be a user-visible wrong
// answer. The true-negative counter is the denominator; without it a zero
// numerator could just mean the path never ran.
var kvFalseNegativeTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_lake_latest_kv_false_negative_total",
	Help: "Shadow misses the cache would have answered 'no data' for but the rollup had rows for. MUST be zero before LATEST_KV_NEGATIVE=serve.",
})

var kvTrueNegativeTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_lake_latest_kv_true_negative_total",
	Help: "Shadow misses the rollup confirmed empty — the denominator for dq_lake_latest_kv_false_negative_total.",
})

// activeCoverageWatcher is the watcher the kvCoverageTrusted GaugeFunc reads.
// An atomic indirection because the gauge is a package-level singleton while the
// watcher arrives later, at WithLatestKVNegative time.
var activeCoverageWatcher atomic.Pointer[latestkv.CoverageWatcher]

// kvCoverageTrusted is the reader's live view of the writer's coverage
// assertion: 1 while a cache miss may be answered as "no data", 0 while misses
// fall back to the rollup.
//
// A GaugeFunc, evaluated at scrape time, NOT a gauge set on each query. It was
// the latter first, on the reasoning that a per-query write reflects what
// queries actually observed — but that conflates two very different states into
// the same 0: "coverage is not trusted" and "no eligible query has run yet". On
// a node whose load is mirrored from a partner oracle, the second is the normal
// state for most of the day, so the series carried no information exactly when
// you most wanted to check it — after a deploy, before traffic. Scrape-time
// evaluation asks the watcher directly and is correct with zero traffic.
//
// Registered lazily (registerCoverageTrusted) rather than by promauto: a fleet
// with LATEST_KV_NEGATIVE=off would otherwise export a permanent 0, which reads
// as "untrusted" to any alert on it. Absent is the honest answer there.
var kvCoverageTrusted = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
	Name: "dq_lake_latest_kv_coverage_trusted",
	Help: "1 when this query pod trusts the signals-latest coverage assertion (cache misses answered as 'no data'), 0 when misses fall back to the rollup.",
}, func() float64 {
	return boolToFloat(activeCoverageWatcher.Load().Trusted())
})

var registerCoverageTrustedOnce sync.Once

// registerCoverageTrusted exports the trust gauge, once, for a pod that actually
// has authoritative-miss serving configured.
func registerCoverageTrusted() {
	registerCoverageTrustedOnce.Do(func() { prometheus.MustRegister(kvCoverageTrusted) })
}

// kvShadowTotal classifies shadow-mode comparisons (KVReadShadow): match,
// kv_newer (the benign pre-commit-fold freshness race), kv_miss (coverage
// gap), and mismatch — the one class that must stay at zero before flipping
// LATEST_KV_READ_MODE to serve.
var kvShadowTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "dq_lake_latest_kv_shadow_total",
	Help: "Shadow comparisons of the signals-latest KV cache against the rollup (match|kv_newer|kv_miss|mismatch).",
}, []string{"result"})

// lakeLatestQuerySeconds measures the wall-clock duration of latest/summary/
// available reads, split by serving path (rollup vs scan) and operation. This
// is the true single-read latency the pool-saturation symptom lacked: a
// subject-scoped rollup point-read SHOULD be single-digit ms, so a rollup-path
// p50 in the hundreds-of-ms/seconds range is direct evidence that per-query
// DuckLake planning (snapshot resolution + file listing over the fragmented,
// constantly-rewritten rollup tables) — not scan volume — is what saturates the
// read pool under the morning mirror burst. Buckets span 1ms..~32s.
var lakeLatestQuerySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "dq_lake_latest_query_seconds",
	Help:    "Wall-clock duration of latest/summary/available reads by serving path (rollup|scan) and operation.",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
}, []string{"path", "op"})

// observeLakeQuery records the duration of a latest/summary/available read.
// Call it deferred with time.Now() evaluated at the query start:
//
//	defer observeLakeQuery(rollup, "signalsLatest", time.Now())
func observeLakeQuery(rollup bool, op string, start time.Time) {
	path := "scan"
	if rollup {
		path = "rollup"
	}
	lakeLatestQuerySeconds.WithLabelValues(path, op).Observe(time.Since(start).Seconds())
}

// fetchBlobMissingTotal counts fetch reads whose externalized blob payload was
// permanently missing (404 / aged out of retention) and was returned as an empty
// payload rather than failing the whole multi-event fetch.
var fetchBlobMissingTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_fetch_blob_missing_total",
	Help: "Fetch reads whose externalized blob payload was permanently missing (returned empty, not errored).",
})

// fetchMalformedRowTotal makes a poisoned raw_events row (one whose extras panic
// cloudevent.RestoreNonColumnFields) alertable instead of silently dropped on the
// read path. See restoreNonColumnFieldsSafe.
var fetchMalformedRowTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_fetch_malformed_row_total",
	Help: "raw_events rows whose non-column fields could not be restored (malformed extras); the row is kept without them.",
})

// blobPrefixAnomalyTotal counts rows whose data_index_key is non-empty but NOT
// under eventrepo.BlobKeyPrefix — a din BLOB_PREFIX misconfig that would silently
// empty every externalized payload (S6). Incremented on BOTH the fetch read path
// (resolvePayload, below) and the materializer scan path (via
// ObserveBlobPrefixAnomaly). It lives here — not in internal/materializer —
// because the duck package's promauto metrics register in EVERY dq process (the
// materializer imports duck), so a second registration of the same name in the
// materializer package would panic on the shared default registry.
var blobPrefixAnomalyTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_blob_prefix_anomaly_total",
	Help: "raw_events rows with a data_index_key not under BlobKeyPrefix; the externalized payload is served/decoded empty (likely a din BLOB_PREFIX misconfig).",
})

// ObserveBlobPrefixAnomaly increments the blob-prefix-drift counter. Exported so
// the materializer scan path shares this single registration (see the var doc).
func ObserveBlobPrefixAnomaly() { blobPrefixAnomalyTotal.Inc() }
