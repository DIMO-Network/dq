package duck

import (
	"database/sql"
	"sync"
	"time"

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
