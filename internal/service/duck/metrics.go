package duck

import (
	"database/sql"
	"sync"

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
