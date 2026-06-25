package duck

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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
