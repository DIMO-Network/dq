package repositories

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// segmentGapFillErrorsTotal counts transient backend errors during best-effort segment
// location gap-fill. The caller falls back to (0,0), so without this the error is invisible —
// a rising count points at a flaky location lookup rather than genuinely location-less trips.
var segmentGapFillErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_segment_gapfill_location_errors_total",
	Help: "Transient backend errors during best-effort segment start/end location gap-fill; caller falls back to (0,0).",
})
