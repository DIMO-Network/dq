package materializer

import (
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Decode-lag and throughput metrics. Lag is the SLO that matters at scale:
// the age of the oldest raw object the watermark has not passed yet.
var (
	lagSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dq_materializer_lag_seconds",
		Help: "Age of the oldest raw object not yet covered by the watermark, per cloudevent type. Zero when caught up.",
	}, []string{"type"})
	batchesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dq_materializer_batches_total",
		Help: "Raw batches fully committed (manifest + watermark).",
	}, []string{"type"})
	rowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dq_materializer_rows_total",
		Help: "Decoded rows written, by table.",
	}, []string{"table"})
	errorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_errors_total",
		Help: "Conversion failures and undecodable raw files (rows are salvaged where possible).",
	})
	compactionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_compactions_total",
		Help: "Decoded partitions merged by the decoded-layer compactor.",
	})
)

// ingestKeyTime extracts the ingest timestamp from a raw object name
// (ingest-<unixms>-<ulid>.parquet). Returns zero for other names
// (e.g. compacted c1- files, whose rows were already covered by older
// ingest keys).
func ingestKeyTime(key string) time.Time {
	name := path.Base(key)
	rest, ok := strings.CutPrefix(name, "ingest-")
	if !ok {
		return time.Time{}
	}
	msPart, _, ok := strings.Cut(rest, "-")
	if !ok {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(msPart, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// observeLag sets the per-type lag gauge from the oldest pending key.
func observeLag(ceType string, batches []rawBatch) {
	var oldest time.Time
	for _, b := range batches {
		for _, key := range b.keys {
			if ts := ingestKeyTime(key); !ts.IsZero() && (oldest.IsZero() || ts.Before(oldest)) {
				oldest = ts
			}
		}
	}
	if oldest.IsZero() {
		lagSeconds.WithLabelValues(ceType).Set(0)
		return
	}
	lagSeconds.WithLabelValues(ceType).Set(time.Since(oldest).Seconds())
}
