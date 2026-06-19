package materializer

import (
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
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
	cursorResetsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_cursor_resets_total",
		Help: "DuckLake snapshot cursor resets after the consumer lagged past LAKE_SNAPSHOT_RETENTION (expired change feed). Each reset skips an un-decoded gap — alert on any increase.",
	})
	// cursorSnapshotID / headSnapshotID expose the DuckLake decode position so
	// head - cursor is the snapshot backlog at a glance (CHD-12).
	cursorSnapshotID = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dq_materializer_cursor_snapshot_id",
		Help: "DuckLake snapshot id the decoder has processed up to (lake.ingest_progress cursor).",
	})
	headSnapshotID = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dq_materializer_head_snapshot_id",
		Help: "Latest committed DuckLake snapshot id (raw_events head). head - cursor is the snapshot backlog.",
	})
	cursorResetGap = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dq_materializer_last_cursor_reset_gap_snapshots",
		Help: "Snapshot span skipped by the most recent cursor reset (to - from). Non-zero means un-decoded data was permanently skipped — backfill the range.",
	})
)

// lakeMetricType labels the DuckLake-path materializer metrics. The bucket path
// labels lag/batches by cloudevent type; the lake path commits mixed types per
// snapshot, so it reports under one series.
const lakeMetricType = "ducklake"

// observeLakeLag sets the decode-lag gauge from the oldest un-decoded event in a
// DuckLake snapshot delta (now - min(event time)); zero when caught up. This is
// what makes the DecodeLag / Stalled alerts live in ducklake mode — before
// CHD-12 the lake path emitted only cursor resets, so those alerts were dead.
func observeLakeLag(events []cloudevent.RawEvent) {
	var oldest time.Time
	for i := range events {
		ts := events[i].Time
		if !ts.IsZero() && (oldest.IsZero() || ts.Before(oldest)) {
			oldest = ts
		}
	}
	if oldest.IsZero() {
		lagSeconds.WithLabelValues(lakeMetricType).Set(0)
		return
	}
	lagSeconds.WithLabelValues(lakeMetricType).Set(time.Since(oldest).Seconds())
}

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
