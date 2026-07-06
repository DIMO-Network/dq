package materializer

import (
	"sync"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/prometheus/client_golang/prometheus"
)

// Decode-lag and throughput metrics. Lag is the SLO that matters at scale:
// the age of the oldest raw event the decoder has not consumed yet.
//
// NOT promauto: this package is imported by every dq binary (internal/app),
// but only processes that actually construct a materializer may export
// dq_materializer_* series. A query-fleet pod exporting the cursor/head
// gauges as 0 defeats the absent()-based DQMaterializerDown alert (the gauge
// is never absent while any query pod is up) and drags the
// dq:pipeline_snapshot_backlog record to 0−0 during a real materializer
// outage (H2). Registration happens in registerMetrics, called from the
// materializer constructors only.
var (
	lagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dq_materializer_lag_seconds",
		Help: "Age of the oldest raw event not yet decoded. Zero when caught up.",
	}, []string{"type"})
	batchesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dq_materializer_batches_total",
		Help: "Raw batches committed to the DuckLake catalog.",
	}, []string{"type"})
	rowsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dq_materializer_rows_total",
		Help: "Decoded rows written, by table.",
	}, []string{"table"})
	errorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_errors_total",
		Help: "Conversion failures and undecodable raw events (rows are salvaged where possible).",
	})
	// pruneErrorsTotal makes a silently-failing decoded-retention prune alertable
	// instead of only logged (SR-14).
	pruneErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_prune_errors_total",
		Help: "Decoded-retention prune passes that failed.",
	})
	// passErrorsTotal makes a wedged decode loop alertable. A RunOnce spinning on the
	// same readDelta/commit/CAS error increments this every pass while the cursor and
	// lag gauges stay flat (cursorSnapshotID is re-published unchanged, lag isn't
	// touched on the error path) — so the Down (absent-gauge) and Stalled (lag>0)
	// alerts miss it. Alert on a sustained increase here instead.
	passErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_pass_errors_total",
		Help: "RunOnce passes that returned an error (readDelta/commit/CAS); a sustained increase means the decode loop is wedged.",
	})
	// rollupRefreshSeconds tracks the cost of the decoupled signals_latest
	// maintenance pass (FlushRollup): a bucket-partitioned recompute of every
	// subject_bucket dirtied since the last flush, run OFF the decode commit. This
	// gauge makes its growth with history visible (the SR-1 incremental-merge
	// follow-up would make it O(batch)).
	rollupRefreshSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dq_materializer_rollup_refresh_seconds",
		Help: "Wall-clock of the most recent signals_latest rollup flush (bucket-partitioned recompute of dirty buckets).",
	})
	// rollupFlushErrorsTotal counts decoupled signals_latest flush failures. The
	// base tables stay durable and the rollup self-heals on the next flush, so a
	// flush failure is non-fatal — but a sustained increase means latest/summary
	// reads are going stale; alert on it.
	rollupFlushErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_rollup_flush_errors_total",
		Help: "signals_latest flush passes that failed; a sustained increase means the latest/summary view is stale.",
	})
	cursorResetsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_cursor_resets_total",
		Help: "DuckLake snapshot cursor resets after the consumer lagged past LAKE_SNAPSHOT_RETENTION (expired change feed). Each reset skips an un-decoded gap — alert on any increase.",
	})
	// cursorSnapshotID / headSnapshotID expose the DuckLake decode position so
	// head - cursor is the snapshot backlog at a glance (CHD-12).
	cursorSnapshotID = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dq_materializer_cursor_snapshot_id",
		Help: "DuckLake snapshot id the decoder has processed up to (lake.ingest_progress cursor).",
	})
	headSnapshotID = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dq_materializer_head_snapshot_id",
		// Catalog-global max snapshot id — it includes this decoder's own
		// signals/events/rollup writes and din's maintenance snapshots, so
		// head - cursor is NOT the raw_events backlog and stays non-zero even when
		// fully caught up. Use dq_materializer_lag_seconds for decode lag.
		Help: "Latest catalog snapshot id (NOT a backlog gauge; see dq_materializer_lag_seconds).",
	})
	cursorResetGap = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dq_materializer_last_cursor_reset_gap_snapshots",
		Help: "Snapshot span skipped by the most recent cursor reset (to - from). Non-zero means un-decoded data was permanently skipped — backfill the range.",
	})
	// blobMissingTotal counts raw_events rows whose externalized payload is
	// permanently gone (S3 NoSuchKey/404). Such a row is skipped so it can't wedge
	// the whole delta; alert on any increase — it means decoded data was dropped.
	blobMissingTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_blob_missing_total",
		Help: "raw_events rows skipped because their externalized blob payload was permanently missing (S3 404).",
	})
	// blobPoisonTotal counts raw_events rows skipped because their externalized
	// payload is a DETERMINISTIC poison pill — one that would abort and retry the
	// same delta forever, wedging the whole fleet's decode (S6/S7). reason:
	// "oversize" (blob exceeds eventrepo max size), "decrypt" (wrong key / corrupt
	// / truncated), or "sealed_no_key" (sealed blob but no BLOB_ENCRYPTION_KEY set).
	// Transient S3 errors still abort the pass for retry — only deterministic
	// failures are classified here. Alert on any increase: decoded data was dropped.
	blobPoisonTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dq_materializer_blob_poison_total",
		Help: "raw_events rows skipped because their externalized blob payload is a deterministic poison pill (oversize/decrypt/sealed_no_key).",
	}, []string{"reason"})
	// progressReportErrorsTotal counts failures writing dq's snapshot floor to
	// meta.din_consumer_progress. Decode keeps succeeding (a separate txn) so dq's own
	// lag/cursor gauges stay healthy — without this counter the only signal is din's
	// DinConsumerStale ~1h later, which misattributes the cause to a dropped consumer.
	progressReportErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dq_materializer_progress_report_errors_total",
		Help: "Failures writing the consumer-progress floor; din's snapshot expiry stops advancing for dq.",
	})
)

// registerMetricsOnce guards registerMetrics: New and NewDuckLakeMaterializer
// both call it, and tests construct many runners per process.
var registerMetricsOnce sync.Once

// registerMetrics exports the dq_materializer_* set with the default registry.
// Called from the materializer constructors (New, NewDuckLakeMaterializer) so
// only processes that actually run a materializer expose the series — see the
// package var block for why query-fleet pods must not (H2).
func registerMetrics() {
	registerMetricsOnce.Do(func() {
		prometheus.MustRegister(
			lagSeconds, batchesTotal, rowsTotal, errorsTotal,
			pruneErrorsTotal, passErrorsTotal, rollupRefreshSeconds,
			rollupFlushErrorsTotal, cursorResetsTotal, cursorSnapshotID,
			headSnapshotID, cursorResetGap, blobMissingTotal, blobPoisonTotal,
			progressReportErrorsTotal,
		)
	})
}

// lakeMetricType is the single value of the "type" label on the materializer
// lag/batch metrics. The materializer commits mixed cloudevent types per snapshot,
// so all series report under this one label. (This is a Prometheus/alert wire
// contract — don't rename the value.)
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
