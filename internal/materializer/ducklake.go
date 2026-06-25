package materializer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// DuckLakeMaterializer decodes din's raw layer entirely through the shared
// DuckLake catalog: it reads new rows from lake.raw_events via a snapshot
// diff, decodes them with model-garage, and writes lake.signals / lake.events
// — advancing a single snapshot cursor in lake.ingest_progress in the same
// transaction as the inserts. The transaction is the commit protocol:
// exactly-once by construction, concurrent-writer safe (a same-range race
// conflicts at commit and the loser retries from the new snapshot).
//
// There is no S3 LIST, no watermark.json, and no settle window: a row appears
// in lake.raw_events only after din committed its snapshot, so there is no
// pre-PUT key race to guard against. din's lake maintenance bounds history by
// LAKE_SNAPSHOT_RETENTION; the cursor must stay within that window.
type DuckLakeMaterializer struct {
	db  *sql.DB
	log zerolog.Logger
	// blobs resolves externalized payloads: din writes payloads larger than
	// the inline threshold to S3 and leaves only a data_index_key (under
	// BlobKeyPrefix) on the raw_events row. Without it those rows decode to
	// nothing. nil disables blob resolution (blobs not expected, e.g. tests).
	blobs      eventrepo.ObjectGetter
	blobBucket string
	// maxSnapshotSpan bounds the snapshot span processed per RunOnce pass so a
	// large backlog (lag, restart, historical backfill) is drained in
	// memory-bounded chunks instead of materializing the entire (cur, head]
	// delta at once and OOM-killing the single writer. <= 0 means unbounded.
	maxSnapshotSpan int64
	// tempDir is where per-batch parquet is staged for read_parquet. Empty uses
	// the OS default ($TMPDIR/tmp on the container root, which has no size limit);
	// set it (DUCKDB_TEMP_DIRECTORY) to the sized spill volume so a batch's temp
	// file lands there, not on the root fs.
	tempDir string
	// lastProgressReport / lastReportedSnapshot throttle the per-batch
	// expiry-floor heartbeat (see maybeReportProgress) and track what was last
	// published so the tail is flushed exactly once on catch-up.
	//
	// These are deliberately unsynchronized: the materializer is a single writer
	// (RunOnce/reportProgress run only on the one decode-loop goroutine, enforced by
	// the MaterializerShardCount>1 refusal in backend.go). If a second concurrent
	// caller of RunOnce/reportProgressNow is ever added, guard these with a mutex —
	// they would otherwise be a data race.
	lastProgressReport   time.Time
	lastReportedSnapshot int64
}

// snapshotCursorPartition is the single ingest_progress key holding the last
// raw_events snapshot id this decoder has processed.
const snapshotCursorPartition = "lake.raw_events#snapshot"

// defaultMaxSnapshotSpan caps snapshots processed per pass. din bundles flush at
// up to 128 MiB, so this bounds the per-pass working set; the Run loop re-polls
// immediately while a batch was processed, so a backlog still drains continuously.
const defaultMaxSnapshotSpan = 16

// NewDuckLakeMaterializer ensures the decoded tables + cursor row exist and
// returns a materializer over db (which must have the shared catalog attached
// as schema "lake", with din's raw_events present).
func NewDuckLakeMaterializer(ctx context.Context, db *sql.DB, log zerolog.Logger) (*DuckLakeMaterializer, error) {
	m := &DuckLakeMaterializer{
		db:              db,
		log:             log.With().Str("component", "ducklake-materializer").Logger(),
		maxSnapshotSpan: defaultMaxSnapshotSpan,
	}
	if err := m.ensureSchema(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// WithMaxSnapshotSpan overrides the per-pass snapshot-span bound (see the field
// doc). A non-positive n restores unbounded behavior. Returns m for chaining.
func (m *DuckLakeMaterializer) WithMaxSnapshotSpan(n int64) *DuckLakeMaterializer {
	m.maxSnapshotSpan = n
	return m
}

// WithBlobStore configures blob-payload resolution: when a raw_events row has
// no inline data but a data_index_key under BlobKeyPrefix, the materializer
// downloads the payload from bucket via getter before decoding. Mirrors the
// fetch path's LakeEventService.resolvePayload. Returns m for chaining.
func (m *DuckLakeMaterializer) WithBlobStore(getter eventrepo.ObjectGetter, bucket string) *DuckLakeMaterializer {
	m.blobs = getter
	m.blobBucket = bucket
	return m
}

// WithTempDir stages per-batch parquet under dir (the sized DuckDB spill volume)
// instead of the OS default temp dir. Empty keeps the default. Returns m.
func (m *DuckLakeMaterializer) WithTempDir(dir string) *DuckLakeMaterializer {
	m.tempDir = dir
	return m
}

func (m *DuckLakeMaterializer) ensureSchema(ctx context.Context) error {
	sigTmp, err := writeTempParquet(m.tempDir, writeSignalParquet, []SignalRow{})
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(sigTmp) }()
	evTmp, err := writeTempParquet(m.tempDir, writeEventParquet, []EventRow{})
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(evTmp) }()

	stmts := []string{
		// CREATE then ALTER the partition/sort layout, mirroring din's
		// raw_events (lake/ddl.go). Partitioning is catalog metadata applied to
		// data DuckLake writes from here on; the decoded tables don't exist in
		// prod yet (materializer disabled), so they are partitioned from the
		// first write and no re-materialization of old rows is needed. ALTER
		// SET is idempotent, so re-running on every boot is a no-op. "timestamp"
		// is quoted because it is a DuckDB keyword.
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS lake.signals AS SELECT * FROM read_parquet(%s) LIMIT 0", sqlLit(sigTmp)),
		`ALTER TABLE lake.signals SET PARTITIONED BY (subject_bucket, day("timestamp"))`,
		`ALTER TABLE lake.signals SET SORTED BY (subject, "timestamp")`,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS lake.events AS SELECT * FROM read_parquet(%s) LIMIT 0", sqlLit(evTmp)),
		`ALTER TABLE lake.events SET PARTITIONED BY (subject_bucket, day("timestamp"))`,
		`ALTER TABLE lake.events SET SORTED BY (subject, "timestamp")`,
		// signals_latest is the per-(subject,name) latest+summary rollup (CHD-3):
		// it makes latest/summary/availableSignals O(distinct-names) instead of a
		// full-history GROUP BY per request. Maintained per batch (refreshRollup)
		// by recomputing affected subjects from the deduped base table, so it is
		// a materialized view of getAllLatestSignalsLake (no source filter) —
		// parity is by construction. Partitioned by subject_bucket like the base.
		`CREATE TABLE IF NOT EXISTS lake.signals_latest (
			subject VARCHAR, subject_bucket INTEGER, name VARCHAR,
			"timestamp" TIMESTAMP WITH TIME ZONE,
			value_number DOUBLE, value_string VARCHAR,
			loc_lat DOUBLE, loc_lon DOUBLE, loc_hdop DOUBLE, loc_heading DOUBLE,
			count BIGINT, first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`,
		`ALTER TABLE lake.signals_latest SET PARTITIONED BY (subject_bucket)`,
		"CREATE TABLE IF NOT EXISTS lake.ingest_progress (partition VARCHAR, cursor VARCHAR)",
		// Seed the cursor row once so every advance is a compare-and-swap UPDATE
		// against a single row (CHD-9). Without a pre-seeded row the first writer
		// does a guard-less INSERT, and two concurrent first-writers both insert
		// and then both decode the same snapshot range. NOT EXISTS keeps
		// re-bootstrap (restart, second replica) idempotent; the conflict-retry
		// loop below serializes the rare concurrent first seed.
		fmt.Sprintf("INSERT INTO lake.ingest_progress (partition, cursor) "+
			"SELECT %s, '0' WHERE NOT EXISTS (SELECT 1 FROM lake.ingest_progress WHERE partition = %s)",
			sqlLit(snapshotCursorPartition), sqlLit(snapshotCursorPartition)),
		// din owns this table (the snapshot-expiry floor); create it defensively
		// so dq can report progress before din has booted against a fresh catalog.
		`CREATE TABLE IF NOT EXISTS meta.din_consumer_progress (consumer VARCHAR, snapshot_id BIGINT, updated_at TIMESTAMP WITH TIME ZONE)`,
	}
	// IF NOT EXISTS still raises a commit conflict when two materializers
	// bootstrap a fresh catalog at once (both transactions start before
	// either commits). Retry: by the next attempt the other transaction has
	// committed and IF NOT EXISTS is a no-op.
	for _, s := range stmts {
		var err error
		for attempt := 0; attempt < 5; attempt++ {
			if _, err = m.db.ExecContext(ctx, s); err == nil || !isCommitConflict(err) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}
		}
		if err != nil {
			return fmt.Errorf("ensuring lake schema: %w", err)
		}
	}
	return nil
}

// errSnapshotMoved means a concurrent decoder advanced the cursor first; the
// caller retries from the new cursor next pass.
var errSnapshotMoved = errors.New("snapshot cursor advanced by another writer")

// resetCursor advances the cursor from->to without decoding, used only on
// expired-feed recovery (maybeRecoverExpired). The skipped span is un-decoded
// data that was already expired from the change feed.
func (m *DuckLakeMaterializer) resetCursor(ctx context.Context, from, to int64) error {
	// CAS against the seeded row (from=0 matches the seeded '0'). If another
	// writer already advanced past `from`, the UPDATE matches nothing and the
	// reset is a no-op — that writer owns the recovery.
	_, err := m.db.ExecContext(ctx,
		"UPDATE lake.ingest_progress SET cursor = ? WHERE partition = ? AND cursor = ?",
		fmt.Sprint(to), snapshotCursorPartition, fmt.Sprint(from))
	return err
}

// RunOnce processes every raw_events row committed since the cursor in one
// transaction and returns the number of raw events consumed. Zero means the
// decoder is caught up.
func (m *DuckLakeMaterializer) RunOnce(ctx context.Context, dec eventDecoder) (int, error) {
	cur, err := m.cursor(ctx)
	if err != nil {
		return 0, err
	}
	head, err := m.headSnapshot(ctx)
	if err != nil {
		return 0, err
	}
	cursorSnapshotID.Set(float64(cur))
	headSnapshotID.Set(float64(head))
	if head <= cur {
		observeLakeLag(nil)           // caught up: no decode lag
		m.reportProgressNow(ctx, cur) // flush any throttled tail (no-op if already reported)
		return 0, nil
	}

	// Drain the (cur, head] backlog in memory-bounded snapshot-span chunks so a
	// large lag/restart/backfill can't materialize the whole delta at once and
	// OOM the single writer (which would then crash-loop on the same delta).
	// Every snapshot id is a valid CAS target, so chunking preserves
	// exactly-once; Run re-polls immediately while processed>0, so the backlog
	// still drains continuously. The loop only iterates past the first chunk to
	// skip empty sub-spans (windows of this decoder's own snapshots).
	for {
		processed, newCur, done, err := m.processChunk(ctx, cur, head, dec)
		if done {
			return processed, err
		}
		cur = newCur
	}
}

// processChunk drains one memory-bounded snapshot-span chunk of the (cur, head]
// backlog. done=true ends RunOnce: with processed>0 when a batch committed, or
// processed=0 when caught up, when a peer won the range (errSnapshotMoved), or on
// error. done=false returns newCur advanced past an empty sub-span (only this
// decoder's own snapshots) for the caller to skip and retry. Every snapshot id is a
// valid CAS target, so chunking preserves exactly-once.
func (m *DuckLakeMaterializer) processChunk(ctx context.Context, cur, head int64, dec eventDecoder) (processed int, newCur int64, done bool, err error) {
	to := head
	if m.maxSnapshotSpan > 0 && to-cur > m.maxSnapshotSpan {
		to = cur + m.maxSnapshotSpan
	}

	tReadDelta := time.Now()
	events, err := m.readDelta(ctx, cur, to)
	PerfStats.addDur(&PerfStats.readDelta, time.Since(tReadDelta))
	if err != nil {
		// Any feed-read failure might mean din's maintenance expired the cursor
		// range. Decide on retention (the oldest retained snapshot), not on the
		// error text — so a real expiry with unmatched wording can't wedge us
		// forever, and a transient error that merely looks like expiry can't make
		// us skip retained data.
		if n, handled, rerr := m.maybeRecoverExpired(ctx, cur, err); handled {
			return n, cur, true, rerr
		}
		return 0, cur, true, err
	}

	if len(events) > 0 {
		observeLakeLag(events) // decode lag = age of the oldest pending event
		tDecode := time.Now()
		decoded := dec.decodeEvents(ctx, events)
		PerfStats.addDur(&PerfStats.decode, time.Since(tDecode))
		PerfStats.addPass(int64(len(events)), int64(decoded.signalCount), int64(decoded.eventCount), int64(len(distinctSubjects(decoded.signals))))
		if cerr := m.commit(ctx, decoded, cur, to); cerr != nil {
			if errors.Is(cerr, errSnapshotMoved) {
				return 0, cur, true, nil // another decoder won this range; retry next pass
			}
			return 0, cur, true, cerr
		}
		// A batch committed: feed the freshness/throughput alerts (CHD-12).
		batchesTotal.WithLabelValues(lakeMetricType).Inc()
		rowsTotal.WithLabelValues("signals").Add(float64(decoded.signalCount))
		rowsTotal.WithLabelValues("events").Add(float64(decoded.eventCount))
		errorsTotal.Add(float64(decoded.errorCount))
		cursorSnapshotID.Set(float64(to))
		// Report progress to din's snapshot-expiry floor. Throttled: the batch is
		// already durable, and din only needs the floor within its retention window
		// — a lagging report just holds expiry back slightly (conservative, never
		// unsafe), so it needn't be a catalog txn on every batch.
		m.maybeReportProgress(ctx, to)
		return len(events), to, true, nil
	}

	// Empty span. If it reached head, the decoder is caught up: head advanced only
	// via this decoder's own writes. Don't burn a cursor-advance — the next pass
	// re-reads the empty range cheaply and moves once real data arrives.
	if to >= head {
		observeLakeLag(nil)           // no pending raw events: no decode lag
		m.reportProgressNow(ctx, cur) // flush the drained position once (no-op if unchanged)
		return 0, cur, true, nil
	}
	// Empty sub-span below head (only this decoder's own snapshots in the window):
	// advance the cursor past it so the next chunk reaches the data beyond, then
	// continue. CAS so a decoder that advanced first wins.
	if aerr := m.advanceCursor(ctx, cur, to); aerr != nil {
		if errors.Is(aerr, errSnapshotMoved) {
			return 0, cur, true, nil
		}
		return 0, cur, true, aerr
	}
	return 0, to, false, nil // advanced past the empty sub-span; caller continues
}

// advanceCursor moves the cursor from->to via compare-and-swap, used to skip an
// empty sub-span (no raw_events inserts) without decoding. RowsAffected==0 means
// another writer advanced first → errSnapshotMoved, and the caller retries.
func (m *DuckLakeMaterializer) advanceCursor(ctx context.Context, from, to int64) error {
	res, err := m.db.ExecContext(ctx,
		"UPDATE lake.ingest_progress SET cursor = ? WHERE partition = ? AND cursor = ?",
		fmt.Sprint(to), snapshotCursorPartition, fmt.Sprint(from))
	if err != nil {
		return fmt.Errorf("advance cursor over empty span: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return errSnapshotMoved
	}
	return nil
}

// maybeRecoverExpired classifies a readDelta error as an expired change feed
// using retention, not error text. It reads the oldest retained snapshot:
//   - oldest <= cur+1: the range is still retained, so the error was transient —
//     returns handled=false so the caller propagates it for retry (no data skip).
//   - oldest >  cur+1: din expired (cur, oldest); skip ONLY that unretained
//     prefix (to oldest-1) and resume decode from oldest — never jump to head,
//     which would drop everything still retained. Reports the recovered position
//     to din's expiry floor (a reset otherwise leaves it stale).
//
// returns (n, handled, err): handled means the caller should return (n, err).
func (m *DuckLakeMaterializer) maybeRecoverExpired(ctx context.Context, cur int64, cause error) (int, bool, error) {
	oldest, err := m.oldestSnapshot(ctx)
	if err != nil {
		// Can't determine retention; treat as transient so we never skip data on a
		// guess. The caller propagates the original error and retries.
		m.log.Warn().Err(err).Msg("could not read oldest snapshot to classify feed error")
		return 0, false, nil
	}
	if oldest <= cur+1 {
		return 0, false, nil // range still retained → genuine transient error
	}
	skipTo := oldest - 1 // resume decode from the oldest retained snapshot
	cursorResetsTotal.Inc()
	cursorResetGap.Set(float64(skipTo - cur))
	m.log.Error().Err(cause).Int64("from", cur).Int64("to", skipTo).Int64("oldest_retained", oldest).
		Int64("skipped_snapshots", skipTo-cur).
		Msg("DuckLake change feed expired below retention; skipping only the unretained prefix and resuming (increase LAKE_SNAPSHOT_RETENTION)")
	if err := m.resetCursor(ctx, cur, skipTo); err != nil {
		return 0, true, err
	}
	// The reset path skips the normal commit's progress report; do it here so
	// din's expiry floor reflects the post-gap position instead of staying stale.
	m.reportProgressNow(ctx, skipTo)
	return 0, true, nil
}

// oldestSnapshot returns the smallest retained snapshot id (0 when the catalog
// has none).
func (m *DuckLakeMaterializer) oldestSnapshot(ctx context.Context) (int64, error) {
	var oldest sql.NullInt64
	if err := m.db.QueryRowContext(ctx, "SELECT min(snapshot_id) FROM lake.snapshots()").Scan(&oldest); err != nil {
		return 0, fmt.Errorf("reading oldest snapshot: %w", err)
	}
	if !oldest.Valid {
		return 0, nil
	}
	return oldest.Int64, nil
}

// consumerName is the identity dq reports under in meta.din_consumer_progress.
// din takes MIN(snapshot_id) over live consumers as the expiry floor; all dq
// replicas share one logical cursor, so they all report under this name.
const consumerName = "dq"

// progressReportInterval throttles the per-batch expiry-floor heartbeat. din
// reads the floor at coarse (≤1h) granularity, so reporting every few seconds is
// ample and avoids a second catalog transaction on every committed batch.
const progressReportInterval = 5 * time.Second

// reportProgressNow publishes snapshotID immediately (and records it, resetting
// the throttle). A no-op if that id was already the last reported. Used where the
// floor must be current: catch-up and expiry-reset.
func (m *DuckLakeMaterializer) reportProgressNow(ctx context.Context, snapshotID int64) {
	if snapshotID == m.lastReportedSnapshot {
		return
	}
	// Stamp the throttle before the attempt so a failed write retries at the
	// interval (not every batch); advance the reported cursor only on success so a
	// transient failure is retried for the same snapshot, not skipped.
	m.lastProgressReport = time.Now()
	if m.reportProgress(ctx, snapshotID) {
		m.lastReportedSnapshot = snapshotID
	}
}

// maybeReportProgress reports at most once per progressReportInterval on the hot
// path; the unreported tail is flushed by reportProgressNow when the decoder
// catches up, so din's floor is never left stale while idle.
func (m *DuckLakeMaterializer) maybeReportProgress(ctx context.Context, snapshotID int64) {
	if snapshotID == m.lastReportedSnapshot {
		return
	}
	if !m.lastProgressReport.IsZero() && time.Since(m.lastProgressReport) < progressReportInterval {
		return
	}
	m.lastProgressReport = time.Now()
	if m.reportProgress(ctx, snapshotID) {
		m.lastReportedSnapshot = snapshotID
	}
}

// reportProgress upserts dq's processed snapshot id into din's consumer-floor
// table so the maintainer never expires snapshots dq hasn't read.
// reportProgress writes the floor and reports whether it succeeded. Best-effort: the
// batch is already durable and the floor is conservative, so a failure only holds expiry
// back briefly — the caller leaves the reported cursor unadvanced so the next pass
// retries. Count + log it so a persistent failure is visible on the dq side immediately,
// not only via din's DinConsumerStale ~1h later (which misattributes the cause).
func (m *DuckLakeMaterializer) reportProgress(ctx context.Context, snapshotID int64) bool {
	if err := m.tryReportProgress(ctx, snapshotID); err != nil {
		progressReportErrorsTotal.Inc()
		m.log.Warn().Err(err).Msg("consumer progress report failed; will retry next pass")
		return false
	}
	return true
}

func (m *DuckLakeMaterializer) tryReportProgress(ctx context.Context, snapshotID int64) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM meta.din_consumer_progress WHERE consumer = ?", consumerName); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO meta.din_consumer_progress VALUES (?, ?, now())", consumerName, snapshotID); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// eventDecoder is the materializer's decode surface (implemented by *Runner).
type eventDecoder interface {
	decodeEvents(ctx context.Context, events []cloudevent.RawEvent) *decodedBatch
}

func (m *DuckLakeMaterializer) cursor(ctx context.Context) (int64, error) {
	var raw sql.NullString
	err := m.db.QueryRowContext(ctx,
		"SELECT cursor FROM lake.ingest_progress WHERE partition = ?", snapshotCursorPartition).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) || !raw.Valid {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading snapshot cursor: %w", err)
	}
	var n int64
	if _, err := fmt.Sscan(raw.String, &n); err != nil {
		return 0, fmt.Errorf("parsing snapshot cursor %q: %w", raw.String, err)
	}
	return n, nil
}

func (m *DuckLakeMaterializer) headSnapshot(ctx context.Context) (int64, error) {
	var head sql.NullInt64
	if err := m.db.QueryRowContext(ctx, "SELECT max(snapshot_id) FROM lake.snapshots()").Scan(&head); err != nil {
		return 0, fmt.Errorf("reading head snapshot: %w", err)
	}
	if !head.Valid {
		return 0, nil
	}
	return head.Int64, nil
}

// readDelta reconstructs the raw events inserted in (from, to].
func (m *DuckLakeMaterializer) readDelta(ctx context.Context, from, to int64) ([]cloudevent.RawEvent, error) {
	q := fmt.Sprintf(
		"SELECT %s FROM ducklake_table_changes('lake', 'main', 'raw_events', %d, %d) WHERE change_type = 'insert'",
		duck.RawEventColumns, from+1, to)
	rows, err := m.db.QueryContext(ctx, q)
	if err != nil {
		// Don't classify here — RunOnce decides expired-vs-transient on retention.
		return nil, fmt.Errorf("reading raw_events delta: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	// Phase 1: scan every row + its blob key (no network in this loop).
	var out []cloudevent.RawEvent
	var blobKeys []string
	for rows.Next() {
		ev, blobKey, err := scanRawEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
		blobKeys = append(blobKeys, blobKey)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Phase 2: resolve externalized payloads concurrently — serial S3 GETs here
	// would block the single writer on N round-trips (blob-heavy batches are also
	// the largest to decode, so the latency stacks).
	return m.resolveBlobs(ctx, out, blobKeys)
}

// blobFetchConcurrency bounds the parallel blob downloads in resolveBlobs.
const blobFetchConcurrency = 16

// resolveBlobs fetches the externalized payloads for the rows that need them,
// concurrently and bounded, then drops any row whose blob is permanently missing
// (S3 404) — a transient fetch error still aborts the pass for retry, and a
// missing object store surfaces loudly (not a NotFound). Rows with inline
// payloads or no blob reference are untouched.
func (m *DuckLakeMaterializer) resolveBlobs(ctx context.Context, events []cloudevent.RawEvent, blobKeys []string) ([]cloudevent.RawEvent, error) {
	var fetch []int
	for i := range events {
		if len(events[i].Data) == 0 && events[i].DataBase64 == "" && strings.HasPrefix(blobKeys[i], eventrepo.BlobKeyPrefix) {
			fetch = append(fetch, i)
		}
	}
	if len(fetch) == 0 {
		return events, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(blobFetchConcurrency)
	missing := make([]bool, len(events))
	for _, idx := range fetch {
		g.Go(func() error {
			if err := m.resolveBlob(gctx, &events[idx], blobKeys[idx]); err != nil {
				if eventrepo.IsObjectNotFound(err) {
					blobMissingTotal.Inc()
					m.log.Error().Err(err).Str("id", events[idx].ID).Str("blob", blobKeys[idx]).
						Msg("raw_events blob payload permanently missing; skipping row")
					missing[idx] = true
					return nil
				}
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Compact out the permanently-missing rows, preserving order (in place: the
	// write index never overtakes the read index).
	kept := events[:0]
	for i := range events {
		if !missing[i] {
			kept = append(kept, events[i])
		}
	}
	return kept, nil
}

// resolveBlob populates ev.Data from S3 when ev carries no inline payload but a
// data_index_key under BlobKeyPrefix. din externalizes payloads larger than the
// inline threshold to a blob and leaves only the key on the raw_events row, so
// without this every such payload decodes to nothing (CHD-8). Mirrors the fetch
// path's LakeEventService.resolvePayload: din stores the raw decoded bytes at
// the key and the decode path reads ev.Data.
func (m *DuckLakeMaterializer) resolveBlob(ctx context.Context, ev *cloudevent.RawEvent, dataIndexKey string) error {
	if len(ev.Data) > 0 || ev.DataBase64 != "" {
		return nil // inline payload present
	}
	if !strings.HasPrefix(dataIndexKey, eventrepo.BlobKeyPrefix) {
		return nil // no blob reference: genuinely empty payload
	}
	if m.blobs == nil {
		return fmt.Errorf("raw_events row %s references blob payload %s but no object store is configured", ev.ID, dataIndexKey)
	}
	data, err := eventrepo.DownloadObject(ctx, m.blobs, m.blobBucket, dataIndexKey)
	if err != nil {
		return fmt.Errorf("fetching blob payload %s: %w", dataIndexKey, err)
	}
	ev.Data = data
	return nil
}

// restoreNonColumnFieldsSafe wraps cloudevent.RestoreNonColumnFields, which rebuilds
// Tags (and other non-column fields) from the extras map via unchecked type
// assertions — a malformed element (e.g. {"tags":[42]}) panics. din currently writes
// extras only from a typed Tags []string (validated at ingest), so a non-string tag
// never reaches storage; this is defense-in-depth at the din→dq trust boundary, not a
// live path. It matters because the scan runs OUTSIDE the decode recover: an unguarded
// panic here would propagate to RunOnce and crash-loop the single-writer materializer
// on that row forever (the cursor never advances). Contain it — keep the row with its
// scanned columns, skip the non-column reconstruction, count it via errorsTotal.
// TWIN: internal/service/duck.restoreNonColumnFieldsSafe (the fetch read path) has the
// same body but its own counter (dq_fetch_malformed_row_total) — mirror any change here.
func restoreNonColumnFieldsSafe(hdr *cloudevent.CloudEventHeader) {
	defer func() {
		if recover() != nil {
			errorsTotal.Inc()
		}
	}()
	cloudevent.RestoreNonColumnFields(hdr)
}

// scanRawEvent rebuilds a RawEvent from a raw_events row, restoring the
// non-column header fields from extras exactly like the parquet decoder.
func scanRawEvent(rows *sql.Rows) (cloudevent.RawEvent, string, error) {
	var ev cloudevent.RawEvent
	var ts time.Time
	var extras, data, dataIndexKey, voidsID sql.NullString
	var dataBase64 []byte
	if err := rows.Scan(&ev.Subject, &ts, &ev.Type, &ev.ID, &ev.Source, &ev.Producer,
		&ev.DataContentType, &ev.DataVersion, &extras, &data, &dataBase64, &dataIndexKey, &voidsID); err != nil {
		return ev, "", fmt.Errorf("scanning raw_events row: %w", err)
	}
	ev.SpecVersion = cloudevent.SpecVersion
	ev.Time = ts.UTC()
	if extras.Valid && extras.String != "" && extras.String != "{}" {
		ev.Extras = map[string]any{}
		if err := json.Unmarshal([]byte(extras.String), &ev.Extras); err != nil {
			// Malformed extras is a poison pill at the din→dq trust boundary: a hard
			// error here aborts the entire delta scan and crash-loops the single
			// writer on the same row forever (cursor never advances). Salvage exactly
			// like restoreNonColumnFieldsSafe's panic path — the core columns and Data
			// are already scanned and still decode; only the non-column header restore
			// is lost. Count it, drop the partial extras, and keep the event.
			errorsTotal.Inc()
			ev.Extras = nil
		} else {
			restoreNonColumnFieldsSafe(&ev.CloudEventHeader)
		}
	}
	if len(dataBase64) > 0 {
		ev.DataBase64 = string(dataBase64)
	} else if data.Valid {
		ev.Data = json.RawMessage(data.String)
	}
	// voids_id is selected (column parity with raw_events) but not applied:
	// the decode path routes only status->signals and events->events;
	// tombstones are skipped, and voiding is a read-side concern handled on
	// the raw query path. Discard it here. data_index_key is returned so the
	// caller can resolve an externalized blob payload before decode.
	_ = voidsID
	return ev, dataIndexKey.String, nil
}

// commit writes the decoded rows and advances the snapshot cursor in one
// transaction: the inserts and the cursor move atomically.
func (m *DuckLakeMaterializer) commit(ctx context.Context, dec *decodedBatch, from, to int64) error {
	var cleanup []string
	defer func() {
		for _, f := range cleanup {
			_ = os.Remove(f)
		}
	}()

	conn, err := m.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring conn: %w", err)
	}
	defer conn.Close() //nolint:errcheck
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if len(dec.signals) > 0 {
		tw := time.Now()
		tmp, err := writeTempParquet(m.tempDir, writeSignalParquet, dec.signals)
		PerfStats.addDur(&PerfStats.writeTmp, time.Since(tw))
		if err != nil {
			return err
		}
		cleanup = append(cleanup, tmp)
		ta := time.Now()
		_, aerr := tx.ExecContext(ctx, antiJoinInsert("lake.signals", tmp))
		PerfStats.addDur(&PerfStats.antiSig, time.Since(ta))
		if aerr != nil {
			return fmt.Errorf("insert signals: %w", aerr)
		}
	}
	if len(dec.events) > 0 {
		tw := time.Now()
		tmp, err := writeTempParquet(m.tempDir, writeEventParquet, dec.events)
		PerfStats.addDur(&PerfStats.writeTmp, time.Since(tw))
		if err != nil {
			return err
		}
		cleanup = append(cleanup, tmp)
		ta := time.Now()
		_, aerr := tx.ExecContext(ctx, antiJoinInsert("lake.events", tmp))
		PerfStats.addDur(&PerfStats.antiEvt, time.Since(ta))
		if aerr != nil {
			return fmt.Errorf("insert events: %w", aerr)
		}
	}

	// Refresh the latest/summary rollup for the subjects this batch touched, in
	// the same transaction so it advances atomically with the base rows (CHD-3).
	if len(dec.signals) > 0 {
		tr := time.Now()
		err := m.refreshRollup(ctx, tx, dec.signals)
		PerfStats.addDur(&PerfStats.rollup, time.Since(tr))
		if err != nil {
			return err
		}
	}

	// Advance the cursor as a compare-and-swap against the seeded row (from=0
	// matches the seeded '0'). RowsAffected==0 means another writer advanced it
	// first — back off and retry from the new cursor next pass. The seeded row
	// means there is never a guard-less INSERT branch for two writers to race.
	res, err := tx.ExecContext(ctx,
		"UPDATE lake.ingest_progress SET cursor = ? WHERE partition = ? AND cursor = ?",
		fmt.Sprint(to), snapshotCursorPartition, fmt.Sprint(from))
	if err != nil {
		return fmt.Errorf("advance cursor: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return errSnapshotMoved
	}

	tc := time.Now()
	cerr := tx.Commit()
	PerfStats.addDur(&PerfStats.commitTx, time.Since(tc))
	if cerr != nil {
		if isCommitConflict(cerr) {
			return errSnapshotMoved
		}
		return fmt.Errorf("commit: %w", cerr)
	}
	return nil
}

func isCommitConflict(err error) bool {
	// DuckLake reports "Transaction conflict - ..." / "Failed to commit
	// DuckLake transaction". Match those specifically so an unrelated error
	// that merely contains "conflict" isn't swallowed as a retryable race.
	s := err.Error()
	return strings.Contains(s, "Transaction conflict") ||
		strings.Contains(s, "Failed to commit DuckLake transaction")
}

// writeTempParquet writes rows via enc into a temp file under dir (empty = OS
// default) that DuckDB can read, and returns its path; the caller removes it.
func writeTempParquet[T any](dir string, enc func([]T) ([]byte, error), rows []T) (string, error) {
	body, err := enc(rows)
	if err != nil {
		return "", fmt.Errorf("encoding parquet: %w", err)
	}
	f, err := os.CreateTemp(dir, "ducklake-*.parquet")
	if err != nil {
		return "", fmt.Errorf("temp parquet: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("writing temp parquet: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("closing temp parquet: %w", err)
	}
	return f.Name(), nil
}

// sqlLit single-quotes a string literal for inlined DuckDB SQL (paths only).
func sqlLit(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// PruneDecoded deletes decoded rows older than retention from lake.signals and
// lake.events, returning the number removed. DuckLake snapshot expiry bounds
// history AGE (LAKE_SNAPSHOT_RETENTION) but not data SIZE, so without a
// row-level TTL the decoded tables grow unbounded (CHD-38). retention <= 0
// disables it — the default, since deleting customer history is a product
// decision. The rollup (lake.signals_latest) is left intact: it is current
// state, not history. din's catalog maintenance reclaims the deleted files.
func (m *DuckLakeMaterializer) PruneDecoded(ctx context.Context, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-retention).UTC().UnixMicro()
	var total int64
	for _, table := range []string{"lake.signals", "lake.events"} {
		res, err := m.db.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE timestamp < make_timestamp(%d)", table, cutoff))
		if err != nil {
			return total, fmt.Errorf("pruning %s: %w", table, err)
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}

// refreshRollup recomputes lake.signals_latest for every subject the batch
// touched, inside the commit transaction (CHD-3). It deletes the affected
// subjects' rollup rows and re-inserts a fresh per-(subject,name) latest+summary
// computed from the deduped base table, so the rollup is always a materialized
// view of getAllLatestSignalsLake (no source filter). Bounded to the batch's
// subjects, which prune by subject_bucket.
func (m *DuckLakeMaterializer) refreshRollup(ctx context.Context, tx *sql.Tx, signals []SignalRow) error {
	subjects := distinctSubjects(signals)
	if len(subjects) == 0 {
		return nil
	}
	start := time.Now()
	ph := strings.TrimSuffix(strings.Repeat("?,", len(subjects)), ",")
	args := make([]any, len(subjects))
	for i, s := range subjects {
		args[i] = s
	}
	// Prune the rollup delete and the base recompute to the batch's hash buckets.
	// signals_latest and lake.signals are PARTITIONED BY subject_bucket, so this
	// skips every partition but the batch's (SR-6) — the recompute reads only the
	// touched buckets' files, not the whole table. (The recompute is still
	// O(history within those buckets); the SR-1 incremental-merge follow-up would
	// make it O(batch), but that must replicate the (subject,name,timestamp)
	// read-dedup exactly, so it is deferred over correctness risk.)
	buckets := distinctBucketClause(subjects)
	where := "WHERE subject IN (" + ph + ")" + buckets
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.signals_latest "+where, args...); err != nil {
		return fmt.Errorf("rollup delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.signals_latest"+signalsLatestColumns+rollupSelectSQL(where), args...); err != nil {
		return fmt.Errorf("rollup insert: %w", err)
	}
	rollupRefreshSeconds.Set(time.Since(start).Seconds())
	return nil
}

// distinctBucketClause returns " AND subject_bucket IN (b1,b2,...)" for the hash
// buckets of the given subjects, or "" when empty. Buckets are small ints — a
// deterministic function of subject — so they are inlined like the read path's
// subjectBucketPredicate; no injection risk. duck.HashBucket matches the
// subject_bucket the decoder stamped (decode.go), so the partitions line up.
func distinctBucketClause(subjects []string) string {
	seen := make(map[int]struct{}, len(subjects))
	bs := make([]string, 0, len(subjects))
	for _, s := range subjects {
		b := duck.HashBucket(s)
		if _, ok := seen[b]; ok {
			continue
		}
		seen[b] = struct{}{}
		bs = append(bs, strconv.Itoa(b))
	}
	if len(bs) == 0 {
		return ""
	}
	return " AND subject_bucket IN (" + strings.Join(bs, ",") + ")"
}

// distinctSubjects returns the unique subjects in a decoded signal batch.
func distinctSubjects(signals []SignalRow) []string {
	seen := make(map[string]struct{}, len(signals))
	var out []string
	for i := range signals {
		s := signals[i].Subject
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// rollupSelectSQL builds the per-(subject,name) latest+summary SELECT over the
// deduped base table, restricted by whereClause (the full "WHERE …" string, or ""
// for the whole table). It mirrors getAllLatestSignalsLake's aggregation exactly
// (max/arg_max + (0,0)-loc FILTER + count/min/max), folding sources — the
// no-source-filter case the rollup serves. The QUALIFY dedup matches the read
// path (CHD-2). refreshRollup passes a subject-IN + bucket clause for the per-batch
// recompute; RecomputeRollup passes "" for a full rebuild.
// signalsLatestColumns names lake.signals_latest's columns in CREATE order so the
// rollup INSERTs below bind by name, not position. Without it, INSERT INTO … SELECT
// maps by position and rollupSelectSQL's "AS …" aliases are decorative — a reordered
// or appended SELECT column would silently shift every value one column over (data
// corruption, not an error). Named columns make that drift fail loud instead.
const signalsLatestColumns = ` (subject, subject_bucket, name, "timestamp", value_number, ` +
	`value_string, loc_lat, loc_lon, loc_hdop, loc_heading, count, first_seen, last_seen) `

func rollupSelectSQL(whereClause string) string {
	const locNonzero = "(loc_lat != 0 OR loc_lon != 0)"
	return fmt.Sprintf(`SELECT subject, any_value(subject_bucket) AS subject_bucket, name,
		max(timestamp) AS timestamp,
		arg_max(value_number, timestamp) AS value_number,
		arg_max(value_string, timestamp) AS value_string,
		coalesce(arg_max(loc_lat, timestamp) FILTER (WHERE %[1]s), 0) AS loc_lat,
		coalesce(arg_max(loc_lon, timestamp) FILTER (WHERE %[1]s), 0) AS loc_lon,
		coalesce(arg_max(loc_hdop, timestamp) FILTER (WHERE %[1]s), 0) AS loc_hdop,
		coalesce(arg_max(loc_heading, timestamp) FILTER (WHERE %[1]s), 0) AS loc_heading,
		CAST(count(*) AS BIGINT) AS count,
		min(timestamp) AS first_seen, max(timestamp) AS last_seen
	FROM (SELECT * FROM lake.signals %[2]s
	      QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, name, timestamp ORDER BY cloud_event_id) = 1)
	GROUP BY subject, name`, locNonzero, whereClause)
}

// RecomputeRollup rebuilds lake.signals_latest from scratch over the ENTIRE
// lake.signals base in one transaction — the O(history) full recompute, byte-
// identical to the per-batch refreshRollup but unrestricted. The per-batch path
// only recomputes subjects present in a batch, so a rollup that was dropped or
// truncated leaves dormant (no-longer-reporting) vehicles permanently missing from
// the latest/summary reads. This is the disaster-recovery rebuild for that case;
// it is opt-in (LAKE_REBUILD_ROLLUP_ON_BOOT) because over deep history it is
// expensive. Safe to run while the single-writer materializer is offline.
func (m *DuckLakeMaterializer) RecomputeRollup(ctx context.Context) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.signals_latest"); err != nil {
		return fmt.Errorf("rollup rebuild delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.signals_latest"+signalsLatestColumns+rollupSelectSQL("")); err != nil {
		return fmt.Errorf("rollup rebuild insert: %w", err)
	}
	return tx.Commit()
}

// antiJoinInsert builds the idempotent INSERT for a decoded table: it skips any
// source row whose cloudevent identity (cloud_event_id, name, timestamp) is
// already present at rest, pruned by subject_bucket (CHD-7). The pipeline is
// at-least-once at every seam, so the same cloudevent can be redelivered in a
// later snapshot; without this guard it would be decoded and stored twice,
// inflating every aggregate that reads the table. Intra-batch duplicates are
// already collapsed in decodeEvents (ev.Key()); this guards the cross-batch
// case. subject_bucket is a deterministic function of the (cloudevent-derived)
// subject, so adding it to the predicate is always satisfied for a true
// duplicate and lets DuckLake probe only the subject's partition.
func antiJoinInsert(table, parquetPath string) string {
	return fmt.Sprintf(
		"INSERT INTO %[1]s SELECT * FROM read_parquet(%[2]s) AS src "+
			"WHERE NOT EXISTS (SELECT 1 FROM %[1]s AS t "+
			"WHERE t.subject_bucket = src.subject_bucket "+
			"AND t.cloud_event_id = src.cloud_event_id "+
			"AND t.name = src.name AND t.timestamp = src.timestamp)",
		table, sqlLit(parquetPath))
}
