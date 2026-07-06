package materializer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/dq/pkg/blobcrypt"
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
	// blobCipher decrypts payloads din sealed client-side (nil = read as-is).
	blobCipher *blobcrypt.Cipher
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
	// lastRawMissingLog throttles the "lake.raw_events not present yet" info log so
	// a dq that booted before din doesn't log every poll (S8). Single-writer, so
	// unsynchronized like the progress fields.
	lastRawMissingLog time.Time

	// backfillMode tunes the writer for a large one-time catch-up: it skips the
	// cross-batch dedup anti-join on insert (a clean historical load carries no
	// redeliveries, and the read path dedups regardless). Steady state leaves it
	// false so the bounded anti-join still guards against NATS redeliveries.
	backfillMode bool
	// dirtySubjects accumulates the subjects whose lake.signals changed since
	// the last FlushRollup. The rollup is maintained OFF the decode commit (a
	// materialized view should never block the writer); FlushRollup recomputes
	// only these subjects' rollup rows. Subjects, not buckets (B2): bucket
	// dirtiness saturates — with ~1.5k active vehicles per flush window all
	// 256 buckets are dirty and a bucket recompute is a full-table recompute,
	// O(retained history) per flush on the decode goroutine. Subject-scoped
	// recompute is O(active subjects' own histories), keeps the same
	// self-healing exactness, and is bounded per txn by rollupSubjectChunk.
	// Single-writer: mutated only on the decode-loop goroutine
	// (commit/FlushRollup), so unsynchronized like the progress fields.
	dirtySubjects map[string]struct{}
	// rollupFullRebuild escalates the next FlushRollup to a full
	// RecomputeRollup: set when the dirty set overflowed maxDirtySubjects
	// (fleet-wide catch-up) — per-subject tracking is no cheaper than a
	// rebuild at that point, and the map must not grow unbounded.
	rollupFullRebuild bool
	// maxDirtySubjects is the overflow cap (defaultMaxDirtySubjects).
	maxDirtySubjects int
}

// WithBackfillMode toggles backfill tuning (skip the cross-batch dedup anti-join;
// the caller defers rollup maintenance to a single FlushRollup at catch-up).
// Returns m for chaining.
func (m *DuckLakeMaterializer) WithBackfillMode(on bool) *DuckLakeMaterializer {
	m.backfillMode = on
	return m
}

// snapshotCursorPartition is the single ingest_progress key holding the last
// raw_events snapshot id this decoder has processed.
const snapshotCursorPartition = "lake.raw_events#snapshot"

// defaultMaxSnapshotSpan caps how many snapshots one RunOnce pass drains. It
// bounds the pass by snapshot COUNT, not by bytes: the real worst-case working set
// is up to span × the per-bundle inline payload size — din flushes bundles at up to
// 128 MiB, so on the order of ~2 GiB of inline payload at span=16 — PLUS the resident
// bytes of every blob resolved for the pass, which is currently UNBOUNDED (resolveBlobs
// holds all fetched payloads in memory at once). So this is a COARSE memory guard, not
// a tight one; a byte-budget bound over the combined inline+blob working set is the real
// fix (H6, deferred). The Run loop re-polls immediately while a batch was processed, so
// a backlog still drains continuously.
const defaultMaxSnapshotSpan = 16

// NewDuckLakeMaterializer ensures the decoded tables + cursor row exist and
// returns a materializer over db (which must have the shared catalog attached
// as schema "lake", with din's raw_events present).
func NewDuckLakeMaterializer(ctx context.Context, db *sql.DB, log zerolog.Logger) (*DuckLakeMaterializer, error) {
	registerMetrics()
	m := &DuckLakeMaterializer{
		db:               db,
		log:              log.With().Str("component", "ducklake-materializer").Logger(),
		maxSnapshotSpan:  defaultMaxSnapshotSpan,
		dirtySubjects:    map[string]struct{}{},
		maxDirtySubjects: defaultMaxDirtySubjects,
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

// WithBlobCipher sets the cipher used to decrypt downloaded blob payloads (din
// seals them client-side). A nil cipher leaves payloads untouched. Returns m.
func (m *DuckLakeMaterializer) WithBlobCipher(c *blobcrypt.Cipher) *DuckLakeMaterializer {
	m.blobCipher = c
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

	// The partition/sort ALTERs must run ONLY when a table is first created, so
	// check existence before building the DDL. The three decoded tables are only
	// ever created by this block, so "exists" implies "already partitioned/sorted"
	// (see setupStatements for why re-ALTERing is a crash, not a no-op).
	exists := map[string]bool{}
	for _, t := range []string{"signals", "events", "signals_latest"} {
		ok, err := m.tableExists(ctx, "lake", t)
		if err != nil {
			return err
		}
		exists[t] = ok
	}
	if exists["signals_latest"] {
		hasLocTS, err := m.columnExists(ctx, "signals_latest", "loc_ts")
		if err != nil {
			return err
		}
		exists["signals_latest.loc_ts"] = hasLocTS
	}
	stmts := setupStatements(exists, sigTmp, evTmp)
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
	// H9 upgrade backfill, crash-safe: rollup rows written before the loc_ts
	// column read as NULL, which the read path serves as the 1970 epoch —
	// DORMANT vehicles (never dirtied again) would return epoch location
	// timestamps forever. The NULL rows themselves are the persistent
	// migration marker: every write path (subject flush AND full rebuild)
	// coalesces loc_ts to at least the epoch literal, so "any NULL exists"
	// precisely means "backfill incomplete" — surviving any crash between the
	// ADD COLUMN and the first successful flush, unlike an in-memory flag.
	if exists["signals_latest"] {
		var pending bool
		if err := m.db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM lake.signals_latest WHERE loc_ts IS NULL)`).Scan(&pending); err != nil {
			return fmt.Errorf("checking loc_ts backfill marker: %w", err)
		}
		if pending {
			m.rollupFullRebuild = true
		}
	}
	return nil
}

// tableExists reports whether schema.name is already in the attached catalog.
// Used to decide whether a decoded table needs its first-creation layout applied.
func (m *DuckLakeMaterializer) tableExists(ctx context.Context, schema, name string) (bool, error) {
	var n int
	if err := m.db.QueryRowContext(ctx,
		`SELECT count(*) FROM duckdb_tables() WHERE database_name = ? AND table_name = ?`,
		schema, name).Scan(&n); err != nil {
		return false, fmt.Errorf("checking table %s.%s exists: %w", schema, name, err)
	}
	return n > 0, nil
}

// columnExists reports whether lake.<table> already has the named column.
func (m *DuckLakeMaterializer) columnExists(ctx context.Context, table, column string) (bool, error) {
	var n int
	if err := m.db.QueryRowContext(ctx,
		`SELECT count(*) FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = ? AND column_name = ?`,
		table, column).Scan(&n); err != nil {
		return false, fmt.Errorf("checking column lake.%s.%s exists: %w", table, column, err)
	}
	return n > 0, nil
}

// setupStatements builds the ordered ensureSchema DDL given which of the three
// decoded tables already exist (keyed "signals"/"events"/"signals_latest").
//
// The SET PARTITIONED BY / SET SORTED BY ALTERs are emitted ONLY for a table that
// does not yet exist. They are NOT idempotent: DuckLake bumps the catalog
// schema_version on every ALTER even when the spec is unchanged, and it names the
// inline-data tables ducklake_inlined_data_<table_id>_<schema_version> — so each
// re-ALTER renames them out from under din's maintenance, whose inline_flush then
// drops the superseded ones; an in-flight inline read hits the missing table and
// the ducklake extension throws a FATAL that invalidates the session and crash-
// loops the materializer. Gating the ALTERs on first creation is what makes a
// reboot mint zero new snapshots. Partitioning is catalog metadata that only
// affects data written after it is set, so a genuinely fresh catalog must still
// configure it on creation — which this does. "timestamp" is quoted because it is
// a DuckDB keyword.
//
// Trade-off: a decoded table created WITHOUT this layout (older code, partial
// setup) would not be re-laid-out. That can't happen here because this block is
// their only creator, so the existence gate is sufficient and simpler than reading
// the live partition spec and ALTERing on drift. setupStatements is pure so the
// idempotency guarantee is unit-testable without a live DuckLake.
func setupStatements(exists map[string]bool, sigTmp, evTmp string) []string {
	var stmts []string
	if !exists["signals"] {
		stmts = append(stmts,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS lake.signals AS SELECT * FROM read_parquet(%s) LIMIT 0", sqlLit(sigTmp)),
			`ALTER TABLE lake.signals SET PARTITIONED BY (subject_bucket, day("timestamp"))`,
			`ALTER TABLE lake.signals SET SORTED BY (subject, "timestamp")`,
		)
	}
	if !exists["events"] {
		stmts = append(stmts,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS lake.events AS SELECT * FROM read_parquet(%s) LIMIT 0", sqlLit(evTmp)),
			`ALTER TABLE lake.events SET PARTITIONED BY (subject_bucket, day("timestamp"))`,
			`ALTER TABLE lake.events SET SORTED BY (subject, "timestamp")`,
		)
	}
	if !exists["signals_latest"] {
		// signals_latest is the per-(subject,name) latest+summary rollup (CHD-3):
		// it makes latest/summary/availableSignals O(distinct-names) instead of a
		// full-history GROUP BY per request. Maintained OFF the decode commit by
		// FlushRollup, which recomputes the touched subjects from the deduped
		// base table, so it is a materialized view of getAllLatestSignalsLake (no
		// source filter) — parity is by construction. Partitioned by subject_bucket
		// like the base. loc_ts (H9) is the (0,0)-filtered latest-location
		// timestamp (epoch when the name has no nonzero fix) — it is what lets
		// LOCATION latest queries (currentLocationCoordinates, the most common
		// telemetry read) serve from the rollup instead of a full-history
		// deduped GROUP BY.
		stmts = append(stmts,
			`CREATE TABLE IF NOT EXISTS lake.signals_latest (
				subject VARCHAR, subject_bucket INTEGER, name VARCHAR,
				"timestamp" TIMESTAMP WITH TIME ZONE,
				value_number DOUBLE, value_string VARCHAR,
				loc_lat DOUBLE, loc_lon DOUBLE, loc_hdop DOUBLE, loc_heading DOUBLE,
				loc_ts TIMESTAMP WITH TIME ZONE,
				count BIGINT, first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`,
			`ALTER TABLE lake.signals_latest SET PARTITIONED BY (subject_bucket)`,
		)
	} else if !exists["signals_latest.loc_ts"] {
		// Migrate catalogs whose rollup predates loc_ts (H9). Emitted only
		// when the column is actually missing, so a normal re-boot mints zero
		// snapshots; ensureSchema pairs it with a one-time full-rebuild
		// escalation that backfills existing rows (dormant vehicles would
		// otherwise serve epoch location timestamps forever).
		stmts = append(stmts,
			`ALTER TABLE lake.signals_latest ADD COLUMN IF NOT EXISTS loc_ts TIMESTAMP WITH TIME ZONE`)
	}
	// The remaining statements are genuinely idempotent (CREATE IF NOT EXISTS /
	// guarded INSERT) and mint no schema change, so they run on every boot.
	stmts = append(stmts,
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
	)
	return stmts
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
	// dq can boot before din has created lake.raw_events against a fresh catalog.
	// Without this guard, readDelta's ducklake_table_changes(…, 'raw_events', …)
	// errors every pass, the hourly failure backstop trips, and the pod crash-loops
	// until din appears (S8). Treat a missing source table as caught-up (0, nil) and
	// info-log it throttled, so dq simply waits instead of restarting.
	exists, err := m.tableExists(ctx, "lake", "raw_events")
	if err != nil {
		return 0, err
	}
	if !exists {
		m.logRawEventsMissing()
		return 0, nil
	}
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

// rawEventsMissingLogInterval throttles the pre-din boot log (S8).
const rawEventsMissingLogInterval = 5 * time.Minute

// logRawEventsMissing info-logs that lake.raw_events isn't present yet, at most
// once per rawEventsMissingLogInterval, so a dq that came up before din doesn't
// spam a line every poll.
func (m *DuckLakeMaterializer) logRawEventsMissing() {
	if !m.lastRawMissingLog.IsZero() && time.Since(m.lastRawMissingLog) < rawEventsMissingLogInterval {
		return
	}
	m.lastRawMissingLog = time.Now()
	m.log.Info().Msg("lake.raw_events not present yet (din has not created it against this catalog); waiting, treating as caught up")
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

	events, err := m.readDelta(ctx, cur, to)
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
		decoded := dec.decodeEvents(ctx, events)
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
	// Return processed=1 (pseudo-processed) so RunOnce reports >0 and Run's drain
	// loop re-polls immediately (M5) instead of sleeping a full PollInterval before
	// draining the — possibly huge — still-retained backlog past the skipped gap.
	// This is NOT a committed batch: batchesTotal is incremented only on the commit
	// path (processChunk), never here, so throughput metrics stay honest.
	return 1, true, nil
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

// progressKeepaliveInterval re-stamps the consumer floor's updated_at while dq is caught up
// and idle. din's ConsumerFloor only counts consumers seen within LAKE_CONSUMER_STALENESS
// (default 1h); without a keepalive the heartbeat is coupled to cursor MOVEMENT, so an idle
// dq sitting at head stops refreshing updated_at and din presumes it dead and drops the
// floor — defeating the floor's protection exactly when there's no new data. Must stay
// comfortably under that staleness window.
const progressKeepaliveInterval = 5 * time.Minute

// reportProgressNow publishes snapshotID immediately (and records it, resetting the throttle).
// When the id is unchanged (caught up / idle) it re-stamps updated_at on a keepalive cadence
// rather than no-op'ing, so din's floor stays fresh while dq idles at head. Used where the
// floor must be current: catch-up and expiry-reset.
func (m *DuckLakeMaterializer) reportProgressNow(ctx context.Context, snapshotID int64) {
	if snapshotID == m.lastReportedSnapshot {
		// Same snapshot: re-stamp updated_at on the keepalive cadence (pure liveness; the
		// cursor is unchanged) instead of no-op'ing, so din doesn't drop the floor while idle.
		if !m.lastProgressReport.IsZero() && time.Since(m.lastProgressReport) < progressKeepaliveInterval {
			return
		}
		m.lastProgressReport = time.Now()
		m.reportProgress(ctx, snapshotID)
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
	if errors.Is(err, sql.ErrNoRows) {
		// The seeded cursor row is gone (catalog restore/truncate). Left missing, every
		// CAS advance matches 0 rows → errSnapshotMoved → RunOnce returns (0,nil) and dq
		// looks caught-up FOREVER while decode silently halts (M6). Self-heal: re-seed
		// the row (the bootstrap INSERT is idempotent — NOT EXISTS no-ops if a peer beat
		// us) and resume from 0. Re-decoding from 0 is safe — the insert anti-join and
		// the read-path QUALIFY dedup make it idempotent. Loud + counted so a recurring
		// disappearance is alertable, not silent.
		passErrorsTotal.Inc()
		m.log.Error().Str("partition", snapshotCursorPartition).
			Msg("ingest_progress cursor row missing; re-seeding and resuming decode from snapshot 0")
		if _, serr := m.db.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO lake.ingest_progress (partition, cursor) "+
				"SELECT %s, '0' WHERE NOT EXISTS (SELECT 1 FROM lake.ingest_progress WHERE partition = %s)",
			sqlLit(snapshotCursorPartition), sqlLit(snapshotCursorPartition))); serr != nil {
			return 0, fmt.Errorf("re-seeding missing ingest_progress cursor: %w", serr)
		}
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading snapshot cursor: %w", err)
	}
	if !raw.Valid {
		return 0, nil
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
// concurrently and bounded, then drops any row whose blob is a PERMANENT failure:
// a 404 (blobMissingTotal), or a deterministic poison pill — oversize, undecryptable,
// or sealed with no key (blobPoisonTotal). Only those are skipped; a TRANSIENT
// fetch error (timeout/throttle/5xx) still aborts the pass so the SAME delta is
// retried, and a missing object store surfaces loudly (not a NotFound). Rows with
// inline payloads or no blob reference are untouched. A data_index_key that is set
// but not under BlobKeyPrefix is a prefix misconfig: counted, logged, decoded empty.
func (m *DuckLakeMaterializer) resolveBlobs(ctx context.Context, events []cloudevent.RawEvent, blobKeys []string) ([]cloudevent.RawEvent, error) {
	var fetch []int
	for i := range events {
		if len(events[i].Data) != 0 || events[i].DataBase64 != "" {
			continue // inline payload present
		}
		switch {
		case blobKeys[i] == "":
			// genuinely no payload
		case strings.HasPrefix(blobKeys[i], eventrepo.BlobKeyPrefix):
			fetch = append(fetch, i)
		default:
			// data_index_key set but not under BlobKeyPrefix: a din BLOB_PREFIX
			// misconfig (S6) that would silently empty every externalized payload.
			// Count + log; still decode as empty (don't hard-fail the whole pass).
			duck.ObserveBlobPrefixAnomaly()
			m.log.Warn().Str("id", events[i].ID).Str("data_index_key", blobKeys[i]).
				Msgf("data_index_key not under BlobKeyPrefix %q; decoding empty payload (check din BLOB_PREFIX)", eventrepo.BlobKeyPrefix)
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
			err := m.resolveBlob(gctx, &events[idx], blobKeys[idx])
			if err == nil {
				return nil
			}
			// Permanent conditions are contained (skip the row) so one poison payload
			// can't abort and re-abort the identical delta forever, halting decode
			// fleet-wide. Everything else is transient → abort the pass for retry.
			switch {
			case eventrepo.IsObjectNotFound(err):
				blobMissingTotal.Inc()
				m.log.Error().Err(err).Str("id", events[idx].ID).Str("blob", blobKeys[idx]).
					Msg("raw_events blob payload permanently missing; skipping row")
			case eventrepo.IsObjectTooLarge(err):
				blobPoisonTotal.WithLabelValues("oversize").Inc()
				m.log.Error().Err(err).Str("id", events[idx].ID).Str("blob", blobKeys[idx]).
					Msg("raw_events blob payload exceeds max object size; skipping poison row")
			case errors.Is(err, errSealedNoKey):
				blobPoisonTotal.WithLabelValues("sealed_no_key").Inc()
				m.log.Error().Err(err).Str("id", events[idx].ID).Str("blob", blobKeys[idx]).
					Msg("raw_events blob payload is sealed but BLOB_ENCRYPTION_KEY is not configured; skipping poison row (set BLOB_ENCRYPTION_KEY)")
			case blobcrypt.IsDecryptError(err):
				blobPoisonTotal.WithLabelValues("decrypt").Inc()
				m.log.Error().Err(err).Str("id", events[idx].ID).Str("blob", blobKeys[idx]).
					Msg("raw_events blob payload failed to decrypt (wrong BLOB_ENCRYPTION_KEY or corruption); skipping poison row")
			default:
				return err // transient: abort the pass and retry the same delta
			}
			missing[idx] = true
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
	if m.blobCipher != nil {
		if data, err = m.blobCipher.Open(dataIndexKey, data); err != nil {
			return fmt.Errorf("decrypting blob payload %s: %w", dataIndexKey, err)
		}
	} else if blobcrypt.IsSealed(data) {
		// Sealed ciphertext with no cipher configured (S7): without this the raw
		// ciphertext flows into decode as the payload and yields undecodable rows
		// counted as generic decode errors — the root cause (a missing
		// BLOB_ENCRYPTION_KEY) invisible. Classify as deterministic poison instead.
		return fmt.Errorf("raw_events row %s blob payload %s is sealed but BLOB_ENCRYPTION_KEY is not configured: %w", ev.ID, dataIndexKey, errSealedNoKey)
	}
	ev.Data = data
	return nil
}

// errSealedNoKey marks a downloaded blob that carries the blobcrypt seal but has
// no BLOB_ENCRYPTION_KEY to open it — a deterministic misconfiguration classified
// as poison (S7), never a transient fetch failure.
var errSealedNoKey = errors.New("sealed blob payload but no BLOB_ENCRYPTION_KEY configured")

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
		tmp, err := writeTempParquet(m.tempDir, writeSignalParquet, dec.signals)
		if err != nil {
			return err
		}
		cleanup = append(cleanup, tmp)
		tsMin, tsMax := timeRange(dec.signals, func(r SignalRow) time.Time { return r.Timestamp })
		if _, err := tx.ExecContext(ctx, antiJoinInsert("lake.signals", tmp, tsMin, tsMax, m.backfillMode)); err != nil {
			return fmt.Errorf("insert signals: %w", err)
		}
	}
	if len(dec.events) > 0 {
		tmp, err := writeTempParquet(m.tempDir, writeEventParquet, dec.events)
		if err != nil {
			return err
		}
		cleanup = append(cleanup, tmp)
		tsMin, tsMax := timeRange(dec.events, func(r EventRow) time.Time { return r.Timestamp })
		if _, err := tx.ExecContext(ctx, antiJoinInsert("lake.events", tmp, tsMin, tsMax, m.backfillMode)); err != nil {
			return fmt.Errorf("insert events: %w", err)
		}
	}

	// NOTE: the lake.signals_latest rollup is no longer maintained here. It is a
	// materialized view; recomputing it inside the decode commit made the writer
	// O(history-per-bucket) per batch and OOM-killed on a backlog. The rollup is
	// now maintained OFF this path by FlushRollup (bucket-partitioned recompute);
	// commit only records which buckets changed (after the commit succeeds, below).

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

	if err := tx.Commit(); err != nil {
		if isCommitConflict(err) {
			return errSnapshotMoved
		}
		// Ambiguous commit (M2): a non-conflict commit error can be a LOST ACK — the
		// transaction may have durably landed even though the client saw an error. The
		// cursor advance rides in this SAME txn, so if the durable cursor now equals our
		// target `to`, the commit landed: the base rows AND the cursor moved atomically.
		// Retrying would skip this span (the cursor already advanced) and leave these
		// subjects' rollup stale forever for dormant vehicles — so treat a landed commit
		// exactly as success (fall through to mark dirtySubjects). Only a genuinely
		// failed commit propagates the error.
		if landed, cerr := m.commitLanded(ctx, to); cerr != nil || !landed {
			return fmt.Errorf("commit: %w", err)
		}
		m.log.Warn().Err(err).Int64("to", to).
			Msg("commit returned an error but the cursor advanced to the target; treating as committed (lost ack)")
	}
	// The base rows are durable. Mark the subjects this batch wrote so the
	// decoupled FlushRollup recomputes only their rollup rows (B2). Done after
	// commit so a rolled-back batch never dirties the rollup.
	for i := range dec.signals {
		m.dirtySubjects[dec.signals[i].Subject] = struct{}{}
	}
	// Bound the dirty set: a fleet-wide catch-up (initial backfill defers the
	// flush until fully drained) would otherwise grow this map with every
	// subject in the fleet — ~1GB+ at 10M vehicles, inside the pod's ~2GB
	// non-DuckDB headroom. Past the cap, per-subject tracking is no cheaper
	// than a full rebuild anyway, so escalate: clear the map and let the next
	// flush run the bucket-chunked, memory-bounded RecomputeRollup.
	if len(m.dirtySubjects) > m.maxDirtySubjects {
		m.dirtySubjects = map[string]struct{}{}
		m.rollupFullRebuild = true
	}
	return nil
}

// commitLanded reports whether the durable snapshot cursor has reached `to`. It
// disambiguates a lost-ack commit error (M2): the insert+cursor-advance are one
// transaction, so a cursor at `to` proves that transaction actually committed even
// though the client saw a commit error.
func (m *DuckLakeMaterializer) commitLanded(ctx context.Context, to int64) (bool, error) {
	cur, err := m.cursor(ctx)
	if err != nil {
		return false, err
	}
	return cur == to, nil
}

// defaultMaxDirtySubjects caps the per-subject dirty set (~25MB of map at the
// cap); beyond it FlushRollup escalates to a full rebuild.
const defaultMaxDirtySubjects = 250_000

// WithMaxDirtySubjects overrides the dirty-set overflow cap (tests use a tiny
// value to exercise the full-rebuild escalation). Non-positive restores the
// default. Returns m for chaining.
func (m *DuckLakeMaterializer) WithMaxDirtySubjects(n int) *DuckLakeMaterializer {
	if n <= 0 {
		n = defaultMaxDirtySubjects
	}
	m.maxDirtySubjects = n
	return m
}

// timeRange returns the min and max ts(row) over rows (both zero when empty).
func timeRange[T any](rows []T, ts func(T) time.Time) (minT, maxT time.Time) {
	for i := range rows {
		t := ts(rows[i])
		if i == 0 || t.Before(minT) {
			minT = t
		}
		if i == 0 || t.After(maxT) {
			maxT = t
		}
	}
	return clampProbeFloor(minT, maxT)
}

// dedupProbeFloor bounds how far back the insert anti-join's probe window may
// reach (M5). Decode prunes only FUTURE clock skew (+5m); a single device
// emitting a 1970/epoch timestamp dragged tsMin to 1970 and made the probe
// scan every day-partition in the table — for every batch containing that
// device, forever. The cost of the floor: a redelivered/crash-replayed row
// whose EVENT timestamp is >30d old (an offline vehicle uploading months of
// buffered history, replayed) can slip the write-side probe and store twice —
// rare, storage-only, and collapsed by the read-path QUALIFY dedup, the
// designed final guard on every query and on the rollup recompute.
const dedupProbeFloor = 30 * 24 * time.Hour

// clampProbeFloor floors minT at now-dedupProbeFloor (and keeps maxT >= minT
// so an all-ancient batch yields an empty probe window, not an inverted one).
func clampProbeFloor(minT, maxT time.Time) (time.Time, time.Time) {
	if minT.IsZero() {
		return minT, maxT
	}
	floor := time.Now().UTC().Add(-dedupProbeFloor)
	if minT.Before(floor) {
		minT = floor
	}
	if maxT.Before(minT) {
		maxT = minT
	}
	return minT, maxT
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
	// Drop rollup rows whose base signals were ALL pruned away. signals_latest is only
	// recomputed for subjects in a live batch (FlushRollup), so a (subject,name) that
	// stopped reporting long enough ago to be fully pruned is never cleaned up — and the
	// no-source latest/summary/availableSignals reads off the rollup (lake_rollup.go) would
	// keep serving a phantom "latest" + count for data that no longer exists in lake.signals.
	// The candidate set is bounded to sl.last_seen < cutoff (H5): a rollup row whose
	// latest base signal is at/after the cutoff cannot have lost every base row to a
	// timestamp-based prune, so the anti-join probes only long-dormant rows instead of
	// hash-anti-joining the entire base table every pruneInterval. The base table is
	// PARTITIONED BY subject_bucket, so each candidate's NOT EXISTS lookup is
	// partition-pruned. (A surviving signal's count/first_seen can still reference pruned
	// history until its next batch refreshes the rollup — a smaller, self-healing residual.)
	if _, err := m.db.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM lake.signals_latest sl WHERE sl.last_seen < make_timestamp(%d) AND NOT EXISTS (
			SELECT 1 FROM lake.signals s
			WHERE s.subject_bucket = sl.subject_bucket AND s.subject = sl.subject AND s.name = sl.name)`, cutoff)); err != nil {
		return total, fmt.Errorf("pruning orphaned rollup rows: %w", err)
	}
	return total, nil
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
	`value_string, loc_lat, loc_lon, loc_hdop, loc_heading, loc_ts, count, first_seen, last_seen) `

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
		coalesce(max(timestamp) FILTER (WHERE %[1]s), make_timestamp(0)) AS loc_ts,
		CAST(count(*) AS BIGINT) AS count,
		min(timestamp) AS first_seen, max(timestamp) AS last_seen
	FROM (SELECT * FROM lake.signals %[2]s
	      QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, name, timestamp ORDER BY cloud_event_id) = 1)
	GROUP BY subject, name`, locNonzero, whereClause)
}

// rollupSubjectChunk bounds one rollup-recompute transaction: the IN-list
// length, the DELETE width, and the recompute's aggregate window are all
// limited to this many subjects, so a huge flush (catch-up after an outage)
// commits progress in bounded pieces instead of one giant txn.
const rollupSubjectChunk = 500

// FlushRollup recomputes lake.signals_latest for every subject whose base rows
// changed since the last flush (the decoupled, off-commit rollup maintenance).
// A no-op when nothing is dirty. Subject-scoped, not bucket-scoped (B2): a
// bucket recompute is O(the bucket's entire retained history) and bucket
// dirtiness saturates at trivial fleet activity, which made every flush a
// full-table recompute on the decode goroutine. Recomputing only the dirty
// subjects keeps the same self-healing exactness (each row is rebuilt from the
// deduped base) at O(active subjects' histories). Chunked per bucket so each
// transaction is partition-pruned and bounded; a failure leaves already-
// flushed chunks refreshed and the rest dirty for the next pass.
// Single-writer: called only on the decode-loop goroutine.
func (m *DuckLakeMaterializer) FlushRollup(ctx context.Context) error {
	if m.rollupFullRebuild {
		// The dirty set overflowed maxDirtySubjects (fleet-wide catch-up):
		// rebuild everything, bucket-chunked and memory-bounded. Clear the
		// flag only on success so a failed rebuild retries next flush.
		start := time.Now()
		defer func() { rollupRefreshSeconds.Set(time.Since(start).Seconds()) }()
		if err := m.RecomputeRollup(ctx); err != nil {
			return err
		}
		m.rollupFullRebuild = false
		m.dirtySubjects = map[string]struct{}{} // rebuilt set covers anything accrued since the overflow
		return nil
	}
	if len(m.dirtySubjects) == 0 {
		return nil
	}
	byBucket := map[int][]string{}
	for s := range m.dirtySubjects {
		b := duck.HashBucket(s)
		byBucket[b] = append(byBucket[b], s)
	}
	buckets := make([]int, 0, len(byBucket))
	for b := range byBucket {
		buckets = append(buckets, b)
	}
	sort.Ints(buckets)

	start := time.Now()
	defer func() { rollupRefreshSeconds.Set(time.Since(start).Seconds()) }()
	for _, b := range buckets {
		subjects := byBucket[b]
		sort.Strings(subjects) // deterministic chunking/ordering
		for len(subjects) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk := subjects
			if len(chunk) > rollupSubjectChunk {
				chunk = chunk[:rollupSubjectChunk]
			}
			subjects = subjects[len(chunk):]
			if err := m.recomputeSubjects(ctx, b, chunk); err != nil {
				return fmt.Errorf("rollup recompute bucket %d (%d subjects): %w", b, len(chunk), err)
			}
			for _, s := range chunk {
				delete(m.dirtySubjects, s) // keep unflushed subjects dirty for retry
			}
		}
	}
	return nil
}

// recomputeSubjects DELETEs+recomputes the rollup rows of one bucket's given
// subjects in a single transaction. The WHERE lives inside the recompute
// subquery at QUALIFY level so the bucket predicate prunes partitions (B1).
func (m *DuckLakeMaterializer) recomputeSubjects(ctx context.Context, bucket int, subjects []string) error {
	args := make([]any, len(subjects))
	marks := make([]string, len(subjects))
	for i, s := range subjects {
		args[i] = s
		marks[i] = "?"
	}
	where := fmt.Sprintf("WHERE subject_bucket = %d AND subject IN (%s)", bucket, strings.Join(marks, ", "))

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.signals_latest "+where, args...); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.signals_latest"+signalsLatestColumns+rollupSelectSQL(where), args...); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return tx.Commit()
}

// RecomputeRollup rebuilds lake.signals_latest from scratch over the ENTIRE
// lake.signals base, byte-identical to the per-bucket FlushRollup but for every
// bucket. The per-batch/flush path only touches buckets that changed, so a rollup
// that was dropped or truncated leaves dormant (no-longer-reporting) vehicles
// permanently missing from the latest/summary reads. This is the disaster-recovery
// rebuild for that case; it is opt-in (LAKE_REBUILD_ROLLUP_ON_BOOT). Partitioned
// over the 256 buckets (one txn each) so it stays memory-bounded over deep history
// instead of materializing the whole-table QUALIFY window at once. Safe to run
// while the single-writer materializer is offline.
func (m *DuckLakeMaterializer) RecomputeRollup(ctx context.Context) error {
	buckets := make([]int, duck.NumLatestBuckets)
	for i := range buckets {
		buckets[i] = i
	}
	_, err := m.recomputeBuckets(ctx, buckets)
	return err
}

// recomputeBuckets DELETEs+recomputes lake.signals_latest for each given
// subject_bucket, one transaction per bucket. Bounding each recompute to a single
// partition keeps the GROUP BY / QUALIFY window memory-bounded (the whole-table
// recompute OOMs on deep history). Returns the buckets that committed successfully
// (so the caller can clear exactly those from the dirty set) alongside the first
// error.
func (m *DuckLakeMaterializer) recomputeBuckets(ctx context.Context, buckets []int) (done []int, err error) {
	for _, b := range buckets {
		if err = ctx.Err(); err != nil {
			return done, err
		}
		where := fmt.Sprintf("WHERE subject_bucket = %d", b)
		if rerr := m.recomputeOneBucket(ctx, where); rerr != nil {
			return done, fmt.Errorf("rollup recompute bucket %d: %w", b, rerr)
		}
		done = append(done, b)
	}
	return done, nil
}

func (m *DuckLakeMaterializer) recomputeOneBucket(ctx context.Context, where string) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.signals_latest "+where); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.signals_latest"+signalsLatestColumns+rollupSelectSQL(where)); err != nil {
		return fmt.Errorf("insert: %w", err)
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
//
// The probe is additionally bounded to the batch's own [tsMin, tsMax] timestamp
// window. A redelivered row carries the SAME timestamp as the original, so every
// possible duplicate falls inside the batch's range — the bound can never miss
// one. What it does is let DuckLake prune the probe to the day-partitions the
// batch actually spans (it is PARTITIONED BY day("timestamp")) instead of
// scanning the whole table, which is what made the anti-join O(history)/batch and
// the dominant steady-state cost on a backlog. skipDedup drops the guard entirely
// for a backfill of a known-clean dump (the read path dedups regardless).
func antiJoinInsert(table, parquetPath string, tsMin, tsMax time.Time, skipDedup bool) string {
	if skipDedup {
		return fmt.Sprintf("INSERT INTO %s SELECT * FROM read_parquet(%s)", table, sqlLit(parquetPath))
	}
	return fmt.Sprintf(
		"INSERT INTO %[1]s SELECT * FROM read_parquet(%[2]s) AS src "+
			"WHERE NOT EXISTS (SELECT 1 FROM %[1]s AS t "+
			"WHERE t.subject_bucket = src.subject_bucket "+
			`AND t."timestamp" >= %[3]s AND t."timestamp" <= %[4]s `+
			"AND t.cloud_event_id = src.cloud_event_id "+
			"AND t.name = src.name AND t.timestamp = src.timestamp)",
		table, sqlLit(parquetPath), sqlTimestampLit(tsMin), sqlTimestampLit(tsMax))
}

// sqlTimestampLit renders t as a DuckDB TIMESTAMPTZ literal in UTC, for inlining
// as a constant the planner can use for day-partition pruning (no injection risk:
// the value is a formatted timestamp).
func sqlTimestampLit(t time.Time) string {
	return "TIMESTAMPTZ '" + t.UTC().Format("2006-01-02 15:04:05.999999-07:00") + "'"
}
