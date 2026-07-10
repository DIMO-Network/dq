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
	// dirtyEventSubjects is the events_latest analogue of dirtySubjects (finding
	// #5a): subjects whose lake.events rows changed since the last FlushEventRollup.
	// The events rollup is maintained OFF the decode commit exactly like
	// signals_latest, mirroring the same subject-scoped, chunked, self-healing
	// recompute. Single-writer, so unsynchronized like dirtySubjects.
	dirtyEventSubjects map[string]struct{}
	// eventRollupFullRebuild escalates the next FlushEventRollup to a full
	// RecomputeEventRollup — set on overflow (like rollupFullRebuild) and, once at
	// first-create, to backfill events_latest from a pre-existing lake.events base
	// (the migration case: an existing catalog getting the rollup for the first time).
	eventRollupFullRebuild bool
	// maxDirtySubjects is the overflow cap (defaultMaxDirtySubjects). Shared by both
	// dirty sets.
	maxDirtySubjects int

	// windowByteBudget / maxRowsPerWindow bound the resident working set of a SINGLE
	// snapshot's decode+write (finding #1c). maxSnapshotSpan count-bounds the
	// MULTI-snapshot backlog, but a single fat snapshot (span can't drop below 1) with
	// a large blob fan-out would still materialize whole and OOM the writer. Instead
	// the (cursor, to] insert delta is read+decoded+written in row-key-ordered WINDOWS:
	// intermediate windows are written idempotently WITHOUT advancing the cursor, and
	// only the FINAL window's transaction advances it — so "cursor advanced ⟺ every
	// window durable" still holds and a crash mid-span re-reads from the un-advanced
	// cursor, the anti-join collapsing already-written windows. A window is flushed once
	// its resident raw-payload bytes reach windowByteBudget OR its row count reaches
	// maxRowsPerWindow. <= 0 disables that bound (the other still applies).
	windowByteBudget int64
	maxRowsPerWindow int
	// windowCommitHook is a TEST-ONLY seam: called with the 0-based index after each
	// INTERMEDIATE window commits (never for the final cursor-advancing commit). Returning
	// an error aborts the pass after that window is durable but before the cursor advances,
	// deterministically reproducing a crash mid-span. nil in production.
	windowCommitHook func(windowIndex int) error
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
//
// TODO(load-review #1c): bound the pass by a BYTE budget over the resident inline+blob
// working set, paginating WITHIN a single oversized snapshot so no snapshot is atomic.
// The multi-snapshot case is already count-bounded here; the residual OOM is ONE
// snapshot with a large blob fan-out (span can't drop below 1), whose readDelta slice +
// resolveBlobs payloads are unbounded. This was NOT implemented because it changes the
// commit's atomicity model, and the exactly-once protocol is chaos-test-proven (60+
// SIGKILL iterations) — a change here must be re-proven the same way, not just unit-
// tested. Design for the human implementer:
//   - Page a single snapshot's rows by a stable ROW-KEY window (order by
//     (subject, timestamp, cloud_event_id); carry a >last-key cursor), reading and
//     decoding one byte-bounded window at a time instead of the whole delta.
//   - Write each window with the EXISTING idempotent antiJoinInsert (keyed on
//     cloud_event_id) in its own txn that DOES NOT advance the ingest_progress cursor.
//     Intermediate windows are safe to re-do: a crash mid-snapshot re-reads from the
//     window start and the anti-join collapses the already-written rows.
//   - Advance the cursor to `to` only in the FINAL window's txn, coupled with that
//     window's insert exactly as commit() does today — so "cursor advanced ⟺ every
//     window durable" still holds and commitLanded's lost-ack disambiguation is
//     unchanged. Do NOT split the cursor advance from ALL inserts (that breaks the
//     lost-ack invariant); only the LAST insert stays coupled to it.
//   - Note the interaction with maybeRecoverExpired: if din expires the snapshot after
//     some windows are written but before the final cursor advance, restart takes the
//     expiry-skip path leaving a partial write — same class as today's cursor-expiry
//     gap, now recoverable via BackfillTimeRange (finding #1a).
// Until then, size LAKE_SNAPSHOT_RETENTION and pod memory so a single snapshot's
// working set fits, and rely on the crashloop being visible (pass errors) rather than
// silent.
const defaultMaxSnapshotSpan = 16

// NewDuckLakeMaterializer ensures the decoded tables + cursor row exist and
// returns a materializer over db (which must have the shared catalog attached
// as schema "lake", with din's raw_events present).
func NewDuckLakeMaterializer(ctx context.Context, db *sql.DB, log zerolog.Logger) (*DuckLakeMaterializer, error) {
	registerMetrics()
	m := &DuckLakeMaterializer{
		db:                 db,
		log:                log.With().Str("component", "ducklake-materializer").Logger(),
		maxSnapshotSpan:    defaultMaxSnapshotSpan,
		dirtySubjects:      map[string]struct{}{},
		dirtyEventSubjects: map[string]struct{}{},
		maxDirtySubjects:   defaultMaxDirtySubjects,
		windowByteBudget:   defaultWindowByteBudget,
		maxRowsPerWindow:   defaultWindowMaxRows,
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

// defaultWindowByteBudget / defaultWindowMaxRows bound one intra-snapshot window's
// resident working set (finding #1c). 64 MiB of raw payloads keeps a window well inside
// the pod's non-DuckDB headroom even under a large blob fan-out; the row cap bounds the
// count when payloads are tiny (so the decoded-row and temp-parquet working set stays
// bounded too). windowReadChunk is how many change-feed rows are read per query while
// filling a window.
const (
	defaultWindowByteBudget = 64 << 20 // 64 MiB
	defaultWindowMaxRows    = 100_000
	windowReadChunk         = 512
)

// WithWindowByteBudget overrides the per-window resident-byte bound (finding #1c).
// A non-positive n disables the byte bound (the row cap still applies). Returns m.
func (m *DuckLakeMaterializer) WithWindowByteBudget(n int64) *DuckLakeMaterializer {
	m.windowByteBudget = n
	return m
}

// WithMaxRowsPerWindow overrides the per-window row cap (finding #1c). A non-positive n
// disables the row bound (the byte budget still applies). Tests use a tiny value to force
// multi-window pagination of a small snapshot. Returns m.
func (m *DuckLakeMaterializer) WithMaxRowsPerWindow(n int) *DuckLakeMaterializer {
	m.maxRowsPerWindow = n
	return m
}

// WithWindowCommitHook installs the TEST-ONLY crash seam (see the field doc): the hook
// runs after each intermediate window commits, and a returned error aborts the pass
// before the cursor advances. Returns m.
func (m *DuckLakeMaterializer) WithWindowCommitHook(fn func(windowIndex int) error) *DuckLakeMaterializer {
	m.windowCommitHook = fn
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
	for _, t := range []string{"signals", "events", "signals_latest", "events_latest"} {
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
	// events_latest completeness marker (crash-safe, mirrors the loc_ts pattern above).
	// A first-create migration over a PRE-EXISTING lake.events base must backfill every
	// already-decoded subject, or dormant vehicles (no new events) read an empty summary.
	// Deriving that from PRE-DDL existence (an in-memory flag) is NOT crash-safe: a pod
	// killed after events_latest is created but before RecomputeEventRollup finishes would,
	// on restart, see the table exists and silently skip the rebuild — stranding dormant
	// subjects forever (Q3b). Instead re-derive it from DB STATE each boot: "the event base
	// is non-empty but the rollup has NO rows" precisely means "rollup incomplete", and it
	// survives any crash mid-rebuild. A truly fresh catalog (events not yet flowing) has an
	// empty base, so this does not fire; once the base has data, one rebuild populates the
	// rollup and it never fires again. (The per-subject GetEventSummaries fallback keeps
	// reads correct during the window.)
	var eventsPending bool
	if err := m.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM lake.events) AND NOT EXISTS (SELECT 1 FROM lake.events_latest)`).Scan(&eventsPending); err != nil {
		return fmt.Errorf("checking events_latest backfill marker: %w", err)
	}
	if eventsPending {
		m.eventRollupFullRebuild = true
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
// The time partition is the year/month/day TRIPLE: DuckLake's year()/month()/
// day() are component extractions (day(x) alone is day-of-month 1-31, NOT a
// date — the original single-day() spec cycled 31 buckets mixing every month),
// evaluated in the session TimeZone (UTC, pinned per-conn). Known trade-off of
// daily grain: ducklake_merge_adjacent_files only consolidates within one
// partition, so a low-traffic (bucket, day) keeps its tiny files forever;
// accepted for layout legibility.
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
			`ALTER TABLE lake.signals SET PARTITIONED BY (subject_bucket, year("timestamp"), month("timestamp"), day("timestamp"))`,
			`ALTER TABLE lake.signals SET SORTED BY (subject, "timestamp")`,
		)
	}
	if !exists["events"] {
		stmts = append(stmts,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS lake.events AS SELECT * FROM read_parquet(%s) LIMIT 0", sqlLit(evTmp)),
			`ALTER TABLE lake.events SET PARTITIONED BY (subject_bucket, year("timestamp"), month("timestamp"), day("timestamp"))`,
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
	if !exists["events_latest"] {
		// events_latest is the per-(subject,name) event summary rollup (finding #5a):
		// it makes GetEventSummaries/dataSummary O(distinct-names) instead of an
		// all-history GROUP BY over lake.events per request — the events analogue of
		// signals_latest. Maintained OFF the decode commit by FlushEventRollup, which
		// recomputes touched subjects from the (subject,timestamp,name,source)-deduped
		// base (LakeEventsDeduped's key), so it is a materialized view of
		// GetEventSummaries — parity by construction. Partitioned by subject_bucket
		// like the base. Same first-creation ALTER gating as the other decoded tables
		// (see the doc above): the ALTER is emitted only on create, so a reboot mints
		// zero schema changes.
		stmts = append(stmts,
			`CREATE TABLE IF NOT EXISTS lake.events_latest (
				subject VARCHAR, subject_bucket INTEGER, name VARCHAR,
				count BIGINT,
				first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`,
			`ALTER TABLE lake.events_latest SET PARTITIONED BY (subject_bucket)`,
		)
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

	// Decode+write the (cur, to] insert delta in byte/row-bounded windows so a single
	// fat snapshot can't materialize whole and OOM the writer (#1c). Only the final
	// window advances the cursor, so a crash mid-span re-reads from cur and the
	// anti-join collapses already-written windows — exactly-once preserved.
	res, err := m.decodeAndWriteSpan(ctx, cur, to, dec)
	if err != nil {
		if errors.Is(err, errSnapshotMoved) {
			return 0, cur, true, nil // another decoder won this range; retry next pass
		}
		// A feed-read failure might mean din's maintenance expired the cursor range.
		// Decide on retention (the oldest retained snapshot), not on the error text —
		// so a real expiry with unmatched wording can't wedge us forever, and a
		// transient error that merely looks like expiry can't make us skip retained data.
		if n, handled, rerr := m.maybeRecoverExpired(ctx, cur, err); handled {
			return n, cur, true, rerr
		}
		return 0, cur, true, err
	}

	if res.rawRows > 0 {
		observeLakeLagAt(res.oldest) // decode lag = age of the oldest pending event
		// A span committed: feed the freshness/throughput alerts (CHD-12).
		batchesTotal.WithLabelValues(lakeMetricType).Inc()
		rowsTotal.WithLabelValues("signals").Add(float64(res.signalRows))
		rowsTotal.WithLabelValues("events").Add(float64(res.eventRows))
		errorsTotal.Add(float64(res.errRows))
		cursorSnapshotID.Set(float64(to))
		// Report progress to din's snapshot-expiry floor. Throttled: the span is
		// already durable, and din only needs the floor within its retention window
		// — a lagging report just holds expiry back slightly (conservative, never
		// unsafe), so it needn't be a catalog txn on every batch.
		m.maybeReportProgress(ctx, to)
		return res.rawRows, to, true, nil
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
// rawRowKey is the unique, totally-ordered key used to paginate the (cur, to] insert
// change feed: (subject, time, id). id (the cloud_event_id) is globally unique, so the
// triple is a strict total order — a `> after` cursor + ORDER BY partitions the feed
// into windows with no row skipped or repeated.
type rawRowKey struct {
	subject string
	ts      time.Time
	id      string
}

// spanCounts accumulates what a span (or window) processed, for metrics and the
// caught-up/empty-span decision. rawRows counts raw_events rows READ (before any poison
// drop) so the cursor and lag stay correct even when a payload is dropped.
type spanCounts struct {
	rawRows    int
	signalRows int
	eventRows  int
	errRows    int
	oldest     time.Time // oldest raw event Time seen (for decode-lag)
}

// readDeltaWindow reads up to `limit` raw_events INSERT rows from the (from, to] change
// feed, ordered by the unique row key and strictly after `after` when hasAfter, then
// resolves their blob payloads. It returns the resolved rows (poison rows dropped), the
// key of the LAST row read (captured from the raw scan, before any drop, so pagination
// never skips a row whose payload was dropped), and got = the number of rows actually
// read (0 ⇒ the feed is exhausted past `after`).
func (m *DuckLakeMaterializer) readDeltaWindow(ctx context.Context, from, to int64, after rawRowKey, hasAfter bool, limit int) (events []cloudevent.RawEvent, last rawRowKey, got int, err error) {
	where := ""
	var args []any
	if hasAfter {
		where = ` AND (subject, "time", id) > (?, ?, ?)`
		args = []any{after.subject, after.ts.UTC(), after.id}
	}
	q := fmt.Sprintf(
		`SELECT %s FROM ducklake_table_changes('lake', 'main', 'raw_events', %d, %d) `+
			`WHERE change_type = 'insert'%s ORDER BY subject, "time", id LIMIT %d`,
		duck.RawEventColumns, from+1, to, where, limit)
	rows, qerr := m.db.QueryContext(ctx, q, args...)
	if qerr != nil {
		// Don't classify here — processChunk decides expired-vs-transient on retention.
		return nil, rawRowKey{}, 0, fmt.Errorf("reading raw_events delta window: %w", qerr)
	}
	defer rows.Close() //nolint:errcheck

	var out []cloudevent.RawEvent
	var blobKeys []string
	for rows.Next() {
		ev, blobKey, serr := scanRawEvent(rows)
		if serr != nil {
			return nil, rawRowKey{}, 0, serr
		}
		out = append(out, ev)
		blobKeys = append(blobKeys, blobKey)
		last = rawRowKey{subject: ev.Subject, ts: ev.Time, id: ev.ID}
		got++
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, rawRowKey{}, 0, rerr
	}
	resolved, berr := m.resolveBlobs(ctx, out, blobKeys)
	if berr != nil {
		return nil, rawRowKey{}, 0, berr
	}
	return resolved, last, got, nil
}

// rawResidentBytes estimates the resident payload bytes of a batch of raw events (the
// working set the window byte-budget bounds — blob fan-out is the OOM risk in #1c).
func rawResidentBytes(events []cloudevent.RawEvent) int64 {
	var n int64
	for i := range events {
		n += int64(len(events[i].Data)) + int64(len(events[i].DataBase64))
	}
	return n
}

// appendDecoded folds src into dst (rows and counts).
func appendDecoded(dst, src *decodedBatch) {
	dst.signals = append(dst.signals, src.signals...)
	dst.events = append(dst.events, src.events...)
	dst.signalCount += src.signalCount
	dst.eventCount += src.eventCount
	dst.errorCount += src.errorCount
}

// earlier returns the earlier of two times, ignoring the zero value.
func earlier(a, b time.Time) time.Time {
	switch {
	case a.IsZero():
		return b
	case b.IsZero():
		return a
	case b.Before(a):
		return b
	default:
		return a
	}
}

// nextWindow accumulates one byte/row-bounded window of the (from, to] delta, advancing
// *after past the rows it consumed. It reads the feed in windowReadChunk-sized pages and
// decodes each page, stopping once the window's resident bytes reach windowByteBudget or
// its row count reaches maxRowsPerWindow (or the feed is exhausted). c.rawRows == 0 means
// no rows remain past *after.
func (m *DuckLakeMaterializer) nextWindow(ctx context.Context, from, to int64, after *rawRowKey, hasAfter *bool, dec eventDecoder) (*decodedBatch, spanCounts, error) {
	win := &decodedBatch{}
	var c spanCounts
	var winBytes int64
	for {
		readLimit := windowReadChunk
		if m.maxRowsPerWindow > 0 {
			rem := m.maxRowsPerWindow - c.rawRows
			if rem <= 0 {
				break // row cap reached: window is full
			}
			if rem < readLimit {
				readLimit = rem
			}
		}
		resolved, last, got, err := m.readDeltaWindow(ctx, from, to, *after, *hasAfter, readLimit)
		if err != nil {
			return nil, c, err
		}
		if got == 0 {
			break // feed exhausted past `after`
		}
		*after = last
		*hasAfter = true
		c.rawRows += got
		winBytes += rawResidentBytes(resolved)
		for i := range resolved {
			c.oldest = earlier(c.oldest, resolved[i].Time)
		}
		d := dec.decodeEvents(ctx, resolved)
		appendDecoded(win, d)
		c.signalRows += d.signalCount
		c.eventRows += d.eventCount
		c.errRows += d.errorCount
		if got < readLimit {
			break // fewer rows than asked ⇒ feed exhausted (tail window)
		}
		if m.windowByteBudget > 0 && winBytes >= m.windowByteBudget {
			break // byte budget reached: window is full
		}
	}
	return win, c, nil
}

// decodeAndWriteSpan decodes and writes the (from, to] insert delta in byte/row-bounded
// windows (finding #1c). Every window but the last is written idempotently WITHOUT
// advancing the cursor; the final window's commit advances the cursor from→to coupled to
// its insert, exactly as the pre-#1c monolithic commit did. So the invariant holds:
// cursor advanced ⟺ every window durable. A crash mid-span leaves the cursor at `from`;
// restart re-reads the whole span and the cloud_event_id anti-join collapses the windows
// that already landed. Returns 0 rawRows (and does NOT touch the cursor) for an empty
// span, leaving the caller to run the empty-span/caught-up handling.
func (m *DuckLakeMaterializer) decodeAndWriteSpan(ctx context.Context, from, to int64, dec eventDecoder) (spanCounts, error) {
	var total spanCounts
	var after rawRowKey
	hasAfter := false
	var pending *decodedBatch
	havePending := false
	windowIdx := 0
	for {
		win, c, err := m.nextWindow(ctx, from, to, &after, &hasAfter, dec)
		if err != nil {
			return total, err
		}
		if c.rawRows == 0 {
			break // no more rows: `pending`, if any, is the final window
		}
		total.rawRows += c.rawRows
		total.signalRows += c.signalRows
		total.eventRows += c.eventRows
		total.errRows += c.errRows
		total.oldest = earlier(total.oldest, c.oldest)
		if havePending {
			// The previous window is now known NOT to be the last, so write it as an
			// intermediate window (idempotent, no cursor advance).
			if err := m.writeWindow(ctx, pending); err != nil {
				return total, err
			}
			if m.windowCommitHook != nil {
				if err := m.windowCommitHook(windowIdx); err != nil {
					return total, err
				}
			}
			windowIdx++
		}
		pending = win
		havePending = true
	}
	if !havePending {
		return total, nil // empty span
	}
	// The final window advances the cursor, coupled to its insert.
	if err := m.commit(ctx, pending, from, to); err != nil {
		return total, err
	}
	return total, nil
}

// scanAndResolveRaw runs a raw_events SELECT (projected with duck.RawEventColumns so
// scanRawEvent's positional scan matches), reconstructs each RawEvent + its blob key
// with no network in the scan loop, then resolves externalized payloads concurrently
// (serial S3 GETs would block the writer on N round-trips). Shared by the change-feed
// delta read (readDelta) and the base-table backfill read (readRawByTime, finding #1a).
func (m *DuckLakeMaterializer) scanAndResolveRaw(ctx context.Context, query, errCtx string) ([]cloudevent.RawEvent, error) {
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errCtx, err)
	}
	defer rows.Close() //nolint:errcheck

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
	return m.resolveBlobs(ctx, out, blobKeys)
}

// readRawByTime reads raw_events in [from, to) DIRECTLY from the base table — NOT the
// snapshot change feed, whose history may have expired past LAKE_SNAPSHOT_RETENTION.
// The rows themselves survive (din's row retention is separate), so a backfill can
// re-read a range the decode loop skipped on cursor expiry (finding #1a). Half-open
// [from, to) mirrors the query layer's [from, to) event window.
func (m *DuckLakeMaterializer) readRawByTime(ctx context.Context, from, to time.Time) ([]cloudevent.RawEvent, error) {
	q := fmt.Sprintf(`SELECT %s FROM lake.raw_events WHERE "time" >= %s AND "time" < %s`,
		duck.RawEventColumns, sqlTimestampLit(from), sqlTimestampLit(to))
	return m.scanAndResolveRaw(ctx, q, "reading raw_events by time")
}

// BackfillTimeRange re-decodes raw_events in [from, to) into lake.signals/events,
// idempotently, WITHOUT touching the ingest_progress cursor — the out-of-band repair
// for a range the main decode loop permanently skipped on cursor expiry (finding #1a,
// the counterpart to cursorResetsTotal, which only records that a skip happened). It
// reuses the exact decode + idempotent-insert path, so re-running is a no-op: the
// cloud_event_id anti-join (backfillWrite) collapses rows already at rest, and the
// read-path QUALIFY dedup is the final guard on every query. Marks the touched
// subjects dirty so the next FlushRollup / FlushEventRollup refreshes their rollups.
// Returns the number of raw events decoded. Safe to run while the single-writer
// materializer is offline OR alongside it (both go through the same idempotent insert).
func (m *DuckLakeMaterializer) BackfillTimeRange(ctx context.Context, dec eventDecoder, from, to time.Time) (int, error) {
	if !from.Before(to) {
		return 0, fmt.Errorf("backfill range is empty or inverted: from %s must be before to %s", from, to)
	}
	events, err := m.readRawByTime(ctx, from, to)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}
	decoded := dec.decodeEvents(ctx, events)
	if err := m.backfillWrite(ctx, decoded); err != nil {
		return 0, err
	}
	return len(events), nil
}

// backfillWrite inserts a decoded batch into lake.signals/events in one transaction,
// idempotently, and WITHOUT the cursor CAS (backfill is out-of-band, so it must never
// move the main decode cursor). It mirrors commit's insert blocks but, unlike the
// steady-state path, uses the UNCLAMPED [min,max] timestamp window for the dedup
// anti-join probe (minMaxTime, not the now-30d clampProbeFloor): a backfill re-decodes
// arbitrarily old data and MUST find the existing rows at their true timestamps to stay
// idempotent — a probe floored to 30d would miss old duplicates and double-insert on a
// re-run. The wider probe is the correct trade-off for a rare repair op. The dedup
// anti-join is always kept (never skipDedup) for the same reason.
func (m *DuckLakeMaterializer) backfillWrite(ctx context.Context, dec *decodedBatch) error {
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
		tsMin, tsMax := minMaxTime(dec.signals, func(r SignalRow) time.Time { return r.Timestamp })
		if _, err := tx.ExecContext(ctx, antiJoinInsert("lake.signals", tmp, tsMin, tsMax, false)); err != nil {
			return fmt.Errorf("insert signals: %w", err)
		}
	}
	if len(dec.events) > 0 {
		tmp, err := writeTempParquet(m.tempDir, writeEventParquet, dec.events)
		if err != nil {
			return err
		}
		cleanup = append(cleanup, tmp)
		tsMin, tsMax := minMaxTime(dec.events, func(r EventRow) time.Time { return r.Timestamp })
		if _, err := tx.ExecContext(ctx, antiJoinInsert("lake.events", tmp, tsMin, tsMax, false)); err != nil {
			return fmt.Errorf("insert events: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit backfill: %w", err)
	}
	// Refresh the rollups for the backfilled subjects on the next flush.
	for i := range dec.signals {
		m.dirtySubjects[dec.signals[i].Subject] = struct{}{}
	}
	for i := range dec.events {
		m.dirtyEventSubjects[dec.events[i].Subject] = struct{}{}
	}
	return nil
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

	files, err := m.insertDecodedSteady(ctx, tx, dec)
	cleanup = append(cleanup, files...)
	if err != nil {
		return err
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
	// The base rows are durable. Mark the subjects this batch wrote so the decoupled
	// FlushRollup/FlushEventRollup recompute only their rollup rows (B2/#5a). Done after
	// commit so a rolled-back batch never dirties the rollup.
	m.markDirtyFromBatch(dec)
	return nil
}

// insertDecodedSteady stages dec's signals+events as temp parquet and issues the
// idempotent (cloud_event_id anti-join) INSERTs into lake.signals/events within tx,
// clamping the dedup probe to dedupProbeFloor (the steady-state live-decode window —
// redeliveries are recent). Returns the temp-file paths the caller must remove after
// the transaction completes. Shared by commit (the cursor-advancing final window) and
// writeWindow (a non-cursor-advancing intermediate window); backfillWrite keeps its own
// unclamped probe for arbitrarily-old data.
func (m *DuckLakeMaterializer) insertDecodedSteady(ctx context.Context, tx *sql.Tx, dec *decodedBatch) ([]string, error) {
	var cleanup []string
	if len(dec.signals) > 0 {
		tmp, err := writeTempParquet(m.tempDir, writeSignalParquet, dec.signals)
		if err != nil {
			return cleanup, err
		}
		cleanup = append(cleanup, tmp)
		tsMin, tsMax := timeRange(dec.signals, func(r SignalRow) time.Time { return r.Timestamp })
		// #5b: steady-state lake.signals_latest is maintained INCREMENTALLY here
		// (O(batch), not an O(history) recompute per flush). The count delta must be
		// captured BEFORE the base insert — afterwards the batch rows are in the base and
		// the NOT-EXISTS probe finds them, yielding delta 0 (that is exactly what makes a
		// replayed window idempotent). Backfill (bulk/arbitrarily-old) skips the fold and
		// defers to the end-of-catch-up recompute (markDirtyFromBatch marks it dirty).
		if !m.backfillMode {
			if err := m.captureRollupDelta(ctx, tx, tmp); err != nil {
				return cleanup, err
			}
		}
		if _, err := tx.ExecContext(ctx, antiJoinInsert("lake.signals", tmp, tsMin, tsMax, m.backfillMode)); err != nil {
			return cleanup, fmt.Errorf("insert signals: %w", err)
		}
		if !m.backfillMode {
			if err := m.foldSignalsRollup(ctx, tx, tmp); err != nil {
				return cleanup, err
			}
		}
	}
	if len(dec.events) > 0 {
		tmp, err := writeTempParquet(m.tempDir, writeEventParquet, dec.events)
		if err != nil {
			return cleanup, err
		}
		cleanup = append(cleanup, tmp)
		tsMin, tsMax := timeRange(dec.events, func(r EventRow) time.Time { return r.Timestamp })
		if _, err := tx.ExecContext(ctx, antiJoinInsert("lake.events", tmp, tsMin, tsMax, m.backfillMode)); err != nil {
			return cleanup, fmt.Errorf("insert events: %w", err)
		}
	}
	return cleanup, nil
}

// markDirtyFromBatch records the subjects dec wrote so the decoupled rollups refresh
// only their rows, escalating to a full rebuild if a dirty set overflows its cap (a
// fleet-wide catch-up would otherwise grow the maps unbounded — see the field docs).
// signals_latest is now maintained incrementally at commit time (#5b), so signals are
// dirtied ONLY under backfillMode (the deferred bulk catch-up recomputes them at the
// end); events_latest still uses the decoupled recompute, so events are always dirtied.
// Single-writer: mutated only on the decode-loop goroutine.
func (m *DuckLakeMaterializer) markDirtyFromBatch(dec *decodedBatch) {
	if m.backfillMode {
		for i := range dec.signals {
			m.dirtySubjects[dec.signals[i].Subject] = struct{}{}
		}
		if len(m.dirtySubjects) > m.maxDirtySubjects {
			m.dirtySubjects = map[string]struct{}{}
			m.rollupFullRebuild = true
		}
	}
	for i := range dec.events {
		m.dirtyEventSubjects[dec.events[i].Subject] = struct{}{}
	}
	if len(m.dirtyEventSubjects) > m.maxDirtySubjects {
		m.dirtyEventSubjects = map[string]struct{}{}
		m.eventRollupFullRebuild = true
	}
}

// captureRollupDelta stages, into the per-connection temp table _rollup_delta, the number
// of NEWLY-DISTINCT (subject, name, timestamp) tuples this batch adds — i.e. the exact
// increment to lake.signals_latest.count (#5b). It must run BEFORE the base insert: it
// probes lake.signals for tuples that DON'T already exist, so a redelivery or a
// same-(subject,name,timestamp) collision (which the count must not double) contributes 0,
// and a replayed window (rows already at rest) yields 0 — the idempotency the crash-
// recovery path relies on. The probe matches on the EXACT timestamp (partition-pruned by
// subject_bucket + the day partition), deliberately NOT clamped to the anti-join's 30d
// dedup window: an ancient (>30d) redelivery must still find its existing row so count
// stays equal to a full RecomputeRollup, even though the physical anti-join may re-insert a
// duplicate row (the read-path QUALIFY dedup collapses that, and count must match it).
func (m *DuckLakeMaterializer) captureRollupDelta(ctx context.Context, tx *sql.Tx, sigParquet string) error {
	q := fmt.Sprintf(`CREATE OR REPLACE TEMPORARY TABLE _rollup_delta AS
SELECT b.subject, b.name, CAST(count(*) AS BIGINT) AS delta
FROM (SELECT DISTINCT subject, name, subject_bucket, "timestamp" FROM read_parquet(%[1]s)) b
WHERE NOT EXISTS (
  SELECT 1 FROM lake.signals s
  WHERE s.subject_bucket = b.subject_bucket AND s.subject = b.subject AND s.name = b.name
    AND s."timestamp" = b."timestamp"
)
GROUP BY b.subject, b.name`, sqlLit(sigParquet))
	if _, err := tx.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("capture rollup count delta: %w", err)
	}
	return nil
}

// foldSignalsRollup folds this batch into lake.signals_latest incrementally, EXACTLY as a
// full recompute would (proven by the differential test), but O(batch) instead of
// O(history) (#5b). It runs AFTER the base insert, inside the same transaction:
//   - RECENCY (timestamp, value_*, loc_*, loc_ts) is recomputed from the base but BOUNDED
//     to "timestamp >= the row's prior latest" — the new latest is either in this batch
//     (newer) or unchanged (the prior row still qualifies), so the bound is exact yet
//     prunes every day-partition older than the prior latest. This makes recency
//     SELF-HEALING (recomputed from the base each batch), matching the recompute's deduped
//     arg_max via ORDER BY timestamp DESC, cloud_event_id ASC.
//   - COUNT is prev.count + the captured NOT-EXISTS delta; FIRST_SEEN is min(prev, batch).
//     These carry forward (idempotent on replay), and self-heal via the boot rebuild
//     (RecomputeRollup / LAKE_REBUILD_ROLLUP_ON_BOOT) if a rollup row is ever lost.
// A (subject,name) with no prior rollup row folds against zero — correct in steady state
// (a newly-seen signal has no prior base); a mass-loss (dropped rollup) is the boot
// rebuild's job, exactly as before.
func (m *DuckLakeMaterializer) foldSignalsRollup(ctx context.Context, tx *sql.Tx, sigParquet string) error {
	build := fmt.Sprintf(`CREATE OR REPLACE TEMPORARY TABLE _rollup_new AS
WITH affected AS (
  SELECT subject, name, any_value(subject_bucket) AS subject_bucket, min("timestamp") AS batch_min
  FROM read_parquet(%[1]s) GROUP BY subject, name
),
prev AS (
  SELECT l.subject, l.name, l."timestamp" AS prev_ts, l.loc_ts AS prev_loc_ts, l.count AS prev_count, l.first_seen AS prev_first
  FROM lake.signals_latest l
  WHERE EXISTS (SELECT 1 FROM affected a WHERE a.subject = l.subject AND a.name = l.name)
),
recency AS (
  SELECT s.subject, s.name, s."timestamp" AS ts, s.value_number, s.value_string
  FROM lake.signals s
  JOIN affected a ON s.subject = a.subject AND s.name = a.name AND s.subject_bucket = a.subject_bucket
  LEFT JOIN prev p ON p.subject = s.subject AND p.name = s.name
  WHERE s."timestamp" >= coalesce(p.prev_ts, make_timestamp(0))
  QUALIFY row_number() OVER (PARTITION BY s.subject, s.name ORDER BY s."timestamp" DESC, s.cloud_event_id ASC) = 1
),
locrec AS (
  SELECT s.subject, s.name, s."timestamp" AS loc_ts, s.loc_lat, s.loc_lon, s.loc_hdop, s.loc_heading
  FROM lake.signals s
  JOIN affected a ON s.subject = a.subject AND s.name = a.name AND s.subject_bucket = a.subject_bucket
  LEFT JOIN prev p ON p.subject = s.subject AND p.name = s.name
  WHERE (s.loc_lat != 0 OR s.loc_lon != 0) AND s."timestamp" >= coalesce(p.prev_loc_ts, make_timestamp(0))
  QUALIFY row_number() OVER (PARTITION BY s.subject, s.name ORDER BY s."timestamp" DESC, s.cloud_event_id ASC) = 1
)
SELECT a.subject, a.subject_bucket, a.name,
  r.ts AS "timestamp", r.value_number, r.value_string,
  coalesce(lr.loc_lat, 0) AS loc_lat, coalesce(lr.loc_lon, 0) AS loc_lon,
  coalesce(lr.loc_hdop, 0) AS loc_hdop, coalesce(lr.loc_heading, 0) AS loc_heading,
  coalesce(lr.loc_ts, make_timestamp(0)) AS loc_ts,
  coalesce(p.prev_count, 0) + coalesce(d.delta, 0) AS count,
  LEAST(coalesce(p.prev_first, a.batch_min), a.batch_min) AS first_seen,
  r.ts AS last_seen
FROM affected a
JOIN recency r ON r.subject = a.subject AND r.name = a.name
LEFT JOIN locrec lr ON lr.subject = a.subject AND lr.name = a.name
LEFT JOIN prev p ON p.subject = a.subject AND p.name = a.name
LEFT JOIN _rollup_delta d ON d.subject = a.subject AND d.name = a.name`, sqlLit(sigParquet))
	if _, err := tx.ExecContext(ctx, build); err != nil {
		return fmt.Errorf("build incremental rollup rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM lake.signals_latest WHERE EXISTS (SELECT 1 FROM _rollup_new n WHERE n.subject = lake.signals_latest.subject AND n.name = lake.signals_latest.name)`); err != nil {
		return fmt.Errorf("delete superseded rollup rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO lake.signals_latest (subject, subject_bucket, name, "timestamp", value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading, loc_ts, count, first_seen, last_seen)
		 SELECT subject, subject_bucket, name, "timestamp", value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading, loc_ts, count, first_seen, last_seen FROM _rollup_new`); err != nil {
		return fmt.Errorf("insert incremental rollup rows: %w", err)
	}
	return nil
}

// writeWindow writes an INTERMEDIATE pagination window (finding #1c): the decoded rows
// go into lake.signals/events in their own transaction that does NOT advance the ingest
// cursor. It is idempotent (the cloud_event_id anti-join) and cursor-independent, so —
// unlike the final commit — ANY commit error, lost ack included, is safe to treat as a
// failure: the pass aborts, and restart re-reads from the un-advanced cursor while the
// anti-join collapses whatever landed. A window with no decoded rows (e.g. all payloads
// were poison-dropped) is a no-op.
func (m *DuckLakeMaterializer) writeWindow(ctx context.Context, dec *decodedBatch) error {
	if len(dec.signals) == 0 && len(dec.events) == 0 {
		return nil
	}
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
		return fmt.Errorf("begin window: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	files, err := m.insertDecodedSteady(ctx, tx, dec)
	cleanup = append(cleanup, files...)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		if isCommitConflict(err) {
			return errSnapshotMoved
		}
		return fmt.Errorf("commit window: %w", err)
	}
	m.markDirtyFromBatch(dec)
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

// recoverPoisonedSession detects a DuckLake session invalidated mid-query and
// discards the pool's idle connections so the next pass runs on fresh
// sessions. The trigger: din's catalog-wide maintenance (flush_inlined_data /
// merge) can rewrite the ducklake_inlined_data_* tables out from under an
// in-flight dq read; the ducklake extension throws a FATAL that leaves the
// connection's catalog transaction permanently aborted, and the pool happily
// re-serves that connection — every subsequent pass fails with "Current
// transaction is aborted (please ROLLBACK)" until the 30m conn lifetime or
// the 1h restart backstop. Proven end-to-end with both binaries sharing a
// live catalog; in prod the collision recurs every maintenance interval.
// SetMaxIdleConns(0) closes all idle conns (the poisoned one was just
// returned by the failed pass); restoring the limit lets fresh sessions
// re-open lazily. Returns true when the error matched.
// isDatabaseInvalidated reports the unrecoverable in-process failure mode:
// a ducklake-extension crash (observed: "Attempted to access index 0 within
// vector of size 0" when din maintenance flushes inlined data under a
// concurrent read) invalidates the ENTIRE embedded DuckDB instance — every
// future query on every connection fails with "database has been
// invalidated". No pool recycling can heal it; only a process restart can.
// Proven by the two-binary e2e; classified so the runner restarts in seconds
// instead of grinding the 1h failure window while decode is down.
func isDatabaseInvalidated(err error) bool {
	return err != nil && strings.Contains(err.Error(), "database has been invalidated")
}

func (m *DuckLakeMaterializer) recoverPoisonedSession(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.Contains(msg, "transaction is aborted") &&
		!strings.Contains(msg, "Current transaction is aborted") {
		return false
	}
	max := m.db.Stats().MaxOpenConnections
	m.db.SetMaxIdleConns(0)
	if max > 0 {
		m.db.SetMaxIdleConns(max)
	}
	return true
}

// minMaxTime returns the min and max ts(row) over rows (both zero when empty), with
// NO probe-floor clamp. The backfill path uses this directly so its dedup anti-join
// probes the data's TRUE timestamp window and stays idempotent on arbitrarily old data
// (finding #1a) — the steady-state timeRange clamp would miss old duplicates.
func minMaxTime[T any](rows []T, ts func(T) time.Time) (minT, maxT time.Time) {
	for i := range rows {
		t := ts(rows[i])
		if i == 0 || t.Before(minT) {
			minT = t
		}
		if i == 0 || t.After(maxT) {
			maxT = t
		}
	}
	return minT, maxT
}

// timeRange is minMaxTime with the steady-state dedup probe floor (clampProbeFloor):
// a single epoch/ancient row must not drag the anti-join probe to a full-history scan
// on every batch (M5). Used by the hot commit path; backfill uses minMaxTime instead.
func timeRange[T any](rows []T, ts func(T) time.Time) (minT, maxT time.Time) {
	return clampProbeFloor(minMaxTime(rows, ts))
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
	// Same orphan cleanup for the events rollup (finding #5a): drop events_latest rows
	// whose base events were all pruned away, bounded to last_seen < cutoff so the
	// anti-join probes only long-dormant rows and each NOT EXISTS is partition-pruned.
	if _, err := m.db.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM lake.events_latest el WHERE el.last_seen < make_timestamp(%d) AND NOT EXISTS (
			SELECT 1 FROM lake.events e
			WHERE e.subject_bucket = el.subject_bucket AND e.subject = el.subject AND e.name = el.name)`, cutoff)); err != nil {
		return total, fmt.Errorf("pruning orphaned event rollup rows: %w", err)
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

// rollupSelectSQL is the FULL-history recompute of a set of rollup rows. Steady-state
// maintenance no longer uses it — lake.signals_latest is folded INCREMENTALLY at commit
// time (foldSignalsRollup, #5b), O(batch) not O(history). This recompute is retained for
// the paths that genuinely need a from-scratch rebuild: the disaster-recovery / boot
// rebuild (RecomputeRollup / LAKE_REBUILD_ROLLUP_ON_BOOT) and the deferred bulk-backfill
// catch-up (FlushRollup over backfill-dirtied subjects). Kept byte-identical to the fold's
// result — the differential test (tests/ducklake_incremental_rollup_test.go) asserts the
// incremental path equals this recompute across redelivery, same-timestamp collision,
// out-of-order arrival, multi-window spans, and crash-replay.
//
// Why the fold is exact where a naive `timestamp >= floor` recompute would not be:
// RECENCY (timestamp/value_*/loc_*/loc_ts) IS recomputed each batch, but bounded to
// `timestamp >= the row's prior latest` — the new latest is either in the batch or the
// prior row still qualifies, so the bound prunes old day-partitions without dropping the
// answer (self-healing + exact). COUNT and FIRST_SEEN are the full-history aggregates a
// floor would corrupt, so they are NOT floored: count carries forward as prev.count + a
// NOT-EXISTS delta over DISTINCT (subject,name,timestamp) — collisions and redeliveries
// contribute 0, and a replayed window contributes 0 (idempotent) — and first_seen min-folds
// the batch min. Both self-heal via this recompute on boot if a rollup row is ever lost.
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

// eventsLatestColumns names lake.events_latest's columns in CREATE order so the
// rollup INSERTs bind by name, not position (see signalsLatestColumns for why a
// positional bind would silently corrupt on a reordered SELECT).
const eventsLatestColumns = ` (subject, subject_bucket, name, count, first_seen, last_seen) `

// eventRollupSelectSQL builds the per-(subject,name) event summary SELECT over the
// deduped base table, restricted by whereClause ("" for the whole table). It mirrors
// GetEventSummaries exactly: the same (subject,timestamp,name,source) dedup as
// LakeEventsDeduped (events include source — the events read dedup key #3), then
// count/min/max GROUP BY name. So events_latest is a materialized view of
// GetEventSummaries — parity by construction (finding #5a).
func eventRollupSelectSQL(whereClause string) string {
	return fmt.Sprintf(`SELECT subject, any_value(subject_bucket) AS subject_bucket, name,
		CAST(count(*) AS BIGINT) AS count,
		min(timestamp) AS first_seen, max(timestamp) AS last_seen
	FROM (SELECT * FROM lake.events %[1]s
	      QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, timestamp, name, source ORDER BY cloud_event_id) = 1)
	GROUP BY subject, name`, whereClause)
}

// FlushEventRollup is the events_latest analogue of FlushRollup (finding #5a):
// recompute lake.events_latest for every subject whose lake.events rows changed
// since the last flush, subject-scoped and bucket-chunked off the decode commit. A
// no-op when nothing is dirty. On overflow / first-create it escalates to a full
// RecomputeEventRollup. Single-writer: called only on the decode-loop goroutine.
func (m *DuckLakeMaterializer) FlushEventRollup(ctx context.Context) error {
	if m.eventRollupFullRebuild {
		start := time.Now()
		defer func() { eventRollupRefreshSeconds.Set(time.Since(start).Seconds()) }()
		if err := m.RecomputeEventRollup(ctx); err != nil {
			return err
		}
		m.eventRollupFullRebuild = false
		m.dirtyEventSubjects = map[string]struct{}{} // the rebuild covers anything accrued since
		return nil
	}
	if len(m.dirtyEventSubjects) == 0 {
		return nil
	}
	byBucket := map[int][]string{}
	for s := range m.dirtyEventSubjects {
		b := duck.HashBucket(s)
		byBucket[b] = append(byBucket[b], s)
	}
	buckets := make([]int, 0, len(byBucket))
	for b := range byBucket {
		buckets = append(buckets, b)
	}
	sort.Ints(buckets)

	start := time.Now()
	defer func() { eventRollupRefreshSeconds.Set(time.Since(start).Seconds()) }()
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
			if err := m.recomputeEventSubjects(ctx, b, chunk); err != nil {
				return fmt.Errorf("event rollup recompute bucket %d (%d subjects): %w", b, len(chunk), err)
			}
			for _, s := range chunk {
				delete(m.dirtyEventSubjects, s) // keep unflushed subjects dirty for retry
			}
		}
	}
	return nil
}

// recomputeEventSubjects DELETEs+recomputes the events_latest rows of one bucket's
// given subjects in a single transaction (the events analogue of recomputeSubjects).
func (m *DuckLakeMaterializer) recomputeEventSubjects(ctx context.Context, bucket int, subjects []string) error {
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
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.events_latest "+where, args...); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.events_latest"+eventsLatestColumns+eventRollupSelectSQL(where), args...); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return tx.Commit()
}

// RecomputeEventRollup rebuilds lake.events_latest from the ENTIRE lake.events base,
// bucket-partitioned (one txn each) so it stays memory-bounded over deep history.
// The events analogue of RecomputeRollup — the disaster-recovery / first-create
// backfill path (LAKE_REBUILD_ROLLUP_ON_BOOT, and the events_latest migration).
func (m *DuckLakeMaterializer) RecomputeEventRollup(ctx context.Context) error {
	for b := 0; b < duck.NumLatestBuckets; b++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.recomputeOneEventBucket(ctx, fmt.Sprintf("WHERE subject_bucket = %d", b)); err != nil {
			return fmt.Errorf("event rollup recompute bucket %d: %w", b, err)
		}
	}
	return nil
}

func (m *DuckLakeMaterializer) recomputeOneEventBucket(ctx context.Context, where string) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.events_latest "+where); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.events_latest"+eventsLatestColumns+eventRollupSelectSQL(where)); err != nil {
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
// batch actually spans (it is PARTITIONED BY year/month/day("timestamp")) instead of
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
