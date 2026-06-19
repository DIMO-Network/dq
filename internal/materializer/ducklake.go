package materializer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/rs/zerolog"
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
}

// snapshotCursorPartition is the single ingest_progress key holding the last
// raw_events snapshot id this decoder has processed.
const snapshotCursorPartition = "lake.raw_events#snapshot"

// rawEventCols is the lake.raw_events projection, matching din's DDL order.
const rawEventCols = `subject, "time", type, id, source, producer, ` +
	`data_content_type, data_version, extras, data, data_base64, data_index_key, voids_id`

// NewDuckLakeMaterializer ensures the decoded tables + cursor row exist and
// returns a materializer over db (which must have the shared catalog attached
// as schema "lake", with din's raw_events present).
func NewDuckLakeMaterializer(ctx context.Context, db *sql.DB, log zerolog.Logger) (*DuckLakeMaterializer, error) {
	m := &DuckLakeMaterializer{db: db, log: log.With().Str("component", "ducklake-materializer").Logger()}
	if err := m.ensureSchema(ctx); err != nil {
		return nil, err
	}
	return m, nil
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

func (m *DuckLakeMaterializer) ensureSchema(ctx context.Context) error {
	sigTmp, err := writeTempParquet(writeSignalParquet, []SignalRow{})
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(sigTmp) }()
	evTmp, err := writeTempParquet(writeEventParquet, []EventRow{})
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

// errExpiredCursor means the cursor points before the oldest retained
// snapshot: din expired the change feed for the range while the consumer
// lagged past LAKE_SNAPSHOT_RETENTION.
var errExpiredCursor = errors.New("snapshot cursor expired")

// isExpiredSnapshot best-effort classifies a ducklake_table_changes error as
// "the requested snapshot range is no longer retained" vs a transient fault.
func isExpiredSnapshot(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "snapshot") &&
		(strings.Contains(s, "expired") || strings.Contains(s, "not found") ||
			strings.Contains(s, "does not exist") || strings.Contains(s, "out of range"))
}

// resetCursor advances the cursor from->to without decoding, used only on
// expired-feed recovery. The gap is unavoidably skipped.
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
		observeLakeLag(nil) // caught up: no decode lag
		return 0, nil
	}

	events, err := m.readDelta(ctx, cur, head)
	if errors.Is(err, errExpiredCursor) {
		// The consumer lagged past LAKE_SNAPSHOT_RETENTION: din expired the
		// snapshots covering (cur, oldestRetained], so the change feed for
		// that range is gone. Skip to head and alert — wedging in a permanent
		// error loop is worse. The gap is a misconfiguration (retention must
		// exceed max consumer lag), made visible by the counter.
		cursorResetsTotal.Inc()
		cursorResetGap.Set(float64(head - cur))
		m.log.Error().Int64("from", cur).Int64("to", head).Int64("skipped_snapshots", head-cur).
			Msg("DuckLake change feed expired; resetting cursor to head (un-decoded gap skipped — increase LAKE_SNAPSHOT_RETENTION)")
		if rerr := m.resetCursor(ctx, cur, head); rerr != nil {
			return 0, rerr
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		// The (cur, head] range held no raw_events inserts — head advanced
		// only because of this decoder's own writes to signals/events. Don't
		// burn a cursor-advance transaction; the next pass re-reads the same
		// empty range cheaply and the cursor moves once real data arrives.
		observeLakeLag(nil) // no pending raw events: no decode lag
		return 0, nil
	}
	observeLakeLag(events) // decode lag = age of the oldest pending event
	decoded := dec.decodeEvents(ctx, events)

	if err := m.commit(ctx, decoded, cur, head); err != nil {
		if errors.Is(err, errSnapshotMoved) {
			return 0, nil // another decoder won this range; retry next pass
		}
		return 0, err
	}
	// A batch committed: feed the freshness/throughput alerts the bucket path
	// already drove (dead in ducklake mode before CHD-12).
	batchesTotal.WithLabelValues(lakeMetricType).Inc()
	rowsTotal.WithLabelValues("signals").Add(float64(decoded.signalCount))
	rowsTotal.WithLabelValues("events").Add(float64(decoded.eventCount))
	errorsTotal.Add(float64(decoded.errorCount))
	cursorSnapshotID.Set(float64(head))
	// Report progress to din's snapshot-expiry floor. Best-effort: the batch
	// is already durable, and a failed report only holds expiry back
	// (din won't reclaim past the stale floor — conservative, not unsafe).
	m.reportProgress(ctx, head)
	return len(events), nil
}

// consumerName is the identity dq reports under in meta.din_consumer_progress.
// din takes MIN(snapshot_id) over live consumers as the expiry floor; all dq
// replicas share one logical cursor, so they all report under this name.
const consumerName = "dq"

// reportProgress upserts dq's processed snapshot id into din's consumer-floor
// table so the maintainer never expires snapshots dq hasn't read.
func (m *DuckLakeMaterializer) reportProgress(ctx context.Context, snapshotID int64) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		m.log.Warn().Err(err).Msg("consumer progress report: begin failed (expiry floor not advanced)")
		return
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM meta.din_consumer_progress WHERE consumer = ?", consumerName); err != nil {
		m.log.Warn().Err(err).Msg("consumer progress report: delete failed")
		return
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO meta.din_consumer_progress VALUES (?, ?, now())", consumerName, snapshotID); err != nil {
		m.log.Warn().Err(err).Msg("consumer progress report: insert failed")
		return
	}
	if err := tx.Commit(); err != nil {
		m.log.Warn().Err(err).Msg("consumer progress report: commit failed (expiry floor not advanced)")
	}
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
		rawEventCols, from+1, to)
	rows, err := m.db.QueryContext(ctx, q)
	if err != nil {
		if isExpiredSnapshot(err) {
			return nil, fmt.Errorf("%w: %v", errExpiredCursor, err)
		}
		return nil, fmt.Errorf("reading raw_events delta: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var out []cloudevent.RawEvent
	for rows.Next() {
		ev, blobKey, err := scanRawEvent(rows)
		if err != nil {
			return nil, err
		}
		if err := m.resolveBlob(ctx, &ev, blobKey); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
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
			return ev, "", fmt.Errorf("decoding extras for %s: %w", ev.ID, err)
		}
		cloudevent.RestoreNonColumnFields(&ev.CloudEventHeader)
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
		tmp, err := writeTempParquet(writeSignalParquet, dec.signals)
		if err != nil {
			return err
		}
		cleanup = append(cleanup, tmp)
		if _, err := tx.ExecContext(ctx, antiJoinInsert("lake.signals", tmp)); err != nil {
			return fmt.Errorf("insert signals: %w", err)
		}
	}
	if len(dec.events) > 0 {
		tmp, err := writeTempParquet(writeEventParquet, dec.events)
		if err != nil {
			return err
		}
		cleanup = append(cleanup, tmp)
		if _, err := tx.ExecContext(ctx, antiJoinInsert("lake.events", tmp)); err != nil {
			return fmt.Errorf("insert events: %w", err)
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

	if err := tx.Commit(); err != nil {
		if isCommitConflict(err) {
			return errSnapshotMoved
		}
		return fmt.Errorf("commit: %w", err)
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

// writeTempParquet writes rows via enc into a temp file DuckDB can read and
// returns its path; the caller removes it.
func writeTempParquet[T any](enc func([]T) ([]byte, error), rows []T) (string, error) {
	body, err := enc(rows)
	if err != nil {
		return "", fmt.Errorf("encoding parquet: %w", err)
	}
	f, err := os.CreateTemp("", "ducklake-*.parquet")
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
