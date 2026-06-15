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
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS lake.signals AS SELECT * FROM read_parquet(%s) LIMIT 0", sqlLit(sigTmp)),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS lake.events AS SELECT * FROM read_parquet(%s) LIMIT 0", sqlLit(evTmp)),
		"CREATE TABLE IF NOT EXISTS lake.ingest_progress (partition VARCHAR, cursor VARCHAR)",
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
	if from == 0 {
		_, err := m.db.ExecContext(ctx,
			"INSERT INTO lake.ingest_progress VALUES (?, ?)", snapshotCursorPartition, fmt.Sprint(to))
		return err
	}
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
	if head <= cur {
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
		m.log.Error().Int64("from", cur).Int64("to", head).
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
		return 0, nil
	}
	decoded := dec.decodeEvents(ctx, events)

	if err := m.commit(ctx, decoded, cur, head); err != nil {
		if errors.Is(err, errSnapshotMoved) {
			return 0, nil // another decoder won this range; retry next pass
		}
		return 0, err
	}
	return len(events), nil
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
		ev, err := scanRawEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// scanRawEvent rebuilds a RawEvent from a raw_events row, restoring the
// non-column header fields from extras exactly like the parquet decoder.
func scanRawEvent(rows *sql.Rows) (cloudevent.RawEvent, error) {
	var ev cloudevent.RawEvent
	var ts time.Time
	var extras, data, dataIndexKey, voidsID sql.NullString
	var dataBase64 []byte
	if err := rows.Scan(&ev.Subject, &ts, &ev.Type, &ev.ID, &ev.Source, &ev.Producer,
		&ev.DataContentType, &ev.DataVersion, &extras, &data, &dataBase64, &dataIndexKey, &voidsID); err != nil {
		return ev, fmt.Errorf("scanning raw_events row: %w", err)
	}
	ev.SpecVersion = cloudevent.SpecVersion
	ev.Time = ts.UTC()
	if extras.Valid && extras.String != "" && extras.String != "{}" {
		ev.Extras = map[string]any{}
		if err := json.Unmarshal([]byte(extras.String), &ev.Extras); err != nil {
			return ev, fmt.Errorf("decoding extras for %s: %w", ev.ID, err)
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
	// the raw query path. Discard it here.
	_ = voidsID
	return ev, nil
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
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO lake.signals SELECT * FROM read_parquet(%s)", sqlLit(tmp))); err != nil {
			return fmt.Errorf("insert signals: %w", err)
		}
	}
	if len(dec.events) > 0 {
		tmp, err := writeTempParquet(writeEventParquet, dec.events)
		if err != nil {
			return err
		}
		cleanup = append(cleanup, tmp)
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO lake.events SELECT * FROM read_parquet(%s)", sqlLit(tmp))); err != nil {
			return fmt.Errorf("insert events: %w", err)
		}
	}

	if from == 0 {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO lake.ingest_progress VALUES (?, ?)", snapshotCursorPartition, fmt.Sprint(to)); err != nil {
			return fmt.Errorf("insert cursor: %w", err)
		}
	} else {
		res, err := tx.ExecContext(ctx,
			"UPDATE lake.ingest_progress SET cursor = ? WHERE partition = ? AND cursor = ?",
			fmt.Sprint(to), snapshotCursorPartition, fmt.Sprint(from))
		if err != nil {
			return fmt.Errorf("advance cursor: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return errSnapshotMoved
		}
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
