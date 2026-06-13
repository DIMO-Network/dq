package materializer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// DuckLakeWriter persists decoded batches into a DuckLake catalog instead of
// the bucket-file layout. The commit protocol is one catalog transaction:
// insert the decoded rows and advance the per-partition cursor together, so
// the transaction itself is the manifest+watermark — exactly-once by
// construction. Concurrent writers (multiple replicas on a Postgres catalog)
// are safe: a same-partition race conflicts at commit and the loser aborts,
// so rows are never double-inserted.
//
// din's raw compactor still reads decoded/v1/_state/watermark.json over S3,
// so after each pass the writer projects the catalog cursor back to that key
// — din needs no catalog access.
type DuckLakeWriter struct {
	db            *sql.DB
	store         ObjectStore
	decodedPrefix string
}

// NewDuckLakeWriter ensures the lake schema exists and returns a writer.
// db must have a DuckLake catalog attached as schema "lake" (see
// duck.Config.DuckLakeEnabled).
func NewDuckLakeWriter(ctx context.Context, db *sql.DB, store ObjectStore, decodedPrefix string) (*DuckLakeWriter, error) {
	w := &DuckLakeWriter{db: db, store: store, decodedPrefix: ensureSlash(decodedPrefix)}
	if err := w.ensureSchema(ctx); err != nil {
		return nil, err
	}
	return w, nil
}

func ensureSlash(p string) string {
	if p == "" {
		return "decoded/v1/"
	}
	if !strings.HasSuffix(p, "/") {
		return p + "/"
	}
	return p
}

// ensureSchema creates the lake tables by inferring the signal/event column
// types from an empty parquet write of the row structs, so the table schema
// can never drift from SignalRow/EventRow.
func (w *DuckLakeWriter) ensureSchema(ctx context.Context) error {
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
		// DuckLake rejects PRIMARY KEY/UNIQUE; the cursor's uniqueness is
		// enforced by the conditional UPDATE in commit, not a constraint.
		"CREATE TABLE IF NOT EXISTS lake.ingest_progress (partition VARCHAR, cursor VARCHAR)",
	}
	for _, s := range stmts {
		if _, err := w.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("ensuring lake schema: %w", err)
		}
	}
	return nil
}

// LoadCursors reads every partition cursor from the catalog. The returned
// map plugs straight into pendingBatches in place of watermark.json.
func (w *DuckLakeWriter) LoadCursors(ctx context.Context) (map[string]string, error) {
	rows, err := w.db.QueryContext(ctx, "SELECT partition, cursor FROM lake.ingest_progress")
	if err != nil {
		return nil, fmt.Errorf("loading cursors: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	cursors := map[string]string{}
	for rows.Next() {
		var p, c string
		if err := rows.Scan(&p, &c); err != nil {
			return nil, fmt.Errorf("scanning cursor: %w", err)
		}
		cursors[p] = c
	}
	return cursors, rows.Err()
}

// errCursorMoved means another writer advanced this partition first; the
// caller skips the batch (it is or will be processed by the winner).
var errCursorMoved = errors.New("cursor advanced by another writer")

// WriteBatch inserts a decoded batch and advances the partition cursor in one
// transaction. oldCursor is the cursor the batch was planned against ("" for a
// never-seen partition); newCursor is the batch's last raw key.
func (w *DuckLakeWriter) WriteBatch(ctx context.Context, b rawBatch, dec *decodedBatch, oldCursor string) error {
	var cleanup []string
	defer func() {
		for _, f := range cleanup {
			_ = os.Remove(f)
		}
	}()

	conn, err := w.db.Conn(ctx)
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

	// Conditional cursor advance. A new partition inserts; an existing one
	// updates only if its cursor still equals what we planned against.
	if oldCursor == "" {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO lake.ingest_progress VALUES (?, ?)", b.partition, b.lastKey()); err != nil {
			return fmt.Errorf("insert cursor: %w", err)
		}
	} else {
		res, err := tx.ExecContext(ctx,
			"UPDATE lake.ingest_progress SET cursor = ? WHERE partition = ? AND cursor = ?",
			b.lastKey(), b.partition, oldCursor)
		if err != nil {
			return fmt.Errorf("advance cursor: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return errCursorMoved
		}
	}

	if err := tx.Commit(); err != nil {
		// A concurrent writer that touched the same partition wins; we abort.
		if isCommitConflict(err) {
			return errCursorMoved
		}
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// ProjectWatermark writes the catalog cursors to decoded/v1/_state/watermark.json
// so din's raw compactor advances without catalog access.
func (w *DuckLakeWriter) ProjectWatermark(ctx context.Context, cursors map[string]string) error {
	body, err := json.Marshal(cursors)
	if err != nil {
		return fmt.Errorf("marshaling watermark projection: %w", err)
	}
	if err := w.store.PutObject(ctx, w.decodedPrefix+"_state/watermark.json", body); err != nil {
		return fmt.Errorf("projecting watermark: %w", err)
	}
	return nil
}

func isCommitConflict(err error) bool {
	return strings.Contains(err.Error(), "conflict")
}

// lastKey returns the highest (lexicographic = time) raw key in the batch.
func (b rawBatch) lastKey() string {
	return b.keys[len(b.keys)-1]
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
