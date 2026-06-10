package duck

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
)

// DefaultRawScanWindowDays bounds date-unbounded raw queries. Older data is
// reachable with explicit time bounds; LatestCloudEvent walks past it.
const DefaultRawScanWindowDays = 90

// latestWalkMaxDays caps the newest-first partition walk for latest-event
// queries so a never-seen subject cannot trigger an unbounded scan.
const latestWalkMaxDays = 400

// RawFilter narrows a raw cloudevent scan. Zero values mean "no filter".
// The eventrepo facade translates grpc.SearchOptions into this.
type RawFilter struct {
	Subject   string
	Types     []string
	Sources   []string
	Producers []string
	IDs       []string
	After     time.Time
	Before    time.Time
}

// Raw queries raw cloudevent bundles (raw/type=T/date=D hive layout)
// directly with DuckDB — the replacement for the ClickHouse cloud_event
// index plus per-row parquet seeks. Data comes back inline.
//
// TODO(voids): tombstone voiding (the CH voids_id column) is not applied
// yet; the voided id lives inside tombstone payloads, not in a parquet
// column. Readers see both the attestation and its tombstone until a
// voiding pass lands here or in compaction.
type Raw struct {
	svc       *Service
	bucket    string
	rawPrefix string
}

// NewRaw builds a Raw query service over the given bucket and prefix.
func NewRaw(svc *Service, bucket, rawPrefix string) *Raw {
	if rawPrefix == "" {
		rawPrefix = "raw"
	}
	return &Raw{svc: svc, bucket: bucket, rawPrefix: rawPrefix}
}

// rawColumns matches cloudevent/parquet.ParquetRow.
const rawColumns = "subject, time, type, id, source, producer, data_content_type, data_version, extras, data, data_base64, data_index_key"

// ListCloudEvents returns events matching filter, newest first, capped at
// limit. Duplicate rows (at-least-once ingest, compaction grace window)
// collapse on the header uniqueness key.
func (r *Raw) ListCloudEvents(ctx context.Context, filter RawFilter, limit int) ([]cloudevent.StoredEvent, error) {
	from, to := r.scanWindow(filter)
	globs, err := r.existingGlobs(ctx, filter.Types, from, to)
	if err != nil || len(globs) == 0 {
		return nil, err
	}
	return r.query(ctx, globs, filter, limit)
}

// LatestCloudEvent walks date partitions newest-first in week chunks and
// returns the most recent matching event within latestWalkMaxDays.
func (r *Raw) LatestCloudEvent(ctx context.Context, filter RawFilter) (cloudevent.StoredEvent, error) {
	to := time.Now().UTC()
	if !filter.Before.IsZero() {
		to = filter.Before
	}
	floor := to.AddDate(0, 0, -latestWalkMaxDays)
	if !filter.After.IsZero() && filter.After.After(floor) {
		floor = filter.After
	}

	for chunkEnd := to; chunkEnd.After(floor); chunkEnd = chunkEnd.AddDate(0, 0, -7) {
		chunkStart := chunkEnd.AddDate(0, 0, -7)
		if chunkStart.Before(floor) {
			chunkStart = floor
		}
		globs, err := r.existingGlobs(ctx, filter.Types, chunkStart, chunkEnd)
		if err != nil {
			return cloudevent.StoredEvent{}, err
		}
		if len(globs) == 0 {
			continue
		}
		events, err := r.query(ctx, globs, filter, 1)
		if err != nil {
			return cloudevent.StoredEvent{}, err
		}
		if len(events) > 0 {
			return events[0], nil
		}
	}
	return cloudevent.StoredEvent{}, fmt.Errorf("no cloud event found for subject %q within %d days", filter.Subject, latestWalkMaxDays)
}

// AvailableCloudEventTypes returns the distinct event types present for a
// subject in the window.
func (r *Raw) AvailableCloudEventTypes(ctx context.Context, subject string, from, to time.Time) ([]string, error) {
	filter := RawFilter{Subject: subject, After: from, Before: to}
	winFrom, winTo := r.scanWindow(filter)
	globs, err := r.existingGlobs(ctx, nil, winFrom, winTo)
	if err != nil || len(globs) == 0 {
		return nil, err
	}

	where, args := whereClause(filter)
	query := fmt.Sprintf("SELECT DISTINCT type FROM %s WHERE %s ORDER BY type", ReadParquetSQL(globs), where)
	rows, err := r.svc.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying available types: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var types []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("scanning type: %w", err)
		}
		types = append(types, t)
	}
	return types, rows.Err()
}

// scanWindow applies the default lookback when the caller gave no bounds.
func (r *Raw) scanWindow(filter RawFilter) (time.Time, time.Time) {
	to := time.Now().UTC()
	if !filter.Before.IsZero() {
		to = filter.Before
	}
	from := to.AddDate(0, 0, -DefaultRawScanWindowDays)
	if !filter.After.IsZero() {
		from = filter.After
	}
	return from, to
}

// existingGlobs expands per-day/per-type patterns through DuckDB's glob()
// so read_parquet never sees a zero-match pattern (which errors). An empty
// type list scans every type= partition for the window.
func (r *Raw) existingGlobs(ctx context.Context, types []string, from, to time.Time) ([]string, error) {
	if len(types) == 0 {
		types = []string{"*"}
	}
	patterns := RawGlobs(r.bucket, r.rawPrefix, types, from, to)
	if len(patterns) == 0 {
		return nil, nil
	}
	parts := make([]string, len(patterns))
	for i, p := range patterns {
		parts[i] = "SELECT file FROM glob(" + sqlString(p) + ")"
	}
	stmt := strings.Join(parts, " UNION ALL ")
	rows, err := r.svc.DB().QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("expanding raw globs: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var files []string
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, fmt.Errorf("scanning glob result: %w", err)
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

func whereClause(filter RawFilter) (string, []any) {
	conds := []string{"1=1"}
	var args []any
	addIn := func(col string, vals []string) {
		if len(vals) == 0 {
			return
		}
		conds = append(conds, fmt.Sprintf("%s IN (%s)", col, placeholders(len(vals))))
		for _, v := range vals {
			args = append(args, v)
		}
	}
	if filter.Subject != "" {
		conds = append(conds, "subject = ?")
		args = append(args, filter.Subject)
	}
	addIn("type", filter.Types)
	addIn("source", filter.Sources)
	addIn("producer", filter.Producers)
	addIn("id", filter.IDs)
	if !filter.After.IsZero() {
		conds = append(conds, "time >= ?")
		args = append(args, filter.After.UTC())
	}
	if !filter.Before.IsZero() {
		conds = append(conds, "time < ?")
		args = append(args, filter.Before.UTC())
	}
	return strings.Join(conds, " AND "), args
}

func (r *Raw) query(ctx context.Context, globs []string, filter RawFilter, limit int) ([]cloudevent.StoredEvent, error) {
	where, args := whereClause(filter)
	// Over-fetch headroom so duplicate collapse still fills the limit.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY time DESC LIMIT %d",
		rawColumns, ReadParquetSQL(globs), where, limit*2)

	rows, err := r.svc.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying raw cloudevents: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	seen := map[string]struct{}{}
	var events []cloudevent.StoredEvent
	for rows.Next() && len(events) < limit {
		ev, err := scanStoredEvent(rows)
		if err != nil {
			return nil, err
		}
		key := ev.Key()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// rowScanner abstracts *sql.Rows for scanStoredEvent.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanStoredEvent mirrors cloudevent/parquet's convertRow: rebuild the
// header, restore non-column fields from extras, attach payload.
func scanStoredEvent(row rowScanner) (cloudevent.StoredEvent, error) {
	var ev cloudevent.StoredEvent
	var extras, data, dataIndexKey *string
	var dataBase64 []byte
	if err := row.Scan(&ev.Subject, &ev.Time, &ev.Type, &ev.ID, &ev.Source, &ev.Producer,
		&ev.DataContentType, &ev.DataVersion, &extras, &data, &dataBase64, &dataIndexKey); err != nil {
		return ev, fmt.Errorf("scanning raw cloudevent: %w", err)
	}
	ev.SpecVersion = cloudevent.SpecVersion
	ev.Time = ev.Time.UTC()

	if extras != nil && *extras != "" && *extras != "{}" {
		ev.Extras = map[string]any{}
		if err := json.Unmarshal([]byte(*extras), &ev.Extras); err != nil {
			return ev, fmt.Errorf("decoding extras for %s: %w", ev.ID, err)
		}
		cloudevent.RestoreNonColumnFields(&ev.CloudEventHeader)
	}
	if len(dataBase64) > 0 {
		ev.DataBase64 = string(dataBase64)
	} else if data != nil {
		ev.Data = json.RawMessage(*data)
	}
	if dataIndexKey != nil {
		ev.DataIndexKey = *dataIndexKey
	}
	return ev, nil
}
