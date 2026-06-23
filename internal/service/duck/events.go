package duck

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/qtypes"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// GetEvents returns events for a subject in [from, to), newest first, over the
// deduped decoded events in the DuckLake catalog (lake.events).
func (q *Queries) GetEvents(ctx context.Context, subject string, from, to time.Time, filter *model.EventFilter) ([]*vss.Event, error) {
	events := []*vss.Event{}

	conds := []string{
		"subject = ?",
		subjectBucketPredicate("", subject), // partition pruning (CHD-1): subject_bucket is the leading partition key
		"timestamp >= " + tsMicroLiteral(from),
		"timestamp < " + tsMicroLiteral(to),
	}
	args := []any{subject}
	conds, args = appendEventFilterConds(conds, args, filter)

	// tags is a parquet list; serialize to JSON in SQL so it round-trips
	// through database/sql as a plain string. No LIMIT: GetEvents
	// returns every matching event and the GraphQL Events query exposes no
	// limit/cursor, so a silent cap would lose data. The real
	// blow-up risk — a fleet-wide scan — is removed by the subject_bucket prune
	// above; a single vehicle's events over the window stay bounded by its activity.
	stmt := "SELECT name, source, timestamp, duration_ns, metadata, CAST(to_json(tags) AS VARCHAR) FROM " + lakeEventsDeduped +
		" WHERE " + strings.Join(conds, " AND ") + " ORDER BY timestamp DESC"

	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb for events: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var event vss.Event
		var ts time.Time
		var tagsJSON string
		err := rows.Scan(&event.Data.Name, &event.Source, &ts, &event.Data.DurationNs, &event.Data.Metadata, &tagsJSON)
		if err != nil {
			return nil, fmt.Errorf("failed scanning duckdb event row: %w", err)
		}
		event.Data.Timestamp = ts.UTC()
		if err := json.Unmarshal([]byte(tagsJSON), &event.Tags); err != nil {
			return nil, fmt.Errorf("failed decoding event tags %q: %w", tagsJSON, err)
		}
		events = append(events, &event)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb event row error: %w", rows.Err())
	}
	return events, nil
}

// GetEventCounts returns event counts by name for a subject in [from, to).
// If eventNames is non-empty only those names are counted.
func (q *Queries) GetEventCounts(ctx context.Context, subject string, from, to time.Time, eventNames []string) ([]*qtypes.EventCount, error) {
	var result []*qtypes.EventCount

	conds := []string{
		"subject = ?",
		subjectBucketPredicate("", subject), // partition pruning (CHD-1)
		"timestamp >= " + tsMicroLiteral(from),
		"timestamp < " + tsMicroLiteral(to),
	}
	args := []any{subject}
	if len(eventNames) > 0 {
		conds = append(conds, "name IN ("+placeholders(len(eventNames))+")")
		for _, n := range eventNames {
			args = append(args, n)
		}
	}
	stmt := "SELECT name, CAST(count(*) AS BIGINT) AS count FROM " + lakeEventsDeduped +
		" WHERE " + strings.Join(conds, " AND ") + " GROUP BY name ORDER BY name"

	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb for event counts: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			return nil, fmt.Errorf("failed scanning duckdb event count row: %w", err)
		}
		result = append(result, &qtypes.EventCount{Name: name, Count: int(count)})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb event count row error: %w", rows.Err())
	}
	return result, nil
}

// GetEventCountsForRanges returns event counts by name per segment index for
// multiple [From, To) ranges in one query, classifying each row's range with a
// CASE chain.
func (q *Queries) GetEventCountsForRanges(ctx context.Context, subject string, ranges []qtypes.TimeRange, eventNames []string) ([]*qtypes.EventCountForRange, error) {
	if len(ranges) == 0 {
		return nil, nil
	}
	globalFrom, globalTo := ranges[0].From, ranges[0].To
	for _, r := range ranges[1:] {
		if r.From.Before(globalFrom) {
			globalFrom = r.From
		}
		if r.To.After(globalTo) {
			globalTo = r.To
		}
	}

	var result []*qtypes.EventCountForRange

	conds := []string{"subject = ?", subjectBucketPredicate("", subject)} // partition pruning (CHD-1)
	args := []any{subject}
	if len(eventNames) > 0 {
		conds = append(conds, "name IN ("+placeholders(len(eventNames))+")")
		for _, n := range eventNames {
			args = append(args, n)
		}
	}
	inner := "SELECT " + segmentIndexCaseSQL("timestamp", ranges) + " AS seg_idx, name FROM " + lakeEventsDeduped +
		" WHERE " + strings.Join(conds, " AND ")
	stmt := "SELECT CAST(seg_idx AS BIGINT), name, CAST(count(*) AS BIGINT) AS count FROM (" + inner + ")" +
		" WHERE seg_idx >= 0 GROUP BY seg_idx, name ORDER BY seg_idx, name"

	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb for event counts by range: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var segIdx, count int64
		var name string
		if err := rows.Scan(&segIdx, &name, &count); err != nil {
			return nil, fmt.Errorf("failed scanning duckdb event count by range row: %w", err)
		}
		result = append(result, &qtypes.EventCountForRange{SegIndex: int(segIdx), Name: name, Count: int(count)})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb event count by range row error: %w", rows.Err())
	}
	return result, nil
}

// GetEventSummaries returns per-event-name summaries (count, first/last seen)
// for a subject over all time. This scans every event date partition for the
// subject.
func (q *Queries) GetEventSummaries(ctx context.Context, subject string) ([]*qtypes.EventSummary, error) {
	var result []*qtypes.EventSummary

	stmt := "SELECT name, CAST(count(*) AS UBIGINT) AS count, min(timestamp) AS first_seen, max(timestamp) AS last_seen FROM " + lakeEventsDeduped +
		" WHERE subject = ? AND " + subjectBucketPredicate("", subject) + // partition pruning (CHD-1): all-time scan must still prune to one bucket
		" GROUP BY name ORDER BY name"

	rows, err := q.svc.db.QueryContext(ctx, stmt, subject)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb for event summaries: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var es qtypes.EventSummary
		if err := rows.Scan(&es.Name, &es.Count, &es.FirstSeen, &es.LastSeen); err != nil {
			return nil, fmt.Errorf("failed scanning duckdb event summary row: %w", err)
		}
		es.FirstSeen = es.FirstSeen.UTC()
		es.LastSeen = es.LastSeen.UTC()
		result = append(result, &es)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb event summary row error: %w", rows.Err())
	}
	return result, nil
}

// appendEventFilterConds builds the event filter predicates: name and source
// use the string value filter, tags use the string array filter
// (hasAny/hasAll -> list_has_any/list_has_all).
func appendEventFilterConds(conds []string, args []any, filter *model.EventFilter) ([]string, []any) {
	if filter == nil {
		return conds, args
	}
	if s, a := stringFilterSQL("name", filter.Name); s != "" {
		conds, args = append(conds, s), append(args, a...)
	}
	if s, a := stringFilterSQL("source", filter.Source); s != "" {
		conds, args = append(conds, s), append(args, a...)
	}
	if s, a := stringArrayFilterSQL("tags", filter.Tags); s != "" {
		conds, args = append(conds, s), append(args, a...)
	}
	return conds, args
}
