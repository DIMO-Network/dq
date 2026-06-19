package duck

import (
	"context"
	"fmt"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// The lake.signals_latest rollup (maintained per batch by the materializer,
// CHD-3) is a materialized view of getAllLatestSignalsLake with sources folded.
// Reading it makes latest/summary/availableSignals O(distinct-names) instead of
// a full-history GROUP BY. It serves the no-source-filter case only; a source
// filter falls back to the full scan (still subject-pruned, CHD-1).

// noSourceFilter reports whether a signal filter is absent or has no source
// constraint — the case the source-folded rollup can serve.
func noSourceFilter(f *model.SignalFilter) bool {
	return f == nil || f.Source == nil
}

// getAllLatestSignalsRollup serves GetAllLatestSignals from lake.signals_latest:
// each stored row is already the per-name latest, plus the virtual lastSeen row
// (max last_seen across names), matching getAllLatestSignalsLake.
func (q *Queries) getAllLatestSignalsRollup(ctx context.Context, subject string) ([]*vss.Signal, error) {
	bucket := subjectBucketPredicate("", subject)
	mainStmt := fmt.Sprintf(
		`SELECT name, timestamp AS ts, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading
		 FROM lake.signals_latest WHERE subject = ? AND %s`, bucket)
	lastSeenStmt := fmt.Sprintf(
		`SELECT %s AS name, coalesce(max(last_seen), %s) AS ts,
			0.0 AS value_number, '' AS value_string,
			0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading
		 FROM lake.signals_latest WHERE subject = ? AND %s`,
		sqlString(model.LastSeenField), epochLiteral, bucket)
	stmt := mainStmt + " UNION ALL " + lastSeenStmt + " ORDER BY name"
	return q.querySignals(ctx, stmt, []any{subject, subject})
}

// getSignalSummariesRollup serves GetSignalSummaries from lake.signals_latest.
func (q *Queries) getSignalSummariesRollup(ctx context.Context, subject string) ([]*model.SignalDataSummary, error) {
	bucket := subjectBucketPredicate("", subject)
	stmt := fmt.Sprintf(
		`SELECT name, CAST(count AS UBIGINT) AS count, first_seen, last_seen
		 FROM lake.signals_latest WHERE subject = ? AND %s ORDER BY name`, bucket)
	rows, err := q.svc.db.QueryContext(ctx, stmt, subject)
	if err != nil {
		return nil, fmt.Errorf("querying rollup signal summaries: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	summaries := []*model.SignalDataSummary{}
	for rows.Next() {
		var s model.SignalDataSummary
		if err := rows.Scan(&s.Name, &s.NumberOfSignals, &s.FirstSeen, &s.LastSeen); err != nil {
			return nil, fmt.Errorf("scanning rollup summary: %w", err)
		}
		s.FirstSeen = s.FirstSeen.UTC()
		s.LastSeen = s.LastSeen.UTC()
		summaries = append(summaries, &s)
	}
	return summaries, rows.Err()
}

// getAvailableSignalsRollup serves GetAvailableSignals from lake.signals_latest.
func (q *Queries) getAvailableSignalsRollup(ctx context.Context, subject string) ([]string, error) {
	bucket := subjectBucketPredicate("", subject)
	stmt := fmt.Sprintf("SELECT DISTINCT name FROM lake.signals_latest WHERE subject = ? AND %s ORDER BY name", bucket)
	rows, err := q.svc.db.QueryContext(ctx, stmt, subject)
	if err != nil {
		return nil, fmt.Errorf("querying rollup available signals: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scanning name: %w", err)
		}
		names = append(names, n)
	}
	return names, rows.Err()
}
