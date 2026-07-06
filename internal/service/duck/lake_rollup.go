package duck

import (
	"context"
	"fmt"
	"strings"

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

// getLatestSignalsRollup serves GetLatestSignals for the requested signal
// names from lake.signals_latest (SR-5): each stored row is already the
// per-name latest value at its overall-max timestamp, matching the GROUP BY in
// getLatestSignalsLake by construction. Location names serve from the same
// row's loc_* columns with loc_ts — the (0,0)-filtered latest-fix timestamp
// (H9) — so currentLocationCoordinates no longer full-scans history. The
// caller guarantees no source filter. IncludeLastSeen adds the virtual
// lastSeen row.
func (q *Queries) getLatestSignalsRollup(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	if len(latestArgs.SignalNames) == 0 && len(latestArgs.LocationSignalNames) == 0 && !latestArgs.IncludeLastSeen {
		return nil, nil
	}
	bucket := subjectBucketPredicate("", subject)
	var stmts []string
	var args []any
	if len(latestArgs.SignalNames) > 0 {
		names := mapKeys(latestArgs.SignalNames)
		stmts = append(stmts, fmt.Sprintf(
			`SELECT name, timestamp AS ts, value_number, value_string,
				0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading
			 FROM lake.signals_latest WHERE subject = ? AND %s AND name IN (%s)`,
			bucket, placeholders(len(names))))
		args = append(args, subject)
		for _, n := range names {
			args = append(args, n)
		}
	}
	if len(latestArgs.LocationSignalNames) > 0 {
		names := mapKeys(latestArgs.LocationSignalNames)
		// coalesce(loc_ts, epoch) covers rows written before the loc_ts
		// migration; a full recompute backfills them.
		stmts = append(stmts, fmt.Sprintf(
			`SELECT name, coalesce(loc_ts, %s) AS ts,
				0.0 AS value_number, '' AS value_string,
				loc_lat, loc_lon, loc_hdop, loc_heading
			 FROM lake.signals_latest WHERE subject = ? AND %s AND name IN (%s)`,
			epochLiteral, bucket, placeholders(len(names))))
		args = append(args, subject)
		for _, n := range names {
			args = append(args, n)
		}
	}
	if latestArgs.IncludeLastSeen {
		stmts = append(stmts, fmt.Sprintf(
			`SELECT %s AS name, coalesce(max(last_seen), %s) AS ts,
				0.0 AS value_number, '' AS value_string,
				0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading
			 FROM lake.signals_latest WHERE subject = ? AND %s`,
			sqlString(model.LastSeenField), epochLiteral, bucket))
		args = append(args, subject)
	}
	return q.querySignals(ctx, strings.Join(stmts, " UNION ALL ")+" ORDER BY name", args)
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
		s, err := scanSignalSummary(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning rollup summary: %w", err)
		}
		summaries = append(summaries, s)
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
