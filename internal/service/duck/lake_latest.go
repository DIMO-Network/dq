package duck

import (
	"context"
	"fmt"
	"strings"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// lakeSignals is the DuckLake decoded-signal table.
const lakeSignals = "lake.signals"

// lakeNonZeroLoc is the on-the-fly (0,0)-exclusion over base location
// columns, replacing the bucket layout's precomputed loc_*_nonzero columns.
const lakeNonZeroLoc = "(loc_lat != 0 OR loc_lon != 0)"

// getLatestSignalsLake computes latest values directly from lake.signals
// (no precomputed latest buckets): arg_max by timestamp for plain values,
// and arg_max over (0,0)-filtered base location columns, mirroring
// ch.Service.GetLatestSignals.
func (q *Queries) getLatestSignalsLake(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	if len(latestArgs.SignalNames) == 0 && len(latestArgs.LocationSignalNames) == 0 && !latestArgs.IncludeLastSeen {
		return nil, nil
	}
	srcCond, srcArgs := signalSourceCond("source", latestArgs.Filter)
	srcSQL := ""
	if srcCond != "" {
		srcSQL = " AND " + srcCond
	}

	var stmts []string
	var args []any
	if len(latestArgs.SignalNames) > 0 {
		names := mapKeys(latestArgs.SignalNames)
		stmts = append(stmts, fmt.Sprintf(
			`SELECT name, max(timestamp) AS ts,
				coalesce(arg_max(value_number, timestamp), 0) AS value_number,
				coalesce(arg_max(value_string, timestamp), '') AS value_string,
				0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading
			FROM %s WHERE subject = ? AND name IN (%s)%s GROUP BY name`,
			lakeSignals, placeholders(len(names)), srcSQL))
		args = append(args, subject)
		for _, n := range names {
			args = append(args, n)
		}
		args = append(args, srcArgs...)
	}
	if len(latestArgs.LocationSignalNames) > 0 {
		names := mapKeys(latestArgs.LocationSignalNames)
		stmts = append(stmts, fmt.Sprintf(
			`SELECT name,
				coalesce(max(timestamp) FILTER (WHERE %[1]s), %[2]s) AS ts,
				0.0 AS value_number, '' AS value_string,
				coalesce(arg_max(loc_lat, timestamp) FILTER (WHERE %[1]s), 0) AS loc_lat,
				coalesce(arg_max(loc_lon, timestamp) FILTER (WHERE %[1]s), 0) AS loc_lon,
				coalesce(arg_max(loc_hdop, timestamp) FILTER (WHERE %[1]s), 0) AS loc_hdop,
				coalesce(arg_max(loc_heading, timestamp) FILTER (WHERE %[1]s), 0) AS loc_heading
			FROM %[3]s WHERE subject = ? AND name IN (%[4]s)%[5]s GROUP BY name`,
			lakeNonZeroLoc, epochLiteral, lakeSignals, placeholders(len(names)), srcSQL))
		args = append(args, subject)
		for _, n := range names {
			args = append(args, n)
		}
		args = append(args, srcArgs...)
	}
	if latestArgs.IncludeLastSeen {
		stmt, a := lakeLastSeenQuery(subject, srcSQL, srcArgs)
		stmts = append(stmts, stmt)
		args = append(args, a...)
	}
	return q.querySignals(ctx, strings.Join(stmts, " UNION ALL ")+" ORDER BY name", args)
}

// getAllLatestSignalsLake is getLatestSignalsLake for every stored name.
func (q *Queries) getAllLatestSignalsLake(ctx context.Context, subject string, filter *model.SignalFilter) ([]*vss.Signal, error) {
	srcCond, srcArgs := signalSourceCond("source", filter)
	srcSQL := ""
	if srcCond != "" {
		srcSQL = " AND " + srcCond
	}
	mainStmt := fmt.Sprintf(
		`SELECT name, max(timestamp) AS ts,
			coalesce(arg_max(value_number, timestamp), 0) AS value_number,
			coalesce(arg_max(value_string, timestamp), '') AS value_string,
			coalesce(arg_max(loc_lat, timestamp) FILTER (WHERE %[1]s), 0) AS loc_lat,
			coalesce(arg_max(loc_lon, timestamp) FILTER (WHERE %[1]s), 0) AS loc_lon,
			coalesce(arg_max(loc_hdop, timestamp) FILTER (WHERE %[1]s), 0) AS loc_hdop,
			coalesce(arg_max(loc_heading, timestamp) FILTER (WHERE %[1]s), 0) AS loc_heading
		FROM %[2]s WHERE subject = ?%[3]s GROUP BY name`,
		lakeNonZeroLoc, lakeSignals, srcSQL)
	args := append([]any{subject}, srcArgs...)

	lastSeenStmt, lastSeenArgs := lakeLastSeenQuery(subject, srcSQL, srcArgs)
	stmt := mainStmt + " UNION ALL " + lastSeenStmt + " ORDER BY name"
	args = append(args, lastSeenArgs...)
	return q.querySignals(ctx, stmt, args)
}

// lakeLastSeenQuery computes the virtual lastSeen row (max timestamp over all
// of the subject's signals) directly, since lake.signals stores no
// precomputed lastSeen rows.
func lakeLastSeenQuery(subject, srcSQL string, srcArgs []any) (string, []any) {
	stmt := fmt.Sprintf(
		`SELECT %[1]s AS name, coalesce(max(timestamp), %[2]s) AS ts,
			0.0 AS value_number, '' AS value_string,
			0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading
		FROM %[3]s WHERE subject = ?%[4]s`,
		sqlString(model.LastSeenField), epochLiteral, lakeSignals, srcSQL)
	return stmt, append([]any{subject}, srcArgs...)
}

// getAvailableSignalsLake lists distinct signal names from lake.signals.
func (q *Queries) getAvailableSignalsLake(ctx context.Context, subject string, filter *model.SignalFilter) ([]string, error) {
	srcCond, srcArgs := signalSourceCond("source", filter)
	srcSQL := ""
	if srcCond != "" {
		srcSQL = " AND " + srcCond
	}
	stmt := fmt.Sprintf("SELECT DISTINCT name FROM %s WHERE subject = ?%s ORDER BY name", lakeSignals, srcSQL)
	args := append([]any{subject}, srcArgs...)
	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("querying lake available signals: %w", err)
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

// getSignalSummariesLake counts per-name signals and first/last seen directly
// from lake.signals (the bucket path sums precomputed summary rows).
func (q *Queries) getSignalSummariesLake(ctx context.Context, subject string, filter *model.SignalFilter) ([]*model.SignalDataSummary, error) {
	srcCond, srcArgs := signalSourceCond("source", filter)
	srcSQL := ""
	if srcCond != "" {
		srcSQL = " AND " + srcCond
	}
	stmt := fmt.Sprintf(
		`SELECT name, CAST(count(*) AS UBIGINT) AS count, min(timestamp), max(timestamp)
		FROM %s WHERE subject = ?%s GROUP BY name ORDER BY name`,
		lakeSignals, srcSQL)
	args := append([]any{subject}, srcArgs...)
	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("querying lake signal summaries: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	summaries := []*model.SignalDataSummary{}
	for rows.Next() {
		var s model.SignalDataSummary
		if err := rows.Scan(&s.Name, &s.NumberOfSignals, &s.FirstSeen, &s.LastSeen); err != nil {
			return nil, fmt.Errorf("scanning summary: %w", err)
		}
		s.FirstSeen = s.FirstSeen.UTC()
		s.LastSeen = s.LastSeen.UTC()
		summaries = append(summaries, &s)
	}
	return summaries, rows.Err()
}
