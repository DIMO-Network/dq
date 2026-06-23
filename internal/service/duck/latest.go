package duck

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// nonZeroLocCond mirrors ch's latestLocationCond: latest-location reads use
// the *_nonzero columns the materializer maintains, which track the latest
// location whose latitude or longitude is not 0 (excluding (0, 0) points).
const nonZeroLocCond = "(loc_lat_nonzero != 0 OR loc_lon_nonzero != 0)"

// GetLatestSignals returns the latest value for the requested signal names
// from the subject's latest bucket, mirroring ch.Service.GetLatestSignals:
//
//   - non-location names: max(timestamp), arg_max(value_*) over all sources
//   - location names: arg_max of the *_nonzero columns (excludes (0, 0))
//   - IncludeLastSeen adds the virtual lastSeen row (max timestamp over all
//     signals, materialized per (subject, source) under model.LastSeenField)
func (q *Queries) GetLatestSignals(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	if q.lake {
		// The rollup serves named latest in O(distinct-names), but it stores
		// only the per-name latest value + its overall-max timestamp — not the
		// separate nonzero-location timestamp the location-name path needs — so
		// location names and source-filtered queries fall back to the full
		// deduped scan (SR-5).
		if noSourceFilter(latestArgs.Filter) && len(latestArgs.LocationSignalNames) == 0 {
			observeLakePath(true)
			return q.getLatestSignalsRollup(ctx, subject, latestArgs)
		}
		observeLakePath(false)
		return q.getLatestSignalsLake(ctx, subject, latestArgs)
	}
	table, err := q.tableExpr(ctx, q.latestPaths(subject))
	if err != nil {
		return nil, err
	}

	hasWork := len(latestArgs.SignalNames) > 0 || len(latestArgs.LocationSignalNames) > 0 || latestArgs.IncludeLastSeen
	if !hasWork {
		return nil, nil
	}
	if table == "" { // latest bucket not written yet: no data for this subject
		return []*vss.Signal{}, nil
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
			table, placeholders(len(names)), srcSQL))
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
				coalesce(max(loc_nonzero_ts) FILTER (WHERE %[1]s), %[2]s) AS ts,
				0.0 AS value_number, '' AS value_string,
				coalesce(arg_max(loc_lat_nonzero, loc_nonzero_ts) FILTER (WHERE %[1]s), 0) AS loc_lat,
				coalesce(arg_max(loc_lon_nonzero, loc_nonzero_ts) FILTER (WHERE %[1]s), 0) AS loc_lon,
				coalesce(arg_max(loc_hdop_nonzero, loc_nonzero_ts) FILTER (WHERE %[1]s), 0) AS loc_hdop,
				coalesce(arg_max(loc_heading_nonzero, loc_nonzero_ts) FILTER (WHERE %[1]s), 0) AS loc_heading
			FROM %[3]s WHERE subject = ? AND name IN (%[4]s)%[5]s GROUP BY name`,
			nonZeroLocCond, epochLiteral, table, placeholders(len(names)), srcSQL))
		args = append(args, subject)
		for _, n := range names {
			args = append(args, n)
		}
		args = append(args, srcArgs...)
	}
	if latestArgs.IncludeLastSeen {
		stmt, a := lastSeenQuery(table, subject, srcSQL, srcArgs)
		stmts = append(stmts, stmt)
		args = append(args, a...)
	}

	// ORDER BY makes the result deterministic so a shadow-compare
	// diffs values, not engine-specific GROUP BY ordering.
	return q.querySignals(ctx, strings.Join(stmts, " UNION ALL ")+" ORDER BY name", args)
}

// GetAllLatestSignals returns the latest value for every signal name stored
// for the subject, plus the virtual lastSeen row, mirroring
// ch.Service.GetAllLatestSignals: the timestamp is the unconditional
// max(timestamp) while the location value comes from the nonzero columns.
func (q *Queries) GetAllLatestSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]*vss.Signal, error) {
	if q.lake {
		if noSourceFilter(filter) {
			observeLakePath(true)
			return q.getAllLatestSignalsRollup(ctx, subject) // O(distinct-names) rollup (CHD-3)
		}
		observeLakePath(false)
		return q.getAllLatestSignalsLake(ctx, subject, filter)
	}
	table, err := q.tableExpr(ctx, q.latestPaths(subject))
	if err != nil {
		return nil, err
	}
	if table == "" {
		return []*vss.Signal{}, nil
	}

	srcCond, srcArgs := signalSourceCond("source", filter)
	srcSQL := ""
	if srcCond != "" {
		srcSQL = " AND " + srcCond
	}

	mainStmt := fmt.Sprintf(
		`SELECT name, max(timestamp) AS ts,
			coalesce(arg_max(value_number, timestamp), 0) AS value_number,
			coalesce(arg_max(value_string, timestamp), '') AS value_string,
			coalesce(arg_max(loc_lat_nonzero, loc_nonzero_ts) FILTER (WHERE %[1]s), 0) AS loc_lat,
			coalesce(arg_max(loc_lon_nonzero, loc_nonzero_ts) FILTER (WHERE %[1]s), 0) AS loc_lon,
			coalesce(arg_max(loc_hdop_nonzero, loc_nonzero_ts) FILTER (WHERE %[1]s), 0) AS loc_hdop,
			coalesce(arg_max(loc_heading_nonzero, loc_nonzero_ts) FILTER (WHERE %[1]s), 0) AS loc_heading
		FROM %[2]s WHERE subject = ? AND name != %[3]s%[4]s GROUP BY name`,
		nonZeroLocCond, table, sqlString(model.LastSeenField), srcSQL)
	args := append([]any{subject}, srcArgs...)

	lastSeenStmt, lastSeenArgs := lastSeenQuery(table, subject, srcSQL, srcArgs)
	stmt := mainStmt + " UNION ALL " + lastSeenStmt + " ORDER BY name"
	args = append(args, lastSeenArgs...)

	return q.querySignals(ctx, stmt, args)
}

// lastSeenQuery reads the virtual per-(subject, source) lastSeen rows the
// materializer writes under model.LastSeenField, mirroring ch's
// getLastSeenQuery (max timestamp of any signal, no name filter). With no
// rows the timestamp is the Unix epoch, which the repository treats as
// "never seen".
func lastSeenQuery(table, subject, srcSQL string, srcArgs []any) (string, []any) {
	stmt := fmt.Sprintf(
		`SELECT %[1]s AS name, coalesce(max(timestamp), %[2]s) AS ts,
			0.0 AS value_number, '' AS value_string,
			0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading
		FROM %[3]s WHERE subject = ? AND name = %[1]s%[4]s`,
		sqlString(model.LastSeenField), epochLiteral, table, srcSQL)
	return stmt, append([]any{subject}, srcArgs...)
}

// GetAvailableSignals returns the distinct signal names stored for a subject,
// from the summary bucket, sorted ascending. Returns nil when none.
func (q *Queries) GetAvailableSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]string, error) {
	if q.lake {
		if noSourceFilter(filter) {
			observeLakePath(true)
			return q.getAvailableSignalsRollup(ctx, subject) // rollup (CHD-3)
		}
		observeLakePath(false)
		return q.getAvailableSignalsLake(ctx, subject, filter)
	}
	table, err := q.tableExpr(ctx, q.summaryPaths(subject))
	if err != nil {
		return nil, err
	}
	if table == "" {
		return nil, nil
	}

	srcCond, srcArgs := signalSourceCond("source", filter)
	srcSQL := ""
	if srcCond != "" {
		srcSQL = " AND " + srcCond
	}
	stmt := fmt.Sprintf(
		"SELECT name FROM %s WHERE subject = ? AND name != %s%s GROUP BY name ORDER BY name",
		table, sqlString(model.LastSeenField), srcSQL)
	args := append([]any{subject}, srcArgs...)

	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb for available signals: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var signals []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed scanning duckdb row: %w", err)
		}
		signals = append(signals, name)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb row error: %w", rows.Err())
	}
	return signals, nil
}

// GetSignalSummaries returns per-name signal counts and first/last seen
// timestamps for a subject from the summary bucket, aggregated across
// sources, mirroring ch.Service.GetSignalSummaries.
func (q *Queries) GetSignalSummaries(ctx context.Context, subject string, filter *model.SignalFilter) ([]*model.SignalDataSummary, error) {
	if q.lake {
		if noSourceFilter(filter) {
			observeLakePath(true)
			return q.getSignalSummariesRollup(ctx, subject) // rollup (CHD-3)
		}
		observeLakePath(false)
		return q.getSignalSummariesLake(ctx, subject, filter)
	}
	table, err := q.tableExpr(ctx, q.summaryPaths(subject))
	if err != nil {
		return nil, err
	}
	summaries := []*model.SignalDataSummary{}
	if table == "" {
		return summaries, nil
	}

	srcCond, srcArgs := signalSourceCond("source", filter)
	srcSQL := ""
	if srcCond != "" {
		srcSQL = " AND " + srcCond
	}
	stmt := fmt.Sprintf(
		`SELECT name, CAST(sum(count) AS UBIGINT) AS count, min(first_seen), max(last_seen)
		FROM %s WHERE subject = ? AND name != %s%s GROUP BY name ORDER BY name`,
		table, sqlString(model.LastSeenField), srcSQL)
	args := append([]any{subject}, srcArgs...)

	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb for signal summaries: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		s, err := scanSignalSummary(rows)
		if err != nil {
			return nil, fmt.Errorf("failed scanning duckdb row: %w", err)
		}
		summaries = append(summaries, s)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb row error: %w", rows.Err())
	}
	return summaries, nil
}

// scanSignalSummary scans one summary row (name, count, first_seen, last_seen)
// and normalizes both timestamps to UTC. The column order is shared by every
// signal-summary query (bucket, lake, rollup), so it lives in one place.
func scanSignalSummary(rows rowScanner) (*model.SignalDataSummary, error) {
	var s model.SignalDataSummary
	if err := rows.Scan(&s.Name, &s.NumberOfSignals, &s.FirstSeen, &s.LastSeen); err != nil {
		return nil, err
	}
	s.FirstSeen = s.FirstSeen.UTC()
	s.LastSeen = s.LastSeen.UTC()
	return &s, nil
}

// querySignals runs a signal-shaped query (name, ts, value_number,
// value_string, loc_lat, loc_lon, loc_hdop, loc_heading) and scans rows into
// vss.Signal values like ch's getSignals.
func (q *Queries) querySignals(ctx context.Context, stmt string, args []any) ([]*vss.Signal, error) {
	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	signals := []*vss.Signal{}
	for rows.Next() {
		var signal vss.Signal
		var ts time.Time
		var loc vss.Location
		err := rows.Scan(&signal.Data.Name, &ts, &signal.Data.ValueNumber, &signal.Data.ValueString,
			&loc.Latitude, &loc.Longitude, &loc.HDOP, &loc.Heading)
		if err != nil {
			return nil, fmt.Errorf("failed scanning duckdb row: %w", err)
		}
		signal.Data.Timestamp = ts.UTC()
		signal.Data.ValueLocation = loc
		signals = append(signals, &signal)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb row error: %w", rows.Err())
	}
	return signals, nil
}

func mapKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
