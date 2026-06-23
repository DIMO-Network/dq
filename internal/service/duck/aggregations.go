package duck

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/qtypes"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// Result row types (qtypes.AggSignal, qtypes.AggSignalForRange, qtypes.FieldType, ...)
// are reused from the ClickHouse service so the repository layer can swap
// implementations without translation.

// signalSrcColumns is the projection of the inner (pre-aggregation) select
// shared by GetAggregatedSignals and GetAggregatedSignalsForRanges. rnd is a
// uniform per-row random used for RAND aggregations (see aggExpr comments).
const signalSrcColumns = `agg_table.signal_type AS signal_type, agg_table.signal_index AS signal_index,
	s.timestamp AS timestamp, s.value_number AS value_number, s.value_string AS value_string,
	s.loc_lat AS loc_lat, s.loc_lon AS loc_lon, s.loc_hdop AS loc_hdop, s.loc_heading AS loc_heading,
	random() AS rnd`

// GetAggregatedSignals ports ch.Service.GetAggregatedSignals to DuckDB over
// the decoded signal parquet files. Each requested aggregation is identified
// by (signal_type, signal_index) through a VALUES join (the same trick the
// ClickHouse query used), and per-(type, index) CASE expressions pick the
// right aggregate per output column.
//
// Time bucketing decision: ClickHouse's
// toStartOfInterval(ts, toIntervalMicrosecond(n), origin) is implemented with
// pure epoch math — origin + ((epoch_us(ts) - epoch_us(origin)) // n) * n —
// instead of DuckDB's time_bucket(). Both were verified to agree for
// microsecond intervals (time_bucket(to_microseconds(n), ts, origin)), but
// the epoch math has unconditional floor semantics for any microsecond
// interval and no interval-type edge cases, so it is the implementation.
func (q *Queries) GetAggregatedSignals(ctx context.Context, subject string, aggArgs *model.AggregatedSignalArgs) ([]*qtypes.AggSignal, error) {
	if aggArgs == nil || len(aggArgs.FloatArgs)+len(aggArgs.StringArgs)+len(aggArgs.LocationArgs) == 0 {
		return []*qtypes.AggSignal{}, nil
	}
	if aggArgs.Interval <= 0 {
		return nil, errors.New("aggregation interval must be positive")
	}

	table, err := q.signalTable(ctx, aggArgs.FromTS, aggArgs.ToTS)
	if err != nil {
		return nil, err
	}
	signals := []*qtypes.AggSignal{}
	if table == "" {
		return signals, nil
	}

	conds := []string{
		"s.subject = ?",
		"s.timestamp >= " + tsMicroLiteral(aggArgs.FromTS),
		"s.timestamp < " + tsMicroLiteral(aggArgs.ToTS),
	}
	if q.lake {
		conds = append(conds, subjectBucketPredicate("s.", subject)) // partition pruning (CHD-1)
	}
	args := []any{subject}
	if srcCond, srcArgs := signalSourceCond("s.source", aggArgs.Filter); srcCond != "" {
		conds = append(conds, srcCond)
		args = append(args, srcArgs...)
	}
	perSignal, perSignalArgs := perSignalFilterSQL(aggArgs)
	conds = append(conds, perSignal)
	args = append(args, perSignalArgs...)

	inner := "SELECT " + signalSrcColumns +
		" FROM " + table + " AS s JOIN " + aggValuesTable(aggArgs.FloatArgs, aggArgs.StringArgs, aggArgs.LocationArgs) +
		" ON s.name = agg_table.name WHERE " + strings.Join(conds, " AND ")

	originUs := aggArgs.FromTS.UnixMicro()
	bucketExpr := fmt.Sprintf("make_timestamp(((epoch_us(timestamp) - %d) // %d) * %d + %d)",
		originUs, aggArgs.Interval, aggArgs.Interval, originUs)

	stmt := "SELECT CAST(signal_type AS UTINYINT) AS out_type, CAST(signal_index AS USMALLINT) AS out_index, " +
		bucketExpr + " AS group_timestamp, " +
		floatCaseSQL(aggArgs.FloatArgs) + ", " +
		stringCaseSQL(aggArgs.StringArgs) + ", " +
		locationCaseSQL("loc_lat", "agg_lat", aggArgs.LocationArgs) + ", " +
		locationCaseSQL("loc_lon", "agg_lon", aggArgs.LocationArgs) + ", " +
		locationCaseSQL("loc_hdop", "agg_hdop", aggArgs.LocationArgs) + ", " +
		locationCaseSQL("loc_heading", "agg_heading", aggArgs.LocationArgs) +
		" FROM (" + inner + ")" +
		" GROUP BY group_timestamp, signal_type, signal_index" +
		" ORDER BY group_timestamp ASC, signal_type ASC, signal_index ASC"

	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb for aggregated signals: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var signal qtypes.AggSignal
		var ts time.Time
		var loc vss.Location
		err := rows.Scan(&signal.SignalType, &signal.SignalIndex, &ts, &signal.ValueNumber, &signal.ValueString,
			&loc.Latitude, &loc.Longitude, &loc.HDOP, &loc.Heading)
		if err != nil {
			return nil, fmt.Errorf("failed scanning duckdb agg row: %w", err)
		}
		signal.Timestamp = ts.UTC()
		signal.ValueLocation = loc
		signals = append(signals, &signal)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb agg row error: %w", rows.Err())
	}
	return signals, nil
}

// GetAggregatedSignalsForRanges ports ch.Service.GetAggregatedSignalsForRanges:
// the same aggregations computed for multiple [From, To) segments in one
// query. The ClickHouse multiIf segment classifier becomes a CASE chain.
// Mirroring ch, only FloatArgs and LocationArgs are supported, no per-signal
// value filters are applied, and (0, 0) locations are NOT excluded.
func (q *Queries) GetAggregatedSignalsForRanges(ctx context.Context, subject string, ranges []qtypes.TimeRange, globalFrom, globalTo time.Time, floatArgs []model.FloatSignalArgs, locationArgs []model.LocationSignalArgs) ([]*qtypes.AggSignalForRange, error) {
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(floatArgs) == 0 && len(locationArgs) == 0 {
		return []*qtypes.AggSignalForRange{}, nil
	}

	table, err := q.signalTable(ctx, globalFrom, globalTo)
	if err != nil {
		return nil, err
	}
	result := []*qtypes.AggSignalForRange{}
	if table == "" {
		return result, nil
	}

	bucketSQL := ""
	if q.lake {
		bucketSQL = " AND " + subjectBucketPredicate("s.", subject) // partition pruning (CHD-1)
	}
	inner := "SELECT " + segmentIndexCaseSQL("s.timestamp", ranges) + " AS seg_idx, " + signalSrcColumns +
		" FROM " + table + " AS s JOIN " + aggValuesTable(floatArgs, nil, locationArgs) +
		" ON s.name = agg_table.name" +
		" WHERE s.subject = ?" + bucketSQL +
		" AND s.timestamp >= " + tsMicroLiteral(globalFrom) +
		" AND s.timestamp < " + tsMicroLiteral(globalTo)

	stmt := "SELECT CAST(seg_idx AS BIGINT) AS seg_idx, CAST(signal_type AS UTINYINT) AS out_type, CAST(signal_index AS USMALLINT) AS out_index, " +
		floatCaseSQL(floatArgs) + ", " +
		stringCaseSQL(nil) + ", " +
		locationCaseSQL("loc_lat", "agg_lat", locationArgs) + ", " +
		locationCaseSQL("loc_lon", "agg_lon", locationArgs) + ", " +
		locationCaseSQL("loc_hdop", "agg_hdop", locationArgs) + ", " +
		locationCaseSQL("loc_heading", "agg_heading", locationArgs) +
		" FROM (" + inner + ") WHERE seg_idx >= 0" +
		" GROUP BY seg_idx, signal_type, signal_index" +
		" ORDER BY seg_idx ASC, signal_type ASC, signal_index ASC"

	rows, err := q.svc.db.QueryContext(ctx, stmt, subject)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb for batch agg: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var segIdx int64
		var row qtypes.AggSignalForRange
		var loc vss.Location
		err := rows.Scan(&segIdx, &row.SignalType, &row.SignalIndex, &row.ValueNumber, &row.ValueString,
			&loc.Latitude, &loc.Longitude, &loc.HDOP, &loc.Heading)
		if err != nil {
			return nil, fmt.Errorf("failed scanning duckdb batch agg row: %w", err)
		}
		row.SegIndex = int(segIdx)
		row.ValueLocation = loc
		result = append(result, &row)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb batch agg row error: %w", rows.Err())
	}
	return result, nil
}

// locationFilterSQL renders a WGS-84 spatial predicate over latCol/lonCol for a
// location signal filter, or "" when there is none. Pure SQL — no spatial
// extension dependency: inCircle is a haversine great-circle distance (km) and
// inPolygon is an even-odd ray cast unrolled over the request's vertices (fixed
// at query-build time). Replaces the old "not supported" rejection and mirrors
// ClickHouse's geoDistance / pointInPolygon (SR review #10). InCircle takes
// precedence when both are set.
func locationFilterSQL(latCol, lonCol string, f *model.SignalLocationFilter) string {
	if f == nil {
		return ""
	}
	if f.InCircle != nil && f.InCircle.Center != nil {
		return haversineWithinSQL(latCol, lonCol, f.InCircle.Center.Latitude, f.InCircle.Center.Longitude, f.InCircle.Radius)
	}
	if len(f.InPolygon) >= 3 {
		return pointInPolygonSQL(latCol, lonCol, f.InPolygon)
	}
	return ""
}

// haversineWithinSQL constrains the great-circle distance (km, mean Earth radius
// 6371 — matching ClickHouse geoDistance's sphere) from (latCol,lonCol) to the
// center to be within radiusKm.
func haversineWithinSQL(latCol, lonCol string, clat, clon, radiusKm float64) string {
	return fmt.Sprintf(
		"(2 * 6371 * asin(sqrt("+
			"pow(sin(radians((%[1]s - %[3]s) / 2)), 2) + "+
			"cos(radians(%[3]s)) * cos(radians(%[1]s)) * "+
			"pow(sin(radians((%[2]s - %[4]s) / 2)), 2))) <= %[5]s)",
		latCol, lonCol, floatLit(clat), floatLit(clon), floatLit(radiusKm))
}

// pointInPolygonSQL renders an even-odd ray cast: the point (lonCol=px,
// latCol=py) is inside when an odd number of polygon edges straddle its latitude
// to the west. The ring closes by wrapping the last vertex to the first. A
// horizontal edge (yi == yj) makes the straddle guard false and the slope a NaN
// whose comparison is also false, so it contributes zero — no divide-by-zero.
// Vertices are inlined as DOUBLE literals (fixed by the request), so there is no
// bound parameter and no injection surface.
func pointInPolygonSQL(latCol, lonCol string, poly []*model.FilterLocation) string {
	terms := make([]string, 0, len(poly))
	for i := range poly {
		j := (i + 1) % len(poly)
		yi, xi := floatLit(poly[i].Latitude), floatLit(poly[i].Longitude)
		yj, xj := floatLit(poly[j].Latitude), floatLit(poly[j].Longitude)
		terms = append(terms, fmt.Sprintf(
			"CASE WHEN ((%[3]s > %[1]s) <> (%[4]s > %[1]s)) AND "+
				"(%[2]s < (%[6]s - %[5]s) * (%[1]s - %[3]s) / (%[4]s - %[3]s) + %[5]s) "+
				"THEN 1 ELSE 0 END",
			latCol, lonCol, yi, yj, xi, xj))
	}
	return "((" + strings.Join(terms, " + ") + ") % 2 = 1)"
}

// floatLit formats f as a parenthesized DuckDB DOUBLE literal (parens keep a
// negative coordinate from forming "- -122.4" after a subtraction operator).
func floatLit(f float64) string {
	return "(" + strconv.FormatFloat(f, 'g', -1, 64) + ")"
}

// aggValuesTable renders the (signal_type, signal_index, name) inline VALUES
// table identifying each requested aggregation, the DuckDB equivalent of
// ClickHouse's VALUES('...', (1, 0, 'speed'), ...) join.
func aggValuesTable(floatArgs []model.FloatSignalArgs, stringArgs []model.StringSignalArgs, locationArgs []model.LocationSignalArgs) string {
	entries := make([]string, 0, len(floatArgs)+len(stringArgs)+len(locationArgs))
	for i, a := range floatArgs {
		entries = append(entries, fmt.Sprintf("(%d, %d, %s)", qtypes.FloatType, i, sqlString(a.Name)))
	}
	for i, a := range stringArgs {
		entries = append(entries, fmt.Sprintf("(%d, %d, %s)", qtypes.StringType, i, sqlString(a.Name)))
	}
	for i, a := range locationArgs {
		entries = append(entries, fmt.Sprintf("(%d, %d, %s)", qtypes.LocType, i, sqlString(a.Name)))
	}
	return "(VALUES " + strings.Join(entries, ", ") + ") AS agg_table(signal_type, signal_index, name)"
}

// perSignalFilterSQL ports ch getAggQuery's perSignalFilters block:
//
//	(signal_type = 1 AND ((signal_index = 0 AND <float conds>) OR ...))
//	OR signal_type = 2
//	OR (signal_type = 3 AND (lat != 0 OR lon != 0) AND ((signal_index = 0) OR ...))
//
// Location branches carry the index condition, the (0, 0) exclusion, and any
// inPolygon/inCircle predicate (locationFilterSQL).
func perSignalFilterSQL(aggArgs *model.AggregatedSignalArgs) (string, []any) {
	var branches []string
	var args []any

	if len(aggArgs.FloatArgs) != 0 {
		parts := make([]string, 0, len(aggArgs.FloatArgs))
		for i, agg := range aggArgs.FloatArgs {
			cond := fmt.Sprintf("agg_table.signal_index = %d", i)
			if fs, fa := floatFilterSQL("s.value_number", agg.Filter); fs != "" {
				cond += " AND " + fs
				args = append(args, fa...)
			}
			parts = append(parts, "("+cond+")")
		}
		branches = append(branches, fmt.Sprintf("(agg_table.signal_type = %d AND (%s))", qtypes.FloatType, strings.Join(parts, " OR ")))
	}
	if len(aggArgs.StringArgs) != 0 {
		branches = append(branches, fmt.Sprintf("agg_table.signal_type = %d", qtypes.StringType))
	}
	if len(aggArgs.LocationArgs) != 0 {
		parts := make([]string, 0, len(aggArgs.LocationArgs))
		for i, la := range aggArgs.LocationArgs {
			cond := fmt.Sprintf("agg_table.signal_index = %d", i)
			if lf := locationFilterSQL("s.loc_lat", "s.loc_lon", la.Filter); lf != "" {
				cond += " AND " + lf
			}
			parts = append(parts, "("+cond+")")
		}
		branches = append(branches, fmt.Sprintf("(agg_table.signal_type = %d AND (s.loc_lat != 0 OR s.loc_lon != 0) AND (%s))",
			qtypes.LocType, strings.Join(parts, " OR ")))
	}
	return "(" + strings.Join(branches, " OR ") + ")", args
}

// segmentIndexCaseSQL ports ch's buildSegmentIndexMultiIf: rows falling in
// ranges[i] get segment index i, everything else -1. Range timestamps are
// inlined with microsecond precision.
func segmentIndexCaseSQL(tsCol string, ranges []qtypes.TimeRange) string {
	parts := make([]string, 0, len(ranges))
	for i, r := range ranges {
		parts = append(parts, fmt.Sprintf("WHEN %s >= %s AND %s < %s THEN %d",
			tsCol, tsMicroLiteral(r.From), tsCol, tsMicroLiteral(r.To), i))
	}
	return "CASE " + strings.Join(parts, " ") + " ELSE -1 END"
}

// floatCaseSQL renders the agg_number output column: a CASE choosing the
// requested float aggregate per (signal_type, signal_index). NULL branches
// (rows of other types) are coalesced to 0 like the ClickHouse scan did.
func floatCaseSQL(floatArgs []model.FloatSignalArgs) string {
	if len(floatArgs) == 0 {
		return "0.0 AS agg_number"
	}
	parts := make([]string, 0, len(floatArgs))
	for i, agg := range floatArgs {
		parts = append(parts, fmt.Sprintf("WHEN signal_type = %d AND signal_index = %d THEN %s",
			qtypes.FloatType, i, floatAggExpr(agg.Agg)))
	}
	return "coalesce(CASE " + strings.Join(parts, " ") + " ELSE NULL END, 0) AS agg_number"
}

// stringCaseSQL renders the agg_string output column.
func stringCaseSQL(stringArgs []model.StringSignalArgs) string {
	if len(stringArgs) == 0 {
		return "'' AS agg_string"
	}
	parts := make([]string, 0, len(stringArgs))
	for i, agg := range stringArgs {
		parts = append(parts, fmt.Sprintf("WHEN signal_type = %d AND signal_index = %d THEN %s",
			qtypes.StringType, i, stringAggExpr(agg.Agg)))
	}
	return "coalesce(CASE " + strings.Join(parts, " ") + " ELSE NULL END, '') AS agg_string"
}

// locationCaseSQL renders one component (lat/lon/hdop/heading) of the
// location aggregate output. ClickHouse aggregated the location tuple
// atomically; here each component is aggregated with the same ordering key
// (timestamp, or the shared per-row rnd for RAND), so components still come
// from the same row except for AVG, which averages component-wise like CH.
func locationCaseSQL(component, outCol string, locationArgs []model.LocationSignalArgs) string {
	if len(locationArgs) == 0 {
		return "0.0 AS " + outCol
	}
	parts := make([]string, 0, len(locationArgs))
	for i, agg := range locationArgs {
		parts = append(parts, fmt.Sprintf("WHEN signal_type = %d AND signal_index = %d THEN %s",
			qtypes.LocType, i, locationAggExpr(component, agg.Agg)))
	}
	return "coalesce(CASE " + strings.Join(parts, " ") + " ELSE NULL END, 0) AS " + outCol
}

// floatAggExpr maps a float aggregation to its DuckDB expression.
//
// DELIBERATE DIVERGENCE FROM CLICKHOUSE (exactness): MED is DuckDB's exact
// median(value_number); ClickHouse's median() is an approximate t-digest estimate.
// So a window's median is now EXACT, not estimated — a correctness improvement, not
// a parity bug. Do NOT "restore parity" by switching to an approximate quantile;
// pinned by TestExactAggExpr_DivergesFromClickHouse. (FIRST/LAST use arg_min/
// arg_max over the (subject,name,timestamp)-deduped scan, so they are tie-free and
// deterministic, where CH's FINAL+argMax is not.)
//
// RAND decision: ClickHouse used groupArraySample(1, <now-ms seed>)[1] — a
// uniform random pick, reseeded every query. DuckDB has no seeded sampling
// aggregate, so RAND is arg_max(value, rnd) where rnd is one random() draw
// per input row: also a uniform random pick, also different per query.
func floatAggExpr(aggType model.FloatAggregation) string {
	switch aggType {
	case model.FloatAggregationAvg:
		return "avg(value_number)"
	case model.FloatAggregationRand:
		return "arg_max(value_number, rnd)"
	case model.FloatAggregationMin:
		return "min(value_number)"
	case model.FloatAggregationMax:
		return "max(value_number)"
	case model.FloatAggregationMed:
		return "median(value_number)"
	case model.FloatAggregationFirst:
		return "arg_min(value_number, timestamp)"
	case model.FloatAggregationLast:
		return "arg_max(value_number, timestamp)"
	default:
		return "avg(value_number)"
	}
}

// stringAggExpr maps a string aggregation to its DuckDB expression:
// topK(1) -> mode(), groupUniqArray+arrayStringConcat -> string_agg(DISTINCT),
// argMin/argMax -> arg_min/arg_max, RAND as in floatAggExpr.
//
// DELIBERATE DIVERGENCE (exactness): UNIQUE is an EXACT distinct set (string_agg
// DISTINCT) and TOP is an EXACT mode(); ClickHouse's groupUniqArray/topK are
// approximate. This is a correctness improvement — the same intentional
// exact-over-approximate divergence as median (see floatAggExpr).
func stringAggExpr(aggType model.StringAggregation) string {
	switch aggType {
	case model.StringAggregationRand:
		return "arg_max(value_string, rnd)"
	case model.StringAggregationUnique:
		return "string_agg(DISTINCT value_string, ',' ORDER BY value_string)"
	case model.StringAggregationTop:
		return "mode(value_string)"
	case model.StringAggregationFirst:
		return "arg_min(value_string, timestamp)"
	case model.StringAggregationLast:
		return "arg_max(value_string, timestamp)"
	default:
		return "mode(value_string)"
	}
}

// locationAggExpr maps a location aggregation to the DuckDB expression for
// one location component column.
func locationAggExpr(component string, aggType model.LocationAggregation) string {
	switch aggType {
	case model.LocationAggregationAvg:
		return "avg(" + component + ")"
	case model.LocationAggregationRand:
		return "arg_max(" + component + ", rnd)"
	case model.LocationAggregationFirst:
		return "arg_min(" + component + ", timestamp)"
	case model.LocationAggregationLast:
		return "arg_max(" + component + ", timestamp)"
	default:
		return "arg_min(" + component + ", timestamp)"
	}
}
