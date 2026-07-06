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
// live in qtypes so the repository layer consumes them without translation.

// signalSrcColumns is the projection of the inner (pre-aggregation) select
// shared by GetAggregatedSignals and GetAggregatedSignalsForRanges. rnd is a
// uniform per-row random used for RAND aggregations (see aggExpr comments).
const signalSrcColumns = `agg_table.signal_type AS signal_type, agg_table.signal_index AS signal_index,
	s.timestamp AS timestamp, s.value_number AS value_number, s.value_string AS value_string,
	s.loc_lat AS loc_lat, s.loc_lon AS loc_lon, s.loc_hdop AS loc_hdop, s.loc_heading AS loc_heading,
	random() AS rnd`

// GetAggregatedSignals computes signal aggregations in DuckDB over
// the decoded signal parquet files. Each requested aggregation is identified
// by (signal_type, signal_index) through a VALUES join, and per-(type, index)
// CASE expressions pick the right aggregate per output column.
//
// Time bucketing decision: epoch-aligned bucketing
// (toStartOfInterval-style) is implemented with
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

	signals := []*qtypes.AggSignal{}

	conds := []string{
		// subject_bucket pruning lives inside LakeSignalsDeduped (B1).
		"s.subject = ?",
		"s.timestamp >= " + tsMicroLiteral(aggArgs.FromTS),
		"s.timestamp < " + tsMicroLiteral(aggArgs.ToTS),
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
		" FROM " + LakeSignalsDeduped(subject) + " AS s JOIN " + aggValuesTable(aggArgs.FloatArgs, aggArgs.StringArgs, aggArgs.LocationArgs) +
		" ON s.name = agg_table.name WHERE " + strings.Join(conds, " AND ")

	originUs := aggArgs.FromTS.UnixMicro()
	bucketExpr := fmt.Sprintf("make_timestamp(((epoch_us(timestamp) - %d) // %d) * %d + %d)",
		originUs, aggArgs.Interval, aggArgs.Interval, originUs)

	// COLUMN CONTRACT: this projection's order — out_type, out_index, group_timestamp,
	// <float>, <string>, agg_lat, agg_lon, agg_hdop, agg_heading — must stay in lockstep
	// with the positional rows.Scan below. A reorder or an added location component
	// silently mis-reads (an agg_lat↔agg_lon swap still passes value tests).
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

// GetAggregatedSignalsForRanges computes the same aggregations for multiple
// [From, To) segments in one
// query. The segment classifier is a CASE chain.
// Only FloatArgs and LocationArgs are supported, no per-signal
// value filters are applied, and (0, 0) locations are NOT excluded.
func (q *Queries) GetAggregatedSignalsForRanges(ctx context.Context, subject string, ranges []qtypes.TimeRange, globalFrom, globalTo time.Time, floatArgs []model.FloatSignalArgs, locationArgs []model.LocationSignalArgs) ([]*qtypes.AggSignalForRange, error) {
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(floatArgs) == 0 && len(locationArgs) == 0 {
		return []*qtypes.AggSignalForRange{}, nil
	}

	result := []*qtypes.AggSignalForRange{}

	// subject_bucket pruning lives inside LakeSignalsDeduped (B1).
	inner := "SELECT " + segmentIndexCaseSQL("s.timestamp", ranges) + " AS seg_idx, " + signalSrcColumns +
		" FROM " + LakeSignalsDeduped(subject) + " AS s JOIN " + aggValuesTable(floatArgs, nil, locationArgs) +
		" ON s.name = agg_table.name" +
		" WHERE s.subject = ?" +
		" AND s.timestamp >= " + tsMicroLiteral(globalFrom) +
		" AND s.timestamp < " + tsMicroLiteral(globalTo)

	// COLUMN CONTRACT: seg_idx, out_type, out_index, <float>, <string>, agg_lat,
	// agg_lon, agg_hdop, agg_heading — must stay in lockstep with the positional
	// rows.Scan below (see GetAggregatedSignals).
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
// location signal filter, or "" when there is none, using the DuckDB spatial
// extension (loaded in the bootstrap): inCircle is a spherical great-circle
// distance and inPolygon a point-in-ring containment test. InCircle takes
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

// haversineWithinSQL constrains the spherical great-circle distance from
// (latCol,lonCol) to the center to be within radiusKm, via the spatial extension's
// ST_Distance_Sphere (metres on the WGS-84 sphere). ST_Point takes (x=lon, y=lat).
func haversineWithinSQL(latCol, lonCol string, clat, clon, radiusKm float64) string {
	return fmt.Sprintf(
		"(ST_Distance_Sphere(ST_Point(%[1]s, %[2]s), ST_Point(%[3]s, %[4]s)) <= %[5]s)",
		lonCol, latCol, floatLit(clon), floatLit(clat), floatLit(radiusKm*1000))
}

// pointInPolygonSQL tests ring containment via the spatial extension's ST_Contains
// over a closed WGS-84 polygon. Vertices are inlined as "lon lat" pairs (fixed by
// the request — numeric literals only, no bound parameter or injection surface); the
// ring is closed by repeating the first vertex.
func pointInPolygonSQL(latCol, lonCol string, poly []*model.FilterLocation) string {
	// WKT needs bare numeric coords ("lon lat"), not floatLit's paren-wrapped form.
	coord := func(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
	verts := make([]string, 0, len(poly)+1)
	for _, p := range poly {
		verts = append(verts, coord(p.Longitude)+" "+coord(p.Latitude))
	}
	verts = append(verts, coord(poly[0].Longitude)+" "+coord(poly[0].Latitude)) // close the ring
	return fmt.Sprintf("ST_Contains(ST_GeomFromText('POLYGON((%s))'), ST_Point(%s, %s))",
		strings.Join(verts, ", "), lonCol, latCol)
}

// floatLit formats f as a parenthesized DuckDB DOUBLE literal (parens keep a
// negative coordinate from forming "- -122.4" after a subtraction operator).
func floatLit(f float64) string {
	return "(" + strconv.FormatFloat(f, 'g', -1, 64) + ")"
}

// aggValuesTable renders the (signal_type, signal_index, name) inline VALUES
// table identifying each requested aggregation, e.g.
// VALUES((1, 0, 'speed'), ...) for the join.
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

// perSignalFilterSQL builds the per-signal value-filter block:
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

// segmentIndexCaseSQL builds the segment classifier: rows falling in
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
// (rows of other types) are coalesced to 0.
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
// location aggregate output. Each component is aggregated with the same ordering key
// (timestamp, or the shared per-row rnd for RAND), so components still come
// from the same row except for AVG, which averages component-wise.
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
// EXACT aggregation: MED is DuckDB's exact
// median(value_number) (not an approximate quantile).
// So a window's median is EXACT, not estimated. Do NOT switch to an approximate
// quantile;
// pinned by TestExactAggExpr_Exact. (FIRST/LAST use arg_min/
// arg_max over the (subject,name,timestamp)-deduped scan, so they are tie-free and
// deterministic.)
//
// RAND decision: DuckDB has no seeded sampling
// aggregate, so RAND is arg_max(value, rnd) where rnd is one random() draw
// per input row: a uniform random pick, different per query.
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
// EXACT aggregation: UNIQUE is an EXACT distinct set (string_agg
// DISTINCT) and TOP is an EXACT mode() (not approximate). This is the same
// exact aggregation approach as median (see floatAggExpr).
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
