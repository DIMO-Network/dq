package duck

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// DEDUP KEYS — the four dedup tuples in this codebase and why they differ. At-least-
// once ingest (device retry, sink redelivery, dq cross-batch) can store the same
// reading more than once under different cloud_event_ids; every read/write that must
// not over-count collapses duplicates with the SMALLEST cloud_event_id winning. The
// tuples differ deliberately by what makes a row "the same reading" in each table:
//
//   1. signals READ dedup  — (subject, name, timestamp) ORDER BY cloud_event_id
//        `signalDedupQualify` below; embedded by LakeSignalsDeduped and the segment
//        signal-source queries (segments_source.go). One value per signal per instant.
//   2. signals WRITE anti-join — (subject_bucket, cloud_event_id, name, timestamp)
//        materializer INSERT (ducklake.go): keyed on cloud_event_id so a re-decoded
//        batch is idempotent; subject_bucket prunes the partition.
//   3. events READ dedup   — (subject, timestamp, name, source)
//        LakeEventsDeduped (queries.go): events include `source` because the SAME
//        event from two sources is two distinct events, unlike signals.
//   4. signals_latest rollup recompute — the signal key again
//        (ducklake.go) so the rollup matches the on-read deduped scan exactly.
//
// LocationAt (below) and the ignition seed (segments_source.go) re-derive key #1's
// tie-break (ORDER BY ... cloud_event_id ASC) by hand for a single-row lookup. Change
// any one of these and you silently inflate or drop rows — keep them in lockstep.

// signalDedupQualify collapses duplicate lake.signals rows to one per
// (subject,name,timestamp), keeping the lowest cloud_event_id (read dedup key #1
// above). LakeSignalsDeduped wraps it as an aggregation source; the segment
// signal-source queries (segments_source.go) embed it against the bare table.
const signalDedupQualify = `QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, name, timestamp ORDER BY cloud_event_id) = 1`

// LakeSignalsDeduped is the canonical DuckLake decoded-signal source for one
// subject: lake.signals pruned to the subject's hash-bucket partition and with
// at-rest duplicate (subject,name,timestamp) rows collapsed to one row (lowest
// cloud_event_id). At-least-once ingest (device retry, sink redelivery, dq
// cross-batch) can store the same reading more than once with a different
// cloud_event_id; reading the bare table over-counts
// avg/count/sum/median/latest/summary (CHD-2 / R1-C1). After collapsing, every
// (subject,name,timestamp) is unique, so arg_max(value, timestamp) for latest
// has no tie-break ambiguity either (R1-C2). Matches the segments path
// (segments_source.go), collapsing duplicate rows.
//
// The subject_bucket predicate MUST live here, INSIDE the subquery at the same
// SELECT level as the QUALIFY (B1): DuckDB pushes only filters on the window's
// PARTITION BY columns below a WINDOW operator. subject and timestamp are
// PARTITION BY columns, so outer predicates on them still prune — but
// subject_bucket is not, so an outer bucket filter parks in a FILTER above the
// window and partition pruning silently dies (every query scans all
// NumLatestBuckets buckets). Inside the subquery it reaches the scan. It is
// safe inside: subject_bucket is a pure function of subject, so it is constant
// within every dedup partition and cannot change which row wins. Exported so
// tests/ducklake_partition_test.go can EXPLAIN-pin the pushdown.
//
// srcCond is an OPTIONAL extra pre-QUALIFY predicate ("" = none), carrying a
// single "source = ?" bind marker. A source filter MUST live here, inside the
// dedup subquery at QUALIFY level — NOT as an outer predicate (Item 1): two
// sources can report the same (subject,name,µs-timestamp), and dedup keeps the
// lowest cloud_event_id. If the OTHER source's id sorts lower it wins dedup and
// an OUTER `source = ?` then removes it — so the requested source's genuine
// reading returns NOTHING. Filtering to the source BEFORE dedup makes the
// requested source's row the only candidate, so it always survives. The
// no-source path (srcCond == "") is byte-identical to before (canonical
// one-value-per-instant policy unchanged). CAUTION at call sites: this marker is
// in the FROM subquery, so in SQL text order it precedes every outer `?` — its
// bind arg must be appended FIRST.
func LakeSignalsDeduped(subject, srcCond string) string {
	pred := subjectBucketPredicate("", subject)
	if srcCond != "" {
		pred += " AND " + srcCond
	}
	return `(SELECT * FROM lake.signals WHERE ` + pred + ` ` + signalDedupQualify + `)`
}

// lakeNonZeroLoc is the on-the-fly (0,0)-exclusion computed over the base
// location columns of lake.signals.
const lakeNonZeroLoc = "(loc_lat != 0 OR loc_lon != 0)"

// getLatestSignalsLake computes latest values directly from lake.signals:
// arg_max by timestamp for plain values, and arg_max over (0,0)-filtered base
// location columns.
func (q *Queries) getLatestSignalsLake(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	if len(latestArgs.SignalNames) == 0 && len(latestArgs.LocationSignalNames) == 0 && !latestArgs.IncludeLastSeen {
		return nil, nil
	}
	// The source filter lives INSIDE the dedup subquery (Item 1, see
	// LakeSignalsDeduped), so its bind arg precedes the outer args (subject, names)
	// in every UNION arm below.
	srcCond, srcArgs := signalSourceCond("source", latestArgs.Filter)
	dedup := LakeSignalsDeduped(subject, srcCond)

	var stmts []string
	var args []any
	if len(latestArgs.SignalNames) > 0 {
		names := mapKeys(latestArgs.SignalNames)
		stmts = append(stmts, fmt.Sprintf(
			`SELECT name, max(timestamp) AS ts,
				coalesce(arg_max(value_number, timestamp), 0) AS value_number,
				coalesce(arg_max(value_string, timestamp), '') AS value_string,
				0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading, %s AS loc_ts
			FROM %s WHERE subject = ? AND name IN (%s) GROUP BY name`,
			epochLiteral, dedup, placeholders(len(names))))
		args = append(args, srcArgs...)
		args = append(args, subject)
		for _, n := range names {
			args = append(args, n)
		}
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
				coalesce(arg_max(loc_heading, timestamp) FILTER (WHERE %[1]s), 0) AS loc_heading,
				coalesce(max(timestamp) FILTER (WHERE %[1]s), %[2]s) AS loc_ts
			FROM %[3]s WHERE subject = ? AND name IN (%[4]s) GROUP BY name`,
			lakeNonZeroLoc, epochLiteral, dedup, placeholders(len(names))))
		args = append(args, srcArgs...)
		args = append(args, subject)
		for _, n := range names {
			args = append(args, n)
		}
	}
	if latestArgs.IncludeLastSeen {
		stmt, a := lakeLastSeenQuery(subject, srcCond, srcArgs)
		stmts = append(stmts, stmt)
		args = append(args, a...)
	}
	return q.querySignals(ctx, strings.Join(stmts, " UNION ALL ")+" ORDER BY name", args)
}

// getAllLatestSignalsLake is getLatestSignalsLake for every stored name.
func (q *Queries) getAllLatestSignalsLake(ctx context.Context, subject string, filter *model.SignalFilter) ([]*vss.Signal, error) {
	// The source filter lives INSIDE the dedup subquery (Item 1), so its bind arg
	// precedes the outer subject arg in both UNION arms.
	srcCond, srcArgs := signalSourceCond("source", filter)
	dedup := LakeSignalsDeduped(subject, srcCond)
	// loc_ts (Item 2) carries the (0,0)-filtered latest-fix time so the snapshot
	// consumer stamps the location VALUE with the fix time, not the unfiltered
	// max(timestamp) — a trailing (0,0) reading would otherwise report the last
	// real fix at a later instant, disagreeing with GetLatestSignals.
	mainStmt := fmt.Sprintf(
		`SELECT name, max(timestamp) AS ts,
			coalesce(arg_max(value_number, timestamp), 0) AS value_number,
			coalesce(arg_max(value_string, timestamp), '') AS value_string,
			coalesce(arg_max(loc_lat, timestamp) FILTER (WHERE %[1]s), 0) AS loc_lat,
			coalesce(arg_max(loc_lon, timestamp) FILTER (WHERE %[1]s), 0) AS loc_lon,
			coalesce(arg_max(loc_hdop, timestamp) FILTER (WHERE %[1]s), 0) AS loc_hdop,
			coalesce(arg_max(loc_heading, timestamp) FILTER (WHERE %[1]s), 0) AS loc_heading,
			coalesce(max(timestamp) FILTER (WHERE %[1]s), %[3]s) AS loc_ts
		FROM %[2]s WHERE subject = ? GROUP BY name`,
		lakeNonZeroLoc, dedup, epochLiteral)
	var args []any
	args = append(args, srcArgs...)
	args = append(args, subject)

	lastSeenStmt, lastSeenArgs := lakeLastSeenQuery(subject, srcCond, srcArgs)
	stmt := mainStmt + " UNION ALL " + lastSeenStmt + " ORDER BY name"
	args = append(args, lastSeenArgs...)
	return q.querySignals(ctx, stmt, args)
}

// lakeLastSeenQuery computes the virtual lastSeen row (max timestamp over all
// of the subject's signals) directly, since lake.signals stores no
// precomputed lastSeen rows. srcCond is folded into the dedup subquery (Item 1),
// so its bind arg precedes the subject arg. loc_ts is epoch (the lastSeen row
// carries no location value).
func lakeLastSeenQuery(subject, srcCond string, srcArgs []any) (string, []any) {
	stmt := fmt.Sprintf(
		`SELECT %[1]s AS name, coalesce(max(timestamp), %[2]s) AS ts,
			0.0 AS value_number, '' AS value_string,
			0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading, %[2]s AS loc_ts
		FROM %[3]s WHERE subject = ?`,
		sqlString(model.LastSeenField), epochLiteral, LakeSignalsDeduped(subject, srcCond))
	args := make([]any, 0, len(srcArgs)+1)
	args = append(args, srcArgs...)
	args = append(args, subject)
	return stmt, args
}

// getAvailableSignalsLake lists distinct signal names from lake.signals.
func (q *Queries) getAvailableSignalsLake(ctx context.Context, subject string, filter *model.SignalFilter) ([]string, error) {
	// Source filter folded into the dedup subquery (Item 1): its bind arg precedes
	// the outer subject arg.
	srcCond, srcArgs := signalSourceCond("source", filter)
	stmt := fmt.Sprintf("SELECT DISTINCT name FROM %s WHERE subject = ? ORDER BY name", LakeSignalsDeduped(subject, srcCond))
	args := make([]any, 0, len(srcArgs)+1)
	args = append(args, srcArgs...)
	args = append(args, subject)
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
// from lake.signals.
func (q *Queries) getSignalSummariesLake(ctx context.Context, subject string, filter *model.SignalFilter) ([]*model.SignalDataSummary, error) {
	// Source filter folded into the dedup subquery (Item 1): its bind arg precedes
	// the outer subject arg.
	srcCond, srcArgs := signalSourceCond("source", filter)
	stmt := fmt.Sprintf(
		`SELECT name, CAST(count(*) AS UBIGINT) AS count, min(timestamp), max(timestamp)
		FROM %s WHERE subject = ? GROUP BY name ORDER BY name`,
		LakeSignalsDeduped(subject, srcCond))
	args := make([]any, 0, len(srcArgs)+1)
	args = append(args, srcArgs...)
	args = append(args, subject)
	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("querying lake signal summaries: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	summaries := []*model.SignalDataSummary{}
	for rows.Next() {
		s, err := scanSignalSummary(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning summary: %w", err)
		}
		summaries = append(summaries, s)
	}
	return summaries, rows.Err()
}

// locationGapFillLookback bounds how far before a requested timestamp LocationAt /
// LocationsAt will reach for the nearest non-origin fix; a fix older than this is
// treated as "no fix" and the caller substitutes the (0,0) no-data sentinel. Without
// a floor a GPS-sparse or fix-less vehicle forces a full reverse scan of its entire
// retained subject_bucket partition on every point lookup (finding #8). 90d is
// generous enough that a real trip's prior fix is virtually always inside it, while
// capping the deep scan and letting day-partition pruning drop older files. LocationAt
// and LocationsAt MUST share this floor so the batched path stays exactly equivalent
// to the per-point path.
const locationGapFillLookback = 90 * 24 * time.Hour

// LocationAt returns the nearest non-origin currentLocationCoordinates fix at or
// before ts — a point lookup that reaches back up to locationGapFillLookback before
// ts, deterministic on ties (lowest cloud_event_id, matching the read-path dedup).
// nil means the vehicle has no known fix in [ts-lookback, ts]. The segment enrichment
// uses it to gap-fill a trip's start/end location when no GPS fix landed inside the
// (often short) trip window — a correctness win a window-bounded argMin/argMax
// structurally cannot do. LocationsAt is the batched, index-aligned equivalent.
func (q *Queries) LocationAt(ctx context.Context, subject string, ts time.Time) (*model.Location, error) {
	floorMicro := ts.UTC().Add(-locationGapFillLookback).UnixMicro()
	query := fmt.Sprintf(`
SELECT loc_lat, loc_lon, loc_hdop FROM lake.signals
WHERE subject = ? AND %[2]s AND name = ? AND %[1]s
  AND timestamp <= make_timestamp(%[3]d)
  AND timestamp >= make_timestamp(%[4]d)
ORDER BY timestamp DESC, cloud_event_id ASC LIMIT 1`,
		lakeNonZeroLoc, subjectBucketPredicate("", subject), ts.UTC().UnixMicro(), floorMicro)

	var loc model.Location
	err := q.svc.db.QueryRowContext(ctx, query, subject, vss.FieldCurrentLocationCoordinates).
		Scan(&loc.Latitude, &loc.Longitude, &loc.Hdop)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lake location at %s: %w", ts, err)
	}
	return &loc, nil
}

// LocationsAt resolves, for each timestamp in tss, the nearest non-origin
// currentLocationCoordinates fix at or before it — the batched form of LocationAt.
// The result is index-aligned with tss (out[i] is the fix for tss[i], nil when the
// vehicle has no fix in [tss[i]-lookback, tss[i]]). It replaces the per-boundary
// LocationAt fan-out in segment / daily-activity enrichment: one ASOF join instead of
// O(segments) reverse-scan point queries (finding #8).
//
// Exactly equivalent to calling LocationAt for each timestamp. The right side filters
// to non-origin fixes for the location name FIRST, then collapses duplicate
// (subject,name,timestamp) rows to the lowest cloud_event_id (read dedup key #1) —
// exactly LocationAt's ORDER BY timestamp DESC, cloud_event_id ASC LIMIT 1 tie-break.
// The (0,0) exclusion MUST precede the dedup (like LocationAt's WHERE): a two-source
// collision where the (0,0) row sorts lower on cloud_event_id would otherwise win
// dedup and then be filtered out, dropping the real fix. ASOF LEFT JOIN then picks,
// per probe row, the single right row with the greatest timestamp <= the probe (the
// nearest prior fix), leaving NULL when none — so the tie-free dedup makes the ASOF
// pick deterministic and identical to the point lookup. The lookback floor is the
// minimum requested timestamp minus locationGapFillLookback, so the shared scan prunes
// old day-partitions; a per-probe floor is re-applied below to match LocationAt exactly.
func (q *Queries) LocationsAt(ctx context.Context, subject string, tss []time.Time) ([]*model.Location, error) {
	out := make([]*model.Location, len(tss))
	if len(tss) == 0 {
		return out, nil
	}
	minTS := tss[0].UTC()
	probes := make([]string, len(tss))
	for i, t := range tss {
		tu := t.UTC()
		if tu.Before(minTS) {
			minTS = tu
		}
		probes[i] = fmt.Sprintf("(%d, %d)", i, tu.UnixMicro())
	}
	lookbackMicro := locationGapFillLookback.Microseconds()
	floorMicro := minTS.Add(-locationGapFillLookback).UnixMicro()

	// The right subquery mirrors LocationAt's WHERE (non-origin fixes for the location
	// name in the subject's bucket, globally floored so the shared scan prunes old
	// day-partitions) then dedups per timestamp to the lowest cloud_event_id. ASOF
	// matches the greatest right timestamp <= the probe. The outer WHERE re-applies the
	// PER-PROBE floor (probe - lookback): a probe later than minTS could otherwise ASOF
	// onto a fix older than its own lookback that survived the looser global floor, so
	// this keeps LocationsAt exactly equal to LocationAt. Unmatched probes (NULL right)
	// survive the WHERE and stay nil in the output.
	query := fmt.Sprintf(`
SELECT q.idx, s.loc_lat, s.loc_lon, s.loc_hdop
FROM (VALUES %[1]s) AS q(idx, ts_us)
ASOF LEFT JOIN (
	SELECT timestamp, loc_lat, loc_lon, loc_hdop FROM lake.signals
	WHERE %[2]s AND subject = ? AND name = ? AND %[3]s
	  AND timestamp >= make_timestamp(%[4]d)
	%[5]s
) AS s
ON make_timestamp(q.ts_us) >= s.timestamp
WHERE s.timestamp IS NULL OR s.timestamp >= make_timestamp(q.ts_us - %[6]d)
ORDER BY q.idx`,
		strings.Join(probes, ", "),
		subjectBucketPredicate("", subject),
		lakeNonZeroLoc,
		floorMicro,
		signalDedupQualify,
		lookbackMicro)

	rows, err := q.svc.db.QueryContext(ctx, query, subject, vss.FieldCurrentLocationCoordinates)
	if err != nil {
		return nil, fmt.Errorf("lake locations at (%d probes): %w", len(tss), err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var idx int
		var lat, lon, hdop sql.NullFloat64
		if err := rows.Scan(&idx, &lat, &lon, &hdop); err != nil {
			return nil, fmt.Errorf("scanning locations-at row: %w", err)
		}
		if idx < 0 || idx >= len(out) || !lat.Valid {
			continue // unmatched probe (ASOF NULL) → leave nil (no fix within lookback)
		}
		out[idx] = &model.Location{Latitude: lat.Float64, Longitude: lon.Float64, Hdop: hdop.Float64}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("locations-at row error: %w", rows.Err())
	}
	return out, nil
}
