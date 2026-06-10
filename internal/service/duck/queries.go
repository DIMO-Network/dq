package duck

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/graph/model"
)

// Queries answers the signal/latest/summary/event queries previously served by
// the ClickHouse service (internal/service/ch) from parquet files written by
// the materializer:
//
//   - decoded/v1/signals/date=YYYY-MM-DD/*.parquet  (full-history signals)
//   - decoded/v1/latest/bucket=NN/latest.parquet    (per-(subject,source,name) latest)
//   - decoded/v1/summary/bucket=NN/summary.parquet  (per-(subject,source,name) counts)
//   - decoded/v1/events/date=YYYY-MM-DD/*.parquet   (full-history events)
//
// Method signatures mirror ch.Service so the repository layer can swap
// implementations. Segment detection and cloudevent queries are out of scope.
type Queries struct {
	svc           *Service
	bucket        string
	decodedPrefix string
}

// NewQueries creates a query layer over the given DuckDB service and parquet
// bucket. An empty bucket falls back to the service config's Bucket.
func NewQueries(svc *Service, bucket string) *Queries {
	cfg := svc.Config()
	if bucket == "" {
		bucket = cfg.Bucket
	}
	return &Queries{
		svc:           svc,
		bucket:        bucket,
		decodedPrefix: cfg.DecodedPrefix,
	}
}

// SummaryBucketPath returns the summary parquet path for a subject:
// <root>/<decodedPrefix>/summary/bucket=<HashBucket(subjectDID)>/summary.parquet.
func SummaryBucketPath(bucket, decodedPrefix, subjectDID string) string {
	pb := NewPathBuilder(bucket)
	return pb.Join(decodedPrefix, "summary", fmt.Sprintf("bucket=%03d", HashBucket(subjectDID)), "summary.parquet")
}

// EventGlobs returns explicit per-day parquet globs for decoded events:
// <root>/<decodedPrefix>/events/date=<YYYY-MM-DD>/*.parquet.
// The day range [from, to] is inclusive in UTC.
func EventGlobs(bucket, decodedPrefix string, from, to time.Time) []string {
	pb := NewPathBuilder(bucket)
	days := daysBetween(from, to)
	globs := make([]string, 0, len(days))
	for _, day := range days {
		globs = append(globs, pb.Join(decodedPrefix, "events", "date="+day, "*.parquet"))
	}
	return globs
}

// AllEventsGlob returns a glob matching decoded events across all dates,
// for all-time queries like event summaries.
func AllEventsGlob(bucket, decodedPrefix string) string {
	pb := NewPathBuilder(bucket)
	return pb.Join(decodedPrefix, "events", "date=*", "*.parquet")
}

func (q *Queries) latestPath(subject string) string {
	return LatestBucketPath(q.bucket, q.decodedPrefix, subject)
}

func (q *Queries) summaryPath(subject string) string {
	return SummaryBucketPath(q.bucket, q.decodedPrefix, subject)
}

func (q *Queries) signalGlobs(from, to time.Time) []string {
	return DecodedSignalGlobs(q.bucket, q.decodedPrefix, from, to)
}

func (q *Queries) eventGlobs(from, to time.Time) []string {
	return EventGlobs(q.bucket, q.decodedPrefix, from, to)
}

// expandGlobs resolves glob patterns to concrete files using DuckDB's glob()
// table function. read_parquet errors out if ANY pattern in its list matches
// zero files, so partitions with no data (missing days, absent latest buckets)
// must be pruned before querying. glob() returns zero rows for a no-match
// pattern instead of erroring, on both local filesystems and S3.
func (q *Queries) expandGlobs(ctx context.Context, patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	parts := make([]string, len(patterns))
	for i, p := range patterns {
		parts[i] = "SELECT file FROM glob(" + sqlString(p) + ")"
	}
	stmt := strings.Join(parts, " UNION ALL ")
	rows, err := q.svc.db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("failed expanding parquet globs: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var files []string
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, fmt.Errorf("failed scanning glob row: %w", err)
		}
		files = append(files, f)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("glob row error: %w", rows.Err())
	}
	return files, nil
}

// tableExpr returns a read_parquet table expression over all files matched by
// the patterns, or "" when no files exist (callers return empty results).
func (q *Queries) tableExpr(ctx context.Context, patterns []string) (string, error) {
	files, err := q.expandGlobs(ctx, patterns)
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", nil
	}
	return ReadParquetSQL(files), nil
}

// tsMicroLiteral formats a time.Time as a DuckDB TIMESTAMP literal with
// microsecond precision. Timestamps are inlined (like ch's dateTime64Micro)
// instead of bound, so sub-second precision can never be lost in binding.
func tsMicroLiteral(t time.Time) string {
	return fmt.Sprintf("make_timestamp(%d)", t.UnixMicro())
}

// epochLiteral is the zero timestamp, mirroring ClickHouse's default value
// for max()/maxIf() over empty sets. The repository layer treats the Unix
// epoch as "no data".
const epochLiteral = "make_timestamp(0)"

// placeholders renders n comma-separated '?' bind markers.
func placeholders(n int) string {
	if n == 0 {
		return ""
	}
	return strings.Repeat("?, ", n-1) + "?"
}

// signalSourceCond ports ch's withSource: an ethr DID source filter is
// reduced to its contract address hex before matching the source column.
func signalSourceCond(col string, filter *model.SignalFilter) (string, []any) {
	if filter == nil || filter.Source == nil {
		return "", nil
	}
	source := *filter.Source
	if did, err := cloudevent.DecodeEthrDID(source); err == nil {
		source = did.ContractAddress.Hex()
	}
	return col + " = ?", []any{source}
}

// floatFilterSQL ports ch's buildFloatConditionList: leaf conditions are
// AND-joined, and the Or list becomes one additional AND-ed group of OR-ed
// sub-filters: base1 AND base2 AND ((or1) OR (or2)).
func floatFilterSQL(col string, fil *model.SignalFloatFilter) (string, []any) {
	if fil == nil {
		return "", nil
	}
	var conds []string
	var args []any
	if fil.Eq != nil {
		conds, args = append(conds, col+" = ?"), append(args, *fil.Eq)
	}
	if fil.Neq != nil {
		conds, args = append(conds, col+" != ?"), append(args, *fil.Neq)
	}
	if fil.Gt != nil {
		conds, args = append(conds, col+" > ?"), append(args, *fil.Gt)
	}
	if fil.Lt != nil {
		conds, args = append(conds, col+" < ?"), append(args, *fil.Lt)
	}
	if fil.Gte != nil {
		conds, args = append(conds, col+" >= ?"), append(args, *fil.Gte)
	}
	if fil.Lte != nil {
		conds, args = append(conds, col+" <= ?"), append(args, *fil.Lte)
	}
	if len(fil.NotIn) != 0 {
		conds = append(conds, col+" NOT IN ("+placeholders(len(fil.NotIn))+")")
		for _, v := range fil.NotIn {
			args = append(args, v)
		}
	}
	if len(fil.In) != 0 {
		conds = append(conds, col+" IN ("+placeholders(len(fil.In))+")")
		for _, v := range fil.In {
			args = append(args, v)
		}
	}

	var orParts []string
	for _, cond := range fil.Or {
		s, a := floatFilterSQL(col, cond)
		if s != "" {
			orParts = append(orParts, "("+s+")")
			args = append(args, a...)
		}
	}
	if len(orParts) != 0 {
		conds = append(conds, "("+strings.Join(orParts, " OR ")+")")
	}
	return strings.Join(conds, " AND "), args
}

// stringFilterSQL ports ch's stringFilterMod. Leaf conditions are AND-joined;
// Or clauses are OR-ed against that AND-ed base (ClickHouse operator
// precedence): (base1 AND base2 OR (or1) OR (or2)).
func stringFilterSQL(col string, fil *model.StringValueFilter) (string, []any) {
	if fil == nil {
		return "", nil
	}
	var conds []string
	var args []any
	if fil.Eq != nil {
		conds, args = append(conds, col+" = ?"), append(args, *fil.Eq)
	}
	if fil.Neq != nil {
		conds, args = append(conds, col+" != ?"), append(args, *fil.Neq)
	}
	if fil.NotIn != nil {
		conds = append(conds, col+" NOT IN ("+placeholders(len(fil.NotIn))+")")
		for _, v := range fil.NotIn {
			args = append(args, v)
		}
	}
	if fil.In != nil {
		conds = append(conds, col+" IN ("+placeholders(len(fil.In))+")")
		for _, v := range fil.In {
			args = append(args, v)
		}
	}
	if fil.StartsWith != nil {
		// DuckDB LIKE has no default escape character, so declare one.
		conds = append(conds, col+` LIKE ? ESCAPE '\'`)
		args = append(args, escapeLikePrefix(*fil.StartsWith))
	}

	var orParts []string
	for _, cond := range fil.Or {
		s, a := stringFilterSQL(col, cond)
		if s != "" {
			orParts = append(orParts, "("+s+")")
			args = append(args, a...)
		}
	}
	if len(orParts) == 0 {
		return strings.Join(conds, " AND "), args
	}
	parts := make([]string, 0, 1+len(orParts))
	if len(conds) != 0 {
		parts = append(parts, strings.Join(conds, " AND "))
	}
	parts = append(parts, orParts...)
	return "(" + strings.Join(parts, " OR ") + ")", args
}

// stringArrayFilterSQL ports ch's stringArrayFilterMod using DuckDB's
// list_has_any/list_has_all in place of ClickHouse's hasAny/hasAll. Or
// handling matches stringFilterSQL.
func stringArrayFilterSQL(col string, fil *model.StringArrayFilter) (string, []any) {
	if fil == nil {
		return "", nil
	}
	var conds []string
	var args []any
	listCond := func(fn, prefix string, vals []string) {
		conds = append(conds, prefix+fn+"("+col+", ["+placeholders(len(vals))+"])")
		for _, v := range vals {
			args = append(args, v)
		}
	}
	if len(fil.ContainsAny) != 0 {
		listCond("list_has_any", "", fil.ContainsAny)
	}
	if len(fil.ContainsAll) != 0 {
		listCond("list_has_all", "", fil.ContainsAll)
	}
	if len(fil.NotContainsAny) != 0 {
		listCond("list_has_any", "NOT ", fil.NotContainsAny)
	}
	if len(fil.NotContainsAll) != 0 {
		listCond("list_has_all", "NOT ", fil.NotContainsAll)
	}

	var orParts []string
	for _, cond := range fil.Or {
		s, a := stringArrayFilterSQL(col, cond)
		if s != "" {
			orParts = append(orParts, "("+s+")")
			args = append(args, a...)
		}
	}
	if len(orParts) == 0 {
		return strings.Join(conds, " AND "), args
	}
	parts := make([]string, 0, 1+len(orParts))
	if len(conds) != 0 {
		parts = append(parts, strings.Join(conds, " AND "))
	}
	parts = append(parts, orParts...)
	return "(" + strings.Join(parts, " OR ") + ")", args
}

// escapeLikePrefix escapes LIKE metacharacters in a prefix and appends '%',
// mirroring ch's escapeLikePrefix.
func escapeLikePrefix(prefix string) string {
	s := strings.ReplaceAll(prefix, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s + "%"
}
