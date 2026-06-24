package duck

import (
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/graph/model"
)

// Queries answers the signal/latest/summary/event queries served by the query
// layer from the DuckLake catalog tables (lake.signals / lake.events) attached
// on the service. Latest/summary are computed from the base table (or the
// signals_latest rollup).
//
// This is the query surface over the DuckLake catalog. Segment detection and
// cloudevent queries are out of scope.
type Queries struct {
	svc *Service
}

// NewLakeQueries creates a query layer that reads the DuckLake catalog tables
// (lake.signals / lake.events) attached on svc.
func NewLakeQueries(svc *Service) *Queries {
	return &Queries{svc: svc}
}

// lakeEventsDeduped is the canonical DuckLake decoded-event source: lake.events
// with at-rest duplicate rows collapsed to one. The materializer's INSERT
// anti-join keys on (subject_bucket, cloud_event_id, name, timestamp), so two
// distinct cloud_event_ids that decode to the same logical event both survive —
// reading the bare table over-counts events, so duplicates are collapsed
// on (subject, timestamp, name, source). Dedup on
// that same key here (lowest cloud_event_id wins, deterministically) restores
// correct counts (SR review #3; the events analogue of lakeSignalsDeduped / CHD-2).
// subject/timestamp remain partition/sort keys, so predicates still prune.
const lakeEventsDeduped = `(SELECT * FROM lake.events ` +
	`QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, timestamp, name, source ORDER BY cloud_event_id) = 1)`

// tsMicroLiteral formats a time.Time as a DuckDB TIMESTAMP literal with
// microsecond precision. Timestamps are inlined instead of bound, so sub-second
// precision can never be lost in binding.
func tsMicroLiteral(t time.Time) string {
	return fmt.Sprintf("make_timestamp(%d)", t.UnixMicro())
}

// epochLiteral is the zero timestamp, used as the default value
// for max() over empty sets. The repository layer treats the Unix
// epoch as "no data".
const epochLiteral = "make_timestamp(0)"

// placeholders renders n comma-separated '?' bind markers.
func placeholders(n int) string {
	if n == 0 {
		return ""
	}
	return strings.Repeat("?, ", n-1) + "?"
}

// signalSourceCond builds the source-column predicate: an ethr DID source
// filter is reduced to its contract address hex before matching the source
// column.
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

// floatFilterSQL builds a float value filter: leaf conditions are AND-joined,
// and the Or list becomes one additional AND-ed group of OR-ed sub-filters:
// base1 AND base2 AND ((or1) OR (or2)).
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

// stringFilterSQL builds a string value filter. Leaf conditions are AND-joined;
// Or clauses are OR-ed against that AND-ed base (operator
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
	// len() != 0, not != nil: a non-nil empty slice (gqlgen yields one for
	// `in: []`) would render "col IN ()", a DuckDB parser error. Matches
	// floatFilterSQL / stringArrayFilterSQL.
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

// stringArrayFilterSQL builds a string-array filter using DuckDB's
// list_has_any/list_has_all for any/all overlap. Or
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

// escapeLikePrefix escapes LIKE metacharacters in a prefix and appends '%'.
func escapeLikePrefix(prefix string) string {
	s := strings.ReplaceAll(prefix, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s + "%"
}
