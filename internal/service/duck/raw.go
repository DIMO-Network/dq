package duck

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
)

// ErrNotFound reports that a query matched no stored cloud event. It wraps
// sql.ErrNoRows so that errors.Is(err, sql.ErrNoRows) is true, which the
// gRPC layer uses to map absence to codes.NotFound.
var ErrNotFound = fmt.Errorf("cloud event not found: %w", sql.ErrNoRows)

// RawFilter narrows a raw cloudevent scan. Zero values mean "no filter".
// The eventrepo facade translates grpc.SearchOptions into this; the live
// DuckLake fetch path (lake_fetch.go) builds its WHERE clause from it via
// whereClauseQ.
//
// Field semantics:
//   - Subject / Subjects: single equality vs. multi-value IN (Subjects takes
//     priority when non-empty; Subject is kept for backward compat).
//   - *NotIn fields: NOT IN exclusion filter, mirrors StringFilterOption.NotIn.
//   - Tags*: array overlap operations on extras.tags via list_has_any / list_has_all.
//   - Extras / ExtrasNotIn: IN / NOT IN filter on the raw extras text column.
type RawFilter struct {
	// Subject is a single-value equality shorthand.  When Subjects is non-empty
	// it takes precedence and Subject is ignored.
	Subject  string
	Subjects []string // multi-value IN filter on the subject column

	Types             []string
	TypesNotIn        []string
	Sources           []string
	SourcesNotIn      []string
	Producers         []string
	ProducersNotIn    []string
	IDs               []string
	IDsNotIn          []string
	DataVersions      []string
	DataVersionsNotIn []string

	// Tags array filters — each list is ANDed together.
	Tags              []string // list_has_any (ContainsAny)
	TagsAll           []string // list_has_all (ContainsAll)
	TagsNotContainAny []string // NOT list_has_any (NotContainsAny)
	TagsNotContainAll []string // NOT list_has_all (NotContainsAll)

	// Extras column (raw text) IN / NOT IN.
	Extras      []string
	ExtrasNotIn []string

	After         time.Time
	Before        time.Time
	ExcludeVoided bool // hide events voided by a tombstone (voids_id anti-join)

	// TimestampAsc controls ORDER BY direction. When true, results are returned
	// oldest-first (ASC); false or unset means newest-first (DESC). Mirrors
	// eventrepo.ListIndexesAdvanced's GetTimestampAsc decision.
	TimestampAsc bool
}

// whereClauseQ builds a WHERE fragment qualifying each column name with prefix
// (e.g. "e." → "e.subject", "e.type", …). Use "" for unqualified columns.
// ExcludeVoided is NOT applied here — it requires a correlated subquery that
// depends on the FROM table, so it is handled by the lake query builder.
//
// All filter semantics mirror eventrepo exactly. See RawFilter for
// field-level documentation.
func whereClauseQ(filter RawFilter, prefix string) (string, []any) {
	conds := []string{"1=1"}
	var args []any
	col := func(name string) string { return prefix + name }

	addIn := func(name string, vals []string) {
		if len(vals) == 0 {
			return
		}
		conds = append(conds, fmt.Sprintf("%s IN (%s)", col(name), placeholders(len(vals))))
		for _, v := range vals {
			args = append(args, v)
		}
	}
	addNotIn := func(name string, vals []string) {
		if len(vals) == 0 {
			return
		}
		conds = append(conds, fmt.Sprintf("%s NOT IN (%s)", col(name), placeholders(len(vals))))
		for _, v := range vals {
			args = append(args, v)
		}
	}

	// Subject: multi-value IN takes precedence over single equality.
	if len(filter.Subjects) > 0 {
		addIn("subject", filter.Subjects)
	} else if filter.Subject != "" {
		conds = append(conds, col("subject")+" = ?")
		args = append(args, filter.Subject)
	}

	addIn("type", filter.Types)
	addNotIn("type", filter.TypesNotIn)
	addIn("source", filter.Sources)
	addNotIn("source", filter.SourcesNotIn)
	addIn("producer", filter.Producers)
	addNotIn("producer", filter.ProducersNotIn)
	addIn("id", filter.IDs)
	addNotIn("id", filter.IDsNotIn)
	addIn("data_version", filter.DataVersions)
	addNotIn("data_version", filter.DataVersionsNotIn)

	// Extras column (raw text) IN / NOT IN — matches eventrepo ExtrasColumn filter.
	addIn("extras", filter.Extras)
	addNotIn("extras", filter.ExtrasNotIn)

	// Tags array filters — extras is a JSON text column; tags live at $.tags.
	// try_cast parses the JSON array element as VARCHAR[] for DuckDB list functions.
	// All conditions are ANDed.
	tagsExpr := func() string {
		return fmt.Sprintf("COALESCE(try_cast(json_extract(%sextras, '$.tags') AS VARCHAR[]), [])", prefix)
	}
	if len(filter.Tags) > 0 {
		ph := placeholders(len(filter.Tags))
		conds = append(conds, fmt.Sprintf("list_has_any(%s, [%s])", tagsExpr(), ph))
		for _, t := range filter.Tags {
			args = append(args, t)
		}
	}
	if len(filter.TagsAll) > 0 {
		ph := placeholders(len(filter.TagsAll))
		conds = append(conds, fmt.Sprintf("list_has_all(%s, [%s])", tagsExpr(), ph))
		for _, t := range filter.TagsAll {
			args = append(args, t)
		}
	}
	if len(filter.TagsNotContainAny) > 0 {
		ph := placeholders(len(filter.TagsNotContainAny))
		conds = append(conds, fmt.Sprintf("NOT list_has_any(%s, [%s])", tagsExpr(), ph))
		for _, t := range filter.TagsNotContainAny {
			args = append(args, t)
		}
	}
	if len(filter.TagsNotContainAll) > 0 {
		ph := placeholders(len(filter.TagsNotContainAll))
		conds = append(conds, fmt.Sprintf("NOT list_has_all(%s, [%s])", tagsExpr(), ph))
		for _, t := range filter.TagsNotContainAll {
			args = append(args, t)
		}
	}

	if !filter.After.IsZero() {
		// Strict greater-than (timestamp > ?) for After-boundary semantics.
		conds = append(conds, col("time")+" > ?")
		args = append(args, filter.After.UTC())
	}
	if !filter.Before.IsZero() {
		conds = append(conds, col("time")+" < ?")
		args = append(args, filter.Before.UTC())
	}
	return strings.Join(conds, " AND "), args
}

// rowScanner abstracts *sql.Rows for scanStoredEvent.
type rowScanner interface {
	Scan(dest ...any) error
}

// restoreNonColumnFieldsSafe wraps cloudevent.RestoreNonColumnFields, which rebuilds
// non-column header fields (Tags, etc.) from the producer-supplied extras map via
// unchecked type assertions — a malformed element (e.g. {"tags":[42]}) panics.
// Containing the panic keeps the row (the malformed field simply not restored)
// instead of aborting the whole multi-row fetch; the gRPC recovery interceptor
// (app.go) would catch an escaped panic, but that fails the entire request. din
// validates Tags as a typed []string at ingest, so this is defense-in-depth at the
// din→dq boundary; the counter makes a poisoned row alertable rather than silent.
func restoreNonColumnFieldsSafe(hdr *cloudevent.CloudEventHeader) {
	defer func() {
		if recover() != nil {
			fetchMalformedRowTotal.Inc()
		}
	}()
	cloudevent.RestoreNonColumnFields(hdr)
}

// scanStoredEvent mirrors cloudevent/parquet's convertRow: rebuild the
// header, restore non-column fields from extras, attach payload.
func scanStoredEvent(row rowScanner) (cloudevent.StoredEvent, error) {
	var ev cloudevent.StoredEvent
	var extras, data, dataIndexKey, voidsID *string
	var dataBase64 []byte
	if err := row.Scan(&ev.Subject, &ev.Time, &ev.Type, &ev.ID, &ev.Source, &ev.Producer,
		&ev.DataContentType, &ev.DataVersion, &extras, &data, &dataBase64, &dataIndexKey, &voidsID); err != nil {
		return ev, fmt.Errorf("scanning raw cloudevent: %w", err)
	}
	ev.SpecVersion = cloudevent.SpecVersion
	ev.Time = ev.Time.UTC()

	if extras != nil && *extras != "" && *extras != "{}" {
		ev.Extras = map[string]any{}
		if err := json.Unmarshal([]byte(*extras), &ev.Extras); err != nil {
			return ev, fmt.Errorf("decoding extras for %s: %w", ev.ID, err)
		}
		restoreNonColumnFieldsSafe(&ev.CloudEventHeader)
	}
	if len(dataBase64) > 0 {
		ev.DataBase64 = string(dataBase64)
	} else if data != nil {
		ev.Data = json.RawMessage(*data)
	}
	if dataIndexKey != nil {
		ev.DataIndexKey = *dataIndexKey
	}
	if voidsID != nil {
		ev.VoidsID = *voidsID
	}
	return ev, nil
}
