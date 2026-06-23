// fetch_lake_parity_test.go documents and pins the fetch-contract semantics of
// duck.LakeEventService against lake.raw_events.
//
// PURPOSE: every bullet in the contract surface must be demonstrably covered
// somewhere in the test suite. Tests that are ALREADY covered by existing
// files are noted by reference; this file adds the GAPS.
//
// ────────────────────────────────────────────────────────────────────────────
// FETCH CONTRACT COVERAGE MAP
// (each eventrepo behaviour → where it is pinned)
// ────────────────────────────────────────────────────────────────────────────
//
//  1. ORDERING — list/latest return newest-first by time.
//     Rule: ORDER BY timestamp DESC.
//     Covered by:
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_ListIndexesAdvanced
//       • tests/ducklake_only_test.go:TestDuckLakeOnly_FetchQueriesWork
//
//  2. STRICT After — event exactly at After is EXCLUDED (timestamp > ?).
//     Covered by:
//       • internal/service/duck/lake_fetch_test.go:TestAfterBoundaryIsStrict
//
//  3. STRICT Before — event exactly at Before is EXCLUDED (timestamp < ?).
//     Note: ducklake_only_test.go tests Before with a non-boundary
//     timestamp. The strict exclusive-boundary case is pinned HERE:
//       • THIS FILE: TestBeforeBoundaryIsStrict
//
//  4. VOIDING — tombstone and its voided target are BOTH absent from
//     list/latest/type-summaries.
//     Covered by:
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_VoidingExcludes
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_ListIndexesAdvanced
//       • tests/ducklake_only_test.go:TestDuckLakeOnly_VoidingExcludesEvents
//
//  5. FILTER NARROWING — each field filter is exercised:
//
//     Type IN + NotIn:
//       • internal/service/duck/lake_fetch_test.go:TestStringNotIn
//
//     Source IN:
//       • THIS FILE: TestSourceINFilter
//
//     Producer IN:
//       • THIS FILE: TestProducerINFilter
//
//     ID IN (via GetCloudEventFromIndex re-fetch path):
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_GetCloudEventFromIndex
//
//     DataVersion IN:
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_DataVersionFilter
//
//     Extras IN + NotIn:
//       • internal/service/duck/lake_fetch_test.go:TestExtrasFilter
//
//  6. TAG FILTERS — all four operators:
//
//     ContainsAny:
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_TagsFilter
//
//     ContainsAll:
//       • internal/service/duck/lake_fetch_test.go:TestTagsContainsAll
//
//     NotContainsAny:
//       • internal/service/duck/lake_fetch_test.go:TestTagsNotContainsAny
//
//     NotContainsAll:
//       • THIS FILE: TestTagsNotContainsAll
//
//  7. TYPE SUMMARIES — per-type count/min(time)/max(time), voided excluded.
//     Covered by:
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_GetCloudEventTypeSummariesAdvanced
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_VoidingExcludes
//
//  8. DEDUP — duplicate header-key rows collapse to one result.
//     Covered by:
//       • internal/service/duck/lake_fetch_test.go:TestLakeEventService_DedupOnKey
//
// ────────────────────────────────────────────────────────────────────────────
package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// insertParityRawEvent is a minimal raw_events inserter for parity tests,
// supporting explicit source and producer columns.
func insertParityRawEvent(t *testing.T, duckSvc *duck.Service, id, subject, evType, source, producer, extras string, ts time.Time) {
	t.Helper()
	if extras == "" {
		extras = "{}"
	}
	_, err := duckSvc.DB().ExecContext(context.Background(),
		`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer,
		 data_content_type, data_version, extras, data, voids_id)
		 VALUES (?, ?, ?, ?, ?, ?, '', '', ?, '{}', '')`,
		subject, ts.UTC(), evType, id, source, producer, extras)
	require.NoError(t, err)
}

// tagsExtras builds the extras JSON string for a tags list.
func tagsExtras(tags []string) string {
	quoted := make([]string, len(tags))
	for i, t := range tags {
		quoted[i] = fmt.Sprintf("%q", t)
	}
	return `{"tags":[` + strings.Join(quoted, ",") + `]}`
}

// TestBeforeBoundaryIsStrict pins CH parity rule 3: an event whose timestamp
// equals filter.Before is EXCLUDED (CH uses timestamp < ?, strict less-than).
//
// CH rule (eventrepo/service.go):
//
//	qm.Where(chindexer.TimestampColumn+" < ?", opts.GetBefore().AsTime())
//
// Lake rule (internal/service/duck/raw.go):
//
//	col("time")+" < ?"
func TestBeforeBoundaryIsStrict(t *testing.T) {
	ctx := context.Background()
	lsvc, duckSvc := newLakeFetchService(t)

	// Use a unique subject to avoid interference with other tests in the package.
	pSubj := fmt.Sprintf("did:erc721:137:%s:300", vehicleNFT.Hex())
	boundary := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Hour)

	// ev-at-boundary: time == Before → must NOT appear (strict <).
	insertParityRawEvent(t, duckSvc, "before-at-boundary", pSubj, "dimo.status",
		"src", "prod", "", boundary)
	// ev-strictly-before: time is 1ms before boundary → must appear.
	insertParityRawEvent(t, duckSvc, "before-strictly-before", pSubj, "dimo.status",
		"src", "prod", "", boundary.Add(-time.Millisecond))

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{pSubj}},
		Before:  timestamppb.New(boundary),
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1,
		"event exactly at Before boundary must be excluded (strict <, CH parity)")
	assert.Equal(t, "before-strictly-before", indexes[0].ID)
}

// TestSourceINFilter pins CH parity rule 5 (source IN): filtering by source
// narrows results to only matching events.
//
// CH rule: addIn on source column via AdvancedSearchOptionsToQueryMod.
// Lake rule: whereClauseQ → addIn("source", filter.Sources) (raw.go).
func TestSourceINFilter(t *testing.T) {
	ctx := context.Background()
	lsvc, duckSvc := newLakeFetchService(t)

	srcSubj := fmt.Sprintf("did:erc721:137:%s:301", vehicleNFT.Hex())
	now := time.Now().UTC()

	insertParityRawEvent(t, duckSvc, "src-ev-a", srcSubj, "dimo.status",
		"source-alpha", "prod", "", now.Add(-2*time.Hour))
	insertParityRawEvent(t, duckSvc, "src-ev-b", srcSubj, "dimo.status",
		"source-beta", "prod", "", now.Add(-time.Hour))

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{srcSubj}},
		Source:  &grpc.StringFilterOption{In: []string{"source-alpha"}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "Source IN must narrow to only source-alpha events")
	assert.Equal(t, "src-ev-a", indexes[0].ID)
}

// TestProducerINFilter pins CH parity rule 5 (producer IN): filtering by
// producer narrows results to only matching events.
//
// CH rule: addIn on producer column via AdvancedSearchOptionsToQueryMod.
// Lake rule: whereClauseQ → addIn("producer", filter.Producers) (raw.go).
func TestProducerINFilter(t *testing.T) {
	ctx := context.Background()
	lsvc, duckSvc := newLakeFetchService(t)

	prodSubj := fmt.Sprintf("did:erc721:137:%s:302", vehicleNFT.Hex())
	now := time.Now().UTC()

	insertParityRawEvent(t, duckSvc, "prod-ev-a", prodSubj, "dimo.status",
		"src", "prod-foo", "", now.Add(-2*time.Hour))
	insertParityRawEvent(t, duckSvc, "prod-ev-b", prodSubj, "dimo.status",
		"src", "prod-bar", "", now.Add(-time.Hour))

	opts := &grpc.AdvancedSearchOptions{
		Subject:  &grpc.StringFilterOption{In: []string{prodSubj}},
		Producer: &grpc.StringFilterOption{In: []string{"prod-foo"}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "Producer IN must narrow to only prod-foo events")
	assert.Equal(t, "prod-ev-a", indexes[0].ID)
}

// TestTagsNotContainsAll pins CH parity rule 6 (tags NotContainsAll): events
// that contain ALL of the specified tags are EXCLUDED; events missing at least
// one of the tags pass through.
//
// CH rule: NOT has(tags, ?) for each tag in NotContainsAll (ANDed).
// Lake rule: whereClauseQ → NOT list_has_all(tags_expr, [...]) (raw.go).
func TestTagsNotContainsAll(t *testing.T) {
	ctx := context.Background()
	lsvc, duckSvc := newLakeFetchService(t)

	ncaSubj := fmt.Sprintf("did:erc721:137:%s:303", vehicleNFT.Hex())
	now := time.Now().UTC()

	// ev-both: has "trip" AND "safety" → excluded by NotContainsAll(trip, safety).
	insertParityRawEvent(t, duckSvc, "ncall-both", ncaSubj, "dimo.status",
		"src", "prod", tagsExtras([]string{"trip", "safety"}), now.Add(-3*time.Hour))
	// ev-one: has only "trip" → missing "safety", so NOT(has_all) passes through.
	insertParityRawEvent(t, duckSvc, "ncall-one", ncaSubj, "dimo.status",
		"src", "prod", tagsExtras([]string{"trip"}), now.Add(-2*time.Hour))
	// ev-none: no tags → passes through.
	insertParityRawEvent(t, duckSvc, "ncall-none", ncaSubj, "dimo.status",
		"src", "prod", "{}", now.Add(-time.Hour))

	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{ncaSubj}},
		Tags:    &grpc.ArrayFilterOption{NotContainsAll: []string{"trip", "safety"}},
	}
	indexes, err := lsvc.ListIndexesAdvanced(ctx, 10, opts)
	require.NoError(t, err)
	require.Len(t, indexes, 2,
		"NotContainsAll must exclude only events containing ALL specified tags")
	ids := make([]string, len(indexes))
	for i, idx := range indexes {
		ids[i] = idx.ID
	}
	assert.ElementsMatch(t, []string{"ncall-one", "ncall-none"}, ids)
}
