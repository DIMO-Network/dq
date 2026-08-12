// lake_typesummary_rollup_test.go pins the dq#40 routing rules for
// GetCloudEventTypeSummariesAdvanced: subject/type-only filters serve from the
// lake.raw_types_latest rollup, narrower filters (source, time bounds, …) and
// an empty rollup result fall back to the live scan. The rollup fixtures here
// deliberately DISAGREE with the base table so every assertion proves which
// path answered.
package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// createRawTypesRollupForTest creates lake.raw_types_latest the way the
// materializer's ensureSchema does (the query pod never creates it).
func createRawTypesRollupForTest(t *testing.T, svc *Service) {
	t.Helper()
	_, err := svc.db.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS lake.raw_types_latest (
		subject VARCHAR, type VARCHAR, count BIGINT,
		first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`)
	require.NoError(t, err)
}

func insertRawTypeRow(t *testing.T, svc *Service, subject, ceType string, count int64, first, last time.Time) {
	t.Helper()
	_, err := svc.db.ExecContext(context.Background(),
		"INSERT INTO lake.raw_types_latest (subject, type, count, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
		subject, ceType, count, first.UTC(), last.UTC())
	require.NoError(t, err)
}

const lakeRawSubj2 = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:202"

func TestTypeSummary_RollupServesSubjectAndTypeFilters(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	// Base table: ONE dimo.status event. The rollup below claims different
	// numbers, so a count of 8 can only have come from the rollup.
	insertRawEvent(t, svc, mkStoredEvent("rt-1", "dimo.status", lakeRawSubj, now))

	createRawTypesRollupForTest(t, svc)
	insertRawTypeRow(t, svc, lakeRawSubj, "dimo.status", 8, now.Add(-48*time.Hour), now)
	insertRawTypeRow(t, svc, lakeRawSubj, "dimo.attestation", 2, now.Add(-24*time.Hour), now.Add(-time.Hour))
	insertRawTypeRow(t, svc, lakeRawSubj2, "dimo.status", 5, now.Add(-72*time.Hour), now.Add(-2*time.Hour))

	// Subject-only filter: both of the subject's types, from the rollup.
	sums, err := lsvc.GetCloudEventTypeSummariesAdvanced(ctx, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	})
	require.NoError(t, err)
	require.Len(t, sums, 2)
	assert.Equal(t, "dimo.attestation", sums[0].Type)
	assert.Equal(t, uint64(2), sums[0].Count)
	assert.Equal(t, "dimo.status", sums[1].Type)
	assert.Equal(t, uint64(8), sums[1].Count, "count 8 exists only in the rollup — the scan would say 1")
	assert.True(t, sums[1].FirstSeen.Equal(now.Add(-48*time.Hour)))
	assert.True(t, sums[1].LastSeen.Equal(now))

	// Subject + type filter is still rollup-eligible.
	sums, err = lsvc.GetCloudEventTypeSummariesAdvanced(ctx, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
		Type:    &grpc.StringFilterOption{In: []string{"dimo.status"}},
	})
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.Equal(t, uint64(8), sums[0].Count)

	// Type NotIn is rollup-eligible too.
	sums, err = lsvc.GetCloudEventTypeSummariesAdvanced(ctx, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
		Type:    &grpc.StringFilterOption{NotIn: []string{"dimo.status"}},
	})
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.Equal(t, "dimo.attestation", sums[0].Type)

	// No filter at all: per-type re-aggregation across subjects — counts sum,
	// first_seen/last_seen span both subjects' rows.
	sums, err = lsvc.GetCloudEventTypeSummariesAdvanced(ctx, nil)
	require.NoError(t, err)
	require.Len(t, sums, 2)
	assert.Equal(t, "dimo.status", sums[1].Type)
	assert.Equal(t, uint64(13), sums[1].Count, "8 + 5 across the two subjects")
	assert.True(t, sums[1].FirstSeen.Equal(now.Add(-72*time.Hour)), "first_seen is the min across subjects")
	assert.True(t, sums[1].LastSeen.Equal(now), "last_seen is the max across subjects")
}

func TestTypeSummary_ScanWhenFilterNarrower(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	insertRawEvent(t, svc, mkStoredEvent("rt-2", "dimo.status", lakeRawSubj, now))
	createRawTypesRollupForTest(t, svc)
	// A rollup row that would be WRONG for the narrowed filters below.
	insertRawTypeRow(t, svc, lakeRawSubj, "dimo.status", 8, now.Add(-48*time.Hour), now)

	// Source filter: not representable in the rollup → live scan (count 1, the
	// base truth). mkStoredEvent stamps source "src-test".
	sums, err := lsvc.GetCloudEventTypeSummariesAdvanced(ctx, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
		Source:  &grpc.StringFilterOption{In: []string{"src-test"}},
	})
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.Equal(t, uint64(1), sums[0].Count, "a source filter must scan, not serve the rollup's 8")

	// A time bound must scan too: the rollup's first/last are all-time.
	sums, err = lsvc.GetCloudEventTypeSummariesAdvanced(ctx, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
		After:   timestamppb.New(now.Add(-time.Hour)),
	})
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.Equal(t, uint64(1), sums[0].Count, "a time bound must scan, not serve the rollup's 8")
}

// TestTypeSummary_FallsBackWhenRollupEmpty pins the rollout/first-create window:
// the table exists but has no rows yet (created, first rebuild pending — or a
// brand-new subject inside the rebuild interval). Serving the empty rollup
// would transiently report "no event types" for a vehicle that has them.
func TestTypeSummary_FallsBackWhenRollupEmpty(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Second)

	insertRawEvent(t, svc, mkStoredEvent("rt-3", "dimo.status", lakeRawSubj, now))
	createRawTypesRollupForTest(t, svc)

	sums, err := lsvc.GetCloudEventTypeSummariesAdvanced(ctx, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	})
	require.NoError(t, err)
	require.Len(t, sums, 1, "empty rollup must fall back to the scan")
	assert.Equal(t, "dimo.status", sums[0].Type)
	assert.Equal(t, uint64(1), sums[0].Count)
}
