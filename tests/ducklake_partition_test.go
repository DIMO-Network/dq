// ducklake_partition_test.go proves the decoded tables carry the subject_bucket
// partition column the read path prunes on. Without it every per-vehicle query
// full-scans the fleet (CHD-1): raw_events is partitioned/bloomed, the decoded
// tables were not.
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_DecodedRowsCarrySubjectBucket(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	ts := time.Now().UTC().AddDate(0, 0, -1).Truncate(time.Hour)
	seedRawStatus(t, db, "p-1", subject, ts, speedAt(ts, 50))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	// The materializer stamps each decoded row with the same hash bucket the
	// query layer computes from the subject, so reads can prune to one bucket.
	var bucket int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT DISTINCT subject_bucket FROM lake.signals WHERE subject = ?", subject).Scan(&bucket))
	assert.Equal(t, duck.HashBucket(subject), bucket)
}

// TestDuckLake_SubjectBucketPredicateIsPushedToScan proves the read-side
// subject_bucket predicate actually reaches the DuckLake scan (the prerequisite
// for partition pruning), not just that the column is stamped. Without pushdown
// every per-vehicle query full-scans the fleet — the whole point of CHD-1/SR-6.
func TestDuckLake_SubjectBucketPredicateIsPushedToScan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()

	// Two subjects in two different hash buckets, on two days.
	subjA := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	subjB := fmt.Sprintf("did:erc721:137:%s:43", vehicleNFT.Hex())
	require.NotEqual(t, duck.HashBucket(subjA), duck.HashBucket(subjB), "test needs two distinct buckets")
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	seedRawStatus(t, db, "pa-1", subjA, day.Add(time.Hour), speedAt(day.Add(time.Hour), 50))
	seedRawStatus(t, db, "pb-1", subjB, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 70))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	// The DuckLake scan must carry the subject_bucket filter (partition pushdown),
	// so subject A's bucket value appears in the plan as a scan-level predicate.
	bucketA := duck.HashBucket(subjA)
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("EXPLAIN SELECT count(*) FROM lake.signals WHERE subject_bucket = %d", bucketA))
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var plan strings.Builder
	for rows.Next() {
		var a, b string
		require.NoError(t, rows.Scan(&a, &b))
		plan.WriteString(b)
		plan.WriteString("\n")
	}
	require.NoError(t, rows.Err())
	assert.Contains(t, plan.String(), "subject_bucket",
		"EXPLAIN plan must show the subject_bucket filter at the scan (partition pruning enabled)")
}

// explainPlan returns the concatenated EXPLAIN output for stmt.
func explainPlan(t *testing.T, ctx context.Context, db *sql.DB, stmt string, args ...any) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "EXPLAIN "+stmt, args...)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var plan strings.Builder
	for rows.Next() {
		var a, b string
		require.NoError(t, rows.Scan(&a, &b))
		plan.WriteString(b)
		plan.WriteString("\n")
	}
	require.NoError(t, rows.Err())
	return plan.String()
}

// aboveScan returns the plan text ABOVE the deepest scan operator. A predicate
// that reaches the scan is either consumed by DuckLake partition pruning
// (vanishing from the plan entirely) or listed in the scan's Filters — either
// way it does not appear above the scan. A predicate stuck in a FILTER above
// the dedup window (the B1 regression) does.
func aboveScan(t *testing.T, plan string) string {
	t.Helper()
	idx := strings.LastIndex(plan, "DUCKLAKE_SCAN")
	for _, alt := range []string{"SEQ_SCAN", "TABLE_SCAN"} {
		if i := strings.LastIndex(plan, alt); i > idx {
			idx = i
		}
	}
	require.GreaterOrEqual(t, idx, 0, "no scan operator in plan:\n%s", plan)
	return plan[:idx]
}

// TestDuckLake_DedupedSourcesPushBucketToScan pins B1: the canonical deduped
// sources (duck.LakeSignalsDeduped / duck.LakeEventsDeduped) — the FROM shape
// of every aggregation/latest/summary/events query — must deliver the
// subject_bucket predicate to the DuckLake scan. DuckDB only pushes filters on
// the dedup window's PARTITION BY columns below the WINDOW operator;
// subject_bucket is not one, so it prunes only when written INSIDE the dedup
// subquery. A negative control proves this test can tell the difference: the
// regression shape (bucket filter outside the subquery) must park the filter
// above the window, NOT at the scan.
func TestDuckLake_DedupedSourcesPushBucketToScan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()

	subject := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	seedRawStatus(t, db, "pd-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 50))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	// Production latest/summary shape over the canonical signal source: the
	// bucket predicate must be consumed at/below the scan (pruning), never
	// parked in a FILTER above the dedup window.
	plan := explainPlan(t, ctx, db,
		"SELECT name, max(timestamp) FROM "+duck.LakeSignalsDeduped(subject, "")+" WHERE subject = ? GROUP BY name", subject)
	assert.NotContains(t, aboveScan(t, plan), "subject_bucket",
		"signals dedup source: subject_bucket stuck above the scan (B1 regression):\n%s", plan)

	// Production events shape over the canonical event source.
	plan = explainPlan(t, ctx, db,
		"SELECT name, count(*) FROM "+duck.LakeEventsDeduped(subject)+" WHERE subject = ? GROUP BY name", subject)
	assert.NotContains(t, aboveScan(t, plan), "subject_bucket",
		"events dedup source: subject_bucket stuck above the scan (B1 regression):\n%s", plan)

	// Negative control — the B1 regression shape: dedup subquery WITHOUT the
	// bucket predicate, bucket filtered outside. The filter must appear ABOVE
	// the scan (stuck at the window boundary). If a DuckDB upgrade ever pushes
	// it through, this control fails and the test's premise must be
	// re-evaluated — better than silently proving nothing.
	regression := `(SELECT * FROM lake.signals QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, name, timestamp ORDER BY cloud_event_id) = 1)`
	plan = explainPlan(t, ctx, db,
		fmt.Sprintf("SELECT name, max(timestamp) FROM %s WHERE subject = ? AND subject_bucket = %d GROUP BY name",
			regression, duck.HashBucket(subject)), subject)
	assert.Contains(t, aboveScan(t, plan), "subject_bucket",
		"negative control: outer bucket filter should be stuck above the scan — re-evaluate this test's premise if not:\n%s", plan)
}
