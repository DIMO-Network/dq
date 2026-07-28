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

// atScanAndBelow returns the plan text from the deepest scan operator down —
// i.e. the scan's own Projections/Filters block. A predicate that reached the
// scan is listed there (unless DuckLake partition pruning consumed it outright,
// which only happens for partition columns).
func atScanAndBelow(t *testing.T, plan string) string {
	t.Helper()
	idx := strings.LastIndex(plan, "DUCKLAKE_SCAN")
	for _, alt := range []string{"SEQ_SCAN", "TABLE_SCAN"} {
		if i := strings.LastIndex(plan, alt); i > idx {
			idx = i
		}
	}
	require.GreaterOrEqual(t, idx, 0, "no scan operator in plan:\n%s", plan)
	return plan[idx:]
}

// TestDuckLake_DedupedSourcePushesRequestedNamesToScan pins the aggregation
// path's name restriction, the `name` analogue of B1. The range aggregations
// identify the requested signals by INNER JOIN against an inline VALUES table
// (aggValuesTable), and a join is not a filter: on its own it cannot reach the
// scan, so the dedup window ends up sorting every signal the vehicle ever
// reported and the join discards all but the requested few. Passing the names to
// LakeSignalsDeduped puts them in the subquery's WHERE, where they do reach the
// scan. The negative control is the pre-PR shape — same query, names only in the
// join — and must NOT show the name at the scan; if a DuckDB upgrade ever pushes
// join conditions through, this control fails and the pushdown becomes redundant
// rather than silently unnecessary.
func TestDuckLake_DedupedSourcePushesRequestedNamesToScan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()

	subject := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	seedRawStatus(t, db, "pn-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 50))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	// A second signal name, so restricting to 'speed' is a restriction at all.
	_, err = db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO lake.signals (subject, subject_bucket, name, timestamp,
		source, producer, cloud_event_id, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading)
		VALUES ('%s', %d, 'obdEngineLoad', TIMESTAMPTZ '%s', 'src', 'prod', 'ce-other', 1.0, '', 0, 0, 0, 0)`,
		subject, duck.HashBucket(subject), day.Add(time.Hour).Format("2006-01-02 15:04:05")))
	require.NoError(t, err)

	// The production aggregation FROM shape: deduped source JOINed to the
	// requested-name VALUES table.
	aggShape := func(src string) string {
		return "SELECT s.name, max(s.timestamp) FROM " + src +
			" AS s JOIN (VALUES ('speed')) AS agg_table(name) ON s.name = agg_table.name" +
			" WHERE s.subject = ? GROUP BY s.name"
	}

	plan := explainPlan(t, ctx, db, aggShape(duck.LakeSignalsDeduped(subject, "", "speed")), subject)
	assert.Contains(t, atScanAndBelow(t, plan), "speed",
		"requested name did not reach the DuckLake scan — the dedup window is still reading every signal:\n%s", plan)

	// Negative control: the pre-PR shape, names carried by the join alone.
	plan = explainPlan(t, ctx, db, aggShape(duck.LakeSignalsDeduped(subject, "")), subject)
	assert.NotContains(t, atScanAndBelow(t, plan), "speed",
		"join-only shape now reaches the scan — re-evaluate whether the name pushdown is still needed:\n%s", plan)
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
