// ducklake_partition_test.go proves the decoded tables carry the subject_bucket
// partition column the read path prunes on. Without it every per-vehicle query
// full-scans the fleet (CHD-1): raw_events is partitioned/bloomed, the decoded
// tables were not.
package tests

import (
	"context"
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
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, nil, zerolog.Nop()).
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
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, nil, zerolog.Nop()).
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
