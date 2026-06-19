// ducklake_partition_test.go proves the decoded tables carry the subject_bucket
// partition column the read path prunes on. Without it every per-vehicle query
// full-scans the fleet (CHD-1): raw_events is partitioned/bloomed, the decoded
// tables were not.
package tests

import (
	"context"
	"fmt"
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
