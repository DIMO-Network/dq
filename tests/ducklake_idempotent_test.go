// ducklake_idempotent_test.go proves decoded writes are idempotent at rest. The
// pipeline is at-least-once at every seam (device retry, sink redelivery), so
// the same cloudevent can land in raw_events more than once across snapshots.
// Without an at-rest identity guard the decoder writes the same signal twice
// (CHD-7), inflating stored rows and every aggregate that reads them.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_IdempotentDecodeAcrossSnapshots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())
	ts := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	// First delivery of cloudevent "dup-1".
	seedRawStatus(t, db, "dup-1", subject, ts, speedAt(ts, 70))
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	// The same cloudevent id is redelivered as a new snapshot (at-least-once).
	seedRawStatus(t, db, "dup-1", subject, ts, speedAt(ts, 70))
	drainRunner(t, ctx, runner)

	var rows int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, 1, rows, "a redelivered cloudevent decodes to exactly one row at rest")
}
