// ducklake_span_test.go proves the materializer drains a multi-snapshot backlog
// in memory-bounded chunks (maxSnapshotSpan) rather than materializing the whole
// (cursor, head] delta at once — the OOM/crash-loop guard for a lagged consumer.
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

func TestDuckLake_SpanBoundedDrain(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())
	ts := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithMaxSnapshotSpan(1) // one raw_events snapshot of data per pass
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	// Three distinct raw_events, each committed as its own snapshot.
	ids := []string{"span-1", "span-2", "span-3"}
	for i, id := range ids {
		at := ts.Add(time.Duration(i) * time.Minute)
		seedRawStatus(t, db, id, subject, at, speedAt(at, 70))
	}

	// A single pass consumes exactly one snapshot's raw event (the span bound),
	// skipping any empty schema snapshots in between but never the whole backlog.
	first, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, first, "maxSnapshotSpan=1 bounds a pass to one snapshot's raw events")

	// Draining the remainder lands every row — chunking loses nothing across the
	// cursor advances.
	drainRunner(t, ctx, runner)
	var rows int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, 3, rows, "all snapshots drain across span-bounded passes")
}
