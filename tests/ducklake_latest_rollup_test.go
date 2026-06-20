// ducklake_latest_rollup_test.go covers SR-5: GetLatestSignals (named signals,
// no source filter, non-location) is served from the lake.signals_latest rollup
// in O(distinct-names) rather than a full-history dedup scan. Proven by pruning
// the base table (as retention does — the rollup is kept) and asserting the
// named-latest query still returns the value: only a rollup read can.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_GetLatestSignals_ServedFromRollup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)

	seedRawStatus(t, db, "lr-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 40))
	seedRawStatus(t, db, "lr-2", subject, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 65))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, nil, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	// Drop the base history; the rollup is current state and stays (this is
	// exactly what PruneDecoded does at the retention boundary).
	_, err = db.ExecContext(ctx, "DELETE FROM lake.signals")
	require.NoError(t, err)

	q := duck.NewLakeQueries(svc)
	got, err := q.GetLatestSignals(ctx, subject, &model.LatestSignalsArgs{
		SignalNames: map[string]struct{}{"speed": {}},
	})
	require.NoError(t, err)

	var speed float64
	var found bool
	for _, s := range got {
		if s.Data.Name == "speed" {
			speed, found = s.Data.ValueNumber, true
		}
	}
	require.True(t, found, "speed returned from the rollup after base prune")
	assert.Equal(t, 65.0, speed, "latest speed is the newest reading, read from the rollup")
}
