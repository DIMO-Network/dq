// ducklake_rollup_test.go validates the latest/summary rollup (CHD-3): the
// materializer maintains lake.signals_latest per batch, and the query layer
// serves GetAllLatestSignals / GetSignalSummaries / GetAvailableSignals from it
// (no source filter). The asserted values are the correct full-history result,
// so this confirms the rollup is a faithful materialized view, not a shortcut.
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

func TestDuckLake_LatestSummaryRollup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:3", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)

	// Three speed readings across three snapshots; latest is 65 at +3h.
	seedRawStatus(t, db, "rl-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 40))
	seedRawStatus(t, db, "rl-2", subject, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 80))
	seedRawStatus(t, db, "rl-3", subject, day.Add(3*time.Hour), speedAt(day.Add(3*time.Hour), 65))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 3, drainRunner(t, ctx, runner))

	q := duck.NewLakeQueries(svc)

	// Latest (rollup-served): speed is its newest value.
	latest, err := q.GetAllLatestSignals(ctx, subject, nil)
	require.NoError(t, err)
	var speed float64
	var found bool
	for _, s := range latest {
		if s.Data.Name == "speed" {
			speed, found = s.Data.ValueNumber, true
		}
	}
	require.True(t, found, "speed present in latest")
	assert.Equal(t, 65.0, speed, "latest speed is the newest reading")

	// Summary (rollup-served): one count per reading.
	sums, err := q.GetSignalSummaries(ctx, subject, nil)
	require.NoError(t, err)
	var count uint64
	for _, s := range sums {
		if s.Name == "speed" {
			count = uint64(s.NumberOfSignals)
		}
	}
	assert.EqualValues(t, 3, count, "summary counts all three readings")

	// Available signals (rollup-served) include speed.
	avail, err := q.GetAvailableSignals(ctx, subject, nil)
	require.NoError(t, err)
	assert.Contains(t, avail, "speed")

	// Incremental: a fourth, newer reading updates the rollup latest to 90.
	seedRawStatus(t, db, "rl-4", subject, day.Add(4*time.Hour), speedAt(day.Add(4*time.Hour), 90))
	require.Equal(t, 1, drainRunner(t, ctx, runner))
	latest2, err := q.GetAllLatestSignals(ctx, subject, nil)
	require.NoError(t, err)
	for _, s := range latest2 {
		if s.Data.Name == "speed" {
			assert.Equal(t, 90.0, s.Data.ValueNumber, "rollup updated to the newest reading")
		}
	}
}
