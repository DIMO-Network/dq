package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestDuckLake_RunOnceWithoutRawEvents pins Item 6 (S8): dq can boot before din
// has created lake.raw_events against a fresh catalog. RunOnce must treat the
// missing source table as caught-up (0, nil) — repeatedly, without error — so the
// hourly failure backstop never trips and crash-loops the pod.
func TestDuckLake_RunOnceWithoutRawEvents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      dir + "/catalog.ducklake",
		DataPath:        dir + "/lakedata",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	// Deliberately do NOT create lake.raw_events — din has not booted yet.
	mat, err := materializer.NewDuckLakeMaterializer(ctx, svc.DB(), zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{}, zerolog.Nop()).WithDuckLake(mat)

	for i := 0; i < 3; i++ {
		n, err := runner.RunOnce(ctx)
		require.NoError(t, err, "missing raw_events must not error (pre-din boot)")
		require.Zero(t, n, "no raw_events → caught up")
	}
}

// TestDuckLake_CursorRowReseeds pins Item 9 (M6): if the seeded ingest_progress
// cursor row disappears (catalog restore), cursor() must self-heal by re-seeding
// it and resuming from 0 rather than making every CAS match nothing and looking
// caught-up forever. The re-decode from 0 is idempotent (insert anti-join).
func TestDuckLake_CursorRowReseeds(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:11", vehicleNFT.Hex())
	ts := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	seedRawStatus(t, db, "rs-1", subject, ts, speedAt(ts, 42))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat)

	require.Equal(t, 1, drainRunner(t, ctx, runner), "the seeded event is decoded")

	// The cursor row vanishes (catalog restore / truncate).
	_, err = db.ExecContext(ctx, "DELETE FROM lake.ingest_progress WHERE partition = 'lake.raw_events#snapshot'")
	require.NoError(t, err)

	// RunOnce must self-heal (re-seed) instead of erroring or silently wedging.
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)

	var seedCount int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.ingest_progress WHERE partition = 'lake.raw_events#snapshot'").Scan(&seedCount))
	require.Equal(t, 1, seedCount, "cursor row re-seeded")

	// Re-decode from 0 is idempotent: the anti-join keeps signals at one row.
	var rows int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	require.Equal(t, 1, rows, "re-decode from 0 does not duplicate the signal")
}
