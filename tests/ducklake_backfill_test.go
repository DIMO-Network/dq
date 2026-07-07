// ducklake_backfill_test.go proves the re-decode/backfill tool (finding #1a): the
// counterpart to a cursor-expiry skip (cursorResetsTotal), it re-decodes a raw_events
// time range DIRECTLY from the base table (not the expired change feed) into the
// decoded tables, idempotently. Re-running must not double-insert, and it must not
// move the ingest_progress cursor (it is out-of-band repair, not normal decode).
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readCursor returns the materializer's ingest_progress snapshot cursor (0 before any
// decode). Used to prove the backfill tool never advances it.
func readCursor(t *testing.T, ctx context.Context, db *sql.DB) int64 {
	t.Helper()
	var cursor int64
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT CAST(cursor AS BIGINT) FROM lake.ingest_progress WHERE partition = 'lake.raw_events#snapshot'").Scan(&cursor))
	return cursor
}

func TestDuckLake_BackfillTimeRange_Idempotent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:21", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)

	// Three status readings in the window. They are NEVER run through the normal decode
	// loop — the backfill tool alone must land them in lake.signals (the cursor-expiry
	// scenario where the loop skipped this range).
	seedRawStatus(t, db, "bf-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 40))
	seedRawStatus(t, db, "bf-2", subject, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 80))
	seedRawStatus(t, db, "bf-3", subject, day.Add(3*time.Hour), speedAt(day.Add(3*time.Hour), 65))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	from, to := day, day.Add(24*time.Hour)

	// Cursor starts at 0 (nothing decoded by the loop).
	cursorBefore := readCursor(t, ctx, db)
	require.EqualValues(t, 0, cursorBefore)

	// First backfill: decodes the range into lake.signals.
	n, err := mat.BackfillTimeRange(ctx, runner, from, to)
	require.NoError(t, err)
	assert.Equal(t, 3, n, "backfill decoded all three raw events")

	var rows int
	var maxSpeed float64
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*), max(value_number) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).
		Scan(&rows, &maxSpeed))
	assert.Equal(t, 3, rows, "backfill landed the decoded rows")
	assert.Equal(t, 80.0, maxSpeed)

	// The backfill must NOT have advanced the decode cursor (out-of-band repair).
	assert.EqualValues(t, 0, readCursor(t, ctx, db), "backfill must not move the ingest_progress cursor")

	// Idempotent: a second backfill of the same range decodes the rows again but the
	// anti-join collapses them at rest — no double-insert.
	n2, err := mat.BackfillTimeRange(ctx, runner, from, to)
	require.NoError(t, err)
	assert.Equal(t, 3, n2, "re-decode still reads the same three raw events")
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, 3, rows, "re-running the backfill must not double-insert (idempotent)")

	// The backfilled subject's rollup refreshes on flush.
	require.NoError(t, runner.FlushRollup(ctx))
	var rollupCount int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count FROM lake.signals_latest WHERE subject = ? AND name = 'speed'", subject).Scan(&rollupCount))
	assert.Equal(t, 3, rollupCount, "rollup count reflects the deduped backfilled rows")

	// Empty / inverted ranges are rejected.
	_, err = mat.BackfillTimeRange(ctx, runner, to, from)
	require.Error(t, err, "inverted range must error")

	// A range with no raw events is a clean no-op.
	future := time.Now().UTC().Add(48 * time.Hour)
	n3, err := mat.BackfillTimeRange(ctx, runner, future, future.Add(time.Hour))
	require.NoError(t, err)
	assert.Zero(t, n3, "empty range decodes nothing")
}

// TestDuckLake_BackfillRecoversSkippedRange proves the end-to-end recovery: the normal
// loop decodes an early range, a LATER range is left undecoded (as a cursor-expiry skip
// would leave it), and the backfill tool recovers exactly the skipped rows without
// disturbing the loop's cursor.
func TestDuckLake_BackfillRecoversSkippedRange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:23", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -5).Truncate(24 * time.Hour)

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	// The loop decodes the "early" reading normally.
	early := day.Add(time.Hour)
	seedRawStatus(t, db, "sk-early", subject, early, speedAt(early, 30))
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	// A "skipped" reading a day later that the loop never sees (simulating the expiry
	// gap: the operator seeds it, but decode has moved past / expired the range).
	skipped := day.Add(25 * time.Hour)
	seedRawStatus(t, db, "sk-gap", subject, skipped, speedAt(skipped, 99))

	cursorAfterLoop := readCursor(t, ctx, db)

	// Only the early reading is decoded so far.
	var rows int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	require.Equal(t, 1, rows)

	// Backfill exactly the skipped day.
	n, err := mat.BackfillTimeRange(ctx, runner, day.Add(24*time.Hour), day.Add(48*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "backfill recovered the skipped reading")

	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&rows))
	assert.Equal(t, 2, rows, "both the loop-decoded and backfilled readings are present")
	assert.Equal(t, cursorAfterLoop, readCursor(t, ctx, db), "backfill left the loop cursor untouched")
}
