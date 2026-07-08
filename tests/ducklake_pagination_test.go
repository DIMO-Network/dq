// ducklake_pagination_test.go proves finding #1c: a single oversized snapshot is
// decoded and written in byte/row-bounded WINDOWS (so its resident working set can't
// OOM the single writer), while preserving exactly-once — including a crash between an
// intermediate window and the final cursor-advancing commit, which the old monolithic
// "read whole delta, one commit" path could never produce.
package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedRawStatusOneSnapshot inserts n status rows for subject in a SINGLE transaction,
// so DuckLake commits them as ONE snapshot. The span bound (maxSnapshotSpan) cannot
// split a single snapshot, so a fat snapshot must be paged by row/byte budget or it
// materializes whole.
func seedRawStatusOneSnapshot(t *testing.T, db *sql.DB, subject string, base time.Time, n int) {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		at := base.Add(time.Duration(i) * time.Second)
		ev := deviceStatus(fmt.Sprintf("snap-%s-%d", subject, i), subject, at, speedAt(at, float64(i)))
		_, err := tx.Exec(
			`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data)
			 VALUES (?, ?, ?, ?, ?, ?, '', ?, '{}', ?)`,
			ev.Subject, ev.Time.UTC(), ev.Type, ev.ID, ev.Source, ev.Producer, ev.DataVersion, string(ev.Data))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func countSpeedRows(t *testing.T, ctx context.Context, db *sql.DB, subject string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.signals WHERE subject = ? AND name = 'speed'", subject).Scan(&n))
	return n
}

func TestDuckLake_PaginatesFatSnapshotExactlyOnce(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:91", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -1).Truncate(time.Hour)

	seedRawStatusOneSnapshot(t, db, subject, base, 6) // ONE snapshot, six rows

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithMaxRowsPerWindow(2) // force pagination: 6 rows over 2-row windows

	var intermediateWindows int
	mat.WithWindowCommitHook(func(idx int) error { intermediateWindows++; return nil })

	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	n, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, n, "all six rows of the fat snapshot decoded")
	assert.Equal(t, 6, countSpeedRows(t, ctx, db, subject), "exactly six distinct rows landed (no dup, no loss)")
	assert.Positive(t, intermediateWindows, "the fat snapshot was written in multiple bounded windows, not one")
	assert.Positive(t, readCursor(t, ctx, db), "cursor advanced past the snapshot")

	// Idempotent: a re-run with nothing new must not double-insert.
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, countSpeedRows(t, ctx, db, subject), "re-run does not double-insert")
}

func TestDuckLake_PaginationCrashMidSpanRecoversExactlyOnce(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:92", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -1).Truncate(time.Hour)
	seedRawStatusOneSnapshot(t, db, subject, base, 6)

	// mat1 crashes right after the first intermediate window commits, before the final
	// cursor-advancing commit — the mid-snapshot crash the monolithic path (one commit)
	// could never produce.
	mat1, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat1.WithMaxRowsPerWindow(2)
	crashErr := errors.New("injected crash after first window")
	mat1.WithWindowCommitHook(func(idx int) error {
		if idx == 0 {
			return crashErr
		}
		return nil
	})
	runner1 := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat1)
	_, err = runner1.RunOnce(ctx)
	require.Error(t, err, "the pass fails on the injected mid-span crash")

	// Partial durability: the first window's rows are at rest, but the cursor did NOT
	// advance (the span isn't finished) — so restart re-reads the whole snapshot.
	assert.Equal(t, 2, countSpeedRows(t, ctx, db, subject), "first window's rows are durable")
	assert.EqualValues(t, 0, readCursor(t, ctx, db), "cursor not advanced on a partial span")

	// Restart: a fresh materializer re-reads the whole snapshot from the un-advanced
	// cursor; the anti-join collapses the two already-written rows and the pass finishes
	// exactly-once.
	mat2, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat2.WithMaxRowsPerWindow(2)
	runner2 := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat2)
	drainRunner(t, ctx, runner2)
	assert.Equal(t, 6, countSpeedRows(t, ctx, db, subject), "restart converges to exactly six rows (no dup from the replayed window)")
	assert.Positive(t, readCursor(t, ctx, db), "cursor advanced after the span completed")
}

// TestDuckLake_SmallSnapshotSingleWindow proves the common case is unchanged: a
// snapshot that fits in one window is written in a single commit (no intermediate
// windows), behaviorally identical to the pre-#1c monolithic path.
func TestDuckLake_SmallSnapshotSingleWindow(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:93", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -1).Truncate(time.Hour)
	seedRawStatusOneSnapshot(t, db, subject, base, 3)

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithMaxRowsPerWindow(100) // window larger than the snapshot

	var intermediateWindows int
	mat.WithWindowCommitHook(func(idx int) error { intermediateWindows++; return nil })
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat)

	n, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, 3, countSpeedRows(t, ctx, db, subject))
	assert.Zero(t, intermediateWindows, "a snapshot within one window commits once — no intermediate windows")
}
