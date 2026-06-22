// ducklake_rollup_incremental_test.go is the safety net for the SR-1 incremental
// rollup: it drives several overlapping batches through the materializer (which
// merges signals_latest incrementally, O(batch)) and asserts the result is
// byte-identical to a full recompute from the base table (O(history)). If the
// incremental merge ever diverges — wrong latest, miscounted, wrong first/last
// seen — this fails. The loc columns follow the identical CASE-by-timestamp merge
// as value_number, exact because origin (0,0) coordinates are pruned upstream.
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type rollupRow struct {
	subject, name                       string
	timestamp                           time.Time
	valueNumber                         float64
	valueString                         string
	locLat, locLon, locHdop, locHeading float64
	count                               int64
	firstSeen, lastSeen                 time.Time
}

func readRollup(t *testing.T, ctx context.Context, db *sql.DB) []rollupRow {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT subject, name, timestamp,
		coalesce(value_number, 0), coalesce(value_string, ''),
		coalesce(loc_lat, 0), coalesce(loc_lon, 0), coalesce(loc_hdop, 0), coalesce(loc_heading, 0),
		count, first_seen, last_seen
		FROM lake.signals_latest ORDER BY subject, name`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var out []rollupRow
	for rows.Next() {
		var r rollupRow
		require.NoError(t, rows.Scan(&r.subject, &r.name, &r.timestamp,
			&r.valueNumber, &r.valueString, &r.locLat, &r.locLon, &r.locHdop, &r.locHeading,
			&r.count, &r.firstSeen, &r.lastSeen))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

func TestRollup_IncrementalMatchesRecompute(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, nil, zerolog.Nop()).
		WithDuckLake(mat)

	subjA := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())
	subjB := fmt.Sprintf("did:erc721:137:%s:8", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -10).Truncate(time.Hour)
	at := func(h int) time.Time { return base.Add(time.Duration(h) * time.Hour) }

	// Batch 1: two subjects, A gets two readings.
	seedRawStatus(t, db, "a1", subjA, at(1), speedAt(at(1), 40))
	seedRawStatus(t, db, "a2", subjA, at(2), speedAt(at(2), 80))
	seedRawStatus(t, db, "b1", subjB, at(1), speedAt(at(1), 10))
	require.Positive(t, drainRunner(t, ctx, runner))

	// Batch 2: a newer A reading (advances latest/last_seen/count), an older A
	// backfill (lowers first_seen, NOT the latest), and a B redelivery at the same
	// (subject,name,timestamp) with a new id (deduped — must not inflate the count).
	seedRawStatus(t, db, "a3", subjA, at(3), speedAt(at(3), 65))
	seedRawStatus(t, db, "a0", subjA, at(0), speedAt(at(0), 20))
	seedRawStatus(t, db, "b1-dup", subjB, at(1), speedAt(at(1), 10))
	require.Positive(t, drainRunner(t, ctx, runner))

	// Batch 3: B advances; A untouched (its rollup row must be preserved across the
	// merge, not lost).
	seedRawStatus(t, db, "b2", subjB, at(2), speedAt(at(2), 30))
	require.Positive(t, drainRunner(t, ctx, runner))

	incremental := readRollup(t, ctx, db)

	// Oracle: full recompute from the base table.
	require.NoError(t, mat.RecomputeRollup(ctx))
	recomputed := readRollup(t, ctx, db)

	require.Equal(t, recomputed, incremental,
		"the incrementally-merged rollup must equal the full recompute from base")

	// Spot-check the expectations so a bug in BOTH paths can't pass silently.
	require.Len(t, incremental, 2)
	a := incremental[0]
	require.Equal(t, subjA, a.subject)
	require.Equal(t, 65.0, a.valueNumber, "A latest = newest reading (t3=65), not the backfilled t0")
	require.Equal(t, int64(4), a.count, "A counts t0,t1,t2,t3")
	require.Equal(t, at(0), a.firstSeen)
	require.Equal(t, at(3), a.lastSeen)
	b := incremental[1]
	require.Equal(t, subjB, b.subject)
	require.Equal(t, 30.0, b.valueNumber)
	require.Equal(t, int64(2), b.count, "B redelivery at t1 deduped; t1,t2 distinct")
}
