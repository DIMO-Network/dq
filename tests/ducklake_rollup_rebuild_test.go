// ducklake_rollup_rebuild_test.go covers the disaster-recovery rebuild of
// lake.signals_latest (RecomputeRollup): the per-batch refreshRollup only touches
// subjects present in a batch, so a dropped/truncated rollup needs a full rebuild
// from the base to repopulate dormant vehicles. RecomputeRollup must produce a
// rollup byte-identical to what the per-batch recompute built.
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

func dumpRollup(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT subject, name, timestamp,
		coalesce(value_number, 0), coalesce(value_string, ''),
		coalesce(loc_lat, 0), coalesce(loc_lon, 0), count, first_seen, last_seen
		FROM lake.signals_latest ORDER BY subject, name`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var out []string
	for rows.Next() {
		var subj, name, vs string
		var ts, fs, ls time.Time
		var vn, lat, lon float64
		var cnt int64
		require.NoError(t, rows.Scan(&subj, &name, &ts, &vn, &vs, &lat, &lon, &cnt, &fs, &ls))
		out = append(out, fmt.Sprintf("%s|%s|%s|%v|%q|%v|%v|%d|%s|%s",
			subj, name, ts.UTC(), vn, vs, lat, lon, cnt, fs.UTC(), ls.UTC()))
	}
	require.NoError(t, rows.Err())
	return out
}

func TestRecomputeRollup_RebuildsDroppedRollupFromBase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	subjA := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())
	subjB := fmt.Sprintf("did:erc721:137:%s:8", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -5).Truncate(time.Hour)
	seedRawStatus(t, db, "rr1", subjA, base.Add(time.Hour), speedAt(base.Add(time.Hour), 40))
	seedRawStatus(t, db, "rr2", subjA, base.Add(2*time.Hour), speedAt(base.Add(2*time.Hour), 80))
	seedRawStatus(t, db, "rr3", subjB, base.Add(time.Hour), speedAt(base.Add(time.Hour), 12))
	require.Positive(t, drainRunner(t, ctx, runner))

	// The rollup the per-batch recompute built.
	perBatch := dumpRollup(t, ctx, db)
	require.Len(t, perBatch, 2, "one (subject,name) row per vehicle")

	// Simulate a dropped/truncated rollup (the DR scenario): the base lake.signals
	// is intact, but signals_latest is empty — dormant vehicles would never be
	// recomputed by the per-batch path.
	_, err = db.ExecContext(ctx, "DELETE FROM lake.signals_latest")
	require.NoError(t, err)
	require.Empty(t, dumpRollup(t, ctx, db))

	// Full rebuild from the base.
	require.NoError(t, mat.RecomputeRollup(ctx))

	require.Equal(t, perBatch, dumpRollup(t, ctx, db),
		"RecomputeRollup must rebuild signals_latest byte-identical to the per-batch recompute")
}
