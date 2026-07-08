// ducklake_verify_test.go — adversarial verification campaign for the #1c pagination +
// #5b incremental-rollup exactly-once path. Each subtest is a distinct attack vector; the
// contract is always the same: the incrementally-maintained lake.signals_latest equals a
// full RecomputeRollup, and base rows are exactly-once. These are throwaway-hardening
// tests that also stay as regression guards.
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

func locAt(ts time.Time, lat, lon, hdop float64) map[string]any {
	return map[string]any{"name": "currentLocationCoordinates", "timestamp": ts.Format(time.RFC3339Nano),
		"value": map[string]any{"latitude": lat, "longitude": lon, "hdop": hdop}}
}

// V1 — a location signal has value_number NULL and loc_* set; the fold must keep
// value_number NULL and carry loc columns exactly like the recompute.
func TestVerify01_LocationSignalNullValueNumber(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:1", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	runner, mat := incrRunner(t, ctx, db)
	seedRawStatus(t, db, "v1a", subj, base.Add(1*time.Minute), locAt(base.Add(1*time.Minute), 42.1, -83.0, 1.2))
	drainNoFlush(t, ctx, runner)
	seedRawStatus(t, db, "v1b", subj, base.Add(2*time.Minute), locAt(base.Add(2*time.Minute), 42.3, -83.1, 0.9))
	drainNoFlush(t, ctx, runner)
	assertMatchesRecompute(t, ctx, db, mat) // the real contract: value_number (whatever it is) matches the recompute
	got := dumpRollupMap(t, ctx, db)[subj+"|currentLocationCoordinates"]
	assert.InDelta(t, 42.3, got.locLat, 1e-9, "loc_lat folds to the newest fix")
	assert.True(t, got.locTS.Equal(base.Add(2*time.Minute)), "loc_ts is the newest fix")
}

// V2 — location is intermittent: a fix, then non-loc batches, then an OLDER loc; loc_ts
// must stay the newest fix (not regress, not be overwritten by the older one).
func TestVerify02_IntermittentLocation(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:2", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	runner, mat := incrRunner(t, ctx, db)
	seedRawStatus(t, db, "v2loc", subj, base.Add(5*time.Minute), locAt(base.Add(5*time.Minute), 40.0, -80.0, 1.0))
	drainNoFlush(t, ctx, runner)
	seedRawStatus(t, db, "v2sp", subj, base.Add(9*time.Minute), speedAt(base.Add(9*time.Minute), 33)) // newer, no loc
	drainNoFlush(t, ctx, runner)
	seedRawStatus(t, db, "v2old", subj, base.Add(1*time.Minute), locAt(base.Add(1*time.Minute), 1.0, 2.0, 9.0)) // older loc
	drainNoFlush(t, ctx, runner)
	assertMatchesRecompute(t, ctx, db, mat)
	loc := dumpRollupMap(t, ctx, db)[subj+"|currentLocationCoordinates"]
	assert.True(t, loc.locTS.Equal(base.Add(5*time.Minute)), "loc_ts stays the newest fix despite a later-arriving older one")
	assert.InDelta(t, 40.0, loc.locLat, 1e-9)
}

// V3 — a fat single snapshot with many (subject,name) pairs folds exactly in one window.
func TestVerify03_ManyKeysOneSnapshot(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	runner, mat := incrRunner(t, ctx, db)
	tx, err := db.Begin()
	require.NoError(t, err)
	for s := 0; s < 6; s++ {
		subj := fmt.Sprintf("did:erc721:137:%s:%d", vehicleNFT.Hex(), 30+s)
		for i := 0; i < 4; i++ {
			ts := base.Add(time.Duration(s*10+i) * time.Minute)
			for _, sig := range []map[string]any{speedAt(ts, float64(i*5)), odoAt(ts, float64(1000+i))} {
				ev := deviceStatus(fmt.Sprintf("v3-%d-%d-%s", s, i, sig["name"]), subj, ts, sig)
				_, err := tx.Exec(`INSERT INTO lake.raw_events (subject,"time",type,id,source,producer,data_content_type,data_version,extras,data) VALUES (?,?,?,?,?,?,'',?, '{}',?)`,
					ev.Subject, ev.Time.UTC(), ev.Type, ev.ID, ev.Source, ev.Producer, ev.DataVersion, string(ev.Data))
				require.NoError(t, err)
			}
		}
	}
	require.NoError(t, tx.Commit())
	drainNoFlush(t, ctx, runner)
	assertMatchesRecompute(t, ctx, db, mat)
	assert.Len(t, dumpRollupMap(t, ctx, db), 12, "6 subjects x 2 names = 12 rollup rows")
}

// V4 — pagination driven by the BYTE budget (row cap disabled) still exact.
func TestVerify04_ByteBudgetPagination(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:4", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	var intermediate int
	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) {
		m.WithMaxRowsPerWindow(0)   // disable the row cap
		m.WithWindowByteBudget(100) // tiny byte budget -> flush after each read chunk
		m.WithWindowCommitHook(func(int) error { intermediate++; return nil })
	})
	seedRawStatusOneSnapshot(t, db, subj, base, 1100) // > windowReadChunk so the byte path paginates
	drainNoFlush(t, ctx, runner)
	assertMatchesRecompute(t, ctx, db, mat)
	assert.EqualValues(t, 1100, dumpRollupMap(t, ctx, db)[subj+"|speed"].count)
	assert.Positive(t, intermediate, "the byte-budget path split the span into multiple windows")
}

// V5 — crash at the LAST intermediate window (most windows durable, cursor not advanced);
// restart converges exactly for BOTH base rows and the rollup.
func TestVerify05_CrashLastIntermediateWindow(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:5", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	seedRawStatusOneSnapshot(t, db, subj, base, 9) // windows of 2 -> intermediate idx 0..3, final = 1 row
	mat1, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat1.WithMaxRowsPerWindow(2)
	mat1.WithWindowCommitHook(func(idx int) error {
		if idx == 3 {
			return fmt.Errorf("crash at last intermediate window")
		}
		return nil
	})
	r1 := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).WithDuckLake(mat1)
	_, err = r1.RunOnce(ctx)
	require.Error(t, err)
	assert.EqualValues(t, 0, readCursor(t, ctx, db), "cursor not advanced on a partial span")
	runner2, mat2 := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) { m.WithMaxRowsPerWindow(2) })
	drainNoFlush(t, ctx, runner2)
	assert.EqualValues(t, 9, dumpRollupMap(t, ctx, db)[subj+"|speed"].count)
	assertMatchesRecompute(t, ctx, db, mat2)
}

// V6 — a paginated span mixes decodable rows with rows that decode to NOTHING (wrong-chain
// subject). The row-key cursor must advance past the non-decodable rows (no hang), and only
// real signals are counted.
func TestVerify06_MixedNonDecodableRows(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	good := fmt.Sprintf("did:erc721:137:%s:6", vehicleNFT.Hex())
	bad := fmt.Sprintf("did:erc721:1:%s:6", vehicleNFT.Hex()) // wrong chain -> not a vehicle -> 0 signals
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := 0; i < 12; i++ {
		subj := good
		if i%2 == 1 {
			subj = bad
		}
		ts := base.Add(time.Duration(i) * time.Minute)
		ev := deviceStatus(fmt.Sprintf("v6-%d", i), subj, ts, speedAt(ts, float64(i)))
		_, err := tx.Exec(`INSERT INTO lake.raw_events (subject,"time",type,id,source,producer,data_content_type,data_version,extras,data) VALUES (?,?,?,?,?,?,'',?, '{}',?)`,
			ev.Subject, ev.Time.UTC(), ev.Type, ev.ID, ev.Source, ev.Producer, ev.DataVersion, string(ev.Data))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	runner, mat := incrRunner(t, ctx, db, func(m *materializer.DuckLakeMaterializer) { m.WithMaxRowsPerWindow(3) })
	drainNoFlush(t, ctx, runner)
	assert.Positive(t, readCursor(t, ctx, db), "cursor advanced past non-decodable rows")
	assert.EqualValues(t, 6, dumpRollupMap(t, ctx, db)[good+"|speed"].count, "only the 6 vehicle rows counted")
	assertMatchesRecompute(t, ctx, db, mat)
}

// V7 — an out-of-order batch 100 days older than the existing latest: recency unchanged,
// count increments, first_seen moves back, exactly matching the recompute.
func TestVerify07_DeepOutOfOrder(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:7", vehicleNFT.Hex())
	now := time.Now().UTC().Truncate(time.Hour)
	runner, mat := incrRunner(t, ctx, db)
	seedRawStatus(t, db, "v7new", subj, now.Add(-1*time.Hour), speedAt(now.Add(-1*time.Hour), 70))
	drainNoFlush(t, ctx, runner)
	old := now.AddDate(0, 0, -100)
	seedRawStatus(t, db, "v7old", subj, old, speedAt(old, 12))
	drainNoFlush(t, ctx, runner)
	assertMatchesRecompute(t, ctx, db, mat)
	got := dumpRollupMap(t, ctx, db)[subj+"|speed"]
	assert.EqualValues(t, 2, got.count)
	assert.EqualValues(t, 70, got.valueNumber.Float64, "recency unchanged by the 100-day-old arrival")
	assert.True(t, got.firstSeen.Equal(old), "first_seen moved back 100 days")
}

// V8 — interleave a full RecomputeRollup with incremental folds: the incremental path must
// stay exact when it continues on top of a freshly recomputed rollup (self-healing).
func TestVerify08_RecomputeInterleave(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subj := fmt.Sprintf("did:erc721:137:%s:8", vehicleNFT.Hex())
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(time.Hour)
	runner, mat := incrRunner(t, ctx, db)
	seedRawStatus(t, db, "v8a", subj, base.Add(1*time.Minute), speedAt(base.Add(1*time.Minute), 10))
	seedRawStatus(t, db, "v8b", subj, base.Add(2*time.Minute), speedAt(base.Add(2*time.Minute), 20))
	drainNoFlush(t, ctx, runner)
	require.NoError(t, mat.RecomputeRollup(ctx)) // rebuild from base mid-stream
	seedRawStatus(t, db, "v8c", subj, base.Add(3*time.Minute), speedAt(base.Add(3*time.Minute), 30))
	drainNoFlush(t, ctx, runner)
	assertMatchesRecompute(t, ctx, db, mat)
	assert.EqualValues(t, 3, dumpRollupMap(t, ctx, db)[subj+"|speed"].count)
}
