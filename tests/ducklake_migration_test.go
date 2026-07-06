// ducklake_migration_test.go pins the UPGRADE PATH for catalogs created before
// loc_ts (H9): ensureSchema must ADD COLUMN IF NOT EXISTS on the existing
// rollup table — executed against a real DuckLake, not just present in the
// statement list — and stay idempotent across re-boots. A failure here is a
// CrashLooping materializer on the first post-upgrade deploy.
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

func TestDuckLake_LocTSMigration_ExistingCatalog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()

	// Pre-H9 catalog shape: signals_latest WITHOUT loc_ts, already partitioned
	// (what the June deploys created).
	_, err := db.ExecContext(ctx, `CREATE TABLE lake.signals_latest (
		subject VARCHAR, subject_bucket INTEGER, name VARCHAR,
		"timestamp" TIMESTAMP WITH TIME ZONE,
		value_number DOUBLE, value_string VARCHAR,
		loc_lat DOUBLE, loc_lon DOUBLE, loc_hdop DOUBLE, loc_heading DOUBLE,
		count BIGINT, first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `ALTER TABLE lake.signals_latest SET PARTITIONED BY (subject_bucket)`)
	require.NoError(t, err)

	// New-binary boot: ensureSchema migrates the existing table.
	_, err = materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err, "boot against a pre-loc_ts catalog must migrate, not crash")

	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*) FROM duckdb_columns() WHERE table_name = 'signals_latest' AND column_name = 'loc_ts'`).Scan(&count))
	assert.Equal(t, 1, count, "loc_ts column added to the existing rollup")

	// Second boot: idempotent (ADD COLUMN IF NOT EXISTS + no re-layout crash).
	_, err = materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err, "re-boot after migration must be a no-op")

	// The migrated rollup is live: decode + flush writes loc_ts.
	subject := fmt.Sprintf("did:erc721:137:%s:9", vehicleNFT.Hex())
	ts := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	seedRawStatus(t, db, "mig-1", subject, ts,
		map[string]any{"name": "currentLocationCoordinates", "timestamp": ts.Format(time.RFC3339Nano),
			"value": map[string]any{"latitude": 42.0, "longitude": -83.0}},
	)
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner))

	var locTS time.Time
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT loc_ts FROM lake.signals_latest WHERE subject = ? AND name = 'currentLocationCoordinates'", subject).Scan(&locTS))
	assert.True(t, locTS.Equal(ts), "migrated rollup serves the nonzero-fix timestamp, got %v want %v", locTS, ts)
}
