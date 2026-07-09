// ducklake_latest_sort_test.go pins scale-review #4: signals_latest / events_latest must
// be SORTED BY (subject) so a per-vehicle latest/dataSummary/location read prunes to a few
// row groups within its subject_bucket instead of scanning the whole bucket. Verified on a
// fresh catalog (first-create) and migrated onto an existing unsorted rollup (prod upgrade).
package tests

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// latestSortedBySubject reports whether table currently has an ACTIVE SORTED BY spec
// (a ducklake_sort_info row with end_snapshot IS NULL), mirroring din's isSorted probe.
func latestSortedBySubject(t *testing.T, ctx context.Context, db *sql.DB, table string) bool {
	t.Helper()
	var ok bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT count(*) > 0 FROM __ducklake_metadata_lake.ducklake_sort_info
		WHERE end_snapshot IS NULL
		  AND table_id = (SELECT table_id FROM ducklake_table_info('lake') WHERE table_name = ?)`, table).Scan(&ok))
	return ok
}

func TestDuckLake_LatestTablesSortedOnCreate(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	mustMat(t, ctx, db) // ensureSchema creates the rollups
	assert.True(t, latestSortedBySubject(t, ctx, db, "signals_latest"), "signals_latest must be SORTED BY (subject) on create")
	assert.True(t, latestSortedBySubject(t, ctx, db, "events_latest"), "events_latest must be SORTED BY (subject) on create")
}

func TestDuckLake_LatestTablesSortMigration(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	// Simulate a pre-#4 catalog: the rollups exist, partitioned but UNSORTED.
	for _, ddl := range []string{
		`CREATE TABLE lake.signals_latest (
			subject VARCHAR, subject_bucket INTEGER, name VARCHAR, "timestamp" TIMESTAMP WITH TIME ZONE,
			value_number DOUBLE, value_string VARCHAR, loc_lat DOUBLE, loc_lon DOUBLE, loc_hdop DOUBLE, loc_heading DOUBLE,
			loc_ts TIMESTAMP WITH TIME ZONE, count BIGINT, first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`,
		`ALTER TABLE lake.signals_latest SET PARTITIONED BY (subject_bucket)`,
		`CREATE TABLE lake.events_latest (
			subject VARCHAR, subject_bucket INTEGER, name VARCHAR, count BIGINT,
			first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`,
		`ALTER TABLE lake.events_latest SET PARTITIONED BY (subject_bucket)`,
	} {
		_, err := db.ExecContext(ctx, ddl)
		require.NoError(t, err, ddl)
	}
	require.False(t, latestSortedBySubject(t, ctx, db, "signals_latest"), "precondition: unsorted")
	require.False(t, latestSortedBySubject(t, ctx, db, "events_latest"), "precondition: unsorted")

	// A fresh materializer boot must migrate the existing unsorted rollups.
	mustMat(t, ctx, db)
	assert.True(t, latestSortedBySubject(t, ctx, db, "signals_latest"), "migration must sort an existing unsorted signals_latest")
	assert.True(t, latestSortedBySubject(t, ctx, db, "events_latest"), "migration must sort an existing unsorted events_latest")

	// Idempotent: a second boot must not error (re-derives from catalog; no re-ALTER churn).
	mustMat(t, ctx, db)
	assert.True(t, latestSortedBySubject(t, ctx, db, "signals_latest"))
}
