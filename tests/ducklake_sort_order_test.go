package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestDuckLake_SignalsFilesSortedBySubject pins the physical layout the
// per-vehicle telemetry reads depend on: lake.signals is SORTED BY (subject,
// "timestamp"), so each file is written in that order and a single-vehicle query
// skips most row groups by subject min/max. The seed is one snapshot of several
// vehicles over two days with two signals per event, deliberately newest-first,
// so a write that drops the sort shows up as inversions within a file.
//
// DuckDB 2.0.0's ducklake extension can lose the declared sort on inserts into
// partitioned sorted tables (duckdb/ducklake#1740; din's arrival-order appends
// hit it). dq is shielded because writeSignalParquet pre-sorts each batch by
// subject before the INSERT; this test is what notices if that stops holding.
func TestDuckLake_SignalsFilesSortedBySubject(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()

	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	// One INSERT = one snapshot: 4 vehicles x 2 days x 6000 events, newest first.
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data)
		SELECT subj, ts, 'dimo.status', 'so-' || i, '0xConnLicense', subj, '', 'default/v1.0', '{}',
			'{"signals":[{"name":"speed","timestamp":"' || strftime(ts, '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ') || '","value":' || (i %% 120) || '},{"name":"powertrainTransmissionTravelledDistance","timestamp":"' || strftime(ts, '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ') || '","value":' || i || '}]}'
		FROM (
			SELECT i,
				'did:erc721:137:%s:' || ((i * 7) %% 4 + 1) AS subj,
				TIMESTAMPTZ '%s' + INTERVAL 2 DAY - (i * 3.6) * INTERVAL 1 SECOND AS ts
			FROM range(48000) t(i))`,
		vehicleNFT.Hex(), day.Format("2006-01-02 15:04:05+00")))
	require.NoError(t, err)

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 48000, drainRunner(t, ctx, runner))

	rows, err := db.QueryContext(ctx, `SELECT data_file FROM ducklake_list_files('lake', 'signals')`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var files []string
	for rows.Next() {
		var f string
		require.NoError(t, rows.Scan(&f))
		if !filepath.IsAbs(f) {
			f = filepath.Join(dir, "lakedata", f)
		}
		files = append(files, f)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, files)

	for _, f := range files {
		var inversions int
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT count(*) FROM (
				SELECT subject, "timestamp",
					lag(subject) OVER (ORDER BY file_row_number) AS ps,
					lag("timestamp") OVER (ORDER BY file_row_number) AS pt
				FROM read_parquet('%s', file_row_number = true))
			WHERE subject < ps OR (subject = ps AND "timestamp" < pt)`, f)).Scan(&inversions))
		require.Zerof(t, inversions, "%s is not in (subject, timestamp) order", filepath.Base(f))
	}
}
