package duck

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The duck query layer reads the DuckLake catalog tables lake.signals /
// lake.events (the only backend). These helpers stand up a file-backed catalog,
// create those two tables matching the materializer's SignalRow / EventRow
// schema (rows.go), and insert decoded rows directly — the same rows the
// DuckLake materializer would write — so the query unit tests exercise the live
// lake path.

func mkts(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return ts.UTC()
}

// sigFixture is one decoded signal row (SignalRow schema).
type sigFixture struct {
	subject, source, name   string
	ts                      time.Time
	num                     float64
	str                     string
	lat, lon, hdop, heading float64
}

// writeSignalsFixture inserts decoded signal rows into lake.signals. The day
// argument is retained for call-site readability but no longer partitions
// physically — DuckLake partitions by (subject_bucket, year/month/day(timestamp)) from the
// row's own timestamp.
func writeSignalsFixture(t *testing.T, svc *Service, _ string, _ string, rows []sigFixture) {
	t.Helper()
	insertSignalRows(t, svc, rows)
}

// insertSignalRows inserts signal rows into lake.signals. cloud_event_id is
// unique per row so the deduped read source keeps every distinct row.
func insertSignalRows(t *testing.T, svc *Service, rows []sigFixture) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	valueRows := make([]string, len(rows))
	for i, r := range rows {
		valueRows[i] = fmt.Sprintf("(%s, %d, %s, %s, %s, 'prod-1', %s, %s, %s, %s, %s, %s, %s)",
			sqlString(r.subject), HashBucket(r.subject), sqlString(r.name), tsMicroLiteral(r.ts),
			sqlString(r.source), sqlString(fmt.Sprintf("ce-sig-%d", i)),
			f64Lit(r.num), sqlString(r.str), f64Lit(r.lat), f64Lit(r.lon), f64Lit(r.hdop), f64Lit(r.heading))
	}
	stmt := `INSERT INTO lake.signals (subject, subject_bucket, name, timestamp, source, producer,
		cloud_event_id, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading) VALUES ` +
		strings.Join(valueRows, ", ")
	_, err := svc.db.ExecContext(context.Background(), stmt)
	require.NoError(t, err)
}

// eventFixture is one decoded event row (EventRow schema).
type eventFixture struct {
	subject, source, name string
	ts                    time.Time
	durNs                 uint64
	metadata              string
	tags                  []string
}

// writeEventsFixture inserts decoded event rows into lake.events.
func writeEventsFixture(t *testing.T, svc *Service, _ string, _ string, rows []eventFixture) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	valueRows := make([]string, len(rows))
	for i, r := range rows {
		// cloud_event_id is unique per row so the deduped events source (which
		// collapses on subject,timestamp,name,source) keeps every distinct row.
		valueRows[i] = fmt.Sprintf("(%s, %d, %s, 'prod-1', %s, 'dimo.event', '1.0', %s, %s, CAST(%d AS UBIGINT), %s, %s)",
			sqlString(r.subject), HashBucket(r.subject), sqlString(r.source), sqlString(fmt.Sprintf("ce-evt-%d", i)),
			sqlString(r.name), tsMicroLiteral(r.ts), r.durNs, sqlString(r.metadata), tagsLit(r.tags))
	}
	stmt := `INSERT INTO lake.events (subject, subject_bucket, source, producer, cloud_event_id, type,
		data_version, name, timestamp, duration_ns, metadata, tags) VALUES ` + strings.Join(valueRows, ", ")
	_, err := svc.db.ExecContext(context.Background(), stmt)
	require.NoError(t, err)
}

func f64Lit(v float64) string {
	return "CAST(" + strconv.FormatFloat(v, 'g', -1, 64) + " AS DOUBLE)"
}

func tagsLit(tags []string) string {
	if len(tags) == 0 {
		return "CAST([] AS VARCHAR[])"
	}
	quoted := make([]string, len(tags))
	for i, tag := range tags {
		quoted[i] = sqlString(tag)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// newQueriesHarness spins up a file-backed DuckLake catalog with lake.signals /
// lake.events created, and a lake-backed Queries layer over it (LoadSpatial on
// for the ST_* geofence filters the aggregation tests exercise).
func newQueriesHarness(t *testing.T) (string, *Service, *Queries) {
	t.Helper()
	dir := t.TempDir()
	svc := newLocalService(t, Config{
		DuckLakeEnabled: true,
		CatalogDSN:      dir + "/catalog.ducklake",
		DataPath:        dir + "/lakedata",
		LoadSpatial:     true, // ST_* geofence filters
	})
	ctx := context.Background()
	// Schemas match materializer.SignalRow / EventRow (rows.go).
	_, err := svc.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS lake.signals (
		subject VARCHAR, subject_bucket INTEGER, name VARCHAR, timestamp TIMESTAMPTZ,
		source VARCHAR, producer VARCHAR, cloud_event_id VARCHAR,
		value_number DOUBLE, value_string VARCHAR,
		loc_lat DOUBLE, loc_lon DOUBLE, loc_hdop DOUBLE, loc_heading DOUBLE)`)
	require.NoError(t, err)
	_, err = svc.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS lake.events (
		subject VARCHAR, subject_bucket INTEGER, source VARCHAR, producer VARCHAR,
		cloud_event_id VARCHAR, type VARCHAR, data_version VARCHAR, name VARCHAR,
		timestamp TIMESTAMPTZ, duration_ns UBIGINT, metadata VARCHAR, tags VARCHAR[])`)
	require.NoError(t, err)
	return dir, svc, NewLakeQueries(svc)
}
