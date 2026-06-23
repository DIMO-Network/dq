package duck

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Fixture writers for the materializer's decoded parquet layout. Schemas
// must stay in sync with internal/materializer/rows.go (SignalRow, EventRow,
// LatestRow, SummaryRow); the materializer is the writer in production.

func mkts(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return ts.UTC()
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

// copyValuesToParquet writes one parquet file from inline VALUES rows.
func copyValuesToParquet(t *testing.T, svc *Service, path string, columns []string, valueRows []string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	stmt := fmt.Sprintf(
		"COPY (SELECT * FROM (VALUES %s) AS t(%s)) TO %s (FORMAT PARQUET)",
		strings.Join(valueRows, ", "), strings.Join(columns, ", "), sqlString(path))
	_, err := svc.DB().Exec(stmt)
	require.NoError(t, err)
}

// sigFixture is one decoded signal row (SignalRow schema).
type sigFixture struct {
	subject, source, name   string
	ts                      time.Time
	num                     float64
	str                     string
	lat, lon, hdop, heading float64
}

var signalFixtureColumns = []string{
	"subject", "name", "timestamp", "source", "producer", "cloud_event_id",
	"value_number", "value_string", "loc_lat", "loc_lon", "loc_hdop", "loc_heading",
}

func writeSignalsFixture(t *testing.T, svc *Service, root, day string, rows []sigFixture) {
	t.Helper()
	path := filepath.Join(root, "decoded", "v1", "signals", "date="+day, "part-0.parquet")
	valueRows := make([]string, len(rows))
	for i, r := range rows {
		valueRows[i] = fmt.Sprintf("(%s, %s, %s, %s, 'prod-1', 'ce-1', %s, %s, %s, %s, %s, %s)",
			sqlString(r.subject), sqlString(r.name), tsMicroLiteral(r.ts), sqlString(r.source),
			f64Lit(r.num), sqlString(r.str), f64Lit(r.lat), f64Lit(r.lon), f64Lit(r.hdop), f64Lit(r.heading))
	}
	copyValuesToParquet(t, svc, path, signalFixtureColumns, valueRows)
}

// latestFixture is one row of the latest bucket (LatestRow schema).
type latestFixture struct {
	name, subject, source           string
	ts                              time.Time
	num                             float64
	str                             string
	lat, lon, hdop, heading         float64
	latNZ, lonNZ, hdopNZ, headingNZ float64
	nzTS                            time.Time
}

var latestFixtureColumns = []string{
	"name", "subject", "source", "timestamp", "value_number", "value_string",
	"loc_lat", "loc_lon", "loc_hdop", "loc_heading",
	"loc_lat_nonzero", "loc_lon_nonzero", "loc_hdop_nonzero", "loc_heading_nonzero", "loc_nonzero_ts",
}

func writeLatestFixture(t *testing.T, svc *Service, root, subject string, rows []latestFixture) {
	t.Helper()
	path := filepath.Join(root, "decoded", "v1", "latest", fmt.Sprintf("bucket=%03d", HashBucket(subject)), "latest.parquet")
	valueRows := make([]string, len(rows))
	for i, r := range rows {
		valueRows[i] = fmt.Sprintf("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
			sqlString(r.name), sqlString(r.subject), sqlString(r.source), tsMicroLiteral(r.ts),
			f64Lit(r.num), sqlString(r.str),
			f64Lit(r.lat), f64Lit(r.lon), f64Lit(r.hdop), f64Lit(r.heading),
			f64Lit(r.latNZ), f64Lit(r.lonNZ), f64Lit(r.hdopNZ), f64Lit(r.headingNZ), tsMicroLiteral(r.nzTS))
	}
	copyValuesToParquet(t, svc, path, latestFixtureColumns, valueRows)
}

// summaryFixture is one row of the summary bucket (SummaryRow schema).
type summaryFixture struct {
	subject, source, name string
	count                 int
	first, last           time.Time
}

var summaryFixtureColumns = []string{"subject", "source", "name", "count", "first_seen", "last_seen"}

func writeSummaryFixture(t *testing.T, svc *Service, root, subject string, rows []summaryFixture) {
	t.Helper()
	path := filepath.Join(root, "decoded", "v1", "summary", fmt.Sprintf("bucket=%03d", HashBucket(subject)), "summary.parquet")
	valueRows := make([]string, len(rows))
	for i, r := range rows {
		valueRows[i] = fmt.Sprintf("(%s, %s, %s, CAST(%d AS UBIGINT), %s, %s)",
			sqlString(r.subject), sqlString(r.source), sqlString(r.name), r.count,
			tsMicroLiteral(r.first), tsMicroLiteral(r.last))
	}
	copyValuesToParquet(t, svc, path, summaryFixtureColumns, valueRows)
}

// eventFixture is one decoded event row (EventRow schema).
type eventFixture struct {
	subject, source, name string
	ts                    time.Time
	durNs                 uint64
	metadata              string
	tags                  []string
}

var eventFixtureColumns = []string{
	"subject", "subject_bucket", "source", "producer", "cloud_event_id", "type", "data_version",
	"name", "timestamp", "duration_ns", "metadata", "tags",
}

func writeEventsFixture(t *testing.T, svc *Service, root, day string, rows []eventFixture) {
	t.Helper()
	path := filepath.Join(root, "decoded", "v1", "events", "date="+day, "part-0.parquet")
	valueRows := make([]string, len(rows))
	for i, r := range rows {
		// subject_bucket mirrors the materializer's stamping so the partition-prune
		// predicate (subject_bucket = HashBucket(subject)) matches the fixture.
		valueRows[i] = fmt.Sprintf("(%s, %d, %s, 'prod-1', 'ce-1', 'dimo.event', '1.0', %s, %s, CAST(%d AS UBIGINT), %s, %s)",
			sqlString(r.subject), HashBucket(r.subject), sqlString(r.source), sqlString(r.name), tsMicroLiteral(r.ts),
			r.durNs, sqlString(r.metadata), tagsLit(r.tags))
	}
	copyValuesToParquet(t, svc, path, eventFixtureColumns, valueRows)
}

// newQueriesHarness spins up a local DuckDB service rooted at a temp dir and
// a Queries layer over it.
func newQueriesHarness(t *testing.T) (string, *Service, *Queries) {
	t.Helper()
	root := t.TempDir()
	svc := newLocalService(t, Config{Bucket: root, LoadSpatial: true}) // ST_* geofence filters
	return root, svc, NewQueries(svc, root)
}
