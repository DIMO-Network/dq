package duck

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// histCount returns the observation count of a labelled histogram. Fetching the
// series creates it at zero when absent, which is what the "before" reads want.
func histCount(t *testing.T, h *prometheus.HistogramVec, label string) uint64 {
	t.Helper()
	o, err := h.GetMetricWithLabelValues(label)
	require.NoError(t, err)
	m, ok := o.(prometheus.Metric)
	require.True(t, ok, "histogram observer is not a Metric")
	var pb dto.Metric
	require.NoError(t, m.Write(&pb))
	return pb.GetHistogram().GetSampleCount()
}

func TestQueryLake_CountsRowsReturned(t *testing.T) {
	svc, err := NewService(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	before := histCount(t, lakeRowsReturned, "probeRows")

	rows, err := svc.queryLake(context.Background(), "probeRows", "SELECT * FROM range(7)")
	require.NoError(t, err)
	n := 0
	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
		n++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, 7, n)

	// One observation recorded, and the probe counted what the caller drained.
	require.Equal(t, before+1, histCount(t, lakeRowsReturned, "probeRows"))
	require.Equal(t, 7, rows.n)
}

func TestQueryLake_CloseIsIdempotent(t *testing.T) {
	svc, err := NewService(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	before := histCount(t, lakeRowsReturned, "probeIdem")

	rows, err := svc.queryLake(context.Background(), "probeIdem", "SELECT 1")
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Close()) // call sites both defer Close and close early

	// Exactly one observation despite two Close calls — otherwise every
	// early-return path would double-count.
	require.Equal(t, before+1, histCount(t, lakeRowsReturned, "probeIdem"))
}

// TestQueryLake_ProfilingReportsRowsScanned pins the part of the probe that
// depends on DuckDB internals: that the custom_profiling_settings key names are
// accepted and that the tree GetProfilingInfo returns actually carries
// OPERATOR_ROWS_SCANNED. A DuckDB upgrade that renames either would otherwise
// silently reduce dq_lake_rows_scanned to "never emitted".
func TestQueryLake_ProfilingReportsRowsScanned(t *testing.T) {
	svc, err := NewService(Config{ProfileReads: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	before := histCount(t, lakeRowsScanned, "probeProfile")

	rows, err := svc.queryLake(context.Background(), "probeProfile",
		"SELECT count(*) FROM range(1000) t(i) WHERE i % 2 = 0")
	require.NoError(t, err)
	require.NotNil(t, rows.conn, "ProfileReads must pin a connection for the read")
	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
	}
	require.NoError(t, rows.Close())

	require.Equal(t, before+1, histCount(t, lakeRowsScanned, "probeProfile"),
		"profiling tree carried no OPERATOR_ROWS_SCANNED; check DuckDB metric names")
}

func TestQueryLake_ProfilingOffDoesNotPinConnection(t *testing.T) {
	svc, err := NewService(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	rows, err := svc.queryLake(context.Background(), "probeNoPin", "SELECT 1")
	require.NoError(t, err)
	require.Nil(t, rows.conn, "default path must go through the pool, not a checked-out conn")
	require.NoError(t, rows.Close())
}

func TestFingerprintStmt_RemovesLiterals(t *testing.T) {
	in := `SELECT type FROM lake.raw_events WHERE subject = 'did:erc721:137:0xbA5:000042' ` +
		`AND subject_bucket = 137 AND time > make_timestamp(1750000000000000)`
	got := fingerprintStmt(in)

	// The two things a slow-read line must not leak, and the shape it must keep.
	require.NotContains(t, got, "0xbA5")
	require.NotContains(t, got, "1750000000000000")
	require.Contains(t, got, "FROM lake.raw_events WHERE subject = ?")
	require.Contains(t, got, "subject_bucket = ?")
}

func TestFingerprintStmt_Truncates(t *testing.T) {
	got := fingerprintStmt(strings.Repeat("a ", 500))
	require.LessOrEqual(t, len(got), 404)
	require.True(t, strings.HasSuffix(got, "…"))
}

func TestSlowReadThreshold_Default(t *testing.T) {
	require.Equal(t, DefaultSlowReadThreshold, (&Service{}).slowReadThreshold())
	require.Equal(t, 5*time.Second, (&Service{slowRead: 5 * time.Second}).slowReadThreshold())
}
