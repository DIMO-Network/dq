package duck

import (
	"context"
	"os"
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

// TestQueryLake_ProfilingWritesAndCleansUp pins the SQL profiling plumbing: a
// profiled read pins a connection, DuckDB writes a plan file to the configured
// path, and Close removes it. The file carries the plan's filter literals, so
// leaving it behind is a data-hygiene problem as well as a disk one.
func TestQueryLake_ProfilingWritesAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	svc, err := NewService(Config{ProfileReads: true, TempDirectory: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	rows, err := svc.queryLake(context.Background(), "probeProfile",
		"SELECT count(*) FROM range(1000) t(i) WHERE i % 2 = 0")
	require.NoError(t, err)
	require.NotNil(t, rows.conn, "a profiled read must pin its connection")
	require.NotEmpty(t, rows.profilePath, "profiling_output must be set for the read")

	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
	}
	require.NoError(t, rows.Close())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "the profiling file must not outlive the read")
	require.False(t, svc.profileBroken.Load(), "SQL profiling must not trip the panic latch")
}

// The parser is pinned against a real DuckLake plan rather than a hand-written
// fixture: "Total Files Read" is free text inside extra_info, so its exact
// rendering is what the metric depends on.
func TestParseProfileFilesRead(t *testing.T) {
	ducklakePlan := []byte(`{"extra_info":{},"children":[
		{"operator_type":"TABLE_SCAN","extra_info":{"Table":"raw_events","Total Files Read":"90"},"children":[]},
		{"operator_type":"TABLE_SCAN","extra_info":{"Table":"other","Total Files Read":"7"},"children":[]}]}`)

	files, ok := parseProfileFilesRead(ducklakePlan)
	require.True(t, ok)
	require.Equal(t, int64(90), files, "the widest scan characterises the read, so max not sum")

	// A plan with no file count (a range scan, an in-memory table) suppresses the
	// metric rather than reporting zero files read.
	_, ok = parseProfileFilesRead([]byte(`{"extra_info":{"Function":"RANGE"},"children":[]}`))
	require.False(t, ok)

	// Malformed JSON must not panic the read that already returned rows.
	_, ok = parseProfileFilesRead([]byte("{not json"))
	require.False(t, ok)
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

// TestQueryLake_ProfilingWorksWithPoisonRecovery is the regression test for the
// 2026-07-29 incident: ProfileReads AND PoisonRecovery together — production's
// exact configuration, and the one combination the original tests omitted.
//
// Under the old C-API path this panicked (duckdb-go's GetProfilingInfo
// type-asserts the raw driver connection to its own *Conn, which dq's
// recoveringConn wrapper is not), the panic unwound into the request's recovery
// middleware, and live GraphQL reads failed. Going through SQL means the
// wrapper is irrelevant: profiling now WORKS in this configuration rather than
// merely failing safely.
func TestQueryLake_ProfilingWorksWithPoisonRecovery(t *testing.T) {
	dir := t.TempDir()
	svc, err := NewService(Config{ProfileReads: true, PoisonRecovery: true, TempDirectory: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	before := histCount(t, lakeRowsReturned, "probePoison")

	require.NotPanics(t, func() {
		rows, err := svc.queryLake(context.Background(), "probePoison", "SELECT * FROM range(3)")
		require.NoError(t, err)
		require.NotEmpty(t, rows.profilePath, "profiling must be active despite the connection wrapper")
		for rows.Next() {
			var v int64
			require.NoError(t, rows.Scan(&v))
		}
		require.NoError(t, rows.Close())
	})

	// The row count must survive: under the old path the panic fired ahead of
	// this observation, which is why the whole probe went dark in prod.
	require.Equal(t, before+1, histCount(t, lakeRowsReturned, "probePoison"))

	// And profiling must NOT be latched off — the wrapper no longer breaks it.
	require.False(t, svc.profileBroken.Load(),
		"SQL profiling must survive the poison-recovery connection wrapper")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "the profiling file must not outlive the read")
}
