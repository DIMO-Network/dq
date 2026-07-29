package duck

import (
	"context"
	"database/sql"
	"regexp"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	zlog "github.com/rs/zerolog/log"
)

// The read probe answers the question dq_lake_read_seconds alone cannot: WHY an
// op is slow. A duration on its own is ambiguous — 7s could be a wide scan, a
// cold file-metadata pass over a table nothing has touched in hours, or pool
// contention — and distinguishing them previously meant rebuilding the lake
// geometry locally and guessing. The probe adds, in ascending order of cost:
//
//   - rows returned (free: counted on the *sql.Rows the caller already drains).
//     A read that returns 6 rows and takes 7s is not returning data slowly, and
//     that single fact rules out most hypotheses immediately.
//   - a slow-read log line above SlowReadThreshold, carrying the op, the
//     duration, the row count and a statement fingerprint — so the FILTER SHAPE
//     that went slow is recoverable after the fact instead of inferred from a
//     p95.
//   - DuckDB's own per-query profiling (rows scanned, files read), gated behind
//     Config.ProfileReads because it requires running every read on a checked-out
//     connection with profiling enabled. Off by default: it is an investigation
//     tool, not steady-state instrumentation.
//
// Rows returned is the one that pays for itself continuously; the other two are
// for the window in which someone is actually looking.

// lakeRowsReturned is the row count each lake read handed back, per op. Paired
// with dq_lake_read_seconds it separates "slow because it moved a lot of data"
// from "slow before it moved any" — the latter being the DuckLake planning /
// file-metadata floor, which is invisible in a latency histogram alone. Buckets
// span 1..~32k rows; the zero bucket (an empty result that still cost seconds)
// is deliberately observable.
var lakeRowsReturned = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "dq_lake_rows_returned",
	Help:    "Rows returned by each lake read, by op. Compare with dq_lake_read_seconds to separate scan volume from planning overhead.",
	Buckets: prometheus.ExponentialBuckets(1, 2, 16),
}, []string{"op"})

// lakeRowsScanned is the rows DuckDB actually read to produce those results,
// summed over the plan's scan operators. The ratio scanned:returned is the
// selectivity of the read as executed — a summary that scans a subject's entire
// history to return one row per type shows up here as a four-order-of-magnitude
// gap, which no wall-clock metric expresses. Only emitted when ProfileReads is
// on.
var lakeRowsScanned = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "dq_lake_rows_scanned",
	Help:    "Rows scanned by each lake read (sum over plan scan operators), by op. Requires DUCKDB_PROFILE_READS.",
	Buckets: prometheus.ExponentialBuckets(1, 4, 16),
}, []string{"op"})

// lakeFilesRead is the parquet data-file count one read opened. This is THE
// number for a lake whose partition key does not match the query's filter: a
// subject-scoped read of a (type, day)-partitioned table cannot prune files, so
// it opens every one of them, and on S3 each open is a round trip. Only emitted
// when ProfileReads is on AND the plan reported a file count.
var lakeFilesRead = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "dq_lake_files_read",
	Help:    "Parquet data files opened by each lake read, by op. Requires DUCKDB_PROFILE_READS.",
	Buckets: prometheus.ExponentialBuckets(1, 2, 16),
}, []string{"op"})

// DefaultSlowReadThreshold is the duration above which a lake read logs a
// slow-read line. Chosen to sit above every healthy op's p95 (the fetch and
// range reads run 100-250ms) and below the shared DuckLake planning floor
// (~450ms rollup p50, ~950ms p95), so a firing line means "slower than planning
// alone explains" rather than "the lake is a lake".
const DefaultSlowReadThreshold = 1500 * time.Millisecond

// stmtFingerprintRE collapses the parts of a statement that vary per request
// (bound-parameter placeholders are already opaque, but inlined literals — the
// timestamp and subject_bucket literals the query builders emit — are not) so
// the slow-read log groups by query SHAPE. Without this every line is unique
// and the log answers no aggregate question.
var stmtFingerprintRE = regexp.MustCompile(`'[^']*'|\b\d[\d.]*\b`)

// fingerprintStmt renders a statement as its shape: literals replaced with ?,
// whitespace collapsed, truncated. Safe to log — subjects and timestamps are
// exactly what it removes, so a slow-read line carries no vehicle identity.
func fingerprintStmt(stmt string) string {
	s := stmtFingerprintRE.ReplaceAllString(stmt, "?")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 400 {
		s = s[:400] + "…"
	}
	return s
}

// lakeRows wraps the *sql.Rows a lake read returns so the probe can count rows
// as the caller drains them and record everything on Close. Callers already
// `defer rows.Close()`, so no call site needs a second deferred hook.
//
// Next and Close are overridden; Scan/Err/Columns come from the embedded
// *sql.Rows, which keeps lakeRows a drop-in for the rowScanner call sites.
type lakeRows struct {
	*sql.Rows

	op    string
	stmt  string
	start time.Time
	svc   *Service
	// conn is non-nil only under ProfileReads: the checked-out connection whose
	// profiling info describes this query. Released on Close.
	conn   *sql.Conn
	n      int
	closed bool
}

// Next advances the cursor, counting rows the caller actually consumed.
func (r *lakeRows) Next() bool {
	if r.Rows.Next() {
		r.n++
		return true
	}
	return false
}

// Close records the probe's observations and releases the connection. Idempotent:
// call sites defer Close and some also close on an early error return.
func (r *lakeRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Read profiling BEFORE closing the rows: the profiling info belongs to the
	// last query executed on the connection, and closing the result set first
	// leaves it intact, but releasing the connection does not.
	if r.conn != nil {
		r.observeProfile()
	}
	err := r.Rows.Close()
	if r.conn != nil {
		_ = r.conn.Close() // return it to the pool
	}

	lakeRowsReturned.WithLabelValues(r.op).Observe(float64(r.n))

	if d := time.Since(r.start); d >= r.svc.slowReadThreshold() {
		zlog.Warn().
			Str("op", r.op).
			Dur("duration", d).
			Int("rows_returned", r.n).
			Str("stmt", fingerprintStmt(r.stmt)).
			Msg("slow lake read")
	}
	return err
}

// observeProfile pulls DuckDB's profiling tree for the query just executed on
// this connection and records the scan-volume metrics. Best-effort throughout:
// profiling is a diagnostic, and a malformed or missing tree must never fail a
// read that already produced correct rows.
//
// The recover() is load-bearing, not defensive boilerplate. duckdb-go's
// GetProfilingInfo type-asserts the raw driver connection to its own *Conn
// WITHOUT the comma-ok form (profiling.go:25), so any wrapper around the
// connection panics it. dq wraps every query-backend connection in
// recoveringConn for poison recovery — which took down live GraphQL requests
// the first time this shipped, because the panic unwound out of Close() past
// the row-count observation and into the request's recovery middleware. A
// diagnostic must never be able to do that, whatever the next incompatibility
// turns out to be.
func (r *lakeRows) observeProfile() {
	defer func() {
		if p := recover(); p != nil {
			// Stop trying for the life of the process: the causes are structural
			// (driver/wrapper shape), not transient, so retrying just burns a
			// panic per read.
			if r.svc.profileBroken.CompareAndSwap(false, true) {
				profileDisabledTotal.Inc()
				zlog.Error().Interface("panic", p).
					Msg("DuckDB profiling panicked; disabling dq_lake_files_read/rows_scanned for this process (reads are unaffected)")
			}
		}
	}()
	info, err := duckdb.GetProfilingInfo(r.conn)
	if err != nil {
		return
	}
	scanned, files, ok := summarizeProfile(info)
	if !ok {
		return
	}
	lakeRowsScanned.WithLabelValues(r.op).Observe(float64(scanned))
	if files > 0 {
		lakeFilesRead.WithLabelValues(r.op).Observe(float64(files))
	}
}

// filesReadRE extracts DuckLake's file count from a scan operator's EXTRA_INFO,
// which renders it as a free-text "Total Files Read: N" line. Parsed rather than
// read as a first-class metric because DuckDB exposes no typed counter for it;
// a miss simply suppresses dq_lake_files_read for that read.
var filesReadRE = regexp.MustCompile(`Total Files Read:\s*([0-9]+)`)

// summarizeProfile walks the profiling tree, summing rows scanned over every
// operator that reports them and taking the largest reported file count (the
// table scan; a plan with several scans reports each, and the widest is the one
// that characterises the read). ok is false when the tree carried neither.
func summarizeProfile(info duckdb.ProfilingInfo) (scanned, files int64, ok bool) {
	if v, err := strconv.ParseInt(strings.TrimSpace(info.Metrics["OPERATOR_ROWS_SCANNED"]), 10, 64); err == nil {
		scanned += v
		ok = true
	}
	if m := filesReadRE.FindStringSubmatch(info.Metrics["EXTRA_INFO"]); m != nil {
		if v, err := strconv.ParseInt(m[1], 10, 64); err == nil && v > files {
			files = v
			ok = true
		}
	}
	for _, child := range info.Children {
		cs, cf, cok := summarizeProfile(child)
		scanned += cs
		if cf > files {
			files = cf
		}
		ok = ok || cok
	}
	return scanned, files, ok
}

// profileDisabledTotal counts processes that turned profiling off after it
// panicked. Non-zero means dq_lake_files_read / dq_lake_rows_scanned are
// silently absent rather than simply unobserved — the distinction a missing
// series cannot express on its own.
var profileDisabledTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_lake_profile_disabled_total",
	Help: "Processes that disabled DuckDB read profiling after it panicked; the scan-volume metrics are absent, not merely unobserved.",
})

// slowReadThreshold is the configured slow-read log threshold, or the default.
func (s *Service) slowReadThreshold() time.Duration {
	if s.slowRead > 0 {
		return s.slowRead
	}
	return DefaultSlowReadThreshold
}

// queryLake runs stmt as lake read `op` and returns rows wrapped in the probe.
// It is the single entry point every timed lake read goes through, so the
// probe's observations cannot drift out of sync with dq_lake_read_seconds the
// way per-call-site instrumentation would.
//
// Under ProfileReads the query runs on a connection checked out for its
// lifetime so DuckDB's profiling info can be read back against it; otherwise it
// goes through the pool exactly as before.
//
// rowserrcheck is suppressed on both query calls: the linter tracks a *sql.Rows
// only within the function that opened it, and this function's whole job is to
// hand it back. Err() is checked by the caller that drains it — every call site
// already ends its scan loop with `return ..., rows.Err()` or an explicit check.
func (s *Service) queryLake(ctx context.Context, op, stmt string, args ...any) (*lakeRows, error) {
	start := time.Now()
	if !s.profileReads || s.profileBroken.Load() {
		rows, err := s.db.QueryContext(ctx, stmt, args...) //nolint:rowserrcheck // returned to the caller, which checks Err
		if err != nil {
			return nil, err
		}
		return &lakeRows{Rows: rows, op: op, stmt: stmt, start: start, svc: s}, nil
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, stmt, args...) //nolint:rowserrcheck // returned to the caller, which checks Err
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &lakeRows{Rows: rows, op: op, stmt: stmt, start: start, svc: s, conn: conn}, nil
}
