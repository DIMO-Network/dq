package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

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

// lakeFilesRead is the parquet data-file count one read opened. This is THE
// number for a lake whose partition key does not match the query's filter: a
// subject-scoped read of a (type, day)-partitioned table cannot prune files, so
// it opens every one of them, and on S3 each open is a round trip. Only emitted
// when ProfileReads is on AND the plan reported a file count.
//
// There is deliberately no companion rows-scanned metric. DuckDB's
// operator_rows_scanned is not a row count in any usable sense — measured at
// 25,920,000 for a 3,240,000-row table where the matching subject held 3,600
// rows — so exporting it would publish an authoritative-looking number that
// means something else. Rows RETURNED (above) is the honest denominator.
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
	// conn is non-nil ONLY for a successfully armed profiled read — armedConn
	// either hands back a connection with profiling on and its output pinned, or
	// nothing at all (queryLake then falls back to the pool). So conn != nil is
	// exactly the set of reads that must disarm on release, and profilePath is
	// non-empty whenever it is set.
	conn *sql.Conn
	// profilePath is where DuckDB wrote this query's profiling JSON. Read and
	// removed by observeProfile.
	profilePath string
	n           int
	closed      bool
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

	// Close the rows FIRST: DuckDB writes the profiling file when the query
	// completes, so reading it before the result set is done can race an
	// unwritten or partial file. (The old C-API path had the opposite
	// constraint, which is why the order changed.)
	err := r.Rows.Close()
	if r.conn != nil {
		r.observeProfile()
		// Disarms, then returns the connection to the pool — see
		// Service.releaseArmed for why an armed pooled connection would print later
		// plans (and the subject DIDs in them) to stdout.
		r.svc.releaseArmed(r.conn)
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

// observeProfile parses the profiling JSON DuckDB wrote for the query that just
// ran on this connection, and records the file count.
//
// This goes through SQL (SET enable_profiling='json' + SET profiling_output),
// NOT duckdb-go's GetProfilingInfo, and that choice is the whole point. The C
// API type-asserts the raw driver connection to duckdb-go's own *Conn without
// the comma-ok form (profiling.go:25), so it panics on any wrapped connection —
// and dq wraps every query-backend connection in recoveringConn for poison
// recovery. That panic took down live GraphQL reads on 2026-07-29. Going
// through SQL sidesteps the assertion entirely: by the time this runs the query
// has already returned and we are only reading a file.
//
// The recover() stays regardless. A diagnostic must never be able to fail the
// read it is measuring, whatever the next incompatibility turns out to be.
func (r *lakeRows) observeProfile() {
	defer func() {
		if p := recover(); p != nil {
			if r.svc.profileBroken.CompareAndSwap(false, true) {
				profileDisabledTotal.Inc()
				zlog.Error().Interface("panic", p).
					Msg("DuckDB profiling panicked; disabling dq_lake_files_read for this process (reads are unaffected)")
			}
		}
	}()
	if r.profilePath == "" {
		return // profiling_output could not be set; nothing was written
	}
	// Always remove the file: it carries the plan's filter literals (a subject
	// DID among them) and there is one per profiled read.
	defer func() { _ = os.Remove(r.profilePath) }()

	raw, err := os.ReadFile(r.profilePath)
	if err != nil {
		// No file means the query never completed far enough to emit one
		// (cancelled, failed). Not worth a log per occurrence.
		return
	}
	if files, ok := parseProfileFilesRead(raw); ok {
		lakeFilesRead.WithLabelValues(r.op).Observe(float64(files))
	}
}

// filesReadRE extracts DuckLake's file count from a scan operator's extra_info,
// which renders it as free text ("Total Files Read: N") rather than a typed
// metric. A miss simply suppresses dq_lake_files_read for that read.
var filesReadRE = regexp.MustCompile(`Total Files Read.{0,4}?\s*([0-9]+)`)

// profileNode is the subset of DuckDB's profiling JSON the probe reads.
type profileNode struct {
	ExtraInfo json.RawMessage `json:"extra_info"`
	Children  []profileNode   `json:"children"`
}

// parseProfileFilesRead walks the profiling tree and returns the LARGEST file
// count any scan operator reported. Max rather than sum: a plan with several
// scans reports each, and the widest one characterises the read — summing would
// inflate a self-join into looking like twice the S3 traffic it caused.
func parseProfileFilesRead(raw []byte) (files int64, ok bool) {
	var root profileNode
	if err := json.Unmarshal(raw, &root); err != nil {
		return 0, false
	}
	var walk func(profileNode)
	walk = func(n profileNode) {
		if m := filesReadRE.FindSubmatch(n.ExtraInfo); m != nil {
			if v, err := strconv.ParseInt(string(m[1]), 10, 64); err == nil && v > files {
				files, ok = v, true
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(root)
	return files, ok
}

// profileDisabledTotal counts processes that turned profiling off after it
// panicked. Non-zero means dq_lake_files_read is silently absent rather than
// simply unobserved — a distinction a missing series cannot express on its own.
var profileDisabledTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_lake_profile_disabled_total",
	Help: "Processes that disabled DuckDB read profiling after it panicked; dq_lake_files_read is absent, not merely unobserved.",
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
		return s.queryUnprofiled(ctx, op, stmt, start, args...)
	}

	conn, path, err := s.armedConn(ctx)
	if err != nil {
		// No trustworthy profiled connection. Serve the read the ordinary way
		// rather than failing it: the metric is a diagnostic, the query is the
		// product.
		zlog.Warn().Err(err).Msg("could not arm DuckDB read profiling; serving this read unprofiled")
		return s.queryUnprofiled(ctx, op, stmt, start, args...)
	}
	rows, err := conn.QueryContext(ctx, stmt, args...) //nolint:rowserrcheck // returned to the caller, which checks Err
	if err != nil {
		_ = os.Remove(path)
		s.releaseArmed(conn)
		return nil, err
	}
	return &lakeRows{Rows: rows, op: op, stmt: stmt, start: start, svc: s, conn: conn, profilePath: path}, nil
}

// queryUnprofiled runs stmt through the pool with no profiling state whatsoever,
// which is both the ProfileReads=false path and the fallback whenever profiling
// could not be armed safely.
func (s *Service) queryUnprofiled(ctx context.Context, op, stmt string, start time.Time, args ...any) (*lakeRows, error) {
	rows, err := s.db.QueryContext(ctx, stmt, args...) //nolint:rowserrcheck // returned to the caller, which checks Err
	if err != nil {
		return nil, err
	}
	return &lakeRows{Rows: rows, op: op, stmt: stmt, start: start, svc: s}, nil
}

// customProfilingSettings names the profiling fields the probe reports. Explicit
// because DuckDB's defaults omit EXTRA_INFO, which is the one parseProfileFilesRead
// reads the file count out of.
const customProfilingSettings = `PRAGMA custom_profiling_settings = ` +
	`'{"EXTRA_INFO":"true","OPERATOR_NAME":"true","OPERATOR_TIMING":"true"}'`

// armedConn checks out a connection with profiling armed and its output pinned to
// a fresh scratch path, and returns that path. On any error it leaves nothing
// armed — the connection is disarmed and released, or discarded outright — so a
// caller that gets an error can safely fall back to the ordinary pool.
//
// The two statements must run in THIS order, and it is DuckDB that forces it:
// `SET profiling_output` validates the file extension against the ACTIVE profiling
// type, so pinning a .json path before arming is rejected outright ("Profiler file
// type … must either have the same file extension as the profiling output type").
//
// That ordering opens a window where profiling is armed with profiling_output
// still empty — and empty means "print the plan to stdout", which is exactly how
// the bootstrap-armed version leaked subject DIDs into the container log. The
// window is safe here for one reason only: nothing that produces a plan runs
// inside it. DuckDB does not profile SET/PRAGMA statements (verified against
// duckdb-go v2 on 2026-07-29: stdout stayed empty across the pair), and the
// connection is pinned to this goroutine, so no query can interleave. Do not put
// a query between these two statements.
func (s *Service) armedConn(ctx context.Context) (*sql.Conn, string, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, "", err
	}
	// 'json' (not 'no_output'): the probe reads the tree back as a FILE over SQL,
	// deliberately avoiding duckdb-go's GetProfilingInfo, whose unchecked type
	// assertion panics on dq's wrapped connections.
	if _, err := conn.ExecContext(ctx, "PRAGMA enable_profiling = 'json'"); err != nil {
		_ = conn.Close()
		return nil, "", fmt.Errorf("arming profiling: %w", err)
	}
	path := s.profilePath()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET profiling_output = %s", sqlString(path))); err != nil {
		// Armed-but-unpinned is the one state that must never reach a query: its
		// plan would go to stdout. releaseArmed guarantees the connection does not
		// return to the pool in it.
		s.releaseArmed(conn)
		return nil, "", fmt.Errorf("pinning profiling_output: %w", err)
	}
	if _, err := conn.ExecContext(ctx, customProfilingSettings); err != nil {
		// No EXTRA_INFO means no file count to parse. The connection is armed AND
		// pinned, so the read is still safe to serve — just possibly without
		// dq_lake_files_read.
		zlog.Warn().Err(err).Msg("could not set custom_profiling_settings; dq_lake_files_read may be absent for this read")
	}
	return conn, path, nil
}

// profileDisarmTimeout bounds the disarm PRAGMA. Close carries no context, and a
// hung disarm would pin a connection from a pool sized in single digits.
const profileDisarmTimeout = 5 * time.Second

// releaseArmed disarms profiling and returns conn to the pool. This is the other
// half of the invariant armedConn establishes: a pooled connection left armed
// profiles whatever query runs on it next, and the reads that do not route through
// queryLake (lake_latest.go, lake_rollup.go, latest.go, segments_source.go) never
// pin a profiling_output — so DuckDB would print their plans, filter literals and
// all, to the container log.
//
// If the disarm fails the connection is DISCARDED rather than returned, because
// there is no third option that keeps the invariant: the pool must never hold an
// armed connection. Losing one connection is recoverable; leaking subject DIDs
// into logs is not.
//
// profiling_output is deliberately NOT reset to ”. Leaving it pointing at the
// (already removed) scratch path means that even if profiling were somehow
// re-armed on this connection, the plan lands in one overwritten scratch file
// instead of on stdout.
func (s *Service) releaseArmed(conn *sql.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), profileDisarmTimeout)
	defer cancel()
	if _, err := conn.ExecContext(ctx, "PRAGMA disable_profiling"); err != nil {
		// Latch so no further read arms a connection, and make the degradation
		// visible rather than silent.
		if s.profileBroken.CompareAndSwap(false, true) {
			profileDisabledTotal.Inc()
			zlog.Error().Err(err).
				Msg("could not disarm DuckDB profiling; discarding the connection and disabling dq_lake_files_read for this process (reads are unaffected)")
		}
		// Raw returning ErrBadConn marks the connection bad, so database/sql
		// destroys it on Close instead of handing it to the next query.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	_ = conn.Close()
}

// profileSeq makes each profiled read's output path unique. Per-connection
// would be enough today (the connection is pinned for the read), but a bare
// per-process filename would collide the moment two pods share a mounted temp
// directory, and the failure mode — one read reading another's plan — is a
// silently wrong metric rather than an error.
var profileSeq atomic.Uint64

// profilePath returns the file DuckDB should write this read's profiling JSON
// to. It lives under DUCKDB_TEMP_DIRECTORY when configured (a real volume in
// prod, sized for DuckDB spill) so profiling never writes into an image layer.
func (s *Service) profilePath() string {
	dir := s.tempDir
	if dir == "" {
		dir = os.TempDir()
	}
	return filepath.Join(dir, fmt.Sprintf("dq-profile-%d-%d.json", os.Getpid(), profileSeq.Add(1)))
}
