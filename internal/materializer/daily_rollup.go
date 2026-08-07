package materializer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/service/duck"
)

// The daily rollup refresh (dq#55, step 1).
//
// The per-pass incremental fold (captureRollupDelta + foldSignalsRollup) keeps
// lake.signals_latest continuously exact, but its lake.signals scans carry a
// correlated bound (s.timestamp >= prev_ts) that cannot drive static partition
// pruning — every span re-opens roughly the day partition's current file set,
// three times, and the file count grows all day. That is the daily pass-duration
// sawtooth. Since the KV serves signalsLatest (LATEST_KV_READ_MODE=serve), the
// rollup's remaining jobs — KV bootstrap/reconcile source, rare fallback — have
// no freshness SLA of their own, so the plan is to refresh it once daily from a
// WATERMARK: a constant timestamp literal that prunes the fold to the settled
// day partition.
//
// Step 1 (this file) ships the mechanism WITHOUT touching the serving table:
// a shadow table, lake.signals_latest_daily, is maintained exclusively by the
// daily refresh while the per-pass fold keeps maintaining lake.signals_latest.
// A diff pass after each refresh compares the two over the settled window —
// the production differential evidence that gates the step-4 flip (at which
// point the validated shadow table is promoted and the fold removed).
//
// Invariant (the induction base every fold step relies on): after a refresh to
// watermark W, lake.signals_latest_daily is EXACTLY rollupSelectSQL over
// lake.signals restricted to timestamp < W. Three mechanisms preserve it:
//
//   - the seed: a full bounded recompute (timestamp < W), bucket-chunked;
//   - the daily fold: rollupSelectSQL over [W_old, W_new) merged onto the
//     table. Every tail row's timestamp is >= W_old and every stored row's
//     last_seen is < W_old, so the tail wins recency unconditionally — no
//     cross-boundary tie-break exists, and within the tail the QUALIFY dedup
//     matches the recompute. count adds disjoint ranges; first_seen min-folds.
//     The watermark advances IN THE SAME TRANSACTION, so a refresh is
//     exactly-once: a crash before commit reruns identically, after commit the
//     next refresh targets the next boundary.
//   - the late set: rows can ARRIVE stamped before W (devices buffer offline
//     and upload later) — invisible to every future [W, W') fold. The commit
//     path records their subjects durably in lake.rollup_late_subjects (same
//     transaction as the base insert), and the refresh recomputes those
//     subjects bounded to timestamp < W_new, then clears them. Without this
//     the shadow table would be stale for a buffered-upload subject not for
//     <=24h but forever.
const (
	// dailyWatermarkPartition is the lake.ingest_progress key holding the daily
	// rollup watermark — an RFC3339 UTC-midnight partition boundary. Stored
	// beside the snapshot cursor because ingest_progress is already the
	// catalog's (partition, cursor) progress table, and the same-transaction
	// advance gives the refresh its exactly-once property.
	dailyWatermarkPartition = "lake.signals_latest#daily_watermark"
	// dailyRollupTable is the shadow rollup maintained by the daily refresh in
	// step 1 (mode shadow). The step-4 flip promotes it to lake.signals_latest.
	dailyRollupTable = "lake.signals_latest_daily"
	// lateSubjectsTable records subjects that committed base rows stamped
	// BEFORE the current watermark (late arrivals). Written in the decode
	// commit transaction (crash-atomic with the rows themselves), read and
	// cleared by the refresh. Normally empty; duplicates are harmless (the
	// refresh reads DISTINCT).
	lateSubjectsTable = "lake.rollup_late_subjects"
)

// defaultDailyRollupDelay is how long after the UTC-midnight rollover the
// refresh waits before folding the just-settled partition: long enough for the
// cursor to settle past midnight and for stragglers to land (~03:30 UTC is
// also this node's traffic trough), short enough to stay well inside the day.
const defaultDailyRollupDelay = 3*time.Hour + 30*time.Minute

// DailyRollupMode gates the daily signals_latest refresh (dq#55).
type DailyRollupMode string

const (
	// DailyRollupOff disables the daily refresh entirely (default).
	DailyRollupOff DailyRollupMode = "off"
	// DailyRollupShadow maintains lake.signals_latest_daily by daily refresh
	// while the per-pass fold keeps maintaining lake.signals_latest, and diffs
	// the two after each refresh — the production differential evidence that
	// gates the flip. Serving is untouched.
	DailyRollupShadow DailyRollupMode = "shadow"
)

// ParseDailyRollupMode validates a MATERIALIZER_DAILY_ROLLUP_MODE value; empty
// means off. Mode "on" (daily refresh maintains lake.signals_latest itself,
// per-pass fold off) is dq#55 step 4 and deliberately not parseable yet.
func ParseDailyRollupMode(s string) (DailyRollupMode, bool) {
	switch DailyRollupMode(s) {
	case "", DailyRollupOff:
		return DailyRollupOff, true
	case DailyRollupShadow:
		return DailyRollupShadow, true
	}
	return DailyRollupOff, false
}

// WithDailyRollup configures the daily rollup refresh (dq#55). delay <= 0 uses
// defaultDailyRollupDelay. Returns m for chaining.
func (m *DuckLakeMaterializer) WithDailyRollup(mode DailyRollupMode, delay time.Duration) *DuckLakeMaterializer {
	if delay <= 0 {
		delay = defaultDailyRollupDelay
	}
	m.dailyMode = mode
	m.dailyDelay = delay
	return m
}

// dailyConfigured reports whether the daily refresh is enabled at all.
func (m *DuckLakeMaterializer) dailyConfigured() bool {
	return m.dailyMode != "" && m.dailyMode != DailyRollupOff
}

// dailyActive reports whether the HOT PATH must record late subjects: only
// once state is loaded and a watermark exists (before the first refresh there
// is no boundary to be late against — the seed recomputes everything anyway).
func (m *DuckLakeMaterializer) dailyActive() bool {
	return m.dailyConfigured() && m.dailyStateLoaded && !m.dailyWatermark.IsZero()
}

// LoadDailyRollupState ensures the daily-refresh tables exist and loads the
// watermark. Called once from the Runner before the decode loop (and lazily by
// MaybeDailyRollupRefresh if that call failed); a no-op when the mode is off.
func (m *DuckLakeMaterializer) LoadDailyRollupState(ctx context.Context) error {
	if !m.dailyConfigured() || m.dailyStateLoaded {
		return nil
	}
	daily, err := m.tableExists(ctx, "lake", "signals_latest_daily")
	if err != nil {
		return err
	}
	if !daily {
		if err := m.createDailyTable(ctx); err != nil {
			return err
		}
	}
	if err := m.execRetryConflict(ctx, "CREATE TABLE IF NOT EXISTS "+lateSubjectsTable+" (subject VARCHAR)"); err != nil {
		return fmt.Errorf("ensuring daily rollup objects: %w", err)
	}
	w, err := m.loadDailyWatermark(ctx)
	if err != nil {
		return err
	}
	m.dailyWatermark = w
	if !w.IsZero() {
		dailyRollupWatermark.Set(float64(w.Unix()))
	}
	m.dailyLateSeen = map[string]struct{}{}
	m.dailyStateLoaded = true
	return nil
}

// createDailyTable creates the shadow table with EXPLICIT DDL — a column-level
// copy of lake.signals_latest's creation statement (setupStatements), and
// deliberately NOT a zero-row CTAS. The original `CREATE ... AS SELECT * FROM
// lake.signals_latest WHERE false` left degenerate inlined-data state on the
// production catalog (Postgres, din's data inlining on): the table's very
// first scan — the seed's bucket-0 DELETE — died inside
// DuckLakeInlinedDataReader::TryInitializeScan with "Attempted to access index
// 0 within vector of size 0" (the ducklake#281 error family, still present at
// v1.5.4), invalidating the embedded database and restart-looping the writer
// (2026-08-07). Every other lake table is created with plain DDL and scans
// fine under inlining; the shadow table now matches. The partition ALTER must
// run only at first creation (re-ALTERing is a crash — see setupStatements).
func (m *DuckLakeMaterializer) createDailyTable(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + dailyRollupTable + ` (
			subject VARCHAR, subject_bucket INTEGER, name VARCHAR,
			"timestamp" TIMESTAMP WITH TIME ZONE,
			value_number DOUBLE, value_string VARCHAR,
			loc_lat DOUBLE, loc_lon DOUBLE, loc_hdop DOUBLE, loc_heading DOUBLE,
			loc_ts TIMESTAMP WITH TIME ZONE,
			count BIGINT, first_seen TIMESTAMP WITH TIME ZONE, last_seen TIMESTAMP WITH TIME ZONE)`,
		"ALTER TABLE " + dailyRollupTable + " SET PARTITIONED BY (subject_bucket)",
	}
	for _, s := range stmts {
		if err := m.execRetryConflict(ctx, s); err != nil {
			return fmt.Errorf("creating daily rollup table: %w", err)
		}
	}
	return nil
}

// execRetryConflict runs one DDL/DML statement, retrying commit conflicts —
// the same courtesy ensureSchema extends to din racing catalog maintenance.
func (m *DuckLakeMaterializer) execRetryConflict(ctx context.Context, stmt string) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if _, err = m.db.ExecContext(ctx, stmt); err == nil || !isCommitConflict(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	return err
}

// loadDailyWatermark reads the stored boundary; zero means "never refreshed"
// (the next refresh seeds). An unparseable value is an error, not a silent
// reseed — a reseed is a full-history recompute and must be operator-visible.
func (m *DuckLakeMaterializer) loadDailyWatermark(ctx context.Context) (time.Time, error) {
	var raw string
	err := m.db.QueryRowContext(ctx,
		"SELECT cursor FROM lake.ingest_progress WHERE partition = ?", dailyWatermarkPartition).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("loading daily rollup watermark: %w", err)
	}
	w, perr := time.Parse(time.RFC3339, raw)
	if perr != nil {
		return time.Time{}, fmt.Errorf("unparseable daily rollup watermark %q: %w", raw, perr)
	}
	return w.UTC(), nil
}

// dailyRefreshBoundary returns the boundary W_new a refresh should target at
// `now`, or zero when none is due. The boundary is always a UTC-midnight
// partition rollover — the rollup/tail split must land exactly on a partition
// edge (that is what makes the fold's predicate prune statically and the count
// ranges disjoint) — and only becomes due `delay` after it passes, so the
// cursor has settled and the partition has stopped growing. A watermark more
// than one day behind folds every missed day in one refresh (idempotent
// catch-up; no operator memory needed).
func dailyRefreshBoundary(now, watermark time.Time, delay time.Duration) time.Time {
	now = now.UTC()
	boundary := now.Truncate(24 * time.Hour)
	if now.Sub(boundary) < delay {
		boundary = boundary.Add(-24 * time.Hour)
	}
	if !watermark.IsZero() && !watermark.Before(boundary) {
		return time.Time{}
	}
	return boundary
}

// MaybeDailyRollupRefresh runs the daily refresh if one is due. Called from
// the Runner's caught-up branch — "caught up" is the settled-cursor condition
// the boundary math assumes (everything that has ARRIVED is committed), and it
// keeps the refresh off the mid-drain path. Single-writer: decode goroutine
// only, like every other rollup entry point.
func (m *DuckLakeMaterializer) MaybeDailyRollupRefresh(ctx context.Context) error {
	if !m.dailyConfigured() || m.backfillMode {
		return nil
	}
	if err := m.LoadDailyRollupState(ctx); err != nil {
		return err
	}
	boundary := dailyRefreshBoundary(time.Now(), m.dailyWatermark, m.dailyDelay)
	if boundary.IsZero() {
		return nil
	}
	return m.RunDailyRollupRefresh(ctx, boundary)
}

// RunDailyRollupRefresh refreshes the daily rollup to the given UTC-midnight
// boundary: seed (no watermark yet) or incremental fold + late-set recompute.
// Exported for tests and one-shot drivers; production cadence comes from
// MaybeDailyRollupRefresh. boundary must be later than the stored watermark.
func (m *DuckLakeMaterializer) RunDailyRollupRefresh(ctx context.Context, boundary time.Time) error {
	if err := m.LoadDailyRollupState(ctx); err != nil {
		return err
	}
	boundary = boundary.UTC()
	// The boundary is a partition edge BY INVARIANT, not convention: the
	// rollup/tail split (and the count-range disjointness every consumer of the
	// watermark relies on) only holds when W lands exactly on the day-partition
	// rollover. Enforce it rather than document it.
	if !boundary.Equal(boundary.Truncate(24 * time.Hour)) {
		return fmt.Errorf("daily rollup boundary %s is not a UTC-midnight partition edge", boundary.Format(time.RFC3339))
	}
	// Never fold backwards: a stale caller (or a clock excursion) must not move
	// the watermark down — the CAS below would happily let it, and every count
	// after that would double-fold the re-covered window.
	if !m.dailyWatermark.IsZero() && !boundary.After(m.dailyWatermark) {
		return nil
	}
	start := time.Now()
	var err error
	if m.dailyWatermark.IsZero() {
		err = m.seedDailyRollup(ctx, boundary)
	} else {
		err = m.foldDailyRollup(ctx, m.dailyWatermark, boundary)
	}
	dailyRollupRefreshSeconds.Set(time.Since(start).Seconds())
	if err != nil {
		dailyRollupRefreshTotal.WithLabelValues("error").Inc()
		return err
	}
	m.dailyWatermark = boundary
	// The in-memory dedup guard exists to skip re-INSERTing subjects already
	// persisted for THIS window; the table was just cleared, so reset it.
	m.dailyLateSeen = map[string]struct{}{}
	dailyRollupWatermark.Set(float64(boundary.Unix()))
	dailyRollupRefreshTotal.WithLabelValues("ok").Inc()
	m.log.Info().Time("watermark", boundary).Dur("took", time.Since(start)).
		Msg("daily signals_latest refresh complete")
	if m.dailyMode == DailyRollupShadow {
		// Diff is evidence, not correctness: a failure must not fail the refresh
		// (the watermark has advanced; rerunning the fold would double-fold).
		if _, derr := m.DailyRollupDiff(ctx); derr != nil {
			m.log.Error().Err(derr).Msg("daily rollup shadow diff failed; no comparison this cycle")
		}
	}
	return nil
}

// seedDailyRollup establishes the induction base: the daily table becomes
// exactly rollupSelectSQL over timestamp < boundary. The table is DROPPED and
// recreated rather than per-bucket DELETEd — the seed must never scan the
// table it is establishing (an empty or half-seeded table's scan is where the
// inlined-reader crash lived, see createDailyTable; a drop is catalog-only),
// and it makes the seed self-healing over ANY damaged prior state, including
// the poisoned CTAS table the first rollout left behind. Then bucket-chunked
// INSERTs like RecomputeRollup (one txn per bucket, memory-bounded over deep
// history). The watermark is written only after every bucket committed, so a
// crash mid-seed simply reseeds from the drop. This is the RecomputeRollup
// cost class, run once at enable (and on operator reseed: delete the
// watermark row).
func (m *DuckLakeMaterializer) seedDailyRollup(ctx context.Context, boundary time.Time) error {
	m.log.Info().Time("boundary", boundary).
		Msg("seeding lake.signals_latest_daily (bounded full recompute; one-time, O(history))")
	if err := m.execRetryConflict(ctx, "DROP TABLE IF EXISTS "+dailyRollupTable); err != nil {
		return fmt.Errorf("daily seed drop: %w", err)
	}
	if err := m.createDailyTable(ctx); err != nil {
		return err
	}
	bound := fmt.Sprintf(`"timestamp" < make_timestamp(%d)`, boundary.UnixMicro())
	for b := 0; b < duck.NumLatestBuckets; b++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		where := fmt.Sprintf("WHERE subject_bucket = %d AND %s", b, bound)
		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+dailyRollupTable+signalsLatestColumns+rollupSelectSQL(where)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("daily seed bucket %d insert: %w", b, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("daily seed bucket %d commit: %w", b, err)
		}
	}
	// Late rows accrued before/during the seed are covered by the seed's own
	// bound (it recomputed everything < boundary), so clear them with the
	// watermark write — atomically, so a crash between the two cannot leave a
	// watermark asserting a base the late set contradicts.
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.ingest_progress WHERE partition = ?", dailyWatermarkPartition); err != nil {
		return fmt.Errorf("daily seed watermark delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.ingest_progress (partition, cursor) VALUES (?, ?)",
		dailyWatermarkPartition, boundary.Format(time.RFC3339)); err != nil {
		return fmt.Errorf("daily seed watermark insert: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+lateSubjectsTable); err != nil {
		return fmt.Errorf("daily seed late-set clear: %w", err)
	}
	return tx.Commit()
}

// foldDailyRollup folds [from, to) into the daily table and advances the
// watermark in the same transaction, then recomputes the late set. The tail
// aggregate IS rollupSelectSQL restricted to the window — a constant-literal
// predicate on the partition column's source, so the scan prunes to the
// settled day partition(s) instead of re-deriving bounds per row (the whole
// point of dq#55).
func (m *DuckLakeMaterializer) foldDailyRollup(ctx context.Context, from, to time.Time) error {
	window := fmt.Sprintf(`WHERE "timestamp" >= make_timestamp(%d) AND "timestamp" < make_timestamp(%d)`,
		from.UnixMicro(), to.UnixMicro())
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "CREATE OR REPLACE TEMPORARY TABLE _daily_tail AS "+rollupSelectSQL(window)); err != nil {
		return fmt.Errorf("daily fold tail aggregate: %w", err)
	}
	// Merge tail onto stored rows. Recency needs no tie-break: every stored
	// row's last_seen < from <= every tail row's timestamp, so the tail wins
	// the value part unconditionally (within-tail ties were already resolved
	// by rollupSelectSQL's QUALIFY, identically to the recompute). The
	// location part moves only if the tail saw a nonzero fix (loc_ts > epoch),
	// which is then necessarily newer than the stored fix. count ranges are
	// disjoint by the watermark split, so they sum exactly; first_seen
	// min-folds (least is belt-and-braces — the stored value always wins when
	// present).
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE OR REPLACE TEMPORARY TABLE _daily_merged AS
SELECT t.subject, t.subject_bucket, t.name,
  t."timestamp", t.value_number, t.value_string,
  CASE WHEN t.loc_ts > make_timestamp(0) THEN t.loc_lat     ELSE coalesce(p.loc_lat, 0)     END AS loc_lat,
  CASE WHEN t.loc_ts > make_timestamp(0) THEN t.loc_lon     ELSE coalesce(p.loc_lon, 0)     END AS loc_lon,
  CASE WHEN t.loc_ts > make_timestamp(0) THEN t.loc_hdop    ELSE coalesce(p.loc_hdop, 0)    END AS loc_hdop,
  CASE WHEN t.loc_ts > make_timestamp(0) THEN t.loc_heading ELSE coalesce(p.loc_heading, 0) END AS loc_heading,
  CASE WHEN t.loc_ts > make_timestamp(0) THEN t.loc_ts ELSE coalesce(p.loc_ts, make_timestamp(0)) END AS loc_ts,
  coalesce(p.count, 0) + t.count AS count,
  least(coalesce(p.first_seen, t.first_seen), t.first_seen) AS first_seen,
  t.last_seen AS last_seen
FROM _daily_tail t
LEFT JOIN %s p ON p.subject_bucket = t.subject_bucket AND p.subject = t.subject AND p.name = t.name`,
		dailyRollupTable)); err != nil {
		return fmt.Errorf("daily fold merge: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %[1]s WHERE EXISTS (SELECT 1 FROM _daily_merged n
		  WHERE n.subject_bucket = %[1]s.subject_bucket AND n.subject = %[1]s.subject AND n.name = %[1]s.name)`,
		dailyRollupTable)); err != nil {
		return fmt.Errorf("daily fold delete superseded: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO "+dailyRollupTable+signalsLatestColumns+
			"SELECT subject, subject_bucket, name, \"timestamp\", value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading, loc_ts, count, first_seen, last_seen FROM _daily_merged"); err != nil {
		return fmt.Errorf("daily fold insert: %w", err)
	}
	// The watermark advance rides in the fold's transaction (CAS'd against the
	// old value like the snapshot cursor): "watermark = to" and "the fold for
	// [from, to) landed" are one atomic fact, which is the refresh's
	// exactly-once property.
	res, err := tx.ExecContext(ctx, "UPDATE lake.ingest_progress SET cursor = ? WHERE partition = ? AND cursor = ?",
		to.Format(time.RFC3339), dailyWatermarkPartition, from.Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("daily fold watermark advance: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("daily fold watermark moved out from under the refresh (expected %s)", from.Format(time.RFC3339))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("daily fold commit: %w", err)
	}
	// Late-set recompute runs AFTER the fold on purpose: the fold may have
	// summed tail counts onto a late subject's already-wrong row, and the
	// bounded recompute replaces the whole row with the exact answer
	// regardless of what the fold did. Each chunk clears its late rows in its
	// own transaction, so a crash leaves the remainder marked and the next
	// refresh (next boundary, still a valid bound) heals them.
	return m.recomputeLateDailySubjects(ctx, to)
}

// recomputeLateDailySubjects rebuilds the daily rows of every late-marked
// subject from the base, bounded to timestamp < boundary, and clears the
// marks. A late set larger than maxDirtySubjects means a fleet-scale rewrite
// of history (a bulk backfill) — per-subject recompute is no cheaper than the
// seed at that point, so escalate.
func (m *DuckLakeMaterializer) recomputeLateDailySubjects(ctx context.Context, boundary time.Time) error {
	rows, err := m.db.QueryContext(ctx, "SELECT DISTINCT subject FROM "+lateSubjectsTable)
	if err != nil {
		return fmt.Errorf("reading late subjects: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var subjects []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return fmt.Errorf("scanning late subject: %w", err)
		}
		subjects = append(subjects, s)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("late subject scan: %w", err)
	}
	dailyRollupLateSubjects.Set(float64(len(subjects)))
	if len(subjects) == 0 {
		return nil
	}
	if len(subjects) > m.maxDirtySubjects {
		m.log.Warn().Int("late_subjects", len(subjects)).
			Msg("daily rollup late set overflowed; reseeding the daily table instead of per-subject recompute")
		if err := m.execRetryConflict(ctx, "DELETE FROM "+lateSubjectsTable); err != nil {
			return err
		}
		return m.seedDailyRollup(ctx, boundary)
	}
	byBucket := map[int][]string{}
	for _, s := range subjects {
		b := duck.HashBucket(s)
		byBucket[b] = append(byBucket[b], s)
	}
	buckets := make([]int, 0, len(byBucket))
	for b := range byBucket {
		buckets = append(buckets, b)
	}
	sort.Ints(buckets)
	bound := fmt.Sprintf(`"timestamp" < make_timestamp(%d)`, boundary.UnixMicro())
	for _, b := range buckets {
		bucketSubjects := byBucket[b]
		sort.Strings(bucketSubjects)
		for len(bucketSubjects) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk := bucketSubjects
			if len(chunk) > rollupSubjectChunk {
				chunk = chunk[:rollupSubjectChunk]
			}
			bucketSubjects = bucketSubjects[len(chunk):]
			if err := m.recomputeDailyChunk(ctx, b, chunk, bound); err != nil {
				return fmt.Errorf("daily late recompute bucket %d (%d subjects): %w", b, len(chunk), err)
			}
		}
	}
	return nil
}

// recomputeDailyChunk DELETEs+recomputes one bucket's given subjects in the
// daily table (bounded by `bound`) and clears their late marks, all in one
// transaction — the marks disappear exactly when the recompute that makes
// them unnecessary lands.
func (m *DuckLakeMaterializer) recomputeDailyChunk(ctx context.Context, bucket int, subjects []string, bound string) error {
	args := make([]any, len(subjects))
	marks := make([]string, len(subjects))
	for i, s := range subjects {
		args[i] = s
		marks[i] = "?"
	}
	in := strings.Join(marks, ", ")
	where := fmt.Sprintf("WHERE subject_bucket = %d AND subject IN (%s) AND %s", bucket, in, bound)
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE subject_bucket = %d AND subject IN (%s)", dailyRollupTable, bucket, in), args...); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+dailyRollupTable+signalsLatestColumns+rollupSelectSQL(where), args...); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE subject IN (%s)", lateSubjectsTable, in), args...); err != nil {
		return fmt.Errorf("clear late marks: %w", err)
	}
	return tx.Commit()
}

// lateDailyInsert builds the in-transaction INSERT recording this batch's
// late subjects (rows stamped before the current watermark). Returns ("",
// nil) when there are none — the common case, costing the hot path one map
// probe per distinct late subject and nothing else. Subjects already
// persisted since the last refresh (dailyLateSeen, updated post-commit in
// markDirtyFromBatch) are skipped; a crash between INSERT and the seen-mark
// only costs a harmless duplicate row.
func (m *DuckLakeMaterializer) lateDailyInsert(signals []SignalRow) (string, []any) {
	var args []any
	seen := map[string]struct{}{}
	for i := range signals {
		if !signals[i].Timestamp.Before(m.dailyWatermark) {
			continue
		}
		s := signals[i].Subject
		if _, ok := m.dailyLateSeen[s]; ok {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		args = append(args, s)
	}
	if len(args) == 0 {
		return "", nil
	}
	values := strings.TrimSuffix(strings.Repeat("(?), ", len(args)), ", ")
	return "INSERT INTO " + lateSubjectsTable + " (subject) VALUES " + values, args
}

// markLateDailySeen updates the post-commit dedup set (see lateDailyInsert).
// Bounded like the dirty sets: on overflow the map resets — the only cost is
// duplicate rows in the late table, which the refresh reads DISTINCT.
func (m *DuckLakeMaterializer) markLateDailySeen(signals []SignalRow) {
	for i := range signals {
		if signals[i].Timestamp.Before(m.dailyWatermark) {
			m.dailyLateSeen[signals[i].Subject] = struct{}{}
		}
	}
	if len(m.dailyLateSeen) > m.maxDirtySubjects {
		m.dailyLateSeen = map[string]struct{}{}
	}
}

// PersistDailyLateSubjects records the current dirty set into the late table —
// the bulk-backfill hand-off (RunBackfill dirties every touched subject; those
// rows are late by definition since backfills rewrite history). If the dirty
// set overflowed into a full-rebuild escalation the touched set is unknown, so
// drop the watermark instead: the next refresh reseeds. Call before
// FlushRollup (which drains the dirty set).
func (m *DuckLakeMaterializer) PersistDailyLateSubjects(ctx context.Context) error {
	if !m.dailyConfigured() {
		return nil
	}
	if err := m.LoadDailyRollupState(ctx); err != nil {
		return err
	}
	if m.rollupFullRebuild {
		if err := m.execRetryConflict(ctx, fmt.Sprintf(
			"DELETE FROM lake.ingest_progress WHERE partition = %s", sqlLit(dailyWatermarkPartition))); err != nil {
			return fmt.Errorf("dropping daily watermark for reseed: %w", err)
		}
		m.dailyWatermark = time.Time{}
		m.log.Warn().Msg("backfill overflowed the dirty set; daily rollup watermark dropped, next refresh reseeds")
		return nil
	}
	subjects := make([]string, 0, len(m.dirtySubjects))
	for s := range m.dirtySubjects {
		subjects = append(subjects, s)
	}
	sort.Strings(subjects)
	for len(subjects) > 0 {
		chunk := subjects
		if len(chunk) > rollupSubjectChunk {
			chunk = chunk[:rollupSubjectChunk]
		}
		subjects = subjects[len(chunk):]
		args := make([]any, len(chunk))
		for i, s := range chunk {
			args[i] = s
		}
		values := strings.TrimSuffix(strings.Repeat("(?), ", len(chunk)), ", ")
		if _, err := m.db.ExecContext(ctx,
			"INSERT INTO "+lateSubjectsTable+" (subject) VALUES "+values, args...); err != nil {
			return fmt.Errorf("persisting backfill late subjects: %w", err)
		}
	}
	return nil
}

// DailyRollupDiff compares the daily table against the live rollup over the
// settled window (live rows with last_seen < watermark; fresher live rows are
// legitimately ahead of the daily table and excluded). This is the
// production differential evidence for the dq#55 flip: classes are
// missing_daily (live has a settled row the daily table lacks), missing_live
// (the daily table has a row live lacks — also what retention-prune drift
// looks like), and mismatch (any column differs). Zero across days of real
// traffic is the gate.
func (m *DuckLakeMaterializer) DailyRollupDiff(ctx context.Context) (map[string]int, error) {
	start := time.Now()
	stmt := fmt.Sprintf(`
SELECT
  CASE WHEN l.subject IS NULL THEN 'missing_live'
       WHEN d.subject IS NULL THEN 'missing_daily'
       ELSE 'mismatch' END AS class,
  coalesce(l.subject, d.subject) AS subject, coalesce(l.name, d.name) AS name
FROM lake.signals_latest l
FULL OUTER JOIN %s d ON d.subject = l.subject AND d.name = l.name
WHERE (l.subject IS NULL OR l.last_seen < make_timestamp(%d))
  AND (l.subject IS NULL OR d.subject IS NULL
    OR l.count != d.count
    OR l."timestamp" != d."timestamp"
    OR l.value_number IS DISTINCT FROM d.value_number
    OR l.value_string IS DISTINCT FROM d.value_string
    OR l.loc_lat != d.loc_lat OR l.loc_lon != d.loc_lon
    OR l.loc_hdop != d.loc_hdop OR l.loc_heading != d.loc_heading
    OR coalesce(l.loc_ts, make_timestamp(0)) != coalesce(d.loc_ts, make_timestamp(0))
    OR l.first_seen != d.first_seen OR l.last_seen != d.last_seen)`,
		dailyRollupTable, m.dailyWatermark.UnixMicro())
	rows, err := m.db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("daily rollup diff: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	counts := map[string]int{}
	logged := 0
	for rows.Next() {
		var class, subject, name string
		if err := rows.Scan(&class, &subject, &name); err != nil {
			return nil, fmt.Errorf("scanning diff row: %w", err)
		}
		counts[class]++
		if logged < 10 {
			logged++
			m.log.Warn().Str("class", class).Str("subject", subject).Str("name", name).
				Msg("daily rollup shadow diff: daily table disagrees with the incremental rollup")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("diff scan: %w", err)
	}
	for _, class := range []string{"missing_daily", "missing_live", "mismatch"} {
		dailyRollupDiffRows.WithLabelValues(class).Set(float64(counts[class]))
	}
	total := counts["missing_daily"] + counts["missing_live"] + counts["mismatch"]
	m.log.Info().Int("diff_rows", total).Dur("took", time.Since(start)).
		Msg("daily rollup shadow diff complete")
	return counts, nil
}
