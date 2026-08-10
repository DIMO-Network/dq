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

// The daily rollup refresh (dq#55) — since step 5, THE maintenance path for
// lake.signals_latest.
//
// History, briefly: the per-pass incremental fold (captureRollupDelta +
// foldSignalsRollup, removed in step 5) kept lake.signals_latest continuously
// exact, but its lake.signals scans carried a correlated bound (s.timestamp >=
// prev_ts) that cannot drive static partition pruning — every span re-opened
// roughly the day partition's current file set, three times, growing all day
// (the daily pass-duration sawtooth). Worse, the fold's DELETE racing din's
// rewrite_data_files compaction silently removed nothing and accumulated
// visible duplicate rows (2026-08-08: 823k rows over 7.7k keys on the live
// table). Since the KV serves signalsLatest (LATEST_KV_READ_MODE=serve), the
// rollup's remaining jobs — KV bootstrap/reconcile source, summaries baseline,
// rare fallback — have no freshness SLA of their own, so it is refreshed once
// daily from a WATERMARK: a constant timestamp literal that prunes the fold to
// the settled day partition. Step 1 (#56) shipped the mechanism against a
// shadow table (lake.signals_latest_daily) with a per-refresh diff as the flip
// evidence — creating that table with plain DDL and never scanning it pre-seed
// (#59), after a zero-row CTAS left degenerate inlined-data state whose first
// scan crashed the ducklake extension (the ducklake#281 family, 2026-08-07);
// the cardinality probe (#62) then quantified the fold-era duplicate
// corruption; step 4 (#63) flipped prod to mode on (promoting the validated
// shadow table, which also remediated that corruption); step 5 removed the
// per-pass fold and the shadow scaffolding. What remains of the shadow era is
// the boot-time promote in LoadDailyRollupState, for a node upgrading straight
// from mode=shadow with a leftover shadow table.
//
// Invariant (the induction base every fold step relies on): after a refresh to
// watermark W, lake.signals_latest is EXACTLY rollupSelectSQL over
// lake.signals restricted to timestamp < W (plus, above W, nothing — the tail
// is the read side's job: the summaries union and the KV serve it). Three
// mechanisms preserve it:
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
//     the rollup would be stale for a buffered-upload subject not for
//     <=24h but forever.
const (
	// dailyWatermarkPartition is the lake.ingest_progress key holding the daily
	// rollup watermark — an RFC3339 UTC-midnight partition boundary. Stored
	// beside the snapshot cursor because ingest_progress is already the
	// catalog's (partition, cursor) progress table, and the same-transaction
	// advance gives the refresh its exactly-once property. The canonical
	// definition lives in duck (the query layer's summaries union reads it
	// too); internal/latestkv carries a documented duplicate (import cycle).
	dailyWatermarkPartition = duck.RollupDailyWatermarkPartition
	// dailyRollupTable is the shadow-era rollup table (dq#55 step 1). It is no
	// longer written; the name survives only for the boot-time promote/drop of
	// a leftover copy in LoadDailyRollupState — after which it is gone forever.
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
	// DailyRollupOff disables the daily refresh entirely. With the per-pass
	// fold removed (dq#55 step 5) NOTHING maintains lake.signals_latest in
	// this mode — it exists for tests and one-off ops only, and
	// LoadDailyRollupState warns when it sees it.
	DailyRollupOff DailyRollupMode = "off"
	// DailyRollupOn (the default): the daily refresh maintains
	// lake.signals_latest — since the dq#55 step-4 flip the only writer of the
	// rollup, and since step 5 the only mechanism that exists. On the first
	// boot after an upgrade straight from mode=shadow, the (validated,
	// one-row-per-key) shadow table is PROMOTED into lake.signals_latest —
	// which is also the remediation for the duplicate-row corruption the
	// shadow diff exposed (2026-08-08: live carried 823k rows over 7.7k keys;
	// the promote discards them). Pair with LAKE_ROLLUP_DAILY_SERVING=true on
	// the query fleet, or summaries under-count the tail.
	DailyRollupOn DailyRollupMode = "on"
)

// ParseDailyRollupMode validates a MATERIALIZER_DAILY_ROLLUP_MODE value. Empty
// means ON: with the per-pass fold gone, an unmaintained rollup must not be
// reachable by default — a node with no explicit mode gets the daily refresh.
// The retired "shadow" value is now invalid so a stale config fails loud at
// boot instead of silently running an unmaintained mode.
func ParseDailyRollupMode(s string) (DailyRollupMode, bool) {
	switch DailyRollupMode(s) {
	case "", DailyRollupOn:
		return DailyRollupOn, true
	case DailyRollupOff:
		return DailyRollupOff, true
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
// MaybeDailyRollupRefresh if that call failed); a no-op when the mode is off —
// but a LOUD one: with the fold gone, off means nothing maintains the rollup.
func (m *DuckLakeMaterializer) LoadDailyRollupState(ctx context.Context) error {
	if !m.dailyConfigured() || m.dailyStateLoaded {
		if m.dailyMode == DailyRollupOff {
			m.log.Warn().Msg("MATERIALIZER_DAILY_ROLLUP_MODE=off: NOTHING maintains lake.signals_latest (the per-pass fold was removed in dq#55 step 5) — tests/one-off ops only")
		}
		return nil
	}
	daily, err := m.tableExists(ctx, "lake", "signals_latest_daily")
	if err != nil {
		return err
	}
	if err := m.execRetryConflict(ctx, "CREATE TABLE IF NOT EXISTS "+lateSubjectsTable+" (subject VARCHAR)"); err != nil {
		return fmt.Errorf("ensuring daily rollup objects: %w", err)
	}
	w, err := m.loadDailyWatermark(ctx)
	if err != nil {
		return err
	}
	// The shadow→on transition (a node upgrading straight from shadow-era
	// config): a leftover shadow table with a valid watermark is the
	// validated, one-row-per-key copy — promote it into lake.signals_latest
	// (and discard whatever the per-pass fold era left there, duplicate-row
	// corruption included). A failure leaves state unloaded, so the next
	// caught-up pass retries; every serving-critical read is KV-backed
	// meanwhile. After the promote the table is gone forever.
	if daily {
		if w.IsZero() {
			// A shadow table without a watermark is an aborted shadow seed —
			// worthless as a promote source. Drop it; the first refresh seeds
			// lake.signals_latest directly.
			m.log.Warn().Msg("shadow table present but no watermark; dropping it (aborted shadow seed) — first refresh will seed lake.signals_latest")
			if err := m.execRetryConflict(ctx, "DROP TABLE IF EXISTS "+dailyRollupTable); err != nil {
				return fmt.Errorf("dropping unwatermarked shadow table: %w", err)
			}
		} else if err := m.promoteDailyRollup(ctx); err != nil {
			return err
		}
	}
	m.dailyWatermark = w
	if !w.IsZero() {
		dailyRollupWatermark.Set(float64(w.Unix()))
	}
	m.dailyLateSeen = map[string]struct{}{}
	m.dailyStateLoaded = true
	// Best-effort at boot so a deploy of this probe answers the cardinality
	// question immediately instead of at the next daily refresh.
	if err := m.observeRollupCardinality(ctx); err != nil {
		m.log.Warn().Err(err).Msg("rollup cardinality probe failed at boot; next daily refresh retries")
	}
	return nil
}

// promoteDailyRollup swaps the validated shadow table's content into
// lake.signals_latest — the dq#55 step-4 cutover, and the remediation for the
// duplicate-row corruption the per-pass-fold era accumulated (2026-08-08:
// 823,520 rows over 7,666 keys, average ~266 visible copies per active key,
// traced to the fold's DELETE racing din's rewrite_data_files compaction).
//
// One transaction — DELETE everything, INSERT the shadow content — so readers
// see the old table until the commit and the new one after; never a dropped
// or partially-filled serving table. The post-swap cardinality check guards
// the exact failure mode that CAUSED the corruption (a DELETE that silently
// removes nothing under a concurrent compaction): if lake.signals_latest is
// not one-row-per-key afterwards, the promote FAILED regardless of what the
// transaction reported, and it must not be treated as done. The shadow table
// is dropped only after verification, so a crash anywhere retries the whole
// promote from an intact source (the DELETE+INSERT is idempotent).
func (m *DuckLakeMaterializer) promoteDailyRollup(ctx context.Context) error {
	m.log.Info().Msg("promoting lake.signals_latest_daily into lake.signals_latest (dq#55 flip)")
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.signals_latest"); err != nil {
		return fmt.Errorf("promote delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO lake.signals_latest"+signalsLatestColumns+
			"SELECT subject, subject_bucket, name, \"timestamp\", value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading, loc_ts, count, first_seen, last_seen FROM "+dailyRollupTable); err != nil {
		return fmt.Errorf("promote insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("promote commit: %w", err)
	}
	var keys, rows int64
	if err := m.db.QueryRowContext(ctx,
		`SELECT count(*), coalesce(sum(n), 0) FROM (SELECT count(*) AS n FROM lake.signals_latest GROUP BY subject, name)`).
		Scan(&keys, &rows); err != nil {
		return fmt.Errorf("promote verification: %w", err)
	}
	if keys != rows {
		return fmt.Errorf("promote verification failed: lake.signals_latest has %d rows over %d keys — the DELETE did not fully apply (the compaction race); retrying next pass", rows, keys)
	}
	if err := m.execRetryConflict(ctx, "DROP TABLE IF EXISTS "+dailyRollupTable); err != nil {
		return fmt.Errorf("dropping promoted shadow table: %w", err)
	}
	m.log.Info().Int64("keys", keys).Msg("shadow table promoted; lake.signals_latest is one-row-per-key and daily-maintained")
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
	// The cardinality probe is evidence, not correctness: a failure must not
	// fail the refresh (the watermark has advanced; rerunning the fold would
	// double-fold). It fires every refresh — the STANDING proof the fold-era
	// duplicate corruption stays gone (dq#64).
	if err := m.observeRollupCardinality(ctx); err != nil {
		m.log.Error().Err(err).Msg("rollup cardinality probe failed; no corruption check this cycle")
	}
	return nil
}

// seedDailyRollup establishes the induction base: lake.signals_latest becomes
// exactly rollupSelectSQL over timestamp < boundary. The table is being SERVED
// — never drop it. It is cleared transactionally instead: readers see the old
// content until the commit, then a briefly-empty rollup that fills bucket by
// bucket — the same partial visibility the LAKE_REBUILD_ROLLUP_ON_BOOT
// recovery has always had, and every serving-critical read is KV-backed
// anyway. Then bucket-chunked INSERTs like RecomputeRollup (one txn per
// bucket, memory-bounded over deep history). The watermark is written only
// after every bucket committed, so a crash mid-seed simply reseeds from the
// clear. This is the RecomputeRollup cost class, run once at enable (and on
// operator reseed: delete the watermark row).
func (m *DuckLakeMaterializer) seedDailyRollup(ctx context.Context, boundary time.Time) error {
	m.log.Info().Time("boundary", boundary).
		Msg("seeding the daily rollup (bounded full recompute; one-time, O(history))")
	if err := m.execRetryConflict(ctx, "DELETE FROM lake.signals_latest"); err != nil {
		return fmt.Errorf("daily seed clear: %w", err)
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
		if _, err := tx.ExecContext(ctx, "INSERT INTO lake.signals_latest"+signalsLatestColumns+rollupSelectSQL(where)); err != nil {
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

// foldDailyRollup folds [from, to) into lake.signals_latest and advances the
// watermark in the same transaction, then recomputes the late set. The tail
// aggregate IS rollupSelectSQL restricted to the window — a constant-literal
// predicate on the partition column's source, so the scan prunes to the
// settled day partition(s) instead of re-deriving bounds per row (the whole
// point of dq#55).
func (m *DuckLakeMaterializer) foldDailyRollup(ctx context.Context, from, to time.Time) error {
	const target = "lake.signals_latest"
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
		target)); err != nil {
		return fmt.Errorf("daily fold merge: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %[1]s WHERE EXISTS (SELECT 1 FROM _daily_merged n
		  WHERE n.subject_bucket = %[1]s.subject_bucket AND n.subject = %[1]s.subject AND n.name = %[1]s.name)`,
		target)); err != nil {
		return fmt.Errorf("daily fold delete superseded: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO "+target+signalsLatestColumns+
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
			Msg("daily rollup late set overflowed; reseeding lake.signals_latest instead of per-subject recompute")
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

// recomputeDailyChunk DELETEs+recomputes one bucket's given subjects in
// lake.signals_latest (bounded by `bound`) and clears their late marks, all in
// one transaction — the marks disappear exactly when the recompute that makes
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
		fmt.Sprintf("DELETE FROM lake.signals_latest WHERE subject_bucket = %d AND subject IN (%s)", bucket, in), args...); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.signals_latest"+signalsLatestColumns+rollupSelectSQL(where), args...); err != nil {
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

// observeRollupCardinality measures how many physical lake.signals_latest
// rows exist per (subject, name) key — the rollup contract is EXACTLY ONE.
// Added when the 2026-08-08 shadow diff hit 300k all-mismatch rows over a
// ≤39k-key fleet: the cause was the per-pass fold's DELETE racing din's
// rewrite_data_files compaction (the delete landed on rows whose files were
// just rewritten and removed nothing; din's 2026-08-06 flush conflict is the
// same collision seen from the other side). Duplicate keys mean
// dataSummary/rollup-fallback reads serve duplicated rows; the daily refresh
// (one fold/day, no per-pass DELETE) is the structural fix, and this gauge is
// the standing proof the corruption stays gone.
func (m *DuckLakeMaterializer) observeRollupCardinality(ctx context.Context) error {
	var keys, rows, dupKeys, dupRows int64
	if err := m.db.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(n), 0),
		count(*) FILTER (WHERE n > 1), coalesce(sum(n) FILTER (WHERE n > 1), 0)
		FROM (SELECT count(*) AS n FROM lake.signals_latest GROUP BY subject, name)`).
		Scan(&keys, &rows, &dupKeys, &dupRows); err != nil {
		return fmt.Errorf("cardinality of lake.signals_latest: %w", err)
	}
	dailyRollupSideRows.WithLabelValues("live", "keys").Set(float64(keys))
	dailyRollupSideRows.WithLabelValues("live", "rows").Set(float64(rows))
	dailyRollupSideRows.WithLabelValues("live", "dup_keys").Set(float64(dupKeys))
	dailyRollupSideRows.WithLabelValues("live", "dup_rows").Set(float64(dupRows))
	evt := m.log.Info()
	if dupKeys > 0 {
		evt = m.log.Warn()
	}
	evt.Int64("keys", keys).Int64("rows", rows).
		Int64("dup_keys", dupKeys).Int64("dup_rows", dupRows).
		Msg("rollup cardinality (rows must equal keys; every excess row is a visible duplicate)")
	return nil
}
