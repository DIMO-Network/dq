package duck

// Summaries under the daily rollup (dq#55 step 3). Once lake.signals_latest
// refreshes from a daily watermark (step 4's flip), its count/first_seen/
// last_seen cover exactly the rows with timestamp < W — so summaries serve
// the exact answer as (rollup ∪ signals-since-watermark), full-outer-joined
// by name. The watermark splits the base rows disjointly (rollup counts < W,
// the tail counts >= W), so counts SUM exactly — including names first seen
// after the watermark, which exist only in the tail. count(DISTINCT
// timestamp) self-dedups physical duplicates (anti-join re-inserts) with no
// NOT-EXISTS probe. This was chosen over keeping counts in the KV: a counter
// cannot be folded idempotently under window replay / CAS retry / redelivery
// without exactly the dedup machinery dq#55 removes (see the issue thread).
//
// The tail leg carries a CONSTANT watermark literal next to the subject_bucket
// predicate, so it prunes to one bucket × 1–2 day partitions, riding on a
// query that already pays the lake floor. Gated by dailyServing
// (LAKE_ROLLUP_DAILY_SERVING, set at the step-4 flip): while the per-pass fold
// still maintains the rollup, its counts already include the tail rows and
// the union would double-count.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
)

// RollupDailyWatermarkPartition is the lake.ingest_progress key holding the
// daily rollup watermark (dq#55) — the UTC-midnight boundary the rollup is
// exact through, written by the materializer's daily refresh in the same
// transaction as the fold. The materializer references this constant;
// internal/latestkv duplicates the string (importing this package there would
// cycle through Queries' kvStore field).
const RollupDailyWatermarkPartition = "lake.signals_latest#daily_watermark"

// rollupWatermarkTTL bounds how stale the query layer's cached watermark can
// be. The value changes once per UTC day; the TTL only bounds how long after
// the daily refresh this pod keeps using the previous boundary — during which
// the union is still EXACT (the rollup is exact through the old W, and the
// tail from the old W covers the freshly-folded day redundantly; the split
// stays disjoint from the rollup's perspective only if the rollup still
// matches the cached W — see cachedRollupWatermark for the guard).
const rollupWatermarkTTL = time.Minute

// WithDailyServingRollup marks lake.signals_latest as maintained by the daily
// watermarked refresh (the step-4 flip), switching summaries to the exact
// (rollup ∪ tail) union. Returns q for chaining.
func (q *Queries) WithDailyServingRollup(on bool) *Queries {
	q.dailyServing = on
	return q
}

// cachedRollupWatermark returns the daily watermark, refreshed at most every
// rollupWatermarkTTL. Zero when no watermark row exists (no daily refresh has
// ever run — the rollup is then per-pass fresh and the plain rollup read is
// exact).
//
// The TTL creates one subtle window: right after the daily refresh advances W,
// a pod still holding the old W folds the just-covered day into the tail
// TWICE — once from the (freshly refreshed) rollup's counts, once from the
// tail leg. The short TTL bounds the window to a minute a day at ~03:30 UTC;
// exactness the rest of the day. (Reading the watermark inside the union's
// transaction would close it entirely at the cost of defeating the constant
// literal — deliberately not done; see the pruning rationale above.)
func (q *Queries) cachedRollupWatermark(ctx context.Context) (time.Time, error) {
	q.wmMu.Lock()
	defer q.wmMu.Unlock()
	if !q.wmAt.IsZero() && time.Since(q.wmAt) < rollupWatermarkTTL {
		return q.wmVal, nil
	}
	var raw string
	err := q.svc.db.QueryRowContext(ctx,
		"SELECT cursor FROM lake.ingest_progress WHERE partition = ?", RollupDailyWatermarkPartition).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		q.wmVal, q.wmAt = time.Time{}, time.Now()
		return q.wmVal, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("reading daily rollup watermark: %w", err)
	}
	w, perr := time.Parse(time.RFC3339, raw)
	if perr != nil {
		return time.Time{}, fmt.Errorf("unparseable daily rollup watermark %q: %w", raw, perr)
	}
	q.wmVal, q.wmAt = w.UTC(), time.Now()
	return q.wmVal, nil
}

// getSignalSummariesUnion serves GetSignalSummaries as the exact
// (rollup ∪ tail-since-watermark) union (see the file comment). Each leg is
// subject- and bucket-pruned; the tail's watermark bound is a constant
// literal.
func (q *Queries) getSignalSummariesUnion(ctx context.Context, subject string, watermark time.Time) ([]*model.SignalDataSummary, error) {
	bucket := subjectBucketPredicate("", subject)
	stmt := fmt.Sprintf(`
		WITH r AS (
			SELECT name, count, first_seen, last_seen
			FROM lake.signals_latest WHERE subject = ? AND %[1]s
		),
		t AS (
			SELECT name, CAST(count(DISTINCT "timestamp") AS BIGINT) AS count,
			       min("timestamp") AS first_seen, max("timestamp") AS last_seen
			FROM lake.signals
			WHERE subject = ? AND %[1]s AND "timestamp" >= make_timestamp(%[2]d)
			GROUP BY name
		)
		SELECT coalesce(r.name, t.name) AS name,
		       CAST(coalesce(r.count, 0) + coalesce(t.count, 0) AS UBIGINT) AS count,
		       least(coalesce(r.first_seen, t.first_seen), coalesce(t.first_seen, r.first_seen)) AS first_seen,
		       greatest(coalesce(r.last_seen, t.last_seen), coalesce(t.last_seen, r.last_seen)) AS last_seen
		FROM r FULL OUTER JOIN t ON t.name = r.name
		ORDER BY name`, bucket, watermark.UnixMicro())
	rows, err := q.svc.db.QueryContext(ctx, stmt, subject, subject)
	if err != nil {
		return nil, fmt.Errorf("querying union signal summaries: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	summaries := []*model.SignalDataSummary{}
	for rows.Next() {
		s, err := scanSignalSummary(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning union summary: %w", err)
		}
		summaries = append(summaries, s)
	}
	return summaries, rows.Err()
}
