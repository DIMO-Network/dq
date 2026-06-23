package duck

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/segments"
)

// LakeSignalSource implements segments.SignalSource over the DuckLake
// lake.signals table. Detection logic lives in internal/segments; this only
// fetches data, mirroring ch.chSignalSource one-for-one.
type LakeSignalSource struct {
	svc *Service
}

// Compile-time assertion that LakeSignalSource satisfies the interface.
var _ segments.SignalSource = (*LakeSignalSource)(nil)

// NewLakeSignalSource builds a SignalSource bound to the catalog-attached svc.
func NewLakeSignalSource(svc *Service) *LakeSignalSource {
	return &LakeSignalSource{svc: svc}
}

// WindowedSignalCounts returns per-window signal counts in [from, to),
// bucketed to windowSizeSeconds, keeping only windows meeting the count and
// distinct-count thresholds, ordered by window start.
//
// Buckets are epoch-aligned (origin = Unix epoch 1970-01-01T00:00:00Z), matching
// ClickHouse's toStartOfInterval(timestamp, INTERVAL N second) exactly. Both
// functions floor to the nearest multiple of the window size measured from the
// epoch, so [00:00:00,00:01:00), [00:01:00,00:02:00), ... regardless of `from`.
// A `from` that is not a multiple of the window size does NOT shift bucket
// boundaries — only the filter range changes.
//
// Use // (integer division, not /) to avoid DOUBLE promotion in DuckDB.
//
// lake.signals has no unique constraint per (subject,name,timestamp): duplicate
// rows can be present across materializer batches. The inner QUALIFY dedup
// keeps one row per (subject,name,timestamp,cloud_event_id) to prevent
// inflated counts, mirroring ClickHouse's FINAL merge semantics.
func (s *LakeSignalSource) WindowedSignalCounts(ctx context.Context, subject string, from, to time.Time, win, sig, dist int) ([]segments.ActiveWindow, error) {
	winUS := int64(win) * 1_000_000
	fromUS := from.UTC().UnixMicro()
	toUS := to.UTC().UnixMicro()
	// Epoch-aligned bucket: floor(epoch_us(ts), winUS).
	// This matches CH toStartOfInterval which also uses epoch as origin.
	// Do NOT subtract fromUS here — that was the parity bug (from-aligned != epoch-aligned).
	bucketExpr := fmt.Sprintf("make_timestamp((epoch_us(timestamp) // CAST(%d AS BIGINT)) * CAST(%d AS BIGINT))",
		winUS, winUS)
	// CAST(%d AS BIGINT): winUS is the window size in microseconds (int64); the
	// CAST ties directly to the winUS format arg, preventing arg-count mismatches
	// if the query is refactored.
	q := fmt.Sprintf(`
SELECT window_start,
       make_timestamp(epoch_us(window_start) + CAST(%d AS BIGINT)) AS window_end,
       count(*) AS signal_count,
       count(DISTINCT name) AS distinct_signal_count
FROM (
  SELECT %s AS window_start, name
  FROM lake.signals
  WHERE subject = ? AND `+subjectBucketPredicate("", subject)+` AND timestamp >= make_timestamp(%d) AND timestamp < make_timestamp(%d)
  `+signalDedupQualify+`
)
GROUP BY window_start
HAVING count(*) >= ? AND count(DISTINCT name) >= ?
ORDER BY window_start`,
		winUS, bucketExpr, fromUS, toUS)
	rows, err := s.svc.db.QueryContext(ctx, q, subject, sig, dist)
	if err != nil {
		return nil, fmt.Errorf("lake windowed signal counts: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []segments.ActiveWindow
	for rows.Next() {
		var w segments.ActiveWindow
		if err := rows.Scan(&w.WindowStart, &w.WindowEnd, &w.SignalCount, &w.DistinctSignalCount); err != nil {
			return nil, fmt.Errorf("scanning windowed signal count row: %w", err)
		}
		w.WindowStart = w.WindowStart.UTC()
		w.WindowEnd = w.WindowEnd.UTC()
		out = append(out, w)
	}
	return out, rows.Err()
}

// LevelSamples returns timestamped numeric samples for one signal in [from, to),
// ordered by timestamp ascending. Dedup keeps one row per (subject,name,timestamp).
func (s *LakeSignalSource) LevelSamples(ctx context.Context, subject, name string, from, to time.Time) ([]segments.LevelSample, error) {
	fromUS := from.UTC().UnixMicro()
	toUS := to.UTC().UnixMicro()
	q := fmt.Sprintf(`
SELECT timestamp, value_number
FROM lake.signals
WHERE subject = ? AND `+subjectBucketPredicate("", subject)+` AND name = ? AND timestamp >= make_timestamp(%d) AND timestamp < make_timestamp(%d)
`+signalDedupQualify+`
ORDER BY timestamp`,
		fromUS, toUS)
	rows, err := s.svc.db.QueryContext(ctx, q, subject, name)
	if err != nil {
		return nil, fmt.Errorf("lake level samples: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []segments.LevelSample
	for rows.Next() {
		var ls segments.LevelSample
		if err := rows.Scan(&ls.TS, &ls.Value); err != nil {
			return nil, fmt.Errorf("scanning level sample row: %w", err)
		}
		ls.TS = ls.TS.UTC()
		out = append(out, ls)
	}
	return out, rows.Err()
}

// ignitionLookbackDays limits how far back we search for a prior ignition
// state change, matching ch.chSignalSource (maxLookbackDays = 30).
const ignitionLookbackDays = 30

// IgnitionStateChanges returns isIgnitionOn transitions in [from, to) plus
// exactly one pre-from seed row (the last transition before from, within a
// 30-day lookback window), ordered by timestamp ascending.
//
// This matches ch.stateChangesQueryWithLookback exactly: one seed row from a
// DESC LIMIT 1 arm, plus all transitions in [from, to).
//
// Transition detection uses LAG() over value_number within the lookback window,
// filtering to rows where the value differs from its predecessor. The seed arm
// independently picks the latest such transition before from.
func (s *LakeSignalSource) IgnitionStateChanges(ctx context.Context, subject string, from, to time.Time) ([]segments.StateChange, error) {
	lookback := from.AddDate(0, 0, -ignitionLookbackDays)
	fromUS := from.UTC().UnixMicro()
	toUS := to.UTC().UnixMicro()
	lookbackUS := lookback.UTC().UnixMicro()

	// Build transitions over the full lookback+range window, then split into
	// two arms identical to the CH UNION ALL structure:
	//   1. seed: last transition strictly before from (ORDER BY ts DESC LIMIT 1)
	//   2. range: all transitions in [from, to)
	// Both arms draw from the same CTE to avoid duplicating the LAG computation.
	//
	// Note: value_number for isIgnitionOn is 0.0 or 1.0 (numeric bool).
	// A transition is a row where new_state != prev_state.
	// `value_number IS NOT NULL` in the deduped CTE drops missing readings: a NULL
	// would be silently excluded from transitions (NULL != x is NULL, not true)
	// AND would poison the next row's LAG (prev_state coalesces to -1), wrongly
	// emitting an unchanged reading as a spurious transition.
	// coalesce(lag(...), 0): if a vehicle has been ON continuously for MORE than
	// the 30-day lookback (no OFF event in the window), this LAG yields NULL for
	// the very first row. Coalesce maps it to 0 — matching CH's pre-materialized
	// signal_state_changes, which seeds prev_state=0 for an ongoing segment — so
	// the first ON row IS emitted as a transition and the in-progress segment is
	// reported, exactly as CH does (segments parity at cutover; the previous -1
	// seed suppressed that segment for duck only).
	q := fmt.Sprintf(`
WITH deduped AS (
  SELECT timestamp, value_number
  FROM lake.signals
  WHERE subject = ? AND `+subjectBucketPredicate("", subject)+` AND name = 'isIgnitionOn'
    AND value_number IS NOT NULL
    AND timestamp >= make_timestamp(%d) AND timestamp < make_timestamp(%d)
  `+signalDedupQualify+`
),
transitions AS (
  SELECT
    timestamp,
    value_number AS new_state,
    coalesce(
      lag(value_number) OVER (ORDER BY timestamp),
      0
    ) AS prev_state
  FROM deduped
)
SELECT timestamp, new_state, prev_state FROM transitions
WHERE new_state != prev_state AND timestamp >= make_timestamp(%d) AND timestamp < make_timestamp(%d)
UNION ALL
SELECT timestamp, new_state, prev_state FROM (
  SELECT timestamp, new_state, prev_state
  FROM transitions
  WHERE new_state != prev_state AND timestamp < make_timestamp(%d)
  ORDER BY timestamp DESC
  LIMIT 1
)
ORDER BY timestamp`,
		lookbackUS, toUS, fromUS, toUS, fromUS)

	rows, err := s.svc.db.QueryContext(ctx, q, subject)
	if err != nil {
		return nil, fmt.Errorf("lake ignition state changes: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []segments.StateChange
	for rows.Next() {
		var sc segments.StateChange
		if err := rows.Scan(&sc.TS, &sc.NewState, &sc.PrevState); err != nil {
			return nil, fmt.Errorf("scanning ignition state change row: %w", err)
		}
		sc.TS = sc.TS.UTC()
		out = append(out, sc)
	}
	return out, rows.Err()
}
