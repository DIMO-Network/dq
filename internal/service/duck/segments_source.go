package duck

import (
	"context"
	"database/sql"
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
// Buckets are epoch-aligned (origin = Unix epoch 1970-01-01T00:00:00Z): the
// window start is floor(timestamp / N seconds) * N. Both
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
// inflated counts by collapsing duplicate rows.
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

	// Build transitions over the lookback+range window, split into the CH UNION ALL
	// structure: (1) seed = last transition strictly before from; (2) range = all
	// transitions in [from, to). value_number is 0.0/1.0; a transition is new!=prev.
	// `value_number IS NOT NULL` drops missing readings (a NULL never compares true
	// and would poison the next row's LAG).
	//
	// prev_state for the window's FIRST row seeds from the TRUE last isIgnitionOn
	// value strictly before the window — reconstructing CH's unbounded
	// signal_last_state — rather than a hardcoded 0. Without it, a vehicle that was
	// already ON entering the window has its first ON reading fabricated into a
	// transition, inventing a phantom trip (the parity divergence the ongoing-trip
	// work surfaced). No prior reading at all (brand-new vehicle) falls back to 0
	// (off), so a first-ever ON is a genuine trip start. The seed lookup is a single
	// subject_bucket-pruned LIMIT-1 row, evaluated once.
	bucket := subjectBucketPredicate("", subject)
	q := fmt.Sprintf(`
WITH deduped AS (
  SELECT timestamp, value_number
  FROM lake.signals
  WHERE subject = ? AND `+bucket+` AND name = 'isIgnitionOn'
    AND value_number IS NOT NULL
    AND timestamp >= make_timestamp(%[1]d) AND timestamp < make_timestamp(%[2]d)
  `+signalDedupQualify+`
),
transitions AS (
  SELECT
    timestamp,
    value_number AS new_state,
    coalesce(
      lag(value_number) OVER (ORDER BY timestamp),
      (SELECT value_number FROM lake.signals
       WHERE subject = ? AND `+bucket+` AND name = 'isIgnitionOn' AND value_number IS NOT NULL
         AND timestamp < make_timestamp(%[1]d)
       -- timestamp DESC for the latest pre-window row; cloud_event_id ASC matches
       -- signalDedupQualify (smallest id wins at a tied timestamp) so the seed picks
       -- the same canonical value the deduped window would — deterministic.
       ORDER BY timestamp DESC, cloud_event_id ASC LIMIT 1),
      0
    ) AS prev_state
  FROM deduped
)
SELECT timestamp, new_state, prev_state FROM transitions
WHERE new_state != prev_state AND timestamp >= make_timestamp(%[3]d) AND timestamp < make_timestamp(%[2]d)
UNION ALL
SELECT timestamp, new_state, prev_state FROM (
  SELECT timestamp, new_state, prev_state
  FROM transitions
  WHERE new_state != prev_state AND timestamp < make_timestamp(%[3]d)
  ORDER BY timestamp DESC
  LIMIT 1
)
ORDER BY timestamp`,
		lookbackUS, toUS, fromUS)

	rows, err := s.svc.db.QueryContext(ctx, q, subject, subject)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Continuously-ON trip: no transition fired (the value never changed from the
	// true prior state), but a trip ongoing since before the lookback would
	// otherwise be invisible. If the latest reading is ON, surface the in-progress
	// trip as a synthetic ON at the earliest known reading so the detector reports
	// it (the product choice: an ongoing trip must show as ongoing).
	if len(out) == 0 {
		var firstTS sql.NullTime
		var latest sql.NullFloat64
		err := s.svc.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT min(timestamp), arg_max(value_number, timestamp)
FROM lake.signals
WHERE subject = ? AND `+bucket+` AND name = 'isIgnitionOn' AND value_number IS NOT NULL
  AND timestamp >= make_timestamp(%[1]d) AND timestamp < make_timestamp(%[2]d)`, lookbackUS, toUS),
			subject).Scan(&firstTS, &latest)
		if err != nil {
			return nil, fmt.Errorf("lake ignition current state: %w", err)
		}
		if latest.Valid && latest.Float64 == 1 && firstTS.Valid {
			out = append(out, segments.StateChange{TS: firstTS.Time.UTC(), NewState: 1, PrevState: 0})
		}
	}
	return out, nil
}

// IdleRuns computes contiguous idle-RPM runs in SQL via gaps-and-islands, instead of
// streaming every RPM sample to Go (segments.IdleRunSource). A run is a maximal
// sequence of idle readings (0 < value <= maxIdleRpm) with no non-idle reading
// between them (a non-idle reading breaks the run) and no gap > maxGapSeconds between
// consecutive idle readings — matching findIdleRpmRanges exactly. Returns the raw
// runs; the detector clips them to [from, to] and applies minDuration. The CH backend
// has no equivalent and falls back to LevelSamples + the Go scan.
func (s *LakeSignalSource) IdleRuns(ctx context.Context, subject, name string, from, to time.Time, maxIdleRpm, maxGapSeconds int) ([]segments.TimeRange, error) {
	fromUS := from.UTC().UnixMicro()
	toUS := to.UTC().UnixMicro()
	maxGapUS := int64(maxGapSeconds) * 1_000_000
	bucket := subjectBucketPredicate("", subject)
	// A new island opens at an idle reading whose immediately-prior reading is not an
	// idle reading within maxGap: the first row (prev NULL), a non-idle predecessor
	// (NOT prev_idle), or a too-large gap. Non-idle rows carry an island id but are
	// dropped by WHERE idle, so a non-idle reading between two idle ones still splits
	// the run (the next idle row sees a non-idle predecessor).
	q := fmt.Sprintf(`
WITH s AS (
  SELECT timestamp, (value_number > 0 AND value_number <= %[4]d) AS idle
  FROM lake.signals
  WHERE subject = ? AND `+bucket+` AND name = ? AND value_number IS NOT NULL
    AND timestamp >= make_timestamp(%[1]d) AND timestamp < make_timestamp(%[2]d)
  `+signalDedupQualify+`
),
m AS (
  SELECT timestamp, idle,
    lag(idle) OVER w AS prev_idle, lag(timestamp) OVER w AS prev_ts
  FROM s WINDOW w AS (ORDER BY timestamp)
),
isl AS (
  SELECT timestamp, idle,
    sum(CASE WHEN idle AND (prev_idle IS NULL OR NOT prev_idle
              OR epoch_us(timestamp) - epoch_us(prev_ts) > %[3]d) THEN 1 ELSE 0 END)
      OVER (ORDER BY timestamp) AS island
  FROM m
)
SELECT min(timestamp), max(timestamp) FROM isl WHERE idle GROUP BY island ORDER BY 1`,
		fromUS, toUS, maxGapUS, maxIdleRpm)

	rows, err := s.svc.db.QueryContext(ctx, q, subject, name)
	if err != nil {
		return nil, fmt.Errorf("lake idle runs: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []segments.TimeRange
	for rows.Next() {
		var r segments.TimeRange
		if err := rows.Scan(&r.Start, &r.End); err != nil {
			return nil, fmt.Errorf("scanning idle run: %w", err)
		}
		r.Start, r.End = r.Start.UTC(), r.End.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}
