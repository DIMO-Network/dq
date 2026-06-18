package ch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/dq/internal/segments"
)

var _ segments.SignalSource = chSignalSource{}

// chSignalSource implements segments.SignalSource over ClickHouse.
type chSignalSource struct {
	conn clickhouse.Conn
}

func (c chSignalSource) WindowedSignalCounts(ctx context.Context, subject string, from, to time.Time, win, sig, dist int) ([]segments.ActiveWindow, error) {
	ws, err := getWindowedSignalCounts(ctx, c.conn, subject, from, to, win, sig, dist)
	if err != nil {
		return nil, err
	}
	out := make([]segments.ActiveWindow, len(ws))
	for i, w := range ws {
		out[i] = segments.ActiveWindow(w) // activeWindow has identical field layout
	}
	return out, nil
}

func (c chSignalSource) LevelSamples(ctx context.Context, subject, name string, from, to time.Time) ([]segments.LevelSample, error) {
	ls, err := getLevelSamples(ctx, c.conn, subject, name, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]segments.LevelSample, len(ls))
	for i, s := range ls {
		out[i] = segments.LevelSample{TS: s.ts, Value: s.value}
	}
	return out, nil
}

func (c chSignalSource) IgnitionStateChanges(ctx context.Context, subject string, from, to time.Time) (_ []segments.StateChange, retErr error) {
	stmt, args := stateChangesQueryWithLookback(subject, from, to)
	rows, err := c.conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("querying ignition state changes: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	var out []segments.StateChange
	for rows.Next() {
		var sc segments.StateChange
		if err := rows.Scan(&sc.TS, &sc.NewState, &sc.PrevState); err != nil {
			return nil, fmt.Errorf("scanning state change: %w", err)
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

// maxLookbackDays limits how far back we search for prior state changes.
const maxLookbackDays = 30

// stateChangesQueryWithLookback builds a single query that fetches:
// - The last state change before 'from' (seed for open-state machine)
// - All state changes in [from, to)
// Results are ordered by timestamp.
//
// Performance notes:
// - PREWHERE filters on primary key columns before FINAL merge (much faster)
// - Lookback is bounded to maxLookbackDays to prevent unbounded scans
func stateChangesQueryWithLookback(subject string, from, to time.Time) (string, []any) {
	// Bound the lookback to prevent scanning unlimited history
	lookbackLimit := from.AddDate(0, 0, -maxLookbackDays)

	// Use UNION ALL to combine:
	// - Last state change before 'from' (LIMIT 1, ordered DESC then re-ordered)
	// - All state changes in range [from, to)
	//
	// PREWHERE on subject filters before FINAL merge, significantly reducing work
	query := fmt.Sprintf(`
SELECT timestamp, new_state, prev_state FROM (
  SELECT timestamp, new_state, prev_state
  FROM signal_state_changes FINAL
  PREWHERE subject = ?
  WHERE signal_name = 'isIgnitionOn'
    AND timestamp >= %s
    AND timestamp < %s
    AND prev_state != new_state
  ORDER BY timestamp DESC
  LIMIT 1

  UNION ALL

  -- All state changes within the query range
  SELECT timestamp, new_state, prev_state
  FROM signal_state_changes FINAL
  PREWHERE subject = ?
  WHERE signal_name = 'isIgnitionOn'
    AND timestamp >= %s
    AND timestamp < %s
    AND prev_state != new_state
)
ORDER BY timestamp`, dateTime64Micro(lookbackLimit), dateTime64Micro(from), dateTime64Micro(from), dateTime64Micro(to))

	args := []any{subject, subject}

	return query, args
}
