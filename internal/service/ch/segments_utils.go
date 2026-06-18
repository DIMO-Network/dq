package ch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// activeWindow represents a time window with sufficient signal activity.
// Used by frequency and changepoint detectors (ch-internal only; converted to
// segments.ActiveWindow before being returned to callers outside this package).
type activeWindow struct {
	WindowStart         time.Time
	WindowEnd           time.Time
	SignalCount         uint64
	DistinctSignalCount uint64
}

// getWindowedSignalCounts queries per-window signal counts from ClickHouse.
// Shared by FrequencyDetector and ChangePointDetector.
//
// Performance notes:
//   - PREWHERE filters on primary key (token_id) before FINAL merge
//   - Pre-allocates result slice based on expected window count
func getWindowedSignalCounts(
	ctx context.Context,
	conn clickhouse.Conn,
	subject string,
	from, to time.Time,
	windowSizeSeconds int,
	signalThreshold int,
	distinctSignalThreshold int,
) (_ []activeWindow, retErr error) {
	query := fmt.Sprintf(`
SELECT
    toStartOfInterval(timestamp, INTERVAL ? second) AS window_start,
    toStartOfInterval(timestamp, INTERVAL ? second) + INTERVAL ? second AS window_end,
    count() AS signal_count,
    uniq(name) AS distinct_signal_count
FROM signal FINAL
PREWHERE subject = ?
WHERE timestamp >= %s
  AND timestamp < %s
GROUP BY window_start
HAVING signal_count >= ? AND distinct_signal_count >= ?
ORDER BY window_start`, dateTime64Micro(from), dateTime64Micro(to))

	rows, err := conn.Query(ctx, query, windowSizeSeconds, windowSizeSeconds, windowSizeSeconds, subject, signalThreshold, distinctSignalThreshold)
	if err != nil {
		return nil, fmt.Errorf("failed to query windowed signal counts: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()

	expectedWindows := int(to.Sub(from).Seconds()) / windowSizeSeconds
	windows := make([]activeWindow, 0, expectedWindows)
	for rows.Next() {
		var w activeWindow
		if err := rows.Scan(&w.WindowStart, &w.WindowEnd, &w.SignalCount, &w.DistinctSignalCount); err != nil {
			return nil, fmt.Errorf("failed to scan windowed signal count row: %w", err)
		}
		windows = append(windows, w)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("failed to iterate windowed signal count rows: %w", rows.Err())
	}
	return windows, nil
}

// levelSample is a timestamped numeric sample (fuel, SoC, RPM, etc.).
type levelSample struct {
	ts    time.Time
	value float64
}

// getLevelSamples fetches timestamped level samples for a signal.
// Results are returned in timestamp order (ORDER BY in the query).
// Uses PREWHERE on subject for efficient primary-key filtering before FINAL merge.
func getLevelSamples(ctx context.Context, conn clickhouse.Conn, subject string, name string, from, to time.Time) (_ []levelSample, retErr error) {
	query := "SELECT " + vss.TimestampCol + ", " + vss.ValueNumberCol +
		" FROM " + vss.TableName + " FINAL" +
		" PREWHERE " + vss.SubjectCol + " = ?" +
		" WHERE " + vss.NameCol + " = ? AND " + vss.TimestampCol + " >= " + dateTime64Micro(from) + " AND " + vss.TimestampCol + " < " + dateTime64Micro(to) +
		" ORDER BY " + vss.TimestampCol
	rows, err := conn.Query(ctx, query, subject, name)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	out := make([]levelSample, 0, 1024)
	for rows.Next() {
		var s levelSample
		if err := rows.Scan(&s.ts, &s.value); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
