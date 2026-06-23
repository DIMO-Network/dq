// Package segments holds backend-agnostic vehicle usage-segment detection.
// Detectors contain only algorithm logic; all data access goes through a
// SignalSource, implemented once per storage backend (DuckLake).
package segments

import (
	"context"
	"time"
)

// ActiveWindow is a fixed-width time window with its signal activity counts.
// Produced by SignalSource.WindowedSignalCounts; consumed by the frequency
// and change-point detectors.
type ActiveWindow struct {
	WindowStart         time.Time
	WindowEnd           time.Time
	SignalCount         uint64
	DistinctSignalCount uint64
}

// LevelSample is a timestamped numeric reading (RPM, fuel %, SoC %, odometer).
type LevelSample struct {
	TS    time.Time
	Value float64
}

// StateChange is a transition of a discrete signal (e.g. isIgnitionOn),
// carrying the new and previous state values.
type StateChange struct {
	TS        time.Time
	NewState  float64
	PrevState float64
}

// SignalSource is the data-access seam for segment detection. One
// implementation per backend; detectors are written against this interface.
type SignalSource interface {
	// WindowedSignalCounts returns per-window signal counts in [from, to),
	// bucketed to windowSizeSeconds, keeping only windows meeting the count
	// and distinct-count thresholds, ordered by window start.
	WindowedSignalCounts(ctx context.Context, subject string, from, to time.Time, windowSizeSeconds, signalThreshold, distinctSignalThreshold int) ([]ActiveWindow, error)

	// LevelSamples returns timestamped numeric samples for one signal name in
	// [from, to), ordered by timestamp ascending.
	LevelSamples(ctx context.Context, subject, name string, from, to time.Time) ([]LevelSample, error)

	// IgnitionStateChanges returns isIgnitionOn transitions in [from, to),
	// plus the last transition before from (seed for the open state), ordered
	// by timestamp ascending. Lookback for the seed is capped at 30 days.
	IgnitionStateChanges(ctx context.Context, subject string, from, to time.Time) ([]StateChange, error)
}

// TimeRange is a [Start, End] interval of raw signal readings (e.g. an idle run),
// before any [from, to] clipping or minDuration filtering — the detector applies
// those. Both bounds are inclusive sample timestamps.
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// IdleRunSource is an OPTIONAL capability: a backend that can compute idle-RPM runs
// in the store (window functions / gaps-and-islands) instead of streaming every RPM
// sample to Go. The lake backend implements it; a backend that does not triggers the
// idling detector's fallback to LevelSamples + findIdleRpmRanges. Runs are the raw
// contiguous idle intervals over [from, to) — a run breaks on a non-idle reading or
// a gap > maxGapSeconds between consecutive idle readings (matching the Go logic);
// the detector clips and minDuration-filters them.
type IdleRunSource interface {
	IdleRuns(ctx context.Context, subject, name string, from, to time.Time, maxIdleRpm, maxGapSeconds int) ([]TimeRange, error)
}
