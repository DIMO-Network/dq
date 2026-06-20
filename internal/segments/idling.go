package segments

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

const (
	defaultMaxIdleRpm = 1000 // max RPM to count as idle
	minRunningRpm     = 0    // RPM must be strictly above this to count as engine running (excludes key-on-engine-off)
)

// IdlingDetector detects segments where engine RPM remains in idle range.
// Processes RPM samples in-memory for exact segment boundaries (no window discretization).
// Note: Detection is RPM-only. Callers (e.g. repository) filter out segments with speed > 0.
type IdlingDetector struct {
	src SignalSource
}

// NewIdlingDetector creates a new IdlingDetector with the given source.
func NewIdlingDetector(src SignalSource) *IdlingDetector {
	return &IdlingDetector{src: src}
}

// DetectSegments fetches RPM samples (1 query) and finds contiguous runs of idle RPM in-memory.
func (d *IdlingDetector) DetectSegments(
	ctx context.Context,
	subject string,
	from, to time.Time,
	config *model.SegmentConfig,
) ([]*model.Segment, error) {
	rc := resolveBaseConfig(config)
	maxGap := rc.maxGapSeconds
	minDuration := rc.minDuration
	maxIdleRpm := defaultMaxIdleRpm
	if config != nil && config.MaxIdleRpm != nil {
		maxIdleRpm = *config.MaxIdleRpm
	}

	lookbackFrom := from.Add(-time.Duration(maxGap) * time.Second)
	// Single query: RPM samples (returned sorted)
	samples, err := d.src.LevelSamples(ctx, subject, vss.FieldPowertrainCombustionEngineSpeed, lookbackFrom, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query RPM samples: %w", err)
	}
	if len(samples) == 0 {
		return []*model.Segment{}, nil
	}

	ranges := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, from, to)
	return timeRangesToSegments(ranges, from), nil
}

// findIdleRpmRanges walks sorted RPM samples and finds contiguous runs where 0 < RPM <= maxIdleRpm.
// A gap between consecutive idle samples larger than maxGap seconds ends the current run.
// Only runs with duration >= minDuration are emitted. Ranges are clipped to [from, to].
func findIdleRpmRanges(samples []LevelSample, maxIdleRpm, maxGap, minDuration int, from, to time.Time) []timeRange {
	maxGapDur := time.Duration(maxGap) * time.Second
	var ranges []timeRange
	var runStart, runEnd time.Time
	inRun := false

	for _, s := range samples {
		isIdle := s.Value > minRunningRpm && s.Value <= float64(maxIdleRpm)
		if isIdle {
			if !inRun {
				runStart = s.TS
				runEnd = s.TS
				inRun = true
			} else if s.TS.Sub(runEnd) > maxGapDur {
				appendIdleRange(runStart, runEnd, minDuration, from, to, &ranges)
				runStart = s.TS
				runEnd = s.TS
			} else {
				runEnd = s.TS
			}
		} else {
			if inRun {
				appendIdleRange(runStart, runEnd, minDuration, from, to, &ranges)
				inRun = false
			}
		}
	}
	if inRun {
		appendIdleRange(runStart, runEnd, minDuration, from, to, &ranges)
	}
	return ranges
}

// appendIdleRange appends a timeRange if the run meets duration and time-range criteria.
func appendIdleRange(runStart, runEnd time.Time, minDuration int, from, to time.Time, out *[]timeRange) {
	if tr, ok := clipTimeRange(timeRange{start: runStart, end: runEnd}, from, to, minDuration); ok {
		*out = append(*out, tr)
	}
}
