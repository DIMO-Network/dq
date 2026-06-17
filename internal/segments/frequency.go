package segments

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
)

const (
	defaultWindowSizeSeconds            = 60 // 1 minute windows
	defaultSignalCountThreshold         = 10 // Minimum signals per window for activity
	defaultDistinctSignalCountThreshold = 2  // Minimum distinct signal types per window
)

// FrequencyDetector detects segments using frequency analysis of signal updates.
// Analyzes signal update patterns to identify vehicle activity periods.
type FrequencyDetector struct {
	src SignalSource
}

// NewFrequencyDetector creates a new FrequencyDetector with the given source.
func NewFrequencyDetector(src SignalSource) *FrequencyDetector {
	return &FrequencyDetector{src: src}
}

// DetectSegments implements frequency-based segment detection
func (d *FrequencyDetector) DetectSegments(
	ctx context.Context,
	subject string,
	from, to time.Time,
	config *model.SegmentConfig,
) ([]*model.Segment, error) {
	rc := resolveBaseConfig(config)
	maxGap := rc.maxGapSeconds
	minDuration := rc.minDuration
	signalThreshold := defaultSignalCountThreshold
	if config != nil && config.SignalCountThreshold != nil {
		signalThreshold = *config.SignalCountThreshold
	}

	// Look back maxGap seconds before 'from' to detect segments that started before the query range.
	lookbackFrom := from.Add(-time.Duration(maxGap) * time.Second)
	windows, err := d.src.WindowedSignalCounts(ctx, subject, lookbackFrom, to, defaultWindowSizeSeconds, signalThreshold, defaultDistinctSignalCountThreshold)
	if err != nil {
		return nil, fmt.Errorf("failed to query active windows: %w", err)
	}

	if len(windows) == 0 {
		return []*model.Segment{}, nil
	}

	// Merge consecutive active windows into segments (in Go for flexibility)
	segments := mergeWindowsIntoSegments(windows, from, to, maxGap, minDuration)

	return segments, nil
}

// GetMechanismName returns the name of this detection mechanism.
func (d *FrequencyDetector) GetMechanismName() string {
	return "FREQUENCY_ANALYSIS"
}
