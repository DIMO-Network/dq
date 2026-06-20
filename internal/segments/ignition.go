package segments

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
)

// IgnitionDetector detects segments using ignition state transitions
type IgnitionDetector struct {
	src SignalSource
}

// NewIgnitionDetector creates a new IgnitionDetector with the given source
func NewIgnitionDetector(src SignalSource) *IgnitionDetector {
	return &IgnitionDetector{src: src}
}

// DetectSegments implements ignition-based segment detection
func (d *IgnitionDetector) DetectSegments(
	ctx context.Context,
	subject string,
	from, to time.Time,
	config *model.SegmentConfig,
) ([]*model.Segment, error) {
	rc := resolveBaseConfig(config)
	minIdle := rc.maxGapSeconds
	minDuration := rc.minDuration

	// Fetch all state changes via the SignalSource interface
	stateChanges, err := d.src.IgnitionStateChanges(ctx, subject, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query state changes: %w", err)
	}

	// Process state changes in Go to build segments with debouncing
	segments := d.buildSegmentsWithDebouncing(stateChanges, from, to, minIdle, minDuration)

	return segments, nil
}

// buildSegmentsWithDebouncing processes state changes and applies debouncing logic
// to merge consecutive short segments separated by less than minIdle seconds
func (d *IgnitionDetector) buildSegmentsWithDebouncing(stateChanges []StateChange, from, to time.Time, minIdle, minDuration int) []*model.Segment {
	if len(stateChanges) == 0 {
		return []*model.Segment{}
	}

	// First pass: filter out noise (OFF signals followed by ON within minIdle seconds)
	filtered := d.filterNoise(stateChanges, minIdle)

	// Second pass: build segments from cleaned state changes
	var segments []*model.Segment
	var currentSegmentStart *time.Time
	var startedWithPrevMinus1 bool

	for _, sc := range filtered {
		if sc.NewState == 1 {
			// ON signal - start a new segment if we don't have one
			if currentSegmentStart == nil && sc.PrevState != 1 {
				currentSegmentStart = &sc.TS
				startedWithPrevMinus1 = (sc.PrevState == -1)
			}
		} else if sc.NewState == 0 && currentSegmentStart != nil {
			// OFF signal - end the segment
			segmentEnd := sc.TS
			duration := int32(segmentEnd.Sub(*currentSegmentStart).Seconds())

			if int(duration) >= minDuration {
				segments = append(segments, newSegment(*currentSegmentStart, &segmentEnd, duration, false, currentSegmentStart.Before(from)))
			}

			currentSegmentStart = nil
			startedWithPrevMinus1 = false
		}
	}

	// Handle ongoing segment (started but no end signal)
	if currentSegmentStart != nil && !startedWithPrevMinus1 {
		duration := int32(to.Sub(*currentSegmentStart).Seconds())
		if int(duration) >= minDuration {
			segments = append(segments, newSegment(*currentSegmentStart, nil, duration, true, currentSegmentStart.Before(from)))
		}
	}

	if segments == nil {
		return []*model.Segment{}
	}
	return segments
}

// filterNoise removes OFF signals that are followed by ON within minIdle seconds
// O(n) algorithm: pre-compute next ON index for each position, then single pass
func (d *IgnitionDetector) filterNoise(stateChanges []StateChange, minIdle int) []StateChange {
	n := len(stateChanges)
	if n == 0 {
		return nil
	}

	// Pre-compute next ON signal index for each position (O(n) reverse scan)
	nextON := make([]int, n)
	lastON := -1
	for i := n - 1; i >= 0; i-- {
		if stateChanges[i].NewState == 1 {
			lastON = i
		}
		nextON[i] = lastON
	}

	filtered := make([]StateChange, 0, n)
	minIdleDuration := time.Duration(minIdle) * time.Second

	for i := 0; i < n; i++ {
		sc := stateChanges[i]

		// Keep all ON signals
		if sc.NewState == 1 {
			filtered = append(filtered, sc)
			continue
		}

		// For OFF signals, check if next ON is within minIdle (O(1) lookup)
		if sc.NewState == 0 {
			keep := true
			if j := nextON[i]; j > i {
				gap := stateChanges[j].TS.Sub(sc.TS)
				if gap < minIdleDuration {
					keep = false
				}
			}
			if keep {
				filtered = append(filtered, sc)
			}
		}
	}

	return filtered
}
