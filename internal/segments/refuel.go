package segments

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

const (
	refuelWindowMinutes         = 5  // window length for fuel rise detection
	refuelMinRisePercent        = 30 // fuel must rise more than this % (relative) in the window
	refuelMinFuelEpsilon        = 1e-6
	refuelMinAbsoluteRisePct    = 20.0 // trough-to-peak must rise at least this much in absolute % to be a real refuel
	refuelPeakSearchMaxMin      = 30   // max minutes to search forward from rise window for the peak
	refuelPeakStabilizationDrop = 1.0  // if fuel drops more than this from current sample to next, consider current sample the peak
)

// RefuelDetector detects refuel segments by finding large fuel rises and emitting segments from
// the last low reading (trough) to the first stable high reading (peak).
type RefuelDetector struct {
	src SignalSource
}

// NewRefuelDetector creates a new RefuelDetector with the given source.
func NewRefuelDetector(src SignalSource) *RefuelDetector {
	return &RefuelDetector{src: src}
}

// DetectSegments finds 5-min windows with >30% fuel rise, then for each rise emits a segment
// from the trough (last low sample before the jump) to the peak (first stable high after).
// 1 query (fuel only).
func (d *RefuelDetector) DetectSegments(
	ctx context.Context,
	subject string,
	from, to time.Time,
	config *model.SegmentConfig,
) ([]*model.Segment, error) {
	rc := resolveBaseConfig(config)
	minDuration := rc.minDuration
	minRise := refuelMinRisePercent
	if config != nil && config.MinIncreasePercent != nil && *config.MinIncreasePercent > 0 {
		minRise = *config.MinIncreasePercent
	}
	windowDur := time.Duration(refuelWindowMinutes) * time.Minute
	fuelFrom := from.Add(-windowDur)
	// Fetch far enough past `to` that findRefuelTroughAndPeak can still find a
	// peak that stabilizes up to refuelPeakSearchMaxMin after a rise near the end
	// of the range; fetching only to+windowDur truncated the search and dropped
	// real refuels there (undersized absRise failing the absolute-rise gate).
	fuelTo := to.Add(time.Duration(refuelPeakSearchMaxMin) * time.Minute)

	// Single query: fuel samples (returned sorted)
	samples, err := d.src.LevelSamples(ctx, subject, vss.FieldPowertrainFuelSystemRelativeLevel, fuelFrom, fuelTo)
	if err != nil {
		return nil, fmt.Errorf("failed to query fuel samples: %w", err)
	}
	if len(samples) < 2 {
		return []*model.Segment{}, nil
	}

	// Scan 5-min windows for large rises; track sample index incrementally
	var raw []timeRange
	t := from.Truncate(time.Minute)
	if t.Before(from) {
		t = t.Add(time.Minute)
	}
	for !t.Add(windowDur).After(to) {
		windowEnd := t.Add(windowDur)
		fuelStart := sampleAtOrBefore(samples, t)
		fuelEnd := sampleAtOrBefore(samples, windowEnd)
		if fuelStart >= refuelMinFuelEpsilon {
			risePct := (fuelEnd - fuelStart) / fuelStart * 100
			if risePct > float64(minRise) {
				troughTime, peakTime, absRise := findRefuelTroughAndPeak(samples, t, windowEnd)
				if !troughTime.IsZero() && !peakTime.IsZero() && peakTime.After(troughTime) && absRise >= refuelMinAbsoluteRisePct {
					if troughTime.Before(from) {
						troughTime = from
					}
					if peakTime.After(to) {
						peakTime = to
					}
					if int(peakTime.Sub(troughTime).Seconds()) >= minDuration {
						raw = append(raw, timeRange{start: troughTime, end: peakTime})
					}
				}
			}
		}
		t = t.Add(time.Minute)
	}

	merged := mergeTimeRanges(raw, 0, minDuration, from, to, nil)
	return timeRangesToSegments(merged, from), nil
}

// findRefuelTroughAndPeak finds the trough (last low sample at or before riseStart) and
// peak (first sample where fuel stabilizes high after riseEnd) around a detected fuel rise.
// Uses binary search to jump to the relevant indices. samples must be sorted by TS.
func findRefuelTroughAndPeak(samples []LevelSample, riseStart, riseEnd time.Time) (trough, peak time.Time, absRise float64) {
	peakDeadline := riseEnd.Add(time.Duration(refuelPeakSearchMaxMin) * time.Minute)

	// Find trough: binary search to first index at or before riseStart, then walk backward for local min.
	startIdx := sort.Search(len(samples), func(i int) bool { return samples[i].TS.After(riseStart) })
	if startIdx > 0 {
		startIdx--
	}
	troughIdx := -1
	troughVal := 0.0
	for i := startIdx; i >= 0; i-- {
		// Strictly-decreasing (`<`, not `<=`): stop at the local min nearest the rise.
		// `<=` walked back across a flat trough to the EARLIEST equal sample, pulling the
		// segment start far earlier than the refuel and inflating its reported duration.
		if troughIdx == -1 || samples[i].Value < troughVal {
			troughIdx = i
			troughVal = samples[i].Value
		} else {
			break
		}
	}

	// Find peak: binary search to first index at or after riseEnd, then walk forward capped at deadline.
	peakStart := sort.Search(len(samples), func(i int) bool { return !samples[i].TS.Before(riseEnd) })
	peakIdx := -1
	peakVal := 0.0
	for i := peakStart; i < len(samples); i++ {
		if samples[i].TS.After(peakDeadline) {
			break
		}
		if peakIdx == -1 || samples[i].Value >= peakVal {
			peakIdx = i
			peakVal = samples[i].Value
		}
		if i+1 < len(samples) && !samples[i+1].TS.After(peakDeadline) && samples[i+1].Value < samples[i].Value-refuelPeakStabilizationDrop {
			peakIdx = i
			break
		}
	}

	if troughIdx < 0 || peakIdx < 0 {
		return time.Time{}, time.Time{}, 0
	}
	rise := samples[peakIdx].Value - samples[troughIdx].Value
	return samples[troughIdx].TS, samples[peakIdx].TS, rise
}
