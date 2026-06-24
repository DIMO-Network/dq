package segments

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

const (
	rechargeDefaultMinDurationSeconds = 60            // shorter default than other detectors — charge sessions can be brief
	rechargeSessionGapMax             = 2 * time.Hour // merge consecutive segments if gap ≤ this and odometer unchanged
	rechargeOdometerEpsilonKm         = 0.5           // allow odometer increase ≤ this (noise) to still count as stationary
	rechargeSmoothWindow              = 11            // rolling average window for SoC smoothing (~11 samples ≈ 11 min)
	rechargeMinRisePct                = 1.0           // trough-to-peak must rise at least this much to be a candidate
)

// RechargeDetector detects recharge segments by finding trough-to-peak rises in the SoC curve.
type RechargeDetector struct {
	src SignalSource
}

// NewRechargeDetector creates a new RechargeDetector with the given source.
func NewRechargeDetector(src SignalSource) *RechargeDetector {
	return &RechargeDetector{src: src}
}

// DetectSegments finds periods where state of charge rises (trough to peak), filters by odometer, and merges nearby sessions.
func (d *RechargeDetector) DetectSegments(
	ctx context.Context,
	subject string,
	from, to time.Time,
	config *model.SegmentConfig,
) ([]*model.Segment, error) {
	rc := resolveBaseConfig(config)
	// Use a shorter default minDuration for recharge; still honor explicit user override.
	if config == nil || config.MinSegmentDurationSeconds == nil {
		rc.minDuration = rechargeDefaultMinDurationSeconds
	}
	minRisePct := rechargeMinRisePct
	if config != nil && config.MinIncreasePercent != nil && *config.MinIncreasePercent > 0 {
		minRisePct = float64(*config.MinIncreasePercent)
	}
	return detectRechargeSegments(ctx, d.src, subject, from, to, rc.minDuration, minRisePct)
}

// detectRechargeSegments: 2 queries (SoC + odometer), then all processing in-memory.
func detectRechargeSegments(ctx context.Context, src SignalSource, subject string, from, to time.Time, minDuration int, minRisePct float64) ([]*model.Segment, error) {
	// Query 1: SoC samples (returned sorted)
	socSamples, err := src.LevelSamples(ctx, subject, vss.FieldPowertrainTractionBatteryStateOfChargeCurrent, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query SoC samples: %w", err)
	}
	if len(socSamples) < rechargeSmoothWindow+2 {
		return []*model.Segment{}, nil
	}

	// Query 2: Odometer samples (returned sorted)
	odoSamples, err := src.LevelSamples(ctx, subject, vss.FieldPowertrainTransmissionTravelledDistance, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query odometer samples: %w", err)
	}

	// Step 1: Smooth SoC to eliminate per-sample noise
	smoothed := smoothSamples(socSamples, rechargeSmoothWindow)

	// Step 2: Find trough-to-peak ranges from smoothed curve
	candidates := findTroughToPeakRanges(smoothed, minRisePct, minDuration)

	// Step 3: Filter by SoC increase and odometer non-increase
	filtered := filterRangesBySocAndOdo(candidates, socSamples, odoSamples)

	// Step 4: Merge consecutive sessions (with odometer check)
	shouldMerge := func(a, b timeRange) bool {
		_, odoCurEnd, ok1 := levelFirstLastInRange(odoSamples, a.start, a.end)
		odoNextStart, _, ok2 := levelFirstLastInRange(odoSamples, b.start, b.end)
		// Stationary between sessions: allow ≤ the same odometer-noise epsilon used in
		// filterRangesBySocAndOdo (a raw float == would fail to merge two real charge
		// sessions if the odometer wobbled by even 0.001 km).
		return ok1 && ok2 && math.Abs(odoNextStart-odoCurEnd) <= rechargeOdometerEpsilonKm
	}
	// Merge with zero from/to to skip clipping (already filtered/clipped upstream)
	merged := mergeTimeRanges(filtered, rechargeSessionGapMax, minDuration, time.Time{}, time.Time{}, shouldMerge)

	return timeRangesToSegments(merged, from), nil
}

// smoothSamples applies a rolling average over the given window size.
// Timestamps are taken from the center sample of each window.
// Uses per-position summation for exact floating-point reproducibility.
func smoothSamples(samples []LevelSample, window int) []LevelSample {
	if window <= 1 || len(samples) <= window {
		return samples
	}
	half := window / 2
	// The window is symmetric (±half), so it sums 2*half+1 samples — exactly `window`
	// for an odd window but window+1 for an even one. Divide by the actual count so an
	// even window isn't biased high (the only caller uses the odd rechargeSmoothWindow,
	// but the helper must hold its "rolling average" contract for any window).
	denom := float64(2*half + 1)
	out := make([]LevelSample, 0, len(samples)-window+1)
	for i := half; i < len(samples)-half; i++ {
		sum := 0.0
		for j := i - half; j <= i+half; j++ {
			sum += samples[j].Value
		}
		out = append(out, LevelSample{TS: samples[i].TS, Value: sum / denom})
	}
	return out
}

// findTroughToPeakRanges walks smoothed SoC samples and finds every rise from a local trough to a local peak.
func findTroughToPeakRanges(samples []LevelSample, minRisePct float64, minDuration int) []timeRange {
	if len(samples) < 2 {
		return nil
	}

	const (
		dirRising  = 1
		dirFalling = -1
	)

	var ranges []timeRange
	dir := 0
	troughIdx := 0
	peakIdx := 0

	for i := 1; i < len(samples); i++ {
		diff := samples[i].Value - samples[i-1].Value
		if diff > 0 {
			if dir == dirFalling {
				troughIdx = i - 1
			}
			dir = dirRising
			peakIdx = i
		} else if diff < 0 {
			if dir == dirRising {
				appendTroughToPeak(samples, troughIdx, peakIdx, minRisePct, minDuration, &ranges)
			}
			dir = dirFalling
		}
	}
	if dir == dirRising {
		appendTroughToPeak(samples, troughIdx, peakIdx, minRisePct, minDuration, &ranges)
	}
	return ranges
}

// appendTroughToPeak appends a timeRange if the rise meets minimum criteria.
func appendTroughToPeak(samples []LevelSample, troughIdx, peakIdx int, minRisePct float64, minDuration int, out *[]timeRange) {
	rise := samples[peakIdx].Value - samples[troughIdx].Value
	if rise < minRisePct {
		return
	}
	start := samples[troughIdx].TS
	end := samples[peakIdx].TS
	if int(end.Sub(start).Seconds()) < minDuration {
		return
	}
	*out = append(*out, timeRange{start: start, end: end})
}

// filterRangesBySocAndOdo keeps only ranges where SoC increased and odometer did not increase beyond epsilon.
func filterRangesBySocAndOdo(ranges []timeRange, socSamples, odoSamples []LevelSample) []timeRange {
	out := make([]timeRange, 0, len(ranges))
	for _, tr := range ranges {
		socFirst, socLast, socOk := levelFirstLastInRange(socSamples, tr.start, tr.end)
		if !socOk || socLast <= socFirst {
			continue
		}
		odoFirst, odoLast, odoOk := levelFirstLastInRange(odoSamples, tr.start, tr.end)
		if odoOk && (odoLast-odoFirst) > rechargeOdometerEpsilonKm {
			continue
		}
		out = append(out, tr)
	}
	return out
}
