package segments

import (
	"sort"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
)

// timeRange is a lightweight start/end pair used internally by detection and merge pipelines.
// Converted to *model.Segment only at the final return to avoid intermediate heap allocations.
type timeRange struct {
	start time.Time
	end   time.Time
}

// sampleAtOrBefore returns the value of the latest sample at or before t.
// Returns 0 if no sample exists at or before t (i.e. all samples are in the future).
// samples must be sorted by TS.
func sampleAtOrBefore(samples []LevelSample, t time.Time) float64 {
	if len(samples) == 0 {
		return 0
	}
	idx := sort.Search(len(samples), func(i int) bool { return samples[i].TS.After(t) })
	if idx == 0 {
		// All samples are after t; no valid sample exists at or before t.
		return 0
	}
	return samples[idx-1].Value
}

// levelFirstLastInRange returns the first and last level value within [segStart, segEnd].
// samples must be sorted by TS. ok is false if no samples fall in range.
func levelFirstLastInRange(samples []LevelSample, segStart, segEnd time.Time) (first, last float64, ok bool) {
	if len(samples) == 0 {
		return 0, 0, false
	}
	startIdx := sort.Search(len(samples), func(i int) bool { return !samples[i].TS.Before(segStart) })
	if startIdx >= len(samples) || samples[startIdx].TS.After(segEnd) {
		return 0, 0, false
	}
	endIdx := sort.Search(len(samples), func(i int) bool { return samples[i].TS.After(segEnd) })
	if endIdx == 0 {
		return 0, 0, false
	}
	endIdx--
	if samples[endIdx].TS.Before(segStart) {
		return 0, 0, false
	}
	return samples[startIdx].Value, samples[endIdx].Value, true
}

// mergeTimeRanges merges sorted time ranges within maxGap. If shouldMerge is non-nil it is called
// to decide whether two ranges within maxGap should actually merge (e.g. odometer check).
// Only ranges with duration >= minDuration are kept. Ranges are clipped to [from, to].
func mergeTimeRanges(ranges []timeRange, maxGap time.Duration, minDuration int, from, to time.Time, shouldMerge func(a, b timeRange) bool) []timeRange {
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start.Before(ranges[j].start) })
	var out []timeRange
	cur := ranges[0]
	for i := 1; i < len(ranges); i++ {
		next := ranges[i]
		gap := next.start.Sub(cur.end)
		doMerge := gap <= maxGap
		if doMerge && shouldMerge != nil {
			doMerge = shouldMerge(cur, next)
		}
		if doMerge {
			if next.end.After(cur.end) {
				cur.end = next.end
			}
		} else {
			if tr, ok := clipTimeRange(cur, from, to, minDuration); ok {
				out = append(out, tr)
			}
			cur = next
		}
	}
	if tr, ok := clipTimeRange(cur, from, to, minDuration); ok {
		out = append(out, tr)
	}
	return out
}

// clipTimeRange clips a timeRange to [from, to] and checks minDuration. Zero from/to disables clipping.
// Segments that started before from are kept if their original (pre-clip) duration meets minDuration,
// so "started before range" segments are not dropped when the visible portion is short.
func clipTimeRange(tr timeRange, from, to time.Time, minDuration int) (timeRange, bool) {
	originalDuration := int(tr.end.Sub(tr.start).Seconds())
	clippedStart := false
	if !from.IsZero() && tr.start.Before(from) {
		clippedStart = true
		tr.start = from
	}
	if !to.IsZero() && tr.end.After(to) {
		tr.end = to
	}
	if !tr.end.After(tr.start) {
		return timeRange{}, false
	}
	clippedDuration := int(tr.end.Sub(tr.start).Seconds())
	if clippedDuration < minDuration {
		// Keep segment only if it started before from and original duration was sufficient
		if !clippedStart || originalDuration < minDuration {
			return timeRange{}, false
		}
	}
	return tr, true
}

// timeRangesToSegments converts a slice of timeRange to []*model.Segment.
// This is the single point where heap-allocated model objects are created.
// Returns an empty (non-nil) slice when ranges is empty for consistent downstream handling.
func timeRangesToSegments(ranges []timeRange, from time.Time) []*model.Segment {
	if len(ranges) == 0 {
		return []*model.Segment{}
	}
	out := make([]*model.Segment, 0, len(ranges))
	for _, tr := range ranges {
		startedBefore := tr.start.Equal(from) || tr.start.Before(from)
		end := tr.end
		durSec := int32(end.Sub(tr.start).Seconds())
		out = append(out, newSegment(tr.start, &end, durSec, false, startedBefore))
	}
	return out
}

// timeNow is the clock function used by mergeWindowsIntoSegments/windowRunToSegment.
// Tests override this to make ongoing-detection deterministic.
var timeNow = time.Now

// mergeWindowsIntoSegments merges consecutive ActiveWindows within maxGap seconds, clips to [from, to],
// and marks segments as ongoing when the last window end is within maxGap of to.
// Used by frequency and changepoint detectors.
func mergeWindowsIntoSegments(windows []ActiveWindow, from, to time.Time, maxGap, minDuration int) []*model.Segment {
	if len(windows) == 0 {
		return []*model.Segment{}
	}

	// Convert ActiveWindows to timeRanges and delegate to the shared merge pipeline.
	ranges := make([]timeRange, len(windows))
	for i, w := range windows {
		// window_end is a nominal bucket end (toStartOfInterval + 60s) that can
		// overshoot the query boundary; clamp it so the ongoing/duration math below
		// sees end <= to (a negative to-end would skew the ongoing decision and a
		// non-ongoing segment's duration would extend past the requested range).
		end := w.WindowEnd
		if end.After(to) {
			end = to
		}
		ranges[i] = timeRange{start: w.WindowStart, end: end}
	}
	merged := mergeTimeRanges(ranges, time.Duration(maxGap)*time.Second, minDuration, from, to, nil)

	// Check whether the last merged range qualifies as ongoing:
	// the run end is within maxGap of the query boundary AND the query boundary is near real-time.
	maxGapDur := time.Duration(maxGap) * time.Second
	out := make([]*model.Segment, 0, len(merged))
	for i, tr := range merged {
		isLast := i == len(merged)-1
		if isLast && to.Sub(tr.end) <= maxGapDur && timeNow().Sub(to) <= maxGapDur {
			// Ongoing segment: duration extends to 'to', end is nil.
			durSec := int32(to.Sub(tr.start).Seconds())
			if int(durSec) >= minDuration {
				out = append(out, newSegment(tr.start, nil, durSec, true, tr.start.Equal(from) || tr.start.Before(from)))
			}
		} else {
			end := tr.end
			durSec := int32(end.Sub(tr.start).Seconds())
			out = append(out, newSegment(tr.start, &end, durSec, false, tr.start.Equal(from) || tr.start.Before(from)))
		}
	}
	if len(out) == 0 {
		return []*model.Segment{}
	}
	return out
}
