package segments

// detector_unit_test.go — restores unit-test coverage that was dropped when
// internal/service/ch/*_detector_test.go files were deleted during the ch→segments refactor.
// Only scenarios NOT already covered in parity_test.go are added here.

import (
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// frequency / mergeWindowsIntoSegments — near-real-time ongoing logic
// ---------------------------------------------------------------------------

// TestMergeWindowsOngoingNearRealTime mirrors ch/TestMergeWindowsIntoSegments
// "ongoing segment - near real-time logic": window ends within maxGap of `to`
// AND timeNow().Sub(to) <= maxGap → IsOngoing, End==nil.
func TestMergeWindowsOngoingNearRealTime(t *testing.T) {
	now := time.Now()
	// Pin timeNow so the "near real-time" gate is deterministic.
	timeNow = func() time.Time { return now }
	t.Cleanup(func() { timeNow = time.Now })

	from := now.Add(-time.Hour)
	to := now
	maxGap := 300 // 5 minutes
	minDuration := 60

	// Window ends 1 minute ago — within the 5-min maxGap of `to`.
	lastWindowEnd := now.Add(-time.Minute)
	windows := []ActiveWindow{
		{WindowStart: from, WindowEnd: lastWindowEnd},
	}
	segments := mergeWindowsIntoSegments(windows, from, to, maxGap, minDuration)
	require.Len(t, segments, 1)
	require.True(t, segments[0].IsOngoing)
	require.Nil(t, segments[0].End)
	// Duration should extend from start to `to`.
	expectedDuration := int(to.Sub(from).Seconds())
	require.InDelta(t, expectedDuration, segments[0].Duration, 1)
}

// TestMergeWindowsCompletedOutsideRealTimeGap mirrors ch/TestMergeWindowsIntoSegments
// "completed segment - outside real-time gap": window ends >maxGap before `to`,
// near-now query → NOT ongoing.
func TestMergeWindowsCompletedOutsideRealTimeGap(t *testing.T) {
	now := time.Now()
	timeNow = func() time.Time { return now }
	t.Cleanup(func() { timeNow = time.Now })

	from := now.Add(-time.Hour)
	to := now
	maxGap := 300 // 5 minutes
	minDuration := 60

	// Window ends 10 minutes ago — outside the 5-min maxGap.
	lastWindowEnd := now.Add(-10 * time.Minute)
	windows := []ActiveWindow{
		{WindowStart: from, WindowEnd: lastWindowEnd},
	}
	segments := mergeWindowsIntoSegments(windows, from, to, maxGap, minDuration)
	require.Len(t, segments, 1)
	require.False(t, segments[0].IsOngoing)
	require.NotNil(t, segments[0].End)
	require.Equal(t, lastWindowEnd, segments[0].End.Timestamp)
}

// ---------------------------------------------------------------------------
// idling / findIdleRpmRanges — boundary and clip scenarios
// ---------------------------------------------------------------------------

func TestFindIdleRpmRanges(t *testing.T) {
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	from := base
	to := base.Add(time.Hour)

	maxIdleRpm := 1000
	maxGap := 300      // 5 minutes
	minDuration := 240 // 4 minutes

	t.Run("RPM exactly at maxIdleRpm boundary is idle", func(t *testing.T) {
		var samples []LevelSample
		for i := 0; i <= 10; i++ {
			samples = append(samples, LevelSample{TS: min(i), Value: 1000})
		}
		result := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, from, to)
		require.Len(t, result, 1)
	})

	t.Run("RPM just above maxIdleRpm is not idle", func(t *testing.T) {
		var samples []LevelSample
		for i := 0; i <= 10; i++ {
			samples = append(samples, LevelSample{TS: min(i), Value: 1001})
		}
		result := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, from, to)
		require.Empty(t, result)
	})

	t.Run("gap within maxGap keeps single segment", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 700},
			{TS: min(1), Value: 700},
			{TS: min(2), Value: 700},
			// Gap of 4 minutes (< 5-min maxGap)
			{TS: min(6), Value: 700},
			{TS: min(7), Value: 700},
			{TS: min(8), Value: 700},
			{TS: min(9), Value: 700},
			{TS: min(10), Value: 700},
		}
		result := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, from, to)
		require.Len(t, result, 1)
		require.Equal(t, min(0), result[0].start)
		require.Equal(t, min(10), result[0].end)
	})

	t.Run("short segment filtered by minDuration", func(t *testing.T) {
		// Only 3 minutes (180s < 240s minDuration)
		samples := []LevelSample{
			{TS: min(0), Value: 700},
			{TS: min(1), Value: 700},
			{TS: min(2), Value: 700},
			{TS: min(3), Value: 700},
		}
		result := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, from, to)
		require.Empty(t, result)
	})

	t.Run("clipTimeRange clips start before from", func(t *testing.T) {
		clipFrom := min(3)
		var samples []LevelSample
		for i := 0; i <= 10; i++ {
			samples = append(samples, LevelSample{TS: min(i), Value: 700})
		}
		result := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, clipFrom, to)
		require.Len(t, result, 1)
		require.Equal(t, clipFrom, result[0].start)
	})

	t.Run("clipTimeRange clips end after to", func(t *testing.T) {
		clipTo := min(7)
		var samples []LevelSample
		for i := 0; i <= 10; i++ {
			samples = append(samples, LevelSample{TS: min(i), Value: 700})
		}
		result := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, from, clipTo)
		require.Len(t, result, 1)
		require.Equal(t, clipTo, result[0].end)
	})

	t.Run("non-idle samples break segment", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 700},
			{TS: min(1), Value: 700},
			{TS: min(2), Value: 700},
			{TS: min(3), Value: 700},
			{TS: min(4), Value: 700},
			{TS: min(5), Value: 3000}, // not idle
			{TS: min(6), Value: 700},
			{TS: min(7), Value: 700},
			{TS: min(8), Value: 700},
			{TS: min(9), Value: 700},
			{TS: min(10), Value: 700},
			{TS: min(11), Value: 700},
		}
		result := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, from, to)
		// First run: min(0)–min(4) = 4 min; second run: min(6)–min(11) = 5 min.
		require.Len(t, result, 2)
	})

	t.Run("mixed idle and high RPM", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 700},
			{TS: min(5), Value: 700},
			{TS: min(6), Value: 5000},  // driving
			{TS: min(10), Value: 5000}, // driving
			{TS: min(15), Value: 700},
			{TS: min(20), Value: 700},
		}
		result := findIdleRpmRanges(samples, maxIdleRpm, maxGap, minDuration, from, to)
		require.Len(t, result, 2)
	})
}

// ---------------------------------------------------------------------------
// idling / resolveBaseConfig
// ---------------------------------------------------------------------------

func TestResolveBaseConfig(t *testing.T) {
	t.Run("nil config uses defaults", func(t *testing.T) {
		rc := resolveBaseConfig(nil)
		require.Equal(t, defaultMaxGapSeconds, rc.maxGapSeconds)
		require.Equal(t, defaultMinSegmentDurationSeconds, rc.minDuration)
	})

	t.Run("config overrides applied", func(t *testing.T) {
		gap := 120
		dur := 60
		rc := resolveBaseConfig(&model.SegmentConfig{
			MaxGapSeconds:             &gap,
			MinSegmentDurationSeconds: &dur,
		})
		require.Equal(t, 120, rc.maxGapSeconds)
		require.Equal(t, 60, rc.minDuration)
	})
}

// ---------------------------------------------------------------------------
// refuel / findRefuelTroughAndPeak — edge cases
// ---------------------------------------------------------------------------

func TestFindRefuelTroughAndPeakEdgeCases(t *testing.T) {
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	t.Run("single sample returns zero/empty", func(t *testing.T) {
		samples := []LevelSample{{TS: min(0), Value: 50}}
		trough, peak, absRise := findRefuelTroughAndPeak(samples, min(0), min(5))
		// With only one sample, either trough or peak will be zero, or absRise will be 0.
		require.True(t, trough.IsZero() || peak.IsZero() || absRise == 0)
	})

	t.Run("peak search capped by 30-min deadline", func(t *testing.T) {
		// Peak at min(60) is beyond the 30-min deadline from riseEnd=min(5).
		samples := []LevelSample{
			{TS: min(0), Value: 10},
			{TS: min(5), Value: 60},
			{TS: min(60), Value: 95}, // beyond deadline
		}
		trough, peak, _ := findRefuelTroughAndPeak(samples, min(0), min(5))
		require.False(t, trough.IsZero())
		// Peak should be at min(5) since min(60) is past the 30-min deadline from min(5).
		require.Equal(t, min(5), peak)
	})
}

// ---------------------------------------------------------------------------
// refuel / mergeTimeRanges
// ---------------------------------------------------------------------------

func TestRefuelMergeTimeRanges(t *testing.T) {
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	t.Run("overlapping ranges merged", func(t *testing.T) {
		ranges := []timeRange{
			{start: min(0), end: min(10)},
			{start: min(5), end: min(15)},
		}
		merged := mergeTimeRanges(ranges, 0, 0, base, min(30), nil)
		require.Len(t, merged, 1)
		require.Equal(t, min(0), merged[0].start)
		require.Equal(t, min(15), merged[0].end)
	})

	t.Run("non-overlapping ranges stay separate", func(t *testing.T) {
		ranges := []timeRange{
			{start: min(0), end: min(5)},
			{start: min(10), end: min(15)},
		}
		merged := mergeTimeRanges(ranges, 0, 0, base, min(30), nil)
		require.Len(t, merged, 2)
	})

	t.Run("short ranges filtered by minDuration", func(t *testing.T) {
		ranges := []timeRange{
			{start: min(0), end: min(1)}, // 60s < 240s minDuration
		}
		merged := mergeTimeRanges(ranges, 0, 240, base, min(30), nil)
		require.Empty(t, merged)
	})
}

// ---------------------------------------------------------------------------
// recharge / smoothSamples — window larger than samples
// ---------------------------------------------------------------------------

func TestSmoothSamplesWindowLargerThanSamples(t *testing.T) {
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	// window (5) > len(samples) (2) → input returned unchanged.
	samples := []LevelSample{{TS: min(0), Value: 10}, {TS: min(1), Value: 20}}
	result := smoothSamples(samples, 5)
	require.Equal(t, samples, result)
}

// ---------------------------------------------------------------------------
// recharge / findTroughToPeakRanges — filtered cases
// ---------------------------------------------------------------------------

func TestFindTroughToPeakRangesFiltered(t *testing.T) {
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	t.Run("empty and single-sample return nil", func(t *testing.T) {
		require.Nil(t, findTroughToPeakRanges(nil, 1.0, 61))
		require.Nil(t, findTroughToPeakRanges([]LevelSample{{TS: min(0), Value: 10}}, 1.0, 61))
	})

	t.Run("rise below minRisePct filtered", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 50},
			{TS: min(2), Value: 50.5}, // rise of 0.5 < 1.0 minRisePct
		}
		ranges := findTroughToPeakRanges(samples, 1.0, 0)
		require.Empty(t, ranges)
	})

	t.Run("rise below minDuration filtered", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 20},
			{TS: min(0).Add(30 * time.Second), Value: 30}, // 30s < 61s minDuration
		}
		ranges := findTroughToPeakRanges(samples, 1.0, 61)
		require.Empty(t, ranges)
	})
}

// ---------------------------------------------------------------------------
// recharge / filterRangesBySocAndOdo
// ---------------------------------------------------------------------------

func TestFilterRangesBySocAndOdo(t *testing.T) {
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	t.Run("SoC decreases — range dropped", func(t *testing.T) {
		ranges := []timeRange{{start: min(0), end: min(10)}}
		soc := []LevelSample{{TS: min(0), Value: 80}, {TS: min(10), Value: 60}}
		odo := []LevelSample{{TS: min(0), Value: 1000}, {TS: min(10), Value: 1000}}
		result := filterRangesBySocAndOdo(ranges, soc, odo)
		require.Empty(t, result)
	})

	t.Run("odometer increases beyond epsilon — range dropped", func(t *testing.T) {
		ranges := []timeRange{{start: min(0), end: min(10)}}
		soc := []LevelSample{{TS: min(0), Value: 20}, {TS: min(10), Value: 80}}
		// 2km > 0.5km epsilon
		odo := []LevelSample{{TS: min(0), Value: 1000}, {TS: min(10), Value: 1002}}
		result := filterRangesBySocAndOdo(ranges, soc, odo)
		require.Empty(t, result)
	})

	t.Run("odometer within epsilon is kept", func(t *testing.T) {
		ranges := []timeRange{{start: min(0), end: min(10)}}
		soc := []LevelSample{{TS: min(0), Value: 20}, {TS: min(10), Value: 80}}
		// 0.3km < 0.5km epsilon
		odo := []LevelSample{{TS: min(0), Value: 1000}, {TS: min(10), Value: 1000.3}}
		result := filterRangesBySocAndOdo(ranges, soc, odo)
		require.Len(t, result, 1)
	})

	t.Run("no odometer data keeps range", func(t *testing.T) {
		ranges := []timeRange{{start: min(0), end: min(10)}}
		soc := []LevelSample{{TS: min(0), Value: 20}, {TS: min(10), Value: 80}}
		result := filterRangesBySocAndOdo(ranges, soc, nil)
		require.Len(t, result, 1)
	})
}

// ---------------------------------------------------------------------------
// recharge / levelFirstLastInRange
// ---------------------------------------------------------------------------

func TestLevelFirstLastInRange(t *testing.T) {
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	t.Run("returns first and last in range", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 10},
			{TS: min(5), Value: 50},
			{TS: min(10), Value: 90},
		}
		first, last, ok := levelFirstLastInRange(samples, min(0), min(10))
		require.True(t, ok)
		require.Equal(t, 10.0, first)
		require.Equal(t, 90.0, last)
	})

	t.Run("no samples in range", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 10},
		}
		_, _, ok := levelFirstLastInRange(samples, min(5), min(10))
		require.False(t, ok)
	})

	t.Run("empty samples", func(t *testing.T) {
		_, _, ok := levelFirstLastInRange(nil, min(0), min(10))
		require.False(t, ok)
	})
}

// ---------------------------------------------------------------------------
// ignition / filterNoise — direct unit tests
// ---------------------------------------------------------------------------

func TestFilterNoise(t *testing.T) {
	d := &IgnitionDetector{}
	now := time.Now()
	minIdle := 300 // 5 minutes

	t.Run("empty input returns nil", func(t *testing.T) {
		result := d.filterNoise(nil, minIdle)
		require.Nil(t, result)

		result = d.filterNoise([]StateChange{}, minIdle)
		require.Nil(t, result)
	})

	t.Run("keeps all ON signals", func(t *testing.T) {
		changes := []StateChange{
			{TS: now, NewState: 1, PrevState: 0},
			{TS: now.Add(time.Minute), NewState: 1, PrevState: 0},
		}
		result := d.filterNoise(changes, minIdle)
		require.Len(t, result, 2)
	})

	t.Run("keeps long OFF signals", func(t *testing.T) {
		// OFF at T=0, ON at T=10min (9-min gap > 5-min minIdle) → OFF is kept.
		changes := []StateChange{
			{TS: now, NewState: 1, PrevState: 0},
			{TS: now.Add(time.Minute), NewState: 0, PrevState: 1},
			{TS: now.Add(10 * time.Minute), NewState: 1, PrevState: 0},
		}
		result := d.filterNoise(changes, minIdle)
		require.Len(t, result, 3)
	})

	t.Run("keeps final OFF with no following ON", func(t *testing.T) {
		changes := []StateChange{
			{TS: now, NewState: 1, PrevState: 0},
			{TS: now.Add(10 * time.Minute), NewState: 0, PrevState: 1},
		}
		result := d.filterNoise(changes, minIdle)
		require.Len(t, result, 2)
		require.Equal(t, float64(0), result[1].NewState)
	})
}

// ---------------------------------------------------------------------------
// ignition / buildSegmentsWithDebouncing — short segment filtered
// ---------------------------------------------------------------------------

func TestBuildSegmentsWithDebouncingFiltersShortSegment(t *testing.T) {
	d := &IgnitionDetector{}
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now
	minIdle := 300    // 5 minutes
	minDuration := 60 // 1 minute

	// Very short segment (30 seconds < minDuration=60s) must not be emitted.
	changes := []StateChange{
		{TS: from.Add(10 * time.Minute), NewState: 1, PrevState: 0},
		{TS: from.Add(10*time.Minute + 30*time.Second), NewState: 0, PrevState: 1},
	}
	result := d.buildSegmentsWithDebouncing(changes, from, to, minIdle, minDuration)
	require.Empty(t, result)
}
