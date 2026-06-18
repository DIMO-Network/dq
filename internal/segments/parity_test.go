package segments

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/require"
)

type fakeSource struct {
	windows []ActiveWindow
	levels  map[string][]LevelSample
	changes []StateChange
}

func (f fakeSource) WindowedSignalCounts(_ context.Context, _ string, _, _ time.Time, _, _, _ int) ([]ActiveWindow, error) {
	return f.windows, nil
}
func (f fakeSource) LevelSamples(_ context.Context, _, name string, _, _ time.Time) ([]LevelSample, error) {
	return f.levels[name], nil
}
func (f fakeSource) IgnitionStateChanges(_ context.Context, _ string, _, _ time.Time) ([]StateChange, error) {
	return f.changes, nil
}

// ---------------------------------------------------------------------------
// Frequency
// ---------------------------------------------------------------------------

func TestFrequencyDetectorMergesAdjacentWindows(t *testing.T) {
	timeNow = func() time.Time { return time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC) }
	t.Cleanup(func() { timeNow = time.Now })
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Windows: [base, base+2min] then gap to [base+4min, base+5min].
	// Gap = 2 min = 120s < defaultMaxGapSeconds(300s) → merged into one segment.
	src := fakeSource{windows: []ActiveWindow{
		{WindowStart: base, WindowEnd: base.Add(time.Minute), SignalCount: 50, DistinctSignalCount: 5},
		{WindowStart: base.Add(time.Minute), WindowEnd: base.Add(2 * time.Minute), SignalCount: 50, DistinctSignalCount: 5},
		{WindowStart: base.Add(4 * time.Minute), WindowEnd: base.Add(5 * time.Minute), SignalCount: 50, DistinctSignalCount: 5},
	}}
	d := NewFrequencyDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", base, base.Add(30*time.Minute), nil)
	require.NoError(t, err)
	require.Len(t, got, 1) // 2-min gap < default 300s maxGap → single merged segment
}

func TestFrequencyDetectorNormalHistoricalSegment(t *testing.T) {
	// Mirrors ch/TestMergeWindowsIntoSegments "normal segment - historical"
	historicalNow := time.Now().Add(-24 * time.Hour)
	from := historicalNow.Add(-time.Hour)
	to := historicalNow

	src := fakeSource{windows: []ActiveWindow{
		{WindowStart: from, WindowEnd: from.Add(10 * time.Minute)},
		{WindowStart: from.Add(10 * time.Minute), WindowEnd: from.Add(30 * time.Minute)},
	}}
	d := NewFrequencyDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.False(t, got[0].IsOngoing)
	require.NotNil(t, got[0].End)
	require.Equal(t, from.Add(30*time.Minute), got[0].End.Timestamp)
}

func TestFrequencyDetectorOngoingSegment(t *testing.T) {
	// Mirrors ch/TestMergeWindowsIntoSegments "ongoing segment - hits to time"
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now
	// Stub timeNow so "near real-time" check passes deterministically
	timeNow = func() time.Time { return now }
	t.Cleanup(func() { timeNow = time.Now })

	src := fakeSource{windows: []ActiveWindow{
		{WindowStart: from, WindowEnd: now},
	}}
	d := NewFrequencyDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.True(t, got[0].IsOngoing)
	require.Nil(t, got[0].End)
}

// ---------------------------------------------------------------------------
// ChangePoint / CUSUM
// ---------------------------------------------------------------------------

func TestChangePointDetectorHighSignalCount(t *testing.T) {
	// Mirrors ch/TestApplyCUSUM "high signal count windows marked active".
	// Use a historical base so "near real-time" ongoing logic does not fire.
	// Windows must span >= defaultMinSegmentDurationSeconds(240s=4min) to survive the filter.
	base := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	from := base
	to := base.Add(30 * time.Minute)
	src := fakeSource{windows: []ActiveWindow{
		{WindowStart: base, WindowEnd: base.Add(time.Minute), SignalCount: 20},
		{WindowStart: base.Add(time.Minute), WindowEnd: base.Add(2 * time.Minute), SignalCount: 25},
		{WindowStart: base.Add(2 * time.Minute), WindowEnd: base.Add(3 * time.Minute), SignalCount: 30},
		{WindowStart: base.Add(3 * time.Minute), WindowEnd: base.Add(4 * time.Minute), SignalCount: 30},
		{WindowStart: base.Add(4 * time.Minute), WindowEnd: base.Add(5 * time.Minute), SignalCount: 30},
	}}
	d := NewChangePointDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	// All 5 windows trigger CUSUM → merged into one segment of 5 min (>= 4-min minDuration).
	require.Len(t, got, 1)
}

func TestChangePointDetectorLowSignalCountNoSegment(t *testing.T) {
	// Mirrors ch/TestApplyCUSUM "low signal count windows not marked active"
	now := time.Now().Add(-24 * time.Hour)
	src := fakeSource{windows: []ActiveWindow{
		{WindowStart: now, WindowEnd: now.Add(time.Minute), SignalCount: 1},
		{WindowStart: now.Add(time.Minute), WindowEnd: now.Add(2 * time.Minute), SignalCount: 2},
	}}
	from := now.Add(-time.Hour)
	to := now.Add(-time.Hour + 30*time.Minute)
	d := NewChangePointDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestChangePointDetectorCUSUMAlgorithm(t *testing.T) {
	// Direct unit test of applyCUSUM — same fixture as ch package
	d := &ChangePointDetector{}
	now := time.Now()

	t.Run("empty returns nil", func(t *testing.T) {
		require.Nil(t, d.applyCUSUM(nil))
		require.Nil(t, d.applyCUSUM([]ActiveWindow{}))
	})

	t.Run("high signal count marked active", func(t *testing.T) {
		windows := []ActiveWindow{
			{WindowStart: now, WindowEnd: now.Add(time.Minute), SignalCount: 20},
			{WindowStart: now.Add(time.Minute), WindowEnd: now.Add(2 * time.Minute), SignalCount: 25},
			{WindowStart: now.Add(2 * time.Minute), WindowEnd: now.Add(3 * time.Minute), SignalCount: 30},
		}
		result := d.applyCUSUM(windows)
		require.Len(t, result, 3)
	})

	t.Run("inactive first window not returned", func(t *testing.T) {
		windows := []ActiveWindow{
			{WindowStart: now, WindowEnd: now.Add(time.Minute), SignalCount: 1},
			{WindowStart: now.Add(time.Minute), WindowEnd: now.Add(2 * time.Minute), SignalCount: 100},
		}
		result := d.applyCUSUM(windows)
		require.Len(t, result, 1)
		require.Equal(t, uint64(100), result[0].SignalCount)
	})
}

// ---------------------------------------------------------------------------
// Idling
// ---------------------------------------------------------------------------

func TestIdlingDetectorSingleRun(t *testing.T) {
	// Mirrors ch/TestFindIdleRpmRanges "single contiguous idle run"
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	from := base
	to := base.Add(time.Hour)

	var samples []LevelSample
	for i := 0; i <= 10; i++ {
		samples = append(samples, LevelSample{TS: min(i), Value: 700})
	}
	src := fakeSource{levels: map[string][]LevelSample{
		"powertrainCombustionEngineSpeed": samples,
	}}
	d := NewIdlingDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestIdlingDetectorGapSplitsSegments(t *testing.T) {
	// Mirrors ch/TestFindIdleRpmRanges "gap larger than maxGap splits segments"
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	from := base
	to := base.Add(time.Hour)

	samples := []LevelSample{
		{TS: min(0), Value: 700},
		{TS: min(1), Value: 700},
		{TS: min(2), Value: 700},
		{TS: min(3), Value: 700},
		{TS: min(4), Value: 700},
		{TS: min(5), Value: 700},
		// Gap of 6 minutes (> 5 min maxGap)
		{TS: min(11), Value: 700},
		{TS: min(12), Value: 700},
		{TS: min(13), Value: 700},
		{TS: min(14), Value: 700},
		{TS: min(15), Value: 700},
		{TS: min(16), Value: 700},
	}
	src := fakeSource{levels: map[string][]LevelSample{
		"powertrainCombustionEngineSpeed": samples,
	}}
	d := NewIdlingDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestIdlingDetectorZeroRpmNotIdle(t *testing.T) {
	// RPM=0 is engine off, not idle
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	from := base
	to := base.Add(time.Hour)

	var samples []LevelSample
	for i := 0; i <= 10; i++ {
		samples = append(samples, LevelSample{TS: min(i), Value: 0})
	}
	src := fakeSource{levels: map[string][]LevelSample{
		"powertrainCombustionEngineSpeed": samples,
	}}
	d := NewIdlingDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

// ---------------------------------------------------------------------------
// Refuel
// ---------------------------------------------------------------------------

func TestRefuelDetectorBasicRise(t *testing.T) {
	// Mirrors ch/TestFindRefuelTroughAndPeak "basic refuel rise"
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	// Build samples with trough at min(3)=20 and peak at min(6)=80.
	// The detector scans forward from 'from' in 5-min windows.
	// from=min(0), to=min(60). Window min(0)->min(5): fuelStart=50, fuelEnd~75 → rise ~50%.
	samples := []LevelSample{
		{TS: min(0), Value: 50},
		{TS: min(1), Value: 40},
		{TS: min(2), Value: 25},
		{TS: min(3), Value: 20}, // trough
		{TS: min(4), Value: 60},
		{TS: min(5), Value: 75},
		{TS: min(6), Value: 80}, // peak
		{TS: min(7), Value: 80},
		{TS: min(60), Value: 80},
	}
	src := fakeSource{levels: map[string][]LevelSample{
		"powertrainFuelSystemRelativeLevel": samples,
	}}
	from := base
	to := base.Add(60 * time.Minute)
	d := NewRefuelDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	// Expect exactly one refuel segment from trough (min(0)/from) to peak (min(7))
	require.Len(t, got, 1)
	require.Equal(t, from, got[0].Start.Timestamp)
	require.NotNil(t, got[0].End)
	require.Equal(t, min(7), got[0].End.Timestamp)
	require.Equal(t, 420, got[0].Duration) // 7 minutes = 420 seconds
	require.False(t, got[0].IsOngoing)
}

func TestRefuelDetectorNoRise(t *testing.T) {
	// Flat fuel → no refuel detected
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	var samples []LevelSample
	for i := 0; i <= 60; i++ {
		samples = append(samples, LevelSample{TS: min(i), Value: 50})
	}
	src := fakeSource{levels: map[string][]LevelSample{
		"powertrainFuelSystemRelativeLevel": samples,
	}}
	from := base
	to := base.Add(60 * time.Minute)
	d := NewRefuelDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestRefuelFindTroughAndPeak(t *testing.T) {
	// Direct unit test ported from ch/TestFindRefuelTroughAndPeak
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	t.Run("basic refuel rise", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 50},
			{TS: min(1), Value: 40},
			{TS: min(2), Value: 25},
			{TS: min(3), Value: 20}, // trough
			{TS: min(4), Value: 60},
			{TS: min(5), Value: 75},
			{TS: min(6), Value: 80}, // peak
			{TS: min(7), Value: 80},
		}
		trough, peak, absRise := findRefuelTroughAndPeak(samples, min(3), min(5))
		require.Equal(t, min(3), trough)
		require.Equal(t, min(7), peak)
		require.InDelta(t, 60.0, absRise, 0.01)
	})

	t.Run("trough walk-back finds local minimum", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 60},
			{TS: min(1), Value: 50},
			{TS: min(2), Value: 30},
			{TS: min(3), Value: 15}, // local min
			{TS: min(4), Value: 20}, // riseStart
			{TS: min(5), Value: 70}, // riseEnd
		}
		trough, _, _ := findRefuelTroughAndPeak(samples, min(4), min(5))
		require.Equal(t, min(3), trough)
	})

	t.Run("peak stabilization detection", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 10},
			{TS: min(5), Value: 70},
			{TS: min(6), Value: 75},
			{TS: min(7), Value: 73}, // drop of 2.0 from 75 → triggers early stabilization
			{TS: min(8), Value: 72},
		}
		_, peak, _ := findRefuelTroughAndPeak(samples, min(0), min(5))
		require.Equal(t, min(6), peak)
	})
}

// ---------------------------------------------------------------------------
// Recharge
// ---------------------------------------------------------------------------

func TestRechargeDetectorSocRiseWithStationaryOdo(t *testing.T) {
	// Mirrors ch/TestFilterRangesBySocAndOdo "keeps range with SoC increase and no odometer change"
	// We need enough SoC samples to pass the rechargeSmoothWindow+2 check (13 samples).
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	// Build a clean rising SoC curve (13+ samples so smoothing works)
	var socSamples []LevelSample
	for i := 0; i <= 20; i++ {
		socSamples = append(socSamples, LevelSample{TS: min(i), Value: float64(20 + i*3)})
	}
	// Odometer: no change
	odoSamples := []LevelSample{
		{TS: min(0), Value: 1000},
		{TS: min(20), Value: 1000},
	}
	src := fakeSource{levels: map[string][]LevelSample{
		"powertrainTractionBatteryStateOfChargeCurrent": socSamples,
		"powertrainTransmissionTravelledDistance":       odoSamples,
	}}
	from := base
	to := base.Add(30 * time.Minute)
	d := NewRechargeDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	// Expect exactly one recharge segment: smoothed SoC (window=11) spans min(5)–min(15)
	require.Len(t, got, 1)
	require.Equal(t, min(5), got[0].Start.Timestamp)
	require.NotNil(t, got[0].End)
	require.Equal(t, min(15), got[0].End.Timestamp)
	require.Equal(t, 600, got[0].Duration) // 10 minutes = 600 seconds
	require.False(t, got[0].IsOngoing)
}

func TestRechargeDetectorSocRiseWithMovingOdoFiltered(t *testing.T) {
	// Odometer increases > epsilon → segment filtered out.
	// Odo samples must be spread across the smoothed SoC range so levelFirstLastInRange
	// finds them and the filter fires. With rechargeSmoothWindow=11, smoothed samples
	// span min(5) to min(15), so odo samples must cover that interval.
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	var socSamples []LevelSample
	for i := 0; i <= 20; i++ {
		socSamples = append(socSamples, LevelSample{TS: min(i), Value: float64(20 + i*3)})
	}
	// Odo samples covering smoothed range min(5)–min(15) with >0.5km increase
	var odoSamples []LevelSample
	for i := 0; i <= 20; i++ {
		odoSamples = append(odoSamples, LevelSample{TS: min(i), Value: float64(1000 + i)}) // 1km/min
	}
	src := fakeSource{levels: map[string][]LevelSample{
		"powertrainTractionBatteryStateOfChargeCurrent": socSamples,
		"powertrainTransmissionTravelledDistance":       odoSamples,
	}}
	from := base
	to := base.Add(30 * time.Minute)
	d := NewRechargeDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestRechargeSmoothSamples(t *testing.T) {
	// Ported from ch/TestSmoothSamples
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	t.Run("window=1 returns input unchanged", func(t *testing.T) {
		samples := []LevelSample{{TS: min(0), Value: 10}, {TS: min(1), Value: 20}}
		result := smoothSamples(samples, 1)
		require.Equal(t, samples, result)
	})

	t.Run("window=3 computes rolling average", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 10},
			{TS: min(1), Value: 20},
			{TS: min(2), Value: 30},
			{TS: min(3), Value: 40},
			{TS: min(4), Value: 50},
		}
		result := smoothSamples(samples, 3)
		require.Len(t, result, 3)
		require.InDelta(t, 20.0, result[0].Value, 0.01)
		require.Equal(t, min(1), result[0].TS)
		require.InDelta(t, 30.0, result[1].Value, 0.01)
		require.InDelta(t, 40.0, result[2].Value, 0.01)
	})
}

func TestRechargeFindTroughToPeakRanges(t *testing.T) {
	// Ported from ch/TestFindTroughToPeakRanges
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	min := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }

	t.Run("single rise detected", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 20},
			{TS: min(2), Value: 22},
			{TS: min(4), Value: 25},
		}
		ranges := findTroughToPeakRanges(samples, 1.0, 60)
		require.Len(t, ranges, 1)
		require.Equal(t, min(0), ranges[0].start)
		require.Equal(t, min(4), ranges[0].end)
	})

	t.Run("two rises with dip between", func(t *testing.T) {
		samples := []LevelSample{
			{TS: min(0), Value: 20},
			{TS: min(5), Value: 30},  // peak 1
			{TS: min(10), Value: 25}, // dip
			{TS: min(15), Value: 40}, // peak 2
		}
		ranges := findTroughToPeakRanges(samples, 1.0, 60)
		require.Len(t, ranges, 2)
	})
}

// ---------------------------------------------------------------------------
// Ignition
// ---------------------------------------------------------------------------

func TestIgnitionDetectorSimpleOnOff(t *testing.T) {
	// Mirrors ch/TestBuildSegmentsWithDebouncing "simple ON/OFF creates segment"
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now

	changes := []StateChange{
		{TS: from.Add(10 * time.Minute), NewState: 1, PrevState: 0},
		{TS: from.Add(20 * time.Minute), NewState: 0, PrevState: 1},
	}
	src := fakeSource{changes: changes}
	d := NewIgnitionDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, from.Add(10*time.Minute), got[0].Start.Timestamp)
	require.NotNil(t, got[0].End)
	require.Equal(t, from.Add(20*time.Minute), got[0].End.Timestamp)
	require.False(t, got[0].IsOngoing)
}

func TestIgnitionDetectorOngoingSegment(t *testing.T) {
	// Mirrors ch/TestBuildSegmentsWithDebouncing "ongoing segment without OFF"
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now

	changes := []StateChange{
		{TS: from.Add(10 * time.Minute), NewState: 1, PrevState: 0},
	}
	src := fakeSource{changes: changes}
	d := NewIgnitionDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.True(t, got[0].IsOngoing)
	require.Nil(t, got[0].End)
}

func TestIgnitionDetectorFiltersShortOff(t *testing.T) {
	// Mirrors ch/TestFilterNoise "filters short OFF followed by ON"
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now

	changes := []StateChange{
		{TS: from.Add(10 * time.Minute), NewState: 1, PrevState: 0},
		{TS: from.Add(11 * time.Minute), NewState: 0, PrevState: 1}, // OFF: only 1 min until next ON
		{TS: from.Add(12 * time.Minute), NewState: 1, PrevState: 0}, // back ON within 5-min minIdle
		{TS: from.Add(30 * time.Minute), NewState: 0, PrevState: 1}, // real OFF
	}
	src := fakeSource{changes: changes}
	d := NewIgnitionDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	// The short OFF at 11min is debounced; result is one segment 10-30min.
	require.Len(t, got, 1)
	require.Equal(t, from.Add(10*time.Minute), got[0].Start.Timestamp)
	require.Equal(t, from.Add(30*time.Minute), got[0].End.Timestamp)
}

func TestIgnitionDetectorIgnoresPrevStateMinus1(t *testing.T) {
	// Mirrors ch/TestBuildSegmentsWithDebouncing "ignores initial ON with prev_state -1"
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now

	changes := []StateChange{
		{TS: from.Add(10 * time.Minute), NewState: 1, PrevState: -1},
	}
	src := fakeSource{changes: changes}
	d := NewIgnitionDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestIgnitionDetectorStartedBeforeRange(t *testing.T) {
	// Mirrors ch/TestBuildSegmentsWithDebouncing "startedBeforeRange flag set correctly"
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now

	changes := []StateChange{
		{TS: from.Add(-10 * time.Minute), NewState: 1, PrevState: 0},
		{TS: from.Add(10 * time.Minute), NewState: 0, PrevState: 1},
	}
	src := fakeSource{changes: changes}
	d := NewIgnitionDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.True(t, got[0].StartedBeforeRange)
}

func TestIgnitionDetectorMultipleSegments(t *testing.T) {
	// Mirrors ch/TestBuildSegmentsWithDebouncing "multiple segments"
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now

	changes := []StateChange{
		{TS: from.Add(10 * time.Minute), NewState: 1, PrevState: 0},
		{TS: from.Add(20 * time.Minute), NewState: 0, PrevState: 1},
		{TS: from.Add(40 * time.Minute), NewState: 1, PrevState: 0},
		{TS: from.Add(50 * time.Minute), NewState: 0, PrevState: 1},
	}
	src := fakeSource{changes: changes}
	d := NewIgnitionDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestIgnitionDetectorDuration(t *testing.T) {
	// Mirrors ch/TestBuildSegmentsWithDebouncing "duration calculated correctly"
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now

	start := from.Add(10 * time.Minute)
	end := from.Add(20 * time.Minute)
	changes := []StateChange{
		{TS: start, NewState: 1, PrevState: 0},
		{TS: end, NewState: 0, PrevState: 1},
	}
	src := fakeSource{changes: changes}
	d := NewIgnitionDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", from, to, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, 600, got[0].Duration) // 10 minutes = 600 seconds
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

func TestNewDetectorRegistry(t *testing.T) {
	src := fakeSource{}

	for _, mech := range []model.DetectionMechanism{
		model.DetectionMechanismIgnitionDetection,
		model.DetectionMechanismFrequencyAnalysis,
		model.DetectionMechanismChangePointDetection,
		model.DetectionMechanismIdling,
		model.DetectionMechanismRefuel,
		model.DetectionMechanismRecharge,
	} {
		det, err := NewDetector(src, mech)
		require.NoError(t, err)
		require.NotNil(t, det)
	}

	_, err := NewDetector(src, "UNKNOWN")
	require.Error(t, err)
}
