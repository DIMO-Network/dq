// segments_lake_parity_test.go — e2e test for LakeSegments.
//
// Approach: golden-vector comparison against expected segment shapes.
// The detector algorithms are backend-agnostic (segments.NewDetector with only
// the SignalSource differing), so correctness is
// guaranteed by construction; the unit tests in internal/segments/parity_test.go
// pin the algorithm. This test validates that LakeSegments correctly fetches
// data from lake.signals AND invokes the detectors, producing the expected
// segment shapes. The golden vectors are ported directly from the unit tests in
// internal/segments/parity_test.go and internal/segments/detector_unit_test.go.
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLakeSignalsService opens a DuckDB service with a file-backed DuckLake
// catalog and creates the minimal lake.signals table needed for segment tests.
func newLakeSignalsService(t *testing.T) *duck.Service {
	t.Helper()
	dir := t.TempDir()
	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      dir + "/catalog.ducklake",
		DataPath:        dir + "/lakedata",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	_, err = svc.DB().ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS lake.signals (
			subject VARCHAR,
			subject_bucket INTEGER,
			name VARCHAR,
			timestamp TIMESTAMPTZ,
			source VARCHAR,
			producer VARCHAR,
			cloud_event_id VARCHAR,
			value_number DOUBLE,
			value_string VARCHAR,
			loc_lat DOUBLE,
			loc_lon DOUBLE,
			loc_hdop DOUBLE,
			loc_heading DOUBLE
		)`)
	require.NoError(t, err)
	return svc
}

// insertLakeSignal inserts one signal row into lake.signals for segment parity tests.
func insertLakeSignal(t *testing.T, svc *duck.Service, subject, name, ceID string, ts time.Time, valNum float64) {
	t.Helper()
	_, err := svc.DB().ExecContext(context.Background(),
		`INSERT INTO lake.signals (subject, subject_bucket, name, timestamp, source, producer, cloud_event_id, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading)
		 VALUES (?, ?, ?, ?, 'src', 'prod', ?, ?, '', 0.0, 0.0, 0.0, 0.0)`,
		subject, duck.HashBucket(subject), name, ts.UTC(), ceID, valNum)
	require.NoError(t, err)
}

const pParitySubject = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:500"

// TestSegmentsLakeParity_Ignition checks that LakeSegments detects a simple
// ignition ON→OFF pair, matching golden vectors from parity_test.go
// TestIgnitionDetectorSimpleOnOff.
func TestSegmentsLakeParity_Ignition(t *testing.T) {
	ctx := context.Background()
	svc := newLakeSignalsService(t)
	ls := duck.NewLakeSegments(svc)

	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	from := now.Add(-time.Hour)
	to := now

	// ON transition at from+10min, OFF at from+20min
	insertLakeSignal(t, svc, pParitySubject, "isIgnitionOn", "ce-pre", from.Add(-5*time.Minute), 0.0) // pre-from seed: OFF
	insertLakeSignal(t, svc, pParitySubject, "isIgnitionOn", "ce-1", from.Add(10*time.Minute), 1.0)
	insertLakeSignal(t, svc, pParitySubject, "isIgnitionOn", "ce-2", from.Add(20*time.Minute), 0.0)

	got, err := ls.GetSegments(ctx, pParitySubject, from, to, model.DetectionMechanismIgnitionDetection, nil)
	require.NoError(t, err)
	// Golden: one segment from+10min → from+20min, not ongoing, duration=600s
	require.Len(t, got, 1)
	assert.Equal(t, from.Add(10*time.Minute), got[0].Start.Timestamp)
	require.NotNil(t, got[0].End)
	assert.Equal(t, from.Add(20*time.Minute), got[0].End.Timestamp)
	assert.Equal(t, 600, got[0].Duration)
	assert.False(t, got[0].IsOngoing)
}

// TestSegmentsLakeParity_IgnitionOngoing verifies that LakeSegments marks a
// segment as ongoing when there is no OFF transition.
func TestSegmentsLakeParity_IgnitionOngoing(t *testing.T) {
	ctx := context.Background()
	svc := newLakeSignalsService(t)
	ls := duck.NewLakeSegments(svc)

	now := time.Now().UTC().Truncate(time.Second)
	from := now.Add(-time.Hour)
	to := now

	insertLakeSignal(t, svc, pParitySubject+"_ong", "isIgnitionOn", "ce-pre", from.Add(-5*time.Minute), 0.0)
	insertLakeSignal(t, svc, pParitySubject+"_ong", "isIgnitionOn", "ce-1", from.Add(10*time.Minute), 1.0)

	got, err := ls.GetSegments(ctx, pParitySubject+"_ong", from, to, model.DetectionMechanismIgnitionDetection, nil)
	require.NoError(t, err)
	// Golden: one ongoing segment, no End.
	require.Len(t, got, 1)
	assert.True(t, got[0].IsOngoing)
	assert.Nil(t, got[0].End)
}

// TestSegmentsLakeParity_FrequencyMergesAdjacentWindows checks that LakeSegments
// merges adjacent active windows within the gap threshold into one segment,
// porting the golden vector from parity_test.go TestFrequencyDetectorMergesAdjacentWindows.
//
// The frequency detector requires defaultSignalCountThreshold=10 signals and
// defaultDistinctSignalCountThreshold=2 distinct names per 60s window. We seed
// two qualifying windows in minute 0 and minute 4 (gap=4min < 5min maxGap), which
// must merge into one segment.
func TestSegmentsLakeParity_FrequencyMergesAdjacentWindows(t *testing.T) {
	ctx := context.Background()
	svc := newLakeSignalsService(t)
	ls := duck.NewLakeSegments(svc)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	from := base
	to := base.Add(30 * time.Minute)
	subject := pParitySubject + "_freq"

	// Insert 10 signal rows into minute 0 (alternating 2 distinct names) and
	// 10 rows into minute 4 — both windows pass sig>=10 AND dist>=2 thresholds.
	for i := 0; i < 5; i++ {
		ts0 := base.Add(time.Duration(i*6) * time.Second)
		ts4 := base.Add(4*time.Minute + time.Duration(i*6)*time.Second)
		insertLakeSignal(t, svc, subject, "speed", "f0a-"+ts0.Format(time.RFC3339Nano), ts0, 50)
		insertLakeSignal(t, svc, subject, "powertrainTransmissionTravelledDistance", "f0b-"+ts0.Format(time.RFC3339Nano), ts0.Add(time.Second), 100)
		insertLakeSignal(t, svc, subject, "speed", "f4a-"+ts4.Format(time.RFC3339Nano), ts4, 55)
		insertLakeSignal(t, svc, subject, "powertrainTransmissionTravelledDistance", "f4b-"+ts4.Format(time.RFC3339Nano), ts4.Add(time.Second), 110)
	}

	got, err := ls.GetSegments(ctx, subject, from, to, model.DetectionMechanismFrequencyAnalysis, nil)
	require.NoError(t, err)
	// Golden: minute 0 and minute 4 windows are within 5-min gap → merged into one segment.
	require.Len(t, got, 1, "two active windows within maxGap must merge into one segment")
}

// TestSegmentsLakeParity_IdlingSingleRun checks that LakeSegments detects a
// contiguous idle RPM run, matching parity_test.go TestIdlingDetectorSingleRun.
func TestSegmentsLakeParity_IdlingSingleRun(t *testing.T) {
	ctx := context.Background()
	svc := newLakeSignalsService(t)
	ls := duck.NewLakeSegments(svc)

	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	from := base
	to := base.Add(time.Hour)
	subject := pParitySubject + "_idle"

	// 11 RPM samples at 700 (idle) covering 10 minutes → one idle segment.
	for i := 0; i <= 10; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		insertLakeSignal(t, svc, subject, "powertrainCombustionEngineSpeed", "idle-"+time.Duration(i).String(), ts, 700)
	}

	got, err := ls.GetSegments(ctx, subject, from, to, model.DetectionMechanismIdling, nil)
	require.NoError(t, err)
	// Golden: one idle segment.
	require.Len(t, got, 1)
}

// TestSegmentsLakeParity_RefuelBasicRise checks that LakeSegments detects a
// refuel event from a fuel level rise, matching parity_test.go TestRefuelDetectorBasicRise.
func TestSegmentsLakeParity_RefuelBasicRise(t *testing.T) {
	ctx := context.Background()
	svc := newLakeSignalsService(t)
	ls := duck.NewLakeSegments(svc)

	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	from := base
	to := base.Add(60 * time.Minute)
	subject := pParitySubject + "_refuel"

	// Fuel level: falls from 50 to 20 (trough at min 3), then rises to 80 (peak at min 6).
	fuelLevels := []struct {
		min float64
		val float64
	}{
		{0, 50}, {1, 40}, {2, 25}, {3, 20}, {4, 60}, {5, 75}, {6, 80}, {7, 80}, {60, 80},
	}
	for _, fl := range fuelLevels {
		ts := base.Add(time.Duration(fl.min) * time.Minute)
		insertLakeSignal(t, svc, subject, "powertrainFuelSystemRelativeLevel", "rf-"+time.Duration(fl.min).String(), ts, fl.val)
	}

	got, err := ls.GetSegments(ctx, subject, from, to, model.DetectionMechanismRefuel, nil)
	require.NoError(t, err)
	// Golden: one refuel segment.
	require.Len(t, got, 1)
	assert.Equal(t, from, got[0].Start.Timestamp, "refuel starts at from (trough walk-back reaches from)")
	require.NotNil(t, got[0].End)
	assert.False(t, got[0].IsOngoing)
}

// TestSegmentsLakeParity_RechargeSoCRise checks that LakeSegments detects a
// recharge from a rising SoC with stationary odometer,
// matching parity_test.go TestRechargeDetectorSocRiseWithStationaryOdo.
func TestSegmentsLakeParity_RechargeSoCRise(t *testing.T) {
	ctx := context.Background()
	svc := newLakeSignalsService(t)
	ls := duck.NewLakeSegments(svc)

	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	from := base
	to := base.Add(30 * time.Minute)
	subject := pParitySubject + "_recharge"

	// Rising SoC: 20+i*3 for i in 0..20 (21 samples; smoothing window=11 needs 13+).
	for i := 0; i <= 20; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		insertLakeSignal(t, svc, subject, "powertrainTractionBatteryStateOfChargeCurrent", "soc-"+time.Duration(i).String(), ts, float64(20+i*3))
	}
	// Stationary odometer.
	insertLakeSignal(t, svc, subject, "powertrainTransmissionTravelledDistance", "odo-0", base, 1000)
	insertLakeSignal(t, svc, subject, "powertrainTransmissionTravelledDistance", "odo-20", base.Add(20*time.Minute), 1000)

	got, err := ls.GetSegments(ctx, subject, from, to, model.DetectionMechanismRecharge, nil)
	require.NoError(t, err)
	// Golden: one recharge segment.
	require.Len(t, got, 1)
	assert.False(t, got[0].IsOngoing)
}

// TestSegmentsLakeParity_ChangePointHighSignal checks that LakeSegments detects
// a segment when CUSUM flags sustained high signal counts, matching
// parity_test.go TestChangePointDetectorHighSignalCount.
//
// The CUSUM detector requires defaultCUSUMSignalCountThreshold=10 signals AND
// defaultCUSUMDistinctSignalCountThreshold=2 distinct names per 60s window. We
// seed 5 consecutive windows with 20 rows each across 2 distinct signal names so
// CUSUM accumulates and triggers, merging into one segment of >=4min duration.
func TestSegmentsLakeParity_ChangePointHighSignal(t *testing.T) {
	ctx := context.Background()
	svc := newLakeSignalsService(t)
	ls := duck.NewLakeSegments(svc)

	base := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	from := base
	to := base.Add(30 * time.Minute)
	subject := pParitySubject + "_cp"

	// 5 consecutive 60s windows, each with 20 rows across 2 distinct signal names.
	names := []string{"speed", "powertrainTransmissionTravelledDistance"}
	for winIdx := 0; winIdx < 5; winIdx++ {
		for j := 0; j < 10; j++ {
			for _, name := range names {
				ts := base.Add(time.Duration(winIdx)*time.Minute + time.Duration(j*3)*time.Second)
				insertLakeSignal(t, svc, subject, name, "cp-"+name[:3]+"-"+ts.Format(time.RFC3339Nano), ts, 50)
			}
		}
	}

	got, err := ls.GetSegments(ctx, subject, from, to, model.DetectionMechanismChangePointDetection, nil)
	require.NoError(t, err)
	// Golden: one segment (high-count CUSUM windows merged; >=4min duration).
	require.Len(t, got, 1)
}
