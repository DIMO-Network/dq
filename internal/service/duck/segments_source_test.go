package duck

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLakeServiceForTest opens a DuckDB service with a file-backed DuckLake
// catalog and creates the minimal lake.signals table needed for segment tests.
func newLakeServiceForTest(t *testing.T) *Service {
	t.Helper()
	dir := t.TempDir()
	svc, err := NewService(Config{
		DuckLakeEnabled: true,
		CatalogDSN:      dir + "/catalog.ducklake",
		DataPath:        dir + "/lakedata",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	// Create lake.signals matching materializer.SignalRow schema (rows.go).
	_, err = svc.db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS lake.signals (
			subject VARCHAR,
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

// insertSignal inserts one signal row into lake.signals for testing.
func insertSignal(t *testing.T, svc *Service, subject, name, ceID string, ts time.Time, valNum float64) {
	t.Helper()
	_, err := svc.db.ExecContext(context.Background(),
		`INSERT INTO lake.signals (subject, name, timestamp, source, producer, cloud_event_id, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading)
		 VALUES (?, ?, ?, 'src', 'prod', ?, ?, '', 0.0, 0.0, 0.0, 0.0)`,
		subject, name, ts.UTC(), ceID, valNum)
	require.NoError(t, err)
}

// TestLakeSignalSource_WindowedSignalCounts verifies bucketing and threshold
// filtering over lake.signals, including dedup of duplicate (subject,name,ts).
func TestLakeSignalSource_WindowedSignalCounts(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := testSubject1
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Window size: 60 seconds. Threshold: 2 signals, 2 distinct names.
	win := 60

	// Minute 0: 2 distinct signals — should pass threshold.
	insertSignal(t, svc, subject, "speed", "ce-1", base.Add(10*time.Second), 50)
	insertSignal(t, svc, subject, "powertrainTransmissionTravelledDistance", "ce-2", base.Add(20*time.Second), 100)
	// Minute 1: only 1 distinct signal — below distinctSignalThreshold=2.
	insertSignal(t, svc, subject, "speed", "ce-3", base.Add(70*time.Second), 60)
	// Minute 2: 3 distinct signals — should pass.
	insertSignal(t, svc, subject, "speed", "ce-4", base.Add(130*time.Second), 55)
	insertSignal(t, svc, subject, "powertrainTransmissionTravelledDistance", "ce-5", base.Add(140*time.Second), 110)
	insertSignal(t, svc, subject, "powertrainCombustionEngineRPM", "ce-6", base.Add(150*time.Second), 1000)
	// Duplicate row for same (subject, name, timestamp) — must be deduped, not double-counted.
	insertSignal(t, svc, subject, "speed", "ce-4-dup", base.Add(130*time.Second), 55) // same ts as ce-4

	windows, err := src.WindowedSignalCounts(ctx, subject, base, base.Add(3*time.Minute), win, 2, 2)
	require.NoError(t, err)

	// Expect windows for minute 0 and minute 2 (minute 1 has only 1 distinct name).
	require.Len(t, windows, 2, "expected 2 windows passing threshold (minutes 0 and 2)")

	w0 := windows[0]
	assert.Equal(t, base, w0.WindowStart)
	assert.Equal(t, base.Add(time.Minute), w0.WindowEnd)
	assert.EqualValues(t, 2, w0.SignalCount)
	assert.EqualValues(t, 2, w0.DistinctSignalCount)

	w2 := windows[1]
	assert.Equal(t, base.Add(2*time.Minute), w2.WindowStart)
	assert.Equal(t, base.Add(3*time.Minute), w2.WindowEnd)
	// After dedup, ce-4-dup collapses with ce-4: 3 rows, 3 distinct.
	assert.EqualValues(t, 3, w2.SignalCount)
	assert.EqualValues(t, 3, w2.DistinctSignalCount)
}

// TestLakeSignalSource_LevelSamples verifies timestamp-ordered samples and dedup.
func TestLakeSignalSource_LevelSamples(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := testSubject1
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	insertSignal(t, svc, subject, "powertrainCombustionEngineRPM", "ce-1", base, 1000)
	insertSignal(t, svc, subject, "powertrainCombustionEngineRPM", "ce-2", base.Add(5*time.Second), 1200)
	insertSignal(t, svc, subject, "powertrainCombustionEngineRPM", "ce-3", base.Add(10*time.Second), 900)
	// Duplicate timestamp — should be deduped.
	insertSignal(t, svc, subject, "powertrainCombustionEngineRPM", "ce-2b", base.Add(5*time.Second), 1300) // same ts as ce-2
	// Different signal name — must not appear.
	insertSignal(t, svc, subject, "speed", "ce-4", base.Add(3*time.Second), 50)

	samples, err := src.LevelSamples(ctx, subject, "powertrainCombustionEngineRPM", base, base.Add(time.Minute))
	require.NoError(t, err)

	// After dedup, 3 distinct timestamps for the RPM signal.
	require.Len(t, samples, 3)
	assert.Equal(t, base, samples[0].TS)
	assert.Equal(t, 1000.0, samples[0].Value)
	// ce-2 wins over ce-2b (ORDER BY cloud_event_id picks "ce-2" < "ce-2b").
	assert.Equal(t, base.Add(5*time.Second), samples[1].TS)
	assert.Equal(t, 1200.0, samples[1].Value)
	assert.Equal(t, base.Add(10*time.Second), samples[2].TS)
	assert.Equal(t, 900.0, samples[2].Value)
}

// TestLakeSignalSource_IgnitionStateChanges verifies:
//   - Transitions in [from, to) are returned.
//   - Exactly one pre-from seed row is returned (last change before from).
//   - The lookback is capped at 30 days.
//   - No seed row when there is no prior transition.
func TestLakeSignalSource_IgnitionStateChanges(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := testSubject1
	from := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	to := from.Add(2 * time.Hour)

	// Pre-from transitions (within 30-day lookback):
	//   t-2h: OFF→ON (ignition was off then switched on)
	//   t-1h: ON→OFF
	// The seed should be the LAST one before from, i.e. t-1h: ON→OFF.
	tMinus2h := from.Add(-2 * time.Hour)
	tMinus1h := from.Add(-time.Hour)
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-pre1", tMinus2h, 1.0) // first reading: ON
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-pre2", tMinus1h, 0.0) // transition ON→OFF

	// In-range transitions:
	//   from+30min: OFF→ON
	//   from+90min: ON→OFF
	tPlus30 := from.Add(30 * time.Minute)
	tPlus90 := from.Add(90 * time.Minute)
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-r1", tPlus30, 1.0)
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-r2", tPlus90, 0.0)

	changes, err := src.IgnitionStateChanges(ctx, subject, from, to)
	require.NoError(t, err)

	// Expected: seed row (tMinus1h) + 2 in-range rows = 3 total, ordered by ts.
	require.Len(t, changes, 3, "expected seed row + 2 in-range transitions")

	// Seed row: the transition at tMinus1h (ON→OFF), placed first in ORDER BY ts.
	assert.Equal(t, tMinus1h, changes[0].TS, "seed row timestamp")
	assert.Equal(t, 0.0, changes[0].NewState, "seed: ignition OFF")
	assert.Equal(t, 1.0, changes[0].PrevState, "seed: previous was ON")

	// In-range row 1: OFF→ON at tPlus30.
	assert.Equal(t, tPlus30, changes[1].TS)
	assert.Equal(t, 1.0, changes[1].NewState)
	assert.Equal(t, 0.0, changes[1].PrevState)

	// In-range row 2: ON→OFF at tPlus90.
	assert.Equal(t, tPlus90, changes[2].TS)
	assert.Equal(t, 0.0, changes[2].NewState)
	assert.Equal(t, 1.0, changes[2].PrevState)
}

// TestLakeSignalSource_IgnitionStateChanges_NoSeed verifies that when there
// is no prior transition within the 30-day lookback, only in-range rows are
// returned (no seed row).
func TestLakeSignalSource_IgnitionStateChanges_NoSeed(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := testSubject2
	from := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	// No pre-from rows; only one in-range transition.
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-r1", from.Add(10*time.Minute), 1.0)
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-r2", from.Add(40*time.Minute), 0.0)

	changes, err := src.IgnitionStateChanges(ctx, subject, from, to)
	require.NoError(t, err)

	// First row in the window is treated as a transition (prev=-1, new=1 → transition),
	// and second is ON→OFF. No pre-from seed.
	require.Len(t, changes, 2, "expected 2 in-range transitions, no seed")
	assert.Equal(t, from.Add(10*time.Minute), changes[0].TS)
	assert.Equal(t, 1.0, changes[0].NewState)
	assert.Equal(t, from.Add(40*time.Minute), changes[1].TS)
	assert.Equal(t, 0.0, changes[1].NewState)
}

// TestLakeSignalSource_IgnitionStateChanges_OldSeedIgnored verifies that a
// transition older than 30 days is NOT returned as a seed row.
func TestLakeSignalSource_IgnitionStateChanges_OldSeedIgnored(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:99"
	from := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	// Seed candidate older than 30 days — must be excluded by lookback cap.
	tooOld := from.AddDate(0, 0, -31)
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-old", tooOld, 1.0)

	// One in-range transition.
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-r1", from.Add(10*time.Minute), 0.0)

	changes, err := src.IgnitionStateChanges(ctx, subject, from, to)
	require.NoError(t, err)

	// The old row is outside the lookback window; only the in-range transition.
	require.Len(t, changes, 1)
	assert.Equal(t, from.Add(10*time.Minute), changes[0].TS)
}
