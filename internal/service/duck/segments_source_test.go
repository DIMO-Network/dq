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

// insertSignal inserts one signal row into lake.signals for testing.
func insertSignal(t *testing.T, svc *Service, subject, name, ceID string, ts time.Time, valNum float64) {
	t.Helper()
	_, err := svc.db.ExecContext(context.Background(),
		`INSERT INTO lake.signals (subject, subject_bucket, name, timestamp, source, producer, cloud_event_id, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading)
		 VALUES (?, ?, ?, ?, 'src', 'prod', ?, ?, '', 0.0, 0.0, 0.0, 0.0)`,
		subject, HashBucket(subject), name, ts.UTC(), ceID, valNum)
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

// TestLakeSignalSource_WindowedSignalCounts_EpochAligned is the regression guard
// for the parity bug: window buckets must be epoch-aligned (like CH's
// toStartOfInterval), NOT aligned to the `from` argument.
//
// Setup: from = HH:MM:30 (30s past a minute boundary), win = 60s.
//   - Signal A at HH:MM:00  → must land in [HH:MM:00, HH:MM+1:00) — epoch-aligned
//   - Signal B at HH:MM:59  → same bucket as A
//   - Signal C at HH:MM:00  → same bucket as A (same name, dedup collapses with A)
//   - Signal D at HH:MM+1:00 → lands in [HH:MM+1:00, HH:MM+2:00) — next bucket
//   - Signal E at HH:MM+1:30 → same bucket as D
//
// With from-alignment (the OLD buggy behaviour) from=HH:MM:30 would give buckets
// [HH:MM:30, HH:MM+1:30) and [HH:MM+1:30, ...), so A & B & D & E would all land
// in the same first bucket and C (same ts as A, different ceID) would dedup away.
// With epoch alignment we get two distinct buckets as described above.
func TestLakeSignalSource_WindowedSignalCounts_EpochAligned(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := testSubject2
	win := 60

	// Use a base that is on a clean minute boundary for easy reasoning.
	// The minute is 2026-04-01 12:34:00 UTC.
	minuteBase := time.Date(2026, 4, 1, 12, 34, 0, 0, time.UTC)

	// from is 30 seconds PAST the minute boundary — deliberately non-aligned.
	from := minuteBase.Add(30 * time.Second)
	// to covers two full minutes from from.
	to := from.Add(2 * time.Minute)

	// Bucket 1 (epoch-aligned): [12:34:00, 12:35:00)
	//   Signal A at 12:34:00 — exactly on the epoch-aligned boundary, inside [from, to) is false!
	//   Wait: from = 12:34:30, so 12:34:00 < from. We need signals >= from to be counted.
	//   Use signals at 12:34:30 and 12:34:59 — both inside [from, to) AND in [12:34:00,12:35:00).
	tA := minuteBase.Add(30 * time.Second) // 12:34:30 — == from, included
	tB := minuteBase.Add(59 * time.Second) // 12:34:59 — still in [12:34:00,12:35:00)

	// Bucket 2 (epoch-aligned): [12:35:00, 12:36:00)
	tD := minuteBase.Add(60 * time.Second) // 12:35:00
	tE := minuteBase.Add(90 * time.Second) // 12:35:30

	// Insert 2 distinct signals into bucket 1 (threshold: sig=2, dist=2).
	insertSignal(t, svc, subject, "speed", "ep-A", tA, 50)
	insertSignal(t, svc, subject, "rpm", "ep-B", tB, 1000)

	// Insert 2 distinct signals into bucket 2.
	insertSignal(t, svc, subject, "speed", "ep-D", tD, 55)
	insertSignal(t, svc, subject, "rpm", "ep-E", tE, 1100)

	windows, err := src.WindowedSignalCounts(ctx, subject, from, to, win, 2, 2)
	require.NoError(t, err)

	require.Len(t, windows, 2, "expected exactly 2 epoch-aligned buckets")

	// Bucket 1: epoch-aligned start = 12:34:00, end = 12:35:00.
	// (NOT 12:34:30 which would be from-aligned — that was the bug.)
	w0 := windows[0]
	assert.Equal(t, minuteBase, w0.WindowStart, "bucket 1 start must be epoch-aligned (12:34:00), not from-aligned (12:34:30)")
	assert.Equal(t, minuteBase.Add(time.Minute), w0.WindowEnd, "bucket 1 end must be 12:35:00")
	assert.EqualValues(t, 2, w0.SignalCount)
	assert.EqualValues(t, 2, w0.DistinctSignalCount)

	// Bucket 2: epoch-aligned start = 12:35:00, end = 12:36:00.
	w1 := windows[1]
	assert.Equal(t, minuteBase.Add(time.Minute), w1.WindowStart, "bucket 2 start must be 12:35:00")
	assert.Equal(t, minuteBase.Add(2*time.Minute), w1.WindowEnd, "bucket 2 end must be 12:36:00")
	assert.EqualValues(t, 2, w1.SignalCount)
	assert.EqualValues(t, 2, w1.DistinctSignalCount)
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

	// No prior reading at all → the first window row seeds prev=0 (off), so 0→1 is a
	// genuine transition, and the second is ON→OFF. No pre-from seed.
	require.Len(t, changes, 2, "expected 2 in-range transitions, no seed")
	assert.Equal(t, from.Add(10*time.Minute), changes[0].TS)
	assert.Equal(t, 1.0, changes[0].NewState)
	assert.Equal(t, from.Add(40*time.Minute), changes[1].TS)
	assert.Equal(t, 0.0, changes[1].NewState)
}

// TestLakeSignalSource_IgnitionStateChanges_ContinuouslyOn verifies a vehicle ON
// since before the lookback (no in-window state change) still surfaces its
// in-progress trip: the true prior state seeds ON so no lag-transition fires, and
// the source emits a synthetic ON at the earliest reading so the detector reports
// the ongoing trip (an ongoing trip must show as ongoing).
func TestLakeSignalSource_IgnitionStateChanges_ContinuouslyOn(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:99"
	from := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	// ON before the lookback and still ON in range, no OFF — a trip ongoing since
	// before the queryable window.
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-old", from.AddDate(0, 0, -31), 1.0)
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-r1", from.Add(10*time.Minute), 1.0)

	changes, err := src.IgnitionStateChanges(ctx, subject, from, to)
	require.NoError(t, err)

	require.Len(t, changes, 1, "synthetic ON surfaces the in-progress trip")
	assert.Equal(t, from.Add(10*time.Minute), changes[0].TS, "synthetic ON at the earliest in-window reading")
	assert.Equal(t, 1.0, changes[0].NewState, "ON")
}

// TestLakeSignalSource_IgnitionStateChanges_PriorOnNoFabrication locks in the
// parity fix: a vehicle already ON entering the window that turns OFF in range
// must NOT fabricate a phantom trip. With prev_state seeded from the true prior ON,
// the in-window ON reading is not a transition; only the OFF is emitted, and the
// detector (OFF with no open segment) produces no segment — matching ClickHouse,
// not the old hardcoded-0 seed that invented one.
func TestLakeSignalSource_IgnitionStateChanges_PriorOnNoFabrication(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := testSubject1
	from := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	to := from.Add(2 * time.Hour)

	insertSignal(t, svc, subject, "isIgnitionOn", "ce-old", from.AddDate(0, 0, -31), 1.0)  // before lookback: ON (true seed)
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-lb", from.Add(-5*24*time.Hour), 1.0) // in lookback: still ON
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-off", from.Add(30*time.Minute), 0.0) // in range: OFF

	changes, err := src.IgnitionStateChanges(ctx, subject, from, to)
	require.NoError(t, err)

	require.Len(t, changes, 1, "only the OFF; the entered-ON reading is not a fabricated transition")
	assert.Equal(t, from.Add(30*time.Minute), changes[0].TS)
	assert.Equal(t, 0.0, changes[0].NewState, "OFF")
	assert.Equal(t, 1.0, changes[0].PrevState, "prev was ON (true prior state)")
}

// TestLakeSignalSource_IgnitionStateChanges_NullValueDoesNotPoison verifies
// that a NULL value_number row (a missing ignition reading) neither emits a
// spurious transition itself nor poisons the following row's LAG. Without the
// `value_number IS NOT NULL` filter, the NULL row makes the next real row's
// prev_state coalesce to -1, wrongly emitting an unchanged ON reading as a
// transition (a spurious trip boundary).
func TestLakeSignalSource_IgnitionStateChanges_NullValueDoesNotPoison(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)
	src := NewLakeSignalSource(svc)

	subject := testSubject1
	from := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	t1 := from.Add(10 * time.Minute) // ON (first reading → genuine transition)
	t2 := from.Add(20 * time.Minute) // NULL reading
	t3 := from.Add(30 * time.Minute) // still ON → NOT a transition

	insertSignal(t, svc, subject, "isIgnitionOn", "ce-1", t1, 1.0)
	// NULL ignition reading at t2 (missing value).
	_, err := svc.db.ExecContext(ctx,
		`INSERT INTO lake.signals (subject, subject_bucket, name, timestamp, source, producer, cloud_event_id, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading)
		 VALUES (?, ?, 'isIgnitionOn', ?, 'src', 'prod', 'ce-null', NULL, '', 0.0, 0.0, 0.0, 0.0)`,
		subject, HashBucket(subject), t2.UTC())
	require.NoError(t, err)
	insertSignal(t, svc, subject, "isIgnitionOn", "ce-3", t3, 1.0)

	changes, err := src.IgnitionStateChanges(ctx, subject, from, to)
	require.NoError(t, err)

	require.Len(t, changes, 1, "NULL value_number must not emit or poison a spurious transition at t3")
	assert.Equal(t, t1, changes[0].TS)
	assert.Equal(t, 1.0, changes[0].NewState)
}
