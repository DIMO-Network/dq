package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sigOtherLoc = "otherLocation"

var unixEpoch = time.Unix(0, 0).UTC()

func d3(t *testing.T, hms string) time.Time { return mkts(t, "2026-06-03T"+hms+"Z") }

// setupLatestFixtures writes a latest bucket for testSubject1 with two
// sources and the materializer's virtual lastSeen rows. testSubject2 has no
// bucket file at all.
func setupLatestFixtures(t *testing.T) *Queries {
	t.Helper()
	root, svc, q := newQueriesHarness(t)

	writeLatestFixture(t, svc, root, testSubject1, []latestFixture{
		{name: sigSpeed, subject: testSubject1, source: srcOne, ts: d3(t, "10:00:00"), num: 88, nzTS: unixEpoch},
		{name: sigPower, subject: testSubject1, source: srcOne, ts: d3(t, "09:00:00"), str: "EV", nzTS: unixEpoch},
		// Latest raw location is (0, 0); the nonzero columns carry the
		// latest real fix from 08:00.
		{
			name: sigLoc, subject: testSubject1, source: srcOne, ts: d3(t, "10:00:00"),
			lat: 0, lon: 0, hdop: 0, heading: 0,
			latNZ: 7, lonNZ: 8, hdopNZ: 0.9, headingNZ: 45, nzTS: d3(t, "08:00:00"),
		},
		// A location signal that never had a non-(0,0) fix.
		{name: sigOtherLoc, subject: testSubject1, source: srcOne, ts: d3(t, "09:30:00"), nzTS: unixEpoch},
		// Virtual per-(subject, source) lastSeen rows.
		{name: model.LastSeenField, subject: testSubject1, source: srcOne, ts: d3(t, "10:30:00"), nzTS: unixEpoch},
		{name: sigSpeed, subject: testSubject1, source: srcTwo, ts: d3(t, "11:00:00"), num: 99, nzTS: unixEpoch},
		{name: model.LastSeenField, subject: testSubject1, source: srcTwo, ts: d3(t, "11:30:00"), nzTS: unixEpoch},
	})
	return q
}

func signalsByName(signals []*vss.Signal) map[string]*vss.Signal {
	m := make(map[string]*vss.Signal, len(signals))
	for _, s := range signals {
		m[s.Data.Name] = s
	}
	return m
}

func TestGetLatestSignals(t *testing.T) {
	q := setupLatestFixtures(t)

	latestArgs := &model.LatestSignalsArgs{
		SignalArgs:          model.SignalArgs{Subject: testSubject1},
		SignalNames:         map[string]struct{}{sigSpeed: {}, sigPower: {}},
		LocationSignalNames: map[string]struct{}{sigLoc: {}},
		IncludeLastSeen:     true,
	}
	signals, err := q.GetLatestSignals(context.Background(), testSubject1, latestArgs)
	require.NoError(t, err)
	require.Len(t, signals, 4)
	m := signalsByName(signals)

	speed := m[sigSpeed]
	require.NotNil(t, speed)
	assert.Equal(t, d3(t, "11:00:00"), speed.Data.Timestamp, "latest speed must win across sources")
	assert.InDelta(t, 99, speed.Data.ValueNumber, 1e-9)

	power := m[sigPower]
	require.NotNil(t, power)
	assert.Equal(t, "EV", power.Data.ValueString)
	assert.Equal(t, d3(t, "09:00:00"), power.Data.Timestamp)

	loc := m[sigLoc]
	require.NotNil(t, loc)
	assert.Equal(t, d3(t, "08:00:00"), loc.Data.Timestamp, "location timestamp must come from the last non-(0,0) fix")
	assert.InDelta(t, 7, loc.Data.ValueLocation.Latitude, 1e-9)
	assert.InDelta(t, 8, loc.Data.ValueLocation.Longitude, 1e-9)
	assert.InDelta(t, 0.9, loc.Data.ValueLocation.HDOP, 1e-9)
	assert.InDelta(t, 45, loc.Data.ValueLocation.Heading, 1e-9)

	lastSeen := m[model.LastSeenField]
	require.NotNil(t, lastSeen)
	assert.Equal(t, d3(t, "11:30:00"), lastSeen.Data.Timestamp)
}

func TestGetLatestSignalsSourceFilter(t *testing.T) {
	q := setupLatestFixtures(t)
	src := srcOne
	latestArgs := &model.LatestSignalsArgs{
		SignalArgs:      model.SignalArgs{Subject: testSubject1, Filter: &model.SignalFilter{Source: &src}},
		SignalNames:     map[string]struct{}{sigSpeed: {}},
		IncludeLastSeen: true,
	}
	signals, err := q.GetLatestSignals(context.Background(), testSubject1, latestArgs)
	require.NoError(t, err)
	require.Len(t, signals, 2)
	m := signalsByName(signals)
	assert.InDelta(t, 88, m[sigSpeed].Data.ValueNumber, 1e-9)
	assert.Equal(t, d3(t, "10:00:00"), m[sigSpeed].Data.Timestamp)
	assert.Equal(t, d3(t, "10:30:00"), m[model.LastSeenField].Data.Timestamp)
}

func TestGetLatestSignalsAllZeroLocation(t *testing.T) {
	q := setupLatestFixtures(t)
	latestArgs := &model.LatestSignalsArgs{
		SignalArgs:          model.SignalArgs{Subject: testSubject1},
		LocationSignalNames: map[string]struct{}{sigOtherLoc: {}},
	}
	signals, err := q.GetLatestSignals(context.Background(), testSubject1, latestArgs)
	require.NoError(t, err)
	require.Len(t, signals, 1)
	// Default for an empty latest-location aggregate: the row exists but with
	// epoch timestamp and zero location; the repository treats it as no data.
	assert.Equal(t, unixEpoch, signals[0].Data.Timestamp)
	assert.Zero(t, signals[0].Data.ValueLocation.Latitude)
	assert.Zero(t, signals[0].Data.ValueLocation.Longitude)
}

func TestGetLatestSignalsNoWork(t *testing.T) {
	q := setupLatestFixtures(t)
	signals, err := q.GetLatestSignals(context.Background(), testSubject1, &model.LatestSignalsArgs{
		SignalArgs: model.SignalArgs{Subject: testSubject1},
	})
	require.NoError(t, err)
	assert.Nil(t, signals)
}

func TestGetLatestSignalsMissingBucket(t *testing.T) {
	q := setupLatestFixtures(t)
	latestArgs := &model.LatestSignalsArgs{
		SignalArgs:      model.SignalArgs{Subject: testSubject2},
		SignalNames:     map[string]struct{}{sigSpeed: {}},
		IncludeLastSeen: true,
	}
	signals, err := q.GetLatestSignals(context.Background(), testSubject2, latestArgs)
	require.NoError(t, err, "absent latest bucket must not error")
	assert.Empty(t, signals)
}

func TestGetAllLatestSignals(t *testing.T) {
	q := setupLatestFixtures(t)
	signals, err := q.GetAllLatestSignals(context.Background(), testSubject1, nil)
	require.NoError(t, err)
	require.Len(t, signals, 5, "4 real signal names + lastSeen; virtual rows must not leak as a regular name")
	m := signalsByName(signals)

	assert.InDelta(t, 99, m[sigSpeed].Data.ValueNumber, 1e-9)
	assert.Equal(t, "EV", m[sigPower].Data.ValueString)

	// ch.getAllLatestQuery semantics: plain max(timestamp), but the location
	// value from the nonzero (argMaxIf) columns.
	loc := m[sigLoc]
	require.NotNil(t, loc)
	assert.Equal(t, d3(t, "10:00:00"), loc.Data.Timestamp)
	assert.InDelta(t, 7, loc.Data.ValueLocation.Latitude, 1e-9)
	assert.InDelta(t, 8, loc.Data.ValueLocation.Longitude, 1e-9)

	other := m[sigOtherLoc]
	require.NotNil(t, other)
	assert.Zero(t, other.Data.ValueLocation.Latitude)
	assert.Zero(t, other.Data.ValueLocation.Longitude)

	assert.Equal(t, d3(t, "11:30:00"), m[model.LastSeenField].Data.Timestamp)

	t.Run("missing bucket", func(t *testing.T) {
		signals, err := q.GetAllLatestSignals(context.Background(), testSubject2, nil)
		require.NoError(t, err)
		assert.Empty(t, signals)
	})
}

func setupSummaryFixtures(t *testing.T) *Queries {
	t.Helper()
	root, svc, q := newQueriesHarness(t)
	writeSummaryFixture(t, svc, root, testSubject1, []summaryFixture{
		{subject: testSubject1, source: srcOne, name: sigSpeed, count: 4, first: mkts(t, "2026-06-01T00:00:10Z"), last: mkts(t, "2026-06-02T12:00:00Z")},
		{subject: testSubject1, source: srcTwo, name: sigSpeed, count: 2, first: mkts(t, "2026-06-01T00:00:20Z"), last: mkts(t, "2026-06-03T11:00:00Z")},
		{subject: testSubject1, source: srcOne, name: sigPower, count: 3, first: mkts(t, "2026-06-01T00:00:05Z"), last: mkts(t, "2026-06-01T00:01:05Z")},
	})
	return q
}

func TestGetAvailableSignals(t *testing.T) {
	q := setupSummaryFixtures(t)

	names, err := q.GetAvailableSignals(context.Background(), testSubject1, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{sigPower, sigSpeed}, names, "names must be distinct and sorted")

	t.Run("source filter", func(t *testing.T) {
		src := srcTwo
		names, err := q.GetAvailableSignals(context.Background(), testSubject1, &model.SignalFilter{Source: &src})
		require.NoError(t, err)
		assert.Equal(t, []string{sigSpeed}, names)
	})

	t.Run("missing summary bucket", func(t *testing.T) {
		names, err := q.GetAvailableSignals(context.Background(), testSubject2, nil)
		require.NoError(t, err)
		assert.Nil(t, names)
	})
}

func TestGetSignalSummaries(t *testing.T) {
	q := setupSummaryFixtures(t)

	summaries, err := q.GetSignalSummaries(context.Background(), testSubject1, nil)
	require.NoError(t, err)
	require.Len(t, summaries, 2)

	assert.Equal(t, sigPower, summaries[0].Name)
	assert.EqualValues(t, 3, summaries[0].NumberOfSignals)

	speed := summaries[1]
	assert.Equal(t, sigSpeed, speed.Name)
	assert.EqualValues(t, 6, speed.NumberOfSignals, "counts must be summed across sources")
	assert.Equal(t, mkts(t, "2026-06-01T00:00:10Z"), speed.FirstSeen)
	assert.Equal(t, mkts(t, "2026-06-03T11:00:00Z"), speed.LastSeen)

	t.Run("missing summary bucket", func(t *testing.T) {
		summaries, err := q.GetSignalSummaries(context.Background(), testSubject2, nil)
		require.NoError(t, err)
		assert.Empty(t, summaries)
	})
}
