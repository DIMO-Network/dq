package latestkv

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

func speedRow(ts time.Time, ceid string, v float64) Row {
	return Row{Subject: "did:erc721:137:0xAB:1", Name: "speed", Timestamp: ts, CloudEventID: ceid, ValueNumber: v}
}

func locRow(ts time.Time, ceid string, lat, lon float64) Row {
	return Row{Subject: "did:erc721:137:0xAB:1", Name: "currentLocationLatitude", Timestamp: ts, CloudEventID: ceid, ValueNumber: lat, LocLat: lat, LocLon: lon}
}

func TestFold_NewerTimestampWins(t *testing.T) {
	var e Entry
	assert.True(t, e.Fold(speedRow(t0, "a", 40)))
	assert.True(t, e.Fold(speedRow(t0.Add(time.Minute), "b", 65)))
	assert.Equal(t, 65.0, e.Signals["speed"].Num)

	// An older reading (redelivery, replayed window) changes nothing.
	assert.False(t, e.Fold(speedRow(t0, "a", 40)))
	assert.Equal(t, 65.0, e.Signals["speed"].Num)
	assert.Equal(t, t0.Add(time.Minute), e.Signals["speed"].TS)
}

// The rollup breaks exact-timestamp ties by cloud_event_id ASC
// (foldSignalsRollup's QUALIFY ordering); the KV fold must pick the same
// winner so the phase-2 rollup fallback can't flap between two values.
func TestFold_EqualTimestampSmallerCEIDWins(t *testing.T) {
	var e Entry
	require.True(t, e.Fold(speedRow(t0, "b", 50)))
	assert.True(t, e.Fold(speedRow(t0, "a", 40)), "smaller ceid at the same ts wins")
	assert.Equal(t, 40.0, e.Signals["speed"].Num)
	assert.False(t, e.Fold(speedRow(t0, "c", 60)), "larger ceid at the same ts loses")
	assert.Equal(t, 40.0, e.Signals["speed"].Num)
}

// The location part only ever advances to a nonzero fix, independently of the
// value part — a trailing (0,0) reading updates the value but keeps the last
// real fix (H9), exactly like the rollup's loc_*/loc_ts columns.
func TestFold_LocationKeepsLastNonzeroFix(t *testing.T) {
	var e Entry
	require.True(t, e.Fold(locRow(t0, "a", 40.7, -74.0)))
	sig := e.Signals["currentLocationLatitude"]
	require.NotNil(t, sig.Loc)
	assert.Equal(t, 40.7, sig.Loc.Lat)

	// Newer (0,0) reading: value part advances, fix stays.
	require.True(t, e.Fold(locRow(t0.Add(time.Minute), "b", 0, 0)))
	sig = e.Signals["currentLocationLatitude"]
	assert.Equal(t, 0.0, sig.Num, "value part advanced to the newer reading")
	assert.Equal(t, t0.Add(time.Minute), sig.TS)
	require.NotNil(t, sig.Loc, "fix survives a (0,0) reading")
	assert.Equal(t, 40.7, sig.Loc.Lat)
	assert.Equal(t, t0, sig.Loc.TS, "fix time is the last nonzero fix's, not the value's")

	// Newer real fix: both parts advance.
	require.True(t, e.Fold(locRow(t0.Add(2*time.Minute), "c", 41.0, -73.9)))
	assert.Equal(t, 41.0, e.Signals["currentLocationLatitude"].Loc.Lat)
}

// A bootstrap row (FoldValue with loc TS ≠ value TS, empty ceid) merges with a
// live entry without regressing anything fresher.
func TestFoldValue_BootstrapMergesWithoutRegression(t *testing.T) {
	var e Entry
	require.True(t, e.Fold(speedRow(t0.Add(time.Hour), "live", 80)))
	assert.False(t, e.FoldValue("speed", SignalValue{TS: t0, Num: 40}),
		"older rollup row must not regress a fresher live value")
	assert.Equal(t, 80.0, e.Signals["speed"].Num)

	assert.True(t, e.FoldValue("odometer", SignalValue{TS: t0, Num: 12345}),
		"rollup row for an unseen name lands")
}

func TestLastSeen(t *testing.T) {
	var e Entry
	assert.True(t, e.LastSeen().IsZero())
	e.Fold(speedRow(t0, "a", 40))
	e.Fold(Row{Subject: "s", Name: "odometer", Timestamp: t0.Add(time.Hour), CloudEventID: "b", ValueNumber: 9})
	assert.Equal(t, t0.Add(time.Hour), e.LastSeen(), "lastSeen is the max value-part ts across names")
}

// NATS KV keys must match [-/_=.a-zA-Z0-9]+; subject DIDs contain colons, so
// the key contract is the base64url encoding — pin both validity and
// distinctness.
func TestKeyForSubject(t *testing.T) {
	valid := regexp.MustCompile(`^[-/_=.a-zA-Z0-9]+$`)
	a := KeyForSubject("did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:7")
	b := KeyForSubject("did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:8")
	assert.Regexp(t, valid, a)
	assert.Regexp(t, valid, b)
	assert.NotEqual(t, a, b)
	assert.NotEqual(t, bootstrapMarkerKey, a[:len(bootstrapMarkerKey)], "subject keys are namespaced away from meta keys")
}
