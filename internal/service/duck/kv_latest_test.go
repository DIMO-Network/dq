package duck

import (
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/latestkv"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var kvT0 = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

func kvEntry() *latestkv.Entry {
	return &latestkv.Entry{V: latestkv.EntryVersion, Signals: map[string]latestkv.SignalValue{
		"speed":    {TS: kvT0.Add(time.Hour), Num: 65},
		"odometer": {TS: kvT0, Num: 12345},
		"currentLocationCoordinates": {TS: kvT0.Add(2 * time.Hour), Num: 0,
			Loc: &latestkv.LocValue{TS: kvT0.Add(time.Hour), Lat: 40.7, Lon: -74.0, HDOP: 0.8}},
		"neverFixed": {TS: kvT0, Num: 1}, // no nonzero fix ever
	}}
}

func args(named, location []string, lastSeen bool) *model.LatestSignalsArgs {
	a := &model.LatestSignalsArgs{SignalNames: map[string]struct{}{}, LocationSignalNames: map[string]struct{}{}, IncludeLastSeen: lastSeen}
	for _, n := range named {
		a.SignalNames[n] = struct{}{}
	}
	for _, n := range location {
		a.LocationSignalNames[n] = struct{}{}
	}
	return a
}

// Pins the rollup-SQL parity of the entry conversion (see signalsFromKVEntry's
// case list): value rows verbatim, location rows stamped with the FIX time
// (H9), fixless location rows at epoch, unseen names absent, lastSeen = max
// value ts, output ordered by name.
func TestSignalsFromKVEntry_RollupParity(t *testing.T) {
	rows := signalsFromKVEntry(kvEntry(), args(
		[]string{"speed", "unseenName"},
		[]string{"currentLocationCoordinates", "neverFixed"},
		true,
	))

	byName := map[string]*vss.Signal{}
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		byName[r.Data.Name] = r
		names = append(names, r.Data.Name)
	}
	assert.IsIncreasing(t, names, "rows are ordered by name (the SQL ORDER BY)")
	assert.NotContains(t, byName, "unseenName", "no rollup row, no KV row")

	require.Contains(t, byName, "speed")
	assert.Equal(t, 65.0, byName["speed"].Data.ValueNumber)
	assert.Equal(t, kvT0.Add(time.Hour), byName["speed"].Data.Timestamp)
	assert.Zero(t, byName["speed"].Data.ValueLocation, "named rows carry no location")

	loc := byName["currentLocationCoordinates"]
	require.NotNil(t, loc)
	assert.Equal(t, kvT0.Add(time.Hour), loc.Data.Timestamp, "location row is stamped with the fix time, not the value ts")
	assert.Equal(t, vss.Location{Latitude: 40.7, Longitude: -74.0, HDOP: 0.8}, loc.Data.ValueLocation)

	require.Contains(t, byName, "neverFixed")
	assert.Equal(t, epochTime, byName["neverFixed"].Data.Timestamp, "fixless location row sits at epoch (coalesce(loc_ts, epoch))")
	assert.Zero(t, byName["neverFixed"].Data.ValueLocation)

	require.Contains(t, byName, model.LastSeenField)
	assert.Equal(t, kvT0.Add(2*time.Hour), byName[model.LastSeenField].Data.Timestamp, "lastSeen = max value ts across ALL names")
}

// A name requested as both a named and a location signal yields two rows,
// exactly like the SQL UNION ALL of the two statements.
func TestSignalsFromKVEntry_DualRequestedName(t *testing.T) {
	rows := signalsFromKVEntry(kvEntry(), args(
		[]string{"currentLocationCoordinates"}, []string{"currentLocationCoordinates"}, false))
	require.Len(t, rows, 2)
	// One value row (value ts, no location), one location row (fix ts, location).
	tss := []time.Time{rows[0].Data.Timestamp, rows[1].Data.Timestamp}
	assert.ElementsMatch(t, []time.Time{kvT0.Add(2 * time.Hour), kvT0.Add(time.Hour)}, tss)
}

func sig(name string, ts time.Time, num float64) *vss.Signal {
	s := &vss.Signal{}
	s.Data.Name, s.Data.Timestamp, s.Data.ValueNumber = name, ts, num
	return s
}

func TestClassifyShadowDiff(t *testing.T) {
	base := []*vss.Signal{sig("speed", kvT0, 65), sig("odometer", kvT0, 12345)}

	assert.Equal(t, "match", classifyShadowDiff(base, []*vss.Signal{sig("odometer", kvT0, 12345), sig("speed", kvT0, 65)}))

	// Sub-µs tails are representation (lake stores µs; live folds carry ns).
	assert.Equal(t, "match", classifyShadowDiff(base, []*vss.Signal{sig("speed", kvT0.Add(300*time.Nanosecond), 65), sig("odometer", kvT0, 12345)}))

	// Every differing name newer in the KV = the pre-commit-fold freshness race.
	assert.Equal(t, "kv_newer", classifyShadowDiff(base, []*vss.Signal{sig("speed", kvT0.Add(time.Minute), 70), sig("odometer", kvT0, 12345)}))

	// Same ts, different value = genuine divergence.
	assert.Equal(t, "mismatch", classifyShadowDiff(base, []*vss.Signal{sig("speed", kvT0, 99), sig("odometer", kvT0, 12345)}))

	// A rollup row absent from the KV is never a freshness race.
	assert.Equal(t, "mismatch", classifyShadowDiff(base, []*vss.Signal{sig("speed", kvT0, 65)}))

	// A KV-only row means the reading landed after the rollup query — newer.
	assert.Equal(t, "kv_newer", classifyShadowDiff(base, []*vss.Signal{sig("speed", kvT0, 65), sig("odometer", kvT0, 12345), sig("soc", kvT0.Add(time.Minute), 80)}))
}

func TestParseKVReadMode(t *testing.T) {
	for s, want := range map[string]KVReadMode{"": KVReadOff, "off": KVReadOff, "shadow": KVReadShadow, "serve": KVReadServe} {
		got, ok := ParseKVReadMode(s)
		assert.True(t, ok, s)
		assert.Equal(t, want, got, s)
	}
	_, ok := ParseKVReadMode("on")
	assert.False(t, ok, "unknown modes are rejected, not defaulted")
}
