package repositories

import (
	"math"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
)

// validateAggSigArgs must reject NaN/Inf/out-of-range geofence coordinates and radius.
// A GraphQL Float! accepts the JSON strings "NaN"/"Inf", which would otherwise be
// inlined into the ST_GeomFromText WKT / haversine SQL and 500 the query (or silently
// match nothing). Drives the full validator, so it also catches the check being unwired.
func TestValidateAggSigArgs_GeofenceBounds(t *testing.T) {
	now := time.Now()
	good := &model.FilterLocation{Latitude: 37.5, Longitude: -122.3}
	base := func(f *model.SignalLocationFilter) *model.AggregatedSignalArgs {
		a := &model.AggregatedSignalArgs{
			FromTS:       now.Add(-time.Minute),
			ToTS:         now,
			Interval:     int64(time.Minute / time.Microsecond),
			LocationArgs: []model.LocationSignalArgs{{Filter: f}},
		}
		a.Subject = "did:erc721:1:0x0000000000000000000000000000000000000001:1"
		return a
	}

	bad := map[string]*model.SignalLocationFilter{
		"polygon NaN lat":    {InPolygon: []*model.FilterLocation{{Latitude: math.NaN()}, good, good}},
		"polygon +Inf lon":   {InPolygon: []*model.FilterLocation{{Longitude: math.Inf(1)}, good, good}},
		"polygon out-range":  {InPolygon: []*model.FilterLocation{{Latitude: 9999}, good, good}},
		"polygon nil vertex": {InPolygon: []*model.FilterLocation{nil, good, good}},
		"circle NaN radius":  {InCircle: &model.InCircleFilter{Center: good, Radius: math.NaN()}},
		"circle Inf radius":  {InCircle: &model.InCircleFilter{Center: good, Radius: math.Inf(1)}},
		"circle neg radius":  {InCircle: &model.InCircleFilter{Center: good, Radius: -5}},
	}
	for name, f := range bad {
		if err := validateAggSigArgs(base(f)); err == nil {
			t.Errorf("%s: expected a validation error, got nil", name)
		}
	}

	// A valid polygon + circle must still pass the geofence check.
	ok := &model.SignalLocationFilter{
		InPolygon: []*model.FilterLocation{good, good, good},
		InCircle:  &model.InCircleFilter{Center: good, Radius: 5},
	}
	if err := validateAggSigArgs(base(ok)); err != nil {
		t.Errorf("valid geofence rejected: %v", err)
	}
}
