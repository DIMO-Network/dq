package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"time"

	"github.com/DIMO-Network/cloudevent"
)

// Scenario is a fleet's worth of made-up driving, read from -scenario: for
// each vehicle where it parks, the trips it makes and the events it reports.
// Between trips a car reports a heartbeat every ParkedEvery with the ignition
// off; on a trip it reports every DrivingEvery with enough signals (speed,
// ignition, rpm, location, odometer, fuel) that dq's trip detection
// (FREQUENCY_ANALYSIS needs ten rows and two signal names a minute,
// IGNITION_DETECTION needs isIgnitionOn transitions) and segment summaries
// (start and end location, odometer and fuel first and last) all have
// something to read.
type Scenario struct {
	From         time.Time     `json:"from"`
	To           time.Time     `json:"to"`
	ParkedEvery  Duration      `json:"parkedEvery"`
	DrivingEvery Duration      `json:"drivingEvery"`
	Seed         uint64        `json:"seed"`
	Vehicles     []ScenarioCar `json:"vehicles"`
}

// ScenarioCar is one vehicle's month.
type ScenarioCar struct {
	Subject  string  `json:"subject"`
	Home     Point   `json:"home"`
	Odometer float64 `json:"odometerKm"`
	Fuel     float64 `json:"fuelPercent"`
	Trips    []Trip  `json:"trips"`
	// Events are reported as dimo.events, e.g. behavior.harshBraking.
	Events []ScenarioEvent `json:"events"`
}

// Trip is one drive from wherever the car is to To, at about CruiseKmh.
type Trip struct {
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	To        Point     `json:"to"`
	CruiseKmh float64   `json:"cruiseKmh"`
	// Refuel tops the tank up when the trip ends.
	Refuel bool `json:"refuel"`
}

// ScenarioEvent is one event a vehicle reports.
type ScenarioEvent struct {
	Name      string    `json:"name"`
	Timestamp time.Time `json:"timestamp"`
}

// Point is a latitude and longitude.
type Point struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// Duration is a time.Duration written as "10s" in JSON.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}

func readScenario(path string) (*Scenario, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Scenario
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if s.ParkedEvery <= 0 {
		s.ParkedEvery = Duration(15 * time.Minute)
	}
	if s.DrivingEvery <= 0 {
		s.DrivingEvery = Duration(10 * time.Second)
	}
	if !s.To.After(s.From) {
		return nil, fmt.Errorf("%s: to must be after from", path)
	}
	return &s, nil
}

// insertScenario writes every vehicle's status and event rows into
// lake.raw_events in one transaction and returns how many it wrote.
func insertScenario(ctx context.Context, tx *sql.Tx, s *Scenario, source string) (int, error) {
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data)
		 VALUES (?, ?, ?, ?, ?, ?, '', ?, '{}', ?)`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	rng := rand.New(rand.NewPCG(s.Seed, 7))
	n := 0
	write := func(subject, typ string, ts time.Time, payload any) error {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		id := fmt.Sprintf("seed-%s-%d", ts.Format("20060102T150405.000"), n)
		if _, err := stmt.ExecContext(ctx, subject, ts, typ, id, source, subject, "default/v1.0", string(data)); err != nil {
			return fmt.Errorf("insert %s: %w", id, err)
		}
		n++
		return nil
	}
	for _, car := range s.Vehicles {
		if err := driveCar(car, s, rng, func(ts time.Time, signals []map[string]any) error {
			return write(car.Subject, cloudevent.TypeStatus, ts, map[string]any{"signals": signals})
		}); err != nil {
			return n, err
		}
		for _, ev := range car.Events {
			if err := write(car.Subject, cloudevent.TypeEvents, ev.Timestamp.UTC(), map[string]any{"events": []map[string]any{{
				"name": ev.Name, "timestamp": ev.Timestamp.UTC().Format(time.RFC3339Nano), "tags": []string{"seed"},
			}}}); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// driveCar walks one car through the scenario's span, calling emit for each
// status report.
func driveCar(car ScenarioCar, s *Scenario, rng *rand.Rand, emit func(time.Time, []map[string]any) error) error {
	at, odo, fuel := car.Home, car.Odometer, car.Fuel
	if fuel == 0 {
		fuel = 70
	}
	sig := func(name string, ts time.Time, v any) map[string]any {
		return map[string]any{"name": name, "timestamp": ts.Format(time.RFC3339Nano), "value": v}
	}
	parked := func(ts time.Time) error {
		return emit(ts, []map[string]any{
			sig("isIgnitionOn", ts, 0), sig("speed", ts, 0),
			sig("currentLocationCoordinates", ts, map[string]any{"latitude": at.Lat, "longitude": at.Lon, "hdop": 1.2}),
			sig("powertrainTransmissionTravelledDistance", ts, round1(odo)),
			sig("powertrainFuelSystemRelativeLevel", ts, round1(fuel)),
			sig("lowVoltageBatteryCurrentVoltage", ts, round1(12.4+rng.Float64()*0.3)),
		})
	}
	ts := s.From.UTC()
	for _, trip := range append(car.Trips, Trip{Start: s.To.UTC().Add(time.Second)}) {
		start := trip.Start.UTC()
		for ; ts.Before(start) && !ts.After(s.To); ts = ts.Add(time.Duration(s.ParkedEvery)) {
			if err := parked(ts); err != nil {
				return err
			}
		}
		if start.After(s.To) {
			break
		}
		from, end := at, trip.End.UTC()
		total := end.Sub(start).Seconds()
		cruise := trip.CruiseKmh
		if cruise == 0 {
			cruise = 55
		}
		every := time.Duration(s.DrivingEvery)
		for t := start; !t.After(end); t = t.Add(every) {
			f := t.Sub(start).Seconds() / total
			// Pull away, cruise with some traffic, stop at the end.
			speed := cruise * math.Min(1, math.Min(f, 1-f)*8) * (0.75 + 0.35*rng.Float64())
			if rng.Float64() < 0.06 {
				speed *= 0.2 // a light
			}
			odo += speed * every.Hours()
			fuel = math.Max(3, fuel-speed*every.Hours()*0.09)
			at = Point{
				Lat: from.Lat + (trip.To.Lat-from.Lat)*f + 0.002*math.Sin(f*9),
				Lon: from.Lon + (trip.To.Lon-from.Lon)*f + 0.002*math.Cos(f*7),
			}
			rpm := 750 + speed*28 + rng.Float64()*150
			if err := emit(t, []map[string]any{
				sig("isIgnitionOn", t, 1), sig("speed", t, round1(speed)),
				sig("powertrainCombustionEngineSpeed", t, math.Round(rpm)),
				sig("currentLocationCoordinates", t, map[string]any{"latitude": at.Lat, "longitude": at.Lon, "hdop": 0.9}),
				sig("powertrainTransmissionTravelledDistance", t, round1(odo)),
				sig("powertrainFuelSystemRelativeLevel", t, round1(fuel)),
			}); err != nil {
				return err
			}
		}
		at = trip.To
		if trip.Refuel {
			fuel = 96
		}
		ts = end.Add(time.Minute)
		if err := parked(ts); err != nil {
			return err
		}
		ts = ts.Add(time.Duration(s.ParkedEvery))
	}
	return nil
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
