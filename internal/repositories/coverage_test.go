package repositories

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/coverage"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The token's windows bound what a read may return even where no range was
// asked for, or where the read reaches past the one that was (spec §11.4).

func signal(name string, ts time.Time, v float64) *vss.Signal {
	s := &vss.Signal{}
	s.Data.Name, s.Data.Timestamp, s.Data.ValueNumber = name, ts, v
	return s
}

// latestFake answers the latest-value reads with fixed signals.
type latestFake struct {
	fakePrimary
	latest []*vss.Signal
}

func (f *latestFake) GetLatestSignals(context.Context, string, *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	return f.latest, nil
}

func (f *latestFake) GetAllLatestSignals(context.Context, string, *model.SignalFilter) ([]*vss.Signal, error) {
	return f.latest, nil
}

// A buyer's consent runs from the sale; the car has been parked since before
// it, so its latest readings are the seller's, and the buyer sees none of them.
func TestLatestValuesStayInsideTheWindows(t *testing.T) {
	sale := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	before, after := sale.Add(-time.Hour), sale.Add(time.Hour)
	fake := &latestFake{latest: []*vss.Signal{
		signal("speed", before, 50),
		signal(model.LastSeenField, before, 0),
		signal("powertrainTransmissionTravelledDistance", after, 1200),
	}}
	r, err := NewRepository(fake)
	require.NoError(t, err)
	ctx := coverage.With(context.Background(), tokenclaims.Windows{{Start: &sale}})

	coll, err := r.GetSignalLatest(ctx, &model.LatestSignalsArgs{Subject: "did:dimo:car"})
	require.NoError(t, err)
	assert.Nil(t, coll.Speed, "a reading from before the window")
	assert.Nil(t, coll.LastSeen, "lastSeen from before the window")
	require.NotNil(t, coll.PowertrainTransmissionTravelledDistance)
	assert.Equal(t, after, coll.PowertrainTransmissionTravelledDistance.Timestamp)

	snap, err := r.GetSignalSnapshot(ctx, "did:dimo:car", nil)
	require.NoError(t, err)
	assert.Nil(t, snap.LastSeen)
	for _, s := range snap.Signals {
		assert.False(t, s.Timestamp.Before(sale), "%s at %s", s.Name, s.Timestamp)
	}

	// With no windows in the context nothing is filtered.
	coll, err = r.GetSignalLatest(context.Background(), &model.LatestSignalsArgs{Subject: "did:dimo:car"})
	require.NoError(t, err)
	assert.NotNil(t, coll.Speed)
}

// probeRecorder records the timestamps location gap-fill asks about.
type probeRecorder struct {
	locationCountingFake
	mu     sync.Mutex
	probes []time.Time
}

func (f *probeRecorder) LocationsAt(ctx context.Context, subject string, tss []time.Time) ([]*model.Location, error) {
	f.mu.Lock()
	f.probes = append(f.probes, tss...)
	f.mu.Unlock()
	return f.locationCountingFake.LocationsAt(ctx, subject, tss)
}

// A renter's window opens at pickup. Trip detection finds the trip that was
// already under way; it is reported from pickup, with its duration recounted,
// and nothing about it is looked up from before.
func TestSegmentsStartInsideTheWindow(t *testing.T) {
	pickup := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	started := pickup.Add(-20 * time.Minute)
	ended := pickup.Add(30 * time.Minute)
	fake := &probeRecorder{locationCountingFake: locationCountingFake{segments: []*model.Segment{locSeg(started, ended)}}}
	r, err := NewRepository(fake)
	require.NoError(t, err)
	ctx := coverage.With(context.Background(), tokenclaims.Windows{{Start: &pickup}})

	got, err := r.GetSegments(ctx, "did:dimo:car", pickup, pickup.Add(time.Hour),
		model.DetectionMechanismIgnitionDetection, nil, []*model.SegmentSignalRequest{sigSpeed}, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, pickup, got[0].Start.Timestamp)
	assert.True(t, got[0].StartedBeforeRange)
	assert.Equal(t, int(ended.Sub(pickup).Seconds()), got[0].Duration)
	for _, p := range fake.probes {
		assert.False(t, p.Before(pickup), "gap-fill probed %s, before the window", p)
	}
}

// Calendar days reach past the window at both ends; what is read for them
// stops at the window.
func TestDailyActivityStaysInsideTheWindow(t *testing.T) {
	pickup := time.Now().UTC().Add(-26 * time.Hour).Truncate(time.Hour)
	back := pickup.Add(3 * time.Hour)
	fake := &probeRecorder{}
	r, err := NewRepository(fake)
	require.NoError(t, err)
	ctx := coverage.With(context.Background(), tokenclaims.Windows{{Start: &pickup, End: &back}})

	days, err := r.GetDailyActivity(ctx, "did:dimo:car", pickup, back, model.DetectionMechanismIgnitionDetection, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, days)
	for _, d := range days {
		assert.False(t, d.Start.Timestamp.Before(pickup), "day starts at %s", d.Start.Timestamp)
		assert.False(t, d.End.Timestamp.After(back), "day ends at %s", d.End.Timestamp)
	}
	for _, p := range fake.probes {
		assert.False(t, p.Before(pickup) || p.After(back), "gap-fill probed %s", p)
	}
}
