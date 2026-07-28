// ducklake_commit_lag_test.go pins dq_materializer_commit_lag_seconds against a
// real DuckLake catalog. The gauge exists because the metric the DecodeLag alert
// used to fire on — dq_materializer_lag_seconds — is now − min(event time) over
// the batch, i.e. PRODUCER lateness reduced to its worst straggler: on the GCP
// node it read 11–13h through every business day while decode ran seconds behind,
// so the alert was permanently firing on a healthy pipeline.
//
// Two things here can only be caught against a real lake:
//
//   - the snapshot_time column and the FILTER aggregate in snapshotState's query
//   - the timezone of what it returns — a naive timestamp read as local time
//     would make the age land hours off, or negative
//
// Both matter more since commit lag was folded into the head read: that query is
// on the decode path, so a mistake in it fails every pass rather than quietly
// skipping a metric. Cheap to get right, expensive to ship wrong — hence a real
// lake rather than a stub. The "> 0 while behind" assertion covers both: a
// timezone misread comes back negative, and a wrong column fails the query
// outright.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// commitLagGauge reads dq_materializer_commit_lag_seconds off the default
// registry (constructing a materializer registers the set — H2).
func commitLagGauge(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "dq_materializer_commit_lag_seconds" {
			require.Len(t, f.GetMetric(), 1)
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("dq_materializer_commit_lag_seconds not exported after constructing a materializer")
	return 0
}

func TestDuckLake_CommitLagTracksDecodePosition(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:11", vehicleNFT.Hex())

	// Event times deliberately BACKDATED three days: commit lag must track when the
	// snapshot was committed to the catalog (just now), NOT how old the telemetry
	// inside it is. This is the exact distinction the old alert got wrong.
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	seedRawStatus(t, db, "cl-1", subject, day.Add(time.Hour), speedAt(day.Add(time.Hour), 40))
	seedRawStatus(t, db, "cl-2", subject, day.Add(2*time.Hour), speedAt(day.Add(2*time.Hour), 80))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)

	// One pass with work pending: the gauge is set from the catalog commit time of
	// the oldest un-decoded snapshot, which is seconds old however old the events are.
	processed, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Positive(t, processed, "the seeded raw events were decoded")

	behind := commitLagGauge(t)
	assert.Positive(t, behind,
		"commit lag must be > 0 with un-decoded snapshots pending: 0 means the snapshots() query failed (it is non-fatal by design), negative means snapshot_time was read in the wrong timezone")
	assert.Less(t, behind, float64(time.Hour/time.Second),
		"commit lag is the age of the SNAPSHOT (seconds old), not of the 3-day-old events inside it")

	// Drained: the gauge must be exactly zero. This is the load-bearing case — when
	// caught up the cursor deliberately sits BELOW head (head counts the
	// materializer's own signals/events snapshots, which never advance it), so a
	// naive "age of any snapshot after the cursor" reading would climb all night on
	// an idle node, reproducing the false positive this metric replaces.
	drainRunner(t, ctx, runner)
	assert.Equal(t, 0.0, commitLagGauge(t),
		"caught up ⇒ exactly zero, however far head has run ahead of the cursor")

	// New data ⇒ pending again, and still measured from the commit, not the event.
	seedRawStatus(t, db, "cl-3", subject, day.Add(3*time.Hour), speedAt(day.Add(3*time.Hour), 65))
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Positive(t, commitLagGauge(t), "a freshly committed snapshot puts the decoder behind again")
}
