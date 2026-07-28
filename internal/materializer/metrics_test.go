package materializer

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMaterializerMetricsLazyRegistration pins H2: importing this package must
// NOT export dq_materializer_* series — a query-fleet pod exporting the
// cursor/head gauges as 0 defeats the absent()-based DQMaterializerDown alert
// and zeroes the backlog record during a real outage. Only constructing a
// materializer registers the set. Runs in a subprocess because sibling tests
// in this binary construct runners and would register the set first.
func TestMaterializerMetricsLazyRegistration(t *testing.T) {
	if os.Getenv("DQ_METRICS_GATE_SUBPROCESS") == "1" {
		families, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, f := range families {
			require.False(t, strings.HasPrefix(f.GetName(), "dq_materializer_"),
				"%s exported before any materializer was constructed (H2 regression)", f.GetName())
		}

		_ = New(Config{}, zerolog.Nop())

		families, err = prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		var found bool
		for _, f := range families {
			if f.GetName() == "dq_materializer_cursor_snapshot_id" {
				found = true
			}
		}
		require.True(t, found, "constructing a materializer must register dq_materializer_*")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestMaterializerMetricsLazyRegistration$", "-test.v")
	cmd.Env = append(os.Environ(), "DQ_METRICS_GATE_SUBPROCESS=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "subprocess failed:\n%s", out)
}

// TestObserveLakeLag verifies the DuckLake decode-lag gauge tracks the age of
// the oldest un-decoded event and resets to zero when caught up. The ducklake
// path emitted no lag before CHD-12, so the DecodeLag/Stalled alerts were dead
// in the target mode.
func TestObserveLakeLag(t *testing.T) {
	now := time.Now()
	events := []cloudevent.RawEvent{
		{CloudEventHeader: cloudevent.CloudEventHeader{Time: now.Add(-10 * time.Minute)}},
		{CloudEventHeader: cloudevent.CloudEventHeader{Time: now.Add(-1 * time.Minute)}},
	}
	observeLakeLag(events)
	lag := testutil.ToFloat64(lagSeconds.WithLabelValues(lakeMetricType))
	assert.InDelta(t, 600.0, lag, 30.0, "lag tracks the oldest un-decoded event (~10m)")

	// Caught up: no pending events → lag resets to zero.
	observeLakeLag(nil)
	assert.Equal(t, 0.0, testutil.ToFloat64(lagSeconds.WithLabelValues(lakeMetricType)))
}

// TestObserveEventAge pins the distribution that lagSeconds collapses to its
// minimum: every event is recorded, not just the oldest, so a 1%-stale tail is
// distinguishable from a fleet that has genuinely gone late. Zero timestamps (a
// producer that never set one) must be skipped rather than recorded as a
// 56-year-old event, which would drag every quantile to the top bucket.
func TestObserveEventAge(t *testing.T) {
	before := testutil.CollectAndCount(eventAgeSeconds)
	require.Equal(t, 1, before, "histogram collects as a single metric")

	// Collect straight off the metric: registration is lazy (H2), so the default
	// gatherer is empty unless a sibling test happened to construct a materializer.
	countBuckets := func() uint64 {
		var m dto.Metric
		require.NoError(t, eventAgeSeconds.Write(&m))
		return m.GetHistogram().GetSampleCount()
	}
	start := countBuckets()

	now := time.Now()
	observeEventAge(now.Add(-2 * time.Second))
	observeEventAge(now.Add(-6 * time.Hour))
	assert.Equal(t, start+2, countBuckets(), "both real events recorded, unlike the min-only gauge")

	observeEventAge(time.Time{})
	assert.Equal(t, start+2, countBuckets(), "a zero event time is skipped, not recorded as ~56 years old")
}

// TestObserveCommitLag pins the decode-position gauge, and specifically its
// zero: when caught up the cursor deliberately sits BELOW head (head counts the
// materializer's own signals/events snapshots and din's maintenance snapshots),
// so a commit-lag reading taken from "any snapshot after the cursor" would climb
// all night on an idle node — the exact false positive this metric replaces.
func TestObserveCommitLag(t *testing.T) {
	observeCommitLag(time.Now().Add(-90 * time.Minute))
	assert.InDelta(t, 5400.0, testutil.ToFloat64(commitLagSeconds), 30.0,
		"commit lag is the age of the oldest un-decoded snapshot")

	observeCommitLag(time.Time{})
	assert.Equal(t, 0.0, testutil.ToFloat64(commitLagSeconds),
		"caught up ⇒ exactly zero, however far head has run ahead of the cursor")
}
