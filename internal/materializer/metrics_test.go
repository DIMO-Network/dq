package materializer

import (
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

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
