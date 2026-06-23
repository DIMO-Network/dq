package repositories

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/dq/internal/service/qtypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time interface assertions, duplicated from backend.go so a regression
// shows up as a test-file build failure with a clear location.
var (
	_ Backend         = (*duck.Queries)(nil)
	_ SegmentsBackend = (*duck.LakeSegments)(nil)
	_ QueryService    = composedBackend{}
)

func TestComposeBackendRouting(t *testing.T) {
	t.Parallel()
	segs := []*model.Segment{{}}
	signalBackend := &fakeBackend{
		getEventCounts: func(context.Context, string, time.Time, time.Time, []string) ([]*qtypes.EventCount, error) {
			return []*qtypes.EventCount{{Name: "tripStart", Count: 2}}, nil
		},
	}
	segmentsBackend := &fakePrimary{segments: segs}

	composed := ComposeBackend(signalBackend, segmentsBackend)

	counts, err := composed.GetEventCounts(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), nil)
	require.NoError(t, err)
	require.Len(t, counts, 1)
	assert.Equal(t, "tripStart", counts[0].Name)

	gotSegs, err := composed.GetSegments(context.Background(), "did:test:1", time.Now().Add(-time.Hour), time.Now(), model.DetectionMechanismIdling, nil)
	require.NoError(t, err)
	assert.Equal(t, segs, gotSegs)
}
