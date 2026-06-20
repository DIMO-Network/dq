package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLakeEventService_DefaultScanWindow proves the default lookback window is a
// DoS guard for subject-less, id-less scans only — NOT a parity bound. A
// subject-scoped fetch prunes to one vehicle's files, so it reaches arbitrarily
// old events (matching ClickHouse, which imposes no floor when given no
// `after`); only a subject-less search keeps the window, and a point lookup by
// id bypasses it (SR review #4).
func TestLakeEventService_DefaultScanWindow(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	old := mkStoredEvent("old-ev", "dimo.status", lakeRawSubj, now.AddDate(0, 0, -500))
	recent := mkStoredEvent("recent-ev", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	insertRawEvent(t, svc, old)
	insertRawEvent(t, svc, recent)

	// Subject-scoped list: no floor, so the 500-day-old event is returned too
	// (a dormant vehicle must not look empty).
	res, err := lsvc.ListIndexesAdvanced(ctx, 100, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	})
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, e := range res {
		ids[e.ID] = true
	}
	assert.Equal(t, map[string]bool{"old-ev": true, "recent-ev": true}, ids,
		"a subject-scoped fetch must reach arbitrarily old events (no floor)")

	// Subject-less, id-less search keeps the default window as a DoS guard: the
	// 500-day-old event is excluded.
	res, err = lsvc.ListIndexesAdvanced(ctx, 100, &grpc.AdvancedSearchOptions{})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, "recent-ev", res[0].ID)

	// A point lookup by id bypasses the window: the old event is still fetchable.
	got, err := lsvc.GetCloudEventFromIndex(ctx, &cloudevent.CloudEvent[eventrepo.ObjectInfo]{
		CloudEventHeader: cloudevent.CloudEventHeader{Subject: lakeRawSubj, ID: "old-ev"},
	}, "")
	require.NoError(t, err)
	assert.Equal(t, "old-ev", got.ID)
}
