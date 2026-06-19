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

// TestLakeEventService_DefaultScanWindow proves an open-ended lake fetch is
// bounded by a default lookback window (CH capped lookbacks; the lake path had
// none → unbounded full scan, CHD-34), while a point lookup by id stays
// unbounded so old events remain fetchable.
func TestLakeEventService_DefaultScanWindow(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	old := mkStoredEvent("old-ev", "dimo.status", lakeRawSubj, now.AddDate(0, 0, -500))
	recent := mkStoredEvent("recent-ev", "dimo.status", lakeRawSubj, now.Add(-time.Hour))
	insertRawEvent(t, svc, old)
	insertRawEvent(t, svc, recent)

	// List with no time bound excludes the 500-day-old event.
	res, err := lsvc.ListIndexesAdvanced(ctx, 100, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	})
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
