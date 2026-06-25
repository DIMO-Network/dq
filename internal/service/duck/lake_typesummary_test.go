package duck

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// availableCloudEventTypes count must dedup redelivered duplicates (same second-key)
// like the fetch path, so the per-type summary count agrees with what cloudEvents
// returns over identical data. din's writer is a blind append, so a duplicate can
// persist past the NATS DuplicateWindow on lag/failover/replay; a bare count(*) would
// report 2 where cloudEvents reports 1.
func TestLakeEventService_TypeSummaryDedupsDuplicates(t *testing.T) {
	ctx := context.Background()
	lsvc, svc := newLakeEventServiceForTest(t)
	sec := time.Now().UTC().Truncate(time.Second)
	// Two physical rows sharing the dedup key (same id, same second, different sub-second).
	insertRawEvent(t, svc, mkStoredEvent("dup-id", "dimo.status", lakeRawSubj, sec.Add(100*time.Millisecond)))
	insertRawEvent(t, svc, mkStoredEvent("dup-id", "dimo.status", lakeRawSubj, sec.Add(900*time.Millisecond)))

	sums, err := lsvc.GetCloudEventTypeSummariesAdvanced(ctx, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{lakeRawSubj}},
	})
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.Equal(t, "dimo.status", sums[0].Type)
	assert.Equal(t, uint64(1), sums[0].Count, "redelivered duplicates must collapse to one in the type count")
}
