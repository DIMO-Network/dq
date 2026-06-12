// sharding_test.go proves the sharded-materializer layout: two shards (as
// two replicas would run) decode disjoint partition sets into per-shard
// watermarks and bucket namespaces, the DuckDB layer reads across shard
// namespaces transparently, and the audit sees no split-brain.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/audit"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func shardRunner(store materializer.ObjectStore, index, count int) *materializer.Runner {
	return materializer.New(materializer.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
		BatchMaxFiles:     1,
		CompactMinFiles:   2,
		ShardIndex:        index,
		ShardCount:        count,
	}, store, zerolog.Nop())
}

func drain(t *testing.T, ctx context.Context, r *materializer.Runner) {
	t.Helper()
	for {
		processed, err := r.RunOnce(ctx)
		require.NoError(t, err)
		if processed == 0 {
			break
		}
	}
}

func TestShardedMaterializer_EndToEnd(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := newFSStore(t, root)
	subject := fmt.Sprintf("did:erc721:137:%s:99", vehicleNFT.Hex())

	// Several date partitions so both shards own work.
	expected := 0
	var lastTS time.Time
	var lastSpeed float64
	for d := 2; d <= 5; d++ {
		day := time.Now().UTC().AddDate(0, 0, -d).Truncate(24 * time.Hour)
		for i := range 3 {
			ts := day.Add(time.Duration(i+1) * time.Hour)
			speed := float64(d*10 + i)
			writeRawBundle(t, store, day, d*10+i,
				deviceStatus(fmt.Sprintf("shard-%d-%d", d, i), subject, ts, speedAt(ts, speed)))
			expected++
			if ts.After(lastTS) {
				lastTS, lastSpeed = ts, speed
			}
		}
	}

	// Two shards, run like two replicas (interleaved passes).
	shard0 := shardRunner(store, 0, 2)
	shard1 := shardRunner(store, 1, 2)
	drain(t, ctx, shard0)
	drain(t, ctx, shard1)
	drain(t, ctx, shard0) // second pass: nothing left anywhere
	drain(t, ctx, shard1)

	// Disjoint per-shard state on disk.
	wm0, err := store.List(ctx, "decoded/v1/_state/watermark-p000of002.json")
	require.NoError(t, err)
	wm1, err := store.List(ctx, "decoded/v1/_state/watermark-p001of002.json")
	require.NoError(t, err)
	require.Len(t, wm0, 1, "shard 0 watermark")
	require.Len(t, wm1, 1, "shard 1 watermark")

	shardBuckets, err := store.List(ctx, "decoded/v1/latest/shard=")
	require.NoError(t, err)
	require.NotEmpty(t, shardBuckets, "latest buckets live under shard namespaces")

	// Sharded compaction: each shard compacts only its own partitions.
	n0, err := shard0.CompactOnce(ctx)
	require.NoError(t, err)
	n1, err := shard1.CompactOnce(ctx)
	require.NoError(t, err)
	require.Positive(t, n0+n1, "closed partitions compacted across shards")

	// DuckDB reads across both shard namespaces transparently.
	svc, err := duck.NewService(duck.Config{S3Enabled: false})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	queries := duck.NewQueries(svc, root)

	latest, err := queries.GetLatestSignals(ctx, subject, &model.LatestSignalsArgs{
		SignalArgs:  model.SignalArgs{Subject: subject},
		SignalNames: map[string]struct{}{vss.FieldSpeed: {}},
	})
	require.NoError(t, err)
	require.Len(t, latest, 1)
	assert.Equal(t, lastSpeed, latest[0].Data.ValueNumber, "latest across shard namespaces is the global latest")
	assert.True(t, lastTS.Equal(latest[0].Data.Timestamp))

	names, err := queries.GetAvailableSignals(ctx, subject, nil)
	require.NoError(t, err)
	assert.Contains(t, names, vss.FieldSpeed, "summary merged across shard namespaces")

	// Summary counts merged across shard namespaces see every event
	// exactly once.
	summaries, err := queries.GetSignalSummaries(ctx, subject, nil)
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	assert.Equal(t, uint64(expected), summaries[0].NumberOfSignals, "every event counted exactly once across shards")

	// Audit: no split-brain, no duplicates, staging clean.
	report, err := audit.CheckPipeline(ctx, store, "raw/", "decoded/v1/")
	require.NoError(t, err)
	for _, v := range report.Violations {
		t.Errorf("invariant violation [%s]: %s", v.Kind, v.Detail)
	}
	assert.Equal(t, expected, report.DecodedRows)
}
