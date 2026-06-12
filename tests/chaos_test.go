// chaos_test.go kills the materializer PROCESS (SIGKILL) repeatedly while
// raw bundles stream in, then proves the pipeline invariants held: every
// seeded event decoded exactly once, no duplicate decoded rows, no orphaned
// compaction manifests, watermark consistent. The unit crash matrices kill
// functions between protocol steps; this kills the whole process at random
// wall-clock points under live load — the closest local approximation of a
// node loss in production.
//
// Run: DQ_CHAOS=1 go test ./tests/ -run TestChaos -v -timeout 600s
package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/audit"
	"github.com/DIMO-Network/dq/internal/fsstore"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	chaosEnv       = "DQ_CHAOS"
	chaosWorkerEnv = "DQ_CHAOS_WORKER"
	chaosRootEnv   = "DQ_CHAOS_ROOT"
)

func chaosRunner(store materializer.ObjectStore) *materializer.Runner {
	return materializer.New(materializer.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
		BatchMaxFiles:     4,
		CompactMinFiles:   2,
	}, store, zerolog.Nop())
}

// TestChaosWorker is the subprocess body: it loops materializer passes and
// compactions against the shared root until SIGKILLed by the parent. It is
// a no-op unless the parent set the worker env.
func TestChaosWorker(t *testing.T) {
	if os.Getenv(chaosWorkerEnv) != "1" {
		t.Skip("subprocess worker only")
	}
	store, err := fsstore.New(os.Getenv(chaosRootEnv))
	require.NoError(t, err)
	runner := chaosRunner(store)
	ctx := context.Background()
	for i := 0; ; i++ {
		// Errors are expected mid-kill (parent may have torn us down
		// between syscalls); the protocol's job is to converge later.
		_, _ = runner.RunOnce(ctx)
		if i%5 == 4 {
			_, _ = runner.CompactOnce(ctx)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestChaosPipeline(t *testing.T) {
	if os.Getenv(chaosEnv) != "1" {
		t.Skip("set DQ_CHAOS=1 to run the process-kill chaos harness")
	}
	root := t.TempDir()
	store, err := fsstore.New(root)
	require.NoError(t, err)
	ctx := context.Background()

	// Seed continuously from this process while workers live and die.
	// Closed-date partitions so the compactor participates in the chaos.
	day1 := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	day2 := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	var (
		mu          sync.Mutex
		expectedIDs = map[string]struct{}{}
		seq         atomic.Int64
		stopSeed    = make(chan struct{})
		seedDone    = make(chan struct{})
	)
	seedBundle := func() {
		n := seq.Add(1)
		day := day1
		if n%2 == 0 {
			day = day2
		}
		subject := fmt.Sprintf("did:erc721:137:%s:%d", vehicleNFT.Hex(), n%7)
		ts := day.Add(time.Duration(n) * time.Second)
		id := fmt.Sprintf("chaos-%d", n)
		writeRawBundle(t, store, day, int(n),
			deviceStatus(id, subject, ts, speedAt(ts, float64(n%130))))
		mu.Lock()
		expectedIDs[id] = struct{}{}
		mu.Unlock()
	}
	go func() {
		defer close(seedDone)
		for {
			select {
			case <-stopSeed:
				return
			case <-time.After(20 * time.Millisecond):
				seedBundle()
			}
		}
	}()

	// Kill cycles: start the worker subprocess, let it run a random
	// 100-900ms, SIGKILL it, repeat.
	self, err := os.Executable()
	require.NoError(t, err)
	const cycles = 15
	for i := range cycles {
		cmd := exec.Command(self, "-test.run", "^TestChaosWorker$", "-test.v")
		cmd.Env = append(os.Environ(), chaosWorkerEnv+"=1", chaosRootEnv+"="+root)
		require.NoError(t, cmd.Start(), "cycle %d", i)
		time.Sleep(time.Duration(100+rand.Intn(800)) * time.Millisecond)
		require.NoError(t, cmd.Process.Kill(), "cycle %d", i)
		_ = cmd.Wait()
	}

	close(stopSeed)
	<-seedDone

	// Final drain in-process: converge fully.
	runner := chaosRunner(store)
	for {
		processed, err := runner.RunOnce(ctx)
		require.NoError(t, err)
		if processed == 0 {
			processed, err = runner.RunOnce(ctx)
			require.NoError(t, err)
			if processed == 0 {
				break
			}
		}
	}
	for {
		compacted, err := runner.CompactOnce(ctx)
		require.NoError(t, err)
		if compacted == 0 {
			break
		}
	}

	// Invariants.
	report, err := audit.CheckPipeline(ctx, store, "raw/", "decoded/v1/")
	require.NoError(t, err)
	for _, v := range report.Violations {
		t.Errorf("invariant violation [%s]: %s", v.Kind, v.Detail)
	}

	// Full coverage: every seeded event decoded despite 15 process kills.
	mu.Lock()
	defer mu.Unlock()
	missing := 0
	for id := range expectedIDs {
		if _, ok := report.DecodedIDs[id]; !ok {
			missing++
			if missing <= 5 {
				t.Errorf("seeded event %s never decoded", id)
			}
		}
	}
	assert.Zero(t, missing, "all %d seeded events must decode exactly once", len(expectedIDs))
	t.Logf("chaos survived: %d kill cycles, %d seeded events, %d raw events, %d decoded rows, %d staged orphans, %d violations",
		cycles, len(expectedIDs), report.RawEvents, report.DecodedRows, report.StagedOrphans, len(report.Violations))
}
