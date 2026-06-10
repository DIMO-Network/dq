package materializer

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storeState is a semantic snapshot of everything the materializer wrote,
// used to compare crash-recovery outcomes against a clean run.
type storeState struct {
	dataKeys    []string
	signalRows  []SignalRow
	latestRows  []LatestRow
	summaryRows []SummaryRow
	watermark   map[string]string
	manifests   []string
}

func snapshotState(t *testing.T, store *memStore, r *Runner) storeState {
	t.Helper()
	ctx := context.Background()
	st := storeState{watermark: readWatermark(t, store, r)}

	for _, key := range store.keys("decoded/v1/signals/") {
		st.dataKeys = append(st.dataKeys, key)
		st.signalRows = append(st.signalRows, readSignalRows(t, store, key)...)
	}
	for _, key := range store.keys("decoded/v1/events/") {
		st.dataKeys = append(st.dataKeys, key)
	}
	for _, key := range store.keys("decoded/v1/latest/") {
		rows, _, err := loadBucket[LatestRow](ctx, store, key)
		require.NoError(t, err)
		st.latestRows = append(st.latestRows, rows...)
	}
	for _, key := range store.keys("decoded/v1/summary/") {
		rows, _, err := loadBucket[SummaryRow](ctx, store, key)
		require.NoError(t, err)
		st.summaryRows = append(st.summaryRows, rows...)
	}
	st.manifests = store.keys("decoded/v1/_state/manifests/")

	// Normalize for comparison: parquet read-back timestamps share a
	// location, so sorting yields a deterministic order.
	slices.SortFunc(st.signalRows, func(a, b SignalRow) int {
		return strings.Compare(
			fmt.Sprint(a.Subject, a.Name, a.Timestamp.UnixMicro(), a.ValueNumber, a.ValueString),
			fmt.Sprint(b.Subject, b.Name, b.Timestamp.UnixMicro(), b.ValueNumber, b.ValueString),
		)
	})
	slices.SortFunc(st.latestRows, func(a, b LatestRow) int {
		return strings.Compare(a.Subject+a.Source+a.Name, b.Subject+b.Source+b.Name)
	})
	slices.SortFunc(st.summaryRows, func(a, b SummaryRow) int {
		return strings.Compare(a.Subject+a.Source+a.Name, b.Subject+b.Source+b.Name)
	})
	return st
}

// seedCrashFixture writes a two-file raw batch spanning two subjects (and
// therefore at least two latest/summary buckets) plus location signals, so
// the commit protocol performs several distinct PUTs:
// signal data, N latest buckets, N summary buckets, manifest, watermark.
func seedCrashFixture(t *testing.T) (*memStore, string, []string) {
	t.Helper()
	source := "0xCrashTestSource"
	SignalRegistry.Override(source, &fakeSignalModule{fn: func(ev cloudevent.RawEvent) ([]vss.Signal, error) {
		return []vss.Signal{
			namedSignal("speed", ev.Time, 11),
			namedSignal("speed", ev.Time.Add(time.Second), 22),
			{Data: vss.SignalData{Name: vss.FieldCurrentLocationCoordinates, Timestamp: ev.Time,
				ValueLocation: vss.Location{Latitude: 1, Longitude: 2}}},
		}, nil
	}})

	// Pick subjects that land in different hash buckets.
	subjects := []string{"did:erc721:1:0xveh:100", "did:erc721:1:0xveh:101"}
	require.NotEqual(t, hashBucket(subjects[0]), hashBucket(subjects[1]),
		"fixture subjects must hash to different buckets")

	store := newMemStore()
	keys := []string{
		rawKey(cloudevent.TypeStatus, "2026-06-08", "ingest-1749384002000-02A"),
		rawKey(cloudevent.TypeStatus, "2026-06-08", "ingest-1749384002001-02B"),
	}
	putRawFile(t, store, keys[0], []cloudevent.StoredEvent{
		statusEvent(subjects[0], source, "ce-c1", baseTime, `{}`),
	})
	putRawFile(t, store, keys[1], []cloudevent.StoredEvent{
		statusEvent(subjects[1], source, "ce-c2", baseTime.Add(time.Minute), `{}`),
	})
	return store, source, keys
}

// TestCrashRecoveryMatrix simulates a crash after every possible write in
// the commit protocol (decoded data PUTs, each latest/summary bucket PUT,
// manifest PUT, watermark PUT) and verifies that re-running the batch
// converges to exactly the clean-run state: no duplicate decoded rows,
// summary counts not double-applied, latest values correct, watermark
// correct.
func TestCrashRecoveryMatrix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Clean reference run, also counting the total writes in the protocol.
	seed, _, rawKeys := seedCrashFixture(t)
	cleanStore := seed.clone()
	counting := &countingStore{ObjectStore: cleanStore}
	cleanRunner := New(Config{}, counting, zerolog.Nop())
	processed, err := cleanRunner.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, len(rawKeys), processed)
	want := snapshotState(t, cleanStore, cleanRunner)

	totalPuts := counting.puts
	// 1 signal data file + 2 latest buckets + 2 summary buckets +
	// 1 manifest + 1 watermark.
	require.Equal(t, 7, totalPuts, "fixture write fan-out changed; matrix below relies on it")
	require.Len(t, want.manifests, 1)
	require.Equal(t, rawKeys[1], want.watermark["type=dimo.status/date=2026-06-08"])
	require.Len(t, want.summaryRows, 4) // (speed, coordinates) x 2 subjects
	for _, row := range want.summaryRows {
		require.Equal(t, expectedSummaryCount(row.Name), row.Count)
	}

	for failAfter := 0; failAfter < totalPuts; failAfter++ {
		t.Run(fmt.Sprintf("crash_after_%d_writes", failAfter), func(t *testing.T) {
			store := seed.clone()
			flaky := &flakyStore{ObjectStore: store, allowedPuts: failAfter}

			// Crashing run: must fail.
			crashed := New(Config{}, flaky, zerolog.Nop())
			_, err := crashed.RunOnce(ctx)
			require.ErrorIs(t, err, errInjected)

			// Recovery run against the same (now healthy) store.
			recovered := New(Config{}, store, zerolog.Nop())
			processed, err := recovered.RunOnce(ctx)
			require.NoError(t, err)
			require.Equal(t, len(rawKeys), processed, "replay must reprocess the whole batch")

			got := snapshotState(t, store, recovered)
			// Deterministic output keys: replay overwrote, never duplicated.
			assert.Equal(t, want.dataKeys, got.dataKeys)
			assert.Equal(t, len(want.signalRows), len(got.signalRows), "no duplicate decoded rows")
			assert.Equal(t, want.signalRows, got.signalRows)
			// Latest values converge to the clean-run state.
			assert.Equal(t, want.latestRows, got.latestRows)
			// Summary increments applied exactly once.
			assert.Equal(t, want.summaryRows, got.summaryRows)
			assert.Equal(t, want.watermark, got.watermark)
			assert.Equal(t, want.manifests, got.manifests)

			// And the batch stays settled: another pass is a no-op.
			processed, err = recovered.RunOnce(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, processed)
		})
	}
}

// TestReplayAfterFullCommitIsIdempotent forces a replay of a fully
// committed batch (crash after manifest+buckets but before the watermark)
// and checks the manifest gate skips the summary increments.
func TestReplayAfterFullCommitIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	seed, _, rawKeys := seedCrashFixture(t)
	store := seed.clone()

	// Crash exactly before the watermark write (last PUT of 7).
	flaky := &flakyStore{ObjectStore: store, allowedPuts: 6}
	crashed := New(Config{}, flaky, zerolog.Nop())
	_, err := crashed.RunOnce(ctx)
	require.ErrorIs(t, err, errInjected)
	require.Len(t, store.keys("decoded/v1/_state/manifests/"), 1, "manifest must be on disk before the watermark write")

	recovered := New(Config{}, store, zerolog.Nop())
	processed, err := recovered.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, len(rawKeys), processed)

	got := snapshotState(t, store, recovered)
	require.Len(t, got.summaryRows, 4)
	for _, row := range got.summaryRows {
		assert.Equal(t, expectedSummaryCount(row.Name), row.Count, "summary increments must not be double-applied")
	}
	assert.Equal(t, rawKeys[1], got.watermark["type=dimo.status/date=2026-06-08"])
}

// expectedSummaryCount returns the per-(subject, source, name) signal
// count the crash fixture produces per subject.
func expectedSummaryCount(name string) uint64 {
	if name == "speed" {
		return 2
	}
	return 1 // currentLocationCoordinates
}
