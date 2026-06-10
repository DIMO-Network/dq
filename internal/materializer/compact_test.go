package materializer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const compactTestSubject = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42"

func compactRunner(store ObjectStore) *Runner {
	return New(Config{CompactMinFiles: 2}, store, zerolog.Nop())
}

func signalRowAt(ceID string, ts time.Time, name string, v float64) SignalRow {
	return SignalRow{
		Subject:      compactTestSubject,
		Name:         name,
		Timestamp:    ts,
		Source:       "0xConn",
		Producer:     compactTestSubject,
		CloudEventID: ceID,
		ValueNumber:  v,
	}
}

func eventRowAt(ceID string, ts time.Time, name string) EventRow {
	return EventRow{
		Subject:      compactTestSubject,
		Source:       "0xConn",
		Producer:     compactTestSubject,
		CloudEventID: ceID,
		Type:         "dimo.events",
		DataVersion:  "default/v1.0",
		Name:         name,
		Timestamp:    ts,
	}
}

func putSignalFile(t *testing.T, store ObjectStore, date, name string, rows ...SignalRow) {
	t.Helper()
	body, err := writeSignalParquet(rows)
	require.NoError(t, err)
	require.NoError(t, store.PutObject(context.Background(), defaultDecodedPrefix+"signals/date="+date+"/"+name, body))
}

func putEventFile(t *testing.T, store ObjectStore, date, name string, rows ...EventRow) {
	t.Helper()
	body, err := writeEventParquet(rows)
	require.NoError(t, err)
	require.NoError(t, store.PutObject(context.Background(), defaultDecodedPrefix+"events/date="+date+"/"+name, body))
}

func TestCompactOnce_MergesClosedPartitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemStore()
	r := compactRunner(store)

	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	date := day.Format(datePartitionFormat)
	today := time.Now().UTC().Format(datePartitionFormat)

	dup := signalRowAt("ce-1", day.Add(time.Hour), "speed", 40)
	putSignalFile(t, store, date, "batch-a.parquet", dup, signalRowAt("ce-2", day.Add(2*time.Hour), "speed", 50))
	putSignalFile(t, store, date, "batch-b.parquet", dup, signalRowAt("ce-3", day.Add(3*time.Hour), "speed", 60))
	putSignalFile(t, store, date, "batch-c.parquet", signalRowAt("ce-4", day.Add(4*time.Hour), "fuel", 0.5))
	putEventFile(t, store, date, "batch-a.parquet", eventRowAt("ce-5", day.Add(time.Hour), "harshBraking"))
	putEventFile(t, store, date, "batch-b.parquet", eventRowAt("ce-6", day.Add(2*time.Hour), "harshBraking"))
	// Open partition: must never be touched.
	putSignalFile(t, store, today, "batch-x.parquet", signalRowAt("ce-7", time.Now().UTC(), "speed", 70))
	putSignalFile(t, store, today, "batch-y.parquet", signalRowAt("ce-8", time.Now().UTC(), "speed", 71))

	n, err := r.CompactOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "one signals and one events partition")

	sigKeys := store.keys(defaultDecodedPrefix + "signals/date=" + date + "/")
	require.Equal(t, []string{defaultDecodedPrefix + "signals/date=" + date + "/" + compactedName}, sigKeys)
	rows := readSignalRows(t, store, sigKeys[0])
	assert.Len(t, rows, 4, "cross-file duplicate collapsed")

	evtKeys := store.keys(defaultDecodedPrefix + "events/date=" + date + "/")
	require.Equal(t, []string{defaultDecodedPrefix + "events/date=" + date + "/" + compactedName}, evtKeys)

	assert.Len(t, store.keys(defaultDecodedPrefix+"signals/date="+today+"/"), 2, "open partition untouched")
	assert.Empty(t, store.keys(defaultDecodedPrefix+compactionPrefix), "staging cleaned up")

	n, err = r.CompactOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "second pass is a no-op")
}

func TestCompactOnce_RecompactionKeepsTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemStore()
	r := compactRunner(store)

	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	date := day.Format(datePartitionFormat)

	putSignalFile(t, store, date, "batch-a.parquet", signalRowAt("ce-1", day.Add(time.Hour), "speed", 40))
	putSignalFile(t, store, date, "batch-b.parquet", signalRowAt("ce-2", day.Add(2*time.Hour), "speed", 50))
	_, err := r.CompactOnce(ctx)
	require.NoError(t, err)

	// Late batch lands after the first compaction.
	putSignalFile(t, store, date, "batch-late.parquet", signalRowAt("ce-3", day.Add(5*time.Hour), "speed", 60))
	n, err := r.CompactOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	keys := store.keys(defaultDecodedPrefix + "signals/date=" + date + "/")
	require.Equal(t, []string{defaultDecodedPrefix + "signals/date=" + date + "/" + compactedName}, keys)
	rows := readSignalRows(t, store, keys[0])
	assert.Len(t, rows, 3, "recompaction folds late batch into existing target")
}

// opLimitStore fails every mutating call after the first allowed ops,
// simulating a crash at an arbitrary point in the compaction protocol.
type opLimitStore struct {
	*memStore
	mu      sync.Mutex
	allowed int
	ops     int
}

func (o *opLimitStore) step() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ops >= o.allowed {
		return errInjected
	}
	o.ops++
	return nil
}

func (o *opLimitStore) PutObject(ctx context.Context, key string, body []byte) error {
	if err := o.step(); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return o.memStore.PutObject(ctx, key, body)
}

func (o *opLimitStore) DeleteObject(ctx context.Context, key string) error {
	if err := o.step(); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return o.memStore.DeleteObject(ctx, key)
}

// TestCompactOnce_CrashMatrix kills the protocol after every possible
// mutating operation and verifies two things at each point: queries never
// see sources and target together (no double count), and a recovery pass
// converges to the exact no-crash end state.
func TestCompactOnce_CrashMatrix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	date := day.Format(datePartitionFormat)
	partition := defaultDecodedPrefix + "signals/date=" + date + "/"
	target := partition + compactedName

	seed := func() *memStore {
		store := newMemStore()
		putSignalFile(t, store, date, "batch-a.parquet", signalRowAt("ce-1", day.Add(time.Hour), "speed", 40))
		putSignalFile(t, store, date, "batch-b.parquet", signalRowAt("ce-2", day.Add(2*time.Hour), "speed", 50))
		putSignalFile(t, store, date, "batch-c.parquet", signalRowAt("ce-3", day.Add(3*time.Hour), "speed", 60))
		return store
	}

	// Reference: end state of an uninterrupted run.
	want := seed()
	_, err := compactRunner(want).CompactOnce(ctx)
	require.NoError(t, err)
	wantKeys := want.keys(defaultDecodedPrefix)

	for crashAfter := 0; ; crashAfter++ {
		store := seed()
		limited := &opLimitStore{memStore: store, allowed: crashAfter}
		_, err := compactRunner(limited).CompactOnce(ctx)
		if err == nil {
			// Enough ops allowed for a full run: matrix exhausted.
			require.Equal(t, wantKeys, store.keys(defaultDecodedPrefix))
			break
		}

		// Invariant at the crash point: a query over the partition glob
		// must never count a row twice. Sources and target are never
		// visible together.
		var total int
		sawTarget := false
		for _, key := range store.keys(partition) {
			rows := readSignalRows(t, store, key)
			total += len(rows)
			if key == target {
				sawTarget = true
			}
		}
		if sawTarget {
			assert.Equal(t, 3, total, "crash after %d ops: target visible means sources gone", crashAfter)
		} else {
			assert.LessOrEqual(t, total, 3, "crash after %d ops: never more rows than the union", crashAfter)
		}

		// Recovery: a fresh pass converges to the no-crash state.
		_, err = compactRunner(store).CompactOnce(ctx)
		require.NoError(t, err, "recovery after crash at op %d", crashAfter)
		assert.Equal(t, wantKeys, store.keys(defaultDecodedPrefix), "post-recovery state after crash at op %d", crashAfter)

		require.Less(t, crashAfter, 50, "crash matrix runaway")
	}
}
