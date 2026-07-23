package latestkv

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startNATS boots an in-process JetStream server (no network listener, like
// din's natsembed) and returns a connection over the in-process transport.
func startNATS(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		DontListen: true,
		NoSigs:     true,
	})
	require.NoError(t, err)
	go srv.Start()
	require.True(t, srv.ReadyForConnections(10*time.Second))
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	conn, err := nats.Connect("", nats.InProcessServer(srv))
	require.NoError(t, err)
	return conn
}

func newStore(t *testing.T, conn *nats.Conn, bucket string) *Store {
	t.Helper()
	s, err := NewWithConn(context.Background(), conn, bucket, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func TestStore_PublishAndMerge(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-publish")
	subject := "did:erc721:137:0xAB:1"

	require.NoError(t, s.PublishSignals(ctx, []Row{
		{Subject: subject, Name: "speed", Timestamp: t0, CloudEventID: "a", ValueNumber: 40},
		{Subject: subject, Name: "odometer", Timestamp: t0, CloudEventID: "a", ValueNumber: 1000},
	}))
	// A later batch advances one name and leaves the other.
	require.NoError(t, s.PublishSignals(ctx, []Row{
		{Subject: subject, Name: "speed", Timestamp: t0.Add(time.Minute), CloudEventID: "b", ValueNumber: 65},
	}))

	entry, err := s.getEntry(ctx, KeyForSubject(subject))
	require.NoError(t, err)
	assert.Equal(t, EntryVersion, entry.V)
	assert.Equal(t, 65.0, entry.Signals["speed"].Num)
	assert.Equal(t, 1000.0, entry.Signals["odometer"].Num)
	assert.Equal(t, t0.Add(time.Minute), entry.LastSeen())
}

// A replayed batch (crash recovery, NATS redelivery) folds to a no-op — the
// entry revision must not move, proving the Put was skipped.
func TestStore_ReplayedBatchSkipsPut(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-replay")
	subject := "did:erc721:137:0xAB:2"
	batch := []Row{{Subject: subject, Name: "speed", Timestamp: t0, CloudEventID: "a", ValueNumber: 40}}

	require.NoError(t, s.PublishSignals(ctx, batch))
	kve, err := s.kv.Get(ctx, KeyForSubject(subject))
	require.NoError(t, err)
	rev := kve.Revision()

	require.NoError(t, s.PublishSignals(ctx, batch))
	kve, err = s.kv.Get(ctx, KeyForSubject(subject))
	require.NoError(t, err)
	assert.Equal(t, rev, kve.Revision(), "identical replay must not rewrite the entry")
}

// An undecodable entry (schema damage) self-heals from the next batch instead
// of wedging that subject's publishes forever.
func TestStore_CorruptEntrySelfHeals(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-corrupt")
	subject := "did:erc721:137:0xAB:3"
	key := KeyForSubject(subject)
	_, err := s.kv.Put(ctx, key, []byte("{not json"))
	require.NoError(t, err)

	require.NoError(t, s.PublishSignals(ctx, []Row{
		{Subject: subject, Name: "speed", Timestamp: t0, CloudEventID: "a", ValueNumber: 40},
	}))
	entry, err := s.getEntry(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, 40.0, entry.Signals["speed"].Num)
}

// Reopening against an existing bucket must look it up, not try to recreate
// it (lookupOrCreateBucket never mutates existing config).
func TestStore_ReopenExistingBucket(t *testing.T) {
	ctx := context.Background()
	conn := startNATS(t)
	s1 := newStore(t, conn, "t-reopen")
	subject := "did:erc721:137:0xAB:4"
	require.NoError(t, s1.PublishSignals(ctx, []Row{
		{Subject: subject, Name: "speed", Timestamp: t0, CloudEventID: "a", ValueNumber: 40},
	}))

	s2, err := NewWithConn(ctx, conn, "t-reopen", zerolog.Nop())
	require.NoError(t, err)
	entry, err := s2.getEntry(ctx, KeyForSubject(subject))
	require.NoError(t, err)
	assert.Equal(t, 40.0, entry.Signals["speed"].Num, "second open sees the first open's data")
}
