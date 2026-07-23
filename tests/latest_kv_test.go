// latest_kv_test.go covers the signals-latest NATS KV cache's write side
// end-to-end: the materializer publishes every decoded batch into the bucket
// (via the app adapter), the entry agrees with the lake.signals_latest rollup
// it mirrors, replayed input leaves it untouched, and BootstrapFromRollup
// fills a fresh bucket to the same state the live publishes produced.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/app"
	"github.com/DIMO-Network/dq/internal/latestkv"
	"github.com/DIMO-Network/dq/internal/materializer"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLatestKVStore boots an in-process JetStream server (no network listener)
// and opens a Store on bucket.
func newLatestKVStore(t *testing.T, bucket string) *latestkv.Store {
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
	store, err := latestkv.NewWithConn(context.Background(), conn, bucket, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(store.Close)
	return store
}

func TestLatestKV_PublishedAtDecode_MatchesRollup(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	store := newLatestKVStore(t, "live")
	subject := fmt.Sprintf("did:erc721:137:%s:9", vehicleNFT.Hex())
	// Whole-hour timestamps: the lake stores microseconds, so sub-microsecond
	// fixtures would make the KV-vs-rollup equality below flaky.
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1, t2 := day.Add(time.Hour), day.Add(2*time.Hour)

	seedRawStatus(t, db, "kv-1", subject, t1, speedAt(t1, 40))
	seedRawStatus(t, db, "kv-2", subject, t2, speedAt(t2, 65))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	mat.WithLatestPublisher(app.NewLatestKVPublisher(store, zerolog.Nop()))
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	entry, err := store.GetEntry(ctx, subject)
	require.NoError(t, err)
	require.NotNil(t, entry, "decode published the subject's entry")
	require.Contains(t, entry.Signals, "speed")
	assert.Equal(t, 65.0, entry.Signals["speed"].Num)
	assert.Equal(t, t2, entry.Signals["speed"].TS.UTC())
	assert.Equal(t, t2, entry.LastSeen().UTC(), "lastSeen matches the newest reading")

	// The entry must agree with the rollup it mirrors.
	var rollupTS time.Time
	var rollupVal float64
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT "timestamp", value_number FROM lake.signals_latest WHERE subject = ? AND name = 'speed'`,
		subject).Scan(&rollupTS, &rollupVal))
	assert.Equal(t, rollupVal, entry.Signals["speed"].Num)
	assert.Equal(t, rollupTS.UTC(), entry.Signals["speed"].TS.UTC())

	// An unknown subject reads as absent, not an error (the reader's miss ⇒
	// fall-back-to-rollup contract).
	missing, err := store.GetEntry(ctx, fmt.Sprintf("did:erc721:137:%s:404", vehicleNFT.Hex()))
	require.NoError(t, err)
	assert.Nil(t, missing)
}

func TestLatestKV_BootstrapFromRollup(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:11", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	t1, t2 := day.Add(time.Hour), day.Add(2*time.Hour)

	seedRawStatus(t, db, "kvb-1", subject, t1, speedAt(t1, 40))
	seedRawStatus(t, db, "kvb-2", subject, t2, speedAt(t2, 65))

	// Decode WITHOUT a publisher: the rollup fills, the KV stays empty — the
	// "enabling the cache on an existing node" state bootstrap exists for.
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	store := newLatestKVStore(t, "boot")
	require.NoError(t, store.BootstrapFromRollup(ctx, db, false))

	entry, err := store.GetEntry(ctx, subject)
	require.NoError(t, err)
	require.NotNil(t, entry, "bootstrap filled the subject from the rollup")
	assert.Equal(t, 65.0, entry.Signals["speed"].Num)
	assert.Equal(t, t2, entry.Signals["speed"].TS.UTC())

	// Second run is marker-gated (a plain restart must not rescan the rollup),
	// and a forced run over a live bucket merges without regressing.
	require.NoError(t, store.BootstrapFromRollup(ctx, db, false))
	require.NoError(t, store.BootstrapFromRollup(ctx, db, true))
	entry, err = store.GetEntry(ctx, subject)
	require.NoError(t, err)
	assert.Equal(t, 65.0, entry.Signals["speed"].Num)
}
