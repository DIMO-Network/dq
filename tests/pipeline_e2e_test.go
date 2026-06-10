// Package tests proves the parse-on-read pipeline end to end on a local
// filesystem store:
//
//	raw cloudevent bundles (byte-identical to what the din ingest service
//	writes: same hive layout, same ingest-<ms>-<ulid> naming, same
//	sorted/zstd/bloom encoding)
//	  → materializer (model-garage post-fact decode, watermark commit)
//	  → DuckDB queries (aggregations, latest, available, raw scans)
//
// The device→raw half of the pipeline (HTTP → NATS → parquet sink) is
// covered by din's own e2e test; the raw layout in between is the contract
// both tests pin.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	vehicleNFT = common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF")
	vehicleA   = fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	vehicleB   = fmt.Sprintf("did:erc721:137:%s:77", vehicleNFT.Hex())
	// A device DID on a different contract: must never reach decoded tables.
	nonVehicle = "did:erc721:137:0x0000000000000000000000000000000000000001:9"
)

// fsStore implements materializer.ObjectStore over a local directory — the
// same files DuckDB later reads through local-path globs, so the store IS
// the bucket.
type fsStore struct {
	root string
}

func (f *fsStore) path(key string) string { return filepath.Join(f.root, filepath.FromSlash(key)) }

func (f *fsStore) List(_ context.Context, prefix string) ([]materializer.ObjectInfo, error) {
	var out []materializer.ObjectInfo
	err := filepath.Walk(f.root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(f.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			out = append(out, materializer.ObjectInfo{Key: key, Size: info.Size()})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, err
}

func (f *fsStore) GetObject(_ context.Context, key string) ([]byte, error) {
	body, err := os.ReadFile(f.path(key))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %s", materializer.ErrNotFound, key)
	}
	return body, err
}

func (f *fsStore) PutObject(_ context.Context, key string, body []byte) error {
	p := f.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, body, 0o644)
}

// deviceStatus builds a dimo.status raw event exactly as din stores it:
// the default-module signal payload verbatim.
func deviceStatus(id, subject string, ts time.Time, signals ...map[string]any) cloudevent.StoredEvent {
	payload, _ := json.Marshal(map[string]any{"signals": signals})
	return cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        cloudevent.TypeStatus,
			Subject:     subject,
			Source:      "0xConnLicense",
			Producer:    subject,
			ID:          id,
			Time:        ts,
			DataVersion: "default/v1.0",
		},
		Data: payload,
	}}
}

func speedAt(ts time.Time, v float64) map[string]any {
	return map[string]any{"name": "speed", "timestamp": ts.Format(time.RFC3339Nano), "value": v}
}

// writeRawBundle persists events the way din's sink does: hive partition
// from the bundle's event-time date, ingest-<ms>-<seq> naming (sorts like
// din's ULIDs), rows sorted by (subject,time), zstd, subject bloom filter.
func writeRawBundle(t *testing.T, store *fsStore, day time.Time, seq int, events ...cloudevent.StoredEvent) string {
	t.Helper()
	key := fmt.Sprintf("raw/type=dimo.status/date=%s/ingest-%013d-SEQ%04d.parquet",
		day.UTC().Format("2006-01-02"), 1749470000000+int64(seq), seq)
	var buf bytes.Buffer
	_, err := ceparquet.Encode(&buf, events, key,
		ceparquet.WithSortedRows(), ceparquet.WithZstdCompression(), ceparquet.WithSubjectBloomFilter())
	require.NoError(t, err)
	require.NoError(t, store.PutObject(context.Background(), key, buf.Bytes()))
	return key
}

func TestPipelineEndToEnd(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := &fsStore{root: root}

	// --- Stage 1: raw bundles land as din writes them. ---------------------
	day1 := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)

	a1 := day1.Add(10 * time.Hour)
	dupEvent := deviceStatus("a-dup", vehicleA, a1.Add(30*time.Minute), speedAt(a1.Add(30*time.Minute), 60))

	writeRawBundle(t, store, day1, 1,
		deviceStatus("a-1", vehicleA, a1, speedAt(a1, 50), speedAt(a1.Add(10*time.Minute), 70)),
		dupEvent,
	)
	// The same event again in a second bundle: at-least-once redelivery.
	writeRawBundle(t, store, day1, 2,
		dupEvent,
		deviceStatus("b-1", vehicleB, a1, speedAt(a1, 100)),
		// Non-vehicle subject: the gate must keep it out of decoded tables.
		deviceStatus("x-1", nonVehicle, a1, speedAt(a1, 999)),
	)
	b2 := day2.Add(9 * time.Hour)
	writeRawBundle(t, store, day2, 3,
		deviceStatus("a-2", vehicleA, b2, speedAt(b2, 90)),
	)

	// --- Stage 2: materializer decodes post fact. ---------------------------
	runner := materializer.New(materializer.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
	}, store, zerolog.Nop())

	processed, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Positive(t, processed)
	// Drain until idle (watermark caught up).
	for processed != 0 {
		processed, err = runner.RunOnce(ctx)
		require.NoError(t, err)
	}

	// Watermark published — the contract the din compactor gates on.
	wm, err := store.GetObject(ctx, "decoded/v1/_state/watermark.json")
	require.NoError(t, err)
	var cursor map[string]string
	require.NoError(t, json.Unmarshal(wm, &cursor))
	assert.Contains(t, cursor, "type=dimo.status/date=2026-06-08")
	assert.Contains(t, cursor, "type=dimo.status/date=2026-06-09")

	// --- Stage 3: DuckDB answers queries over the decoded layout. ----------
	svc, err := duck.NewService(duck.Config{S3Enabled: false})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	queries := duck.NewQueries(svc, root)

	// Aggregation: vehicle A speed over both days, single full-range bucket.
	// Values: 50, 70, 60 (dup collapsed to one), 90 → avg 67.5, max 90.
	from := day1
	to := day2.Add(24 * time.Hour)
	aggs, err := queries.GetAggregatedSignals(ctx, vehicleA, &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: vehicleA},
		FromTS:     from,
		ToTS:       to,
		Interval:   to.Sub(from).Microseconds(),
		FloatArgs: []model.FloatSignalArgs{
			{Name: "speed", Agg: model.FloatAggregationAvg},
			{Name: "speed", Agg: model.FloatAggregationMax},
		},
	})
	require.NoError(t, err)
	require.Len(t, aggs, 2)
	byIndex := map[uint16]float64{}
	for _, agg := range aggs {
		byIndex[agg.SignalIndex] = agg.ValueNumber
	}
	assert.InDelta(t, 67.5, byIndex[0], 1e-9, "avg speed: duplicate event must count once")
	assert.InDelta(t, 90.0, byIndex[1], 1e-9, "max speed")

	// Latest: vehicle A's newest speed and full-history lastSeen.
	latest, err := queries.GetLatestSignals(ctx, vehicleA, &model.LatestSignalsArgs{
		SignalArgs:      model.SignalArgs{Subject: vehicleA},
		SignalNames:     map[string]struct{}{"speed": {}},
		IncludeLastSeen: true,
	})
	require.NoError(t, err)
	got := map[string]float64{}
	var lastSeen time.Time
	for _, sig := range latest {
		if sig.Data.Name == model.LastSeenField {
			lastSeen = sig.Data.Timestamp
			continue
		}
		got[sig.Data.Name] = sig.Data.ValueNumber
	}
	assert.InDelta(t, 90.0, got["speed"], 1e-9, "latest speed is day-2 value")
	assert.True(t, lastSeen.Equal(b2), "lastSeen tracks the newest signal")

	// Available signals come from the summary buckets.
	available, err := queries.GetAvailableSignals(ctx, vehicleA, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"speed"}, available)

	// The non-vehicle subject never reached decoded tables...
	gateLatest, err := queries.GetLatestSignals(ctx, nonVehicle, &model.LatestSignalsArgs{
		SignalArgs:  model.SignalArgs{Subject: nonVehicle},
		SignalNames: map[string]struct{}{"speed": {}},
	})
	require.NoError(t, err)
	assert.Empty(t, gateLatest, "non-vehicle subjects are gated out of decoded tables")

	// ...but its raw cloudevent is still queryable: raw is source of truth.
	raw := duck.NewRaw(svc, root, "raw")
	rawEvents, err := raw.ListCloudEvents(ctx, duck.RawFilter{
		Subject: nonVehicle,
		After:   day1,
		Before:  to,
	}, 10)
	require.NoError(t, err)
	require.Len(t, rawEvents, 1)
	assert.Equal(t, "x-1", rawEvents[0].ID)
	assert.JSONEq(t,
		fmt.Sprintf(`{"signals":[{"name":"speed","timestamp":%q,"value":999}]}`, a1.Format(time.RFC3339Nano)),
		string(rawEvents[0].Data), "raw payload preserved verbatim for parse-on-read")

	// Raw scans collapse the duplicated event too.
	rawA, err := raw.ListCloudEvents(ctx, duck.RawFilter{Subject: vehicleA, After: day1, Before: to}, 10)
	require.NoError(t, err)
	ids := make([]string, len(rawA))
	for i, ev := range rawA {
		ids[i] = ev.ID
	}
	sort.Strings(ids)
	assert.Equal(t, []string{"a-1", "a-2", "a-dup"}, ids, "duplicate raw rows collapse on header key")

	// --- Stage 4: freshness — new raw data appears after the next poll. ----
	day3 := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	c3 := day3.Add(8 * time.Hour)
	writeRawBundle(t, store, day3, 4,
		deviceStatus("a-3", vehicleA, c3, speedAt(c3, 120)),
	)
	processed, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Positive(t, processed, "materializer picks up new raw files incrementally")

	latest2, err := queries.GetLatestSignals(ctx, vehicleA, &model.LatestSignalsArgs{
		SignalArgs:  model.SignalArgs{Subject: vehicleA},
		SignalNames: map[string]struct{}{"speed": {}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, latest2)
	assert.InDelta(t, 120.0, latest2[0].Data.ValueNumber, 1e-9, "new device data is queryable after one poll cycle")
}
