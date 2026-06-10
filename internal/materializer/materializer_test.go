package materializer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	mgconvert "github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	pq "github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var baseTime = time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)

func newTestRunner(store ObjectStore) *Runner {
	return New(Config{}, store, zerolog.Nop())
}

// putRawFile encodes events into a raw cloudevent parquet object.
func putRawFile(t *testing.T, store *memStore, key string, events []cloudevent.StoredEvent) {
	t.Helper()
	var buf bytes.Buffer
	_, err := ceparquet.Encode(&buf, events, key)
	require.NoError(t, err)
	require.NoError(t, store.PutObject(context.Background(), key, buf.Bytes()))
}

func statusEvent(subject, source, id string, ts time.Time, payload string) cloudevent.StoredEvent {
	return cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: "1.0",
				Type:        cloudevent.TypeStatus,
				Subject:     subject,
				Source:      source,
				Producer:    "did:erc721:1:0xprod:1",
				ID:          id,
				Time:        ts,
				DataVersion: "default/v1.0",
			},
			Data: json.RawMessage(payload),
		},
	}
}

func eventsEvent(subject, source, id string, ts time.Time, payload string) cloudevent.StoredEvent {
	ev := statusEvent(subject, source, id, ts, payload)
	ev.Type = cloudevent.TypeEvents
	return ev
}

func rawKey(ceType, date, file string) string {
	return fmt.Sprintf("raw/type=%s/date=%s/%s.parquet", ceType, date, file)
}

func readSignalRows(t *testing.T, store *memStore, key string) []SignalRow {
	t.Helper()
	data, err := store.GetObject(context.Background(), key)
	require.NoError(t, err)
	rows, _, err := readParquet[SignalRow](data)
	require.NoError(t, err)
	return rows
}

func readManifest(t *testing.T, store *memStore, r *Runner, batchID string) batchManifest {
	t.Helper()
	data, err := store.GetObject(context.Background(), r.manifestKey(batchID))
	require.NoError(t, err)
	var m batchManifest
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

func readWatermark(t *testing.T, store *memStore, r *Runner) map[string]string {
	t.Helper()
	w, err := r.loadWatermark(context.Background())
	require.NoError(t, err)
	return w
}

// fakeSignalModule lets tests emit deterministic vss signals.
type fakeSignalModule struct {
	fn func(ev cloudevent.RawEvent) ([]vss.Signal, error)
}

func (f *fakeSignalModule) SignalConvert(_ context.Context, ev cloudevent.RawEvent) ([]vss.Signal, error) {
	return f.fn(ev)
}

func namedSignal(name string, ts time.Time, num float64) vss.Signal {
	return vss.Signal{Data: vss.SignalData{Name: name, Timestamp: ts, ValueNumber: num}}
}

func TestRunOnce_DefaultModuleSignals(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	ctx := context.Background()

	payload := fmt.Sprintf(`{"signals":[
		{"timestamp":%q,"name":"speed","value":55.5},
		{"timestamp":%q,"name":"notARealSignal","value":1}
	]}`, baseTime.Format(time.RFC3339), baseTime.Format(time.RFC3339))
	key := rawKey(cloudevent.TypeStatus, "2026-06-08", "ingest-1749384000000-01A")
	putRawFile(t, store, key, []cloudevent.StoredEvent{
		statusEvent("did:erc721:1:0xveh:1", "0xDefaultSrcTest", "ce-1", baseTime, payload),
	})

	r := newTestRunner(store)
	processed, err := r.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	// Decoded signal object: deterministic name from first raw file base +
	// batch hash, partitioned by signal timestamp date.
	batchID := computeBatchID([]string{key})
	wantKey := "decoded/v1/signals/date=2026-06-08/batch-ingest-1749384000000-01A-" + batchID[:16] + ".parquet"
	require.Equal(t, []string{wantKey}, store.keys("decoded/v1/signals/"))

	rows := readSignalRows(t, store, wantKey)
	require.Len(t, rows, 1, "bad signal must be dropped, good one salvaged")
	row := rows[0]
	assert.Equal(t, "did:erc721:1:0xveh:1", row.Subject)
	assert.Equal(t, "speed", row.Name)
	assert.Equal(t, "0xDefaultSrcTest", row.Source)
	assert.Equal(t, "did:erc721:1:0xprod:1", row.Producer)
	assert.Equal(t, "ce-1", row.CloudEventID)
	assert.Equal(t, 55.5, row.ValueNumber)
	assert.True(t, row.Timestamp.UTC().Equal(baseTime))

	// Bloom filter on subject and declared sorting columns.
	data, err := store.GetObject(ctx, wantKey)
	require.NoError(t, err)
	f, err := pq.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	rg := f.RowGroups()[0]
	require.NotEmpty(t, rg.SortingColumns())
	assert.Equal(t, []string{"subject"}, rg.SortingColumns()[0].Path())
	subjectCol, ok := f.Schema().Lookup("subject")
	require.True(t, ok)
	assert.NotNil(t, rg.ColumnChunks()[subjectCol.ColumnIndex].BloomFilter())

	// Manifest: salvage counted, conversion failure counted, watermark advanced.
	manifest := readManifest(t, store, r, batchID)
	assert.Equal(t, []string{key}, manifest.Inputs)
	assert.Equal(t, []string{wantKey}, manifest.Outputs)
	assert.Equal(t, 1, manifest.SignalCount)
	assert.Equal(t, 1, manifest.ErrorCount)
	assert.Equal(t, 0, manifest.EventCount)

	wm := readWatermark(t, store, r)
	assert.Equal(t, key, wm["type=dimo.status/date=2026-06-08"])

	// Nothing pending afterwards.
	processed, err = r.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, processed)
}

func TestRunOnce_CoordinateMergingAndPruning(t *testing.T) {
	t.Parallel()
	source := "0xCoordsTestSource"
	SignalRegistry.Override(source, &fakeSignalModule{fn: func(ev cloudevent.RawEvent) ([]vss.Signal, error) {
		return []vss.Signal{
			namedSignal(fieldCurrentLocationLatitude, baseTime, 40.7),
			namedSignal(fieldCurrentLocationLongitude, baseTime, -74.0),
			namedSignal(fieldDIMOAftermarketHDOP, baseTime, 1.5),
			namedSignal("speed", baseTime, 30),
			namedSignal("speed", baseTime, 30),                  // duplicate -> pruned
			namedSignal("speed", time.Now().Add(time.Hour), 99), // future -> pruned
			namedSignal("powertrainTransmissionTravelledDistance", baseTime, 1234),
		}, nil
	}})

	store := newMemStore()
	key := rawKey(cloudevent.TypeStatus, "2026-06-08", "ingest-1749384000001-01B")
	putRawFile(t, store, key, []cloudevent.StoredEvent{
		statusEvent("did:erc721:1:0xveh:2", source, "ce-2", baseTime, `{}`),
	})

	r := newTestRunner(store)
	_, err := r.RunOnce(context.Background())
	require.NoError(t, err)

	keys := store.keys("decoded/v1/signals/")
	require.Len(t, keys, 1)
	rows := readSignalRows(t, store, keys[0])

	byName := map[string][]SignalRow{}
	for _, row := range rows {
		byName[row.Name] = append(byName[row.Name], row)
	}
	// lat/lon/hdop merged into a single coordinates signal, originals pruned.
	require.Len(t, byName[vss.FieldCurrentLocationCoordinates], 1)
	loc := byName[vss.FieldCurrentLocationCoordinates][0]
	assert.Equal(t, 40.7, loc.LocLat)
	assert.Equal(t, -74.0, loc.LocLon)
	assert.Equal(t, 1.5, loc.LocHDOP)
	assert.Empty(t, byName[fieldCurrentLocationLatitude])
	assert.Empty(t, byName[fieldCurrentLocationLongitude])
	assert.Empty(t, byName[fieldDIMOAftermarketHDOP])
	// duplicate and future speeds pruned.
	require.Len(t, byName["speed"], 1)
	assert.Equal(t, 30.0, byName["speed"][0].ValueNumber)
	require.Len(t, byName["powertrainTransmissionTravelledDistance"], 1)
	assert.Len(t, rows, 3)
}

func TestRunOnce_PartialSalvageFromConversionError(t *testing.T) {
	t.Parallel()
	source := "0xSalvageTestSource"
	SignalRegistry.Override(source, &fakeSignalModule{fn: func(ev cloudevent.RawEvent) ([]vss.Signal, error) {
		return nil, &mgconvert.ConversionError{
			Errors:         []error{fmt.Errorf("boom on half the payload")},
			DecodedSignals: []vss.Signal{namedSignal("speed", baseTime, 88)},
		}
	}})

	store := newMemStore()
	key := rawKey(cloudevent.TypeStatus, "2026-06-08", "ingest-1749384000002-01C")
	putRawFile(t, store, key, []cloudevent.StoredEvent{
		statusEvent("did:erc721:1:0xveh:3", source, "ce-3", baseTime, `{}`),
	})

	r := newTestRunner(store)
	_, err := r.RunOnce(context.Background())
	require.NoError(t, err)

	keys := store.keys("decoded/v1/signals/")
	require.Len(t, keys, 1, "partial decode must still be written")
	rows := readSignalRows(t, store, keys[0])
	require.Len(t, rows, 1)
	assert.Equal(t, 88.0, rows[0].ValueNumber)

	manifest := readManifest(t, store, r, computeBatchID([]string{key}))
	assert.Equal(t, 1, manifest.SignalCount)
	assert.Equal(t, 1, manifest.ErrorCount)
	// The watermark must advance despite the conversion error.
	wm := readWatermark(t, store, r)
	assert.Equal(t, key, wm["type=dimo.status/date=2026-06-08"])
}

func TestRunOnce_Events(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	payload := fmt.Sprintf(`{"events":[
		{"name":"safety.collision","timestamp":%q,"durationNs":1500,"metadata":"{\"g\":4.2}","tags":["front"]},
		{"name":"not.a.real.event","timestamp":%q}
	]}`, baseTime.Format(time.RFC3339), baseTime.Format(time.RFC3339))
	key := rawKey(cloudevent.TypeEvents, "2026-06-08", "ingest-1749384000003-01D")
	putRawFile(t, store, key, []cloudevent.StoredEvent{
		eventsEvent("did:erc721:1:0xveh:4", "0xEventSrcTest", "ce-4", baseTime, payload),
	})

	r := newTestRunner(store)
	processed, err := r.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	keys := store.keys("decoded/v1/events/")
	require.Len(t, keys, 1)
	assert.Contains(t, keys[0], "decoded/v1/events/date=2026-06-08/batch-ingest-1749384000003-01D-")

	data, err := store.GetObject(context.Background(), keys[0])
	require.NoError(t, err)
	rows, _, err := readParquet[EventRow](data)
	require.NoError(t, err)
	require.Len(t, rows, 1, "unknown event name dropped, valid one salvaged")
	row := rows[0]
	assert.Equal(t, "did:erc721:1:0xveh:4", row.Subject)
	assert.Equal(t, "0xEventSrcTest", row.Source)
	assert.Equal(t, "did:erc721:1:0xprod:1", row.Producer)
	assert.Equal(t, "ce-4", row.CloudEventID)
	assert.Equal(t, cloudevent.TypeEvent, row.Type)
	assert.Equal(t, "default/v1.0", row.DataVersion)
	assert.Equal(t, "safety.collision", row.Name)
	assert.True(t, row.Timestamp.UTC().Equal(baseTime))
	assert.Equal(t, uint64(1500), row.DurationNs)
	assert.Equal(t, `{"g":4.2}`, row.Metadata)
	assert.Equal(t, []string{"front"}, row.Tags)

	manifest := readManifest(t, store, r, computeBatchID([]string{key}))
	assert.Equal(t, 1, manifest.EventCount)
	assert.Equal(t, 1, manifest.ErrorCount)
}

func TestLatestBucketRules(t *testing.T) {
	t.Parallel()
	source := "0xLatestTestSource"
	subject := "did:erc721:1:0xveh:5"
	t1 := baseTime
	t2 := baseTime.Add(time.Minute)
	t3 := baseTime.Add(2 * time.Minute)
	SignalRegistry.Override(source, &fakeSignalModule{fn: func(ev cloudevent.RawEvent) ([]vss.Signal, error) {
		return []vss.Signal{
			namedSignal("speed", t1, 10),
			namedSignal("speed", t2, 20),
			{Data: vss.SignalData{Name: vss.FieldCurrentLocationCoordinates, Timestamp: t1,
				ValueLocation: vss.Location{Latitude: 1.5, Longitude: 2.5, HDOP: 0.9, Heading: 270}}},
			// A (0,0) location arriving later: it wins the plain argMax
			// columns but must NOT touch the loc_*_nonzero columns.
			{Data: vss.SignalData{Name: vss.FieldCurrentLocationCoordinates, Timestamp: t3,
				ValueLocation: vss.Location{Latitude: 0, Longitude: 0}}},
		}, nil
	}})

	store := newMemStore()
	key := rawKey(cloudevent.TypeStatus, "2026-06-08", "ingest-1749384000004-01E")
	putRawFile(t, store, key, []cloudevent.StoredEvent{
		statusEvent(subject, source, "ce-5", baseTime, `{}`),
	})

	r := newTestRunner(store)
	_, err := r.RunOnce(context.Background())
	require.NoError(t, err)

	bucket := hashBucket(subject)
	rows, stamp, err := loadBucket[LatestRow](context.Background(), store, r.latestBucketKey(bucket))
	require.NoError(t, err)
	assert.Equal(t, computeBatchID([]string{key}), stamp)

	byName := map[string]LatestRow{}
	for _, row := range rows {
		require.Equal(t, subject, row.Subject)
		require.Equal(t, source, row.Source)
		byName[row.Name] = row
	}
	require.Len(t, rows, 3) // speed, coordinates, lastSeen

	speed := byName["speed"]
	assert.Equal(t, 20.0, speed.ValueNumber)
	assert.True(t, speed.Timestamp.UTC().Equal(t2))

	coords := byName[vss.FieldCurrentLocationCoordinates]
	// Plain argMax columns: the latest row, even at (0, 0).
	assert.True(t, coords.Timestamp.UTC().Equal(t3))
	assert.Equal(t, 0.0, coords.LocLat)
	assert.Equal(t, 0.0, coords.LocLon)
	// latestLocationCond replica: nonzero columns hold the t1 fix.
	assert.Equal(t, 1.5, coords.LocLatNonzero)
	assert.Equal(t, 2.5, coords.LocLonNonzero)
	assert.Equal(t, 0.9, coords.LocHDOPNonzero)
	assert.Equal(t, 270.0, coords.LocHeadingNonzero)
	assert.True(t, coords.LocNonzeroTS.UTC().Equal(t1))

	// Virtual lastSeen row: max timestamp across all names.
	lastSeen := byName[lastSeenFieldName]
	assert.True(t, lastSeen.Timestamp.UTC().Equal(t3))
	assert.Zero(t, lastSeen.ValueNumber)
	assert.Empty(t, lastSeen.ValueString)
}

func TestSummaryAccumulatesAcrossBatches(t *testing.T) {
	t.Parallel()
	source := "0xSummaryTestSource"
	subject := "did:erc721:1:0xveh:6"
	SignalRegistry.Override(source, &fakeSignalModule{fn: func(ev cloudevent.RawEvent) ([]vss.Signal, error) {
		return []vss.Signal{
			namedSignal("speed", ev.Time, 1),
			namedSignal("speed", ev.Time.Add(time.Second), 2),
		}, nil
	}})

	store := newMemStore()
	ctx := context.Background()
	r := newTestRunner(store)

	key1 := rawKey(cloudevent.TypeStatus, "2026-06-08", "ingest-1749384000005-01F")
	putRawFile(t, store, key1, []cloudevent.StoredEvent{statusEvent(subject, source, "ce-6", baseTime, `{}`)})
	_, err := r.RunOnce(ctx)
	require.NoError(t, err)

	key2 := rawKey(cloudevent.TypeStatus, "2026-06-08", "ingest-1749384000006-01G")
	putRawFile(t, store, key2, []cloudevent.StoredEvent{statusEvent(subject, source, "ce-7", baseTime.Add(time.Hour), `{}`)})
	processed, err := r.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed, "only the new file is pending")

	rows, _, err := loadBucket[SummaryRow](ctx, store, r.summaryBucketKey(hashBucket(subject)))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row := rows[0]
	assert.Equal(t, subject, row.Subject)
	assert.Equal(t, source, row.Source)
	assert.Equal(t, "speed", row.Name)
	assert.Equal(t, uint64(4), row.Count)
	assert.True(t, row.FirstSeen.UTC().Equal(baseTime))
	assert.True(t, row.LastSeen.UTC().Equal(baseTime.Add(time.Hour+time.Second)))

	wm := readWatermark(t, store, r)
	assert.Equal(t, key2, wm["type=dimo.status/date=2026-06-08"])
}

func TestRunOnce_BatchMaxFiles(t *testing.T) {
	t.Parallel()
	source := "0xBatchMaxTestSource"
	SignalRegistry.Override(source, &fakeSignalModule{fn: func(ev cloudevent.RawEvent) ([]vss.Signal, error) {
		return []vss.Signal{namedSignal("speed", ev.Time, 1)}, nil
	}})

	store := newMemStore()
	ctx := context.Background()
	for i := range 3 {
		key := rawKey(cloudevent.TypeStatus, "2026-06-08", fmt.Sprintf("ingest-174938400100%d-01H", i))
		putRawFile(t, store, key, []cloudevent.StoredEvent{
			statusEvent("did:erc721:1:0xveh:7", source, fmt.Sprintf("ce-8-%d", i), baseTime, `{}`),
		})
	}

	r := New(Config{BatchMaxFiles: 2}, store, zerolog.Nop())
	processed, err := r.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, processed)
	processed, err = r.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)
	processed, err = r.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, processed)
}

func TestRunOnce_EmptyStore(t *testing.T) {
	t.Parallel()
	r := newTestRunner(newMemStore())
	processed, err := r.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, processed)
}
