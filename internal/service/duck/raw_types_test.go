package duck

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// knownTypePayloads is one realistic fixture per cloudevent type constant the
// platform defines. Every type the fetch surface can store must round-trip
// through the raw layer verbatim; a new constant in the cloudevent module
// shows up as a failure in TestRaw_AllKnownTypesAreCovered.
var knownTypePayloads = map[string]string{
	cloudevent.TypeStatus:               `{"signals":[{"name":"speed","value":42.5}]}`,
	cloudevent.TypeRawStatus:            `{"frame":"02410D"}`,
	cloudevent.TypeFingerprint:          `{"vin":"1HGCM82633A004352","protocol":"6"}`,
	cloudevent.TypeVerifableCredential:  `{"credentialSubject":{"vin":"1HGCM82633A004352"}}`,
	cloudevent.TypeAttestation:          `{"claim":"odometer","value":123456}`,
	cloudevent.TypeAttestationTombstone: `{"reason":"superseded"}`,
	cloudevent.TypeUnknown:              `{"anything":"goes"}`,
	cloudevent.TypeSignals:              `{"signals":[{"name":"speed","timestamp":"2026-06-01T00:00:00Z","valueNumber":12}]}`,
	cloudevent.TypeSignal:               `{"name":"speed","valueNumber":12}`,
	cloudevent.TypeEvents:               `{"events":[{"name":"harshBraking","durationNs":1200}]}`,
	cloudevent.TypeEvent:                `{"name":"harshBraking","durationNs":1200}`,
	cloudevent.TypeTrigger:              `{"webhookId":"wh_1","signal":"speed"}`,
	cloudevent.TypeSACD:                 `{"grantee":"0xDEV","permissions":"0x3F"}`,
	cloudevent.TypeSACDTemplate:         `{"templateId":"tmpl_1"}`,
}

// allKnownTypes mirrors every Type* constant in the cloudevent module.
var allKnownTypes = []string{
	cloudevent.TypeStatus,
	cloudevent.TypeRawStatus,
	cloudevent.TypeFingerprint,
	cloudevent.TypeVerifableCredential,
	cloudevent.TypeAttestation,
	cloudevent.TypeAttestationTombstone,
	cloudevent.TypeUnknown,
	cloudevent.TypeSignals,
	cloudevent.TypeSignal,
	cloudevent.TypeEvents,
	cloudevent.TypeEvent,
	cloudevent.TypeTrigger,
	cloudevent.TypeSACD,
	cloudevent.TypeSACDTemplate,
}

func TestRaw_AllKnownTypesAreCovered(t *testing.T) {
	t.Parallel()
	require.Len(t, knownTypePayloads, len(allKnownTypes))
	for _, ceType := range allKnownTypes {
		assert.Contains(t, knownTypePayloads, ceType)
	}
}

// writeAllKnownTypes stores one event per known type, each one minute apart
// (newest = allKnownTypes[len-1]), and returns the events keyed by type.
func writeAllKnownTypes(t *testing.T, root string, base time.Time) map[string]cloudevent.StoredEvent {
	t.Helper()
	events := make(map[string]cloudevent.StoredEvent, len(allKnownTypes))
	for i, ceType := range allKnownTypes {
		ts := base.Add(time.Duration(i) * time.Minute)
		ev := rawStored(fmt.Sprintf("ev-%s", ceType), ceType, rawTestSubject, ts, knownTypePayloads[ceType])
		ev.DataVersion = "v1/" + ceType
		writeRawBundle(t, root, ceType, ts.Format("2006-01-02"), "b-"+ceType, ev)
		events[ceType] = ev
	}
	return events
}

// TestRaw_EveryKnownType_ListAndLatest drives ListCloudEvents and
// LatestCloudEvent for every cloudevent type constant and asserts the stored
// event comes back verbatim: header fields, data version, and payload.
func TestRaw_EveryKnownType_ListAndLatest(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	base := time.Now().UTC().Truncate(time.Millisecond).Add(-2 * time.Hour)
	want := writeAllKnownTypes(t, root, base)
	ctx := context.Background()

	for _, ceType := range allKnownTypes {
		t.Run(ceType, func(t *testing.T) {
			expected := want[ceType]

			listed, err := raw.ListCloudEvents(ctx, RawFilter{Subject: rawTestSubject, Types: []string{ceType}}, 10)
			require.NoError(t, err)
			require.Len(t, listed, 1, "type filter isolates the one event of this type")
			got := listed[0]
			assert.Equal(t, expected.ID, got.ID)
			assert.Equal(t, ceType, got.Type)
			assert.Equal(t, rawTestSubject, got.Subject)
			assert.Equal(t, expected.Source, got.Source)
			assert.Equal(t, expected.Producer, got.Producer)
			assert.Equal(t, expected.DataVersion, got.DataVersion)
			assert.True(t, expected.Time.Equal(got.Time), "time round-trips: want %s got %s", expected.Time, got.Time)
			assert.JSONEq(t, knownTypePayloads[ceType], string(got.Data), "payload verbatim")

			latest, err := raw.LatestCloudEvent(ctx, RawFilter{Subject: rawTestSubject, Types: []string{ceType}})
			require.NoError(t, err)
			assert.Equal(t, expected.ID, latest.ID)
			assert.JSONEq(t, knownTypePayloads[ceType], string(latest.Data))
		})
	}
}

// TestRaw_EveryKnownType_MixedScan lists across all types at once: newest
// first, no type leakage, and the full unfiltered set.
func TestRaw_EveryKnownType_MixedScan(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	base := time.Now().UTC().Truncate(time.Millisecond).Add(-2 * time.Hour)
	writeAllKnownTypes(t, root, base)
	ctx := context.Background()

	all, err := raw.ListCloudEvents(ctx, RawFilter{Subject: rawTestSubject}, len(allKnownTypes)+5)
	require.NoError(t, err)
	require.Len(t, all, len(allKnownTypes), "unfiltered scan returns every type once")
	for i, ev := range all {
		wantType := allKnownTypes[len(allKnownTypes)-1-i]
		assert.Equal(t, wantType, ev.Type, "newest first across type partitions (index %d)", i)
	}

	latest, err := raw.LatestCloudEvent(ctx, RawFilter{Subject: rawTestSubject})
	require.NoError(t, err)
	assert.Equal(t, allKnownTypes[len(allKnownTypes)-1], latest.Type, "untyped latest is the newest event overall")

	pair, err := raw.ListCloudEvents(ctx, RawFilter{
		Subject: rawTestSubject,
		Types:   []string{cloudevent.TypeStatus, cloudevent.TypeFingerprint},
	}, 10)
	require.NoError(t, err)
	require.Len(t, pair, 2, "multi-type filter")
}

// TestRaw_EveryKnownType_Available asserts AvailableCloudEventTypes reports
// the complete sorted set when every known type is stored.
func TestRaw_EveryKnownType_Available(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	base := time.Now().UTC().Truncate(time.Millisecond).Add(-2 * time.Hour)
	writeAllKnownTypes(t, root, base)

	types, err := raw.AvailableCloudEventTypes(context.Background(), rawTestSubject, base.Add(-time.Hour), base.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, types, len(allKnownTypes))
	for _, ceType := range allKnownTypes {
		assert.Contains(t, types, ceType)
	}
	assert.IsIncreasing(t, types, "distinct types come back sorted")
}

// TestRaw_TombstoneVoidsIDRoundTrip covers the tombstone-specific column:
// VoidsID names the attestation a dimo.tombstone voids and must survive the
// parquet round trip for read-side voiding.
func TestRaw_TombstoneVoidsIDRoundTrip(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	ev := rawStored("e-tomb", cloudevent.TypeAttestationTombstone, rawTestSubject, now.Add(-time.Hour), `{"reason":"superseded"}`)
	ev.VoidsID = "ev-dimo.attestation"
	writeRawBundle(t, root, cloudevent.TypeAttestationTombstone, now.Format("2006-01-02"), "b1", ev)

	got, err := raw.ListCloudEvents(context.Background(), RawFilter{Subject: rawTestSubject, Types: []string{cloudevent.TypeAttestationTombstone}}, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "ev-dimo.attestation", got[0].VoidsID)
}

// TestRaw_DataBase64RoundTrip covers binary payloads (data_base64 wire form),
// which any type may carry instead of JSON data.
func TestRaw_DataBase64RoundTrip(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	ev := rawStored("e-b64", cloudevent.TypeRawStatus, rawTestSubject, now.Add(-time.Hour), "")
	ev.DataBase64 = base64.StdEncoding.EncodeToString([]byte{0x02, 0x41, 0x0D, 0xFF})
	writeRawBundle(t, root, cloudevent.TypeRawStatus, now.Format("2006-01-02"), "b1", ev)

	got, err := raw.ListCloudEvents(context.Background(), RawFilter{Subject: rawTestSubject, Types: []string{cloudevent.TypeRawStatus}}, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, ev.DataBase64, got[0].DataBase64)
	assert.Empty(t, got[0].Data)
}

// TestRaw_EveryKnownType_SourceProducerIDFilters spot-checks the remaining
// RawFilter axes against the all-types corpus.
func TestRaw_EveryKnownType_SourceProducerIDFilters(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	d0 := now.Format("2006-01-02")

	other := rawStored("e-src", cloudevent.TypeStatus, rawTestSubject, now.Add(-time.Hour), `{"v":1}`)
	other.Source = "0xOtherConn"
	other.Producer = "did:erc721:137:0xAfter:9"
	writeRawBundle(t, root, cloudevent.TypeStatus, d0, "b1",
		rawStored("e-base", cloudevent.TypeStatus, rawTestSubject, now.Add(-2*time.Hour), `{"v":0}`),
		other)
	ctx := context.Background()

	bySource, err := raw.ListCloudEvents(ctx, RawFilter{Subject: rawTestSubject, Sources: []string{"0xOtherConn"}}, 10)
	require.NoError(t, err)
	require.Len(t, bySource, 1)
	assert.Equal(t, "e-src", bySource[0].ID)

	byProducer, err := raw.ListCloudEvents(ctx, RawFilter{Subject: rawTestSubject, Producers: []string{"did:erc721:137:0xAfter:9"}}, 10)
	require.NoError(t, err)
	require.Len(t, byProducer, 1)
	assert.Equal(t, "e-src", byProducer[0].ID)

	byID, err := raw.ListCloudEvents(ctx, RawFilter{Subject: rawTestSubject, IDs: []string{"e-base"}}, 10)
	require.NoError(t, err)
	require.Len(t, byID, 1)
	assert.Equal(t, "e-base", byID[0].ID)
}
