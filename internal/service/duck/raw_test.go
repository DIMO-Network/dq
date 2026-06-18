package duck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rawTestSubject = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42"

func TestWhereClauseTagsAndDataVersion(t *testing.T) {
	where, args := whereClause(RawFilter{
		Subject:      "did:1",
		DataVersions: []string{"v1"},
		Tags:         []string{"a", "b"},
	})
	require.Contains(t, where, "data_version IN")
	require.Contains(t, where, "list_has_any") // tags JSON array overlap
	require.Contains(t, args, "did:1")
	require.Contains(t, args, "v1")
	require.Contains(t, args, "a")
	require.Contains(t, args, "b")
}

func TestWhereClauseQ_PrefixQualifiesColumns(t *testing.T) {
	where, args := whereClauseQ(RawFilter{
		Subject: "did:subj",
		Types:   []string{"dimo.status"},
	}, "e.")
	require.Contains(t, where, "e.subject = ?")
	require.Contains(t, where, "e.type IN")
	require.Contains(t, args, "did:subj")
	require.Contains(t, args, "dimo.status")
}

func rawStored(id, ceType, subject string, ts time.Time, data string) cloudevent.StoredEvent {
	ev := cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        ceType,
			Subject:     subject,
			Source:      "0xConn",
			Producer:    subject,
			ID:          id,
			Time:        ts,
		},
	}}
	if data != "" {
		ev.Data = json.RawMessage(data)
	}
	return ev
}

// writeRawBundle encodes events into the hive layout under root.
func writeRawBundle(t *testing.T, root, ceType, date, name string, events ...cloudevent.StoredEvent) {
	t.Helper()
	dir := filepath.Join(root, "raw", "type="+ceType, "date="+date)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	var buf bytes.Buffer
	_, err := ceparquet.Encode(&buf, events, name)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".parquet"), buf.Bytes(), 0o644))
}

func newRawFixture(t *testing.T) (*Raw, string) {
	t.Helper()
	root := t.TempDir()
	svc, err := NewService(Config{S3Enabled: false})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	return NewRaw(svc, root, "raw"), root
}

func TestRaw_ListCloudEvents(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	d0 := now.Format("2006-01-02")
	d1 := now.AddDate(0, 0, -1).Format("2006-01-02")

	writeRawBundle(t, root, "dimo.status", d1, "b1",
		rawStored("e1", "dimo.status", rawTestSubject, now.AddDate(0, 0, -1), `{"v":1}`))
	writeRawBundle(t, root, "dimo.status", d0, "b2",
		rawStored("e2", "dimo.status", rawTestSubject, now.Add(-time.Hour), `{"v":2}`),
		rawStored("e3", "dimo.status", "did:erc721:137:0xOther:1", now.Add(-time.Hour), `{"v":3}`))
	writeRawBundle(t, root, "dimo.fingerprint", d0, "b3",
		rawStored("e4", "dimo.fingerprint", rawTestSubject, now.Add(-30*time.Minute), `{"vin":"X"}`))

	ctx := context.Background()

	all, err := raw.ListCloudEvents(ctx, RawFilter{Subject: rawTestSubject}, 10)
	require.NoError(t, err)
	require.Len(t, all, 3, "other subject excluded")
	assert.Equal(t, "e4", all[0].ID, "newest first")
	assert.Equal(t, "e2", all[1].ID)
	assert.Equal(t, "e1", all[2].ID)
	assert.JSONEq(t, `{"v":2}`, string(all[1].Data), "data inline")

	statusOnly, err := raw.ListCloudEvents(ctx, RawFilter{Subject: rawTestSubject, Types: []string{"dimo.status"}}, 10)
	require.NoError(t, err)
	require.Len(t, statusOnly, 2)

	limited, err := raw.ListCloudEvents(ctx, RawFilter{Subject: rawTestSubject}, 1)
	require.NoError(t, err)
	require.Len(t, limited, 1)
	assert.Equal(t, "e4", limited[0].ID)
}

func TestRaw_DuplicateRowsCollapse(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	d0 := now.Format("2006-01-02")
	dup := rawStored("e-dup", "dimo.status", rawTestSubject, now.Add(-time.Hour), `{"v":1}`)

	// Same event in two bundles: ingest redelivery during compaction grace.
	writeRawBundle(t, root, "dimo.status", d0, "b1", dup)
	writeRawBundle(t, root, "dimo.status", d0, "b2", dup)

	events, err := raw.ListCloudEvents(context.Background(), RawFilter{Subject: rawTestSubject}, 10)
	require.NoError(t, err)
	require.Len(t, events, 1, "duplicates collapse on header key")
}

func TestRaw_LatestCloudEventWalksBack(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	// Only data point is 30 days old — walker must find it.
	old := now.AddDate(0, 0, -30)
	writeRawBundle(t, root, "dimo.status", old.Format("2006-01-02"), "b1",
		rawStored("e-old", "dimo.status", rawTestSubject, old, `{"v":9}`))

	ev, err := raw.LatestCloudEvent(context.Background(), RawFilter{Subject: rawTestSubject})
	require.NoError(t, err)
	assert.Equal(t, "e-old", ev.ID)

	_, err = raw.LatestCloudEvent(context.Background(), RawFilter{Subject: "did:erc721:137:0xNobody:1"})
	require.Error(t, err, "unknown subject errors instead of scanning forever")
}

func TestRaw_AvailableCloudEventTypes(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	d0 := now.Format("2006-01-02")
	writeRawBundle(t, root, "dimo.status", d0, "b1",
		rawStored("e1", "dimo.status", rawTestSubject, now.Add(-time.Hour), `{"v":1}`))
	writeRawBundle(t, root, "dimo.fingerprint", d0, "b2",
		rawStored("e2", "dimo.fingerprint", rawTestSubject, now.Add(-time.Hour), `{"vin":"X"}`))
	writeRawBundle(t, root, "dimo.attestation", d0, "b3",
		rawStored("e3", "dimo.attestation", "did:erc721:137:0xOther:1", now.Add(-time.Hour), `{}`))

	types, err := raw.AvailableCloudEventTypes(context.Background(), rawTestSubject, now.AddDate(0, 0, -2), now)
	require.NoError(t, err)
	assert.Equal(t, []string{"dimo.fingerprint", "dimo.status"}, types)
}

func TestRaw_ExtrasAndBlobRefsRoundTrip(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	d0 := now.Format("2006-01-02")

	ev := rawStored("e-extras", "dimo.attestation", rawTestSubject, now.Add(-time.Hour), "")
	ev.Signature = "0xsig"
	ev.Extras = map[string]any{"custom": "value"}
	ev.DataIndexKey = "cloudevent/blobs/" + rawTestSubject + "/2026/06/10/blob1"
	writeRawBundle(t, root, "dimo.attestation", d0, "b1", ev)

	got, err := raw.ListCloudEvents(context.Background(), RawFilter{Subject: rawTestSubject}, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "0xsig", got[0].Signature, "signature restored from extras")
	assert.Equal(t, "value", got[0].Extras["custom"])
	assert.Equal(t, ev.DataIndexKey, got[0].DataIndexKey, "blob reference preserved")
	assert.Empty(t, got[0].Data)
}

func TestRaw_MissingPartitionsReturnEmpty(t *testing.T) {
	t.Parallel()
	raw, _ := newRawFixture(t)
	events, err := raw.ListCloudEvents(context.Background(), RawFilter{Subject: rawTestSubject}, 10)
	require.NoError(t, err)
	assert.Empty(t, events)
}

func TestRaw_TimeRangeFilter(t *testing.T) {
	t.Parallel()
	raw, root := newRawFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	for i := range 3 {
		ts := now.AddDate(0, 0, -i).Add(-time.Hour)
		writeRawBundle(t, root, "dimo.status", ts.Format("2006-01-02"), fmt.Sprintf("b%d", i),
			rawStored(fmt.Sprintf("e%d", i), "dimo.status", rawTestSubject, ts, `{"v":1}`))
	}

	events, err := raw.ListCloudEvents(context.Background(), RawFilter{
		Subject: rawTestSubject,
		After:   now.AddDate(0, 0, -1).Add(-2 * time.Hour),
	}, 10)
	require.NoError(t, err)
	require.Len(t, events, 2, "only events inside [after, now]")
}
