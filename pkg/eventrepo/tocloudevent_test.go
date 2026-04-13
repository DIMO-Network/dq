package eventrepo

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToCloudEvent_DataBase64(t *testing.T) {
	t.Parallel()
	// Stored object has data_base64 (no "data"). We never decode into Data; DataBase64 is preserved for round-trip.
	b64 := base64.StdEncoding.EncodeToString([]byte("binary\x00payload"))
	ceJSON := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "` + time.Now().UTC().Format(time.RFC3339Nano) + `",
		"type": "dimo.status",
		"data_base64": "` + b64 + `"
	}`)
	hdr := &cloudevent.CloudEventHeader{
		ID: "ev-1", Source: "test", Producer: "p", Subject: "sub",
		Time: time.Now().UTC(), Type: cloudevent.TypeStatus,
	}
	got, err := toCloudEvent(hdr, ceJSON)
	require.NoError(t, err)
	assert.Nil(t, got.Data, "never decode data_base64 into Data")
	assert.Equal(t, b64, got.DataBase64, "data_base64 preserved for round-trip")
	assert.Equal(t, hdr.ID, got.ID)

	// Round-trip: marshaling must emit data_base64 so it is not lost.
	marshaled, err := got.MarshalJSON()
	require.NoError(t, err)
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(marshaled, &out))
	require.Contains(t, out, "data_base64")
	assert.Equal(t, `"`+b64+`"`, string(out["data_base64"]), "data_base64 preserved in serialization")
}

func TestToCloudEvent_DataOnly(t *testing.T) {
	t.Parallel()
	ceJSON := []byte(`{"id":"e2","source":"s","producer":"p","subject":"sub","time":"2024-01-01T00:00:00Z","type":"dimo.status","data":{"x":1}}`)
	hdr := &cloudevent.CloudEventHeader{
		ID: "e2", Source: "s", Producer: "p", Subject: "sub",
		Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Type: cloudevent.TypeStatus,
	}
	got, err := toCloudEvent(hdr, ceJSON)
	require.NoError(t, err)
	require.NotNil(t, got.Data)
	var m map[string]any
	require.NoError(t, json.Unmarshal(got.Data, &m))
	assert.Equal(t, float64(1), m["x"])
}

// TestToCloudEvent_DataPassthroughNoParse verifies that the "data" payload from S3
// is not parsed and re-serialized: we extract it with gjson and it is written raw
// in RawCloudEvent.MarshalJSON via sjson.SetRawBytes. So the exact bytes from the
// stored object's "data" field should appear unchanged in the marshaled output.
func TestToCloudEvent_DataPassthroughNoParse(t *testing.T) {
	t.Parallel()
	// Exact byte sequence that would often change if unmarshaled and re-marshaled
	// (e.g. key order, number formatting). We assert it appears verbatim in output.
	exactData := []byte(`{"order":"matters","num": 42 , "key":"value"}`)
	ceJSON := []byte(`{"id":"e3","source":"s","producer":"p","subject":"sub","time":"2024-01-01T00:00:00Z","type":"dimo.status","data":` + string(exactData) + `}`)
	hdr := &cloudevent.CloudEventHeader{
		ID: "e3", Source: "s", Producer: "p", Subject: "sub",
		Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Type: cloudevent.TypeStatus,
	}
	raw, err := toCloudEvent(hdr, ceJSON)
	require.NoError(t, err)
	require.NotNil(t, raw.Data)
	assert.Equal(t, exactData, []byte(raw.Data), "data from S3 should be passed through without parse; gjson extracts raw substring")

	// Marshal as we do for GraphQL; the "data" field in output must be exactly exactData.
	marshaled, err := raw.MarshalJSON()
	require.NoError(t, err)
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(marshaled, &out))
	require.Contains(t, out, "data")
	assert.Equal(t, exactData, []byte(out["data"]), "MarshalJSON must write data via SetRawBytes, not re-encode")
}
