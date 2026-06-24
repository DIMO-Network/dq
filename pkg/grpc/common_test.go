package grpc

import (
	"testing"

	"github.com/DIMO-Network/cloudevent"
)

// CloudEventToProto ↔ AsRawCloudEvent must round-trip the payload in BOTH
// representations: Data (decoded, e.g. JSON) and DataBase64 (inline binary). The
// proto carries a data_base64 field so an inline base64 payload isn't silently
// dropped on the fetch RPCs — it was, before the field existed (the proto had only
// `bytes data`, so CloudEventToProto never carried DataBase64).
func TestCloudEventProtoRoundTrip_Payload(t *testing.T) {
	cases := []struct {
		name       string
		data       []byte
		dataBase64 string
	}{
		{"decoded data", []byte(`{"speed":42}`), ""},
		{"inline base64 binary", nil, "YmluYXJ5LXBheWxvYWQ="},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := cloudevent.RawEvent{
				CloudEventHeader: cloudevent.CloudEventHeader{ID: "id-1", Subject: "subj"},
				Data:             tc.data,
				DataBase64:       tc.dataBase64,
			}
			out := CloudEventToProto(in).AsRawCloudEvent()
			if string(out.Data) != string(tc.data) {
				t.Errorf("Data: got %q want %q", out.Data, tc.data)
			}
			if out.DataBase64 != tc.dataBase64 {
				t.Errorf("DataBase64: got %q want %q", out.DataBase64, tc.dataBase64)
			}
			if out.ID != in.ID {
				t.Errorf("ID: got %q want %q", out.ID, in.ID)
			}
		})
	}
}
