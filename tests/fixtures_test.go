// fixtures_test.go holds the raw-event fixtures shared across the test suite:
// the test vehicle DID and the din-shaped dimo.status event builder.
package tests

import (
	"encoding/json"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/ethereum/go-ethereum/common"
)

// vehicleNFT is the vehicle-NFT contract on the test chain (137). Subjects
// derive from it as did:erc721:137:<vehicleNFT>:<tokenId>.
var vehicleNFT = common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF")

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
