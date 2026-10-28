package materializer

import (
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

// TestIsVehicleSignalMessage_DIDDimo pins that a did:dimo vehicle, which din
// admits only from a source with a write grant, decodes alongside vehicle
// NFTs on the configured chain.
func TestIsVehicleSignalMessage_DIDDimo(t *testing.T) {
	nft := common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF")
	r := &Runner{cfg: Config{ChainID: 137, VehicleNFTAddress: nft}}
	for subject, want := range map[string]bool{
		"did:dimo:EiBn2rTVzTD3ql9H6hDW2a1X7Yoo8L7ToQFmVoOKcDuGrA":        true,
		"did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42":   true,
		"did:erc721:80002:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42": false,
		"did:erc721:137:0x0000000000000000000000000000000000000001:42":   false,
		"did:ethr:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF":        false,
	} {
		ev := &cloudevent.RawEvent{CloudEventHeader: cloudevent.CloudEventHeader{Subject: subject}}
		require.Equal(t, want, r.isVehicleSignalMessage(ev), subject)
	}
}
