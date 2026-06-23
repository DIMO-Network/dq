// fixtures_test.go holds the raw-event fixtures shared across the test suite:
// the test vehicle DID, the din-shaped dimo.status event builder, and the raw
// bundle / filesystem-store helpers the bucket-materializer tests still use
// (chaos_test.go).
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/dq/internal/fsstore"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
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

// newFSStore opens the production filesystem store over root — the same files
// the bucket materializer reads/writes, so the store IS the bucket.
func newFSStore(t *testing.T, root string) *fsstore.Store {
	t.Helper()
	store, err := fsstore.New(root)
	require.NoError(t, err)
	return store
}

// writeRawBundle persists events the way din's sink does: hive partition
// from the bundle's event-time date, ingest-<ms>-<seq> naming (sorts like
// din's ULIDs), rows sorted by (subject,time), zstd, subject bloom filter.
func writeRawBundle(t *testing.T, store materializer.ObjectStore, day time.Time, seq int, events ...cloudevent.StoredEvent) string {
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
