package materializer

import (
	"bytes"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// TestWriteSignalParquet_OutputSortedAfterDroppingMetadataSort pins Item 10: the
// explicit slices.SortFunc is the ONLY thing ordering the rows now that the
// metadata-only SortingWriterConfig is gone (parquet-go's GenericWriter never
// reorders). Read the encoded parquet back and confirm the rows are still in
// (subject, name, timestamp) order.
func TestWriteSignalParquet_OutputSortedAfterDroppingMetadataSort(t *testing.T) {
	t1 := time.Unix(1000, 0).UTC()
	t2 := time.Unix(2000, 0).UTC()
	t3 := time.Unix(3000, 0).UTC()

	// Deliberately unsorted across all three keys.
	rows := []SignalRow{
		{Subject: "b", Name: "speed", Timestamp: t2},
		{Subject: "a", Name: "speed", Timestamp: t3},
		{Subject: "a", Name: "rpm", Timestamp: t1},
		{Subject: "a", Name: "speed", Timestamp: t1},
	}
	body, err := writeSignalParquet(rows)
	require.NoError(t, err)

	got, err := pq.Read[SignalRow](bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.Len(t, got, 4)

	for i := 1; i < len(got); i++ {
		prev, cur := got[i-1], got[i]
		ordered := prev.Subject < cur.Subject ||
			(prev.Subject == cur.Subject && prev.Name < cur.Name) ||
			(prev.Subject == cur.Subject && prev.Name == cur.Name && !cur.Timestamp.Before(prev.Timestamp))
		require.Truef(t, ordered, "parquet output not sorted at %d: %+v then %+v", i, prev, cur)
	}
	require.Equal(t, "a", got[0].Subject)
	require.Equal(t, "rpm", got[0].Name)
	require.Equal(t, "b", got[3].Subject)
}
