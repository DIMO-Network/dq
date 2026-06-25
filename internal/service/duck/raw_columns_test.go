package duck

import (
	"reflect"
	"strings"
	"testing"

	"github.com/DIMO-Network/cloudevent/parquet"
	"github.com/stretchr/testify/require"
)

// TestRawEventColumnsMatchParquetRow pins the raw_events column contract. The
// RawEventColumns projection is read POSITIONALLY by scanStoredEvent (here) and the
// materializer's scanRawEvent, and written POSITIONALLY by din's fillRowArgs against
// din's DDL. The canonical, name-tagged source of truth for all of those is
// cloudevent/parquet.ParquetRow (din backfills legacy parquet through it). If a column
// is added or two same-typed columns are reordered on one side only, every positional
// Scan still succeeds but reads values into the wrong field — silent corruption, not a
// crash, and the din↔dq repo split means din's change can't fail dq's compile. This
// test converts that drift into a red build: keep RawEventColumns, ParquetRow, both
// scanners' Scan lists, and din's DDL/fillRowArgs in lockstep.
func TestRawEventColumnsMatchParquetRow(t *testing.T) {
	rt := reflect.TypeOf(parquet.ParquetRow{})
	want := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		// parquet tags look like "name", "name,optional", "time,timestamp(millisecond)".
		name, _, _ := strings.Cut(rt.Field(i).Tag.Get("parquet"), ",")
		want = append(want, name)
	}

	got := make([]string, 0, len(want))
	for _, c := range strings.Split(RawEventColumns, ",") {
		// RawEventColumns quotes the "time" keyword; strip quotes + spacing.
		got = append(got, strings.Trim(strings.TrimSpace(c), `"`))
	}

	require.Equal(t, want, got,
		"raw_events column contract drifted: RawEventColumns (scanned positionally by "+
			"scanStoredEvent + the materializer's scanRawEvent) must equal "+
			"cloudevent/parquet.ParquetRow's tags in struct order. If you changed one, also "+
			"update the other, both scanners' Scan() argument order, and din's DDL + fillRowArgs.")
}
