package duck

import (
	"testing"

	"github.com/DIMO-Network/cloudevent"
)

// malformedExtrasRow scans a raw_events row whose extras JSON carries a non-string
// tag — the input that panics cloudevent.RestoreNonColumnFields. Only the extras
// column (the 9th Scan dest) is populated; the rest stay zero.
type malformedExtrasRow struct{}

func (malformedExtrasRow) Scan(dest ...any) error {
	if p, ok := dest[8].(**string); ok {
		s := `{"tags":[42]}`
		*p = &s
	}
	return nil
}

// scanStoredEvent must not panic on a poisoned raw_events row: the fetch gRPC path has
// no recover middleware, so an unguarded RestoreNonColumnFields would crash the server
// goroutine. This drives the FULL scan (not just the wrapper in isolation), so it also
// fails if the safe wrapper is ever left un-wired from scanStoredEvent.
func TestScanStoredEvent_MalformedExtrasDoesNotPanic(t *testing.T) {
	if _, err := scanStoredEvent(malformedExtrasRow{}); err != nil {
		t.Fatalf("scanStoredEvent returned error on malformed extras: %v", err)
	}
}

// Sanity: the same input panics the raw lib call, so the test above is meaningful.
func TestRestoreNonColumnFields_PanicsOnNonStringTag(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Skip("cloudevent.RestoreNonColumnFields no longer panics on a non-string tag; guard is moot")
		}
	}()
	hdr := cloudevent.CloudEventHeader{Extras: map[string]any{"tags": []any{float64(42)}}}
	cloudevent.RestoreNonColumnFields(&hdr)
}
