package materializer

import (
	"testing"

	"github.com/DIMO-Network/cloudevent"
)

// restoreNonColumnFieldsSafe must contain a panic from cloudevent.RestoreNonColumnFields
// (which does unchecked type assertions on the extras map) so a single poisoned
// raw_events row can't crash-loop the single-writer materializer.
func TestRestoreNonColumnFieldsSafe_ContainsPanic(t *testing.T) {
	// A non-string element in the tags extras is what the cloudevent lib asserts on.
	malformed := func() cloudevent.CloudEventHeader {
		return cloudevent.CloudEventHeader{Extras: map[string]any{"tags": []any{float64(42)}}}
	}

	// Confirm the raw lib call really does panic on this input — otherwise the guard
	// test is vacuous (skip rather than pass silently if the lib stops panicking).
	func() {
		defer func() {
			if recover() == nil {
				t.Skip("cloudevent.RestoreNonColumnFields no longer panics on a non-string tag; guard is moot")
			}
		}()
		h := malformed()
		cloudevent.RestoreNonColumnFields(&h)
	}()

	// The safe wrapper must NOT panic on the same input (a fatal panic here fails the test).
	h := malformed()
	restoreNonColumnFieldsSafe(&h)
}
