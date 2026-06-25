package materializer

import (
	"testing"
	"time"
)

// TestFailureTracker pins the materializer's restart-backstop state machine. A regression
// in the streak reset would either wedge the loop forever (never restart a durably-broken
// catalog) or restart on a transient blip; the window check guards "Ready but not
// decoding". This is the failure path that's otherwise only reachable via a broken catalog.
func TestFailureTracker(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	ft := failureTracker{window: time.Hour}

	if ft.record(false, t0) {
		t.Fatal("first failure starts the streak; must not trip")
	}
	if ft.record(false, t0.Add(59*time.Minute)) {
		t.Fatal("failure still within the window must not trip")
	}
	// A success clears the streak...
	ft.record(true, t0.Add(59*time.Minute+time.Second))
	// ...so a later failure starts a FRESH streak — no trip, even though more than a
	// window has elapsed since the original first failure (the error-error-success-error
	// invariant).
	if ft.record(false, t0.Add(2*time.Hour)) {
		t.Fatal("a failure after a success must start a fresh streak, not trip")
	}
	// Continuous failure from the fresh streak (t0+2h) for exactly the window trips.
	if !ft.record(false, t0.Add(3*time.Hour)) {
		t.Fatal("continuous failure for the whole window must trip")
	}
	// A success after tripping still clears it; the next first failure does not re-trip.
	ft.record(true, t0.Add(3*time.Hour+time.Second))
	if ft.record(false, t0.Add(3*time.Hour+2*time.Second)) {
		t.Fatal("post-reset first failure must not trip")
	}
}
