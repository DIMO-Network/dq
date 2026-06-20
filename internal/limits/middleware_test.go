package limits

import (
	"testing"
	"time"
)

// An empty MAX_REQUEST_DURATION must fall back to a safe default rather than
// failing New — otherwise an omitted env var crashes app startup (and the
// readiness probe with it), crash-looping the pod (SR-12).
func TestNew_EmptyDefaults(t *testing.T) {
	l, err := New("")
	if err != nil {
		t.Fatalf("New(\"\") returned error: %v", err)
	}
	if l.maxRequestDuration != defaultMaxRequestDuration {
		t.Fatalf("maxRequestDuration = %v, want %v", l.maxRequestDuration, defaultMaxRequestDuration)
	}
}

// A non-empty, valid value is still honored.
func TestNew_ExplicitValue(t *testing.T) {
	l, err := New("30s")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if l.maxRequestDuration != 30*time.Second {
		t.Fatalf("maxRequestDuration = %v, want 30s", l.maxRequestDuration)
	}
}

// A non-empty but unparseable value still errors (typos must not be masked).
func TestNew_InvalidErrors(t *testing.T) {
	if _, err := New("banana"); err == nil {
		t.Fatal("expected error for unparseable duration, got nil")
	}
}
