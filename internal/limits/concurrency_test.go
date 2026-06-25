package limits

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConcurrencyLimiter_AcquireRelease(t *testing.T) {
	cl := NewConcurrencyLimiter(2)
	if !cl.acquire("a") || !cl.acquire("a") {
		t.Fatal("first two acquires for a key should succeed")
	}
	if cl.acquire("a") {
		t.Fatal("third acquire over the limit must fail")
	}
	if !cl.acquire("b") {
		t.Fatal("a different key must not be blocked by another's saturation")
	}
	cl.release("a")
	if !cl.acquire("a") {
		t.Fatal("acquire should succeed after a release frees a slot")
	}
	// A key released back to zero must be dropped from the map (no unbounded growth).
	cl.release("b")
	cl.mu.Lock()
	_, exists := cl.inflight["b"]
	cl.mu.Unlock()
	if exists {
		t.Fatal("a key at zero in-flight must be deleted from the map")
	}
}

func TestConcurrencyLimiter_Middleware429(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	cl.acquire("a") // saturate subject "a"
	h := cl.Middleware(func(*http.Request) string { return "a" })(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("must not reach handler when over limit") }),
	)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", rec.Code)
	}
}

func TestConcurrencyLimiter_Disabled(t *testing.T) {
	cl := NewConcurrencyLimiter(0) // disabled
	cl.acquire("a")                // even a saturated key must pass when disabled
	called := false
	h := cl.Middleware(func(*http.Request) string { return "a" })(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }),
	)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Fatal("a disabled limiter must be a pass-through")
	}
}

func TestConcurrencyLimiter_EmptyKeyNotLimited(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	called := 0
	h := cl.Middleware(func(*http.Request) string { return "" })(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ }),
	)
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}
	if called != 3 {
		t.Fatalf("empty-key requests must never be limited, served %d/3", called)
	}
}
