package limits

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestConcurrencyLimiter_ReleasesOnPanic ensures a panicking handler still frees its
// slot (the middleware releases via defer). Without that, a handler panic would leak a
// slot and, after `max` panics, permanently lock out the subject — silent starvation.
func TestConcurrencyLimiter_ReleasesOnPanic(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	h := cl.Middleware(func(*http.Request) string { return "a" })(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }),
	)
	func() {
		defer func() { _ = recover() }() // the panic propagates past the limiter; swallow it here
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	if !cl.acquire("a") {
		t.Fatal("slot leaked: a panicking handler did not release its slot")
	}
}

// TestConcurrencyLimiter_ConcurrentAcquireRelease exercises the inflight map under
// concurrent access (run with -race) and asserts no entry leaks once all are released.
func TestConcurrencyLimiter_ConcurrentAcquireRelease(t *testing.T) {
	cl := NewConcurrencyLimiter(4)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i%5)
			if cl.acquire(key) {
				cl.release(key)
			}
		}(i)
	}
	wg.Wait()
	cl.mu.Lock()
	n := len(cl.inflight)
	cl.mu.Unlock()
	if n != 0 {
		t.Fatalf("inflight map leaked %d entries after all releases", n)
	}
}
