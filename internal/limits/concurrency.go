package limits

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// concurrencyRejectedTotal counts requests rejected because the caller already had
// the per-subject limit in flight. A sustained rate means a caller is hitting the cap
// (raise MAX_CONCURRENT_REQUESTS_PER_SUBJECT or the pool, or the caller is abusive).
var concurrencyRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_requests_rejected_concurrency_total",
	Help: "HTTP requests rejected (429) for exceeding the per-subject in-flight limit.",
})

// ConcurrencyLimiter bounds in-flight requests per key (the authenticated JWT subject)
// so a single caller can't pin the whole DuckDB pool and starve co-tenants on a replica.
// A value of max <= 0 disables limiting (the middleware is a pass-through).
type ConcurrencyLimiter struct {
	max      int
	mu       sync.Mutex
	inflight map[string]int
}

// NewConcurrencyLimiter returns a limiter allowing at most max concurrent requests per
// key. max <= 0 disables it.
func NewConcurrencyLimiter(max int) *ConcurrencyLimiter {
	return &ConcurrencyLimiter{max: max, inflight: make(map[string]int)}
}

// acquire reserves a slot for key, returning false if key is already at the limit.
func (c *ConcurrencyLimiter) acquire(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight[key] >= c.max {
		return false
	}
	c.inflight[key]++
	return true
}

// release returns a slot for key, deleting the entry at zero so the map can't grow
// unbounded with one-off callers.
func (c *ConcurrencyLimiter) release(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight[key] <= 1 {
		delete(c.inflight, key)
	} else {
		c.inflight[key]--
	}
}

// Middleware wraps a handler, rejecting (429) a request whose keyFn value already has
// max requests in flight. keyFn returns the limiting key (the JWT subject); an empty
// key is not limited (e.g. an unauthenticated request — let auth reject it). When the
// limiter is disabled (max <= 0) the original handler is returned unwrapped.
func (c *ConcurrencyLimiter) Middleware(keyFn func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if c.max <= 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFn(r)
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}
			if !c.acquire(key) {
				concurrencyRejectedTotal.Inc()
				http.Error(w, "too many concurrent requests", http.StatusTooManyRequests)
				return
			}
			defer c.release(key)
			next.ServeHTTP(w, r)
		})
	}
}
