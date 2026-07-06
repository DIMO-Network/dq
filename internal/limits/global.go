package limits

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// globalRejectedTotal counts requests shed by the process-wide in-flight cap,
// by transport. Pair with the pool-wait stats: rejections here are the
// designed alternative to unbounded queueing on the DuckDB connection pool.
var globalRejectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "dq_requests_rejected_global_total",
	Help: "Requests rejected (429 / RESOURCE_EXHAUSTED) by the process-wide in-flight cap.",
}, []string{"transport"})

// globalAcquireWait is how long an arriving request may wait for a slot before
// being shed. Long enough to absorb micro-bursts, short enough that a wedged
// pool sheds load instead of stacking goroutines (H11).
const globalAcquireWait = 100 * time.Millisecond

// GlobalLimiter bounds TOTAL in-flight requests across the process. The DuckDB
// pool has a handful of connections and MAX_REQUEST_DURATION can be tens of
// seconds — without a global cap, database/sql queues every excess request
// unboundedly (goroutines + held HTTP/gRPC streams) behind a few pathological
// queries (H11). Size it to a small multiple of DUCKDB_MAX_CONNS.
type GlobalLimiter struct {
	sem chan struct{} // nil = disabled
}

// NewGlobalLimiter returns a limiter admitting at most max concurrent
// requests. max <= 0 disables it.
func NewGlobalLimiter(max int) *GlobalLimiter {
	if max <= 0 {
		return &GlobalLimiter{}
	}
	return &GlobalLimiter{sem: make(chan struct{}, max)}
}

// acquire admits or rejects within globalAcquireWait; ctx abort also rejects.
func (g *GlobalLimiter) acquire(ctx context.Context) bool {
	if g.sem == nil {
		return true
	}
	select {
	case g.sem <- struct{}{}:
		return true
	default:
	}
	t := time.NewTimer(globalAcquireWait)
	defer t.Stop()
	select {
	case g.sem <- struct{}{}:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}

func (g *GlobalLimiter) release() {
	if g.sem != nil {
		<-g.sem
	}
}

// Middleware sheds HTTP requests over the cap with 429 + Retry-After.
func (g *GlobalLimiter) Middleware(next http.Handler) http.Handler {
	if g.sem == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !g.acquire(r.Context()) {
			globalRejectedTotal.WithLabelValues("http").Inc()
			w.Header().Set("Retry-After", "1")
			http.Error(w, "server is at its concurrent-request limit; retry shortly", http.StatusTooManyRequests)
			return
		}
		defer g.release()
		next.ServeHTTP(w, r)
	})
}

// UnaryInterceptor sheds gRPC requests over the cap with RESOURCE_EXHAUSTED —
// the fetch port previously had no concurrency bound at all (H11).
func (g *GlobalLimiter) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !g.acquire(ctx) {
			globalRejectedTotal.WithLabelValues("grpc").Inc()
			return nil, status.Error(codes.ResourceExhausted, "server is at its concurrent-request limit; retry shortly")
		}
		defer g.release()
		return handler(ctx, req)
	}
}
