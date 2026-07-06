package limits

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The global cap must shed (429), not queue: with the limit held, an arriving
// request gets a fast rejection with Retry-After instead of stacking a
// goroutine behind the DuckDB pool (H11).
func TestGlobalLimiter_HTTPShedsOverCap(t *testing.T) {
	g := NewGlobalLimiter(1)

	occupied := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	h := g.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(occupied)
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	// Occupy the single slot.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
	}()
	<-occupied

	// Second request: shed fast with 429 + Retry-After.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))

	close(release)
	wg.Wait()
}

// gRPC shares the same budget semantics: over the cap = RESOURCE_EXHAUSTED.
func TestGlobalLimiter_GRPCShedsOverCap(t *testing.T) {
	g := NewGlobalLimiter(1)
	intercept := g.UnaryInterceptor()

	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := intercept(context.Background(), nil, &grpc.UnaryServerInfo{},
			func(context.Context, any) (any, error) { close(started); <-release; return "ok", nil })
		assert.NoError(t, err)
	}()
	<-started

	_, err := intercept(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(context.Context, any) (any, error) { return "ok", nil })
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	close(release)
	<-done
}

// Zero = disabled: no cap.
func TestGlobalLimiter_ZeroDisables(t *testing.T) {
	g := NewGlobalLimiter(0)
	h := g.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	for range 5 {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, http.StatusOK, rec.Code)
	}
}
