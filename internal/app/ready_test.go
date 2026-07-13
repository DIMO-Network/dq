package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReadyHandler proves the readiness endpoint reflects the backend check: a
// cold or catalog-down pod must report NotReady instead of the static 200 the
// liveness path returns, so it is pulled from the Service before it serves
// errors (CHD-13).
func TestReadyHandler(t *testing.T) {
	okReq := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	ReadyHandler(func(context.Context) error { return nil })(rec, okReq)
	assert.Equal(t, http.StatusOK, rec.Code, "ready when the catalog probe passes")

	failReq := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec = httptest.NewRecorder()
	ReadyHandler(func(context.Context) error { return errors.New("catalog unreachable") })(rec, failReq)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "not ready when the catalog probe fails")
}

// TestLoadTolerantReadiness pins the cascade-breaking behavior: once the backend has been
// ready, a transient failure (pool saturated by load) stays Ready for the grace window;
// only a sustained failure reports NotReady. A cold pod that has never been ready gets no grace.
func TestLoadTolerantReadiness(t *testing.T) {
	var result error
	check := func(context.Context) error { return result }
	ready := loadTolerantReadiness(check, 0, 50*time.Millisecond) // ttl=0 → no caching
	ctx := context.Background()

	result = errors.New("catalog down")
	require.Error(t, ready(ctx), "a cold pod that never succeeded must report NotReady")

	result = nil
	require.NoError(t, ready(ctx))

	result = errors.New("pool saturated")
	require.NoError(t, ready(ctx), "a transient failure within the grace window must stay Ready")

	time.Sleep(60 * time.Millisecond)
	require.Error(t, ready(ctx), "a failure past the grace window must report NotReady")
}

// TestExitOnSustainedFailure pins the NotReady-forever escape hatch (#21):
// liveness always passes, so a pod whose DuckDB instance is poisoned parks
// Running-but-NotReady indefinitely unless sustained readiness failure exits
// the process. Any intervening success must reset the failure window.
func TestExitOnSustainedFailure(t *testing.T) {
	var result error
	fatals := 0
	check := func(context.Context) error { return result }
	ready := exitOnSustainedFailure(check, 50*time.Millisecond, func(error) { fatals++ })
	ctx := context.Background()

	result = errors.New("catalog poisoned")
	require.Error(t, ready(ctx))
	require.Error(t, ready(ctx))
	require.Equal(t, 0, fatals, "failures within the window must not exit")

	result = nil
	require.NoError(t, ready(ctx), "a success must reset the failure window")
	result = errors.New("catalog poisoned")
	require.Error(t, ready(ctx))
	time.Sleep(30 * time.Millisecond)
	require.Error(t, ready(ctx))
	require.Equal(t, 0, fatals, "the window must restart after a success")

	time.Sleep(30 * time.Millisecond)
	require.Error(t, ready(ctx))
	require.Equal(t, 1, fatals, "failing for the whole window must exit the process")
}

// TestLoadTolerantReadiness_CachesWithinTTL verifies a burst of probes reuses one backing
// result within the TTL, so the probe doesn't pile demand onto the query pool.
func TestLoadTolerantReadiness_CachesWithinTTL(t *testing.T) {
	calls := 0
	check := func(context.Context) error { calls++; return nil }
	ready := loadTolerantReadiness(check, time.Minute, time.Minute)
	ctx := context.Background()
	for range 5 {
		require.NoError(t, ready(ctx))
	}
	require.Equal(t, 1, calls, "a burst within the TTL must collapse to one backing query")
}
