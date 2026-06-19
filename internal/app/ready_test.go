package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
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
