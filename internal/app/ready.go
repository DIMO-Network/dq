package app

import (
	"context"
	"net/http"
	"time"

	"github.com/DIMO-Network/dq/internal/service/duck"
)

// readyProbeTimeout bounds the catalog readiness query so a wedged catalog
// surfaces as NotReady rather than hanging the probe.
const readyProbeTimeout = 2 * time.Second

// ReadyHandler returns an HTTP handler reporting 200 when ready returns nil and
// 503 otherwise. Unlike a static liveness 200, this lets a cold or
// catalog-down pod be pulled from the Service before it serves errors (CHD-13).
func ReadyHandler(ready func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyProbeTimeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// Ready runs the backend readiness check (nil = ready); it is the probe behind
// ReadyHandler. A nil check (e.g. a backend with no duck service to probe)
// reports ready.
func (a *App) Ready(ctx context.Context) error {
	if a.readyCheck == nil {
		return nil
	}
	return a.readyCheck(ctx)
}

// duckReadiness builds a readiness probe over the DuckLake service. It runs
// SELECT 1 FROM lake.signals LIMIT 0, which fails unless the catalog is reachable,
// the DuckLake/httpfs extensions are loaded, and the decoded table is present —
// exactly the cold-start conditions a static 200 hides.
func duckReadiness(svc *duck.Service) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := svc.DB().ExecContext(ctx, "SELECT 1 FROM lake.signals LIMIT 0")
		return err
	}
}
