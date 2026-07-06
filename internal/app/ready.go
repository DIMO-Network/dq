package app

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/DIMO-Network/dq/internal/service/duck"
)

// readyProbeTimeout bounds the catalog readiness query so a wedged catalog
// surfaces as NotReady rather than hanging the probe.
const readyProbeTimeout = 2 * time.Second

// readyCacheTTL collapses a burst of probes into one backing catalog query so the
// readiness check doesn't pile demand onto the shared query pool.
const readyCacheTTL = 3 * time.Second

// readyGraceWindow keeps a pod Ready through a TRANSIENT readiness failure once the
// backend has succeeded at least once — a probe that times out because the connection
// pool is saturated by query load (not a wedged catalog) is load, not ill health.
// Without it, query load alone flips a healthy pod to NotReady; Kubernetes sheds its
// traffic to siblings, they saturate, and the failure cascades across the fleet. Only a
// SUSTAINED failure (no success within the window) reports NotReady; a cold pod that has
// never been ready gets no grace (correctly NotReady until first success).
//
// 60s, not 20s (C9): the catalog is a SHARED dependency, so a >window blip flips the
// WHOLE query fleet NotReady simultaneously — Kubernetes then de-endpoints every pod at
// once, turning a brief blip into a total outage. 60s rides out any transient catalog
// hiccup (Postgres failover, S3 slow-start, connection recycle) while still bounding how
// long a pod serves stale reads well under any real outage's response time.
const readyGraceWindow = 60 * time.Second

// loadTolerantReadiness wraps check so a burst of probes reuses one result within ttl,
// and a transient failure after a recent success stays Ready for graceWindow — breaking
// the saturated-pool → NotReady → load-shed → cascade loop. Sustained failure still fails.
func loadTolerantReadiness(check func(context.Context) error, ttl, graceWindow time.Duration) func(context.Context) error {
	var (
		mu      sync.Mutex
		lastRun time.Time
		lastErr error
		lastOK  time.Time
	)
	verdict := func(err error, ok time.Time) error {
		if err == nil || (!ok.IsZero() && time.Since(ok) < graceWindow) {
			return nil
		}
		return err
	}
	return func(ctx context.Context) error {
		mu.Lock()
		if !lastRun.IsZero() && time.Since(lastRun) < ttl {
			err, ok := lastErr, lastOK
			mu.Unlock()
			return verdict(err, ok)
		}
		mu.Unlock()

		err := check(ctx)

		mu.Lock()
		lastRun, lastErr = time.Now(), err
		if err == nil {
			lastOK = lastRun
		}
		ok := lastOK
		mu.Unlock()
		return verdict(err, ok)
	}
}

// ReadyHandler returns an HTTP handler reporting 200 when ready returns nil and
// 503 otherwise. Unlike a static liveness 200, this lets a cold or
// catalog-down pod be pulled from the Service before it serves errors (CHD-13).
func ReadyHandler(ready func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyProbeTimeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			// Generic body on the public mux: ready() surfaces a catalog-attach error
			// that can name the catalog host/dbname/table — internal topology that
			// shouldn't leak to an unauthenticated probe (matches din). The failure
			// itself is observable via the probe status + catalog metrics.
			http.Error(w, "not ready", http.StatusServiceUnavailable)
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
