package app

import (
	"fmt"
	"net/http"
	"os"
	"runtime/debug"

	"github.com/DIMO-Network/dq/internal/auth"
	"github.com/rs/zerolog"
)

// LoggerMiddleware adds method, path, and source IP to the request context logger.
func LoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceIP := r.Header.Get("X-Forwarded-For")
		if sourceIP == "" {
			sourceIP = r.Header.Get("X-Real-IP")
		}
		if sourceIP == "" {
			sourceIP = r.RemoteAddr
		}
		loggerCtx := zerolog.Ctx(r.Context()).With().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("sourceIp", sourceIP).
			Logger().
			WithContext(r.Context())
		r = r.WithContext(loggerCtx)
		next.ServeHTTP(w, r)
	})
}

// authLoggerMiddleware adds the authenticated user and the vehicle subject to
// the request logger. Logging the asset DID (the vehicle being queried) keys
// every query-path log line by subject — the dimension a "vehicle X is wrong"
// report needs to be root-caused, which the query path never logged (CHD-14).
func authLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validateClaims, ok := auth.GetValidatedClaims(r.Context())
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		lc := zerolog.Ctx(r.Context()).With().Str("jwtSubject", validateClaims.RegisteredClaims.Subject)
		if dq, ok := validateClaims.CustomClaims.(*auth.DQClaim); ok && dq.Asset != "" {
			lc = lc.Str("subject", dq.Asset)
		}
		r = r.WithContext(lc.Logger().WithContext(r.Context()))
		next.ServeHTTP(w, r)
	})
}

// PanicRecoveryMiddleware recovers from panics and logs them.
func PanicRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "panic: %v\n%s\n", err, debug.Stack())
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
