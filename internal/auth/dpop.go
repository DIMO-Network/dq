package auth

import (
	"net/http"
	"strings"

	"github.com/DIMO-Network/dauth/pkg/dpop"
	"github.com/rs/zerolog"
)

// NewDPoPMiddleware binds every authenticated request to the key its access
// token names (RFC 9449 §7.1): the request must carry a DPoP proof for its
// method and URL, with ath over the presented token, signed by the key whose
// thumbprint is the token's cnf.jkt. An unauthenticated request passes
// through, since credentials are optional here and the directives refuse it.
//
// publicBaseURL is the origin clients address dq at, for the proof's htu when
// dq sits behind a proxy; empty means the request's own scheme and host.
func NewDPoPMiddleware(publicBaseURL string, verifier *dpop.Verifier) func(http.Handler) http.Handler {
	base := strings.TrimRight(publicBaseURL, "/")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := GetValidatedClaims(r.Context())
			if !ok || claims.CustomClaims == nil {
				next.ServeHTTP(w, r)
				return
			}
			dq, ok := claims.CustomClaims.(*DQClaim)
			if !ok || dq.Confirmation == nil || dq.Confirmation.JKT == "" {
				refuse(w, r, "access token is not bound to a DPoP key")
				return
			}
			proof := r.Header.Get(dpop.HeaderName)
			if proof == "" {
				refuse(w, r, "a DPoP proof header is required")
				return
			}
			token, _ := accessTokenFromHeader(r)
			jkt, err := verifier.Verify(proof, r.Method, requestURL(base, r), token)
			if err != nil {
				refuse(w, r, err.Error())
				return
			}
			if jkt != dq.Confirmation.JKT {
				refuse(w, r, "DPoP proof key does not match the token's cnf.jkt")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func requestURL(base string, r *http.Request) string {
	if base != "" {
		return base + r.URL.Path
	}
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	return scheme + "://" + r.Host + r.URL.Path
}

func refuse(w http.ResponseWriter, r *http.Request, msg string) {
	zerolog.Ctx(r.Context()).Warn().Str("reason", msg).Msg("DPoP check failed")
	w.Header().Set("WWW-Authenticate", `DPoP error="invalid_dpop_proof"`)
	http.Error(w, msg, http.StatusUnauthorized)
}
