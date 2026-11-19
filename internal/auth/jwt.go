package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/graph"
	jwtmiddleware "github.com/auth0/go-jwt-middleware/v2"
	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/rs/zerolog"
)

// newValidator builds the dauth access token validator (RS256, the configured
// audience, *DQClaim custom claims) shared by the HTTP middleware and the gRPC
// fetch interceptor so both enforce identical token semantics.
func newValidator(issuer, jwksURI, audience string) (*validator.Validator, error) {
	issuerURL, err := url.Parse(issuer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse issuer URL: %w", err)
	}
	if audience == "" {
		return nil, errors.New("token audience is required")
	}
	opts := []any{}
	if jwksURI != "" {
		keysURI, err := url.Parse(jwksURI)
		if err != nil {
			return nil, fmt.Errorf("failed to parse jwksURI: %w", err)
		}
		opts = append(opts, jwks.WithCustomJWKSURI(keysURI))
	}
	provider := jwks.NewCachingProvider(issuerURL, 1*time.Minute, opts...)
	newCustomClaims := func() validator.CustomClaims {
		return &DQClaim{}
	}
	return validator.New(
		provider.KeyFunc,
		validator.RS256,
		issuerURL.String(),
		[]string{audience},
		validator.WithCustomClaims(newCustomClaims),
	)
}

// NewJWTMiddleware creates JWT middleware with the given dauth issuer, JWKS
// URI and audience. The token may be presented under the DPoP scheme (RFC
// 9449) or as a Bearer; the DPoP middleware then binds it either way.
func NewJWTMiddleware(issuer, jwksURI, audience string) (*jwtmiddleware.JWTMiddleware, error) {
	jwtValidator, err := newValidator(issuer, jwksURI, audience)
	if err != nil {
		return nil, fmt.Errorf("failed to create validator: %w", err)
	}
	middleware := jwtmiddleware.New(
		jwtValidator.ValidateToken,
		jwtmiddleware.WithErrorHandler(ErrorHandler),
		jwtmiddleware.WithCredentialsOptional(true),
		jwtmiddleware.WithTokenExtractor(accessTokenFromHeader),
	)
	return middleware, nil
}

// accessTokenFromHeader reads the access token from the Authorization header
// under either the DPoP or the Bearer scheme.
func accessTokenFromHeader(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", nil
	}
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || (!strings.EqualFold(scheme, "DPoP") && !strings.EqualFold(scheme, "Bearer")) {
		return "", errors.New("authorization header must use the DPoP or Bearer scheme")
	}
	return strings.TrimSpace(token), nil
}

// AddClaimHandler extracts the *DQClaim from the validated JWT and stores it in
// the request context under two keys:
//   - DQClaimContextKey{}: used by @requiresVehicleToken and the ability directives
//   - graph.ClaimsContextKey{}: used by the cloud event resolvers and the per-signal filter
func AddClaimHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := GetValidatedClaims(r.Context())
		if !ok || claims.CustomClaims == nil {
			next.ServeHTTP(w, r)
			return
		}
		dqClaim, ok := claims.CustomClaims.(*DQClaim)
		if !ok {
			zerolog.Ctx(r.Context()).Error().Msg("could not cast claims to DQClaim")
			jwtmiddleware.DefaultErrorHandler(w, r, jwtmiddleware.ErrJWTMissing)
			return
		}
		ctx := context.WithValue(r.Context(), DQClaimContextKey{}, dqClaim)
		ctx = context.WithValue(ctx, graph.ClaimsContextKey{}, &dqClaim.Token)
		next.ServeHTTP(w, r.Clone(ctx))
	})
}

// GetValidatedClaims returns the validated JWT claims from the request context.
func GetValidatedClaims(ctx context.Context) (*validator.ValidatedClaims, bool) {
	claim := ctx.Value(jwtmiddleware.ContextKey{})
	if claim == nil {
		return nil, false
	}
	vc, ok := claim.(*validator.ValidatedClaims)
	return vc, ok
}

// ErrorHandler logs JWT errors and delegates to the default error handler.
func ErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	zerolog.Ctx(r.Context()).Error().Err(err).Msg("error validating token")
	jwtmiddleware.DefaultErrorHandler(w, r, err)
}
