package auth

import (
	"context"
	"slices"
	"strings"

	"github.com/DIMO-Network/token-exchange-api/pkg/tokenclaims"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// NewGRPCFetchAuthInterceptor builds a unary interceptor that authenticates the
// fetch gRPC service with a DIMO-issued JWT (the same issuer / JWKS / dimo.zone
// audience as the HTTP surface) and requires raw-data permission. The fetch RPCs
// return any subject's cloud events and blob payloads with no other check, so
// without this the port is readable by any in-cluster caller.
//
// require governs rollout: an *invalid* token is always rejected; a *missing*
// token is rejected only when require is true (so existing callers keep working
// while they are migrated to send a token). When require is false a startup
// warning is logged so the still-open state is visible.
//
// NOTE: this authenticates the caller and checks raw-data permission, but does
// not yet scope the read to the token's own subject — the per-subject identity
// linking the HTTP resolver performs needs the IdentityClient threaded into the
// fetch server. Until that lands, keep the NetworkPolicy allowlist in place too.
func NewGRPCFetchAuthInterceptor(issuer, jwksURI string, require bool, log *zerolog.Logger) (grpc.UnaryServerInterceptor, error) {
	jwtValidator, err := newValidator(issuer, jwksURI)
	if err != nil {
		return nil, err
	}
	if !require {
		log.Warn().Msg("fetch gRPC JWT auth is advisory (FETCH_GRPC_REQUIRE_JWT=false): invalid tokens are rejected but a missing token is admitted — enable it once callers send tokens")
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		raw := bearerFromMetadata(ctx)
		if raw == "" {
			if require {
				return nil, status.Error(codes.Unauthenticated, "missing bearer token")
			}
			return handler(ctx, req)
		}
		parsed, err := jwtValidator.ValidateToken(ctx, raw)
		if err != nil {
			log.Warn().Err(err).Str("method", info.FullMethod).Msg("fetch gRPC token validation failed")
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		claims, ok := parsed.(*validator.ValidatedClaims)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "invalid token claims")
		}
		dq, ok := claims.CustomClaims.(*DQClaim)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "invalid token claims")
		}
		if !grpcHasRawDataAccess(dq.Permissions) {
			return nil, status.Error(codes.PermissionDenied, "token lacks raw-data permission")
		}
		return handler(context.WithValue(ctx, DQClaimContextKey{}, dq), req)
	}, nil
}

// bearerFromMetadata extracts the bearer token from the gRPC authorization
// metadata header (case-insensitive scheme); empty when absent.
func bearerFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return ""
	}
	tok := vals[0]
	for _, prefix := range []string{"Bearer ", "bearer "} {
		if strings.HasPrefix(tok, prefix) {
			return strings.TrimPrefix(tok, prefix)
		}
	}
	return tok
}

// grpcHasRawDataAccess mirrors graph.hasRawDataAccess: a token may read raw data
// with the explicit get-raw-data permission, or with both history permissions.
func grpcHasRawDataAccess(perms []string) bool {
	if slices.Contains(perms, tokenclaims.PermissionGetRawData) {
		return true
	}
	return slices.Contains(perms, tokenclaims.PermissionGetLocationHistory) &&
		slices.Contains(perms, tokenclaims.PermissionGetNonLocationHistory)
}
