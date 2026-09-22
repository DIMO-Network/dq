package auth

import (
	"context"
	"strings"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// NewGRPCFetchAuthInterceptor builds a unary interceptor that authenticates the
// fetch gRPC service with a dauth access token (the same issuer, JWKS and
// audience as the HTTP surface) holding raw:read for at least one subject; the
// RPCs then scope each read to the token's subjects. The fetch RPCs return any
// subject's cloud events and blob payloads with no other check, so without
// this the port is readable by any in-cluster caller.
//
// require governs rollout: an *invalid* token is always rejected; a *missing*
// token is rejected only when require is true.
//
// The proof-of-possession check the HTTP surface makes (DPoP) is not made
// here: the port is in-cluster behind the NetworkPolicy, which stays the
// compensating control.
func NewGRPCFetchAuthInterceptor(issuer, jwksURI, audience string, require bool, log *zerolog.Logger) (grpc.UnaryServerInterceptor, error) {
	jwtValidator, err := newValidator(issuer, jwksURI, audience)
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
		if !grpcHasRawDataAccess(&dq.Token) {
			return nil, status.Error(codes.PermissionDenied, "token lacks raw:read")
		}
		return handler(context.WithValue(ctx, DQClaimContextKey{}, dq), req)
	}, nil
}

// bearerFromMetadata extracts the token from the gRPC authorization metadata
// header under the Bearer or DPoP scheme (case-insensitive); empty when absent.
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
	for _, scheme := range []string{"bearer ", "dpop "} {
		if len(tok) >= len(scheme) && strings.EqualFold(tok[:len(scheme)], scheme) {
			return strings.TrimSpace(tok[len(scheme):])
		}
	}
	return tok
}

// grpcHasRawDataAccess reports whether the token holds raw:read for any
// subject; the per-subject check happens on each RPC.
func grpcHasRawDataAccess(tok *tokenclaims.Token) bool {
	for _, s := range tok.Subjects() {
		if _, ok := tok.Holds(s, tokenclaims.AbilityRawRead); ok {
			return true
		}
	}
	return false
}
