// Package auth provides JWT and DPoP middleware and GraphQL directive
// handlers for dq.
package auth

import (
	"context"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	jwtmiddleware "github.com/auth0/go-jwt-middleware/v2"
)

// DQClaimContextKey is the context key for *DQClaim.
type DQClaimContextKey struct{}

// DQClaim is the access token dq checks: dauth's tokenclaims.Token, whose
// grants say which subjects the caller may read, with which abilities, over
// which data-timestamp windows.
type DQClaim struct {
	tokenclaims.Token
}

// Validate implements validator.CustomClaims: the structural checks on the
// grants, so a directive never sees a token without a DID subject or with
// malformed windows.
func (c *DQClaim) Validate(context.Context) error {
	return c.Token.Validate()
}

func getDQClaim(ctx context.Context) (*DQClaim, error) {
	claim, ok := ctx.Value(DQClaimContextKey{}).(*DQClaim)
	if !ok || claim == nil {
		return nil, jwtmiddleware.ErrJWTMissing
	}
	return claim, nil
}

// DQClaimFromContext returns the validated DQ claim stored by the gRPC fetch
// interceptor or the HTTP middleware, and false if none is present. Used by the
// gRPC fetch RPCs to scope reads to the token's subjects.
func DQClaimFromContext(ctx context.Context) (*DQClaim, bool) {
	claim, ok := ctx.Value(DQClaimContextKey{}).(*DQClaim)
	return claim, ok && claim != nil
}
