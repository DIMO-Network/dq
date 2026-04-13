// Package auth provides JWT middleware and GraphQL directive handlers for dq.
package auth

import (
	"context"

	"github.com/DIMO-Network/token-exchange-api/pkg/tokenclaims"
	jwtmiddleware "github.com/auth0/go-jwt-middleware/v2"
)

// DQClaimContextKey is the context key for *DQClaim.
type DQClaimContextKey struct{}

// DQClaim is the unified custom claim for dq. It embeds tokenclaims.Token
// which provides Asset (the vehicle DID string), Permissions, and CloudEvents.
// No DID parsing is needed: the directive compares claim.Asset to the did arg directly.
type DQClaim struct {
	tokenclaims.Token
}

// Validate implements validator.CustomClaims. No additional validation needed
// beyond what the JWT validator already does.
func (*DQClaim) Validate(context.Context) error {
	return nil
}

func getDQClaim(ctx context.Context) (*DQClaim, error) {
	claim, ok := ctx.Value(DQClaimContextKey{}).(*DQClaim)
	if !ok || claim == nil {
		return nil, jwtmiddleware.ErrJWTMissing
	}
	return claim, nil
}
