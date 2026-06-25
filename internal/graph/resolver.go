//go:generate go run github.com/DIMO-Network/server-garage/cmd/mcpgen -schema ../../schema/ -prefix dq -out mcp_tools_gen.go -package graph
package graph

import (
	"github.com/DIMO-Network/dq/internal/identity"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app; add any dependencies you require here.

// ClaimsContextKey is the context key for *tokenclaims.Token (set by auth.AddClaimHandler).
type ClaimsContextKey struct{}

// Resolver is the root resolver for the dq GraphQL schema.
type Resolver struct {
	SignalRepo     *repositories.Repository
	EventService   eventrepo.EventService
	IdentityClient identity.Client
}
