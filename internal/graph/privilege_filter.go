package graph

import (
	"context"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/repositories"
)

// privilegeEnumToAbility maps GraphQL Privilege enum values (as they appear
// in model-garage definitions.yaml) to ability names. The same mapping is in
// gqlgen.yml for the directives; this copy serves the per-signal filter on
// the latest/snapshot/summary reads, which is code rather than schema.
var privilegeEnumToAbility = map[string]string{
	"VEHICLE_NON_LOCATION_DATA":    tokenclaims.AbilityTelemetryRead,
	"VEHICLE_COMMANDS":             tokenclaims.AbilityCommandUnlock,
	"VEHICLE_CURRENT_LOCATION":     tokenclaims.AbilityLocationPrecise,
	"VEHICLE_ALL_TIME_LOCATION":    tokenclaims.AbilityLocationPrecise,
	"VEHICLE_VIN_CREDENTIAL":       tokenclaims.AbilityDocumentsRead,
	"VEHICLE_APPROXIMATE_LOCATION": tokenclaims.AbilityLocationApproximate,
	"VEHICLE_RAW_DATA":             tokenclaims.AbilityRawRead,
}

// abilityCheck answers whether an ability is usable now for a subject.
type abilityCheck func(ability string) bool

func hasPrivilegesForSignal(repo *repositories.Repository, name string, holds abilityCheck) bool {
	// currentLocationApproximateCoordinates is a derived signal not in the
	// definitions file; either approximate or precise location suffices.
	if name == model.ApproximateCoordinatesField {
		return holds(tokenclaims.AbilityLocationApproximate) || holds(tokenclaims.AbilityLocationPrecise)
	}
	required, ok := repo.RequiredPrivileges(name)
	if !ok {
		return false
	}
	for _, priv := range required {
		ability, mapped := privilegeEnumToAbility[priv]
		if !mapped || !holds(ability) {
			return false
		}
	}
	return true
}

// abilitiesFromCtx returns the check for the caller's token over subject at
// the current time, for the reads that have no time range: an ability is
// usable if the token holds it for the subject with no windows, or with a
// window containing now. An unauthenticated caller holds nothing.
func abilitiesFromCtx(ctx context.Context, subject string) abilityCheck {
	tok, _ := ctx.Value(ClaimsContextKey{}).(*tokenclaims.Token)
	if tok == nil {
		return func(string) bool { return false }
	}
	now := time.Now()
	return func(ability string) bool {
		ws, ok := tok.Holds(subject, ability)
		return ok && (ws == nil || ws.Contains(now))
	}
}
