package graph

import (
	"context"
	"fmt"
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

// unknownSignalAbilities is what reading a signal with no definition requires:
// nothing says whether it carries location, so it needs both telemetry and
// precise location.
var unknownSignalAbilities = []string{
	tokenclaims.AbilityTelemetryRead,
	tokenclaims.AbilityLocationPrecise,
}

// canReadSignal reports whether the caller in ctx may read the named signal of
// subject now. It is the one authorization decision behind latestSignals. Like
// the other reads with no time range, an ability counts only if the token holds
// it for the subject with no windows or with a window containing now; the
// repository then drops any value timestamped outside the token's windows.
func canReadSignal(ctx context.Context, repo *repositories.Repository, subject, name string) bool {
	holds := abilitiesFromCtx(ctx, subject)
	if _, defined := repo.RequiredPrivileges(name); defined || name == model.ApproximateCoordinatesField {
		return hasPrivilegesForSignal(repo, name, holds)
	}
	for _, ability := range unknownSignalAbilities {
		if !holds(ability) {
			return false
		}
	}
	return true
}

// requireSignalRead is canReadSignal as a field error.
func requireSignalRead(ctx context.Context, repo *repositories.Repository, subject, name string) error {
	if !canReadSignal(ctx, repo, subject, name) {
		return fmt.Errorf("unauthorized: token does not hold the abilities required for signal %q", name)
	}
	return nil
}
