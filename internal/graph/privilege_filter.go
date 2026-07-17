package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/scope"
)

// privilegeEnumToPermission maps GraphQL Privilege enum values (as they appear
// in model-garage definitions.yaml) to tokenclaims permission strings.
var privilegeEnumToPermission = map[string]string{
	"VEHICLE_NON_LOCATION_DATA":    tokenclaims.PermissionGetNonLocationHistory,
	"VEHICLE_COMMANDS":             tokenclaims.PermissionExecuteCommands,
	"VEHICLE_CURRENT_LOCATION":     tokenclaims.PermissionGetCurrentLocation,
	"VEHICLE_ALL_TIME_LOCATION":    tokenclaims.PermissionGetLocationHistory,
	"VEHICLE_VIN_CREDENTIAL":       tokenclaims.PermissionGetVINCredential,
	"VEHICLE_APPROXIMATE_LOCATION": tokenclaims.PermissionGetApproximateLocation,
	"VEHICLE_RAW_DATA":             tokenclaims.PermissionGetRawData,
}

// signalPermissionCheck evaluates one permission-level predicate for every
// permission a signal requires. It resolves the signal's required privileges
// (failing closed for unknown signals) and handles the derived
// approximate-coordinates signal, which is satisfiable by either the
// approximate- or all-time-location permission.
func signalPermissionCheck(repo *repositories.Repository, name string, tok *tokenclaims.Token, allowed func(*tokenclaims.Token, string) bool) bool {
	if name == model.ApproximateCoordinatesField {
		return allowed(tok, tokenclaims.PermissionGetApproximateLocation) ||
			allowed(tok, tokenclaims.PermissionGetLocationHistory)
	}
	required, ok := repo.RequiredPrivileges(name)
	if !ok {
		return false
	}
	for _, priv := range required {
		perm, mapped := privilegeEnumToPermission[priv]
		if !mapped {
			return false
		}
		if !allowed(tok, perm) {
			return false
		}
	}
	return true
}

// hasPrivilegesForSignal reports whether the token holds every permission the
// named signal requires, scoped or not. Callers are responsible for enforcing
// scoped grants' data windows on whatever they return (range checks before
// querying, or per-value timestamp checks after).
func hasPrivilegesForSignal(repo *repositories.Repository, name string, tok *tokenclaims.Token) bool {
	return signalPermissionCheck(repo, name, tok, scope.Holds)
}

// hasUnscopedPrivilegesForSignal reports whether the token holds every
// permission the named signal requires unconditionally. Surfaces that expose
// all-time facts about a signal (counts, first/last seen) and cannot yet
// window them must gate on this.
func hasUnscopedPrivilegesForSignal(repo *repositories.Repository, name string, tok *tokenclaims.Token) bool {
	return signalPermissionCheck(repo, name, tok, scope.Unscoped)
}

// signalRangeAllowed reports whether the token may read the named signal over
// the half-open interval [from, to): every required permission must be held
// and its data window (if any) must contain the interval.
func signalRangeAllowed(repo *repositories.Repository, name string, tok *tokenclaims.Token, from, to time.Time) bool {
	return signalPermissionCheck(repo, name, tok, func(tok *tokenclaims.Token, perm string) bool {
		return scope.AllowsRange(tok, perm, from, to)
	})
}

// signalAllowsAt reports whether the token may see a value of the named signal
// recorded at the instant t.
func signalAllowsAt(repo *repositories.Repository, name string, tok *tokenclaims.Token, t time.Time) bool {
	return signalPermissionCheck(repo, name, tok, func(tok *tokenclaims.Token, perm string) bool {
		return scope.AllowsAt(tok, perm, t)
	})
}

// signalValueVisible reports whether a fetched value of the named signal,
// recorded at ts, may be shown. Unlike signalAllowsAt it does not re-check
// possession — that is the field directives' job on the latest path — it only
// vetoes values excluded by a held-but-scoped permission's window.
func signalValueVisible(repo *repositories.Repository, name string, tok *tokenclaims.Token, ts time.Time) bool {
	if name == model.ApproximateCoordinatesField {
		return approxLocationVisible(tok, ts)
	}
	required, ok := repo.RequiredPrivileges(name)
	if !ok {
		return false
	}
	for _, priv := range required {
		perm, mapped := privilegeEnumToPermission[priv]
		if !mapped {
			return false
		}
		if scope.Holds(tok, perm) && !scope.AllowsAt(tok, perm, ts) {
			return false
		}
	}
	return true
}

// approxLocationVisible reports whether a derived approximate-location value
// at ts may be shown: either qualifying permission must be held AND allow the
// timestamp.
func approxLocationVisible(tok *tokenclaims.Token, ts time.Time) bool {
	return scope.AllowsAt(tok, tokenclaims.PermissionGetApproximateLocation, ts) ||
		scope.AllowsAt(tok, tokenclaims.PermissionGetLocationHistory, ts)
}

// signalWindowDescription renders the data-window constraints relevant to the
// named signal for error messages; empty when none of its permissions are
// scoped.
func signalWindowDescription(repo *repositories.Repository, name string, tok *tokenclaims.Token) string {
	var perms []string
	if name == model.ApproximateCoordinatesField {
		perms = []string{tokenclaims.PermissionGetApproximateLocation, tokenclaims.PermissionGetLocationHistory}
	} else if required, ok := repo.RequiredPrivileges(name); ok {
		for _, priv := range required {
			if perm, mapped := privilegeEnumToPermission[priv]; mapped {
				perms = append(perms, perm)
			}
		}
	}
	for _, perm := range perms {
		if desc := scope.Describe(tok, perm); desc != "" {
			return desc
		}
	}
	return ""
}

// eventsRangeAllowed rejects a query over [from, to) when either history
// permission (the pair required by the events, segments, and dailyActivity
// queries) is scoped to a data window that does not contain the range.
// Possession itself is the schema directives' job.
func eventsRangeAllowed(ctx context.Context, from, to time.Time) error {
	tok := tokenFromCtx(ctx)
	if !hasScopedPermissions(tok) {
		return nil
	}
	for _, perm := range []string{tokenclaims.PermissionGetNonLocationHistory, tokenclaims.PermissionGetLocationHistory} {
		if scope.Holds(tok, perm) && !scope.AllowsRange(tok, perm, from, to) {
			if desc := scope.Describe(tok, perm); desc != "" {
				return fmt.Errorf("unauthorized: requested range is outside the token's data window: %s is %s", perm, desc)
			}
			return fmt.Errorf("unauthorized: token does not allow %s over the requested range", perm)
		}
	}
	return nil
}

// tokenFromCtx returns the caller's permission token, or nil when
// unauthenticated.
func tokenFromCtx(ctx context.Context) *tokenclaims.Token {
	tok, _ := ctx.Value(ClaimsContextKey{}).(*tokenclaims.Token)
	return tok
}

// hasScopedPermissions reports whether any permission on the token is granted
// under constraints. Surfaces that expose cross-signal facts (e.g. the
// lastSeen timestamp, computed over all signals) suppress them for such
// tokens until they learn to window them.
func hasScopedPermissions(tok *tokenclaims.Token) bool {
	return tok != nil && len(tok.ScopedPermissions) > 0
}
