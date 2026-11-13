package graph

import (
	"context"
	"fmt"
	"slices"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/repositories"
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

func hasPrivilegesForSignal(repo *repositories.Repository, name string, permissions []string) bool {
	// currentLocationApproximateCoordinates is a derived signal not in the
	// definitions file; either approximate or all-time location suffices.
	if name == model.ApproximateCoordinatesField {
		return slices.Contains(permissions, tokenclaims.PermissionGetApproximateLocation) ||
			slices.Contains(permissions, tokenclaims.PermissionGetLocationHistory)
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
		if !slices.Contains(permissions, perm) {
			return false
		}
	}
	return true
}

// unknownSignalPermissions is what reading a signal with no definition requires:
// nothing says whether it carries location, so it needs both history permissions.
var unknownSignalPermissions = []string{
	tokenclaims.PermissionGetNonLocationHistory,
	tokenclaims.PermissionGetLocationHistory,
}

// canReadSignal reports whether the caller in ctx may read the named signal of
// subject. It is the one authorization decision behind latestSignals, so a finer
// grained or per-subject policy replaces this body and nothing else. The subject is
// part of the question for that reason, though the current policy ignores it.
func canReadSignal(ctx context.Context, repo *repositories.Repository, _ string, name string) bool {
	perms := permissionsFromCtx(ctx)
	if _, defined := repo.RequiredPrivileges(name); defined || name == model.ApproximateCoordinatesField {
		return hasPrivilegesForSignal(repo, name, perms)
	}
	for _, perm := range unknownSignalPermissions {
		if !slices.Contains(perms, perm) {
			return false
		}
	}
	return true
}

// requireSignalRead is canReadSignal as a field error.
func requireSignalRead(ctx context.Context, repo *repositories.Repository, subject, name string) error {
	if !canReadSignal(ctx, repo, subject, name) {
		return fmt.Errorf("unauthorized: token does not have the privileges required for signal %q", name)
	}
	return nil
}

// permissionsFromCtx returns the caller's token permissions, or nil when unauthenticated.
func permissionsFromCtx(ctx context.Context) []string {
	if tok, _ := ctx.Value(ClaimsContextKey{}).(*tokenclaims.Token); tok != nil {
		return tok.Permissions
	}
	return nil
}
