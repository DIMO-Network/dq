package graph

import (
	"context"
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

// permissionsFromCtx returns the caller's token permissions, or nil when unauthenticated.
func permissionsFromCtx(ctx context.Context) []string {
	if tok, _ := ctx.Value(ClaimsContextKey{}).(*tokenclaims.Token); tok != nil {
		return tok.Permissions
	}
	return nil
}
