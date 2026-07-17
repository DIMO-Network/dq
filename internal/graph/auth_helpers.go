package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/scope"
	"github.com/DIMO-Network/dq/pkg/grpc"
)

const (
	errNoTokenClaims     = "unauthorized: no token claims"
	errNoPermission      = "unauthorized: token does not have required permission for this operation"
	errNoAccessToSubject = "unauthorized: token does not have access to this subject"
)

func (r *queryResolver) requireSubjectOptsByDID(ctx context.Context, requestedDID string, filter *model.CloudEventFilter) (*grpc.AdvancedSearchOptions, error) {
	token, err := requireRawDataToken(ctx, filter)
	if err != nil {
		return nil, err
	}
	tokenSubjectDID := token.Asset
	searchSubject, err := r.ensureRequestedDIDLinkedToPermissionedSubject(ctx, requestedDID, tokenSubjectDID)
	if err != nil {
		return nil, err
	}
	return filterToAdvancedSearchOptions(filter, searchSubject), nil
}

// requireRawDataToken authorizes a cloud-event read. Raw-data access is
// granted by the explicit GetRawData permission, or by holding both history
// permissions (all-time history implies raw-data access).
//
// When every qualifying permission is unconditional the read is unrestricted,
// matching the historical behavior. When the access derives from scoped
// permissions, the request must carry explicit after/before bounds that sit
// inside the data window — cloud-event queries are ranged reads, so a range
// wider than the window (including the implicit "all time" of an unbounded
// filter) is rejected rather than silently narrowed.
func requireRawDataToken(ctx context.Context, filter *model.CloudEventFilter) (*tokenclaims.Token, error) {
	tok, _ := ctx.Value(ClaimsContextKey{}).(*tokenclaims.Token)
	if tok == nil {
		return nil, fmt.Errorf("%s", errNoTokenClaims)
	}
	if hasUnscopedRawDataAccess(tok) {
		return tok, nil
	}

	rawDataHeld := scope.Holds(tok, tokenclaims.PermissionGetRawData)
	historyHeld := scope.Holds(tok, tokenclaims.PermissionGetLocationHistory) &&
		scope.Holds(tok, tokenclaims.PermissionGetNonLocationHistory)
	if !rawDataHeld && !historyHeld {
		return nil, fmt.Errorf("%s", errNoPermission)
	}

	from, to := requestedEventRange(filter)
	if rawDataHeld && scope.AllowsRange(tok, tokenclaims.PermissionGetRawData, from, to) {
		return tok, nil
	}
	if historyHeld &&
		scope.AllowsRange(tok, tokenclaims.PermissionGetLocationHistory, from, to) &&
		scope.AllowsRange(tok, tokenclaims.PermissionGetNonLocationHistory, from, to) {
		return tok, nil
	}
	return nil, fmt.Errorf("unauthorized: the token's raw-data access is limited to a data window; the request's after/before bounds must sit inside it")
}

// requestedEventRange resolves the half-open interval a cloud-event filter
// could touch. Missing bounds widen to the extremes so an unbounded request
// only passes an unbounded grant.
func requestedEventRange(filter *model.CloudEventFilter) (from, to time.Time) {
	from = time.Unix(0, 0).UTC()
	to = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	if filter != nil {
		if filter.After != nil {
			from = *filter.After
		}
		if filter.Before != nil {
			to = *filter.Before
		}
	}
	return from, to
}

// hasUnscopedRawDataAccess reports whether raw-data access is granted
// unconditionally: either path composed entirely of unscoped permissions.
func hasUnscopedRawDataAccess(tok *tokenclaims.Token) bool {
	if scope.Unscoped(tok, tokenclaims.PermissionGetRawData) {
		return true
	}
	return scope.Unscoped(tok, tokenclaims.PermissionGetLocationHistory) &&
		scope.Unscoped(tok, tokenclaims.PermissionGetNonLocationHistory)
}

func (r *queryResolver) ensureRequestedDIDLinkedToPermissionedSubject(ctx context.Context, requestedDID string, tokenSubjectDID string) (string, error) {
	if requestedDID == tokenSubjectDID {
		return requestedDID, nil
	}
	requestedDIDParsed, err := cloudevent.DecodeERC721DID(requestedDID)
	if err != nil {
		return "", fmt.Errorf("%s", errNoAccessToSubject)
	}
	if r.IdentityClient == nil {
		return "", fmt.Errorf("%s", errNoAccessToSubject)
	}
	linkedDID, err := r.IdentityClient.GetLinkedDIDForDevice(ctx, requestedDIDParsed.String())
	if err != nil || linkedDID != tokenSubjectDID {
		return "", fmt.Errorf("%s", errNoAccessToSubject)
	}
	return requestedDIDParsed.String(), nil
}
