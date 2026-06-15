package graph

import (
	"context"
	"fmt"
	"slices"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/DIMO-Network/token-exchange-api/pkg/tokenclaims"
)

const (
	errNoTokenClaims     = "unauthorized: no token claims"
	errNoPermission      = "unauthorized: token does not have required permission for this operation"
	errNoAccessToSubject = "unauthorized: token does not have access to this subject"
)

func (r *queryResolver) requireSubjectOptsByDID(ctx context.Context, requestedDID string, filter *model.CloudEventFilter) (*grpc.AdvancedSearchOptions, error) {
	tok, _ := ctx.Value(ClaimsContextKey{}).(*tokenclaims.Token)
	if tok == nil {
		return nil, fmt.Errorf("%s", errNoTokenClaims)
	}
	// Two independent authorization paths, OR'd together:
	//   1. The permissions enum grants full access to ALL of the subject's events.
	//   2. The cloud_events grant, evaluated independently of the enum, authorizes exactly the
	//      types/sources/ids its rules describe.
	if !hasFullCloudEventAccess(tok) && !cloudEventRequestAllowed(tok.CloudEvents, filter) {
		return nil, fmt.Errorf("%s", errNoPermission)
	}
	// Subject scoping applies regardless of which path authorized: a grant narrows WHICH
	// types/sources/ids within the subject, it is not a license to read a different subject.
	searchSubject, err := r.ensureRequestedDIDLinkedToPermissionedSubject(ctx, requestedDID, tok.Asset)
	if err != nil {
		return nil, err
	}
	return filterToAdvancedSearchOptions(filter, searchSubject), nil
}

// hasFullCloudEventAccess reports whether the permissions enum grants access to ALL of the
// subject's events: either raw-data, or the (location + non-location) combination.
func hasFullCloudEventAccess(tok *tokenclaims.Token) bool {
	hasGetRawData := slices.Contains(tok.Permissions, tokenclaims.PermissionGetRawData)
	hasLocationHistory := slices.Contains(tok.Permissions, tokenclaims.PermissionGetLocationHistory)
	hasNonLocationHistory := slices.Contains(tok.Permissions, tokenclaims.PermissionGetNonLocationHistory)
	return hasGetRawData || (hasLocationHistory && hasNonLocationHistory)
}

// cloudEventRequestAllowed reports whether every event the given filter could match falls within
// at least one of the bearer's cloud_events grants. It is evaluated independently of the
// permissions enum and is fail-closed: an absent grant, or a filter broader than any grant,
// returns false.
func cloudEventRequestAllowed(grants *tokenclaims.CloudEvents, filter *model.CloudEventFilter) bool {
	if grants == nil || len(grants.Events) == 0 {
		return false
	}
	// Requested types = filter.Type + filter.Types. An empty set means "any type", which only a
	// wildcard grant can cover.
	var types []string
	if filter != nil {
		if filter.Type != nil {
			types = append(types, *filter.Type)
		}
		types = append(types, filter.Types...)
	}
	if len(types) == 0 {
		types = []string{tokenclaims.GlobalIdentifier}
	}
	source := tokenclaims.GlobalIdentifier
	id := tokenclaims.GlobalIdentifier
	if filter != nil {
		if filter.Source != nil {
			source = *filter.Source
		}
		if filter.ID != nil {
			id = *filter.ID
		}
	}
	// The query returns the union over requested types, so EACH requested type must be covered by
	// some grant.
	for _, t := range types {
		if !grantCovers(grants.Events, t, source, id) {
			return false
		}
	}
	return true
}

// grantCovers reports whether any single grant authorizes the (type, source, id) tuple. A grant
// matches a dimension when it equals the requested value or carries the GlobalIdentifier wildcard.
func grantCovers(events []tokenclaims.Event, evtType, source, id string) bool {
	for _, ce := range events {
		if ce.EventType != evtType && ce.EventType != tokenclaims.GlobalIdentifier {
			continue
		}
		if ce.Source != source && ce.Source != tokenclaims.GlobalIdentifier {
			continue
		}
		if slices.Contains(ce.IDs, id) || slices.Contains(ce.IDs, tokenclaims.GlobalIdentifier) {
			return true
		}
	}
	return false
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
