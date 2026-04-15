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
	token, err := requireRawDataToken(ctx)
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

func requireRawDataToken(ctx context.Context) (*tokenclaims.Token, error) {
	tok, _ := ctx.Value(ClaimsContextKey{}).(*tokenclaims.Token)
	if tok == nil {
		return nil, fmt.Errorf("%s", errNoTokenClaims)
	}
	hasGetRawData := slices.Contains(tok.Permissions, tokenclaims.PermissionGetRawData)
	hasLocationHistory := slices.Contains(tok.Permissions, tokenclaims.PermissionGetLocationHistory)
	hasNonLocationHistory := slices.Contains(tok.Permissions, tokenclaims.PermissionGetNonLocationHistory)
	hasAllTimeData := hasLocationHistory && hasNonLocationHistory
	if !hasGetRawData && !hasAllTimeData {
		return nil, fmt.Errorf("%s", errNoPermission)
	}
	return tok, nil
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
