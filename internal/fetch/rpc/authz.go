package rpc

import (
	"context"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/auth"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// authorizeSubject enforces that the caller's token grants access to requestedSubject,
// mirroring the GraphQL resolver's ensureRequestedDIDLinkedToPermissionedSubject. It
// returns the authorized (normalized) subject DID, or a gRPC error. Fail-closed: a
// missing claim, an empty subject, a DID that won't decode, a nil identity client, or an
// unverified device→vehicle link all deny — the fetch RPCs otherwise let any token read
// any subject's raw data.
func (s *Server) authorizeSubject(ctx context.Context, requestedSubject string) (string, error) {
	claim, ok := auth.DQClaimFromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "no token claims")
	}
	if requestedSubject == "" {
		return "", status.Error(codes.PermissionDenied, "subject is required")
	}
	tokenSubject := claim.Asset
	if requestedSubject == tokenSubject {
		return requestedSubject, nil
	}
	// Cross-subject: the requested DID must be a device linked (via identity-api) to the
	// vehicle the token is scoped to.
	parsed, err := cloudevent.DecodeERC721DID(requestedSubject)
	if err != nil {
		return "", status.Error(codes.PermissionDenied, "no access to subject")
	}
	if s.identityClient == nil {
		return "", status.Error(codes.PermissionDenied, "no access to subject")
	}
	linked, err := s.identityClient.GetLinkedDIDForDevice(ctx, parsed.String())
	if err != nil || linked != tokenSubject {
		return "", status.Error(codes.PermissionDenied, "no access to subject")
	}
	return parsed.String(), nil
}

// authorizeAdvancedOpts authorizes every subject in opts and rewrites the subject filter
// to exactly the authorized set, so a caller-supplied subject/NotIn cannot widen the read
// beyond what the token permits. An empty subject filter is rejected (no all-subjects read).
func (s *Server) authorizeAdvancedOpts(ctx context.Context, opts *grpc.AdvancedSearchOptions) error {
	if opts == nil {
		return status.Error(codes.PermissionDenied, "subject is required")
	}
	in := opts.GetSubject().GetIn()
	if len(in) == 0 {
		return status.Error(codes.PermissionDenied, "subject is required")
	}
	authorized := make([]string, 0, len(in))
	for _, subj := range in {
		a, err := s.authorizeSubject(ctx, subj)
		if err != nil {
			return err
		}
		authorized = append(authorized, a)
	}
	opts.Subject = &grpc.StringFilterOption{In: authorized}
	return nil
}

// authorizeOpts authorizes the single subject in a plain SearchOptions and pins it.
func (s *Server) authorizeOpts(ctx context.Context, opts *grpc.SearchOptions) error {
	if opts == nil {
		return status.Error(codes.PermissionDenied, "subject is required")
	}
	authorized, err := s.authorizeSubject(ctx, opts.GetSubject().GetValue())
	if err != nil {
		return err
	}
	opts.Subject = wrapperspb.String(authorized)
	return nil
}
