package rpc

import (
	"context"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/auth"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// authorizeSubject enforces that the caller's token holds raw:read for
// requestedSubject and returns its windows (nil for none). Fail-closed: a
// missing claim, an empty subject or a subject the token does not name all
// deny, since the fetch RPCs otherwise let any token read any subject's raw
// data.
func (s *Server) authorizeSubject(ctx context.Context, requestedSubject string) (tokenclaims.Windows, error) {
	claim, ok := auth.DQClaimFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no token claims")
	}
	if requestedSubject == "" {
		return nil, status.Error(codes.PermissionDenied, "subject is required")
	}
	ws, ok := claim.Holds(requestedSubject, tokenclaims.AbilityRawRead)
	if !ok {
		return nil, status.Error(codes.PermissionDenied, "no access to subject")
	}
	return ws, nil
}

// authorizeAdvancedOpts authorizes every subject in opts, pins the subject
// filter to exactly that set so a caller-supplied NotIn cannot widen the read,
// and clamps the time bounds to the windows the token holds. Several subjects
// with different windows are refused: one read, one window set. An empty
// subject filter is rejected (no all-subjects read).
func (s *Server) authorizeAdvancedOpts(ctx context.Context, opts *grpc.AdvancedSearchOptions) error {
	if opts == nil {
		return status.Error(codes.PermissionDenied, "subject is required")
	}
	in := opts.GetSubject().GetIn()
	if len(in) == 0 {
		return status.Error(codes.PermissionDenied, "subject is required")
	}
	var windows tokenclaims.Windows
	for i, subj := range in {
		ws, err := s.authorizeSubject(ctx, subj)
		if err != nil {
			return err
		}
		if i == 0 {
			windows = ws
			continue
		}
		if !sameWindows(windows, ws) {
			return status.Error(codes.PermissionDenied, "subjects with different coverage windows must be fetched separately")
		}
	}
	opts.Subject = &grpc.StringFilterOption{In: in}
	if windows == nil {
		return nil
	}
	after, before, err := clampBounds(windows, opts.After, opts.Before)
	if err != nil {
		return err
	}
	opts.After, opts.Before = after, before
	return nil
}

// authorizeOpts authorizes the single subject in a plain SearchOptions and pins it.
func (s *Server) authorizeOpts(ctx context.Context, opts *grpc.SearchOptions) error {
	if opts == nil {
		return status.Error(codes.PermissionDenied, "subject is required")
	}
	subject := opts.GetSubject().GetValue()
	ws, err := s.authorizeSubject(ctx, subject)
	if err != nil {
		return err
	}
	opts.Subject = wrapperspb.String(subject)
	if ws == nil {
		return nil
	}
	after, before, err := clampBounds(ws, opts.After, opts.Before)
	if err != nil {
		return err
	}
	opts.After, opts.Before = after, before
	return nil
}

func sameWindows(a, b tokenclaims.Windows) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameTime(a[i].Start, b[i].Start) || !sameTime(a[i].End, b[i].End) {
			return false
		}
	}
	return true
}

func sameTime(a, b *time.Time) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Equal(*b)
	}
}

// clampBounds narrows an optional [after, before) to the windows.
func clampBounds(ws tokenclaims.Windows, after, before *timestamppb.Timestamp) (*timestamppb.Timestamp, *timestamppb.Timestamp, error) {
	var from, to *time.Time
	if after != nil {
		t := after.AsTime()
		from = &t
	}
	if before != nil {
		t := before.AsTime()
		to = &t
	}
	pieces := ws.ClampOpen(from, to)
	switch len(pieces) {
	case 0:
		return nil, nil, status.Error(codes.PermissionDenied, "token covers nothing in the requested range")
	case 1:
		var outAfter, outBefore *timestamppb.Timestamp
		if pieces[0].Start != nil {
			outAfter = timestamppb.New(*pieces[0].Start)
		}
		if pieces[0].End != nil {
			outBefore = timestamppb.New(*pieces[0].End)
		}
		return outAfter, outBefore, nil
	default:
		return nil, nil, status.Error(codes.PermissionDenied, "requested range straddles a gap in coverage; fetch the covered ranges separately")
	}
}
