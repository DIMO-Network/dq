package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	errNoTokenClaims     = "unauthorized: no token claims"
	errNoPermission      = "unauthorized: token does not hold raw:read for this subject"
	errNoAccessToSubject = "unauthorized: token does not have access to this subject"
)

// requireSubjectOptsByDID checks that the token holds raw:read for the
// requested subject and builds the search options, with the time bounds
// clamped to the ability's windows: an unbounded filter becomes bounded by
// the coverage, a bounded one is narrowed, and a filter that straddles a gap
// in coverage is refused so the caller splits it.
func (r *queryResolver) requireSubjectOptsByDID(ctx context.Context, requestedDID string, filter *model.CloudEventFilter) (*grpc.AdvancedSearchOptions, error) {
	tok, _ := ctx.Value(ClaimsContextKey{}).(*tokenclaims.Token)
	if tok == nil {
		return nil, fmt.Errorf("%s", errNoTokenClaims)
	}
	if !tok.Covers(requestedDID) {
		return nil, fmt.Errorf("%s", errNoAccessToSubject)
	}
	ws, ok := tok.Holds(requestedDID, tokenclaims.AbilityRawRead)
	if !ok {
		return nil, fmt.Errorf("%s", errNoPermission)
	}
	opts := filterToAdvancedSearchOptions(filter, requestedDID)
	if ws == nil {
		return opts, nil
	}
	after, before, err := clampSearch(ws, opts.After, opts.Before)
	if err != nil {
		return nil, err
	}
	opts.After, opts.Before = after, before
	return opts, nil
}

// clampSearch narrows an optional [after, before) to the windows.
func clampSearch(ws tokenclaims.Windows, after, before *timestamppb.Timestamp) (*timestamppb.Timestamp, *timestamppb.Timestamp, error) {
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
		return nil, nil, fmt.Errorf("unauthorized: token covers nothing in the requested range")
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
		return nil, nil, fmt.Errorf("unauthorized: requested range straddles a gap in coverage; query the covered ranges separately")
	}
}
