package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/auth"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	authSubjA = "did:dimo:vehicleA"
	authSubjB = "did:dimo:vehicleB"
)

func ts(s string) *time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return &t
}

// ctxWithGrants returns a context carrying a validated DQ claim with the given
// grants, as the gRPC auth interceptor would set after validating the token.
func ctxWithGrants(grants ...tokenclaims.Grant) context.Context {
	return context.WithValue(context.Background(), auth.DQClaimContextKey{},
		&auth.DQClaim{Token: tokenclaims.Token{Grants: grants}})
}

// ctxWithSubject is a token holding raw:read for subject with no window.
func ctxWithSubject(subject string) context.Context {
	return ctxWithGrants(tokenclaims.Grant{Subject: subject, Abilities: []string{tokenclaims.AbilityRawRead}})
}

func TestAuthorizeSubject(t *testing.T) {
	s := NewServer(emptyEventService{})
	t.Run("own subject allowed", func(t *testing.T) {
		ws, err := s.authorizeSubject(ctxWithSubject(authSubjA), authSubjA)
		require.NoError(t, err)
		assert.Nil(t, ws)
	})
	t.Run("other vehicle denied", func(t *testing.T) {
		_, err := s.authorizeSubject(ctxWithSubject(authSubjA), authSubjB)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
	t.Run("subject held without raw:read denied", func(t *testing.T) {
		ctx := ctxWithGrants(tokenclaims.Grant{Subject: authSubjA, Abilities: []string{tokenclaims.AbilityTelemetryRead}})
		_, err := s.authorizeSubject(ctx, authSubjA)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
	t.Run("empty subject denied", func(t *testing.T) {
		_, err := s.authorizeSubject(ctxWithSubject(authSubjA), "")
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
	t.Run("no claim unauthenticated", func(t *testing.T) {
		_, err := s.authorizeSubject(context.Background(), authSubjA)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

func TestAuthorizeAdvancedOptsClampsToWindows(t *testing.T) {
	s := NewServer(emptyEventService{})
	ctx := ctxWithGrants(tokenclaims.Grant{
		Subject: authSubjA, Abilities: []string{tokenclaims.AbilityRawRead},
		Windows: tokenclaims.Windows{
			{Start: ts("2026-01-01T00:00:00Z"), End: ts("2026-02-01T00:00:00Z")},
			{Start: ts("2026-03-01T00:00:00Z")},
		},
	})

	t.Run("unbounded request is bounded by coverage", func(t *testing.T) {
		opts := &grpc.AdvancedSearchOptions{Subject: &grpc.StringFilterOption{In: []string{authSubjA}}, After: timestamppb.New(*ts("2026-03-10T00:00:00Z"))}
		require.NoError(t, s.authorizeAdvancedOpts(ctx, opts))
		assert.Equal(t, *ts("2026-03-10T00:00:00Z"), opts.After.AsTime())
		assert.Nil(t, opts.Before)
	})
	t.Run("request is narrowed", func(t *testing.T) {
		opts := &grpc.AdvancedSearchOptions{Subject: &grpc.StringFilterOption{In: []string{authSubjA}},
			After: timestamppb.New(*ts("2025-12-01T00:00:00Z")), Before: timestamppb.New(*ts("2026-01-15T00:00:00Z"))}
		require.NoError(t, s.authorizeAdvancedOpts(ctx, opts))
		assert.Equal(t, *ts("2026-01-01T00:00:00Z"), opts.After.AsTime())
		assert.Equal(t, *ts("2026-01-15T00:00:00Z"), opts.Before.AsTime())
	})
	t.Run("the gap is denied", func(t *testing.T) {
		opts := &grpc.AdvancedSearchOptions{Subject: &grpc.StringFilterOption{In: []string{authSubjA}},
			After: timestamppb.New(*ts("2026-02-01T00:00:00Z")), Before: timestamppb.New(*ts("2026-03-01T00:00:00Z"))}
		assert.Equal(t, codes.PermissionDenied, status.Code(s.authorizeAdvancedOpts(ctx, opts)))
	})
	t.Run("straddling the gap is denied", func(t *testing.T) {
		opts := &grpc.AdvancedSearchOptions{Subject: &grpc.StringFilterOption{In: []string{authSubjA}}}
		assert.Equal(t, codes.PermissionDenied, status.Code(s.authorizeAdvancedOpts(ctx, opts)))
	})
	t.Run("NotIn cannot widen the read", func(t *testing.T) {
		opts := &grpc.AdvancedSearchOptions{Subject: &grpc.StringFilterOption{In: []string{authSubjA}, NotIn: []string{authSubjB}}, After: timestamppb.New(*ts("2026-03-10T00:00:00Z"))}
		require.NoError(t, s.authorizeAdvancedOpts(ctx, opts))
		assert.Equal(t, []string{authSubjA}, opts.Subject.In)
		assert.Empty(t, opts.Subject.NotIn)
	})
}

// TestListCloudEvents_CrossSubjectDenied is the F1 regression at the RPC layer: a token
// for vehicle A must not read vehicle B's events; an own-subject request passes authz.
func TestListCloudEvents_CrossSubjectDenied(t *testing.T) {
	s := NewServer(emptyEventService{})

	_, err := s.ListCloudEvents(ctxWithSubject(authSubjA), &grpc.ListCloudEventsRequest{
		AdvancedOptions: &grpc.AdvancedSearchOptions{Subject: &grpc.StringFilterOption{In: []string{authSubjB}}},
	})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "token A must not read subject B")

	_, err = s.ListCloudEvents(ctxWithSubject(authSubjA), &grpc.ListCloudEventsRequest{
		AdvancedOptions: &grpc.AdvancedSearchOptions{Subject: &grpc.StringFilterOption{In: []string{authSubjA}}},
	})
	assert.Equal(t, codes.NotFound, status.Code(err), "own-subject request passes authz and reaches the empty backend")
}

// TestListCloudEventsFromIndex_CrossSubjectDenied proves each index's subject is authorized.
func TestListCloudEventsFromIndex_CrossSubjectDenied(t *testing.T) {
	_, err := NewServer(emptyEventService{}).ListCloudEventsFromIndex(ctxWithSubject(authSubjA),
		&grpc.ListCloudEventsFromKeysRequest{
			Indexes: []*grpc.CloudEventIndex{
				{Data: &grpc.ObjectInfo{Key: "cloudevent/blobs/x"}, Header: &grpc.CloudEventHeader{Subject: authSubjB}},
			},
		})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "a crafted index for subject B must be rejected")
}
