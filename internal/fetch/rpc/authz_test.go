package rpc

import (
	"context"
	"errors"
	"testing"

	"github.com/DIMO-Network/dq/internal/auth"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/DIMO-Network/token-exchange-api/pkg/tokenclaims"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	authSubjA   = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42"
	authSubjB   = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:99"
	authDeviceD = "did:erc721:137:0x9c94C395cBcBDe662235E0A9d3bB87Ad708561BA:7"
)

// ctxWithSubject returns a context carrying a validated DQ claim scoped to subject,
// as the gRPC auth interceptor would set after validating the token.
func ctxWithSubject(subject string) context.Context {
	return context.WithValue(context.Background(), auth.DQClaimContextKey{},
		&auth.DQClaim{Token: tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{Asset: subject}}})
}

type fakeIdentity struct {
	returns string
	err     error
}

func (f fakeIdentity) GetLinkedDIDForDevice(context.Context, string) (string, error) {
	return f.returns, f.err
}

func TestAuthorizeSubject(t *testing.T) {
	t.Run("own subject allowed", func(t *testing.T) {
		got, err := NewServer(emptyEventService{}, nil).authorizeSubject(ctxWithSubject(authSubjA), authSubjA)
		require.NoError(t, err)
		assert.Equal(t, authSubjA, got)
	})
	t.Run("other vehicle denied (nil identity)", func(t *testing.T) {
		_, err := NewServer(emptyEventService{}, nil).authorizeSubject(ctxWithSubject(authSubjA), authSubjB)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
	t.Run("empty subject denied", func(t *testing.T) {
		_, err := NewServer(emptyEventService{}, nil).authorizeSubject(ctxWithSubject(authSubjA), "")
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
	t.Run("no claim unauthenticated", func(t *testing.T) {
		_, err := NewServer(emptyEventService{}, nil).authorizeSubject(context.Background(), authSubjA)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})
	t.Run("cross-subject linked allowed", func(t *testing.T) {
		got, err := NewServer(emptyEventService{}, fakeIdentity{returns: authSubjA}).
			authorizeSubject(ctxWithSubject(authSubjA), authDeviceD)
		require.NoError(t, err)
		assert.NotEmpty(t, got)
	})
	t.Run("cross-subject unlinked denied", func(t *testing.T) {
		_, err := NewServer(emptyEventService{}, fakeIdentity{returns: authSubjB}).
			authorizeSubject(ctxWithSubject(authSubjA), authDeviceD)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
	t.Run("cross-subject identity error denied", func(t *testing.T) {
		_, err := NewServer(emptyEventService{}, fakeIdentity{err: errors.New("boom")}).
			authorizeSubject(ctxWithSubject(authSubjA), authDeviceD)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
	t.Run("cross-subject nil identity denied", func(t *testing.T) {
		_, err := NewServer(emptyEventService{}, nil).authorizeSubject(ctxWithSubject(authSubjA), authDeviceD)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
}

// TestListCloudEvents_CrossSubjectDenied is the F1 regression at the RPC layer: a token
// for vehicle A must not read vehicle B's events; an own-subject request passes authz.
func TestListCloudEvents_CrossSubjectDenied(t *testing.T) {
	s := NewServer(emptyEventService{}, nil)

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
	_, err := NewServer(emptyEventService{}, nil).ListCloudEventsFromIndex(ctxWithSubject(authSubjA),
		&grpc.ListCloudEventsFromKeysRequest{
			Indexes: []*grpc.CloudEventIndex{
				{Data: &grpc.ObjectInfo{Key: "cloudevent/blobs/x"}, Header: &grpc.CloudEventHeader{Subject: authSubjB}},
			},
		})
	assert.Equal(t, codes.PermissionDenied, status.Code(err), "a crafted index for subject B must be rejected")
}
