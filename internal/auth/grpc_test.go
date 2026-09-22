package auth

import (
	"context"
	"testing"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestBearerFromMetadata(t *testing.T) {
	cases := []struct {
		name string
		md   metadata.MD
		want string
	}{
		{"no incoming metadata", nil, ""},
		{"Bearer scheme", metadata.Pairs("authorization", "Bearer tok123"), "tok123"},
		{"lowercase bearer", metadata.Pairs("authorization", "bearer tok123"), "tok123"},
		{"uppercase BEARER (case-insensitive)", metadata.Pairs("authorization", "BEARER tok123"), "tok123"},
		{"DPoP scheme", metadata.Pairs("authorization", "DPoP tok123"), "tok123"},
		{"raw token (no scheme)", metadata.Pairs("authorization", "tok123"), "tok123"},
		{"unrelated header", metadata.Pairs("other", "x"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tc.md)
			}
			assert.Equal(t, tc.want, bearerFromMetadata(ctx))
		})
	}
}

func TestGRPCHasRawDataAccess(t *testing.T) {
	assert.True(t, grpcHasRawDataAccess(&tokenclaims.Token{Grants: []tokenclaims.Grant{
		{Subject: "did:dimo:car", Abilities: []string{tokenclaims.AbilityRawRead}},
	}}), "raw:read grants access")
	assert.False(t, grpcHasRawDataAccess(&tokenclaims.Token{Grants: []tokenclaims.Grant{
		{Subject: "did:dimo:car", Abilities: []string{tokenclaims.AbilityTelemetryRead, tokenclaims.AbilityLocationPrecise}},
	}}), "history abilities alone do not")
	assert.False(t, grpcHasRawDataAccess(&tokenclaims.Token{}), "no grants → no access")
}
