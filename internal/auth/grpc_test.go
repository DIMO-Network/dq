package auth

import (
	"context"
	"testing"

	"github.com/DIMO-Network/token-exchange-api/pkg/tokenclaims"
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
	assert.True(t, grpcHasRawDataAccess([]string{tokenclaims.PermissionGetRawData}),
		"explicit get-raw-data permission grants access")
	assert.True(t, grpcHasRawDataAccess([]string{
		tokenclaims.PermissionGetLocationHistory, tokenclaims.PermissionGetNonLocationHistory}),
		"both history permissions together grant access")
	assert.False(t, grpcHasRawDataAccess([]string{tokenclaims.PermissionGetLocationHistory}),
		"a single history permission is insufficient")
	assert.False(t, grpcHasRawDataAccess(nil), "no permissions → no access")
}
