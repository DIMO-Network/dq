package scope

import (
	"testing"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/stretchr/testify/assert"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func windowedToken() *tokenclaims.Token {
	return &tokenclaims.Token{
		CustomClaims: tokenclaims.CustomClaims{
			Permissions: []string{tokenclaims.PermissionGetNonLocationHistory},
			ScopedPermissions: []tokenclaims.ScopedPermission{{
				Name: tokenclaims.PermissionGetLocationHistory,
				Constraint: []tokenclaims.Constraint{
					{LeftOperand: tokenclaims.LeftOperandRecordedAt, Operator: tokenclaims.OperatorGteq, RightOperand: "2026-04-01T00:00:00Z"},
					{LeftOperand: tokenclaims.LeftOperandRecordedAt, Operator: tokenclaims.OperatorLt, RightOperand: "2026-07-01T00:00:00Z"},
				},
			}},
		},
	}
}

func TestHoldsAndUnscoped(t *testing.T) {
	tok := windowedToken()

	assert.True(t, Holds(tok, tokenclaims.PermissionGetNonLocationHistory))
	assert.True(t, Holds(tok, tokenclaims.PermissionGetLocationHistory))
	assert.False(t, Holds(tok, tokenclaims.PermissionGetRawData))

	assert.True(t, Unscoped(tok, tokenclaims.PermissionGetNonLocationHistory))
	assert.False(t, Unscoped(tok, tokenclaims.PermissionGetLocationHistory))
	assert.False(t, Unscoped(tok, tokenclaims.PermissionGetRawData))

	assert.False(t, Holds(nil, tokenclaims.PermissionGetNonLocationHistory))
}

func TestAllowsRange(t *testing.T) {
	tok := windowedToken()
	loc := tokenclaims.PermissionGetLocationHistory
	nonLoc := tokenclaims.PermissionGetNonLocationHistory

	// Unconditional grants allow any range.
	assert.True(t, AllowsRange(tok, nonLoc, ts("1970-01-01T00:00:00Z"), ts("2100-01-01T00:00:00Z")))
	// Scoped grants allow ranges inside the window...
	assert.True(t, AllowsRange(tok, loc, ts("2026-05-01T00:00:00Z"), ts("2026-06-01T00:00:00Z")))
	// ...and reject ranges that exceed it on either side.
	assert.False(t, AllowsRange(tok, loc, ts("2026-03-01T00:00:00Z"), ts("2026-06-01T00:00:00Z")))
	assert.False(t, AllowsRange(tok, loc, ts("2026-05-01T00:00:00Z"), ts("2026-08-01T00:00:00Z")))
	// Unheld permissions allow nothing.
	assert.False(t, AllowsRange(tok, tokenclaims.PermissionGetRawData, ts("2026-05-01T00:00:00Z"), ts("2026-06-01T00:00:00Z")))
}

func TestAllowsAt(t *testing.T) {
	tok := windowedToken()
	loc := tokenclaims.PermissionGetLocationHistory

	assert.True(t, AllowsAt(tok, loc, ts("2026-05-15T12:00:00Z")))
	assert.False(t, AllowsAt(tok, loc, ts("2026-03-15T12:00:00Z")))
	assert.False(t, AllowsAt(tok, loc, ts("2026-07-01T00:00:00Z"))) // lt bound is exclusive
	assert.True(t, AllowsAt(tok, tokenclaims.PermissionGetNonLocationHistory, ts("1999-01-01T00:00:00Z")))
}

func TestUninterpretableConstraintsGrantNothing(t *testing.T) {
	tok := &tokenclaims.Token{
		CustomClaims: tokenclaims.CustomClaims{
			ScopedPermissions: []tokenclaims.ScopedPermission{{
				Name: tokenclaims.PermissionGetLocationHistory,
				Constraint: []tokenclaims.Constraint{
					{LeftOperand: "dimo:geofence", Operator: "within", RightOperand: "POLYGON(...)"},
				},
			}},
		},
	}
	loc := tokenclaims.PermissionGetLocationHistory

	// Held for possession purposes...
	assert.True(t, Holds(tok, loc))
	// ...but no data access: the constraint is not understood, so fail closed.
	assert.False(t, AllowsRange(tok, loc, ts("2026-05-01T00:00:00Z"), ts("2026-06-01T00:00:00Z")))
	assert.False(t, AllowsAt(tok, loc, ts("2026-05-15T12:00:00Z")))
	assert.Equal(t, "granted under constraints this service cannot interpret", Describe(tok, loc))
}

func TestDescribe(t *testing.T) {
	tok := windowedToken()
	assert.Equal(t,
		"granted for data recorded >= 2026-04-01T00:00:00Z and < 2026-07-01T00:00:00Z",
		Describe(tok, tokenclaims.PermissionGetLocationHistory))
	assert.Empty(t, Describe(tok, tokenclaims.PermissionGetNonLocationHistory))
	assert.Empty(t, Describe(tok, tokenclaims.PermissionGetRawData))
}
