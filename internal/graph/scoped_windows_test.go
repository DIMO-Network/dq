package graph

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustTS(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return ts
}

func testRepo(t *testing.T) *repositories.Repository {
	t.Helper()
	repo, err := repositories.NewRepository(nil)
	require.NoError(t, err)
	return repo
}

// q2WindowedToken holds non-location history unconditionally and location
// history only for Q2 2026.
func q2WindowedToken() *tokenclaims.Token {
	return &tokenclaims.Token{
		CustomClaims: tokenclaims.CustomClaims{
			Asset:       "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
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

func TestSignalRangeWithinWindows(t *testing.T) {
	repo := testRepo(t)
	tok := q2WindowedToken()

	inQ2From, inQ2To := mustTS(t, "2026-05-01T00:00:00Z"), mustTS(t, "2026-06-01T00:00:00Z")
	allTimeFrom, allTimeTo := mustTS(t, "2020-01-01T00:00:00Z"), mustTS(t, "2027-01-01T00:00:00Z")

	// The unconditional permission imposes no window.
	assert.True(t, signalRangeWithinWindows(repo, "speed", tok, allTimeFrom, allTimeTo))
	// The windowed location permission admits only in-window ranges.
	assert.True(t, signalRangeWithinWindows(repo, "currentLocationCoordinates", tok, inQ2From, inQ2To))
	assert.False(t, signalRangeWithinWindows(repo, "currentLocationCoordinates", tok, allTimeFrom, allTimeTo))
	// The derived approximate signal follows the location window here (no
	// approximate permission on the token).
	assert.True(t, signalRangeWithinWindows(repo, model.ApproximateCoordinatesField, tok, inQ2From, inQ2To))
	assert.False(t, signalRangeWithinWindows(repo, model.ApproximateCoordinatesField, tok, allTimeFrom, allTimeTo))

	// Possession is NOT this check's job: a signal whose permission the token
	// does not hold at all passes through, to be denied per-field by the
	// privilege directives rather than rejecting the whole query.
	vinOnly := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		ScopedPermissions: []tokenclaims.ScopedPermission{{
			Name: tokenclaims.PermissionGetVINCredential,
			Constraint: []tokenclaims.Constraint{
				{LeftOperand: tokenclaims.LeftOperandRecordedAt, Operator: tokenclaims.OperatorGteq, RightOperand: "2026-04-01T00:00:00Z"},
			},
		}},
	}}
	assert.True(t, signalRangeWithinWindows(repo, "speed", vinOnly, allTimeFrom, allTimeTo))
	assert.True(t, signalRangeWithinWindows(repo, model.ApproximateCoordinatesField, vinOnly, allTimeFrom, allTimeTo))
}

func TestSignalValueVisible_Windowed(t *testing.T) {
	repo := testRepo(t)
	tok := q2WindowedToken()

	inWindow := mustTS(t, "2026-05-15T12:00:00Z")
	outOfWindow := mustTS(t, "2026-03-15T12:00:00Z")

	// Values of the unconditional signal are visible at any timestamp.
	assert.True(t, signalValueVisible(repo, "speed", tok, outOfWindow))
	// Values of the windowed signal are visible only inside the window.
	assert.True(t, signalValueVisible(repo, "currentLocationCoordinates", tok, inWindow))
	assert.False(t, signalValueVisible(repo, "currentLocationCoordinates", tok, outOfWindow))
}

func TestHasUnscopedPrivilegesForSignal(t *testing.T) {
	repo := testRepo(t)
	tok := q2WindowedToken()

	// All-time surfaces (availableSignals, dataSummary) serve the
	// unconditional permission's signals but exclude the windowed one.
	assert.True(t, hasUnscopedPrivilegesForSignal(repo, "speed", tok))
	assert.False(t, hasUnscopedPrivilegesForSignal(repo, "currentLocationCoordinates", tok))
	// Possession still recognizes both.
	assert.True(t, hasPrivilegesForSignal(repo, "currentLocationCoordinates", tok))
}

func TestRequireRawDataToken_Windowed(t *testing.T) {
	// Location scoped to Q2, non-location unconditional: raw-data access is
	// via the history pair, so the location window governs.
	tok := q2WindowedToken()
	ctx := context.WithValue(context.Background(), ClaimsContextKey{}, tok)

	after, before := mustTS(t, "2026-05-01T00:00:00Z"), mustTS(t, "2026-06-01T00:00:00Z")
	outAfter := mustTS(t, "2026-01-01T00:00:00Z")

	// Bounded inside the window: allowed.
	_, err := requireRawDataToken(ctx, &model.CloudEventFilter{After: &after, Before: &before})
	assert.NoError(t, err)
	// Bounds exceeding the window: rejected.
	_, err = requireRawDataToken(ctx, &model.CloudEventFilter{After: &outAfter, Before: &before})
	assert.Error(t, err)
	// No bounds at all means "all time": rejected for a windowed grant.
	_, err = requireRawDataToken(ctx, nil)
	assert.Error(t, err)

	// An unconditional raw-data grant is unaffected by filters.
	rawTok := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Permissions: []string{tokenclaims.PermissionGetRawData},
	}}
	rawCtx := context.WithValue(context.Background(), ClaimsContextKey{}, rawTok)
	_, err = requireRawDataToken(rawCtx, nil)
	assert.NoError(t, err)

	// A token with neither path is rejected outright.
	noneTok := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Permissions: []string{tokenclaims.PermissionGetVINCredential},
	}}
	noneCtx := context.WithValue(context.Background(), ClaimsContextKey{}, noneTok)
	_, err = requireRawDataToken(noneCtx, &model.CloudEventFilter{After: &after, Before: &before})
	assert.Error(t, err)
}
