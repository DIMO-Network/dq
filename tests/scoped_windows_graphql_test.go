// scoped_windows_graphql_test.go drives the REAL GraphQL surface — real auth
// directives, real resolvers, real DuckLake-backed repository — with tokens
// carrying scoped permissions (ODRL data windows), verifying the enforcement
// model end to end: ranged queries reject ranges outside a held window, latest
// paths are evaluated within it, and all-time surfaces exclude scoped signals.
//
// The claims are round-tripped through JSON exactly as they ride in the JWT
// payload, so these tests also pin wire compatibility with dauth's minting.
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/99designs/gqlgen/client"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/auth"
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func locationAt(ts time.Time, lat, lon float64) map[string]any {
	return map[string]any{
		"name":      "currentLocationCoordinates",
		"timestamp": ts.Format(time.RFC3339Nano),
		"value":     map[string]any{"latitude": lat, "longitude": lon},
	}
}

// newScopedGraphQLClient builds a GraphQL client over the real executable
// schema with the REAL auth directives (unlike the passDirective harnesses),
// injecting the given token's claims into every request context the same way
// auth.AddClaimHandler does — after a JSON round trip, so the claims are
// exactly what a dauth-minted JWT payload would parse into.
func newScopedGraphQLClient(t *testing.T, svc *duck.Service, tok *tokenclaims.Token) *client.Client {
	t.Helper()
	repo, err := repositories.NewRepository(repositories.ComposeBackend(duck.NewLakeQueries(svc), nil))
	require.NoError(t, err)

	cfg := graph.Config{Resolvers: &graph.Resolver{SignalRepo: repo}}
	cfg.Directives.RequiresVehicleToken = auth.NewVehicleTokenCheck()
	cfg.Directives.RequiresAllOfPrivileges = auth.AllOfPrivilegeCheck
	cfg.Directives.RequiresOneOfPrivilege = auth.OneOfPrivilegeCheck
	cfg.Directives.IsSignal = passDirective
	cfg.Directives.HasAggregation = passDirective
	cfg.Directives.McpHide = passDirective

	srv := gqlhandler.New(graph.NewExecutableSchema(cfg))
	srv.AddTransport(transport.POST{})

	payload, err := json.Marshal(tok)
	require.NoError(t, err)
	var claim auth.DQClaim
	require.NoError(t, json.Unmarshal(payload, &claim))
	if len(tok.ScopedPermissions) > 0 {
		require.NotEmpty(t, claim.ScopedPermissions, "scoped_permissions lost in JSON round trip")
	}

	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), auth.DQClaimContextKey{}, &claim)
		ctx = context.WithValue(ctx, graph.ClaimsContextKey{}, &claim.Token)
		srv.ServeHTTP(w, r.WithContext(ctx))
	})
	return client.New(wrapped)
}

// scopedFixture seeds speed + location at two instants — outTS (72h ago) and
// inTS (24h ago) — and materializes them into the lake.
func scopedFixture(t *testing.T) (svc *duck.Service, subject string, base, outTS, inTS time.Time) {
	t.Helper()
	ctx := context.Background()
	svc = newLakeService(t, t.TempDir())
	db := svc.DB()
	subject = fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	base = time.Now().UTC().Truncate(time.Hour)
	outTS = base.Add(-72 * time.Hour)
	inTS = base.Add(-24 * time.Hour)

	seedRawStatus(t, db, "sw-1", subject, outTS, speedAt(outTS, 30), locationAt(outTS, 40.0, -70.0))
	seedRawStatus(t, db, "sw-2", subject, inTS, speedAt(inTS, 70), locationAt(inTS, 41.0, -71.0))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Positive(t, drainRunner(t, ctx, runner))
	return svc, subject, base, outTS, inTS
}

func recordedAtWindow(from, to time.Time) []tokenclaims.Constraint {
	return []tokenclaims.Constraint{
		{LeftOperand: tokenclaims.LeftOperandRecordedAt, Operator: tokenclaims.OperatorGteq, RightOperand: from.Format(time.RFC3339)},
		{LeftOperand: tokenclaims.LeftOperandRecordedAt, Operator: tokenclaims.OperatorLt, RightOperand: to.Format(time.RFC3339)},
	}
}

// scopedLocToken holds non-location history unconditionally and location
// history only for [winFrom, winTo).
func scopedLocToken(subject string, winFrom, winTo time.Time) *tokenclaims.Token {
	return &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Asset:       subject,
		Permissions: []string{tokenclaims.PermissionGetNonLocationHistory},
		ScopedPermissions: []tokenclaims.ScopedPermission{{
			Name:       tokenclaims.PermissionGetLocationHistory,
			Constraint: recordedAtWindow(winFrom, winTo),
		}},
	}}
}

func TestScopedWindowsGraphQL_RangedSignals(t *testing.T) {
	svc, subject, base, _, _ := scopedFixture(t)
	// Window covers inTS (24h ago) but not outTS (72h ago).
	c := newScopedGraphQLClient(t, svc, scopedLocToken(subject, base.Add(-48*time.Hour), base.Add(time.Hour)))

	signalsQuery := func(from, to time.Time, fields string) string {
		return fmt.Sprintf(`query { signals(subject:%q, interval:"1h", from:%q, to:%q) { timestamp %s } }`,
			subject, from.Format(time.RFC3339), to.Format(time.RFC3339), fields)
	}

	t.Run("in-window range serves both signals", func(t *testing.T) {
		var resp struct {
			Signals []struct {
				Timestamp                  string
				Speed                      *float64
				CurrentLocationCoordinates *struct{ Latitude float64 }
			}
		}
		err := c.Post(signalsQuery(base.Add(-40*time.Hour), base.Add(-12*time.Hour),
			`speed(agg: MAX) currentLocationCoordinates(agg: LAST) { latitude }`), &resp)
		require.NoError(t, err)
		var gotSpeed, gotLoc bool
		for _, row := range resp.Signals {
			if row.Speed != nil && *row.Speed == 70 {
				gotSpeed = true
			}
			if row.CurrentLocationCoordinates != nil && row.CurrentLocationCoordinates.Latitude == 41.0 {
				gotLoc = true
			}
		}
		assert.True(t, gotSpeed, "in-window speed value missing")
		assert.True(t, gotLoc, "in-window location value missing")
	})

	t.Run("range exceeding the window is rejected, naming the window", func(t *testing.T) {
		var resp any
		err := c.Post(signalsQuery(base.Add(-96*time.Hour), base,
			`speed(agg: MAX) currentLocationCoordinates(agg: LAST) { latitude }`), &resp)
		require.Error(t, err)
		assert.ErrorContains(t, err, "data window")
		assert.ErrorContains(t, err, "currentLocationCoordinates")
	})

	t.Run("same wide range without the windowed signal succeeds", func(t *testing.T) {
		var resp struct {
			Signals []struct {
				Timestamp string
				Speed     *float64
			}
		}
		err := c.Post(signalsQuery(base.Add(-96*time.Hour), base, `speed(agg: MAX)`), &resp)
		require.NoError(t, err)
		var maxSeen float64
		for _, row := range resp.Signals {
			if row.Speed != nil && *row.Speed > maxSeen {
				maxSeen = *row.Speed
			}
		}
		assert.Equal(t, 70.0, maxSeen, "unconditional signal should see the full range")
	})
}

func TestScopedWindowsGraphQL_Latest(t *testing.T) {
	svc, subject, base, _, inTS := scopedFixture(t)

	const latestQuery = `query($subject: String!) { signalsLatest(subject: $subject) {
		lastSeen
		speed { timestamp value }
		currentLocationCoordinates { timestamp value { latitude } }
	} }`

	type latestResp struct {
		SignalsLatest struct {
			LastSeen *string
			Speed    *struct {
				Timestamp string
				Value     float64
			}
			CurrentLocationCoordinates *struct {
				Timestamp string
				Value     struct{ Latitude float64 }
			}
		}
	}

	t.Run("latest inside the window is served, lastSeen suppressed", func(t *testing.T) {
		c := newScopedGraphQLClient(t, svc, scopedLocToken(subject, base.Add(-48*time.Hour), base.Add(time.Hour)))
		var resp latestResp
		require.NoError(t, c.Post(latestQuery, &resp, client.Var("subject", subject)))
		require.NotNil(t, resp.SignalsLatest.Speed)
		assert.Equal(t, 70.0, resp.SignalsLatest.Speed.Value)
		require.NotNil(t, resp.SignalsLatest.CurrentLocationCoordinates, "in-window latest location should be visible")
		assert.Equal(t, 41.0, resp.SignalsLatest.CurrentLocationCoordinates.Value.Latitude)
		assert.Nil(t, resp.SignalsLatest.LastSeen, "lastSeen must be suppressed for scoped tokens")
	})

	t.Run("latest outside the window is withheld", func(t *testing.T) {
		// Window ends before inTS: the latest location (inTS) is out of
		// window and must not be shown; the unconditional speed still is.
		c := newScopedGraphQLClient(t, svc, scopedLocToken(subject, base.Add(-48*time.Hour), inTS.Add(-time.Hour)))
		var resp latestResp
		require.NoError(t, c.Post(latestQuery, &resp, client.Var("subject", subject)))
		require.NotNil(t, resp.SignalsLatest.Speed)
		assert.Equal(t, 70.0, resp.SignalsLatest.Speed.Value)
		assert.Nil(t, resp.SignalsLatest.CurrentLocationCoordinates,
			"latest location recorded outside the window must be withheld")
	})
}

func TestScopedWindowsGraphQL_ApproxLocationSeparatelyGated(t *testing.T) {
	svc, subject, base, _, inTS := scopedFixture(t)
	// Approximate location unconditional; raw location windowed to BEFORE the
	// latest fix. The raw coordinates must be withheld while the approximate
	// value — derived from the same row — is served.
	tok := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Asset:       subject,
		Permissions: []string{tokenclaims.PermissionGetApproximateLocation},
		ScopedPermissions: []tokenclaims.ScopedPermission{{
			Name:       tokenclaims.PermissionGetLocationHistory,
			Constraint: recordedAtWindow(base.Add(-48*time.Hour), inTS.Add(-time.Hour)),
		}},
	}}
	c := newScopedGraphQLClient(t, svc, tok)

	var resp struct {
		SignalsLatest struct {
			CurrentLocationCoordinates *struct {
				Value struct{ Latitude float64 }
			}
			CurrentLocationApproximateCoordinates *struct {
				Value struct{ Latitude float64 }
			}
		}
	}
	require.NoError(t, c.Post(`query($subject: String!) { signalsLatest(subject: $subject) {
		currentLocationCoordinates { value { latitude } }
		currentLocationApproximateCoordinates { value { latitude } }
	} }`, &resp, client.Var("subject", subject)))

	assert.Nil(t, resp.SignalsLatest.CurrentLocationCoordinates,
		"raw coordinates outside the location window must be withheld")
	require.NotNil(t, resp.SignalsLatest.CurrentLocationApproximateCoordinates,
		"approximate location under an unconditional grant must be served")
	// H3-snapped, so not exactly 41.0 — just sanity-check the ballpark.
	assert.InDelta(t, 41.0, resp.SignalsLatest.CurrentLocationApproximateCoordinates.Value.Latitude, 1.0)
}

func TestScopedWindowsGraphQL_AllTimeSurfaces(t *testing.T) {
	svc, subject, base, _, _ := scopedFixture(t)
	c := newScopedGraphQLClient(t, svc, scopedLocToken(subject, base.Add(-48*time.Hour), base.Add(time.Hour)))

	t.Run("availableSignals excludes the scoped signal", func(t *testing.T) {
		var resp struct{ AvailableSignals []string }
		require.NoError(t, c.Post(`query($subject: String!) { availableSignals(subject: $subject) }`,
			&resp, client.Var("subject", subject)))
		assert.Contains(t, resp.AvailableSignals, "speed")
		assert.NotContains(t, resp.AvailableSignals, "currentLocationCoordinates",
			"scoped signals must not appear on all-time surfaces")
	})

	t.Run("dataSummary excludes the scoped signal and drops event summaries", func(t *testing.T) {
		var resp struct {
			DataSummary struct {
				NumberOfSignals   int
				AvailableSignals  []string
				SignalDataSummary []struct {
					Name            string
					NumberOfSignals int
				}
				EventDataSummary []struct{ Name string }
			}
		}
		require.NoError(t, c.Post(`query($subject: String!) { dataSummary(subject: $subject) {
			numberOfSignals availableSignals
			signalDataSummary { name numberOfSignals }
			eventDataSummary { name }
		} }`, &resp, client.Var("subject", subject)))
		require.Len(t, resp.DataSummary.SignalDataSummary, 1)
		assert.Equal(t, "speed", resp.DataSummary.SignalDataSummary[0].Name)
		assert.Equal(t, 2, resp.DataSummary.NumberOfSignals, "count must be recomputed over surviving signals")
		assert.NotContains(t, resp.DataSummary.AvailableSignals, "currentLocationCoordinates")
		assert.Empty(t, resp.DataSummary.EventDataSummary,
			"event summaries must be dropped when the history permissions are not both unconditional")
	})

	t.Run("snapshot serves in-window scoped values but suppresses lastSeen", func(t *testing.T) {
		var resp struct {
			SignalsSnapshot struct {
				LastSeen *string
				Signals  []struct{ Name string }
			}
		}
		require.NoError(t, c.Post(`query($subject: String!) { signalsSnapshot(subject: $subject) {
			lastSeen signals { name }
		} }`, &resp, client.Var("subject", subject)))
		names := make(map[string]bool)
		for _, s := range resp.SignalsSnapshot.Signals {
			names[s.Name] = true
		}
		assert.True(t, names["speed"])
		assert.True(t, names["currentLocationCoordinates"],
			"snapshot is a point query: in-window scoped values are served")
		assert.Nil(t, resp.SignalsSnapshot.LastSeen)
	})

	t.Run("events reject ranges outside the held window", func(t *testing.T) {
		eventsQuery := func(from, to time.Time) string {
			return fmt.Sprintf(`query { events(subject:%q, from:%q, to:%q) { timestamp name } }`,
				subject, from.Format(time.RFC3339), to.Format(time.RFC3339))
		}
		var resp any
		err := c.Post(eventsQuery(base.Add(-96*time.Hour), base), &resp)
		require.Error(t, err)
		assert.ErrorContains(t, err, "data window")

		require.NoError(t, c.Post(eventsQuery(base.Add(-40*time.Hour), base.Add(-12*time.Hour)), &resp),
			"in-window events range must be accepted")
	})
}

func TestScopedWindowsGraphQL_UnheldSignalIsAFieldError(t *testing.T) {
	svc, subject, base, _, _ := scopedFixture(t)
	// The token holds ONLY the scoped location permission — no non-location
	// history at all. Requesting speed over any range must fail as a FIELD
	// possession error from the directive, not a whole-query window rejection:
	// the range check only speaks for permissions the token actually holds.
	tok := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Asset: subject,
		ScopedPermissions: []tokenclaims.ScopedPermission{{
			Name:       tokenclaims.PermissionGetLocationHistory,
			Constraint: recordedAtWindow(base.Add(-48*time.Hour), base.Add(time.Hour)),
		}},
	}}
	c := newScopedGraphQLClient(t, svc, tok)

	var resp any
	err := c.Post(fmt.Sprintf(`query { signals(subject:%q, interval:"1h", from:%q, to:%q) { timestamp speed(agg: MAX) } }`,
		subject, base.Add(-96*time.Hour).Format(time.RFC3339), base.Format(time.RFC3339)), &resp)
	require.Error(t, err)
	assert.ErrorContains(t, err, "missing required privilege")
	assert.NotContains(t, err.Error(), "data window",
		"an unheld permission is a possession problem, not a window problem")
}
