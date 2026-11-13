package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/99designs/gqlgen/client"
	"github.com/99designs/gqlgen/graphql"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLatestSignalsClient is newGraphQLClient with token claims carrying perms in
// the request context: latestSignals authorizes each name in its resolvers, not
// through schema directives, so the pass-through directives do not cover it.
func newLatestSignalsClient(t *testing.T, svc *duck.Service, perms ...string) *client.Client {
	t.Helper()
	repo, err := repositories.NewRepository(repositories.ComposeBackend(duck.NewLakeQueries(svc), nil))
	require.NoError(t, err)

	cfg := graph.Config{Resolvers: &graph.Resolver{SignalRepo: repo}}
	cfg.Directives.RequiresVehicleToken = passDirective
	cfg.Directives.IsSignal = passDirective
	cfg.Directives.HasAggregation = passDirective
	cfg.Directives.McpHide = passDirective
	cfg.Directives.RequiresAllOfPrivileges = passPrivilegeDirective
	cfg.Directives.RequiresOneOfPrivilege = passPrivilegeDirective

	srv := gqlhandler.New(graph.NewExecutableSchema(cfg))
	srv.AddTransport(transport.POST{})
	srv.AroundOperations(func(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
		tok := &tokenclaims.Token{}
		tok.Permissions = perms
		return next(context.WithValue(ctx, graph.ClaimsContextKey{}, tok))
	})
	return client.New(srv)
}

type latestValue struct {
	Timestamp string
	Value     any
}

// postLatest runs query and returns the fields under root with the paths of any
// field errors.
func postLatest(t *testing.T, gql *client.Client, query, root string) (map[string]*latestValue, []string) {
	t.Helper()
	resp, err := gql.RawPost(query)
	require.NoError(t, err)
	rawData, err := json.Marshal(resp.Data)
	require.NoError(t, err)
	var data map[string]map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rawData, &data))
	fields := make(map[string]*latestValue, len(data[root]))
	for alias, raw := range data[root] {
		if alias == "lastSeen" {
			var ts string
			require.NoError(t, json.Unmarshal(raw, &ts))
			fields[alias] = &latestValue{Timestamp: ts}
			continue
		}
		var v *latestValue
		require.NoError(t, json.Unmarshal(raw, &v))
		fields[alias] = v
	}
	var gqlErrs []struct {
		Path []any
	}
	if len(resp.Errors) > 0 {
		require.NoError(t, json.Unmarshal(resp.Errors, &gqlErrs))
	}
	var paths []string
	for _, e := range gqlErrs {
		require.Len(t, e.Path, 2)
		paths = append(paths, fmt.Sprint(e.Path[1]))
	}
	return fields, paths
}

// latestSignals must return exactly what signalsLatest returns for the same
// signals: both read the same latest rows.
func TestLatestSignals_MatchesSignalsLatest(t *testing.T) {
	svc := materializeRuptela(t)
	gql := newLatestSignalsClient(t, svc,
		tokenclaims.PermissionGetNonLocationHistory, tokenclaims.PermissionGetLocationHistory)

	want, errs := postLatest(t, gql, fmt.Sprintf(`{
		signalsLatest(subject: %q) {
			lastSeen
			speed { timestamp value }
			powertrainType { timestamp value }
			currentLocationAltitude { timestamp value }
			currentLocationCoordinates { timestamp value { latitude longitude hdop } }
			currentLocationApproximateCoordinates { timestamp value { latitude longitude hdop } }
		}
	}`, ruptelaVehicle), "signalsLatest")
	require.Empty(t, errs)
	for alias, v := range want {
		require.NotNil(t, v, "fixture has no %s; the comparison below would be vacuous", alias)
	}

	got, errs := postLatest(t, gql, fmt.Sprintf(`{
		latestSignals(subject: %q) {
			lastSeen
			speed: number(name: "speed") { timestamp value }
			powertrainType: string(name: "powertrainType") { timestamp value }
			currentLocationAltitude: number(name: "currentLocationAltitude") { timestamp value }
			currentLocationCoordinates: location(name: "currentLocationCoordinates") { timestamp value { latitude longitude hdop } }
			currentLocationApproximateCoordinates: location(name: "currentLocationApproximateCoordinates") { timestamp value { latitude longitude hdop } }
		}
	}`, ruptelaVehicle), "latestSignals")
	require.Empty(t, errs)
	assert.Equal(t, want, got)
}

// The accessor picks the stored column and the name is not checked against the
// definitions: the wrong accessor reports that column's zero value, and a name
// with no rows is null.
func TestLatestSignals_ProjectionAndMissingNames(t *testing.T) {
	svc := materializeRuptela(t)
	gql := newLatestSignalsClient(t, svc,
		tokenclaims.PermissionGetNonLocationHistory, tokenclaims.PermissionGetLocationHistory)

	got, errs := postLatest(t, gql, fmt.Sprintf(`{
		latestSignals(subject: %q) {
			speed: number(name: "speed") { timestamp value }
			speedAgain: number(name: "speed") { timestamp value }
			speedAsString: string(name: "speed") { timestamp value }
			typeAsNumber: number(name: "powertrainType") { timestamp value }
			speedAsLocation: location(name: "speed") { timestamp }
			coordsAsNumber: number(name: "currentLocationCoordinates") { timestamp value }
			coords: location(name: "currentLocationCoordinates") { timestamp }
			unknown: number(name: "com.test.battery.cell18.temperature") { timestamp value }
			unknownLocation: location(name: "com.test.position") { timestamp }
		}
	}`, ruptelaVehicle), "latestSignals")
	require.Empty(t, errs)

	require.NotNil(t, got["speed"])
	assert.NotZero(t, got["speed"].Value)
	assert.Equal(t, got["speed"], got["speedAgain"])
	require.NotNil(t, got["speedAsString"])
	assert.Equal(t, "", got["speedAsString"].Value)
	assert.Equal(t, got["speed"].Timestamp, got["speedAsString"].Timestamp)
	require.NotNil(t, got["typeAsNumber"])
	assert.Equal(t, float64(0), got["typeAsNumber"].Value)
	assert.Nil(t, got["speedAsLocation"])
	// One name through both a scalar and the location accessor: each keeps its own row.
	require.NotNil(t, got["coordsAsNumber"])
	assert.Equal(t, float64(0), got["coordsAsNumber"].Value)
	require.NotNil(t, got["coords"])
	assert.Nil(t, got["unknown"])
	assert.Nil(t, got["unknownLocation"])
}

// Each name is authorized on its own: a field the token may not read is a field
// error and the rest of the response still resolves.
func TestLatestSignals_Privileges(t *testing.T) {
	svc := materializeRuptela(t)
	query := fmt.Sprintf(`{
		latestSignals(subject: %q) {
			lastSeen
			speed: number(name: "speed") { timestamp value }
			altitude: number(name: "currentLocationAltitude") { timestamp value }
			coords: location(name: "currentLocationCoordinates") { timestamp }
			approx: location(name: "currentLocationApproximateCoordinates") { timestamp }
			unknown: number(name: "com.test.battery.cell18.temperature") { timestamp value }
		}
	}`, ruptelaVehicle)

	for _, tc := range []struct {
		name    string
		perms   []string
		denied  []string
		present []string
	}{
		{
			name:    "non-location only",
			perms:   []string{tokenclaims.PermissionGetNonLocationHistory},
			denied:  []string{"altitude", "coords", "approx", "unknown"},
			present: []string{"lastSeen", "speed"},
		},
		{
			name:    "location only",
			perms:   []string{tokenclaims.PermissionGetLocationHistory},
			denied:  []string{"speed", "unknown"},
			present: []string{"lastSeen", "altitude", "coords", "approx"},
		},
		{
			name:    "approximate location only",
			perms:   []string{tokenclaims.PermissionGetApproximateLocation},
			denied:  []string{"speed", "altitude", "coords", "unknown"},
			present: []string{"lastSeen", "approx"},
		},
		{
			name:    "both history permissions",
			perms:   []string{tokenclaims.PermissionGetNonLocationHistory, tokenclaims.PermissionGetLocationHistory},
			present: []string{"lastSeen", "speed", "altitude", "coords", "approx"},
		},
		{
			name:    "no permissions",
			denied:  []string{"speed", "altitude", "coords", "approx", "unknown"},
			present: []string{"lastSeen"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, errs := postLatest(t, newLatestSignalsClient(t, svc, tc.perms...), query, "latestSignals")
			assert.ElementsMatch(t, tc.denied, errs)
			for _, alias := range tc.denied {
				assert.Nil(t, got[alias], alias)
			}
			for _, alias := range tc.present {
				assert.NotNil(t, got[alias], alias)
			}
		})
	}
}
