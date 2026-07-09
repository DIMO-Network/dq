//go:build e2e186612fetch

// local_e2e_186612_fetch_test.go exercises dq's fetch-api (cloudEvents) surface
// in-process against a din-backfilled DuckLake catalog for one vehicle, with the
// auth directives bypassed. It confirms the three fetch queries serve and that
// availableCloudEventTypes matches the raw_events ground truth (the prod
// cloudevent store the dump was taken from). Env mirrors the signals harness:
//
//	E2E_CATALOG, E2E_DATAPATH, E2E_SUBJECT, E2E_OUT
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"testing"

	"github.com/99designs/gqlgen/client"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/stretchr/testify/require"
)

func fetchEnv(t *testing.T, k string) string {
	t.Helper()
	v := os.Getenv(k)
	require.NotEmptyf(t, v, "env %s is required", k)
	return v
}

func TestLocalE2E_186612_Fetch(t *testing.T) {
	ctx := context.Background()
	catalog := fetchEnv(t, "E2E_CATALOG")
	dataPath := fetchEnv(t, "E2E_DATAPATH")
	subject := fetchEnv(t, "E2E_SUBJECT")
	outPath := os.Getenv("E2E_OUT")

	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      catalog,
		DataPath:        dataPath,
	})
	require.NoError(t, err)
	defer svc.Close() //nolint:errcheck
	db := svc.DB()

	// Ground truth straight from the backfilled catalog: per-type count + span.
	type typeRow struct {
		Type      string `json:"type"`
		Count     int64  `json:"count"`
		FirstSeen string `json:"firstSeen"`
		LastSeen  string `json:"lastSeen"`
	}
	groundRows, err := db.QueryContext(ctx, `SELECT type, count(*),
		CAST(min("time") AS VARCHAR), CAST(max("time") AS VARCHAR)
		FROM lake.raw_events WHERE subject = ? GROUP BY type ORDER BY type`, subject)
	require.NoError(t, err)
	ground := map[string]typeRow{}
	for groundRows.Next() {
		var r typeRow
		require.NoError(t, groundRows.Scan(&r.Type, &r.Count, &r.FirstSeen, &r.LastSeen))
		ground[r.Type] = r
	}
	require.NoError(t, groundRows.Err())
	require.NotEmpty(t, ground, "no raw events for subject")
	t.Logf("ground-truth raw_events types: %d", len(ground))

	// Fetch event service over the same lake (no S3: 186612 has no externalized blobs).
	es := duck.NewLakeEventService(svc, nil, nil, "")
	cfg := graph.Config{Resolvers: &graph.Resolver{EventService: es}}
	cfg.Directives.RequiresVehicleToken = passDirective
	cfg.Directives.IsSignal = passDirective
	cfg.Directives.HasAggregation = passDirective
	cfg.Directives.McpHide = passDirective
	cfg.Directives.RequiresAllOfPrivileges = passPrivilegeDirective
	cfg.Directives.RequiresOneOfPrivilege = passPrivilegeDirective
	srv := gqlhandler.New(graph.NewExecutableSchema(cfg))
	srv.AddTransport(transport.POST{})

	// The cloudEvent resolvers re-check claims in-resolver (requireSubjectOptsByDID),
	// independent of the bypassed directive — inject a raw-data token whose Asset is
	// the requested subject so the DID-link check short-circuits without identity.
	tok := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Asset:       subject,
		Permissions: []string{tokenclaims.PermissionGetRawData},
	}}
	withClaims := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), graph.ClaimsContextKey{}, tok)))
	})
	gql := client.New(withClaims)

	// 1) availableCloudEventTypes — the fetch summary surface.
	var typesResp struct {
		AvailableCloudEventTypes []typeRow `json:"availableCloudEventTypes"`
	}
	require.NoError(t, gql.Post(fmt.Sprintf(
		`{ availableCloudEventTypes(subject: %q) { type count firstSeen lastSeen } }`, subject),
		&typesResp))

	// Cross-check the fetch summary against the raw ground truth.
	require.Len(t, typesResp.AvailableCloudEventTypes, len(ground),
		"fetch type count differs from raw_events")
	gotTypes := map[string]typeRow{}
	for _, r := range typesResp.AvailableCloudEventTypes {
		gotTypes[r.Type] = r
		g, ok := ground[r.Type]
		require.Truef(t, ok, "fetch returned unknown type %q", r.Type)
		require.Equalf(t, g.Count, r.Count, "count mismatch for %s", r.Type)
	}

	// 2) latestCloudEvent (dimo.status) — header + data.
	var latestResp map[string]any
	require.NoError(t, gql.Post(fmt.Sprintf(
		`{ latestCloudEvent(subject: %q, filter: {type: "dimo.status"}) {
		   header { type source id time producer } data } }`, subject),
		&latestResp))

	// 3) cloudEvents list (dimo.events, limit 5) — headers.
	var listResp map[string]any
	require.NoError(t, gql.Post(fmt.Sprintf(
		`{ cloudEvents(subject: %q, limit: 5, filter: {type: "dimo.events"}) {
		   header { type source id time producer } } }`, subject),
		&listResp))

	// Stable, readable summary.
	keys := make([]string, 0, len(gotTypes))
	for k := range gotTypes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		g := ground[k]
		r := gotTypes[k]
		t.Logf("  %-36s fetch.count=%-6d raw.count=%-6d  %s .. %s", k, r.Count, g.Count, r.FirstSeen, r.LastSeen)
	}

	out := map[string]any{
		"system":                   "dq-fetch",
		"subject":                  subject,
		"availableCloudEventTypes": typesResp.AvailableCloudEventTypes,
		"groundTruthTypes":         ground,
		"latestCloudEvent":         latestResp["latestCloudEvent"],
		"cloudEvents":              listResp["cloudEvents"],
	}
	if outPath != "" {
		b, err := json.MarshalIndent(out, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(outPath, b, 0o644))
		t.Logf("wrote dq fetch results -> %s (%d bytes)", outPath, len(b))
	}
}
