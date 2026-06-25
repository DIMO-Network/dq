//go:build e2e186612

// local_e2e_186612_test.go is an out-of-band end-to-end harness (not part of
// the normal suite — guarded by the e2e186612 build tag). It opens a DuckLake
// catalog that `din lake-backfill` already populated with a real day of prod
// cloudevents (s3://dimo-storage-prod/cloudevent/valid/...), runs the real
// materializer over it, then emits dq's telemetry-style answers for one vehicle
// as JSON so they can be diffed against the production telemetry-api.
//
// Driven entirely by env (see scratchpad/e2e-186612/env.sh):
//
//	E2E_CATALOG        DuckLake catalog file (din's LAKE_CATALOG_DSN)
//	E2E_DATAPATH       DuckLake data path     (din's LAKE_DATA_PATH)
//	E2E_SUBJECT        vehicle DID
//	E2E_FROM,E2E_TO    RFC3339 window bounds for the signals() comparison
//	E2E_INTERVAL       bucket interval (default 1h)
//	E2E_FLOAT_SIGNALS  comma-separated float VSS signal names
//	E2E_STRING_SIGNALS comma-separated string VSS signal names
//	E2E_OUT            output JSON path
package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/99designs/gqlgen/client"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// newGraphQLClientFull mirrors the package's newGraphQLClient but also wires the
// DuckLake segments backend (production: ComposeBackend(NewLakeQueries, NewLakeSegments)).
// newGraphQLClient passes nil for segments, which panics on a segments() query.
func newGraphQLClientFull(t *testing.T, svc *duck.Service) *client.Client {
	t.Helper()
	repo, err := repositories.NewRepository(
		repositories.ComposeBackend(duck.NewLakeQueries(svc), duck.NewLakeSegments(svc)))
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
	return client.New(srv)
}

func e2eEnv(t *testing.T, k string) string {
	t.Helper()
	v := os.Getenv(k)
	require.NotEmptyf(t, v, "env %s is required", k)
	return v
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func TestLocalE2E_186612(t *testing.T) {
	ctx := context.Background()
	catalog := e2eEnv(t, "E2E_CATALOG")
	dataPath := e2eEnv(t, "E2E_DATAPATH")
	subject := e2eEnv(t, "E2E_SUBJECT")
	fromStr := e2eEnv(t, "E2E_FROM")
	toStr := e2eEnv(t, "E2E_TO")
	outPath := e2eEnv(t, "E2E_OUT")
	interval := os.Getenv("E2E_INTERVAL")
	if interval == "" {
		interval = "1h"
	}
	floatSignals := splitCSV(os.Getenv("E2E_FLOAT_SIGNALS"))
	stringSignals := splitCSV(os.Getenv("E2E_STRING_SIGNALS"))

	log := zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.InfoLevel)

	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      catalog,
		DataPath:        dataPath,
	})
	require.NoError(t, err)
	defer svc.Close() //nolint:errcheck
	db := svc.DB()

	// Ground-truth raw counts straight from the backfilled catalog.
	var rawTotal, rawSubj int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM lake.raw_events`).Scan(&rawTotal))
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*) FROM lake.raw_events WHERE subject = ?`, subject).Scan(&rawSubj))
	t.Logf("raw_events: total=%d subject=%d", rawTotal, rawSubj)
	require.Positive(t, rawSubj, "no raw events for subject in the backfilled day — wrong vehicle/day?")

	// Materialize raw cloudevents -> decoded signals with the real vendor modules.
	materializer.RegisterVendorModules(materializer.VendorConfig{
		ChainID:               137,
		VehicleNFTAddress:     vehicleNFT,
		AftermarketNFTAddress: common.HexToAddress("0x9c94C395cBcBDe662235E0A9d3bB87Ad708561BA"),
		SyntheticNFTAddress:   common.HexToAddress("0x4804e8D1661cd1a1e5dDdE1ff458A7f878c0aC6D"),
	})
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, log)
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, log).
		WithDuckLake(mat)

	start := time.Now()
	processed := drainRunner(t, ctx, runner)
	t.Logf("materialized %d events in %s", processed, time.Since(start))

	// Decoded counts for the subject (whole materialized day) + window-scoped count.
	var sigSubjAll int64
	var firstAll, lastAll sql.NullString
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*), CAST(min("timestamp") AS VARCHAR), CAST(max("timestamp") AS VARCHAR)
		   FROM lake.signals WHERE subject = ?`, subject).Scan(&sigSubjAll, &firstAll, &lastAll))
	var sigSubjWindow int64
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*) FROM lake.signals WHERE subject = ? AND "timestamp" >= ? AND "timestamp" < ?`,
		subject, fromStr, toStr).Scan(&sigSubjWindow))
	t.Logf("lake.signals subject=%d (window=%d) first=%s last=%s",
		sigSubjAll, sigSubjWindow, firstAll.String, lastAll.String)

	gql := newGraphQLClientFull(t, svc)

	// 1) dataSummary — per-signal counts + first/last over the materialized day.
	dataSummaryQuery := fmt.Sprintf(`{
	  dataSummary(subject: %q) {
	    numberOfSignals
	    availableSignals
	    firstSeen
	    lastSeen
	    signalDataSummary { name numberOfSignals firstSeen lastSeen }
	    eventDataSummary { name numberOfEvents firstSeen lastSeen }
	  }
	}`, subject)
	var dataSummary map[string]any
	require.NoError(t, gql.Post(dataSummaryQuery, &dataSummary))

	// 2) signals — bucketed aggregations over the comparison window (the apples-
	// to-apples surface vs telemetry-api). Built dynamically from the env signal
	// lists so the same set can be aimed at both systems without a recompile.
	var sb strings.Builder
	sb.WriteString("timestamp\n")
	floatAggs := []string{"AVG", "MIN", "MAX", "FIRST", "LAST"}
	for _, s := range floatSignals {
		for _, a := range floatAggs {
			fmt.Fprintf(&sb, "%s_%s: %s(agg: %s)\n", s, a, s, a)
		}
	}
	for _, s := range stringSignals {
		fmt.Fprintf(&sb, "%s_TOP: %s(agg: TOP)\n", s, s)
		fmt.Fprintf(&sb, "%s_LAST: %s(agg: LAST)\n", s, s)
	}
	signalsQuery := fmt.Sprintf(`{ signals(subject: %q, interval: %q, from: %q, to: %q) { %s } }`,
		subject, interval, fromStr, toStr, sb.String())
	var signalsResp map[string]any
	require.NoError(t, gql.Post(signalsQuery, &signalsResp))

	// 3) signalsLatest — latest value+timestamp per signal (all-time in this lake).
	var lb strings.Builder
	lb.WriteString("lastSeen\n")
	for _, s := range floatSignals {
		fmt.Fprintf(&lb, "%s { timestamp value }\n", s)
	}
	for _, s := range stringSignals {
		fmt.Fprintf(&lb, "%s { timestamp value }\n", s)
	}
	latestQuery := fmt.Sprintf(`{ signalsLatest(subject: %q) { %s } }`, subject, lb.String())
	var latestResp map[string]any
	require.NoError(t, gql.Post(latestQuery, &latestResp))

	// 4) segments (trips) via ignition detection over the window.
	segQuery := fmt.Sprintf(`{ segments(subject: %q, from: %q, to: %q, mechanism: IGNITION_DETECTION) {
	  start { timestamp value { latitude longitude } }
	  end { timestamp value { latitude longitude } }
	  duration isOngoing startedBeforeRange
	} }`, subject, fromStr, toStr)
	var segResp map[string]any
	require.NoError(t, gql.Post(segQuery, &segResp))

	// 5) discrete events over the window.
	evQuery := fmt.Sprintf(`{ events(subject: %q, from: %q, to: %q) { timestamp name source } }`,
		subject, fromStr, toStr)
	var evResp map[string]any
	require.NoError(t, gql.Post(evQuery, &evResp))

	out := map[string]any{
		"system":               "dq",
		"subject":              subject,
		"window":               map[string]string{"from": fromStr, "to": toStr, "interval": interval},
		"rawEventsTotal":       rawTotal,
		"rawEventsSubject":     rawSubj,
		"signalsSubjectAll":    sigSubjAll,
		"signalsSubjectWindow": sigSubjWindow,
		"signalsFirst":         firstAll.String,
		"signalsLast":          lastAll.String,
		"materializedCount":    processed,
		"floatSignals":         floatSignals,
		"stringSignals":        stringSignals,
		"dataSummary":          dataSummary["dataSummary"],
		"signals":              signalsResp["signals"],
		"signalsLatest":        latestResp["signalsLatest"],
		"segments":             segResp["segments"],
		"events":               evResp["events"],
	}
	b, err := json.MarshalIndent(out, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(outPath, b, 0o644))
	t.Logf("wrote dq results -> %s (%d bytes)", outPath, len(b))
}
