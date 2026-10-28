// sources_test.go covers events a source writes under a grant
// (did-directory's docs/2026-10-24-sources.md): din stores them for a
// did:dimo vehicle with the source's DID as source and four extensions of
// their own. dq decodes them to signals with the vehicle-NFT gate on, filters
// signals by the source's DID, and serves the extensions on the header.
package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/99designs/gqlgen/client"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sourceVehicle = "did:dimo:EiBn2rTVzTD3ql9H6hDW2a1X7Yoo8L7ToQFmVoOKcDuGrA"
	sourceRelay   = "did:web:relay.example"
	sourceGrant   = "did:dimo:EiCkKx3hD8Hq1Rz2T7lYlq7d5bQv1sZyX3PpA9u0mN4w6Q/delegations/7"
	sourceSig     = "eyJhbGciOiJFUzI1NiIsImtpZCI6ImRpZDp3ZWI6cmVsYXkuZXhhbXBsZSNzaWduaW5nIn0..c2lnbmF0dXJl"
	producerSig   = "eyJhbGciOiJFUzI1NiJ9..bWFjaGluZQ"
	producerKey   = "did:key:zDnaerDaTF5BXEavCrfRZEk316dpbLsfPDZ3WJ5hRTPFU2169"
)

func TestSources_SignalsAndProvenance(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	at := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	arrived := at.Add(1500 * time.Millisecond)

	// din's sink row for an admitted event: extras hold the signatures it
	// verified and the two extensions it stamped.
	ev := deviceStatus("src-1", sourceVehicle, at, speedAt(at, 72))
	extras, err := json.Marshal(map[string]any{
		"sourcesignature":   sourceSig,
		"producersignature": producerSig,
		"arrivedat":         arrived.Format(time.RFC3339Nano),
		"grant":             sourceGrant,
	})
	require.NoError(t, err)
	_, err = svc.DB().ExecContext(ctx,
		`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data)
		 VALUES (?, ?, ?, ?, ?, ?, 'application/json', ?, ?, ?)`,
		ev.Subject, ev.Time, ev.Type, ev.ID, sourceRelay, producerKey, ev.DataVersion, string(extras), string(ev.Data))
	require.NoError(t, err)

	mat, err := materializer.NewDuckLakeMaterializer(ctx, svc.DB(), zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 1, drainRunner(t, ctx, runner), "the did:dimo event passes the vehicle gate")

	gql := newSourcesClient(t, svc, &tokenclaims.Token{Grants: []tokenclaims.Grant{{
		Subject:   sourceVehicle,
		Abilities: []string{tokenclaims.AbilityTelemetryRead, tokenclaims.AbilityRawRead},
	}}})

	t.Run("signals by source", func(t *testing.T) {
		const q = `query($subject: String!, $from: Time!, $to: Time!, $source: String) {
			signals(subject: $subject, interval: "24h", from: $from, to: $to, filter: {source: $source}) { speed(agg: MAX) } }`
		vars := func(source string) client.Option {
			return func(r *client.Request) {
				r.Variables = map[string]any{
					"subject": sourceVehicle, "source": source,
					"from": at.Add(-time.Hour).Format(time.RFC3339), "to": at.Add(time.Hour).Format(time.RFC3339),
				}
			}
		}
		var resp struct {
			Signals []struct{ Speed *float64 } `json:"signals"`
		}
		require.NoError(t, gql.Post(q, &resp, vars(sourceRelay)))
		require.Len(t, resp.Signals, 1)
		require.NotNil(t, resp.Signals[0].Speed)
		assert.Equal(t, 72.0, *resp.Signals[0].Speed)

		resp.Signals = nil
		require.NoError(t, gql.Post(q, &resp, vars("did:web:other.example")))
		assert.Empty(t, resp.Signals, "another source's filter matches nothing")
	})

	t.Run("header extensions", func(t *testing.T) {
		var resp struct {
			LatestCloudEvent struct {
				Header struct {
					Source            string
					Producer          string
					Sourcesignature   *string
					Producersignature *string
					Arrivedat         *string
					Grant             *string
				}
			} `json:"latestCloudEvent"`
		}
		require.NoError(t, gql.Post(`query($subject: String!) { latestCloudEvent(subject: $subject) {
			header { source producer sourcesignature producersignature arrivedat grant } } }`,
			&resp, client.Var("subject", sourceVehicle)))
		h := resp.LatestCloudEvent.Header
		assert.Equal(t, sourceRelay, h.Source)
		assert.Equal(t, producerKey, h.Producer)
		require.NotNil(t, h.Sourcesignature)
		assert.Equal(t, sourceSig, *h.Sourcesignature)
		require.NotNil(t, h.Producersignature)
		assert.Equal(t, producerSig, *h.Producersignature)
		require.NotNil(t, h.Arrivedat)
		got, err := time.Parse(time.RFC3339Nano, *h.Arrivedat)
		require.NoError(t, err)
		assert.True(t, arrived.Equal(got), "arrivedat %s", *h.Arrivedat)
		require.NotNil(t, h.Grant)
		assert.Equal(t, sourceGrant, *h.Grant)
	})

	t.Run("connection events have none", func(t *testing.T) {
		seedRawStatus(t, svc.DB(), "conn-1", sourceVehicle, at.Add(time.Minute), speedAt(at.Add(time.Minute), 10))
		var resp struct {
			LatestCloudEvent struct {
				Header map[string]any
			} `json:"latestCloudEvent"`
		}
		require.NoError(t, gql.Post(`query($subject: String!) { latestCloudEvent(subject: $subject) {
			header { id sourcesignature producersignature arrivedat grant } } }`,
			&resp, client.Var("subject", sourceVehicle)))
		h := resp.LatestCloudEvent.Header
		assert.Equal(t, "conn-1", h["id"])
		for _, f := range []string{"sourcesignature", "producersignature", "arrivedat", "grant"} {
			assert.Nil(t, h[f], f)
		}
	})
}

// newSourcesClient serves signals and cloud events over svc, with tok as the
// caller's claims: the cloud-event resolvers check raw:read themselves.
func newSourcesClient(t *testing.T, svc *duck.Service, tok *tokenclaims.Token) *client.Client {
	t.Helper()
	repo, err := repositories.NewRepository(repositories.ComposeBackend(duck.NewLakeQueries(svc), nil))
	require.NoError(t, err)
	cfg := graph.Config{Resolvers: &graph.Resolver{
		SignalRepo:   repo,
		EventService: duck.NewLakeEventService(svc, nil, nil, ""),
	}}
	cfg.Directives.RequiresVehicleToken = passDirective
	cfg.Directives.IsSignal = passDirective
	cfg.Directives.HasAggregation = passDirective
	cfg.Directives.McpHide = passDirective
	cfg.Directives.RequiresAllOfPrivileges = passPrivilegeDirective
	cfg.Directives.RequiresOneOfPrivilege = passPrivilegeDirective
	srv := gqlhandler.New(graph.NewExecutableSchema(cfg))
	srv.AddTransport(transport.POST{})
	return client.New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), graph.ClaimsContextKey{}, tok)))
	}))
}
