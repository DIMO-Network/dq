// segments_graphql_test.go covers the telemetry-api segments query surface
// through REAL gqlgen execution: the segments and dailyActivity queries run
// against a Repository composed of the real DuckLake backend (lake.signals,
// decoded by the DuckLake materializer exactly as the parse-on-read pipeline
// produces it) and a recording fake SegmentsBackend that returns canned segment
// windows.
//
// Segment DETECTION is supplied by the recording fake here;
// everything around it — argument validation, config plumbing, default
// signal-set merging, per-segment signal aggregation and event counts over
// DuckDB, limit/after pagination, idling speed filtering, and dailyActivity
// bucketing — executes for real.
package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/99designs/gqlgen/client"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// segmentsVehicle is the subject for all segment tests on the test chain.
var segmentsVehicle = fmt.Sprintf("did:erc721:137:%s:55", vehicleNFT.Hex())

// segDay is a past UTC midnight: canned segment windows and materialized
// signals all live inside this day, so GetSegments' to=now clamp never fires.
var segDay = time.Now().UTC().AddDate(0, 0, -7).Truncate(24 * time.Hour)

// Fixed points inside segDay. Materialized speed values per window:
//
//	drive window  [10:00, 10:30): 40 @10:05, 80 @10:10, 65 @10:20 → MAX 80, MIN 40
//	short window  [11:00, 11:10): 55 @11:05                      → MAX 55
//	idle window   [12:00, 12:10): 0 @12:02, 0 @12:05             → MAX 0
//
// An ongoing segment starting 11:00 summarized to to=13:00 sees 55, 0, 0 → MAX 55.
// The whole day sees MAX 80.
var (
	driveStart = segDay.Add(10 * time.Hour)
	driveEnd   = segDay.Add(10*time.Hour + 30*time.Minute)
	shortStart = segDay.Add(11 * time.Hour)
	shortEnd   = segDay.Add(11*time.Hour + 10*time.Minute)
	idleStart  = segDay.Add(12 * time.Hour)
	idleEnd    = segDay.Add(12*time.Hour + 10*time.Minute)

	queryFrom = segDay.Add(6 * time.Hour)
	queryTo   = segDay.Add(13 * time.Hour)

	driveLocStart = &model.Location{Latitude: 40.7, Longitude: -74.0, Hdop: 1}
	driveLocEnd   = &model.Location{Latitude: 40.8, Longitude: -74.1, Hdop: 1}
)

// segmentsCall records one fake GetSegments invocation.
type segmentsCall struct {
	subject   string
	from      time.Time
	to        time.Time
	mechanism model.DetectionMechanism
	config    *model.SegmentConfig
}

// fakeSegments implements repositories.SegmentsBackend: it records every
// call and returns deep copies of canned segments (GetSegments mutates the
// returned segments during summary enrichment).
type fakeSegments struct {
	mu     sync.Mutex
	calls  []segmentsCall
	canned []*model.Segment
}

func (f *fakeSegments) GetSegments(_ context.Context, subject string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig) ([]*model.Segment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, segmentsCall{subject: subject, from: from, to: to, mechanism: mechanism, config: config})
	return cloneSegments(f.canned), nil
}

func cloneSegments(in []*model.Segment) []*model.Segment {
	out := make([]*model.Segment, len(in))
	for i, s := range in {
		c := *s
		if s.Start != nil {
			st := *s.Start
			if s.Start.Value != nil {
				v := *s.Start.Value
				st.Value = &v
			}
			c.Start = &st
		}
		if s.End != nil {
			en := *s.End
			if s.End.Value != nil {
				v := *s.End.Value
				en.Value = &v
			}
			c.End = &en
		}
		out[i] = &c
	}
	return out
}

func cannedComplete(start, end time.Time, startLoc, endLoc *model.Location, startedBefore bool) *model.Segment {
	return &model.Segment{
		Start:              &model.SignalLocation{Timestamp: start, Value: startLoc},
		End:                &model.SignalLocation{Timestamp: end, Value: endLoc},
		Duration:           int(end.Sub(start).Seconds()),
		StartedBeforeRange: startedBefore,
	}
}

func cannedOngoing(start time.Time, duration int) *model.Segment {
	return &model.Segment{
		Start:     &model.SignalLocation{Timestamp: start},
		Duration:  duration,
		IsOngoing: true,
	}
}

// newSegmentsGraphQLClient mirrors newGraphQLClient (dis_parity_test.go) but
// composes the real DuckLake backend (lake.signals on svc) with the provided
// SegmentsBackend fake.
func newSegmentsGraphQLClient(t *testing.T, svc *duck.Service, segs repositories.SegmentsBackend) *client.Client {
	t.Helper()
	repo, err := repositories.NewRepository(repositories.ComposeBackend(duck.NewLakeQueries(svc), segs))
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

// emptySegmentsService returns a DuckLake service with the empty raw_events
// table — for tests that never reach the signal backend (validation,
// config-plumb-through, future-to clamp).
func emptySegmentsService(t *testing.T) *duck.Service {
	t.Helper()
	return newLakeService(t, t.TempDir())
}

// buildSegmentsFixture materializes decoded speed signals for segmentsVehicle:
// the SAME dimo.status events as before are seeded into lake.raw_events and the
// DuckLake materializer decodes them into lake.signals (mirrors
// tests/ducklake_query_test.go), so the per-segment aggregations run over
// lake-sourced data identical to the bucket fixture's.
func buildSegmentsFixture(t *testing.T) *duck.Service {
	t.Helper()
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()

	seedRawStatus(t, db, "seg-drive", segmentsVehicle, driveStart.Add(5*time.Minute),
		speedAt(driveStart.Add(5*time.Minute), 40),
		speedAt(driveStart.Add(10*time.Minute), 80),
		speedAt(driveStart.Add(20*time.Minute), 65))
	seedRawStatus(t, db, "seg-short", segmentsVehicle, shortStart.Add(5*time.Minute),
		speedAt(shortStart.Add(5*time.Minute), 55))
	seedRawStatus(t, db, "seg-idle", segmentsVehicle, idleStart.Add(2*time.Minute),
		speedAt(idleStart.Add(2*time.Minute), 0),
		speedAt(idleStart.Add(5*time.Minute), 0))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
	}, nil, zerolog.Nop()).WithDuckLake(mat)
	require.Positive(t, drainRunner(t, ctx, runner))
	return svc
}

const segmentsQuery = `query Segments($subject: String!, $from: Time!, $to: Time!, $mechanism: DetectionMechanism!, $config: SegmentConfig, $signalRequests: [SegmentSignalRequest!], $eventRequests: [SegmentEventRequest!], $limit: Int, $after: Time) {
	segments(subject: $subject, from: $from, to: $to, mechanism: $mechanism, config: $config, signalRequests: $signalRequests, eventRequests: $eventRequests, limit: $limit, after: $after) {
		start { timestamp value { latitude longitude } }
		end { timestamp value { latitude longitude } }
		duration
		isOngoing
		startedBeforeRange
		signals { name agg value }
		eventCounts { name count }
	}
}`

const dailyActivityQuery = `query Daily($subject: String!, $from: Time!, $to: Time!, $mechanism: DetectionMechanism!, $timezone: String) {
	dailyActivity(subject: $subject, from: $from, to: $to, mechanism: $mechanism, timezone: $timezone) {
		start { timestamp value { latitude longitude } }
		end { timestamp }
		segmentCount
		duration
		signals { name agg value }
		eventCounts { name count }
	}
}`

type gqlSignalLocation struct {
	Timestamp string
	Value     *struct {
		Latitude  float64
		Longitude float64
	}
}

type gqlAggValue struct {
	Name  string
	Agg   string
	Value float64
}

type gqlEventCount struct {
	Name  string
	Count int
}

type gqlSegment struct {
	Start              gqlSignalLocation
	End                *gqlSignalLocation
	Duration           int
	IsOngoing          bool
	StartedBeforeRange bool
	Signals            []gqlAggValue
	EventCounts        []gqlEventCount
}

type segmentsResp struct {
	Segments []gqlSegment
}

type dailyActivityResp struct {
	DailyActivity []struct {
		Start        *gqlSignalLocation
		End          *struct{ Timestamp string }
		SegmentCount int
		Duration     int
		Signals      []gqlAggValue
		EventCounts  []gqlEventCount
	}
}

func parseGQLTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return ts
}

// TestSegmentsGraphQL_AllMechanisms executes the segments query through
// gqlgen for all six detection mechanisms. For each mechanism it asserts the
// fake backend received exactly that mechanism (and the query's subject,
// from, to), the canned segments flow through to JSON (duration, isOngoing
// with end omitted, startedBeforeRange, canned start/end coordinates), and
// the per-segment speed MAX is REALLY computed by DuckDB over the
// materialized signals (hand-computed expectations above). IDLING
// additionally exercises the >0-speed segment filter.
func TestSegmentsGraphQL_AllMechanisms(t *testing.T) {
	svc := buildSegmentsFixture(t)

	drivingCanned := func() []*model.Segment {
		return []*model.Segment{
			cannedComplete(driveStart, driveEnd, driveLocStart, driveLocEnd, true),
			cannedOngoing(shortStart, 7200),
		}
	}

	type expectedSegment struct {
		start        time.Time
		end          *time.Time
		duration     int
		isOngoing    bool
		startedPrior bool
		startLat     float64
		startLon     float64
		speedMax     float64
	}
	drivingExpected := []expectedSegment{
		{start: driveStart, end: &driveEnd, duration: 1800, startedPrior: true, startLat: 40.7, startLon: -74.0, speedMax: 80},
		// Ongoing: end omitted, summary range extends to the query `to`
		// (11:00–13:00 → speeds 55, 0, 0). Start location falls back to the
		// no-data location (0, 0) because nothing canned and no GPS data.
		{start: shortStart, duration: 7200, isOngoing: true, speedMax: 55},
	}

	tests := []struct {
		mechanism model.DetectionMechanism
		canned    []*model.Segment
		expected  []expectedSegment
	}{
		{mechanism: model.DetectionMechanismIgnitionDetection, canned: drivingCanned(), expected: drivingExpected},
		{mechanism: model.DetectionMechanismFrequencyAnalysis, canned: drivingCanned(), expected: drivingExpected},
		{mechanism: model.DetectionMechanismChangePointDetection, canned: drivingCanned(), expected: drivingExpected},
		{
			// IDLING: the drive segment's DuckDB-computed speed MAX is 80 > 0,
			// so the repository filters it out; only the idle window (MAX 0)
			// survives.
			mechanism: model.DetectionMechanismIdling,
			canned: []*model.Segment{
				cannedComplete(driveStart, driveEnd, driveLocStart, driveLocEnd, false),
				cannedComplete(idleStart, idleEnd, driveLocStart, driveLocEnd, false),
			},
			expected: []expectedSegment{
				{start: idleStart, end: &idleEnd, duration: 600, startLat: 40.7, startLon: -74.0, speedMax: 0},
			},
		},
		{mechanism: model.DetectionMechanismRefuel, canned: drivingCanned(), expected: drivingExpected},
		{mechanism: model.DetectionMechanismRecharge, canned: drivingCanned(), expected: drivingExpected},
	}

	for _, tc := range tests {
		t.Run(string(tc.mechanism), func(t *testing.T) {
			fake := &fakeSegments{canned: tc.canned}
			gql := newSegmentsGraphQLClient(t, svc, fake)

			var resp segmentsResp
			gql.MustPost(segmentsQuery, &resp,
				client.Var("subject", segmentsVehicle),
				client.Var("from", queryFrom.Format(time.RFC3339)),
				client.Var("to", queryTo.Format(time.RFC3339)),
				client.Var("mechanism", string(tc.mechanism)),
			)

			// The fake received exactly this mechanism and the query window.
			require.Len(t, fake.calls, 1)
			call := fake.calls[0]
			assert.Equal(t, tc.mechanism, call.mechanism)
			assert.Equal(t, segmentsVehicle, call.subject)
			assert.True(t, call.from.Equal(queryFrom), "fake from: got %v", call.from)
			assert.True(t, call.to.Equal(queryTo), "fake to: got %v", call.to)
			assert.Nil(t, call.config, "no config passed")

			require.Len(t, resp.Segments, len(tc.expected))
			for i, want := range tc.expected {
				got := resp.Segments[i]
				assert.True(t, parseGQLTime(t, got.Start.Timestamp).Equal(want.start), "segment %d start", i)
				assert.Equal(t, want.duration, got.Duration, "segment %d duration", i)
				assert.Equal(t, want.isOngoing, got.IsOngoing, "segment %d isOngoing", i)
				assert.Equal(t, want.startedPrior, got.StartedBeforeRange, "segment %d startedBeforeRange", i)
				if want.end == nil {
					assert.Nil(t, got.End, "ongoing segment %d must omit end", i)
				} else {
					require.NotNil(t, got.End, "segment %d end", i)
					assert.True(t, parseGQLTime(t, got.End.Timestamp).Equal(*want.end), "segment %d end timestamp", i)
				}
				require.NotNil(t, got.Start.Value, "segment %d start location", i)
				assert.InDelta(t, want.startLat, got.Start.Value.Latitude, 1e-9, "segment %d start latitude", i)
				assert.InDelta(t, want.startLon, got.Start.Value.Longitude, 1e-9, "segment %d start longitude", i)

				// Only speed was materialized, so of the default signal set
				// (speed MAX, fuel/SoC first+last, odometer first+last) only
				// speed MAX comes back — with the real DuckDB-computed value.
				require.Len(t, got.Signals, 1, "segment %d signals", i)
				assert.Equal(t, "speed", got.Signals[0].Name)
				assert.Equal(t, "MAX", got.Signals[0].Agg)
				assert.InDelta(t, want.speedMax, got.Signals[0].Value, 1e-9, "segment %d speed MAX", i)

				// No events materialized and none requested → empty counts.
				assert.Empty(t, got.EventCounts, "segment %d eventCounts", i)
			}
		})
	}
}

// TestSegmentsGraphQL_SignalRequestsMerge passes an extra speed MIN request
// plus a duplicate of the default speed MAX, asserting the extra aggregation
// is computed alongside the defaults and the duplicate is not doubled.
func TestSegmentsGraphQL_SignalRequestsMerge(t *testing.T) {
	svc := buildSegmentsFixture(t)
	fake := &fakeSegments{canned: []*model.Segment{
		cannedComplete(driveStart, driveEnd, driveLocStart, driveLocEnd, false),
	}}
	gql := newSegmentsGraphQLClient(t, svc, fake)

	var resp segmentsResp
	gql.MustPost(segmentsQuery, &resp,
		client.Var("subject", segmentsVehicle),
		client.Var("from", queryFrom.Format(time.RFC3339)),
		client.Var("to", queryTo.Format(time.RFC3339)),
		client.Var("mechanism", string(model.DetectionMechanismFrequencyAnalysis)),
		client.Var("signalRequests", []map[string]any{
			{"name": "speed", "agg": "MIN"},
			{"name": "speed", "agg": "MAX"}, // duplicate of a default — must not double
		}),
	)

	require.Len(t, resp.Segments, 1)
	signals := resp.Segments[0].Signals
	// Signals are sorted by (name, agg): exactly one MAX and one MIN.
	require.Len(t, signals, 2, "speed MAX (default) + speed MIN (extra), duplicate dropped")
	assert.Equal(t, gqlAggValue{Name: "speed", Agg: "MAX", Value: 80}, signals[0])
	assert.Equal(t, gqlAggValue{Name: "speed", Agg: "MIN", Value: 40}, signals[1])
}

// TestSegmentsGraphQL_EventRequests requests an event name with no
// materialized events: buildEventSummary returns one entry per requested
// name with count 0.
func TestSegmentsGraphQL_EventRequests(t *testing.T) {
	svc := buildSegmentsFixture(t)
	fake := &fakeSegments{canned: []*model.Segment{
		cannedComplete(driveStart, driveEnd, driveLocStart, driveLocEnd, false),
	}}
	gql := newSegmentsGraphQLClient(t, svc, fake)

	var resp segmentsResp
	gql.MustPost(segmentsQuery, &resp,
		client.Var("subject", segmentsVehicle),
		client.Var("from", queryFrom.Format(time.RFC3339)),
		client.Var("to", queryTo.Format(time.RFC3339)),
		client.Var("mechanism", string(model.DetectionMechanismIgnitionDetection)),
		client.Var("eventRequests", []map[string]any{{"name": "harshBraking"}}),
	)

	require.Len(t, resp.Segments, 1)
	require.Len(t, resp.Segments[0].EventCounts, 1, "requested names always appear in eventCounts")
	assert.Equal(t, gqlEventCount{Name: "harshBraking", Count: 0}, resp.Segments[0].EventCounts[0])
}

// TestSegmentsGraphQL_ConfigPlumbThrough passes a full SegmentConfig for
// IDLING (maxIdleRpm) and REFUEL (minIncreasePercent) and asserts every
// field arrives at the SegmentsBackend unchanged.
func TestSegmentsGraphQL_ConfigPlumbThrough(t *testing.T) {
	fullConfig := map[string]any{
		"maxGapSeconds":             120,
		"minSegmentDurationSeconds": 300,
		"signalCountThreshold":      5,
		"maxIdleRpm":                800,
		"minIncreasePercent":        20,
	}

	for _, mechanism := range []model.DetectionMechanism{
		model.DetectionMechanismIdling,
		model.DetectionMechanismRefuel,
	} {
		t.Run(string(mechanism), func(t *testing.T) {
			fake := &fakeSegments{}
			gql := newSegmentsGraphQLClient(t, emptySegmentsService(t), fake)

			var resp segmentsResp
			gql.MustPost(segmentsQuery, &resp,
				client.Var("subject", segmentsVehicle),
				client.Var("from", queryFrom.Format(time.RFC3339)),
				client.Var("to", queryTo.Format(time.RFC3339)),
				client.Var("mechanism", string(mechanism)),
				client.Var("config", fullConfig),
			)

			require.Len(t, fake.calls, 1)
			cfg := fake.calls[0].config
			require.NotNil(t, cfg)
			require.NotNil(t, cfg.MaxGapSeconds)
			assert.Equal(t, 120, *cfg.MaxGapSeconds)
			require.NotNil(t, cfg.MinSegmentDurationSeconds)
			assert.Equal(t, 300, *cfg.MinSegmentDurationSeconds)
			require.NotNil(t, cfg.SignalCountThreshold)
			assert.Equal(t, 5, *cfg.SignalCountThreshold)
			require.NotNil(t, cfg.MaxIdleRpm)
			assert.Equal(t, 800, *cfg.MaxIdleRpm)
			require.NotNil(t, cfg.MinIncreasePercent)
			assert.Equal(t, 20, *cfg.MinIncreasePercent)
			assert.Empty(t, resp.Segments)
		})
	}
}

// TestSegmentsGraphQL_ValidationErrors asserts every documented validation
// rule surfaces as a GraphQL error (no panic) and that the SegmentsBackend
// is never reached.
func TestSegmentsGraphQL_ValidationErrors(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name      string
		from, to  time.Time
		mechanism model.DetectionMechanism
		limit     *int
		config    map[string]any
		wantMsg   string
	}{
		{
			name: "from equals to",
			from: queryFrom, to: queryFrom,
			mechanism: model.DetectionMechanismFrequencyAnalysis,
			wantMsg:   "from and to times cannot be equal",
		},
		{
			name: "range exceeds 32 days",
			from: now.AddDate(0, 0, -40), to: now.AddDate(0, 0, -7),
			mechanism: model.DetectionMechanismFrequencyAnalysis,
			wantMsg:   "date range exceeds maximum of 32 days",
		},
		{
			name: "limit zero",
			from: queryFrom, to: queryTo,
			mechanism: model.DetectionMechanismFrequencyAnalysis,
			limit:     ptr(0),
			wantMsg:   "limit must be between 1 and 200",
		},
		{
			name: "limit above max",
			from: queryFrom, to: queryTo,
			mechanism: model.DetectionMechanismFrequencyAnalysis,
			limit:     ptr(201),
			wantMsg:   "limit must be between 1 and 200",
		},
		{
			name: "idling maxIdleRpm too low",
			from: queryFrom, to: queryTo,
			mechanism: model.DetectionMechanismIdling,
			config:    map[string]any{"maxIdleRpm": 200},
			wantMsg:   "maxIdleRpm must be between 300 and 3000",
		},
		{
			name: "refuel minIncreasePercent zero",
			from: queryFrom, to: queryTo,
			mechanism: model.DetectionMechanismRefuel,
			config:    map[string]any{"minIncreasePercent": 0},
			wantMsg:   "minIncreasePercent must be between 1 and 100",
		},
		{
			name: "maxGapSeconds too low",
			from: queryFrom, to: queryTo,
			mechanism: model.DetectionMechanismIgnitionDetection,
			config:    map[string]any{"maxGapSeconds": 30},
			wantMsg:   "maxGapSeconds must be between 60 and 3600",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeSegments{}
			gql := newSegmentsGraphQLClient(t, emptySegmentsService(t), fake)

			opts := []client.Option{
				client.Var("subject", segmentsVehicle),
				client.Var("from", tc.from.Format(time.RFC3339)),
				client.Var("to", tc.to.Format(time.RFC3339)),
				client.Var("mechanism", string(tc.mechanism)),
			}
			if tc.limit != nil {
				opts = append(opts, client.Var("limit", *tc.limit))
			}
			if tc.config != nil {
				opts = append(opts, client.Var("config", tc.config))
			}

			var resp segmentsResp
			err := gql.Post(segmentsQuery, &resp, opts...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantMsg)
			assert.Empty(t, fake.calls, "validation must reject before reaching the segments backend")
		})
	}
}

// TestSegmentsGraphQL_FutureToClampedToNow passes a `to` 48h in the future:
// it is clamped to now BEFORE validation, so the query succeeds and the
// backend receives to ≈ now.
func TestSegmentsGraphQL_FutureToClampedToNow(t *testing.T) {
	fake := &fakeSegments{}
	gql := newSegmentsGraphQLClient(t, emptySegmentsService(t), fake)

	from := time.Now().UTC().Add(-2 * time.Hour)
	futureTo := time.Now().UTC().Add(48 * time.Hour)

	var resp segmentsResp
	gql.MustPost(segmentsQuery, &resp,
		client.Var("subject", segmentsVehicle),
		client.Var("from", from.Format(time.RFC3339)),
		client.Var("to", futureTo.Format(time.RFC3339)),
		client.Var("mechanism", string(model.DetectionMechanismFrequencyAnalysis)),
	)

	require.Len(t, fake.calls, 1)
	call := fake.calls[0]
	assert.True(t, call.to.Before(futureTo), "future to must be clamped")
	assert.WithinDuration(t, time.Now(), call.to, time.Minute, "clamped to ≈ now")
	assert.Empty(t, resp.Segments)
}

// TestSegmentsGraphQL_LimitAndAfter covers limit truncation (3 canned, limit
// 2 → first 2 returned, each still summary-enriched) and after-cursor
// pagination (the backend receives from advanced to after+1ns).
func TestSegmentsGraphQL_LimitAndAfter(t *testing.T) {
	svc := buildSegmentsFixture(t)

	t.Run("limit truncates", func(t *testing.T) {
		fake := &fakeSegments{canned: []*model.Segment{
			cannedComplete(driveStart, driveEnd, driveLocStart, driveLocEnd, false),
			cannedComplete(shortStart, shortEnd, nil, nil, false),
			cannedComplete(idleStart, idleEnd, nil, nil, false),
		}}
		gql := newSegmentsGraphQLClient(t, svc, fake)

		var resp segmentsResp
		gql.MustPost(segmentsQuery, &resp,
			client.Var("subject", segmentsVehicle),
			client.Var("from", queryFrom.Format(time.RFC3339)),
			client.Var("to", queryTo.Format(time.RFC3339)),
			client.Var("mechanism", string(model.DetectionMechanismFrequencyAnalysis)),
			client.Var("limit", 2),
		)

		require.Len(t, resp.Segments, 2, "limit 2 truncates 3 canned segments")
		assert.True(t, parseGQLTime(t, resp.Segments[0].Start.Timestamp).Equal(driveStart))
		assert.True(t, parseGQLTime(t, resp.Segments[1].Start.Timestamp).Equal(shortStart))
		// Truncated set is still enriched with real DuckDB aggregates.
		require.Len(t, resp.Segments[0].Signals, 1)
		assert.InDelta(t, 80.0, resp.Segments[0].Signals[0].Value, 1e-9)
		require.Len(t, resp.Segments[1].Signals, 1)
		assert.InDelta(t, 55.0, resp.Segments[1].Signals[0].Value, 1e-9)
	})

	t.Run("after advances from", func(t *testing.T) {
		fake := &fakeSegments{}
		gql := newSegmentsGraphQLClient(t, svc, fake)

		var resp segmentsResp
		gql.MustPost(segmentsQuery, &resp,
			client.Var("subject", segmentsVehicle),
			client.Var("from", queryFrom.Format(time.RFC3339)),
			client.Var("to", queryTo.Format(time.RFC3339)),
			client.Var("mechanism", string(model.DetectionMechanismFrequencyAnalysis)),
			client.Var("after", driveStart.Format(time.RFC3339)),
		)

		require.Len(t, fake.calls, 1)
		wantFrom := driveStart.Add(time.Nanosecond)
		assert.True(t, fake.calls[0].from.Equal(wantFrom),
			"after cursor must advance from to after+1ns: got %v want %v", fake.calls[0].from, wantFrom)
		assert.True(t, fake.calls[0].to.Equal(queryTo))
	})
}

// TestDailyActivityGraphQL_AllowedMechanisms runs dailyActivity for the three
// allowed mechanisms: one record per calendar day with segmentCount, summed
// overlap duration from the canned segments, real DuckDB day-level signal
// aggregates, and day-boundary start/end timestamps.
func TestDailyActivityGraphQL_AllowedMechanisms(t *testing.T) {
	svc := buildSegmentsFixture(t)

	for _, mechanism := range []model.DetectionMechanism{
		model.DetectionMechanismIgnitionDetection,
		model.DetectionMechanismFrequencyAnalysis,
		model.DetectionMechanismChangePointDetection,
	} {
		t.Run(string(mechanism), func(t *testing.T) {
			fake := &fakeSegments{canned: []*model.Segment{
				cannedComplete(driveStart, driveEnd, driveLocStart, driveLocEnd, false), // 1800s
				cannedComplete(idleStart, idleEnd, nil, nil, false),                     // 600s
			}}
			gql := newSegmentsGraphQLClient(t, svc, fake)

			var resp dailyActivityResp
			gql.MustPost(dailyActivityQuery, &resp,
				client.Var("subject", segmentsVehicle),
				client.Var("from", queryFrom.Format(time.RFC3339)),
				client.Var("to", queryTo.Format(time.RFC3339)),
				client.Var("mechanism", string(mechanism)),
			)

			// The underlying segment fetch spans whole calendar days.
			require.Len(t, fake.calls, 1)
			assert.Equal(t, mechanism, fake.calls[0].mechanism)
			assert.True(t, fake.calls[0].from.Equal(segDay), "rangeStart = day start")
			assert.True(t, fake.calls[0].to.Equal(segDay.Add(24*time.Hour)), "rangeEnd = day end")

			require.Len(t, resp.DailyActivity, 1, "single calendar day in range")
			day := resp.DailyActivity[0]
			assert.Equal(t, 2, day.SegmentCount)
			assert.Equal(t, 1800+600, day.Duration, "summed segment overlap durations")

			require.NotNil(t, day.Start)
			assert.True(t, parseGQLTime(t, day.Start.Timestamp).Equal(segDay), "start = UTC day start")
			require.NotNil(t, day.End)
			assert.True(t, parseGQLTime(t, day.End.Timestamp).Equal(segDay.Add(24*time.Hour)), "end = UTC day end")
			// First segment's canned start location wins for the day.
			require.NotNil(t, day.Start.Value)
			assert.InDelta(t, 40.7, day.Start.Value.Latitude, 1e-9)
			assert.InDelta(t, -74.0, day.Start.Value.Longitude, 1e-9)

			// Day-level speed MAX over all materialized points = 80.
			require.Len(t, day.Signals, 1)
			assert.Equal(t, "speed", day.Signals[0].Name)
			assert.Equal(t, "MAX", day.Signals[0].Agg)
			assert.InDelta(t, 80.0, day.Signals[0].Value, 1e-9)
			assert.Empty(t, day.EventCounts)
		})
	}
}

// TestDailyActivityGraphQL_Rejections covers the disallowed mechanisms
// (IDLING, REFUEL, RECHARGE) and invalid timezone — each must produce a
// GraphQL error before any backend call.
func TestDailyActivityGraphQL_Rejections(t *testing.T) {
	for _, mechanism := range []model.DetectionMechanism{
		model.DetectionMechanismIdling,
		model.DetectionMechanismRefuel,
		model.DetectionMechanismRecharge,
	} {
		t.Run(string(mechanism), func(t *testing.T) {
			fake := &fakeSegments{}
			gql := newSegmentsGraphQLClient(t, emptySegmentsService(t), fake)

			var resp dailyActivityResp
			err := gql.Post(dailyActivityQuery, &resp,
				client.Var("subject", segmentsVehicle),
				client.Var("from", queryFrom.Format(time.RFC3339)),
				client.Var("to", queryTo.Format(time.RFC3339)),
				client.Var("mechanism", string(mechanism)),
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "dailyActivity does not accept mechanism "+string(mechanism))
			assert.Empty(t, fake.calls)
		})
	}

	t.Run("invalid timezone", func(t *testing.T) {
		fake := &fakeSegments{}
		gql := newSegmentsGraphQLClient(t, emptySegmentsService(t), fake)

		var resp dailyActivityResp
		err := gql.Post(dailyActivityQuery, &resp,
			client.Var("subject", segmentsVehicle),
			client.Var("from", queryFrom.Format(time.RFC3339)),
			client.Var("to", queryTo.Format(time.RFC3339)),
			client.Var("mechanism", string(model.DetectionMechanismFrequencyAnalysis)),
			client.Var("timezone", "Mars/Olympus"),
		)
		require.Error(t, err)
		// The error surfaces through JSON, so quotes are escaped; match parts.
		assert.Contains(t, err.Error(), "invalid timezone")
		assert.Contains(t, err.Error(), "Mars/Olympus")
		assert.Empty(t, fake.calls)
	})
}

// TestDailyActivityGraphQL_Timezone runs dailyActivity with a valid IANA
// timezone: calendar days are bucketed in that zone (the record's start
// timestamp is the zone's midnight, not UTC's) and segment overlap math
// still works.
func TestDailyActivityGraphQL_Timezone(t *testing.T) {
	svc := buildSegmentsFixture(t)
	fake := &fakeSegments{canned: []*model.Segment{
		cannedComplete(driveStart, driveEnd, driveLocStart, driveLocEnd, false),
		cannedComplete(idleStart, idleEnd, nil, nil, false),
	}}
	gql := newSegmentsGraphQLClient(t, svc, fake)

	var resp dailyActivityResp
	gql.MustPost(dailyActivityQuery, &resp,
		client.Var("subject", segmentsVehicle),
		client.Var("from", queryFrom.Format(time.RFC3339)),
		client.Var("to", queryTo.Format(time.RFC3339)),
		client.Var("mechanism", string(model.DetectionMechanismFrequencyAnalysis)),
		client.Var("timezone", "America/New_York"),
	)

	// Expected day start: midnight America/New_York on queryFrom's local date.
	nyc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	fromNYC := queryFrom.In(nyc)
	wantDayStart := time.Date(fromNYC.Year(), fromNYC.Month(), fromNYC.Day(), 0, 0, 0, 0, nyc)

	require.Len(t, resp.DailyActivity, 1)
	day := resp.DailyActivity[0]
	assert.Equal(t, 2, day.SegmentCount)
	assert.Equal(t, 2400, day.Duration)
	require.NotNil(t, day.Start)
	gotStart := parseGQLTime(t, day.Start.Timestamp)
	assert.True(t, gotStart.Equal(wantDayStart), "day start must be NYC midnight: got %v want %v", gotStart, wantDayStart)
	assert.NotEqual(t, 0, wantDayStart.UTC().Hour(), "sanity: NYC midnight is not UTC midnight")
}

func ptr[T any](v T) *T { return &v }
