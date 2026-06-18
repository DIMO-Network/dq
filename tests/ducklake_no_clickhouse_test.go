// ducklake_no_clickhouse_test.go proves that in QUERY_BACKEND=ducklake mode
// the event service (cloudEvents / latestCloudEvent / availableCloudEventTypes)
// can be fully constructed and serve queries against lake.raw_events without
// any ClickHouse reachable.
//
// The key property being tested: duck.NewLakeEventService (the implementation
// that newEventService selects for ducklake mode) serves all index/summary/
// payload queries from a file-backed DuckLake catalog — no ClickHouse DSN
// required, no ClickHouse client constructed.
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fetchSubject is the test vehicle DID for no-ClickHouse fetch tests.
var fetchSubject = fmt.Sprintf("did:erc721:137:%s:200", vehicleNFT.Hex())

// newLakeFetchService opens a file-catalog DuckDB service (no ClickHouse
// required), creates lake.raw_events, and returns a LakeEventService.
// This mirrors the duck.Service that newEventService constructs in ducklake mode.
func newLakeFetchService(t *testing.T) (*duck.LakeEventService, *duck.Service) {
	t.Helper()
	dir := t.TempDir()
	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      dir + "/catalog.ducklake",
		DataPath:        dir + "/lakedata",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	ctx := context.Background()
	_, err = svc.DB().ExecContext(ctx, `CREATE TABLE IF NOT EXISTS lake.raw_events (
		subject VARCHAR, "time" TIMESTAMP WITH TIME ZONE, type VARCHAR, id VARCHAR,
		source VARCHAR, producer VARCHAR, data_content_type VARCHAR, data_version VARCHAR,
		extras VARCHAR, data VARCHAR, data_base64 BLOB, data_index_key VARCHAR, voids_id VARCHAR)`)
	require.NoError(t, err)

	// NewLakeEventService takes (svc, presigner, bucket). presigner and bucket
	// may be nil/empty when no blob payloads are involved — same as the app
	// wiring in ducklake mode when no S3 presigning is needed.
	return duck.NewLakeEventService(svc, nil, ""), svc
}

// seedFetchRawEvent inserts one raw event into lake.raw_events with the given
// type and inline JSON payload.
func seedFetchRawEvent(t *testing.T, duckSvc *duck.Service, id, subject, evType string, ts time.Time, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	_, err = duckSvc.DB().ExecContext(context.Background(),
		`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer,
		 data_content_type, data_version, extras, data, voids_id)
		 VALUES (?, ?, ?, ?, '', '', '', '', '{}', ?, '')`,
		subject, ts.UTC(), evType, id, string(data))
	require.NoError(t, err)
}

// TestDuckLakeNoClickHouse_FetchQueriesWork boots the lake fetch event service
// (no ClickHouse at all) and asserts all four fetch surfaces serve correctly.
func TestDuckLakeNoClickHouse_FetchQueriesWork(t *testing.T) {
	ctx := context.Background()
	svc, duckSvc := newLakeFetchService(t)

	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	const typeStatus = "dimo.status"
	const typeFingerprint = "dimo.fingerprint"

	// Seed three events: two status, one fingerprint.
	seedFetchRawEvent(t, duckSvc, "fetch-1", fetchSubject, typeStatus, day.Add(time.Hour), map[string]any{"speed": 30})
	seedFetchRawEvent(t, duckSvc, "fetch-2", fetchSubject, typeStatus, day.Add(2*time.Hour), map[string]any{"speed": 60})
	seedFetchRawEvent(t, duckSvc, "fetch-fp", fetchSubject, typeFingerprint, day.Add(3*time.Hour), map[string]any{"vin": "1HG"})

	subjectFilter := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{fetchSubject}},
	}

	// --- ListIndexesAdvanced ---
	indexes, err := svc.ListIndexesAdvanced(ctx, 10, subjectFilter)
	require.NoError(t, err, "ListIndexesAdvanced must succeed with no ClickHouse")
	require.Len(t, indexes, 3)
	// newest-first: fetch-fp (3h) > fetch-2 (2h) > fetch-1 (1h)
	assert.Equal(t, "fetch-fp", indexes[0].ID)
	assert.Equal(t, "fetch-2", indexes[1].ID)
	assert.Equal(t, "fetch-1", indexes[2].ID)

	// --- GetLatestIndexAdvanced ---
	latest, err := svc.GetLatestIndexAdvanced(ctx, subjectFilter)
	require.NoError(t, err, "GetLatestIndexAdvanced must succeed with no ClickHouse")
	assert.Equal(t, "fetch-fp", latest.ID)

	// --- GetCloudEventTypeSummariesAdvanced ---
	summaries, err := svc.GetCloudEventTypeSummariesAdvanced(ctx, subjectFilter)
	require.NoError(t, err, "GetCloudEventTypeSummariesAdvanced must succeed with no ClickHouse")
	require.Len(t, summaries, 2)
	sumByType := map[string]eventrepo.CloudEventTypeSummary{}
	for _, s := range summaries {
		sumByType[s.Type] = s
	}
	assert.Equal(t, uint64(2), sumByType[typeStatus].Count)
	assert.Equal(t, uint64(1), sumByType[typeFingerprint].Count)

	// --- GetCloudEventFromIndex (inline data round-trip) ---
	raw, err := svc.GetCloudEventFromIndex(ctx, &indexes[2], "") // fetch-1
	require.NoError(t, err, "GetCloudEventFromIndex must succeed with no ClickHouse")
	require.NotNil(t, raw.Data)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(raw.Data, &payload))
	assert.EqualValues(t, 30, payload["speed"])

	// --- Type filter narrows correctly ---
	typeFilter := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{fetchSubject}},
		Type:    &grpc.StringFilterOption{In: []string{typeFingerprint}},
	}
	fpIndexes, err := svc.ListIndexesAdvanced(ctx, 10, typeFilter)
	require.NoError(t, err)
	require.Len(t, fpIndexes, 1)
	assert.Equal(t, "fetch-fp", fpIndexes[0].ID)

	// --- Before filter ---
	beforeFilter := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{fetchSubject}},
		Before:  timestamppb.New(day.Add(2*time.Hour + 30*time.Minute)),
	}
	beforeIndexes, err := svc.ListIndexesAdvanced(ctx, 10, beforeFilter)
	require.NoError(t, err)
	// fetch-1 (1h) and fetch-2 (2h) are before 2h30m; fetch-fp (3h) is not
	require.Len(t, beforeIndexes, 2)

	// --- ErrNotFound on empty filter ---
	emptyFilter := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{"did:erc721:137:0x0:9999"}},
	}
	_, err = svc.GetLatestIndexAdvanced(ctx, emptyFilter)
	assert.ErrorIs(t, err, duck.ErrNotFound, "GetLatestIndexAdvanced must return ErrNotFound when no events match")
}

// TestDuckLakeNoClickHouse_VoidingExcludesEvents proves tombstone voiding
// works correctly in the lake fetch path without ClickHouse.
func TestDuckLakeNoClickHouse_VoidingExcludesEvents(t *testing.T) {
	ctx := context.Background()
	svc, duckSvc := newLakeFetchService(t)

	// Use a distinct subject to avoid interference with other tests.
	voidSubject := fmt.Sprintf("did:erc721:137:%s:201", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	const typeStatus = "dimo.status"

	// Insert a normal event and an un-voided event.
	seedFetchRawEvent(t, duckSvc, "void-target", voidSubject, typeStatus, day.Add(time.Hour), map[string]any{"speed": 10})
	seedFetchRawEvent(t, duckSvc, "not-voided", voidSubject, typeStatus, day.Add(2*time.Hour), map[string]any{"speed": 20})

	// Insert tombstone: voids_id = "void-target".
	_, err := duckSvc.DB().ExecContext(ctx,
		`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer,
		 data_content_type, data_version, extras, data, voids_id)
		 VALUES (?, ?, ?, ?, '', '', '', '', '{}', '{}', 'void-target')`,
		voidSubject, day.Add(3*time.Hour).UTC(), typeStatus, "tombstone-1")
	require.NoError(t, err)

	subjectFilter := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{voidSubject}},
	}

	// filterFromAdvanced always sets ExcludeVoided = true. ListIndexesAdvanced
	// must exclude void-target (voided) and the tombstone itself; only
	// not-voided should appear.
	indexes, err := svc.ListIndexesAdvanced(ctx, 10, subjectFilter)
	require.NoError(t, err, "voiding query must succeed without ClickHouse")
	require.Len(t, indexes, 1, "only the non-voided event should be returned")
	assert.Equal(t, "not-voided", indexes[0].ID)

	// Type summaries must also exclude voided events.
	summaries, err := svc.GetCloudEventTypeSummariesAdvanced(ctx, subjectFilter)
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	assert.Equal(t, uint64(1), summaries[0].Count, "voided event excluded from summary count")
}

// TestDuckLakeNoClickHouse_SegmentsSucceed proves the signal/segments path
// also works in ducklake mode without ClickHouse via the LakeQueries backend.
func TestDuckLakeNoClickHouse_SegmentsSucceed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	lkSvc := newLakeService(t, dir)
	db := lkSvc.DB()
	segSubject := fmt.Sprintf("did:erc721:137:%s:202", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)

	// Seed raw events exactly as din writes them; materializer decodes them.
	seedRawStatus(t, db, "nch-1", segSubject, day.Add(time.Hour),
		speedAt(day.Add(time.Hour), 55))
	seedRawStatus(t, db, "nch-2", segSubject, day.Add(2*time.Hour),
		speedAt(day.Add(2*time.Hour), 75))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, nil, zerolog.Nop()).
		WithDuckLake(mat)

	processed := drainRunner(t, ctx, runner)
	require.Equal(t, 2, processed, "two raw events decoded without ClickHouse")

	// LakeQueries serves GetAvailableSignals with no CH.
	lakeQ := duck.NewLakeQueries(lkSvc)
	signals, err := lakeQ.GetAvailableSignals(ctx, segSubject, nil)
	require.NoError(t, err, "GetAvailableSignals must succeed without ClickHouse")
	assert.Contains(t, signals, "speed")
}
