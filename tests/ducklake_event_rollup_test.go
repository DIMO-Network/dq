// ducklake_event_rollup_test.go validates the events_latest rollup (finding #5a):
// the materializer maintains lake.events_latest per batch (dirty-subject, off the
// decode commit, self-healing), and GetEventSummaries serves from it. The asserted
// values are the correct deduped full-history result, so this confirms the rollup is
// a faithful materialized view of GetEventSummaries, not a shortcut.
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	evEngineBlock   = "security.engineBlock"
	evEngineUnblock = "security.engineUnblock"
)

// deviceEvents builds a dimo.events raw event (the din sink's row shape) carrying a
// default-module events payload.
func deviceEvents(id, subject string, ts time.Time, events ...map[string]any) cloudevent.StoredEvent {
	payload, _ := json.Marshal(map[string]any{"events": events})
	return cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        cloudevent.TypeEvents,
			Subject:     subject,
			Source:      "0xConnLicense",
			Producer:    subject,
			ID:          id,
			Time:        ts,
			DataVersion: "default/v1.0",
		},
		Data: payload,
	}}
}

func eventAt(name string, ts time.Time) map[string]any {
	return map[string]any{"name": name, "timestamp": ts.Format(time.RFC3339Nano), "tags": []string{}}
}

func eventSummaryByName(t *testing.T, ctx context.Context, q *duck.Queries, subject string) map[string]struct {
	count       uint64
	first, last time.Time
} {
	t.Helper()
	sums, err := q.GetEventSummaries(ctx, subject)
	require.NoError(t, err)
	out := map[string]struct {
		count       uint64
		first, last time.Time
	}{}
	for _, s := range sums {
		out[s.Name] = struct {
			count       uint64
			first, last time.Time
		}{s.Count, s.FirstSeen, s.LastSeen}
	}
	return out
}

func TestDuckLake_EventSummaryRollup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:9", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)
	t1, t2, t3 := day.Add(time.Hour), day.Add(2*time.Hour), day.Add(3*time.Hour)

	// engineBlock @ t1, engineUnblock @ t2, engineBlock @ t3, plus a DUPLICATE of the
	// t1 engineBlock under a different cloud_event_id: it survives the write anti-join
	// (keyed on cloud_event_id) but the read dedup (subject,timestamp,name,source)
	// collapses it, so the rollup must count engineBlock @ t1 exactly once.
	seedRawEvent(t, svc, deviceEvents("ev-1", subject, t1, eventAt(evEngineBlock, t1)))
	seedRawEvent(t, svc, deviceEvents("ev-2", subject, t2, eventAt(evEngineUnblock, t2)))
	seedRawEvent(t, svc, deviceEvents("ev-3", subject, t3, eventAt(evEngineBlock, t3)))
	seedRawEvent(t, svc, deviceEvents("ev-dup", subject, t1, eventAt(evEngineBlock, t1)))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 4, drainRunner(t, ctx, runner))

	q := duck.NewLakeQueries(svc)

	// The rollup table exists and is being read (not the base fallback): confirm the
	// row was actually written to events_latest.
	var rollupRows int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM lake.events_latest WHERE subject = ?", subject).Scan(&rollupRows))
	assert.Equal(t, 2, rollupRows, "one events_latest row per (subject,name)")

	got := eventSummaryByName(t, ctx, q, subject)
	require.Contains(t, got, evEngineBlock)
	require.Contains(t, got, evEngineUnblock)
	assert.EqualValues(t, 2, got[evEngineBlock].count, "engineBlock deduped: t1 (from ev-1+ev-dup) and t3")
	assert.True(t, got[evEngineBlock].first.Equal(t1), "engineBlock first_seen = t1")
	assert.True(t, got[evEngineBlock].last.Equal(t3), "engineBlock last_seen = t3")
	assert.EqualValues(t, 1, got[evEngineUnblock].count, "engineUnblock once at t2")
	assert.True(t, got[evEngineUnblock].first.Equal(t2))

	// Rollup == base scan: the summary served from events_latest must match a direct
	// deduped scan of lake.events, proving the rollup is a faithful materialized view.
	var baseBlock, baseUnblock int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT 1 FROM lake.events WHERE subject = ? AND name = ?
		QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, timestamp, name, source ORDER BY cloud_event_id) = 1)`,
		subject, evEngineBlock).Scan(&baseBlock))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT 1 FROM lake.events WHERE subject = ? AND name = ?
		QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, timestamp, name, source ORDER BY cloud_event_id) = 1)`,
		subject, evEngineUnblock).Scan(&baseUnblock))
	assert.EqualValues(t, baseBlock, got[evEngineBlock].count, "rollup count matches base dedup scan")
	assert.EqualValues(t, baseUnblock, got[evEngineUnblock].count)

	// Incremental: a fourth engineBlock at t4 updates the rollup in place (count 3,
	// last_seen advances) — the dirty-subject flush path.
	t4 := day.Add(4 * time.Hour)
	seedRawEvent(t, svc, deviceEvents("ev-4", subject, t4, eventAt(evEngineBlock, t4)))
	require.Equal(t, 1, drainRunner(t, ctx, runner))
	got2 := eventSummaryByName(t, ctx, q, subject)
	assert.EqualValues(t, 3, got2[evEngineBlock].count, "rollup incremented to include t4")
	assert.True(t, got2[evEngineBlock].last.Equal(t4), "last_seen advanced to t4")
}

// TestDuckLake_EventRollup_FullRebuildParity proves RecomputeEventRollup (the
// disaster-recovery / first-create backfill) reconstructs events_latest identically
// to the per-batch flush: after dropping the rollup and rebuilding from the full
// base, GetEventSummaries returns the same values.
func TestDuckLake_EventRollup_FullRebuildParity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:11", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)

	seedRawEvent(t, svc, deviceEvents("fr-1", subject, day.Add(time.Hour), eventAt(evEngineBlock, day.Add(time.Hour))))
	seedRawEvent(t, svc, deviceEvents("fr-2", subject, day.Add(2*time.Hour), eventAt(evEngineUnblock, day.Add(2*time.Hour))))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	q := duck.NewLakeQueries(svc)
	before := eventSummaryByName(t, ctx, q, subject)

	// Truncate the rollup and rebuild from the entire base — the DR / first-create path.
	_, err = db.ExecContext(ctx, "DELETE FROM lake.events_latest")
	require.NoError(t, err)
	require.NoError(t, mat.RecomputeEventRollup(ctx))

	after := eventSummaryByName(t, ctx, q, subject)
	assert.Equal(t, before, after, "full rebuild reconstructs events_latest identically to the per-batch flush")
	require.Contains(t, after, evEngineBlock)
	assert.EqualValues(t, 1, after[evEngineBlock].count)
}

// TestDuckLake_EventRollup_FirstCreateBackfill proves the migration case: a catalog
// that already has lake.events but NO events_latest (existing deployment getting the
// rollup for the first time) backfills every already-decoded subject on the first
// flush — a DORMANT vehicle (no new events after the rollup is created) must still
// appear in the summary, not read empty. This is the safety guarantee for the
// read-path switch to the rollup.
func TestDuckLake_EventRollup_FirstCreateBackfill(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()
	subject := fmt.Sprintf("did:erc721:137:%s:13", vehicleNFT.Hex())
	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)

	seedRawEvent(t, svc, deviceEvents("bf-1", subject, day.Add(time.Hour), eventAt(evEngineBlock, day.Add(time.Hour))))
	seedRawEvent(t, svc, deviceEvents("bf-2", subject, day.Add(2*time.Hour), eventAt(evEngineBlock, day.Add(2*time.Hour))))

	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, zerolog.Nop()).
		WithDuckLake(mat)
	require.Equal(t, 2, drainRunner(t, ctx, runner))

	// Simulate the pre-migration state: lake.events populated, but no events_latest.
	_, err = db.ExecContext(ctx, "DROP TABLE lake.events_latest")
	require.NoError(t, err)

	// A fresh materializer over the same catalog: ensureSchema recreates events_latest
	// and, seeing a pre-existing lake.events, escalates the first flush to a full
	// backfill. No NEW raw events arrive, so this only works if first-create backfills
	// dormant subjects.
	mat2, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, mat2.FlushEventRollup(ctx))

	q := duck.NewLakeQueries(svc)
	got := eventSummaryByName(t, ctx, q, subject)
	require.Contains(t, got, evEngineBlock, "dormant subject must be backfilled into events_latest on first create")
	assert.EqualValues(t, 2, got[evEngineBlock].count)
}
