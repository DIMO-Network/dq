// ducklake_raw_types_rollup_test.go validates the raw_types_latest rollup
// (dq#40): RecomputeRawTypesRollup materializes exactly what the live
// type-summary scan computes — voiding and redelivery dedup included — so
// GetCloudEventTypeSummariesAdvanced returns identical values from either path,
// and the full rebuild self-heals after raw_events rows disappear (retention).
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rawTyped builds a raw cloudevent of an arbitrary type (the rollup is
// type-agnostic; nothing here is decoded).
func rawTyped(id, subject, ceType string, ts time.Time) cloudevent.StoredEvent {
	return cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        ceType,
			Subject:     subject,
			Source:      "0xConnLicense",
			Producer:    subject,
			ID:          id,
			Time:        ts,
			DataVersion: "default/v1.0",
		},
		Data: []byte(`{}`),
	}}
}

func typeSummaries(t *testing.T, ctx context.Context, lsvc *duck.LakeEventService, opts *grpc.AdvancedSearchOptions) []eventrepo.CloudEventTypeSummary {
	t.Helper()
	sums, err := lsvc.GetCloudEventTypeSummariesAdvanced(ctx, opts)
	require.NoError(t, err)
	return sums
}

func TestDuckLake_RawTypesRollupParity(t *testing.T) {
	ctx := context.Background()
	svc := newLakeService(t, t.TempDir())
	db := svc.DB()
	subjA := fmt.Sprintf("did:erc721:137:%s:21", vehicleNFT.Hex())
	subjB := fmt.Sprintf("did:erc721:137:%s:22", vehicleNFT.Hex())
	sec := time.Now().UTC().AddDate(0, 0, -3).Truncate(time.Second)

	// subjA: two dimo.status events, plus a REDELIVERED duplicate of the first
	// (same id, same second, different sub-second) — the read dedup key
	// (subject, second, type, source, id) must collapse it to one.
	seedRawEvent(t, svc, rawTyped("rt-1", subjA, "dimo.status", sec.Add(100*time.Millisecond)))
	seedRawEvent(t, svc, rawTyped("rt-1", subjA, "dimo.status", sec.Add(900*time.Millisecond)))
	seedRawEvent(t, svc, rawTyped("rt-2", subjA, "dimo.status", sec.Add(time.Hour)))
	// subjA: an attestation later VOIDED by a tombstone — neither the event nor
	// the tombstone row may appear in any summary.
	seedRawEvent(t, svc, rawTyped("rt-att", subjA, "dimo.attestation", sec.Add(2*time.Hour)))
	_, err := db.ExecContext(ctx,
		`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data, voids_id)
		 VALUES (?, ?, 'dimo.attestation', 'rt-void', '0xConnLicense', ?, '', 'default/v1.0', '{}', '{}', 'rt-att')`,
		subjA, sec.Add(3*time.Hour), subjA)
	require.NoError(t, err)
	// subjB: one event of its own.
	seedRawEvent(t, svc, rawTyped("rt-3", subjB, "dimo.status", sec.Add(4*time.Hour)))

	lsvc := duck.NewLakeEventService(svc, nil, nil, "")

	// Before the materializer exists there is no rollup table: this is the live
	// scan, the oracle the rollup must reproduce.
	fromScan := typeSummaries(t, ctx, lsvc, nil)
	require.Len(t, fromScan, 1, "voided attestation and its tombstone must not surface a type")
	assert.Equal(t, "dimo.status", fromScan[0].Type)
	assert.Equal(t, uint64(3), fromScan[0].Count, "duplicate redelivery collapsed: rt-1 once, rt-2, rt-3")

	// ensureSchema creates lake.raw_types_latest; the first rebuild backfills it.
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, mat.RecomputeRawTypesRollup(ctx))

	// One rollup row per surviving (subject, type).
	var rollupRows int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM lake.raw_types_latest").Scan(&rollupRows))
	assert.Equal(t, 2, rollupRows, "subjA/status and subjB/status; the voided attestation must not be materialized")

	// Same query, now served from the rollup: identical output.
	fromRollup := typeSummaries(t, ctx, lsvc, nil)
	assert.Equal(t, fromScan, fromRollup, "rollup-served summaries must equal the live scan")

	// Prove the rollup (not the scan) is what answers now: corrupt it and watch
	// the served count follow.
	_, err = db.ExecContext(ctx, "UPDATE lake.raw_types_latest SET count = 999 WHERE subject = ?", subjA)
	require.NoError(t, err)
	got := typeSummaries(t, ctx, lsvc, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subjA}},
	})
	require.Len(t, got, 1)
	assert.Equal(t, uint64(999), got[0].Count, "an eligible filter must be answered by the rollup")

	// The next full rebuild repairs the corruption — the same property that
	// self-heals count/first_seen drift when din's retention expires raw rows.
	require.NoError(t, mat.RecomputeRawTypesRollup(ctx))
	got = typeSummaries(t, ctx, lsvc, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subjA}},
	})
	require.Len(t, got, 1)
	assert.Equal(t, uint64(2), got[0].Count, "rebuild restores the deduped truth for subjA")

	// Retention drift: expire subjA's oldest event from raw_events; the rebuild
	// must walk count and first_seen back.
	_, err = db.ExecContext(ctx, "DELETE FROM lake.raw_events WHERE id = 'rt-1'")
	require.NoError(t, err)
	require.NoError(t, mat.RecomputeRawTypesRollup(ctx))
	got = typeSummaries(t, ctx, lsvc, &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subjA}},
	})
	require.Len(t, got, 1)
	assert.Equal(t, uint64(1), got[0].Count)
	assert.True(t, got[0].FirstSeen.Equal(sec.Add(time.Hour)), "first_seen advances past the expired row")
}

// TestDuckLake_RawTypesRollup_MissingRawEvents pins the fresh-catalog guard: dq
// can boot before din has created lake.raw_events, and the rebuild must treat
// that as "nothing to roll up yet", not an error (the RunOnce S8 analogue).
func TestDuckLake_RawTypesRollup_MissingRawEvents(t *testing.T) {
	ctx := context.Background()
	svc, err := duck.NewService(duck.Config{
		DuckLakeEnabled: true,
		CatalogDSN:      t.TempDir() + "/catalog.ducklake",
		DataPath:        t.TempDir() + "/lakedata",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	mat, err := materializer.NewDuckLakeMaterializer(ctx, svc.DB(), zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, mat.RecomputeRawTypesRollup(ctx), "a missing lake.raw_events must be a quiet no-op")
}
