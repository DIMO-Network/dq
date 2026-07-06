package materializer

import (
	"strings"
	"testing"
	"time"

	"database/sql"
	"errors"
	"fmt"
	"github.com/DIMO-Network/cloudevent"
	_ "github.com/duckdb/duckdb-go/v2"
)

// setupStatements must apply the partition/sort layout on a fresh catalog but
// emit NO schema-changing ALTER once the tables exist. Re-ALTERing on every boot
// bumps DuckLake's schema_version, renames the inline-data tables, and crash-loops
// the materializer (see setupStatements' doc) — so the second "run" must be a
// no-op as far as ALTER goes. This is the unit-level proxy for "restart N times,
// zero new altered_table rows".
func TestSetupStatements_LayoutOnlyOnFirstCreation(t *testing.T) {
	const sigTmp, evTmp = "/tmp/sig.parquet", "/tmp/ev.parquet"

	// Fresh catalog: every decoded table is created AND laid out.
	fresh := strings.Join(setupStatements(map[string]bool{}, sigTmp, evTmp), "\n")
	for _, want := range []string{
		`ALTER TABLE lake.signals SET PARTITIONED BY (subject_bucket, day("timestamp"))`,
		`ALTER TABLE lake.signals SET SORTED BY (subject, "timestamp")`,
		`ALTER TABLE lake.events SET PARTITIONED BY (subject_bucket, day("timestamp"))`,
		`ALTER TABLE lake.events SET SORTED BY (subject, "timestamp")`,
		`ALTER TABLE lake.signals_latest SET PARTITIONED BY (subject_bucket)`,
		"CREATE TABLE IF NOT EXISTS lake.signals ",
		"CREATE TABLE IF NOT EXISTS lake.events ",
		"CREATE TABLE IF NOT EXISTS lake.signals_latest ",
	} {
		if !strings.Contains(fresh, want) {
			t.Fatalf("fresh catalog setup missing %q\ngot:\n%s", want, fresh)
		}
	}

	// Re-boot against an existing catalog: no partition/sort re-layout may be
	// issued (re-running SET PARTITIONED BY / SORTED BY is a crash, not a
	// no-op), and the idempotent housekeeping (ingest_progress seed,
	// consumer-floor table) still runs. Idempotent column migrations
	// (ADD COLUMN IF NOT EXISTS — the H9 loc_ts backfill) are allowed: they
	// are how existing catalogs pick up new rollup columns.
	all := map[string]bool{"signals": true, "events": true, "signals_latest": true}
	reboot := setupStatements(all, sigTmp, evTmp)
	for _, s := range reboot {
		if strings.Contains(s, "SET PARTITIONED BY") || strings.Contains(s, "SET SORTED BY") {
			t.Fatalf("re-boot must issue no partition/sort re-layout; got: %s", s)
		}
		if strings.Contains(s, "ALTER TABLE") && !strings.Contains(s, "ADD COLUMN IF NOT EXISTS") {
			t.Fatalf("re-boot ALTERs must be idempotent ADD COLUMN IF NOT EXISTS migrations; got: %s", s)
		}
	}
	if !strings.Contains(strings.Join(reboot, "\n"), "ADD COLUMN IF NOT EXISTS loc_ts") {
		t.Fatal("re-boot must migrate pre-loc_ts rollups (H9)")
	}
	joined := strings.Join(reboot, "\n")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS lake.ingest_progress",
		"INSERT INTO lake.ingest_progress",
		"CREATE TABLE IF NOT EXISTS meta.din_consumer_progress",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("re-boot setup missing idempotent statement %q\ngot:\n%s", want, joined)
		}
	}

	// A partial catalog (only signals created) lays out exactly the missing tables.
	partial := strings.Join(setupStatements(map[string]bool{"signals": true}, sigTmp, evTmp), "\n")
	if strings.Contains(partial, "ALTER TABLE lake.signals ") {
		t.Fatalf("existing signals must not be re-ALTERed\ngot:\n%s", partial)
	}
	for _, want := range []string{
		"ALTER TABLE lake.events SET PARTITIONED BY",
		"ALTER TABLE lake.signals_latest SET PARTITIONED BY",
	} {
		if !strings.Contains(partial, want) {
			t.Fatalf("partial catalog must lay out missing table: %q\ngot:\n%s", want, partial)
		}
	}
}

// restoreNonColumnFieldsSafe must contain a panic from cloudevent.RestoreNonColumnFields
// (which does unchecked type assertions on the extras map) so a single poisoned
// raw_events row can't crash-loop the single-writer materializer.
func TestRestoreNonColumnFieldsSafe_ContainsPanic(t *testing.T) {
	// A non-string element in the tags extras is what the cloudevent lib asserts on.
	malformed := func() cloudevent.CloudEventHeader {
		return cloudevent.CloudEventHeader{Extras: map[string]any{"tags": []any{float64(42)}}}
	}

	// Confirm the raw lib call really does panic on this input — otherwise the guard
	// test is vacuous (skip rather than pass silently if the lib stops panicking).
	func() {
		defer func() {
			if recover() == nil {
				t.Skip("cloudevent.RestoreNonColumnFields no longer panics on a non-string tag; guard is moot")
			}
		}()
		h := malformed()
		cloudevent.RestoreNonColumnFields(&h)
	}()

	// The safe wrapper must NOT panic on the same input (a fatal panic here fails the test).
	h := malformed()
	restoreNonColumnFieldsSafe(&h)
}

// TestClampProbeFloor pins M5: a batch containing one epoch-timestamp row must
// not drag the dedup anti-join's probe window to 1970 (an O(entire-history)
// partition scan on every such batch). The floor clamps tsMin; an all-ancient
// batch yields an empty window, never an inverted one.
func TestClampProbeFloor(t *testing.T) {
	now := time.Now().UTC()
	epoch := time.Unix(0, 0).UTC()

	minT, maxT := clampProbeFloor(epoch, now)
	if minT.Before(now.Add(-dedupProbeFloor - time.Minute)) {
		t.Fatalf("tsMin not floored: %v", minT)
	}
	if !maxT.Equal(now) {
		t.Fatalf("tsMax changed: %v", maxT)
	}

	// All-ancient batch: window collapses to empty (min == max at the floor).
	minT, maxT = clampProbeFloor(epoch, epoch.Add(time.Hour))
	if maxT.Before(minT) {
		t.Fatalf("inverted window: [%v, %v]", minT, maxT)
	}

	// Recent batches pass through untouched.
	recentMin, recentMax := now.Add(-time.Hour), now
	minT, maxT = clampProbeFloor(recentMin, recentMax)
	if !minT.Equal(recentMin) || !maxT.Equal(recentMax) {
		t.Fatalf("recent window altered: [%v, %v]", minT, maxT)
	}

	// Empty batch sentinel (zero times) is preserved.
	minT, maxT = clampProbeFloor(time.Time{}, time.Time{})
	if !minT.IsZero() || !maxT.IsZero() {
		t.Fatal("zero sentinel altered")
	}
}

// TestRecoverPoisonedSession pins the e2e-discovered wedge classifier: only
// the aborted-session cascade (din maintenance FATALing an in-flight inline
// read) triggers the idle-pool recycle; ordinary errors do not.
func TestRecoverPoisonedSession(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := &DuckLakeMaterializer{db: db}
	if m.recoverPoisonedSession(nil) {
		t.Fatal("nil error must not recycle")
	}
	if m.recoverPoisonedSession(errors.New("some transient s3 blip")) {
		t.Fatal("unrelated error must not recycle")
	}
	poisoned := fmt.Errorf("reading raw_events delta: TransactionContext Error: Failed to get table insertion file list from DuckLake: Current transaction is aborted (please ROLLBACK)")
	if !m.recoverPoisonedSession(poisoned) {
		t.Fatal("aborted-session cascade must recycle the idle pool")
	}
}
