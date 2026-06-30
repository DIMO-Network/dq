package materializer

import (
	"strings"
	"testing"

	"github.com/DIMO-Network/cloudevent"
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

	// Re-boot against an existing catalog: no ALTER may be issued, and the
	// idempotent housekeeping (ingest_progress seed, consumer-floor table) still runs.
	all := map[string]bool{"signals": true, "events": true, "signals_latest": true}
	reboot := setupStatements(all, sigTmp, evTmp)
	for _, s := range reboot {
		if strings.Contains(s, "ALTER TABLE") {
			t.Fatalf("re-boot must issue no schema-changing ALTER; got: %s", s)
		}
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
