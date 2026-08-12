// raw_types.go maintains lake.raw_types_latest, the per-(subject,type) summary
// rollup of din's lake.raw_events that serves availableCloudEventTypes (dq#40).
//
// Unlike signals_latest/events_latest there is NO dirty-subject machinery here,
// deliberately. Those rollups recompute touched subjects cheaply because their
// base tables are bucket-partitioned (`WHERE subject_bucket = N AND subject IN
// (…)` prunes). raw_events is partitioned by (type, day) and sorted by subject
// IN-file only — a per-subject recompute pays the same ~all-files scan as a
// full rebuild, per chunk. So the full rebuild is both the cheapest and the
// simplest refresh: one pass over raw_events (~4.5 s in production), one
// transaction, however many subjects changed. It also self-heals retention
// drift (when din expires old raw events, count/first_seen correct themselves
// at the next rebuild), which dirty tracking would not.
package materializer

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/service/duck"
)

// defaultRawTypesInterval is how often the raw_types_latest full rebuild runs
// when MATERIALIZER_RAW_TYPES_INTERVAL is unset. ~96 whole-table raw_events
// scans/day, off the read path and batched; ≤15 min staleness for "which event
// types does this subject have" is acceptable (dq#40).
const defaultRawTypesInterval = 15 * time.Minute

// rawTypesLatestColumns names lake.raw_types_latest's columns in CREATE order so
// the rollup INSERT binds by name, not position (see signalsLatestColumns for why
// a positional bind would silently corrupt on a reordered SELECT).
const rawTypesLatestColumns = ` (subject, type, count, first_seen, last_seen) `

// RecomputeRawTypesRollup rebuilds lake.raw_types_latest in full from
// lake.raw_events, DELETE + INSERT in one transaction so readers never observe
// a partial table. The SELECT is duck.RawTypesRollupSelect — the read path's own
// voiding + redelivery-dedup predicates — so the rollup cannot drift from the
// scan it replaces. The first rebuild after the table is created doubles as the
// backfill (the rebuild is the whole-table pass either way).
//
// Single-writer: called only on the decode-loop goroutine (Runner.Run) and
// interval-gated there; a transient failure is retried at the next interval.
func (m *DuckLakeMaterializer) RecomputeRawTypesRollup(ctx context.Context) error {
	// dq can boot before din has created lake.raw_events against a fresh catalog
	// (the RunOnce S8 guard). Nothing to roll up yet; try again next interval.
	exists, err := m.tableExists(ctx, "lake", "raw_events")
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	start := time.Now()
	defer func() { rawTypesRefreshSeconds.Set(time.Since(start).Seconds()) }()

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.raw_types_latest"); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO lake.raw_types_latest"+rawTypesLatestColumns+duck.RawTypesRollupSelect()); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return tx.Commit()
}
