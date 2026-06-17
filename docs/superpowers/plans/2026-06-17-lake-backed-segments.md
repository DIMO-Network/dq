# Lake-backed Segments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve segment/trip detection (`segments`, `dailyActivity`) from `lake.signals` so `QUERY_BACKEND=ducklake` no longer routes segments to ClickHouse.

**Architecture:** Extract a backend-agnostic `SignalSource` seam: the 6 detector algorithms (pure Go) move to a new `internal/segments` package and take a `SignalSource` interface instead of a `clickhouse.Conn`. ClickHouse keeps its behavior via a thin `SignalSource` adapter; a new DuckDB `SignalSource` reads `lake.signals` (ignition state-changes computed on the fly with `LAG()`). Parity is by construction — identical algorithm code, only the data fetch differs — and validated by shadow.

**Tech Stack:** Go (CGO), DuckDB via `github.com/duckdb/duckdb-go/v2`, DuckLake catalog, ClickHouse (`clickhouse-go/v2`), model-garage `vss`, GraphQL (gqlgen), Prometheus.

**Spec:** `docs/superpowers/specs/2026-06-17-ch-deprecation-dq-segments-fetch-design.md`

**Branch:** `feat/lake-segments-fetch` (already created off `feat/duckdb-parse-on-read`).

---

## File Structure

**New package `internal/segments`** (algorithms + seam, backend-agnostic):
- `internal/segments/source.go` — `SignalSource` interface + the shared data types (`ActiveWindow`, `LevelSample`, `StateChange`).
- `internal/segments/detector.go` — `SegmentDetector` interface, `resolvedConfig`, `newSegment`, `resolveBaseConfig` (moved from `ch/detector.go`).
- `internal/segments/util.go` — merge/clip helpers (moved from `ch/segments_utils.go`, minus the CH SQL fetchers).
- `internal/segments/{frequency,changepoint,idling,refuel,recharge,ignition}.go` — the 6 detectors (moved verbatim, `conn` → `SignalSource`).
- `internal/segments/registry.go` — `NewDetector(src SignalSource, mechanism) (SegmentDetector, error)` (moved from `ch/segments.go:newDetector`).

**ClickHouse adapter** (keeps CH behavior):
- `internal/service/ch/signalsource.go` — `chSignalSource` implementing `segments.SignalSource` using the existing CH SQL.
- `internal/service/ch/segments.go` — `Service.GetSegments` now builds a `chSignalSource` and calls `segments.NewDetector`.

**DuckDB adapter** (new lake backend):
- `internal/service/duck/segments_source.go` — `LakeSignalSource` implementing `segments.SignalSource` over `lake.signals`.
- `internal/service/duck/lake_segments.go` — `LakeSegments` type wrapping a `LakeSignalSource`, exposing `GetSegments` (satisfies `repositories.SegmentsBackend`).

**Wiring + shadow:**
- `internal/app/backend.go` — ducklake case composes `LakeSegments` instead of `chService`.
- `internal/repositories/shadow.go` — `ShadowBackend.GetSegments` shadows lake-vs-CH instead of pass-through.

**Tests:**
- `internal/segments/parity_test.go` — fake `SignalSource` drives all 6 detectors (algorithm regression, no DB).
- `internal/service/duck/segments_source_test.go` — `LakeSignalSource` SQL over a file-catalog `lake.signals`.
- `tests/segments_lake_parity_test.go` — e2e: same signals into CH + lake, assert identical `[]Segment` per mechanism.

---

## Phase 1 — Extract the `internal/segments` package (no behavior change)

### Task 1: Create the `SignalSource` interface and shared types

**Files:**
- Create: `internal/segments/source.go`

- [ ] **Step 1: Write the interface and types**

```go
// Package segments holds backend-agnostic vehicle usage-segment detection.
// Detectors contain only algorithm logic; all data access goes through a
// SignalSource, implemented once per storage backend (ClickHouse, DuckLake).
package segments

import (
	"context"
	"time"
)

// ActiveWindow is a fixed-width time window with its signal activity counts.
// Produced by SignalSource.WindowedSignalCounts; consumed by the frequency
// and change-point detectors.
type ActiveWindow struct {
	WindowStart         time.Time
	WindowEnd           time.Time
	SignalCount         uint64
	DistinctSignalCount uint64
}

// LevelSample is a timestamped numeric reading (RPM, fuel %, SoC %, odometer).
type LevelSample struct {
	TS    time.Time
	Value float64
}

// StateChange is a transition of a discrete signal (e.g. isIgnitionOn),
// carrying the new and previous state values.
type StateChange struct {
	TS        time.Time
	NewState  float64
	PrevState float64
}

// SignalSource is the data-access seam for segment detection. One
// implementation per backend; detectors are written against this interface.
type SignalSource interface {
	// WindowedSignalCounts returns per-window signal counts in [from, to),
	// bucketed to windowSizeSeconds, keeping only windows meeting the count
	// and distinct-count thresholds, ordered by window start.
	WindowedSignalCounts(ctx context.Context, subject string, from, to time.Time, windowSizeSeconds, signalThreshold, distinctSignalThreshold int) ([]ActiveWindow, error)

	// LevelSamples returns timestamped numeric samples for one signal name in
	// [from, to), ordered by timestamp ascending.
	LevelSamples(ctx context.Context, subject, name string, from, to time.Time) ([]LevelSample, error)

	// IgnitionStateChanges returns isIgnitionOn transitions in [from, to),
	// plus the last transition before from (seed for the open state), ordered
	// by timestamp ascending. Lookback for the seed is capped at 30 days.
	IgnitionStateChanges(ctx context.Context, subject string, from, to time.Time) ([]StateChange, error)
}
```

- [ ] **Step 2: Build**

Run: `go build ./internal/segments/`
Expected: PASS (package compiles with only the interface; unused is fine for a package).

- [ ] **Step 3: Commit**

```bash
git add internal/segments/source.go
git commit -m "feat(segments): SignalSource seam interface + shared types"
```

### Task 2: Move detector scaffolding (config, newSegment) into the package

**Files:**
- Create: `internal/segments/detector.go`
- Reference (do not delete yet): `internal/service/ch/detector.go`

- [ ] **Step 1: Create `internal/segments/detector.go`** — copy the bodies of `newSegment`, `resolvedConfig`, `resolveBaseConfig`, the two `default*Seconds` consts, and the `SegmentDetector` interface from `ch/detector.go` verbatim. Change `package ch` → `package segments`. Keep the `model` import (`github.com/DIMO-Network/dq/internal/graph/model`).

- [ ] **Step 2: Build**

Run: `go build ./internal/segments/`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/segments/detector.go
git commit -m "feat(segments): move detector config/scaffolding into package"
```

### Task 3: Move merge/clip utilities (drop CH SQL fetchers)

**Files:**
- Create: `internal/segments/util.go`
- Reference: `internal/service/ch/segments_utils.go`

- [ ] **Step 1: Create `internal/segments/util.go`** — copy from `ch/segments_utils.go` these symbols **verbatim**, changing `package ch` → `package segments`: `levelSample`→ replace with the package's `LevelSample` (rename field refs `.ts`→`.TS`, `.value`→`.Value`), `timeRange`, `sampleAtOrBefore`, `levelFirstLastInRange`, `mergeTimeRanges`, `clipTimeRange`, `timeRangesToSegments`, `timeNow`, `mergeWindowsIntoSegments`. Replace the local `ActiveWindow` struct with the package's `ActiveWindow` (Task 1). **Do NOT copy** `getWindowedSignalCounts` or `getLevelSamples` (those stay in `ch` as the adapter's SQL). Keep imports: `sort`, `time`, `model`.

- [ ] **Step 2: Adjust signatures that used `levelSample`** — `sampleAtOrBefore` and `levelFirstLastInRange` take `[]LevelSample`; update field accesses to `.TS`/`.Value`.

- [ ] **Step 3: Build**

Run: `go build ./internal/segments/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/segments/util.go
git commit -m "feat(segments): move merge/clip utilities into package"
```

### Task 4: Move the 6 detectors, retargeted to `SignalSource`

**Files:**
- Create: `internal/segments/{frequency,changepoint,idling,refuel,recharge,ignition}.go`
- Reference: `internal/service/ch/{frequency,changepoint,idling,refuel,recharge,ignition}_detector.go`

For EACH detector, the mechanical transform is identical:

- [ ] **Step 1: Copy the detector file** to `internal/segments/<name>.go`, `package ch` → `package segments`.
- [ ] **Step 2: Replace the struct field and constructor.** `conn clickhouse.Conn` → `src SignalSource`; `New<Name>Detector(conn clickhouse.Conn)` → `New<Name>Detector(src SignalSource)` storing `src`. Drop the `clickhouse-go/v2` import.
- [ ] **Step 3: Replace data calls with interface calls:**
  - `getWindowedSignalCounts(ctx, d.conn, subject, from, to, win, sig, dist)` → `d.src.WindowedSignalCounts(ctx, subject, from, to, win, sig, dist)`
  - `getLevelSamples(ctx, d.conn, subject, name, from, to)` → `d.src.LevelSamples(ctx, subject, name, from, to)` (returns `[]LevelSample`; update `.ts`/`.value` → `.TS`/`.Value`)
  - **Ignition only:** replace the inline `getStateChangesQueryWithLookback` + `conn.Query` block (`ignition_detector.go:42-72`) with `changes, err := d.src.IgnitionStateChanges(ctx, subject, from, to)`. Delete `getStateChangesQueryWithLookback`. Keep the in-Go noise-filter + state-machine logic (`ignition_detector.go:120-184`) verbatim, reading from `[]StateChange` (`.TS`,`.NewState`,`.PrevState`).
- [ ] **Step 4: Build**

Run: `go build ./internal/segments/`
Expected: PASS after all 6 are moved (move them in one task; intermediate states won't build because `NewDetector` doesn't exist yet — that's Task 5).

- [ ] **Step 5: Commit**

```bash
git add internal/segments/frequency.go internal/segments/changepoint.go internal/segments/idling.go internal/segments/refuel.go internal/segments/recharge.go internal/segments/ignition.go
git commit -m "feat(segments): move 6 detectors, retarget conn -> SignalSource"
```

### Task 5: Add the detector registry

**Files:**
- Create: `internal/segments/registry.go`
- Reference: `internal/service/ch/segments.go:13-29` (`newDetector`)

- [ ] **Step 1: Write the registry**

```go
package segments

import (
	"fmt"

	"github.com/DIMO-Network/dq/internal/graph/model"
)

// NewDetector returns the SegmentDetector for a mechanism, bound to src.
func NewDetector(src SignalSource, mechanism model.DetectionMechanism) (SegmentDetector, error) {
	switch mechanism {
	case model.DetectionMechanismIgnitionDetection:
		return NewIgnitionDetector(src), nil
	case model.DetectionMechanismFrequencyAnalysis:
		return NewFrequencyDetector(src), nil
	case model.DetectionMechanismChangePointDetection:
		return NewChangePointDetector(src), nil
	case model.DetectionMechanismIdling:
		return NewIdlingDetector(src), nil
	case model.DetectionMechanismRefuel:
		return NewRefuelDetector(src), nil
	case model.DetectionMechanismRecharge:
		return NewRechargeDetector(src), nil
	default:
		return nil, fmt.Errorf("unsupported detection mechanism: %s", mechanism)
	}
}
```

(Match the exact error string in `ch/segments.go`'s `newDetector` default arm; if it differs, use that string.)

- [ ] **Step 2: Build + vet**

Run: `go build ./internal/segments/ && go vet ./internal/segments/`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/segments/registry.go
git commit -m "feat(segments): mechanism->detector registry"
```

### Task 6: Algorithm parity test with a fake SignalSource

**Files:**
- Create: `internal/segments/parity_test.go`

- [ ] **Step 1: Write the test** — a `fakeSource` returning canned data, asserting each detector's output shape. This pins the moved algorithms.

```go
package segments

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/require"
)

type fakeSource struct {
	windows []ActiveWindow
	levels  map[string][]LevelSample
	changes []StateChange
}

func (f fakeSource) WindowedSignalCounts(_ context.Context, _ string, _, _ time.Time, _, _, _ int) ([]ActiveWindow, error) {
	return f.windows, nil
}
func (f fakeSource) LevelSamples(_ context.Context, _, name string, _, _ time.Time) ([]LevelSample, error) {
	return f.levels[name], nil
}
func (f fakeSource) IgnitionStateChanges(_ context.Context, _ string, _, _ time.Time) ([]StateChange, error) {
	return f.changes, nil
}

func TestFrequencyDetectorMergesAdjacentWindows(t *testing.T) {
	timeNow = func() time.Time { return time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC) }
	t.Cleanup(func() { timeNow = time.Now })
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	src := fakeSource{windows: []ActiveWindow{
		{WindowStart: base, WindowEnd: base.Add(time.Minute), SignalCount: 50, DistinctSignalCount: 5},
		{WindowStart: base.Add(time.Minute), WindowEnd: base.Add(2 * time.Minute), SignalCount: 50, DistinctSignalCount: 5},
		{WindowStart: base.Add(10 * time.Minute), WindowEnd: base.Add(11 * time.Minute), SignalCount: 50, DistinctSignalCount: 5},
	}}
	d := NewFrequencyDetector(src)
	got, err := d.DetectSegments(context.Background(), "did:1", base, base.Add(30*time.Minute), nil)
	require.NoError(t, err)
	require.Len(t, got, 1) // 5-min gap < default 300s maxGap → single merged segment
}
```

- [ ] **Step 2: Run**

Run: `go test ./internal/segments/ -run TestFrequencyDetector -v`
Expected: PASS.

- [ ] **Step 3: Add one focused test per remaining mechanism** (idling RPM run, refuel rise, recharge SoC trough/peak with stationary odometer, changepoint CUSUM threshold, ignition ON→OFF). Use the same `fakeSource` pattern; assert segment counts/bounds matching the existing `ch/*_detector_test.go` fixtures (port their input vectors).

- [ ] **Step 4: Run the package**

Run: `go test ./internal/segments/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/segments/parity_test.go
git commit -m "test(segments): algorithm parity tests via fake SignalSource"
```

---

## Phase 2 — ClickHouse adapter (keep CH behavior green)

### Task 7: Implement `chSignalSource`

**Files:**
- Create: `internal/service/ch/signalsource.go`
- Reference: `internal/service/ch/segments_utils.go` (`getWindowedSignalCounts`, `getLevelSamples`), `internal/service/ch/ignition_detector.go:79-116` (`getStateChangesQueryWithLookback`)

- [ ] **Step 1: Write the adapter** — keep `getWindowedSignalCounts`/`getLevelSamples` in `ch` (they already live in `segments_utils.go`; leave them). Move the ignition state-changes SQL here.

```go
package ch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/dq/internal/segments"
)

// chSignalSource implements segments.SignalSource over ClickHouse.
type chSignalSource struct {
	conn clickhouse.Conn
}

func (c chSignalSource) WindowedSignalCounts(ctx context.Context, subject string, from, to time.Time, win, sig, dist int) ([]segments.ActiveWindow, error) {
	ws, err := getWindowedSignalCounts(ctx, c.conn, subject, from, to, win, sig, dist)
	if err != nil {
		return nil, err
	}
	out := make([]segments.ActiveWindow, len(ws))
	for i, w := range ws {
		out[i] = segments.ActiveWindow(w) // identical field layout
	}
	return out, nil
}

func (c chSignalSource) LevelSamples(ctx context.Context, subject, name string, from, to time.Time) ([]segments.LevelSample, error) {
	ls, err := getLevelSamples(ctx, c.conn, subject, name, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]segments.LevelSample, len(ls))
	for i, s := range ls {
		out[i] = segments.LevelSample{TS: s.ts, Value: s.value}
	}
	return out, nil
}

func (c chSignalSource) IgnitionStateChanges(ctx context.Context, subject string, from, to time.Time) (_ []segments.StateChange, retErr error) {
	stmt, args := stateChangesQueryWithLookback(subject, from, to)
	rows, err := c.conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("querying ignition state changes: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	var out []segments.StateChange
	for rows.Next() {
		var sc segments.StateChange
		if err := rows.Scan(&sc.TS, &sc.NewState, &sc.PrevState); err != nil {
			return nil, fmt.Errorf("scanning state change: %w", err)
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}
```

> `segments.ActiveWindow(w)` requires field-identical structs (they are). If field order/types ever diverge, build a struct literal instead.

- [ ] **Step 2: Relocate the ignition SQL builder.** Cut `getStateChangesQueryWithLookback` out of `ignition_detector.go` (it's no longer used there — the detector moved to `segments`), rename to package-level `stateChangesQueryWithLookback(subject string, from, to time.Time) (string, []any)` in `signalsource.go`, keeping the SQL verbatim.

- [ ] **Step 3: Delete the now-orphaned CH detector files.** Remove `ch/{frequency,changepoint,idling,refuel,recharge,ignition}_detector.go` and `ch/detector.go` (moved to `segments`). Keep `segments_utils.go` (still hosts the two SQL fetchers + `ActiveWindow`/`levelSample` used by the adapter — **but** the merge/clip helpers there are now duplicated in `segments`). To avoid dead code: trim `ch/segments_utils.go` down to only `ActiveWindow`, `levelSample`, `getWindowedSignalCounts`, `getLevelSamples`, `dateTime64Micro` usage; delete the merge/clip/`timeRangesToSegments`/`mergeWindowsIntoSegments` helpers (now in `segments`).

- [ ] **Step 4: Build**

Run: `go build ./internal/service/ch/`
Expected: FAIL — `ch/segments.go` still calls the deleted `newDetector`. Fixed in Task 8.

- [ ] **Step 5: Commit (after Task 8 builds)** — combined with Task 8.

### Task 8: Rewire `ch.Service.GetSegments` through `segments.NewDetector`

**Files:**
- Modify: `internal/service/ch/segments.go`

- [ ] **Step 1: Replace `newDetector`** (delete the local one) and update `GetSegments` to:

```go
func (s *Service) GetSegments(ctx context.Context, subject string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig) ([]*model.Segment, error) {
	det, err := segments.NewDetector(chSignalSource{conn: s.conn}, mechanism)
	if err != nil {
		return nil, err
	}
	return det.DetectSegments(ctx, subject, from, to, config)
}
```

(Preserve any existing pre/post logic in the current `GetSegments` body — e.g. wrapping errors — keep it around this call. Confirm the field name for the CH connection on `Service`; adjust `s.conn` if different.)

- [ ] **Step 2: Add import** `"github.com/DIMO-Network/dq/internal/segments"` to `segments.go`; drop now-unused imports.

- [ ] **Step 3: Build the repo**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 4: Run CH segment + repository tests**

Run: `go test ./internal/service/ch/... ./internal/repositories/... -count=1`
Expected: PASS — CH behavior unchanged. (Existing detector tests moved with the code; if `ch/*_detector_test.go` reference deleted symbols, move those tests to `internal/segments/` adjusting `New*Detector(conn)` → `New*Detector(fakeSource{...})` or keep CH-integration ones pointed at `chSignalSource`.)

- [ ] **Step 5: Commit**

```bash
git add internal/service/ch/ internal/segments/
git commit -m "refactor(ch): serve segments via internal/segments + chSignalSource"
```

---

## Phase 3 — DuckDB `SignalSource` over `lake.signals`

### Task 9: Inspect `lake.signals` column names

**Files:** none (investigation; record findings in commit message of Task 10)

- [ ] **Step 1:** Read `internal/service/duck/lake_latest.go` and the materializer's `lake.signals` DDL to confirm exact column names (subject, name, timestamp, value_number, source) and that `isIgnitionOn` is stored as a numeric `value_number`. Note the timestamp column name and type.

Run: `grep -rn "lake.signals\|CREATE TABLE.*signals\|value_number\|timestamp" internal/service/duck/ internal/materializer/`
Expected: column names identified.

- [ ] **Step 2:** Confirm whether `lake.signals` is unique per `(subject,name,timestamp)`. If the materializer guarantees uniqueness, the dedup `QUALIFY` is a defensive no-op; if not, it is load-bearing. Record the answer.

### Task 10: Implement `LakeSignalSource`

**Files:**
- Create: `internal/service/duck/segments_source.go`

> Use the actual column names from Task 9. The code below assumes `subject`, `name`, `timestamp`, `value_number` on `lake.signals`. Adjust if Task 9 differs.

- [ ] **Step 1: Write the windowed-counts + level-samples methods**

```go
package duck

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/segments"
)

// LakeSignalSource implements segments.SignalSource over the DuckLake
// lake.signals table. Detection logic lives in internal/segments; this only
// fetches data, mirroring ch.chSignalSource one-for-one.
type LakeSignalSource struct {
	svc *Service
}

// NewLakeSignalSource builds a SignalSource bound to the catalog-attached svc.
func NewLakeSignalSource(svc *Service) *LakeSignalSource { return &LakeSignalSource{svc: svc} }

func (s *LakeSignalSource) WindowedSignalCounts(ctx context.Context, subject string, from, to time.Time, win, sig, dist int) ([]segments.ActiveWindow, error) {
	// date_bin buckets timestamps into win-second windows aligned to epoch.
	const q = `
SELECT window_start,
       window_start + to_seconds(?) AS window_end,
       count(*) AS signal_count,
       count(DISTINCT name) AS distinct_signal_count
FROM (
  SELECT time_bucket(to_seconds(?), timestamp) AS window_start, name
  FROM lake.signals
  WHERE subject = ? AND timestamp >= ? AND timestamp < ?
)
GROUP BY window_start
HAVING signal_count >= ? AND distinct_signal_count >= ?
ORDER BY window_start`
	rows, err := s.svc.DB().QueryContext(ctx, q, win, win, subject, from.UTC(), to.UTC(), sig, dist)
	if err != nil {
		return nil, fmt.Errorf("lake windowed signal counts: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []segments.ActiveWindow
	for rows.Next() {
		var w segments.ActiveWindow
		if err := rows.Scan(&w.WindowStart, &w.WindowEnd, &w.SignalCount, &w.DistinctSignalCount); err != nil {
			return nil, fmt.Errorf("scanning window: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *LakeSignalSource) LevelSamples(ctx context.Context, subject, name string, from, to time.Time) ([]segments.LevelSample, error) {
	const q = `
SELECT timestamp, value_number
FROM lake.signals
WHERE subject = ? AND name = ? AND timestamp >= ? AND timestamp < ?
ORDER BY timestamp`
	rows, err := s.svc.DB().QueryContext(ctx, q, subject, name, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("lake level samples: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []segments.LevelSample
	for rows.Next() {
		var ls segments.LevelSample
		if err := rows.Scan(&ls.TS, &ls.Value); err != nil {
			return nil, fmt.Errorf("scanning sample: %w", err)
		}
		out = append(out, ls)
	}
	return out, rows.Err()
}
```

> Confirm DuckDB window function: `time_bucket(INTERVAL, ts)` exists in DuckDB; `to_seconds(n)` builds an INTERVAL. If `time_bucket` is unavailable in the embedded build, use `date_trunc`/arithmetic. Validate in Task 12. If `lake.signals` is NOT unique per `(subject,name,timestamp)` (Task 9), wrap the inner scan with `QUALIFY ROW_NUMBER() OVER (PARTITION BY subject,name,timestamp ORDER BY <tiebreak>) = 1`.

- [ ] **Step 2: Write the ignition state-changes method (LAG over isIgnitionOn)**

```go
const ignitionLookbackDays = 30

func (s *LakeSignalSource) IgnitionStateChanges(ctx context.Context, subject string, from, to time.Time) ([]segments.StateChange, error) {
	lookback := from.AddDate(0, 0, -ignitionLookbackDays)
	const q = `
WITH s AS (
  SELECT timestamp,
         value_number AS new_state,
         lag(value_number) OVER (PARTITION BY subject ORDER BY timestamp) AS prev_state
  FROM lake.signals
  WHERE subject = ? AND name = 'isIgnitionOn'
    AND timestamp >= ? AND timestamp < ?
),
changes AS (
  SELECT timestamp, new_state, coalesce(prev_state, new_state) AS prev_state
  FROM s
  WHERE prev_state IS NULL OR new_state != prev_state
)
-- keep the last change at/before from (open-state seed) + all changes in [from,to)
SELECT timestamp, new_state, prev_state FROM changes
WHERE timestamp >= ?
UNION ALL
SELECT timestamp, new_state, prev_state FROM changes
WHERE timestamp < ?
ORDER BY timestamp`
	// Note: the seed row is the latest change < from; refine the second arm to
	// "ORDER BY timestamp DESC LIMIT 1" semantics if the CH version seeds a
	// single pre-from row (match ch/ignition_detector.go behavior exactly).
	rows, err := s.svc.DB().QueryContext(ctx, q, subject, lookback.UTC(), to.UTC(), from.UTC(), from.UTC())
	if err != nil {
		return nil, fmt.Errorf("lake ignition state changes: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []segments.StateChange
	for rows.Next() {
		var sc segments.StateChange
		if err := rows.Scan(&sc.TS, &sc.NewState, &sc.PrevState); err != nil {
			return nil, fmt.Errorf("scanning state change: %w", err)
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}
```

> **Match CH seed semantics exactly.** `ch/ignition_detector.go:79-116` seeds exactly one transition before `from` (the `ORDER BY timestamp DESC LIMIT 1` arm). Replace the second `UNION ALL` arm with that single-row seed so the open-state machine starts identically. This is the highest-risk parity detail — Task 13's e2e parity test is the gate.

- [ ] **Step 3: Compile-time assertion**

Add to `segments_source.go`:
```go
var _ segments.SignalSource = (*LakeSignalSource)(nil)
```

- [ ] **Step 4: Build**

Run: `go build ./internal/service/duck/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/service/duck/segments_source.go
git commit -m "feat(duck): LakeSignalSource over lake.signals (ignition via LAG)"
```

### Task 11: `LakeSegments` SegmentsBackend

**Files:**
- Create: `internal/service/duck/lake_segments.go`

- [ ] **Step 1: Write it**

```go
package duck

import (
	"context"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/segments"
)

// LakeSegments serves segment detection from lake.signals. It satisfies
// repositories.SegmentsBackend.
type LakeSegments struct {
	src *LakeSignalSource
}

// NewLakeSegments builds a SegmentsBackend over the catalog-attached svc.
func NewLakeSegments(svc *Service) *LakeSegments {
	return &LakeSegments{src: NewLakeSignalSource(svc)}
}

// GetSegments dispatches to the mechanism's detector over the lake source.
func (l *LakeSegments) GetSegments(ctx context.Context, subject string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig) ([]*model.Segment, error) {
	det, err := segments.NewDetector(l.src, mechanism)
	if err != nil {
		return nil, err
	}
	return det.DetectSegments(ctx, subject, from, to, config)
}
```

- [ ] **Step 2: Build**

Run: `go build ./internal/service/duck/`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/service/duck/lake_segments.go
git commit -m "feat(duck): LakeSegments SegmentsBackend"
```

### Task 12: `LakeSignalSource` SQL test on a file catalog

**Files:**
- Create: `internal/service/duck/segments_source_test.go`

- [ ] **Step 1: Write a test** that boots a DuckLake file catalog, inserts a handful of `lake.signals` rows (windows of `isIgnitionOn` 0/1, RPM, fuel), and asserts each `LakeSignalSource` method returns the expected rows. Reuse the existing file-catalog test harness (see `tests/ducklake_e2e_test.go` / `internal/service/duck/duck_test.go` for the setup helper). Assert:
  - `WindowedSignalCounts` buckets correctly and applies thresholds.
  - `LevelSamples` returns sorted samples for a name.
  - `IgnitionStateChanges` returns transitions + the single pre-`from` seed row.

- [ ] **Step 2: Run**

Run: `go test ./internal/service/duck/ -run TestLakeSignalSource -v`
Expected: PASS. (This is where `time_bucket`/`to_seconds`/`lag` SQL gets validated against the real embedded DuckDB; fix dialect here if it errors.)

- [ ] **Step 3: Commit**

```bash
git add internal/service/duck/segments_source_test.go
git commit -m "test(duck): LakeSignalSource SQL over file-catalog lake.signals"
```

---

## Phase 4 — Wire ducklake backend + shadow

### Task 13: Compose `LakeSegments` in the ducklake backend

**Files:**
- Modify: `internal/app/backend.go:77-81`

- [ ] **Step 1: Change the ducklake case** from segments-on-CH to segments-on-lake:

```go
	case config.QueryBackendDuckLake:
		// Reads and segment detection both come from the DuckLake catalog.
		return repositories.ComposeBackend(duck.NewLakeQueries(duckSvc), duck.NewLakeSegments(duckSvc)), closeDuck(duckSvc, logger), nil
```

Update the comment that said "segment detection stays on ClickHouse."

- [ ] **Step 2: Build**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 3: e2e parity test (the real gate)**

**Files:** Create `tests/segments_lake_parity_test.go`

Write a test that, on a file catalog + a CH test instance (or a recorded fixture if CH isn't available in CI), feeds **identical** signal rows to `lake.signals` and to ClickHouse `signal`/`signal_state_changes`, then for each mechanism asserts `LakeSegments.GetSegments` and `ch.Service.GetSegments` return identical `[]*model.Segment`. Cover: ignition ON/OFF with a pre-`from` seed, frequency gap-merge, idling RPM run, refuel rise, recharge SoC trough/peak + stationary odometer, changepoint CUSUM. Gate CH side on the existing CH-availability env used by other `tests/` CH tests; always run the lake side against the golden vectors ported from `internal/segments/parity_test.go`.

Run: `go test ./tests/ -run TestSegmentsLakeParity -v`
Expected: PASS (lake output equals CH/golden output per mechanism).

- [ ] **Step 4: Commit**

```bash
git add internal/app/backend.go tests/segments_lake_parity_test.go
git commit -m "feat: ducklake serves segments from the lake; e2e parity test"
```

### Task 14: Shadow-compare segments (lake vs CH)

**Files:**
- Modify: `internal/repositories/shadow.go:231-235`

- [ ] **Step 1: Make `ShadowBackend` hold a segments secondary.** The shadow backend currently only shadows the `Backend` surface; segments passes through. Add a secondary segments source. Change `NewShadowBackend` to accept a `SegmentsBackend` for the secondary:

```go
type ShadowBackend struct {
	primary          CHService
	secondary        Backend
	secondarySegment SegmentsBackend
	log              zerolog.Logger
	sem              chan struct{}
	timeout          time.Duration
	wg               sync.WaitGroup
}

func NewShadowBackend(primary CHService, secondary Backend, secondarySegment SegmentsBackend, log zerolog.Logger) *ShadowBackend {
	return &ShadowBackend{
		primary:          primary,
		secondary:        secondary,
		secondarySegment: secondarySegment,
		log:              log.With().Str("component", "shadow").Logger(),
		sem:              make(chan struct{}, defaultShadowMaxConcurrency),
		timeout:          defaultShadowTimeout,
	}
}
```

- [ ] **Step 2: Replace `GetSegments` pass-through with a shadow**

```go
func (s *ShadowBackend) GetSegments(ctx context.Context, subject string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig) ([]*model.Segment, error) {
	res, err := s.primary.GetSegments(ctx, subject, from, to, mechanism, config)
	args := fmt.Sprintf("%s mechanism=%s", subjectRange(subject, from, to), mechanism)
	s.shadow("GetSegments", args, res, err, func(ctx context.Context) (any, error) {
		return s.secondarySegment.GetSegments(ctx, subject, from, to, mechanism, config)
	})
	return res, err
}
```

- [ ] **Step 3: Update the shadow wiring in `backend.go`**

```go
	case config.QueryBackendShadow:
		shadow := repositories.NewShadowBackend(chService, queries, duck.NewLakeSegments(duckSvc), logger)
```

> Shadow currently pairs CH with the bucket `duck.NewQueries`. Segments only exist on the lake source, so pass `duck.NewLakeSegments(duckSvc)` as the segments secondary. This requires the catalog to be attached on `duckSvc` in shadow mode — set `cfg.DuckLakeEnabled` / catalog DSN when shadow is selected, or guard `secondarySegment` as nil-safe (skip the shadow when nil). Add the nil guard:

```go
	s.shadow("GetSegments", args, res, err, func(ctx context.Context) (any, error) {
		if s.secondarySegment == nil {
			return res, nil // nothing to compare; treat as match
		}
		return s.secondarySegment.GetSegments(ctx, subject, from, to, mechanism, config)
	})
```

- [ ] **Step 4: Build + test**

Run: `go build ./... && go test ./internal/repositories/... -count=1`
Expected: PASS. Update `shadow_test.go` constructor calls to the new 4-arg `NewShadowBackend` (pass a fake or nil segments secondary).

- [ ] **Step 5: Commit**

```bash
git add internal/repositories/shadow.go internal/app/backend.go internal/repositories/shadow_test.go
git commit -m "feat(shadow): compare segment detection lake vs ClickHouse"
```

---

## Phase 5 — Verify

### Task 15: Full build + test + lint

- [ ] **Step 1: Build, test, lint**

Run:
```bash
go build ./... && go test ./internal/... ./tests/ -count=1 && golangci-lint run ./...
```
Expected: PASS, `0 issues`.

- [ ] **Step 2: Confirm ducklake mode needs no CH for segments** — grep that `backend.go`'s ducklake arm references no `chService`:

Run: `grep -n "QueryBackendDuckLake" -A3 internal/app/backend.go`
Expected: composes `duck.NewLakeSegments`, no `chService`.

- [ ] **Step 3: Commit any lint fixes**

```bash
git add -A && git commit -m "chore(segments): lint clean"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** Component A (segments on lake) → Tasks 1–13; ignition via LAG → Task 10; exact parity → Tasks 6, 13; shadow validation → Task 14. ✓
- **Placeholders:** none — verbatim moves are exact mechanical transforms with named symbols; SQL dialect risks are flagged with a validation task (12) rather than left vague.
- **Type consistency:** `SignalSource` methods, `ActiveWindow`/`LevelSample`/`StateChange`, `NewDetector`, `LakeSegments.GetSegments` signature matches `repositories.SegmentsBackend`. ✓
- **Open confirmations folded into tasks:** lake.signals column names + uniqueness (Task 9), DuckDB window-fn dialect (Task 12), ignition seed semantics (Tasks 10/13).
