# Lake-backed Segments + Fetch — completing dq's query surface for ClickHouse deprecation

**Date:** 2026-06-17
**Repo:** dq
**Branch:** `feat/lake-segments-fetch` (stacked on `feat/duckdb-parse-on-read`)
**Status:** Design approved; spec for implementation planning

## Goal

Make `QUERY_BACKEND=ducklake` serve **every** dq query surface from the DuckLake lake, so the two
legacy ClickHouse-backed read APIs (telemetry-api, fetch-api) can be retired. Today the lake backend
covers only the signal/event telemetry surface; two surfaces still fall through to ClickHouse:

1. **Segments / trips** (`segments`, `dailyActivity`) — `SegmentsBackend.GetSegments`, CH-only.
2. **Fetch-API** (`latestCloudEvent`, `cloudEvents`, `availableCloudEventTypes`, gRPC `FetchService`) —
   `pkg/eventrepo` hardwired to a ClickHouse `cloud_event` index.

This branch builds both on the lake. After it lands and proves out in shadow, ducklake mode constructs
no ClickHouse client at all.

## Scope

**In scope (dq only):**
- Segment detection on `lake.signals`.
- Fetch on `lake.raw_events`.
- Shadow parity validation for both, lake-vs-CH.

**Out of scope (tracked as separate follow-ups — CH stays running for these):**
- `rewards-api` migrating off ClickHouse — the explicit decommission gate (separate repo/service,
  reads `dimo.signal` aggregates weekly).
- `vehicle-burn-processor` lake-delete equivalent (currently DELETEs CH rows on NFT burn).
- `din` deploy + DIS cutover (din raw-ingest already built in din PR #2; deployment is operational, not
  code, and separate).

Completing this branch is **necessary but not sufficient** for turning ClickHouse off; it removes
telemetry-api + fetch-api as CH readers, leaving rewards-api + burn-processor as the remaining gates.

## Decisions (locked)

| Decision | Choice |
|---|---|
| Scope | dq query surface only; cross-service blockers tracked separately |
| Ignition state-changes source | Computed on-the-fly via `LAG()` over `lake.signals` (no new maintained table) |
| Detector behavioral fidelity | Exact parity — port Go logic verbatim, swap only SQL dialect; validate via shadow |
| Read-side voiding (`voids_id`) | Close it now in the lake fetch path |
| Code structure | Extract a backend-agnostic seam; share logic; swap the data source |
| Branch | One PR, stacked on `feat/duckdb-parse-on-read` |

## Core architectural approach: extract the data-access seam

In both surfaces the *logic* is backend-agnostic and only the *SQL data-fetch* is ClickHouse-specific.
Rather than duplicate logic (parity-drift risk), extract a thin seam per surface and provide a DuckDB
implementation alongside the existing ClickHouse one.

### Rejected alternatives
- **Duplicate the 6 detectors into a duck package** — subtle algorithm code (CUSUM, binary-search
  trough/peak, rolling-average smoothing, ignition state machine) would drift from CH over time.
  Exact parity is a requirement; shared code gives it by construction.
- **Build a CH-shaped `cloud_event` index table inside the lake** for fetch — an extra decoded table to
  maintain. Unnecessary: `lake.raw_events` is partitioned by `(type, day(time))`, sorted by
  `(subject, time)`, with subject bloom filters, so the filtered scans fetch needs are already pruned.

## Component A — Segments on the lake

### Current structure (ClickHouse)
- `internal/service/ch/detector.go` — `SegmentDetector` interface (`DetectSegments`).
- Per-mechanism detectors: `ignition_detector.go`, `frequency_detector.go`, `changepoint_detector.go`,
  `idling_detector.go`, `refuel_detector.go`, `recharge_detector.go`.
- Data access via `segments_utils.go` helpers: windowed signal counts, level samples, and the ignition
  state-change query (the only one reading the CH `signal_state_changes` table).
- 5 of 6 mechanisms read the `signal` table as sorted time-series; all detection math is pure Go.

### Plan
1. **New package `internal/segments`** holding the 6 detector algorithms (moved verbatim from `ch/`) and
   a data-access interface:
   ```go
   type SignalSource interface {
       // FREQUENCY_ANALYSIS, CHANGE_POINT_DETECTION
       WindowedSignalCounts(ctx, subject string, from, to time.Time, windowSec int) ([]WindowCount, error)
       // IDLING, REFUEL, RECHARGE
       LevelSamples(ctx, subject, name string, from, to time.Time) ([]Sample, error)
       // IGNITION_DETECTION
       IgnitionStateChanges(ctx, subject string, from, to time.Time) ([]StateChange, error)
   }
   ```
   Detector functions take a `SignalSource` and a `SegmentConfig`; they contain no SQL.
2. **`ch.Service` provides a CH `SignalSource`** (its existing queries, lightly refactored behind the
   interface). Behavior unchanged.
3. **New duck `SignalSource`** over `lake.signals`, with dialect swaps:
   - `toStartOfInterval(ts, INTERVAL n second)` → `date_trunc`/`date_bin` on `n`-second windows.
   - `uniq(name)` → `COUNT(DISTINCT name)`.
   - `FINAL` dedup → `QUALIFY ROW_NUMBER() OVER (PARTITION BY subject, name, time ORDER BY <tiebreak>) = 1`
     (confirm whether `lake.signals` is already unique per `(subject,name,time)` from the materializer; if
     so, dedup is a no-op guard).
   - **Ignition state-changes** computed on the fly:
     ```sql
     WITH s AS (
       SELECT subject, time, value_number AS new_state,
              LAG(value_number) OVER (PARTITION BY subject ORDER BY time) AS prev_state
       FROM lake.signals
       WHERE subject = ? AND name = 'isIgnitionOn'
         AND time >= ?  -- from minus 30d lookback cap
         AND time <  ?  -- to
     )
     SELECT time, new_state, prev_state FROM s
     WHERE prev_state IS NULL OR new_state != prev_state
     ORDER BY time
     ```
     Preserve the existing 30-day lookback cap and the "last state change before `from`" seed row.
4. **Wire:** `repositories.ComposeBackend(duck.NewLakeQueries(…), lakeSegments)` where `lakeSegments`
   is `segments` detectors + duck `SignalSource`. In ducklake mode, segments no longer touch `chService`.

### Notes
- Window sizes stay hardcoded (frequency/CUSUM 60s, refuel 5min) as today.
- Microsecond timestamp precision preserved (`TIMESTAMPTZ`/`DateTime64(6)` equivalence).
- Repository-layer post-filters (e.g. idling speed filter, default signal sets, segment limit) are
  backend-agnostic and unchanged.

## Component B — Fetch on the lake

### Current structure (ClickHouse)
- `pkg/eventrepo/eventrepo.go` — concrete `*Service` over a `clickhouse.Conn`, querying the
  `cloud_event` ReplacingMergeTree index (`chindexer`), then fetching payloads from S3/parquet.
- Used directly by `internal/graph/cloud_events.resolvers.go` (`r.EventService`) and
  `internal/fetch` gRPC server (`rpc.NewServer(buckets, eventService)`).
- Constructed unconditionally in `internal/app/app.go` with a CH connection.

### Plan
1. **Extract an `EventService` interface** covering the methods resolvers + gRPC use:
   `GetLatestIndexAdvanced`, `ListIndexesAdvanced`, `GetCloudEventTypeSummariesAdvanced`,
   `ListCloudEventsFromIndexes`, `GetCloudEventFromIndex`, `PresignBlobURL` (plus the composite
   `ListCloudEventsAdvanced` / `GetLatestCloudEventAdvanced`). The existing CH `*Service` satisfies it
   unchanged.
2. **New lake-backed `EventService`** over `lake.raw_events`, reusing `internal/service/duck/raw.go`'s
   filter/dedup/scan logic but with `FROM lake.raw_events` instead of `read_parquet(<hive globs>)`.
   `lake.raw_events` carries every needed column inline: metadata + `data` + `data_base64` +
   `data_index_key` + `voids_id`.
   Close the 4 gaps `raw.go` has vs eventrepo:
   - **Type summaries:** `SELECT type, count(*), min(time), max(time) FROM lake.raw_events WHERE subject=? [filters] GROUP BY type ORDER BY type`.
   - **Filters:** add `extras`/`tags` (JSON extract on `extras` — `hasAny`-equivalent over the tags
     array) and `data_version`.
   - **Blob presign:** when payload is not inline (large payload referenced by `data_index_key` blob
     key), presign via the existing S3 presigner, mirroring eventrepo's `BlobKeyPrefix` routing.
   - **Read-side voiding:** exclude any event whose `id` is referenced by a tombstone's `voids_id`:
     ```sql
     ... FROM lake.raw_events e
     WHERE <filters>
       AND NOT EXISTS (
         SELECT 1 FROM lake.raw_events t
         WHERE t.voids_id = e.id AND t.subject = e.subject
       )
     ```
     Confirm tombstone semantics (a `dimo.tombstone` event carries `voids_id` = the voided event's id);
     tombstone rows themselves are also excluded from fetch results.
3. **Wire:** `internal/app/app.go` selects the lake-backed `EventService` when `QUERY_BACKEND=ducklake`
   (and the catalog DSN is set); otherwise the CH `eventrepo` (today's behavior). No CH connection is
   constructed in ducklake mode.

### Notes
- Legacy non-parquet S3 JSON fallback is **not** carried into the lake path (the lake is the
  forward-only world; backfilled DIS bundles are parquet registered into the catalog).
- Dedup by the cloudevent header uniqueness key, as `raw.go` already does (at-least-once duplicates).

## Component C — Validation & cutover

- **Shadow parity.** Extend the existing shadow mechanism (today: signals, `dq_shadow_mismatch_total`)
  to also compare lake-vs-CH **segment** outputs and **fetch index** lists, with new metric labels per
  surface. Serve from CH, mirror to lake, log/measure divergence. Run in prod shadow until clean.
- **Cutover.** Once parity holds, `QUERY_BACKEND=ducklake` serves signals + events + segments + fetch
  entirely from the lake; `chService`/`chConn` are no longer constructed. telemetry-api + fetch-api
  become retirable (downstream operational step, not this branch).

## Testing

- **Segment parity tests:** feed identical synthetic signals to the CH `SignalSource` and the duck
  `SignalSource`; assert byte-identical `[]Segment` from each detector across all 6 mechanisms and edge
  cases (boundary lookback, ongoing segments, gap splitting, trough/peak, CUSUM threshold).
- **Fetch parity tests:** identical raw cloudevents into CH index and `lake.raw_events`; assert
  list/latest/type-summary results match, including filter coverage (extras/tags/data_version) and
  voiding (voided event hidden, tombstone hidden).
- **Reuse harnesses:** `internal/graph/segments_graphql_test.go`, `tests/dis_parity_test.go`,
  file-catalog e2e (`tests/ducklake_e2e_test.go` shape).
- `go build ./...`, `go test ./internal/... ./tests/`, `golangci-lint run ./...` green; CGO.

## Risks / open confirmations (resolve during implementation)

1. **`lake.signals` uniqueness** — confirm the materializer guarantees one row per
   `(subject, name, time)`; if so the `QUALIFY` dedup is a defensive no-op, else it's load-bearing and
   needs a deterministic tiebreak.
2. **`isIgnitionOn` storage** — confirm it lands in `lake.signals` as a numeric `value_number`
   (0/1) the `LAG` query expects.
3. **Tombstone semantics** — confirm `voids_id` direction and whether voiding is scoped per-subject and
   whether a tombstone can void across types.
4. **Tags/extras shape in `raw_events.extras`** — confirm JSON layout so the JSON-extract filters match
   CH's `JSONExtract(extras,'tags','Array(String)')` semantics.

## Out-of-scope follow-ups (record in project memory)

- rewards-api → lake (decommission gate).
- vehicle-burn-processor → lake-delete.
- din deploy + DIS cutover, then CH teardown.
