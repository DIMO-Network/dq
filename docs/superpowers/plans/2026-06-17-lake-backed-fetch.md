# Lake-backed Fetch (raw cloudevents) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the fetch-api surface (`latestCloudEvent`, `cloudEvents`, `availableCloudEventTypes`, gRPC `FetchService`) from `lake.raw_events` so `QUERY_BACKEND=ducklake` no longer needs a ClickHouse `cloud_event` index — and so no ClickHouse client is constructed in ducklake mode.

**Architecture:** Extract an `EventService` interface (the method-union the GraphQL resolver, the gRPC server, and the `internal/fetch` helper call on `*eventrepo.Service`) into `pkg/eventrepo`. The existing ClickHouse `*Service` satisfies it unchanged. Add a DuckLake-backed implementation in `internal/service/duck` that reads `lake.raw_events` directly (reusing `duck/raw.go`'s filter/dedup/scan), closing four gaps vs the CH path: type-summary aggregation, extras/tags/data_version filters, blob presign for large payloads, and read-side voiding (`voids_id` tombstones). The app selects the implementation by `QUERY_BACKEND`.

**Tech Stack:** Go (CGO), DuckDB via `github.com/duckdb/duckdb-go/v2`, DuckLake catalog, `github.com/DIMO-Network/cloudevent` (+ its grpc/parquet types), AWS S3 presign, GraphQL (gqlgen), gRPC.

**Spec:** `docs/superpowers/specs/2026-06-17-ch-deprecation-dq-segments-fetch-design.md`

**Branch:** `feat/lake-segments-fetch` (created off `feat/duckdb-parse-on-read`). Lands with the segments plan on one PR.

---

## File Structure

**Interface (in the package that owns the types):**
- `pkg/eventrepo/service.go` — the `EventService` interface + a compile-time assertion that `*Service` satisfies it.

**Consumers retargeted to the interface:**
- `internal/graph/resolver.go` — `EventService` field type `*eventrepo.Service` → `eventrepo.EventService`.
- `internal/fetch/rpc/rpc.go` — `Server.eventService` field + `NewServer` param → `eventrepo.EventService`.
- `internal/fetch/fetch.go` — the `evtSvc` params → `eventrepo.EventService`.

**Lake-backed implementation:**
- `internal/service/duck/lake_fetch.go` — `LakeEventService` over `lake.raw_events`, satisfying `eventrepo.EventService`.
- `internal/service/duck/raw.go` — extend `whereClause`/`RawFilter` with tags/data_version + voiding (shared with the lake path).

**Wiring + shadow:**
- `internal/app/app.go` — construct `LakeEventService` when `QUERY_BACKEND=ducklake`; CH `eventrepo` otherwise. Don't construct a CH connection in ducklake mode.
- `pkg/eventrepo/shadow.go` — `ShadowEventService` wrapping CH primary + lake secondary, comparing index results (selected when `QUERY_BACKEND=shadow`).

**Tests:**
- `internal/service/duck/lake_fetch_test.go` — `LakeEventService` over a file-catalog `lake.raw_events`.
- `tests/fetch_lake_parity_test.go` — identical cloudevents into CH index + `lake.raw_events`; assert list/latest/type-summary parity incl. filters + voiding.

---

## Phase 1 — Extract the `EventService` interface (no behavior change)

### Task 1: Define `EventService` and assert `*Service` satisfies it

**Files:**
- Create: `pkg/eventrepo/service.go`
- Reference: method signatures at `pkg/eventrepo/eventrepo.go:75,93,99,114,124,206,295,390`

- [ ] **Step 1: Confirm the consumer method-union.** The interface is exactly what resolvers + rpc + fetch call:
  - resolver (`cloud_events.resolvers.go`): `GetLatestIndexAdvanced`, `ListIndexesAdvanced`, `GetCloudEventTypeSummariesAdvanced`, `PresignBlobURL`.
  - rpc (`rpc/rpc.go`): `GetLatestIndex`, `GetLatestIndexAdvanced`, `ListIndexes`, `ListIndexesAdvanced` (verify whether it also calls a data-fetch method; grep below).
  - fetch (`fetch.go`): `ListCloudEventsFromIndexes`, `GetCloudEventFromIndex`.

  Run: `grep -n "s.eventService\.\|evtSvc\.\|EventService\." internal/fetch/rpc/rpc.go internal/fetch/fetch.go internal/graph/cloud_events.resolvers.go`
  Expected: a finite method list — reconcile against the interface below; add any missing method.

- [ ] **Step 2: Write the interface**

```go
package eventrepo

import (
	"context"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/pkg/grpc"
)

// EventService is the cloudevent fetch surface consumed by the GraphQL
// resolver, the gRPC FetchService, and internal/fetch. ClickHouse (*Service)
// and the DuckLake-backed duck.LakeEventService both implement it, selected
// by QUERY_BACKEND.
type EventService interface {
	// Index lookups (metadata + object locator, no payload).
	GetLatestIndex(ctx context.Context, opts *grpc.SearchOptions) (cloudevent.CloudEvent[ObjectInfo], error)
	GetLatestIndexAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[ObjectInfo], error)
	ListIndexes(ctx context.Context, limit int, opts *grpc.SearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error)
	ListIndexesAdvanced(ctx context.Context, limit int, opts *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error)

	// Aggregation.
	GetCloudEventTypeSummariesAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) ([]CloudEventTypeSummary, error)

	// Payload resolution from an index entry.
	GetCloudEventFromIndex(ctx context.Context, index *cloudevent.CloudEvent[ObjectInfo], bucketName string) (cloudevent.RawEvent, error)
	ListCloudEventsFromIndexes(ctx context.Context, indexes []cloudevent.CloudEvent[ObjectInfo], bucketName string) ([]cloudevent.RawEvent, error)

	// Blob payloads served as presigned URLs.
	PresignBlobURL(ctx context.Context, key string) (string, error)
}

var _ EventService = (*Service)(nil)
```

> Use the exact import path for `grpc` (`SearchOptions`/`AdvancedSearchOptions`) as imported in `eventrepo.go` — copy that import line rather than guessing. If Step 1 surfaces extra methods (e.g. `GetCloudEventTypeSummaries`, `GetObjectFromKey`), add them to the interface.

- [ ] **Step 2: Build**

Run: `go build ./pkg/eventrepo/`
Expected: PASS — `var _ EventService = (*Service)(nil)` proves the CH service already satisfies it.

- [ ] **Step 3: Commit**

```bash
git add pkg/eventrepo/service.go
git commit -m "feat(eventrepo): extract EventService interface (CH Service satisfies it)"
```

### Task 2: Retarget consumers to the interface

**Files:**
- Modify: `internal/graph/resolver.go:20`
- Modify: `internal/fetch/rpc/rpc.go:19-27`
- Modify: `internal/fetch/fetch.go` (the `evtSvc` parameter types)

- [ ] **Step 1: resolver** — change `EventService *eventrepo.Service` → `EventService eventrepo.EventService`.

- [ ] **Step 2: rpc** — `Server.eventService *eventrepo.Service` → `eventrepo.EventService`; `NewServer(buckets []string, eventService *eventrepo.Service)` → `NewServer(buckets []string, eventService eventrepo.EventService)`.

- [ ] **Step 3: fetch** — change the `evtSvc` params in `ListCloudEventsFromIndexes`/`GetCloudEventFromIndex` (and any helper) from the concrete type to `eventrepo.EventService`. If `fetch.go` already declares a local interface for `evtSvc`, replace it with `eventrepo.EventService`.

- [ ] **Step 4: Build + test**

Run: `go build ./... && go test ./internal/graph/... ./internal/fetch/... -count=1`
Expected: PASS — purely a type-widening; behavior identical (still the CH service at runtime).

- [ ] **Step 5: Commit**

```bash
git add internal/graph/resolver.go internal/fetch/rpc/rpc.go internal/fetch/fetch.go
git commit -m "refactor: consumers depend on eventrepo.EventService interface"
```

---

## Phase 2 — Extend the raw filter for fetch parity

### Task 3: Add tags/data_version filters + voiding to `RawFilter`/`whereClause`

**Files:**
- Modify: `internal/service/duck/raw.go:26-36` (`RawFilter`), `:185-214` (`whereClause`)

- [ ] **Step 1: Write a failing test** for the new filter SQL.

**Files:** Create/extend `internal/service/duck/raw_test.go`

```go
func TestWhereClauseTagsAndDataVersion(t *testing.T) {
	where, args := whereClause(RawFilter{
		Subject:      "did:1",
		DataVersions: []string{"v1"},
		Tags:         []string{"a", "b"},
		ExcludeVoided: true,
	})
	require.Contains(t, where, "data_version IN")
	require.Contains(t, where, "list_has_any") // tags JSON array overlap
	require.Contains(t, where, "NOT EXISTS")   // voiding anti-join
	require.Contains(t, args, "did:1")
}
```

- [ ] **Step 2: Run — fails to compile** (`DataVersions`/`Tags`/`ExcludeVoided` don't exist).

Run: `go test ./internal/service/duck/ -run TestWhereClauseTags`
Expected: build error.

- [ ] **Step 3: Extend `RawFilter`**

```go
type RawFilter struct {
	Subject       string
	Types         []string
	Sources       []string
	Producers     []string
	IDs           []string
	DataVersions  []string
	Tags          []string // matches when raw_events.extras.tags overlaps any
	After         time.Time
	Before        time.Time
	ExcludeVoided bool // hide events voided by a tombstone (voids_id anti-join)
}
```

- [ ] **Step 4: Extend `whereClause`** — add after the existing `addIn` calls:

```go
	addIn("data_version", filter.DataVersions)
	if len(filter.Tags) > 0 {
		// extras is JSON text; tags live at extras.tags as a string array.
		ph := placeholders(len(filter.Tags))
		conds = append(conds, fmt.Sprintf(
			"list_has_any(COALESCE(json_extract(extras, '$.tags'), '[]')::VARCHAR[], [%s])", ph))
		for _, t := range filter.Tags {
			args = append(args, t)
		}
	}
```

> Voiding is NOT a simple `whereClause` condition (it needs a correlated subquery over the same table); it is applied where the FROM is known. Keep `ExcludeVoided` on the filter but consume it in the lake query builder (Task 5), not in the shared `whereClause`. The test's `NOT EXISTS` assertion therefore belongs in the lake-query test (Task 5), not here — adjust Step 1 to assert only `data_version IN` and `list_has_any` in `whereClause`, and move the `NOT EXISTS` assertion to Task 5.

- [ ] **Step 5: Confirm JSON/tags shape.** Verify `lake.raw_events.extras` holds tags at `$.tags` as a JSON string array (match CH's `JSONExtract(extras,'tags','Array(String)')`).

Run: `grep -rn "tags\|Tags\|extras\|RestoreNonColumnFields" $(go list -m -f '{{.Dir}}' github.com/DIMO-Network/cloudevent)/*.go | grep -i tag | head`
Expected: confirms tags serialization into extras. Adjust the `json_extract` path if different.

- [ ] **Step 6: Run the where-clause test**

Run: `go test ./internal/service/duck/ -run TestWhereClauseTags -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/service/duck/raw.go internal/service/duck/raw_test.go
git commit -m "feat(duck): tags/data_version raw filters; voiding flag"
```

---

## Phase 3 — `LakeEventService` over `lake.raw_events`

### Task 4: Lake list/latest/types reading `lake.raw_events`

**Files:**
- Create: `internal/service/duck/lake_fetch.go`
- Reference: `internal/service/duck/raw.go` (`scanStoredEvent`, `query`, `whereClause`, dedup), `pkg/eventrepo/eventrepo.go` (`ObjectInfo`, `CloudEventTypeSummary`, return types)

- [ ] **Step 1: Investigate the index/blob contract.** Determine how din stores large payloads: confirm `lake.raw_events.data_index_key` carries an S3 blob key (with the `eventrepo.BlobKeyPrefix`) for large payloads, and is empty/null when `data`/`data_base64` are inline. Record findings.

Run: `grep -rn "data_index_key\|BlobKeyPrefix\|blobs/\|DataIndexKey" internal/ ../din 2>/dev/null | head`
Expected: blob-key contract identified.

- [ ] **Step 2: Write the core lake query (full rows from `lake.raw_events`, with voiding)**

```go
package duck

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
)

const lakeRawEvents = "lake.raw_events"

// lakeRawColumns matches scanStoredEvent's column order (raw.go rawColumns).
const lakeRawColumns = "subject, time, type, id, source, producer, data_content_type, data_version, extras, data, data_base64, data_index_key, voids_id"

// queryLakeRaw returns events matching filter, newest first, capped at limit,
// deduped on the header key, with tombstone-voided events excluded.
func (l *LakeEventService) queryLakeRaw(ctx context.Context, filter RawFilter, limit int) ([]cloudevent.StoredEvent, error) {
	where, args := whereClause(filter)
	voiding := ""
	if filter.ExcludeVoided {
		// Hide events whose id is referenced by a tombstone's voids_id, and the
		// tombstones themselves (voids_id != '').
		voiding = fmt.Sprintf(` AND e.voids_id = '' AND NOT EXISTS (
  SELECT 1 FROM %s t WHERE t.subject = e.subject AND t.voids_id = e.id)`, lakeRawEvents)
	}
	q := fmt.Sprintf("SELECT %s FROM %s e WHERE %s%s ORDER BY time DESC LIMIT %d",
		lakeRawColumns, lakeRawEvents, strings.ReplaceAll(where, "subject", "e.subject"), voiding, limit*2)
	rows, err := l.svc.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("querying lake raw_events: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	seen := map[string]struct{}{}
	var events []cloudevent.StoredEvent
	for rows.Next() && len(events) < limit {
		ev, err := scanStoredEvent(rows)
		if err != nil {
			return nil, err
		}
		key := ev.Key()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		events = append(events, ev)
	}
	return events, rows.Err()
}
```

> The `strings.ReplaceAll(where, "subject", "e.subject")` is fragile — instead, qualify columns at build time. Cleaner: add a `qualifyCol` prefix parameter to `whereClause` (e.g. `whereClauseQ(filter, "e.")`) returning `e.subject`, `e.type`, … so no string surgery. Implement that small refactor in `raw.go` and call it here. (Update Task 3's `whereClause` to delegate to `whereClauseQ(filter, "")`.)

- [ ] **Step 3: Build**

Run: `go build ./internal/service/duck/`
Expected: FAIL — `LakeEventService` type not defined yet (Task 5).

### Task 5: `LakeEventService` implementing `eventrepo.EventService`

**Files:**
- Create (continue): `internal/service/duck/lake_fetch.go`

- [ ] **Step 1: Define the type + constructor + index/types/payload methods**

```go
// LakeEventService serves the eventrepo.EventService surface from
// lake.raw_events. Index lookups return a header + an ObjectInfo locator;
// payload resolution reads the inline data (or presigns a blob).
type LakeEventService struct {
	svc       *Service
	presigner eventrepo.Presigner // reused S3 presigner for blob payloads
}

func NewLakeEventService(svc *Service, presigner eventrepo.Presigner) *LakeEventService {
	return &LakeEventService{svc: svc, presigner: presigner}
}

var _ eventrepo.EventService = (*LakeEventService)(nil)

func (l *LakeEventService) ListIndexesAdvanced(ctx context.Context, limit int, opts *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	evs, err := l.queryLakeRaw(ctx, filterFromAdvanced(opts), limit)
	if err != nil {
		return nil, err
	}
	out := make([]cloudevent.CloudEvent[eventrepo.ObjectInfo], len(evs))
	for i, e := range evs {
		out[i] = toIndex(e)
	}
	return out, nil
}

func (l *LakeEventService) GetLatestIndexAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	list, err := l.ListIndexesAdvanced(ctx, 1, opts)
	if err != nil {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, err
	}
	if len(list) == 0 {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, ErrNotFound
	}
	return list[0], nil
}

func (l *LakeEventService) GetCloudEventTypeSummariesAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) ([]eventrepo.CloudEventTypeSummary, error) {
	f := filterFromAdvanced(opts)
	where, args := whereClauseQ(f, "")
	q := fmt.Sprintf(`SELECT type, count(*), min(time), max(time)
FROM %s WHERE %s GROUP BY type ORDER BY type`, lakeRawEvents, where)
	rows, err := l.svc.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("lake type summaries: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []eventrepo.CloudEventTypeSummary
	for rows.Next() {
		var s eventrepo.CloudEventTypeSummary
		// Scan into the fields CloudEventTypeSummary exposes (Type, Count,
		// FirstSeen, LastSeen) — match its exact field names/types.
		if err := rows.Scan(&s.Type, &s.Count, &s.FirstSeen, &s.LastSeen); err != nil {
			return nil, fmt.Errorf("scanning type summary: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
```

> `filterFromAdvanced(opts *grpc.AdvancedSearchOptions) RawFilter` translates the gRPC options to a `RawFilter` and sets `ExcludeVoided: true`. Mirror `eventrepo.AdvancedSearchOptionsToQueryMod` field-for-field (subject/type/source/producer/id/after/before/dataversion/tags). `toIndex(StoredEvent) cloudevent.CloudEvent[ObjectInfo]` builds the index entry: copy the header; set `ObjectInfo.Key` to the blob key (`data_index_key`) when the payload is a blob, else a lake locator that encodes the row id (e.g. `"lake://" + subject + "/" + id`). Confirm `ObjectInfo`'s exact fields from `eventrepo.go:42`.

- [ ] **Step 2: Payload resolution + presign + the SearchOptions (non-Advanced) variants**

```go
func (l *LakeEventService) GetCloudEventFromIndex(ctx context.Context, index *cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) (cloudevent.RawEvent, error) {
	// Re-read the row by (subject, id) from the lake; data is inline.
	evs, err := l.queryLakeRaw(ctx, RawFilter{Subject: index.Subject, IDs: []string{index.ID}, ExcludeVoided: true}, 1)
	if err != nil {
		return cloudevent.RawEvent{}, err
	}
	if len(evs) == 0 {
		return cloudevent.RawEvent{}, ErrNotFound
	}
	return toRawEvent(evs[0]), nil
}

func (l *LakeEventService) ListCloudEventsFromIndexes(ctx context.Context, indexes []cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) ([]cloudevent.RawEvent, error) {
	out := make([]cloudevent.RawEvent, 0, len(indexes))
	for i := range indexes {
		ev, err := l.GetCloudEventFromIndex(ctx, &indexes[i], "")
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}

func (l *LakeEventService) PresignBlobURL(ctx context.Context, key string) (string, error) {
	return l.presigner.PresignGetObject(ctx, key) // match Presigner's actual method signature
}

// SearchOptions (non-Advanced) variants: translate opts to AdvancedSearchOptions
// and delegate, matching how eventrepo.Service relates the two.
func (l *LakeEventService) GetLatestIndex(ctx context.Context, opts *grpc.SearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return l.GetLatestIndexAdvanced(ctx, toAdvanced(opts))
}
func (l *LakeEventService) ListIndexes(ctx context.Context, limit int, opts *grpc.SearchOptions) ([]cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return l.ListIndexesAdvanced(ctx, limit, toAdvanced(opts))
}
```

> `toRawEvent(StoredEvent) cloudevent.RawEvent` — confirm `RawEvent`'s shape; `StoredEvent` embeds the header + data, so this is a field copy (`raw.go:scanStoredEvent` already builds the `StoredEvent`). `toAdvanced(*grpc.SearchOptions) *grpc.AdvancedSearchOptions` mirrors eventrepo's own translation (find it near `eventrepo.go:99/124`). `PresignGetObject` — use the actual `Presigner` interface method name from `eventrepo.go`.

- [ ] **Step 3: Build**

Run: `go build ./internal/service/duck/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/service/duck/lake_fetch.go internal/service/duck/raw.go
git commit -m "feat(duck): LakeEventService over lake.raw_events (types, voiding, presign)"
```

### Task 6: `LakeEventService` test on a file catalog

**Files:**
- Create: `internal/service/duck/lake_fetch_test.go`

- [ ] **Step 1: Write tests** booting a file catalog, inserting `lake.raw_events` rows (inline-data events of two types, one tombstone voiding one event, one large-payload blob event), then asserting:
  - `ListIndexesAdvanced` returns newest-first, deduped, **excludes the voided event and the tombstone**.
  - `GetLatestIndexAdvanced` returns the newest non-voided event; `ErrNotFound` on empty.
  - `GetCloudEventTypeSummariesAdvanced` returns per-type count/first/last (voided excluded).
  - `GetCloudEventFromIndex` returns inline data for an inline event; for the blob event, `toIndex` set a blob key → resolver path would presign (assert the index `ObjectInfo.Key` has the blob prefix).
  - Tags/data_version filters narrow correctly.

- [ ] **Step 2: Run**

Run: `go test ./internal/service/duck/ -run TestLakeEventService -v`
Expected: PASS. (Validates the `json_extract`/`list_has_any`/`NOT EXISTS` SQL against real DuckDB; fix dialect here.)

- [ ] **Step 3: Commit**

```bash
git add internal/service/duck/lake_fetch_test.go
git commit -m "test(duck): LakeEventService over file-catalog lake.raw_events"
```

---

## Phase 4 — Wire selection + shadow

### Task 7: Select the EventService by backend in `app.go`

**Files:**
- Modify: `internal/app/app.go:47-75,166-169`

- [ ] **Step 1: Investigate current construction.** `app.go` builds `chService` (line 47), `chConn` (line 70), and `eventService = eventrepo.New(chConn, …)` (lines 75, 166) unconditionally. Goal: in ducklake mode, build neither `chConn` nor `chService` — construct `LakeEventService` against the catalog-attached duck service.

- [ ] **Step 2: Add an event-service selector** (new function in `internal/app/backend.go`):

```go
// newEventService selects the cloudevent fetch backend per QUERY_BACKEND.
// ducklake → lake.raw_events; everything else → ClickHouse cloud_event index.
func newEventService(settings *config.Settings, duckSvc *duck.Service, s3Client *s3.Client) (eventrepo.EventService, error) {
	presigner := s3.NewPresignClient(s3Client)
	if settings.QueryBackend == config.QueryBackendDuckLake {
		return duck.NewLakeEventService(duckSvc, presigner), nil
	}
	chConn, err := chClientFromSettings(&settings.ClickhouseFileCatalogue)
	if err != nil {
		return nil, fmt.Errorf("ClickHouse connection for event repo: %w", err)
	}
	return eventrepo.New(chConn, s3Client, presigner, settings.ParquetBucket), nil
}
```

> In ducklake mode the `duckSvc` here must have the catalog attached. The query backend (`newQueryBackend`) already creates a catalog-attached `duck.Service` for ducklake; reuse that same `duckSvc` for the event service (thread it out of `newQueryBackend`, or build one shared catalog-attached service for both reads and fetch). Decide and document: simplest is to construct one ducklake `duck.Service` in `New` and pass it to both `newQueryBackend` and `newEventService`. Confirm `Presigner` is the type `eventrepo.New` expects.

- [ ] **Step 3: Use the selector in `app.go`.** Replace the unconditional `eventService := eventrepo.New(chConn, …)` (both sites) with `eventService, err := newEventService(&settings, duckSvc, s3Client)`. Ensure `chConn`/`chService` are only built when the backend needs them (clickhouse/duckdb/shadow), not in ducklake mode.

- [ ] **Step 4: Build**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 5: e2e: ducklake mode constructs no ClickHouse client**

**Files:** extend `tests/ducklake_e2e_test.go` (or add `tests/ducklake_no_clickhouse_test.go`)

Boot the app wiring in ducklake mode against a file catalog with **no ClickHouse reachable** and assert: `cloudEvents`/`latestCloudEvent`/`availableCloudEventTypes` (and a gRPC `Fetch`) all succeed reading the lake; `segments` succeeds (from the segments plan). The absence of a CH connection error is the assertion.

Run: `go test ./tests/ -run TestDuckLakeNoClickHouse -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/app/app.go internal/app/backend.go tests/
git commit -m "feat: select lake EventService in ducklake mode; no CH client"
```

### Task 8: `ShadowEventService` (compare lake vs CH index results)

**Files:**
- Create: `pkg/eventrepo/shadow.go`

- [ ] **Step 1: Write a shadow wrapper** implementing `EventService`, serving from a CH primary and comparing the lake secondary's `ListIndexesAdvanced`/`GetLatestIndexAdvanced`/`GetCloudEventTypeSummariesAdvanced` results in the background. Reuse the `dq_shadow_mismatch_total`/`dq_shadow_error_total` counters (move them to a shared spot or add fetch-specific counters `dq_fetch_shadow_*`). Compare on header identity + type-summary tuples (payload bytes need not match; compare metadata).

```go
package eventrepo

// ShadowEventService serves fetch from primary (CH) and replays index queries
// against secondary (lake), counting metadata mismatches. Payloads are served
// from primary only.
type ShadowEventService struct {
	primary, secondary EventService
	// ... sem, timeout, wg, logger mirroring repositories.ShadowBackend
}
```

> Keep payload methods (`GetCloudEventFromIndex`, `ListCloudEventsFromIndexes`, `PresignBlobURL`) primary-only — only index/list/summary get shadowed. Match the bounded-concurrency + drop-on-saturation pattern from `repositories/shadow.go`.

- [ ] **Step 2: Wire** — in `newEventService`, when `QueryBackend == QueryBackendShadow`, return `eventrepo.NewShadowEventService(chEventService, lakeEventService)`. This requires both a CH conn and a catalog-attached duck service in shadow mode.

- [ ] **Step 3: Build + test**

**Files:** `pkg/eventrepo/shadow_test.go` — primary and secondary fakes; assert mismatch increments and primary always served.

Run: `go build ./... && go test ./pkg/eventrepo/ -count=1`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/eventrepo/shadow.go pkg/eventrepo/shadow_test.go internal/app/backend.go
git commit -m "feat(eventrepo): shadow fetch index results lake vs ClickHouse"
```

---

## Phase 5 — Parity + verify

### Task 9: e2e fetch parity (lake vs CH)

**Files:**
- Create: `tests/fetch_lake_parity_test.go`

- [ ] **Step 1: Write the parity test** — insert identical cloudevents into the CH `cloud_event` index (+ S3/parquet payloads) and `lake.raw_events`; assert for the same `AdvancedSearchOptions`:
  - `ListIndexesAdvanced` returns the same events in the same order (compare header identity).
  - `GetLatestIndexAdvanced` agrees.
  - `GetCloudEventTypeSummariesAdvanced` returns identical per-type count/first/last.
  - filter coverage: subject/type/source/producer/id/after/before/data_version/tags each narrow identically.
  - voiding: an event with a matching tombstone is absent from BOTH (note: CH today does not void — so for the voiding case assert the lake hides it and document the intended divergence; gate the CH-equality assertion off for the voided rows).

  Gate the CH side on the same CH-availability env other `tests/` CH tests use; always run the lake side.

- [ ] **Step 2: Run**

Run: `go test ./tests/ -run TestFetchLakeParity -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/fetch_lake_parity_test.go
git commit -m "test: e2e fetch parity lake vs ClickHouse"
```

### Task 10: Full build + test + lint

- [ ] **Step 1: Run**

```bash
go build ./... && go test ./internal/... ./tests/ ./pkg/... -count=1 && golangci-lint run ./...
```
Expected: PASS, `0 issues`.

- [ ] **Step 2: Confirm ducklake mode is CH-free** (both surfaces). Grep that neither `newQueryBackend` nor `newEventService` builds a CH client in the ducklake arm.

Run: `grep -n "QueryBackendDuckLake" -A4 internal/app/backend.go`
Expected: ducklake arms reference only duck/lake constructors.

- [ ] **Step 3: Commit lint fixes**

```bash
git add -A && git commit -m "chore(fetch): lint clean"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** Component B (fetch on lake) → Tasks 1–9; type-summaries → Task 5; extras/tags/data_version filters → Task 3; blob presign → Tasks 4/5; read-side voiding → Tasks 4/5/9; CH-free ducklake mode → Task 7; shadow validation → Task 8. ✓
- **Placeholders:** none — interface is concrete; the helper functions (`filterFromAdvanced`, `toIndex`, `toRawEvent`, `toAdvanced`, `whereClauseQ`) are each specified with their source-of-truth (`eventrepo.AdvancedSearchOptionsToQueryMod`, `ObjectInfo`, `RawEvent`). Type/field confirmations are explicit investigation steps (Tasks 1,3,4,5) rather than vague TODOs.
- **Type consistency:** `EventService` method set matches the consumer-union (Task 1) and the lake impl (Tasks 5); `RawFilter` fields added in Task 3 are consumed in Tasks 4/5; `lakeRawColumns` matches `scanStoredEvent`'s scan order.
- **Cross-plan:** shares the branch with the segments plan; `internal/app/backend.go` is touched by both — sequence fetch Task 7 after segments Task 13 to avoid a merge of the same arm, or land segments first.
- **Open confirmations folded into tasks:** consumer method-union (Task 1), tags JSON shape (Task 3), blob-key contract (Task 4), `ObjectInfo`/`RawEvent`/`Presigner`/`CloudEventTypeSummary` shapes (Tasks 1,5), shared catalog-attached `duck.Service` for reads+fetch (Task 7).
