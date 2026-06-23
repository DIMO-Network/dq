# dq Merger Review Notes

Review of the telemetry-api + fetch-api → dq merger, 2026-04-23.

## Confirmed clean

- `tokenId` → `subject` rename is total: no remaining `tokenId`/`TokenID` in schema, resolver, or repository signatures.
- `did` → `subject` rename on the fetch side is done everywhere externally (schema, resolvers, directive argument name).
- `DetectionMechanism` is the only enum that actually needed snake-casing, and it was done consistently — including in-doc references like `[frequencyAnalysis]` → `[FREQUENCY_ANALYSIS]`.
- `CloudEventIndex` / `indexKey` / `indexes` / `latestIndex` / `attestations` / `vinVCLatest` are fully gone from the GraphQL surface. Internal `ListIndexesAdvanced` / `GetLatestIndexAdvanced` remain as implementation for `cloudEvents` / `latestCloudEvent`, which is correct.
- Config is collapsed cleanly. Telemetry-only fields (`VehicleNFTAddress`, `ChainID`, `FetchAPIGRPCEndpoint`, `CreditTrackerEndpoint`, `StorageNodeDevLicense`, `VINDataVersion`) and fetch-only `DQEndpoint` are all removed. (The query backend is now DuckLake-only.)
- Auth claim model is simplified correctly: `DQClaim` embeds `tokenclaims.Token`, directive compares `claim.Asset` to the `subject` arg as strings. No more DID reconstruction from contract + tokenId.
- `queryRecorder`, `pricing`, `dtcmiddleware`, `credittracker`, vc/attestation repos — all fully removed, no dangling references.

## Issues

### 1. `signalsSnapshot` privilege filter — FIXED

`internal/graph/base.resolvers.go:69-79` previously hand-rolled a 3-way switch for per-signal privilege filtering:

```go
case vss.FieldCurrentLocationCoordinates:   // VEHICLE_ALL_TIME_LOCATION
case model.ApproximateCoordinatesField:      // approximate OR all-time
default:                                     // VEHICLE_NON_LOCATION_DATA
```

Telemetry's equivalent (`internal/graph/privilege_filter.go` + generated `SignalPrivileges` map) covered the full set from `default-definitions.yaml`, which also marks `currentLocationAltitude` and `currentLocationHeading` as `VEHICLE_ALL_TIME_LOCATION` (model-garage v1.0.11 `pkg/schema/spec/default-definitions.yaml:44-49`).

In dq, a caller with only `VEHICLE_NON_LOCATION_DATA` calling `signalsSnapshot` would receive `currentLocationAltitude` and `currentLocationHeading` values — the schema-level `@requiresAllOfPrivileges` directive on `SignalCollection.currentLocationAltitude/Heading` does not apply to the `SignalsSnapshotResponse.signals` list since those items are just `LatestSignal { name, values... }`. The Go filter was the only gate, and it was too loose.

**Fix applied:** `Repository` now builds a `jsonName → []privilegeEnum` map at init from the already-loaded `schema.Definitions` and exposes `RequiredPrivileges(name)`. `hasPrivilegesForSignal` consults it, maps each enum to the corresponding `tokenclaims` permission, and requires all. `ApproximateCoordinatesField` remains as an OR special case. Unknown signals fail closed.

### 2. MCP support dropped without being in the README — FIXED

Both source services shipped an `/mcp` endpoint backed by `mcpserver.New` + `graph.MCPTools` + `@mcpTool` / `@mcpExample` directives. dq removed all of it: no `mcp.graphqls`, no `mcp_tools_gen.go`, no `directives:` section in `gqlgen.yml`, no `/mcp` route in `cmd/dq/main.go`. If removing MCP is intentional, add it to the migration notes; if not, it's a silent feature loss.

**Fix applied:** Restored `/mcp` with a single merged `dq_` tool prefix and display name "DIMO Query" (server-garage bumped v0.0.7 → v0.1.1). Re-annotated 11 queries that still exist in dq: `signals`, `signalsLatest`, `availableSignals`, `signalsSnapshot`, `dataSummary`, `segments`, `dailyActivity`, `events`, `latestCloudEvent`, `cloudEvents`, `availableCloudEventTypes`. Examples rewritten to use `subject` and `FREQUENCY_ANALYSIS`-style enum values. `mcpgen` wired via `//go:generate` in `internal/graph/resolver.go`, picked up by the existing `make generate` target. `/mcp` shares the same auth chain as `/query` via a new `authChain` closure in `internal/app/app.go`. Side effect: `hasPrivilegesForSignal` + `privilegeEnumToPermission` were moved out of `base.resolvers.go` into a new `internal/graph/privilege_filter.go` — gqlgen strips helper functions from resolver files on regen.

### 3. gRPC interceptor stack thinned out

`internal/app/app.go:135-140` uses only `metrics.GRPCMetricsAndLogMiddleware` + recovery. fetch-api (`internal/app/app.go:155-163`) also had `grpc_prometheus.Unary/StreamServerInterceptor` and `grpc_ctxtags.UnaryServerInterceptor`. If any fetch-api dashboards relied on the Prometheus interceptor's metrics, they'll go silent once fetch-api is switched off. Confirm the shared `metrics.GRPCMetricsAndLogMiddleware` covers the same surface.

### 4. Test coverage loss

**Unit tests** that existed in the source repos but weren't ported and are not feature-specific (i.e., not attestation/vc/pricing/queryRecorder):

- `telemetry-api/internal/auth/auth_test.go`
- `telemetry-api/internal/repositories/{repositories,validate,repositories_mocks}_test.go`
- `fetch-api/internal/graph/{resolver,convert}_test.go` (the resolver test was ~11k — substantial)
- `fetch-api/internal/identity/identity_test.go`
- `fetch-api/pkg/grpc/common_test.go`

Repositories, auth, graph resolver, and identity tests would all port with modest edits. `dq/internal/graph/cloud_events_resolvers_test.go` exists but is only 1.8k.

**E2E tests** from `telemetry-api/e2e/` are missing entirely — dq has no `e2e/` directory. The feature-specific ones are correctly dropped (`attestation_test.go`, `vc_test.go`, `cost_estimator_test.go`, `credit_tracker_test.go`, `fetchapi_test.go`), but the rest cover general functionality and would not have been lost by design:

- `permission_test.go` (4.6k) — directly relevant to the `signalsSnapshot` filter issue fixed in §1; would have caught it
- `approximate_location_test.go` (7.2k) — same territory
- `signals_dataSummary_test.go` (9.8k)
- `signals_latest_test.go` (5.8k)
- `events_test.go` (13k)
- `segments_test.go` (18k)
- `auth_server_test.go` (4.5k)
- `setup_test.go` (test harness)

These ran against a real query DB and a mock auth server, so they exercise the full permissions/resolver/repository/DB path — exactly the surface where a hand-rolled privilege filter can silently lose fidelity. Porting `setup_test.go` + `permission_test.go`, adapted to the DuckLake-backed test harness (the lake parity/integration tests already stand up a real catalog), would be the highest-value starting point.

### 5. Duplicated construction in app init

`app.New` and `app.CreateGRPCServer` both independently build an S3 client, a presign client, and the lake event service — so the process ends up with two of each. Not a correctness bug but worth consolidating (construct once in `New`, pass into `CreateGRPCServer`). Re-verify after the DuckLake-only rewiring, which may already have collapsed some of this.

### 6. Stale naming from the rename

Not functional, but the rename effort stopped at the public surface:

- `internal/auth/directives.go:12` — `const didArg = "subject"` (the constant is still called `didArg`)
- `internal/auth/directives.go:41, 42, 54` — comments and the "DID in query does not match token claim" error message still say "DID"
- `internal/repositories/segments.go:169, 398, 451, 562`, `signals.go:199`, `validate.go:105` — parameter names are still `did string`

### 7. Empty `Cleanup()`

`internal/app/app.go:106` — `cleanup: func() {}`. Both source repos also left this effectively empty, so it's not a merger regression, just a pre-existing TODO carried over. The DuckDB/catalog and S3 connections won't be closed on shutdown.
