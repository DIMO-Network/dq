# DuckLake cutover gate (CHD-15 / CHD-36)

The checklist that must be green before flipping `QUERY_BACKEND=ducklake` in
prod. Never collapse the rollback window and the latent-bug-discovery window
into one step.

## 1. Reconciliation (bulk, explicit)

`internal/reconcile` compares per-signal summaries (count, first/last seen)
between the ClickHouse primary and the DuckLake secondary for a sample of
vehicles. Build a small runner that wires both backends (the `repositories`
CH service and `duck.NewLakeQueries`) and a vehicle sample, then:

```go
rep, err := reconcile.Reconcile(ctx, chBackend, lakeBackend, sampledSubjects)
// require: err == nil && len(rep.Mismatches) == 0
```

`rep.Mismatches` lists every `(subject, name)` disagreement. **Require an empty
report** over a representative sample (high-traffic + long-history + sparse
vehicles) before proceeding. This is the bulk gate the migration previously
lacked — shadow mode only covered organically-queried vehicles.

## 2. Shadow clean

With `QUERY_BACKEND=shadow`, require over the bake period:

- `dq_shadow_mismatch_total` == 0 and `dq_fetch_shadow_mismatch_total` == 0
- `dq_shadow_dropped_total` == 0 and `dq_fetch_shadow_dropped_total` == 0
  (a non-zero drop count means comparisons were skipped — the gate did not look,
  so a clean mismatch counter is not trustworthy; raise shadow concurrency or
  reduce load until drops are zero — alert `DQShadowDropped`).

## 3. Gate suites

```
make test-gated      # standard + PG-concurrency + chaos + perf + MinIO
```

Set `PG_CATALOG_DSN` (Postgres-catalog concurrency), `DQ_CHAOS=1` (SIGKILL
exactly-once), `-perf` (files-scanned pruning), and install `minio` (real-S3).
Suites without their prerequisite skip cleanly, so CI can run the whole target
and gate on what is configured.

## 4. Observability live

- `dq_materializer_lag_seconds{type="ducklake"}` moves under load (not flat-zero
  while behind) and `dq:pipeline_snapshot_backlog` is bounded.
- `/ready` returns 200 only when the catalog is reachable.
- `DQMaterializerCursorReset` has never fired (a reset = permanently skipped
  un-decoded data).

Only when 1–4 hold: flip `QUERY_BACKEND=ducklake`, keep ClickHouse +
telemetry/fetch-api hot for the bake period, and do not retire them until the
rollback window has closed.

## Storage & dual-run budget (CHD-38)

DuckLake **snapshot expiry** (`LAKE_SNAPSHOT_RETENTION`) bounds how far back the
change feed goes; it does **not** bound decoded-table data size — `lake.signals`
/ `lake.events` grow unbounded. Two levers:

- **Row-level TTL (optional):** set `LAKE_DECODED_RETENTION` (Go duration, e.g.
  `8760h`) on the materializer release to prune decoded rows past the window
  (`PruneDecoded`, hourly). Default empty = no TTL — enabling it deletes
  customer history, so it is a product decision. The rollup
  (`lake.signals_latest`) is never pruned (it is current state). din's catalog
  maintenance reclaims the deleted files.
- **Dual-run budget:** during the bake period both ClickHouse and the lake hold
  the full dataset, so plan for ~2× storage until CH teardown. Budget the
  overlap explicitly; do not shorten the bake to save storage.

## Per-signal privilege (CHD-37)

List/latest endpoints enforce per-signal privileges through the GraphQL auth
directives wired in `app.New` (`RequiresAllOfPrivileges` / `RequiresOneOfPrivilege`
/ `RequiresVehicleToken`), bound to the validated token claims. Verify any new
signal field carries the appropriate directive.

## din side (separate)

NATS repartitioning runbook + consumer-skew metric and MsgID sub-second
precision (CHD-37) live in the din ingest service; they are tracked on the din
branch, not here.
