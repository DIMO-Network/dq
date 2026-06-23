# DuckLake can beat ClickHouse: feature-improvement analysis (dq)

The dq query layer was built to **mirror ClickHouse** (CH-parity) for the CH→DuckLake
cutover. That was right for a safe cutover — but it means dq inherits CH's
*limitations*. ClickHouse forced compromises DuckDB doesn't have. This doc lists
where DuckLake can do **better**, prioritized, code-grounded.

_Analysis 2026-06-23, verified against the code, with implementation in progress._

## Implementation status (2026-06-23)

| # | Improvement | Status |
|---|---|---|
| #0 | Ignition ongoing-trips (correct seed + deterministic) | **SHIPPED** `c83d6b5`,`210b345` |
| #1 | Exact-aggregation divergence documented + pinned | **SHIPPED** `406e54c` |
| #2 | ASOF **location gap-fill** (start/end, schema-free) | **SHIPPED** `6723ac3` |
| #3 | SQL-native **idling** (gaps-and-islands) | **SHIPPED** `6f2ebd7` |
| #2 | per-trip distance / avg-speed | distance already in `signals[]` (odo first/last); avg-speed already client-requestable — not forced as default (would churn the API + tests for marginal gain) |
| #6 | on-read latest; rollup is an optional optimization | already the architecture (`lake_latest.go` + optional `lake_rollup.go`) — no build needed |
| #3 | SQL-native **refuel / recharge** | REMAINING — high parity-risk (intricate Go trough/peak + smoothing logic to match exactly); focused PR with parity tests |
| #4 | **Spatial** geofencing | REMAINING — needs `LOAD spatial` in the bootstrap (the extension may be absent in some envs → LOAD-failure risk) + geo-edge parity; behind a fallback |
| #5 | Recharge odometer-filter via ASOF | REMAINING — folds into #3 recharge |
| #7 | Ignition debounce + assembly → SQL | REMAINING — parity-sensitive, perf/cleanliness only (low value) |

The SHIPPED items establish the pattern (the optional-capability interface:
`IdleRunSource`, `LocationAtSource` — CH keeps its path, lake adds the SQL-native one,
output guarded by the existing parity tests). The REMAINING items are the most
parity-sensitive; each is a focused PR.

---

## The architectural insight

Segment/trip detection is **shared Go** (`internal/segments/{ignition,idling,refuel,
recharge,frequency,changepoint}.go`) fed by a 3-method SQL `SignalSource`
(`WindowedSignalCounts`, `LevelSamples`, `IgnitionStateChanges`), with a ClickHouse
and a DuckLake impl; trip enrichment (start/end location, speed) is layered on in
`internal/repositories/segments.go`. CH's limits live where the shared Go layer
can't fix them:

1. **Pre-materialized views.** CH needs `signal_state_changes`/`signal_last_state`
   (`... FINAL`) and the `signal_latest_*` projection because it can't do stateful
   window functions on read — forcing the ignition `-1`-seed + 30-day-lookback
   compromises and MV maintenance/lag.
2. **"Fetch all raw samples to Go and loop."** Idling, refuel, recharge pull *every*
   RPM/fuel/SoC sample over the wire and detect in memory (`idling.go:61`,
   `refuel.go:57`, `recharge.go:52`), because CH SQL couldn't.
3. **Approximate / nondeterministic functions.** `uniq()` (HLL distinct), `median()`
   (t-digest), `argMax` ties, `FINAL` eventual dedup.

DuckDB removes all three (window functions on read, ASOF joins, exact aggregates,
deterministic `QUALIFY` dedup, spatial). Several of these are **already silently
diverging in the good direction** on the lake path — see #1 (exact aggregations).

---

## #0 — Ignition seed + ongoing trips (SHIPPED, dq `c83d6b5`) — the template

CH derives ignition state from the `signal_state_changes` MV (seed `-1`, 30-day
lookback) and suppresses in-progress trips. The lake path now computes state **on
read**: seeds `prev_state` from the **true unbounded last value before the window**
(single bucket-pruned `LIMIT 1` lookup) so an entered-ON vehicle isn't fabricated
into a phantom trip, and surfaces an ongoing trip as a synthetic ON. Template for
the rest: on-read freedom → more correct than CH's MV.

---

## PRIORITY 1 — Recognize + document the exact-aggregation divergence that is ALREADY SHIPPING

**This is the most urgent item: it's live, silent, and could be "fixed" back to
approximate by QA at cutover.** The lake path already diverges from CH toward
*correctness*, with no flag and no doc:

- **`median()` — EXACT on lake (`aggregations.go:380`) vs APPROXIMATE t-digest in CH
  (`ch/queries.go:239`).** A vehicle's median speed/SoC over a window is now exact;
  CH returned a t-digest estimate. Live correctness divergence.
- **`count(DISTINCT name)` — EXACT on lake (`segments_source.go:60,68`) vs
  approximate `uniq(name)` HLL in CH (`ch/segments_utils.go:43`).** Near the
  distinct-signal threshold, frequency/change-point window eligibility can differ.
- **Tie-free deterministic `arg_max` — lake's `QUALIFY ROW_NUMBER() OVER (PARTITION
  BY subject,name,timestamp ORDER BY cloud_event_id)` (`lake_latest.go:19`) makes
  every `(subject,name,timestamp)` unique**, so latest/first/last are deterministic;
  CH's `FINAL`+`argMax` is tie-nondeterministic and `FINAL` is expensive.
- **`mode()` / `string_agg(DISTINCT)` exact (`aggregations.go:398-400`) vs CH
  `topK`/`groupUniqArray` approximate.**

**Action:** decide these are intended improvements, write a divergence note, add
tests pinning the exact behavior, and (optionally) a feature flag if any consumer
depended on CH's approximate median. **Effort S. Value: correctness, effectively
free (already how the code runs). Parity: deliberate, must be documented before QA.**

---

## PRIORITY 2 — ASOF JOIN: turn trips into journeys (highest NEW-build leverage)

**CH limitation:** no ASOF join. Today "location at trip start" is approximated by
`argMin(value_location, timestamp)` **constrained to the trip's own window**
(`ch/queries.go:256-262` → `LocationAggregationFirst`, wired at `segments.go:125`).
Two costs:
- **Gap blindness:** if no GPS fix landed *inside* a short trip, start/end location is
  missing → `noDataLocation()` returns (0,0) (`segments.go:131,290`). A car parked
  with its last fix 10 min before ignition shows (0,0).
- **No distance, no avg speed** — only `MAX(speed)` (`segments.go:91`).

The `Segment` model **already carries a start/end location slot** filled by the repo
(`models_gen.go:142-144`, `detector.go:16`) — so the plumbing exists; it's just
weakly filled.

**DuckDB (ASOF JOIN, native):** join each trip boundary *timestamp* to the location
and speed streams "as of" that instant, reaching back to the last known reading:

```sql
-- gap-filled start/end location AS OF the transition (reaches before the window)
SELECT t.seg_idx, t.boundary_ts, l.loc_lat, l.loc_lon
FROM trip_boundaries t                       -- (seg_idx, boundary_ts) the detector already has
ASOF LEFT JOIN (
  SELECT timestamp, loc_lat, loc_lon FROM lake.signals
  WHERE subject = ? AND name = 'currentLocationCoordinates' AND (loc_lat != 0 OR loc_lon != 0)
) l ON l.timestamp <= t.boundary_ts;

-- distance: odometer delta (odo already fetched as sigOdoFirst/sigOdoLast, segments.go:96-97),
-- ASOF both ends; or sum of haversine over the route within the window.
```

**Value:** correctness (start/end populated even for sparse GPS) **and** per-trip
distance + avg speed. **Effort:** location gap-fill alone **S** (the `Start`/`End`
location slots already exist — schema-free). **NO-SCHEMA CONSTRAINT:** named
`Segment.distanceKm`/`avgSpeed` top-level fields would need a schema change (ruled
out). Under no-schema, surface them through the **existing `Signals[]` channel** —
avg-speed as a `FloatAggregationAvg` aggregate, distance from the odo first/last
already returned there (or a synthetic `Signals[]` entry) — no new fields. **Parity:**
intentional improvement — strictly *more* data; the ASOF start/end loc is a superset
of the windowed value).

---

## PRIORITY 3 — SQL-native run-length / level-jump: delete the fetch-all-to-Go loops

**CH limitation:** can't express run-length or LAG-delta on read, so all three pull
every sample to Go and loop:
- **Idling** = run-length of `0 < RPM <= maxIdle`, gap-broken (`idling.go:61-92`).
- **Refuel** = 5-min-window rise + backward-trough/forward-peak walk (`refuel.go`).
- **Recharge** = 11-sample moving average, trough→peak by `s[i]-s[i-1]`, + odometer
  movement filter (a *second* full stream) (`recharge.go:52-61`).

**DuckDB (window functions + gaps-and-islands):** push detection into SQL, return
only the runs/jumps:

```sql
-- idling: islands of consecutive idle-band samples, gap-broken
WITH s AS (SELECT timestamp, value_number rpm FROM lake.signals
           WHERE name='powertrainCombustionEngineSpeed' AND subject=? AND <bucket> AND ...),
f AS (SELECT *, lag(rpm>0 AND rpm<=:maxIdle) OVER o lag_idle, lag(timestamp) OVER o lag_ts FROM s
      WINDOW o AS (ORDER BY timestamp)),
g AS (SELECT *, (rpm>0 AND rpm<=:maxIdle) idle,
        sum(CASE WHEN (rpm>0 AND rpm<=:maxIdle) AND (NOT coalesce(lag_idle,false)
                    OR timestamp-lag_ts > :maxGap) THEN 1 ELSE 0 END) OVER (ORDER BY timestamp) island
      FROM f)
SELECT island, min(timestamp) start_ts, max(timestamp) end_ts FROM g WHERE idle
GROUP BY island HAVING max(timestamp)-min(timestamp) >= :minDur;

-- refuel/recharge: delta = value_number - lag(value_number) OVER (ORDER BY ts); flag jumps the same way.
-- recharge smoothing → avg(value_number) OVER (ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING).
```

**Implementation:** add richer SQL-returning methods to the **lake** `SignalSource`
(e.g. `IdleRuns`, `LevelJumps`) so the CH path keeps its Go loop — clean backend
separation; `parity_test.go` guards equivalence on shared fixtures. **Value:** much
less data over the wire, exact boundaries, single round trip; retires the Go state
machines. **Effort:** idling **S/M**, refuel/recharge **M** (port the
`refuelPeakStabilizationDrop` / smoothing thresholds exactly). **Parity:**
improvement, low risk if thresholds reproduced.

---

## PRIORITY 4 — Spatial extension for geo filters + trip routes (baked into the image, NOT yet loaded)

**CH limitation:** `pointInPolygon`/`geoDistance` (`ch/queries.go:711-723`); the lake
port reimplements both as **pure-SQL math** — haversine (`aggregations.go:210-217`)
and an even-odd ray-cast unrolled per vertex (`aggregations.go:226-239`), with an
explicit antimeridian-breaks caveat (mirrors CH's own TODO at `ch/queries.go:699`),
no spatial index.

**DuckDB:** the `spatial` extension is **baked into the image** (`installext`) but the
runtime bootstrap (`duck.go` LOADs only httpfs/aws/ducklake/postgres) **never `LOAD
spatial`** — so `ST_*` is unavailable at runtime today (`grep ST_` → only comments).
The first step is adding `LOAD spatial` to the per-connection bootstrap (offline-safe,
binary pre-baked — but it's the hot per-connection path, so verify cost). Then:

```sql
WHERE ST_DWithin(ST_Point(loc_lon, loc_lat), ST_Point(:clon,:clat), :radius_m)  -- inCircle
WHERE ST_Contains(ST_GeomFromText(:wkt_polygon), ST_Point(loc_lon, loc_lat))     -- inPolygon
-- trip route length: ST_Length(ST_MakeLine(list(ST_Point(loc_lon,loc_lat) ORDER BY timestamp)))
```

**Value:** correct geodesic/polygon (incl. antimeridian, complex rings), ~50 fewer
lines of trig-string generation, and **trip route geometry + route distance** (a real
feature, complements #2; `ST_MakeLine`+`list()` showcases DuckDB list aggregates CH
lacks). **Effort:** M — add `LOAD spatial` to the bootstrap (NOT already done), then
swap the two predicates + parity tests. Trip-route geometry as a *field* hits the
no-schema constraint (#2); a `Signals[]`-style surface avoids it. **Keep the pure-SQL
path as a no-extension fallback.** **Parity:** improvement; pin tests —
distances differ at the mm/edge level.

---

## Secondary (worthwhile, lower leverage)

- **#5 Recharge odometer filter via ASOF/window.** `recharge.go` fetches a *second*
  full odometer stream just to reject sessions where the car moved. With #2/#3 this
  becomes an ASOF/`SUM(delta)` join in the same query — one round trip. **Effort S
  once #3 lands; parity-neutral.**
- **#6 Lean on the on-read latest path; the rollup is an optimization, not a
  correctness requirement.** CH *must* maintain `signal_latest_*` with byte-exact
  aggregate text (`ch/queries.go:46-48` — a whitespace change silently disables it).
  Lake has both an on-read path (`lake_latest.go`, exact `arg_max` over the deduped
  scan, O(distinct names), bucket-pruned) **and** the optional `signals_latest`
  rollup (`lake_rollup.go`). The rollup is pure speed and droppable; no projection-
  text footgun, no MV-seed compromises. **Effort: none new — architectural note.**
- **#7 Fold the ignition debounce + segment assembly into SQL.** The seed already
  moved to SQL (`c83d6b5`), but `filterNoise` + assembly still run in Go
  (`ignition.go:46-139`) — expressible with `LAG`/`LEAD` + gaps-and-islands,
  retiring the last app-side loop. **Effort M, parity-sensitive (subtle, well-tested
  — port carefully, keep CH on Go). Perf/cleanliness, not new capability.**

---

## Does NOT improve

**Change-point (CUSUM)** is an inherently sequential cumulative-sum — it belongs in
Go. Keep it; feed it the (now exact, #1) windowed counts.

---

## Parity framing

Everything here **diverges from CH** — the point of the exercise. Two flavors:
- **Strictly-better** (exact vs approximate median/distinct, deterministic vs
  tie-nondeterministic latest, true vs fabricated ignition state, faster geofencing):
  no product debate — just verify no consumer depended on the CH quirk, and
  **document** (#1 is already live and undocumented).
- **New features** (trip distance/route/speed, spatial routes): product calls — quick
  sign-off before building.

## Recommended order

1. **#1 (exact aggregations)** — recognize + document the *already-shipping*
   divergence before cutover so it isn't reverted. Cheap, urgent.
2. **#2 (ASOF trip enrichment)** — highest new-user-value; location gap-fill is S,
   distance/avg-speed is M. Prototype the enriched-trip SQL first.
3. **#3 (SQL-native idling/refuel/recharge)** — perf + the ASOF speed/odo correlation.
4. **#4 (spatial)** — pairs with #2 for route/distance.
5. **#5–#7** as cleanup once the above land.
