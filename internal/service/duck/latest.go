package duck

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// GetLatestSignals returns the latest value for the requested signal names for
// the subject:
//
//   - non-location names: max(timestamp), arg_max(value_*) over all sources
//   - location names: arg_max excluding (0, 0) fixes
//   - IncludeLastSeen adds the virtual lastSeen row (max timestamp over all
//     signals, per (subject, source) under model.LastSeenField)
func (q *Queries) GetLatestSignals(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	// The rollup serves named latest — including location names, whose
	// (0,0)-filtered latest-fix timestamp is stored as loc_ts (H9) — in
	// O(distinct-names). Only source-filtered queries fall back to the full
	// deduped scan (SR-5): the rollup folds sources by construction.
	rollup := noSourceFilter(latestArgs.Filter)
	// The NATS KV cache serves the same no-source-filter case with no DuckLake
	// planning at all (kv_latest.go). Source-filtered queries bypass it exactly
	// as they bypass the rollup; an empty request keeps the paths' shared
	// nothing-to-do answer.
	kvEligible := rollup && q.kvStore != nil &&
		(len(latestArgs.SignalNames) > 0 || len(latestArgs.LocationSignalNames) > 0 || latestArgs.IncludeLastSeen)
	// shadowNegative survives the KV block so the rollup result below can be
	// checked against the answer negative-serving WOULD have given.
	var shadowNegative bool
	if kvEligible && q.kvMode == KVReadServe {
		kvStart := time.Now()
		att := q.attemptLatestSignalsKV(ctx, subject, latestArgs)
		shadowNegative = att.shadowNegative
		if att.served {
			// An authoritative miss is served under path="kv" like any other cache
			// answer: from the caller's side it IS a cache hit — the same sub-ms
			// answer off the same Get. The hit/no-data split lives in
			// dq_lake_latest_kv_read_total, where it belongs.
			lakeLatestServedTotal.WithLabelValues("kv").Inc()
			lakeLatestQuerySeconds.WithLabelValues("kv", "signalsLatest").Observe(time.Since(kvStart).Seconds())
			return att.signals, nil
		}
		// miss/error/version: counted in kvReadTotal; serve from the rollup. Time the
		// FAILED attempt too, under its own path. Charging it nowhere (the rollup
		// timer below starts after this block) meant the one case where the cache
		// hurts — a NATS stall burning up to kvReadTimeout before the fallback even
		// starts — was the one case this histogram could not see: path="kv" records
		// hits only, so the panel went quiet exactly when reads got slowest. A
		// request's true cost is kv_fallback + rollup.
		lakeLatestQuerySeconds.WithLabelValues("kv_fallback", "signalsLatest").Observe(time.Since(kvStart).Seconds())
	}
	observeLakePath(rollup)
	defer observeLakeQuery(rollup, "signalsLatest", time.Now())
	if rollup {
		signals, err := q.getLatestSignalsRollup(ctx, subject, latestArgs)
		if err == nil && kvEligible && q.kvMode == KVReadShadow {
			q.shadowCompareLatest(ctx, subject, latestArgs, signals)
		}
		if err == nil && shadowNegative {
			q.observeShadowNegative(subject, signals)
		}
		return signals, err
	}
	return q.getLatestSignalsLake(ctx, subject, latestArgs)
}

// GetAllLatestSignals returns the latest value for every signal name stored
// for the subject, plus the virtual lastSeen row: the timestamp is the
// unconditional max(timestamp) while the location value comes from the nonzero
// columns.
func (q *Queries) GetAllLatestSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]*vss.Signal, error) {
	rollup := noSourceFilter(filter)
	// The KV serves the no-source-filter case from the same Entry the
	// signalsLatest path reads (dq#55 step 3) — allLatest is "every name in
	// the entry" plus the virtual lastSeen row. Source-filtered queries bypass
	// it exactly as they bypass the rollup.
	kvEligible := rollup && q.kvStore != nil
	var shadowNegative bool
	if kvEligible && q.kvExtMode == KVReadServe {
		kvStart := time.Now()
		att := q.attemptAllLatestKV(ctx, subject)
		shadowNegative = att.shadowNegative
		if att.served {
			lakeLatestServedTotal.WithLabelValues("kv").Inc()
			lakeLatestQuerySeconds.WithLabelValues("kv", "allLatest").Observe(time.Since(kvStart).Seconds())
			return att.signals, nil
		}
		lakeLatestQuerySeconds.WithLabelValues("kv_fallback", "allLatest").Observe(time.Since(kvStart).Seconds())
	}
	observeLakePath(rollup)
	defer observeLakeQuery(rollup, "allLatest", time.Now())
	if rollup {
		signals, err := q.getAllLatestSignalsRollup(ctx, subject) // O(distinct-names) rollup (CHD-3)
		if err == nil && kvEligible && q.kvExtMode == KVReadShadow {
			q.shadowCompareAllLatest(ctx, subject, signals)
		}
		if err == nil && shadowNegative {
			q.observeShadowNegative(subject, signals)
		}
		return signals, err
	}
	return q.getAllLatestSignalsLake(ctx, subject, filter)
}

// GetAvailableSignals returns the distinct signal names stored for a subject,
// sorted ascending. Returns nil when none.
func (q *Queries) GetAvailableSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]string, error) {
	rollup := noSourceFilter(filter)
	// KV: the entry's key set IS the available-names answer (dq#55 step 3).
	kvEligible := rollup && q.kvStore != nil
	var shadowNegative bool
	if kvEligible && q.kvExtMode == KVReadServe {
		kvStart := time.Now()
		att := q.attemptAvailableSignalsKV(ctx, subject)
		shadowNegative = att.shadowNegative
		if att.served {
			lakeLatestServedTotal.WithLabelValues("kv").Inc()
			lakeLatestQuerySeconds.WithLabelValues("kv", "availableSignals").Observe(time.Since(kvStart).Seconds())
			return att.names, nil
		}
		lakeLatestQuerySeconds.WithLabelValues("kv_fallback", "availableSignals").Observe(time.Since(kvStart).Seconds())
	}
	observeLakePath(rollup)
	defer observeLakeQuery(rollup, "availableSignals", time.Now())
	if rollup {
		names, err := q.getAvailableSignalsRollup(ctx, subject) // rollup (CHD-3)
		if err == nil && kvEligible && q.kvExtMode == KVReadShadow {
			q.shadowCompareAvailable(ctx, subject, names)
		}
		if err == nil && shadowNegative {
			q.observeShadowNegativeNames(subject, names)
		}
		return names, err
	}
	return q.getAvailableSignalsLake(ctx, subject, filter)
}

// GetSignalSummaries returns per-name signal counts and first/last seen
// timestamps for a subject, aggregated across sources.
func (q *Queries) GetSignalSummaries(ctx context.Context, subject string, filter *model.SignalFilter) ([]*model.SignalDataSummary, error) {
	rollup := noSourceFilter(filter)
	observeLakePath(rollup)
	defer observeLakeQuery(rollup, "signalSummaries", time.Now())
	if rollup {
		// Under the daily-refreshed rollup (dq#55 step 4), the exact answer is
		// the (rollup ∪ tail-since-watermark) union — see lake_rollup_union.go.
		// A zero watermark means no daily refresh has ever run; the plain
		// rollup read is then exact as before.
		if q.dailyServing {
			w, err := q.cachedRollupWatermark(ctx)
			if err != nil {
				return nil, err
			}
			if !w.IsZero() {
				return q.getSignalSummariesUnion(ctx, subject, w)
			}
		}
		return q.getSignalSummariesRollup(ctx, subject) // rollup (CHD-3)
	}
	return q.getSignalSummariesLake(ctx, subject, filter)
}

// scanSignalSummary scans one summary row (name, count, first_seen, last_seen)
// and normalizes both timestamps to UTC. The column order is shared by every
// signal-summary query (lake, rollup), so it lives in one place.
func scanSignalSummary(rows rowScanner) (*model.SignalDataSummary, error) {
	var s model.SignalDataSummary
	if err := rows.Scan(&s.Name, &s.NumberOfSignals, &s.FirstSeen, &s.LastSeen); err != nil {
		return nil, err
	}
	s.FirstSeen = s.FirstSeen.UTC()
	s.LastSeen = s.LastSeen.UTC()
	return &s, nil
}

// querySignals runs a signal-shaped query (name, ts, value_number,
// value_string, loc_lat, loc_lon, loc_hdop, loc_heading, loc_ts) and scans rows
// into vss.Signal values. Every SELECT composed into stmt MUST emit these nine
// columns in this order (epoch loc_ts on non-location rows) — the positional
// Scan below silently mis-reads otherwise.
func (q *Queries) querySignals(ctx context.Context, stmt string, args []any) ([]*vss.Signal, error) {
	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	signals := []*vss.Signal{}
	for rows.Next() {
		var signal vss.Signal
		var ts, locTS time.Time
		var loc vss.Location
		err := rows.Scan(&signal.Data.Name, &ts, &signal.Data.ValueNumber, &signal.Data.ValueString,
			&loc.Latitude, &loc.Longitude, &loc.HDOP, &loc.Heading, &locTS)
		if err != nil {
			return nil, fmt.Errorf("failed scanning duckdb row: %w", err)
		}
		// A location reading (nonzero fix) carries the fix time in loc_ts (Item 2):
		// the (0,0)-filtered latest-fix timestamp, NOT the row's unfiltered
		// max(timestamp), which a trailing (0,0) reading would push past the last real
		// fix. Non-location rows carry loc_ts = epoch and keep ts. This matches the
		// GetLatestSignals location semantics, so GetAllLatestSignals agrees with it.
		signal.Data.Timestamp = ts.UTC()
		if loc.Latitude != 0 || loc.Longitude != 0 {
			signal.Data.Timestamp = locTS.UTC()
		}
		signal.Data.ValueLocation = loc
		signals = append(signals, &signal)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb row error: %w", rows.Err())
	}
	return signals, nil
}

func mapKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
