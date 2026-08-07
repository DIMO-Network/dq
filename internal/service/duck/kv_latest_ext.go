package duck

// Extended KV serving (dq#55 step 3): allLatest and availableSignals move onto
// the signals-latest cache before the rollup goes daily-stale at the flip.
// Both are answered from the SAME per-subject Entry the (already serving)
// signalsLatest path reads — allLatest is "every name in the entry",
// availableSignals is "the entry's key set" — so data parity is inherited
// from the proven fold; what these paths add is conversion code, which the
// shadow mode and the differential tests cover. Gated by kvExtMode
// (LATEST_KV_READ_MODE_EXTENDED) so the rollout gets its own off → shadow →
// serve ladder without touching signalsLatest serving.

import (
	"context"
	"sort"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/latestkv"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// WithLatestKVExtended enables KV serving for allLatest and availableSignals
// (dq#55 step 3). Requires WithLatestKV to have wired a store; mode off (the
// default) leaves both queries on the DuckLake paths. Returns q for chaining.
func (q *Queries) WithLatestKVExtended(mode KVReadMode) *Queries {
	q.kvExtMode = mode
	return q
}

// allSignalsFromKVEntry converts a cache entry into the same rows
// getAllLatestSignalsRollup returns: one row per stored name — a location fix
// wins the row's timestamp and carries the location value, exactly like
// querySignals' loc stamping over the rollup's loc_* columns — plus the
// virtual lastSeen row, which is emitted even for an empty entry (the
// rollup's UNION ALL aggregate emits one epoch row over zero rows, and the
// authoritative-miss answer must match it). Rows sorted by name, mirroring
// the ORDER BY.
func allSignalsFromKVEntry(entry *latestkv.Entry) []*vss.Signal {
	out := make([]*vss.Signal, 0, len(entry.Signals)+1)
	for name, v := range entry.Signals {
		s := &vss.Signal{}
		s.Data.Name = name
		s.Data.Timestamp = v.TS.UTC()
		s.Data.ValueNumber = v.Num
		s.Data.ValueString = v.Str
		if v.Loc != nil {
			s.Data.Timestamp = v.Loc.TS.UTC()
			s.Data.ValueLocation = vss.Location{Latitude: v.Loc.Lat, Longitude: v.Loc.Lon, HDOP: v.Loc.HDOP, Heading: v.Loc.Heading}
		}
		out = append(out, s)
	}
	ls := &vss.Signal{}
	ls.Data.Name = model.LastSeenField
	ls.Data.Timestamp = epochTime
	if last := entry.LastSeen(); !last.IsZero() {
		ls.Data.Timestamp = last.UTC()
	}
	out = append(out, ls)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Data.Name < out[j].Data.Name })
	return out
}

// availableSignalsFromKVEntry is the entry's sorted key set — the same names
// getAvailableSignalsRollup's SELECT DISTINCT returns. Nil for an empty entry
// (the rollup path also returns nil when the subject has no rows).
func availableSignalsFromKVEntry(entry *latestkv.Entry) []string {
	if len(entry.Signals) == 0 {
		return nil
	}
	names := make([]string, 0, len(entry.Signals))
	for name := range entry.Signals {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// attemptAllLatestKV answers GetAllLatestSignals from the cache where it can,
// with the same miss/negative semantics as attemptLatestSignalsKV: a hit is
// the answer, an authoritative miss serves the empty answer (the epoch
// lastSeen row), anything else falls back to the lake.
func (q *Queries) attemptAllLatestKV(ctx context.Context, subject string) kvAttempt {
	entry, outcome := q.readKVEntry(ctx, subject)
	if outcome == kvOutcomeMiss {
		switch q.negativeDisposition() {
		case negativeServe:
			kvReadTotal.WithLabelValues(string(kvOutcomeMissAuthoritative)).Inc()
			return kvAttempt{signals: allSignalsFromKVEntry(&latestkv.Entry{}), served: true}
		case negativeShadow:
			kvReadTotal.WithLabelValues(string(kvOutcomeMiss)).Inc()
			return kvAttempt{shadowNegative: true}
		}
	}
	kvReadTotal.WithLabelValues(string(outcome)).Inc()
	if outcome != kvOutcomeHit {
		return kvAttempt{}
	}
	return kvAttempt{signals: allSignalsFromKVEntry(entry), served: true}
}

// kvNamesAttempt is kvAttempt for the availableSignals shape.
type kvNamesAttempt struct {
	names          []string
	served         bool
	shadowNegative bool
}

// attemptAvailableSignalsKV answers GetAvailableSignals from the cache. An
// authoritative miss serves nil — the rollup's answer for a subject with no
// rows.
func (q *Queries) attemptAvailableSignalsKV(ctx context.Context, subject string) kvNamesAttempt {
	entry, outcome := q.readKVEntry(ctx, subject)
	if outcome == kvOutcomeMiss {
		switch q.negativeDisposition() {
		case negativeServe:
			kvReadTotal.WithLabelValues(string(kvOutcomeMissAuthoritative)).Inc()
			return kvNamesAttempt{served: true}
		case negativeShadow:
			kvReadTotal.WithLabelValues(string(kvOutcomeMiss)).Inc()
			return kvNamesAttempt{shadowNegative: true}
		}
	}
	kvReadTotal.WithLabelValues(string(outcome)).Inc()
	if outcome != kvOutcomeHit {
		return kvNamesAttempt{}
	}
	return kvNamesAttempt{names: availableSignalsFromKVEntry(entry), served: true}
}

// shadowCompareAllLatest runs the cache read next to an already-served rollup
// allLatest result and classifies agreement into kvExtShadowTotal — the dark
// launch for the allLatest move. classifyShadowDiff is shared with the
// signalsLatest shadow: same row shape, same benign kv_newer race.
func (q *Queries) shadowCompareAllLatest(ctx context.Context, subject string, rollup []*vss.Signal) {
	kvStart := time.Now()
	entry, outcome := q.readKVEntry(ctx, subject)
	lakeLatestQuerySeconds.WithLabelValues("kv_shadow", "allLatest").Observe(time.Since(kvStart).Seconds())
	kvReadTotal.WithLabelValues(string(outcome)).Inc()
	if outcome != kvOutcomeHit {
		if outcome == kvOutcomeMiss && len(rollup) > 1 && ctx.Err() == nil {
			// >1: the virtual lastSeen row alone is the empty answer, and an
			// empty subject legitimately has no cache key.
			kvExtShadowTotal.WithLabelValues("allLatest", "kv_miss").Inc()
		}
		return
	}
	kv := allSignalsFromKVEntry(entry)
	cls := classifyShadowDiff(rollup, kv)
	kvExtShadowTotal.WithLabelValues("allLatest", cls).Inc()
	if cls == "mismatch" {
		q.kvLog.Warn().Str("subject", subject).Int("rollup_rows", len(rollup)).Int("kv_rows", len(kv)).
			Msg("signals-latest cache disagrees with the rollup on allLatest (shadow); investigate before serve mode")
	}
}

// shadowCompareAvailable classifies the availableSignals shadow: identical
// name sets match; cache-only names are the benign freshness race (a reading
// landed after the rollup query); a rollup-only name is a mismatch.
func (q *Queries) shadowCompareAvailable(ctx context.Context, subject string, rollup []string) {
	kvStart := time.Now()
	entry, outcome := q.readKVEntry(ctx, subject)
	lakeLatestQuerySeconds.WithLabelValues("kv_shadow", "availableSignals").Observe(time.Since(kvStart).Seconds())
	kvReadTotal.WithLabelValues(string(outcome)).Inc()
	if outcome != kvOutcomeHit {
		if outcome == kvOutcomeMiss && len(rollup) > 0 && ctx.Err() == nil {
			kvExtShadowTotal.WithLabelValues("availableSignals", "kv_miss").Inc()
		}
		return
	}
	kvNames := map[string]struct{}{}
	for name := range entry.Signals {
		kvNames[name] = struct{}{}
	}
	missing := false
	for _, name := range rollup {
		if _, ok := kvNames[name]; !ok {
			missing = true
			break
		}
	}
	switch {
	case missing:
		kvExtShadowTotal.WithLabelValues("availableSignals", "mismatch").Inc()
		q.kvLog.Warn().Str("subject", subject).
			Msg("signals-latest cache is missing an availableSignals name the rollup has (shadow); investigate before serve mode")
	case len(kvNames) != len(rollup):
		kvExtShadowTotal.WithLabelValues("availableSignals", "kv_newer").Inc()
	default:
		kvExtShadowTotal.WithLabelValues("availableSignals", "match").Inc()
	}
}

// observeShadowNegativeNames is observeShadowNegative for the availableSignals
// shape: any name in the rollup answer is a false negative the cache would
// have hidden.
func (q *Queries) observeShadowNegativeNames(subject string, names []string) {
	if len(names) > 0 {
		kvFalseNegativeTotal.Inc()
		q.kvLog.Warn().Str("subject", subject).Int("rollup_names", len(names)).
			Msg("signals-latest cache would have answered 'no data' for a subject with availableSignals; do not flip LATEST_KV_NEGATIVE to serve")
		return
	}
	kvTrueNegativeTotal.Inc()
}
