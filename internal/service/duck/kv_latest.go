package duck

import (
	"context"
	"sort"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/latestkv"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/rs/zerolog"
)

// KVReadMode selects how GetLatestSignals uses the NATS KV signals-latest
// cache (phase 2 of serving signalsLatest off DuckLake; the write side is
// dq#26). The cache is a per-subject fold of the same decoded batches that
// feed lake.signals_latest, so a KV read answers the no-source-filter case in
// one sub-ms Get with no DuckLake planning (snapshot resolution + file
// listing, the ~0.5-1s floor on every rollup point-read) and no DuckDB pool
// connection.
type KVReadMode string

const (
	// KVReadOff ignores the cache entirely (default).
	KVReadOff KVReadMode = "off"
	// KVReadShadow serves from the rollup exactly as before but ALSO reads the
	// cache and compares, counting dq_lake_latest_kv_shadow_total — the dark
	// launch that proves per-query parity on real traffic before any user
	// request depends on the cache.
	KVReadShadow KVReadMode = "shadow"
	// KVReadServe answers from the cache, falling back to the rollup path on
	// any miss, error, or unknown entry version. NATS unavailability degrades
	// to today's latency, never to an error.
	KVReadServe KVReadMode = "serve"
)

// ParseKVReadMode validates a LATEST_KV_READ_MODE value; empty means off.
func ParseKVReadMode(s string) (KVReadMode, bool) {
	switch KVReadMode(s) {
	case "", KVReadOff:
		return KVReadOff, true
	case KVReadShadow:
		return KVReadShadow, true
	case KVReadServe:
		return KVReadServe, true
	}
	return KVReadOff, false
}

// kvReadTimeout bounds one cache Get so a NATS outage costs a query at most
// this before the rollup fallback (serve) or the comparison is skipped
// (shadow). Generous next to the sub-ms happy path; small next to the ~1s
// rollup read it fronts.
const kvReadTimeout = 1500 * time.Millisecond

// epochTime is the Go value of the SQL epochLiteral (make_timestamp(0)): what
// the rollup emits as the timestamp of a location row that has never had a
// nonzero fix, and of the virtual lastSeen row of a subject with no data.
var epochTime = time.Unix(0, 0).UTC()

// WithLatestKV wires the signals-latest cache into the latest-signals read
// path. mode off (or a nil store) leaves every query on the DuckLake paths.
// Returns q for chaining.
func (q *Queries) WithLatestKV(store *latestkv.Store, mode KVReadMode, log zerolog.Logger) *Queries {
	q.kvStore = store
	q.kvMode = mode
	q.kvLog = log.With().Str("component", "latestkv-read").Logger()
	return q
}

// getLatestSignalsKV answers GetLatestSignals from the cache. ok=false means
// "use the rollup path" for any reason — subject absent, unreadable entry,
// unknown schema version, NATS error — with the reason counted in
// dq_lake_latest_kv_read_total so a rising fallback rate is visible.
func (q *Queries) getLatestSignalsKV(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, bool) {
	entry, ok := q.readKVEntry(ctx, subject)
	if !ok {
		return nil, false
	}
	kvReadTotal.WithLabelValues("hit").Inc()
	return signalsFromKVEntry(entry, latestArgs), true
}

// readKVEntry fetches and vets the subject's cache entry (shared by serve and
// shadow). ok=false is always accompanied by a kvReadTotal outcome count.
func (q *Queries) readKVEntry(ctx context.Context, subject string) (*latestkv.Entry, bool) {
	ctx, cancel := context.WithTimeout(ctx, kvReadTimeout)
	defer cancel()
	entry, err := q.kvStore.GetEntry(ctx, subject)
	if err != nil {
		kvReadTotal.WithLabelValues("error").Inc()
		return nil, false
	}
	if entry == nil {
		kvReadTotal.WithLabelValues("miss").Inc()
		return nil, false
	}
	if entry.V != latestkv.EntryVersion {
		// A newer writer's schema: this reader can't interpret it — fall back
		// rather than guess (the version exists exactly for this).
		kvReadTotal.WithLabelValues("version").Inc()
		return nil, false
	}
	return entry, true
}

// signalsFromKVEntry converts a cache entry into the same rows
// getLatestSignalsRollup would return for latestArgs — semantics pinned to the
// SQL (and to querySignals' scanning rules) case by case:
//   - named signals: the value part verbatim; no location on the row.
//   - location names: served from the entry's per-name fix — the value is the
//     fix (lat/lon/hdop/heading) stamped with the FIX time, exactly like the
//     rollup's loc_ts (H9). A name with no nonzero fix ever gets the epoch
//     timestamp and a zero location, matching coalesce(loc_ts, epoch) +
//     querySignals keeping ts when lat/lon are zero.
//   - a name requested as BOTH gets two rows (the SQL UNION ALL does too).
//   - IncludeLastSeen: the virtual row at max(value ts) over ALL names in the
//     entry (== max(last_seen) over the subject's rollup rows).
//
// Rows are sorted by name, mirroring the ORDER BY.
func signalsFromKVEntry(entry *latestkv.Entry, latestArgs *model.LatestSignalsArgs) []*vss.Signal {
	out := make([]*vss.Signal, 0, len(latestArgs.SignalNames)+len(latestArgs.LocationSignalNames)+1)
	for name := range latestArgs.SignalNames {
		v, ok := entry.Signals[name]
		if !ok {
			continue // the rollup has no row for an unseen name either
		}
		s := &vss.Signal{}
		s.Data.Name = name
		s.Data.Timestamp = v.TS.UTC()
		s.Data.ValueNumber = v.Num
		s.Data.ValueString = v.Str
		out = append(out, s)
	}
	for name := range latestArgs.LocationSignalNames {
		v, ok := entry.Signals[name]
		if !ok {
			continue
		}
		s := &vss.Signal{}
		s.Data.Name = name
		if v.Loc != nil {
			s.Data.Timestamp = v.Loc.TS.UTC()
			s.Data.ValueLocation = vss.Location{Latitude: v.Loc.Lat, Longitude: v.Loc.Lon, HDOP: v.Loc.HDOP, Heading: v.Loc.Heading}
		} else {
			s.Data.Timestamp = epochTime
		}
		out = append(out, s)
	}
	if latestArgs.IncludeLastSeen {
		s := &vss.Signal{}
		s.Data.Name = model.LastSeenField
		s.Data.Timestamp = epochTime
		if last := entry.LastSeen(); !last.IsZero() {
			s.Data.Timestamp = last.UTC()
		}
		out = append(out, s)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Data.Name < out[j].Data.Name })
	return out
}

// shadowCompareLatest runs the cache read next to an already-served rollup
// result and classifies the agreement into dq_lake_latest_kv_shadow_total.
// Serving is never affected. Result classes:
//   - match:    identical rows (name, µs-truncated timestamp, values, location)
//   - kv_newer: rows differ but every differing name is NEWER in the cache —
//     the benign freshness race (the cache folds BEFORE the catalog commit,
//     and a reading can land between the rollup query and the cache Get)
//   - mismatch: a genuine divergence — logged with the subject, this is the
//     signal that blocks the serve cutover
//   - kv_miss:  the rollup had rows but the subject is absent from the cache
//   - kv_error / kv_version: the cache was unreadable; nothing to compare
func (q *Queries) shadowCompareLatest(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs, rollup []*vss.Signal) {
	entry, ok := q.readKVEntry(ctx, subject)
	if !ok {
		if len(rollup) > 0 {
			// Distinguish "cache never saw this subject" from transport errors:
			// readKVEntry already counted error/version; only a true miss with
			// rollup data present is a coverage gap worth its own class.
			if ctx.Err() == nil {
				kvShadowTotal.WithLabelValues("kv_miss").Inc()
			}
		}
		return
	}
	kv := signalsFromKVEntry(entry, latestArgs)
	if cls := classifyShadowDiff(rollup, kv); cls != "match" {
		kvShadowTotal.WithLabelValues(cls).Inc()
		if cls == "mismatch" {
			q.kvLog.Warn().Str("subject", subject).Int("rollup_rows", len(rollup)).Int("kv_rows", len(kv)).
				Msg("signals-latest cache disagrees with the rollup (shadow); investigate before serve mode")
		}
		return
	}
	kvShadowTotal.WithLabelValues("match").Inc()
}

// classifyShadowDiff compares the two row sets. Timestamps are compared at
// microsecond precision: the lake stores µs while live cache folds carry the
// decoder's ns, so sub-µs tails are representation, not divergence.
func classifyShadowDiff(rollup, kv []*vss.Signal) string {
	byName := func(rows []*vss.Signal) map[string]*vss.Signal {
		m := make(map[string]*vss.Signal, len(rows))
		for _, r := range rows {
			m[r.Data.Name] = r
		}
		return m
	}
	rm, km := byName(rollup), byName(kv)
	kvNewerOnly := true
	diff := false
	for name, r := range rm {
		k, ok := km[name]
		if !ok {
			return "mismatch" // rollup row missing from the cache is never a freshness race
		}
		rts, kts := r.Data.Timestamp.Truncate(time.Microsecond), k.Data.Timestamp.Truncate(time.Microsecond)
		if rts.Equal(kts) && r.Data.ValueNumber == k.Data.ValueNumber && r.Data.ValueString == k.Data.ValueString &&
			r.Data.ValueLocation == k.Data.ValueLocation {
			continue
		}
		diff = true
		if !kts.After(rts) {
			kvNewerOnly = false
		}
	}
	for name := range km {
		if _, ok := rm[name]; !ok {
			// A cache-only row: the reading landed after the rollup query saw
			// the subject — newer, not divergent.
			diff = true
		}
	}
	switch {
	case !diff:
		return "match"
	case kvNewerOnly:
		return "kv_newer"
	default:
		return "mismatch"
	}
}
