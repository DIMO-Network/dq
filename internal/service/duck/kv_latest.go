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

// KVNegativeMode selects whether a cache MISS may be served as an
// authoritative "this subject has no data" instead of falling back to the
// rollup (dq#42). Independent of KVReadMode because the two rest on different
// guarantees: a hit is self-evidently an answer, while a miss is only an answer
// while the writer's coverage assertion is live (internal/latestkv/coverage.go).
type KVNegativeMode string

const (
	// KVNegativeOff falls back to the rollup on every miss (today's behavior).
	KVNegativeOff KVNegativeMode = "off"
	// KVNegativeShadow still serves misses from the rollup, but counts how often
	// a miss the reader WOULD have answered as "no data" turned out to have
	// rollup rows — dq_lake_latest_kv_false_negative_total. That counter is the
	// production evidence for the invariant; it must stay flat at zero before
	// flipping to serve.
	KVNegativeShadow KVNegativeMode = "shadow"
	// KVNegativeServe answers a trusted miss directly, with no DuckLake read.
	KVNegativeServe KVNegativeMode = "serve"
)

// ParseKVNegativeMode validates a LATEST_KV_NEGATIVE value; empty means off.
func ParseKVNegativeMode(s string) (KVNegativeMode, bool) {
	switch KVNegativeMode(s) {
	case "", KVNegativeOff:
		return KVNegativeOff, true
	case KVNegativeShadow:
		return KVNegativeShadow, true
	case KVNegativeServe:
		return KVNegativeServe, true
	}
	return KVNegativeOff, false
}

// kvReadTimeout bounds one cache Get before the rollup fallback (serve) or
// before the comparison is skipped (shadow).
//
// Sized BELOW the rollup read it fronts, not above it. A Get still outstanding
// once the fallback would already have answered cannot win, so every further
// millisecond of waiting is pure loss added to the request's total cost. The
// previous 1500ms was set against an estimated "~1s rollup"; the rollup path
// measures 795ms (GCP node, 2026-07-29), so a NATS stall cost the full 1.5s
// timeout PLUS the 795ms fallback — ~2.3s, three times worse than having no
// cache at all. dq_lake_latest_query_seconds{path="kv_fallback"} was added in
// dq#31 to make exactly that visible, and it caught it (dq#41).
//
// 200ms is still ~75x the observed 2.6ms happy-path Get.
const kvReadTimeout = 200 * time.Millisecond

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

// WithLatestKVNegative enables authoritative-miss serving (dq#42). watcher
// carries the writer's coverage assertion; a nil watcher or mode off leaves
// every miss on the rollup fallback. Returns q for chaining.
func (q *Queries) WithLatestKVNegative(watcher *latestkv.CoverageWatcher, mode KVNegativeMode) *Queries {
	q.kvCoverage = watcher
	q.kvNegMode = mode
	return q
}

// kvOutcome is the classification of one cache read, and the label value
// counted into dq_lake_latest_kv_read_total.
type kvOutcome string

const (
	kvOutcomeHit     kvOutcome = "hit"
	kvOutcomeMiss    kvOutcome = "miss"
	kvOutcomeError   kvOutcome = "error"
	kvOutcomeVersion kvOutcome = "version"
	// kvOutcomeMissAuthoritative is a miss answered as "no data" on the strength
	// of live coverage — split from plain miss so the panel distinguishes "the
	// cache answered instantly" from "the cache sent us to DuckLake".
	kvOutcomeMissAuthoritative kvOutcome = "miss_authoritative"
)

// kvAttempt is the result of consulting the cache for one query.
type kvAttempt struct {
	// signals is the answer when served is true. Nil for an authoritative miss
	// with no lastSeen requested — which is itself the correct answer.
	signals []*vss.Signal
	// served means the cache answered completely; the caller must not read the
	// lake.
	served bool
	// shadowNegative marks a miss that WOULD have been served as "no data" had
	// LATEST_KV_NEGATIVE been serve. The caller runs the rollup anyway and, if it
	// comes back with rows, counts a false negative — the evidence that gates the
	// serve flip.
	shadowNegative bool
}

// attemptLatestSignalsKV answers GetLatestSignals from the cache where it can.
// served=false means "read the lake", with the reason counted in
// dq_lake_latest_kv_read_total so a rising fallback rate stays visible.
func (q *Queries) attemptLatestSignalsKV(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) kvAttempt {
	entry, outcome := q.readKVEntry(ctx, subject)
	if outcome == kvOutcomeMiss {
		// The bucket mirrors lake.signals_latest, so under live coverage a missing
		// key is not "I don't know" — it is the same empty answer the rollup would
		// spend ~795ms producing. An empty entry converts to exactly that: no named
		// rows, no location rows, and an epoch lastSeen row, which is what
		// coalesce(max(last_seen), epoch) over zero rows returns.
		switch q.negativeDisposition() {
		case negativeServe:
			kvReadTotal.WithLabelValues(string(kvOutcomeMissAuthoritative)).Inc()
			return kvAttempt{signals: signalsFromKVEntry(&latestkv.Entry{}, latestArgs), served: true}
		case negativeShadow:
			kvReadTotal.WithLabelValues(string(kvOutcomeMiss)).Inc()
			return kvAttempt{shadowNegative: true}
		}
	}
	kvReadTotal.WithLabelValues(string(outcome)).Inc()
	if outcome != kvOutcomeHit {
		return kvAttempt{}
	}
	return kvAttempt{signals: signalsFromKVEntry(entry, latestArgs), served: true}
}

// negativeDisposition is what this query may do with a miss right now.
type negativeDisposition int

const (
	negativeFallback negativeDisposition = iota // read the lake, as always
	negativeShadow                              // read the lake, but count the would-be answer
	negativeServe                               // answer "no data" from the miss
)

// negativeDisposition combines the configured mode with the LIVE coverage
// assertion. Untrusted coverage — writer down, degraded, heartbeat stale, NATS
// unreachable, bucket recreated, or a bucket TTL that could expire entries —
// always yields negativeFallback, i.e. exactly the behavior before dq#42.
func (q *Queries) negativeDisposition() negativeDisposition {
	if q.kvNegMode == KVNegativeOff || q.kvCoverage == nil {
		return negativeFallback
	}
	trusted := q.kvCoverage.Trusted()
	kvCoverageTrusted.Set(boolToFloat(trusted))
	if !trusted {
		return negativeFallback
	}
	if q.kvNegMode == KVNegativeServe {
		return negativeServe
	}
	return negativeShadow
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// readKVEntry fetches and vets the subject's cache entry (shared by serve and
// shadow). The entry is non-nil only for kvOutcomeHit. Counting is left to the
// caller: a miss means different things to the negative path and the shadow
// comparison, and only the caller knows which.
func (q *Queries) readKVEntry(ctx context.Context, subject string) (*latestkv.Entry, kvOutcome) {
	ctx, cancel := context.WithTimeout(ctx, kvReadTimeout)
	defer cancel()
	entry, err := q.kvStore.GetEntry(ctx, subject)
	if err != nil {
		return nil, kvOutcomeError
	}
	if entry == nil {
		return nil, kvOutcomeMiss
	}
	if entry.V != latestkv.EntryVersion {
		// A newer writer's schema: this reader can't interpret it — fall back
		// rather than guess (the version exists exactly for this).
		return nil, kvOutcomeVersion
	}
	return entry, kvOutcomeHit
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

// observeShadowNegative closes the loop on a LATEST_KV_NEGATIVE=shadow miss:
// the rollup has now answered the query the cache would have answered "no data",
// so any rows it returned are a FALSE NEGATIVE — the invariant (every subject in
// lake.signals_latest has a key in the bucket) failing on real traffic.
//
// This is the one counter that gates the serve flip. Unlike the shadow read
// comparison, a hit here is not a benign freshness race in either direction: the
// cache folds BEFORE the catalog commit, so a subject the lake knows about and
// the cache does not cannot be explained by the cache being behind.
//
// A rollup answer of only the virtual lastSeen row at the epoch is NOT a false
// negative — that is the empty answer, identical to what the cache would have
// returned.
func (q *Queries) observeShadowNegative(subject string, rollup []*vss.Signal) {
	for _, s := range rollup {
		if s.Data.Name == model.LastSeenField && s.Data.Timestamp.Equal(epochTime) {
			continue
		}
		kvFalseNegativeTotal.Inc()
		q.kvLog.Warn().Str("subject", subject).Int("rollup_rows", len(rollup)).
			Msg("signals-latest cache would have answered 'no data' for a subject the rollup has rows for; do not flip LATEST_KV_NEGATIVE to serve")
		return
	}
	kvTrueNegativeTotal.Inc()
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
	// Time the cache read under its own path label so the dark launch also
	// measures the latency claim: dq_lake_latest_query_seconds{path="kv_shadow"}
	// vs {path="rollup"} on the SAME real queries is the before/after evidence
	// for the serve flip. Distinct from "kv" so serve-mode series stay clean.
	kvStart := time.Now()
	entry, outcome := q.readKVEntry(ctx, subject)
	lakeLatestQuerySeconds.WithLabelValues("kv_shadow", "signalsLatest").Observe(time.Since(kvStart).Seconds())
	kvReadTotal.WithLabelValues(string(outcome)).Inc()
	if outcome != kvOutcomeHit {
		if outcome == kvOutcomeMiss && len(rollup) > 0 {
			// Distinguish "cache never saw this subject" from transport errors:
			// error/version are counted above; only a true miss with rollup data
			// present is a coverage gap worth its own class.
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
