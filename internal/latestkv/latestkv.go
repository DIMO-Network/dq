// Package latestkv maintains a NATS JetStream KV bucket mirroring
// lake.signals_latest, keyed per subject — the serving path for signalsLatest
// that skips DuckLake entirely (SR-7 follow-up). Even a rollup point-read pays
// per-query DuckLake planning (snapshot resolution against the Postgres
// catalog + parquet file listing), a fixed ~0.5–1s floor that no rollup tuning
// removes; a KV Get is sub-ms and holds no DuckDB pool connection.
//
// The bucket is a CACHE of the lake, never the source of truth: the writer
// (the single materializer, via materializer.LatestPublisher) folds each
// decoded batch in last-write-wins by (timestamp DESC, cloud_event_id ASC) —
// the exact recency order foldSignalsRollup uses — so publishes are idempotent
// under NATS redelivery, window replay, and backfill. A lost update (KV outage,
// crash) heals per (subject, name) on that signal's next reading, or wholesale
// via BootstrapFromRollup. Readers must treat a miss or an unreachable bucket
// as "fall back to the rollup path", never as an error.
package latestkv

import (
	"encoding/base64"
	"time"
)

// EntryVersion is the current Entry schema version. Bump on any
// backward-incompatible change to the JSON shape; readers must ignore entries
// with a version they don't know (and fall back to the rollup).
const EntryVersion = 1

// Row is one decoded signal reading, the publish-side input. It mirrors the
// fields of materializer.SignalRow the latest-value fold needs; the app layer
// maps between the two so this package depends on neither the materializer nor
// parquet.
type Row struct {
	Subject      string
	Name         string
	Timestamp    time.Time
	CloudEventID string
	ValueNumber  float64
	ValueString  string
	LocLat       float64
	LocLon       float64
	LocHDOP      float64
	LocHeading   float64
}

// Entry is the KV value for one subject: the latest reading per signal name,
// sources folded — the same shape as that subject's lake.signals_latest rows.
type Entry struct {
	V       int                    `json:"v"`
	Signals map[string]SignalValue `json:"signals"`
}

// SignalValue is the latest reading for one signal name. The value part
// (TS/Num/Str) and the location part (Loc) advance independently, exactly like
// the rollup's timestamp/value_* vs loc_*/loc_ts columns: Loc only ever moves
// to a nonzero (lat, lon) fix, so a trailing (0,0) reading updates the value
// part but keeps the last real fix (H9).
type SignalValue struct {
	TS time.Time `json:"ts"`
	// CEID is the cloud_event_id of the winning row, kept only to break
	// equal-timestamp ties the same way the rollup's ORDER BY timestamp DESC,
	// cloud_event_id ASC does. Empty (bootstrap rows) loses no information:
	// the bootstrapped value IS the rollup's tie-broken winner already.
	CEID string    `json:"ceid,omitempty"`
	Num  float64   `json:"num"`
	Str  string    `json:"str,omitempty"`
	Loc  *LocValue `json:"loc,omitempty"`
}

// LocValue is the latest nonzero-fix location for one signal name; TS is the
// fix time (the rollup's loc_ts), not the value part's timestamp.
type LocValue struct {
	TS      time.Time `json:"ts"`
	CEID    string    `json:"ceid,omitempty"`
	Lat     float64   `json:"lat"`
	Lon     float64   `json:"lon"`
	HDOP    float64   `json:"hdop,omitempty"`
	Heading float64   `json:"heading,omitempty"`
}

// subjectKeyPrefix namespaces subject entries away from meta keys (the
// bootstrap marker) so an encoded subject can never collide with one.
const subjectKeyPrefix = "s."

// KeyForSubject encodes a subject DID as a NATS KV key. Subjects contain
// colons, which the KV key charset ([-/_=.a-zA-Z0-9]) forbids, so the subject
// is base64url-encoded (its alphabet is a subset of the allowed set). Both the
// writer and the phase-2 reader must use this — the encoding is the contract.
func KeyForSubject(subject string) string {
	return subjectKeyPrefix + base64.RawURLEncoding.EncodeToString([]byte(subject))
}

// Fold merges one decoded reading into the entry, last-write-wins, and reports
// whether anything changed (false ⇒ the caller can skip the Put — this is what
// makes redelivered/replayed batches free). A live reading's fix time is its
// own timestamp, so the location part folds with the same (ts, ceid) as the
// value part.
func (e *Entry) Fold(r Row) bool {
	sv := SignalValue{TS: r.Timestamp, CEID: r.CloudEventID, Num: r.ValueNumber, Str: r.ValueString}
	if r.LocLat != 0 || r.LocLon != 0 {
		sv.Loc = &LocValue{TS: r.Timestamp, CEID: r.CloudEventID, Lat: r.LocLat, Lon: r.LocLon, HDOP: r.LocHDOP, Heading: r.LocHeading}
	}
	return e.FoldValue(r.Name, sv)
}

// FoldValue merges an already-shaped SignalValue (a live reading via Fold, or
// a rollup row during bootstrap, whose loc TS differs from the value TS) into
// the entry. The value part and the location part win independently under
// newerThan, mirroring the rollup's independent recency/locrec folds.
func (e *Entry) FoldValue(name string, in SignalValue) bool {
	cur, ok := e.Signals[name]
	changed := false
	if !ok || newerThan(in.TS, in.CEID, cur.TS, cur.CEID) {
		cur.TS, cur.CEID, cur.Num, cur.Str = in.TS, in.CEID, in.Num, in.Str
		changed = true
	}
	if in.Loc != nil && (cur.Loc == nil || newerThan(in.Loc.TS, in.Loc.CEID, cur.Loc.TS, cur.Loc.CEID)) {
		loc := *in.Loc
		cur.Loc = &loc
		changed = true
	}
	if changed {
		if e.Signals == nil {
			e.Signals = make(map[string]SignalValue)
		}
		e.Signals[name] = cur
	}
	return changed
}

// LastSeen is the max value-part timestamp across all names — the same value
// as max(last_seen) over the subject's rollup rows (the virtual lastSeen row).
func (e *Entry) LastSeen() time.Time {
	var last time.Time
	for _, sv := range e.Signals {
		if sv.TS.After(last) {
			last = sv.TS
		}
	}
	return last
}

// newerThan reports whether (ts, ceid) beats (oldTS, oldCEID) under the
// rollup's recency order: ORDER BY timestamp DESC, cloud_event_id ASC. On an
// exact timestamp tie the LEXICOGRAPHICALLY SMALLER cloud_event_id wins —
// matching foldSignalsRollup/rollupSelectSQL so the KV and the rollup pick the
// same winner and the phase-2 fallback path can't flap between two values.
func newerThan(ts time.Time, ceid string, oldTS time.Time, oldCEID string) bool {
	if ts.After(oldTS) {
		return true
	}
	return ts.Equal(oldTS) && ceid < oldCEID
}
