package latestkv

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// DefaultBucket is the KV bucket name when LATEST_KV_BUCKET is unset. The
// phase-2 reader and the writer must agree on it (both default here).
const DefaultBucket = "signals-latest"

// bootstrapMarkerKey records that BootstrapFromRollup completed (value: the
// RFC3339 completion time). Lives outside the subjectKeyPrefix namespace so it
// can never collide with an encoded subject.
const bootstrapMarkerKey = "meta.bootstrap"

// publishConcurrency bounds concurrent per-subject Get+Put round trips within
// one batch publish. A fleet-wide batch touches many subjects; unbounded
// fan-out would open that many in-flight requests against NATS at once.
const publishConcurrency = 8

// Store wraps the JetStream KV bucket. Constructed only by processes that
// actually publish (the materializer release) — like dq_materializer_*, the
// dq_latest_kv_* series must not appear on query-fleet pods until phase 2
// gives the reader its own series.
type Store struct {
	conn *nats.Conn
	kv   jetstream.KeyValue
	log  zerolog.Logger
	// bucketTTL is the bucket's configured MaxAge, read once at open. Nonzero
	// means entries expire, which invalidates the coverage contract's premise
	// that a missing key proves the subject never had data (see coverage.go).
	bucketTTL time.Duration
}

// Open connects to NATS and ensures the bucket exists. The connection retries
// forever (like din's clients): a NATS outage must degrade to missed cache
// updates, never a crash-looping materializer.
func Open(ctx context.Context, url, bucket string, log zerolog.Logger) (*Store, error) {
	conn, err := nats.Connect(url,
		nats.Name("dq-latest-kv"),
		nats.MaxReconnects(-1),
		// Without these the retry loop is completely silent: a reconnect (or a long
		// disconnect) surfaces only as query latency on the serve path, with nothing
		// in the logs to correlate against. Reconnects should be rare — and when they
		// are not, that is exactly the thing worth knowing.
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn().Err(err).Msg("latest-kv NATS connection lost; latest reads fall back to the rollup until it returns")
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Info().Str("url", c.ConnectedUrl()).Msg("latest-kv NATS reconnected")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", url, err)
	}
	s, err := NewWithConn(ctx, conn, bucket, log)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return s, nil
}

// NewWithConn builds a Store over an existing connection (tests use an
// in-process server). The caller keeps ownership of conn on error; Close
// closes it on success.
func NewWithConn(ctx context.Context, conn *nats.Conn, bucket string, log zerolog.Logger) (*Store, error) {
	registerMetrics()
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	kv, err := lookupOrCreateBucket(ctx, js, bucket)
	if err != nil {
		return nil, err
	}
	s := &Store{conn: conn, kv: kv, log: log.With().Str("component", "latestkv").Logger()}
	// Read the TTL once here rather than per read: it is bucket config, it
	// changes only by operator action, and the coverage watcher needs it to
	// decide whether negative serving is admissible at all. A status read that
	// fails leaves it zero — the same as "no TTL" — so this can never be the
	// reason the cache stops working; the watcher's own staleness check still
	// gates everything.
	if status, err := kv.Status(ctx); err != nil {
		s.log.Warn().Err(err).Msg("could not read KV bucket status; assuming no TTL")
	} else {
		s.bucketTTL = status.TTL()
	}
	return s, nil
}

// lookupOrCreateBucket returns the bucket, creating it only when absent —
// never CreateOrUpdate, which would silently revert operator-applied config
// (e.g. a raised replica count on a clustered NATS) on every boot.
func lookupOrCreateBucket(ctx context.Context, js jetstream.JetStream, bucket string) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, bucket)
	if err == nil {
		return kv, nil
	}
	if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("looking up KV bucket %q: %w", bucket, err)
	}
	kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      bucket,
		Description: "dq signals-latest cache (rebuildable from lake.signals_latest)",
		// History 1: only the latest entry matters and the fold re-reads it
		// anyway; deeper history just multiplies the stream size.
		History: 1,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("creating KV bucket %q: %w", bucket, err)
	}
	return kv, nil
}

// Close drains the NATS connection.
func (s *Store) Close() {
	s.conn.Close()
}

// PublishSignals folds a decoded batch into the bucket, one Get+fold+Put per
// touched subject (concurrency-bounded). Per-subject failures are counted and
// aggregated, not short-circuited: one unreachable key must not starve the
// rest of the batch. Callers treat any error as degradation, never fatal.
func (s *Store) PublishSignals(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	start := time.Now()
	bySubject := make(map[string][]Row)
	for _, r := range rows {
		bySubject[r.Subject] = append(bySubject[r.Subject], r)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(publishConcurrency)
	for subject, subjectRows := range bySubject {
		g.Go(func() error {
			if err := s.publishSubject(gctx, subject, subjectRows); err != nil {
				publishErrorsTotal.Inc()
				return fmt.Errorf("subject %s: %w", subject, err)
			}
			subjectsPublishedTotal.Inc()
			return nil
		})
	}
	err := g.Wait()
	publishSeconds.Observe(time.Since(start).Seconds())
	return err
}

// publishSubject folds rows into the subject's entry and writes it back —
// skipping the write when nothing changed (a replayed window folds to no-ops).
func (s *Store) publishSubject(ctx context.Context, subject string, rows []Row) error {
	return s.updateEntry(ctx, KeyForSubject(subject), func(entry *Entry) bool {
		changed := false
		for _, r := range rows {
			if entry.Fold(r) {
				changed = true
			}
		}
		return changed
	})
}

// casAttempts bounds one entry's read-modify-write retries, and casRetryBase
// spaces them out.
//
// The backoff is not politeness, it is what makes contention converge:
// unspaced retries all re-read at the same instant and collide again, so under
// real contention a bare retry loop starves some writers however high the
// attempt bound is. Exhausting the attempts is reported as a publish failure —
// correct, since the update really was lost — but that also revokes coverage
// and buys an ~18s reconcile, so it must be reserved for genuine trouble rather
// than a burst of ordinary contention.
const (
	casAttempts  = 8
	casRetryBase = 2 * time.Millisecond
)

// updateEntry runs a read-modify-write against one subject's entry under
// compare-and-swap, retrying on a lost race. mutate reports whether it changed
// anything; false skips the write.
//
// CAS rather than a blind Put because this bucket has more than one writer, in
// two ways that both matter:
//
//   - the coverage reconcile (coverage.go) folds lake.signals_latest into the
//     bucket from its own goroutine while the decode loop keeps publishing;
//   - RunBackfill publishes into the same bucket and is documented as safe to
//     run while the live materializer is up.
//
// Under a blind Put either of those interleavings silently drops the other's
// update — a Get, then someone else's Put, then our Put built on the stale
// value. The dropped reading would not come back until that signal's next
// reading, so signalsLatest would serve a stale value with nothing anywhere
// indicating why. The fold itself is last-write-wins and idempotent, so a
// retried attempt converges on the same result.
func (s *Store) updateEntry(ctx context.Context, key string, mutate func(*Entry) bool) error {
	for attempt := range casAttempts {
		entry, rev, err := s.getEntry(ctx, key)
		if err != nil {
			return err
		}
		if !mutate(entry) {
			return nil
		}
		err = s.putEntry(ctx, key, entry, rev)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errCASConflict) {
			return err
		}
		casRetriesTotal.Inc()
		if attempt == casAttempts-1 {
			return fmt.Errorf("entry %s: %w after %d attempts", key, err, casAttempts)
		}
		if err := sleepCtx(ctx, casBackoff(attempt)); err != nil {
			return err
		}
	}
	return nil
}

// casBackoff grows linearly with jitter. Jitter matters more than the growth
// curve: it is what breaks the lockstep that makes colliding writers collide
// again on the retry.
func casBackoff(attempt int) time.Duration {
	base := time.Duration(attempt+1) * casRetryBase
	return base + time.Duration(rand.Int64N(int64(base)))
}

// sleepCtx waits for d, or returns ctx's error if it ends first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// GetEntry returns the subject's entry, or nil when the subject has none. An
// undecodable entry is an error here (unlike the write path's self-heal): the
// phase-2 reader must fall back to the rollup, not serve a half-parsed value.
func (s *Store) GetEntry(ctx context.Context, subject string) (*Entry, error) {
	kve, err := s.kv.Get(ctx, KeyForSubject(subject))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("kv get: %w", err)
	}
	entry := &Entry{}
	if err := json.Unmarshal(kve.Value(), entry); err != nil {
		return nil, fmt.Errorf("decoding entry for %s: %w", subject, err)
	}
	return entry, nil
}

// DeleteSubject removes a subject's entry. Nothing on the serving or publish
// path calls it — it exists so a coverage gap can be reproduced deliberately
// (a lost publish leaves the lake holding a subject the bucket does not), which
// is the only way to test the false-negative detection that gates
// LATEST_KV_NEGATIVE=serve. Deleting a live subject's entry costs a rollup
// fallback until its next reading, and revokes nothing on its own: coverage
// stays asserted, so do not use this against a bucket in negative-serve mode
// without forcing a reconcile after.
func (s *Store) DeleteSubject(ctx context.Context, subject string) error {
	if err := s.kv.Delete(ctx, KeyForSubject(subject)); err != nil {
		return fmt.Errorf("kv delete %s: %w", subject, err)
	}
	return nil
}

// getEntry loads and decodes the subject entry along with the revision to
// compare against on write; a missing key or an undecodable value (schema
// damage) yields a fresh entry — the fold+write then self-heals the key.
// Revision 0 means "no key", which putEntry turns into a create-if-absent.
func (s *Store) getEntry(ctx context.Context, key string) (*Entry, uint64, error) {
	entry := &Entry{V: EntryVersion}
	kve, err := s.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return entry, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("kv get: %w", err)
	}
	if err := json.Unmarshal(kve.Value(), entry); err != nil {
		s.log.Warn().Err(err).Str("key", key).Msg("undecodable latest-kv entry; rebuilding it from this batch")
		// Keep the revision: the rebuild must still CAS against the damaged
		// value it is replacing, or it could clobber a concurrent good write.
		return &Entry{V: EntryVersion}, kve.Revision(), nil
	}
	return entry, kve.Revision(), nil
}

// errCASConflict marks a lost compare-and-swap: the key moved under us between
// the read and the write, so the caller must re-read and re-fold.
var errCASConflict = errors.New("entry changed concurrently")

// putEntry writes the entry only if the key is still at rev — create-if-absent
// when rev is 0. jetstream reports both flavors of conflict with the same API
// error code (JSErrCodeStreamWrongLastSequence, surfaced as ErrKeyExists), so
// one check covers "someone created it" and "someone updated it".
func (s *Store) putEntry(ctx context.Context, key string, entry *Entry, rev uint64) error {
	entry.V = EntryVersion
	b, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}
	if rev == 0 {
		_, err = s.kv.Create(ctx, key, b)
	} else {
		_, err = s.kv.Update(ctx, key, b, rev)
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return errCASConflict
	}
	if err != nil {
		return fmt.Errorf("kv write: %w", err)
	}
	return nil
}

// BootstrapFromRollup populates the bucket from lake.signals_latest — the
// initial fill on first enable and the disaster-recovery rebuild (bucket lost,
// sustained publish outage). Skipped when the completion marker is present
// unless force (LATEST_KV_FORCE_BOOTSTRAP). Rows MERGE via the same
// last-write-wins fold as live publishes (never a blind overwrite), so running
// it against a live bucket can only advance entries. It runs on the decode
// goroutine before the loop starts, so it never races the live publisher.
func (s *Store) BootstrapFromRollup(ctx context.Context, db *sql.DB, force bool) error {
	if !force {
		_, err := s.kv.Get(ctx, bootstrapMarkerKey)
		if err == nil {
			return nil
		}
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("checking bootstrap marker: %w", err)
		}
	}
	subjects, err := s.mergeFromRollup(ctx, db)
	if err != nil {
		return err
	}
	if _, err := s.kv.Put(ctx, bootstrapMarkerKey, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		return fmt.Errorf("writing bootstrap marker: %w", err)
	}
	bootstrapTimestamp.SetToCurrentTime()
	s.log.Info().Int("subjects", subjects).Msg("latest-kv bootstrap from lake.signals_latest complete")
	return nil
}

// ReconcileFromRollup runs the same merge pass without the marker, as the
// coverage contract's proof of completeness (coverage.go): after it returns
// successfully, every subject in lake.signals_latest has a key in the bucket,
// which is exactly what a reader needs before it may treat a missing key as
// "this subject has no data".
//
// This is the recovery path, NOT a periodic job. lake.signals_latest is
// PARTITIONED BY (subject_bucket) across 256 buckets, so a full pass opens on
// the order of 256 parquet files at ~69ms each (dq#40's cost model) — running
// it on a timer would spend ~18s of S3 round trips per pass, forever, to
// re-confirm what the live publisher already knows. The heartbeat asserts that
// for free; this runs only at boot after an unclean exit, or to recover from a
// publish failure.
func (s *Store) ReconcileFromRollup(ctx context.Context, db *sql.DB) error {
	subjects, err := s.mergeFromRollup(ctx, db)
	if err != nil {
		return err
	}
	s.log.Debug().Int("subjects", subjects).Msg("latest-kv reconcile from lake.signals_latest complete")
	return nil
}

// rollupWatermarkPartition is the lake.ingest_progress key holding the daily
// rollup watermark (dq#55). It MUST equal the materializer's
// dailyWatermarkPartition — that package is deliberately not imported here
// (see the package comment's dependency rule), so the coupling is by value,
// and the watermark end-to-end test in tests/ breaks if the two ever diverge
// (a reconcile that cannot find the writer's watermark row skips the tail).
const rollupWatermarkPartition = "lake.signals_latest#daily_watermark"

// rollupWatermark reads the daily rollup watermark; zero when the row is
// absent (the rollup is then maintained per-pass and is continuously exact, so
// there is no tail to replay). A read/parse FAILURE is an error, not zero: the
// caller is about to certify bucket completeness, and "I could not determine
// how stale the rollup is" must fail toward re-proving, never toward
// asserting over an unexamined tail.
func rollupWatermark(ctx context.Context, db *sql.DB) (time.Time, error) {
	var raw string
	err := db.QueryRowContext(ctx,
		"SELECT cursor FROM lake.ingest_progress WHERE partition = ?", rollupWatermarkPartition).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("reading rollup watermark: %w", err)
	}
	w, perr := time.Parse(time.RFC3339, raw)
	if perr != nil {
		return time.Time{}, fmt.Errorf("unparseable rollup watermark %q: %w", raw, perr)
	}
	return w.UTC(), nil
}

// mergeFromRollup folds every lake.signals_latest row into the bucket, then
// replays the base-table tail since the daily rollup watermark (dq#55), and
// returns the number of subjects visited. Merge-only via FoldValue, so it can
// only advance entries and is safe to run against a live bucket.
//
// The tail replay is what keeps the COMPLETENESS PROOF honest once the rollup
// is refreshed daily instead of per-pass: a successful reconcile certifies
// "every subject in the lake has a key in the bucket", which is exactly what
// lets a reader answer a MISS as authoritative "no data"
// (LATEST_KV_NEGATIVE=serve). A subject whose first-ever reading landed after
// the watermark exists in lake.signals but not in the day-stale rollup — a
// rollup-only scan never visits it, and a reconcile after that subject's lost
// publish would certify a bucket with a real hole: a confident wrong "no
// data" for a vehicle that has data. The tail closes it with a
// constant-literal bound (1–2 settled day partitions, minutes not history).
// While the per-pass fold is still on (pre-flip), the tail is redundant by
// construction and folds to no-ops — correct in both worlds, no flag needed.
func (s *Store) mergeFromRollup(ctx context.Context, db *sql.DB) (int, error) {
	start := time.Now()
	// The rollup is one row per (subject, name) — already the fold's output
	// shape. ORDER BY subject lets one pass group rows into per-subject
	// entries without holding the whole table. loc_ts is the (0,0)-filtered
	// fix time (epoch-coalesced for pre-migration rows, which also have no
	// fix worth carrying: lat/lon 0 is filtered below).
	rows, err := db.QueryContext(ctx, `
		SELECT subject, name, "timestamp", value_number, value_string,
		       loc_lat, loc_lon, loc_hdop, loc_heading, coalesce(loc_ts, make_timestamp(0)) AS loc_ts
		FROM lake.signals_latest ORDER BY subject`)
	if err != nil {
		return 0, fmt.Errorf("scanning lake.signals_latest: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	subjects := 0
	var curSubject string
	pending := map[string]SignalValue{}
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		rows := pending
		pending = map[string]SignalValue{}
		subjects++
		// Under CAS, because this pass runs CONCURRENTLY with the live publisher
		// when it is the coverage reconcile: a blind write here would drop a
		// reading that landed between this subject's read and write, and the lost
		// value would not reappear until that signal reported again.
		return s.updateEntry(ctx, KeyForSubject(curSubject), func(entry *Entry) bool {
			changed := false
			for name, sv := range rows {
				if entry.FoldValue(name, sv) {
					changed = true
				}
			}
			return changed
		})
	}
	for rows.Next() {
		var subject, name, str string
		var ts, locTS time.Time
		var num, lat, lon, hdop, heading float64
		if err := rows.Scan(&subject, &name, &ts, &num, &str, &lat, &lon, &hdop, &heading, &locTS); err != nil {
			return 0, fmt.Errorf("scanning rollup row: %w", err)
		}
		if subject != curSubject {
			if err := flush(); err != nil {
				return 0, fmt.Errorf("merging subject %s: %w", curSubject, err)
			}
			curSubject = subject
		}
		sv := SignalValue{TS: ts.UTC(), Num: num, Str: str}
		if lat != 0 || lon != 0 {
			sv.Loc = &LocValue{TS: locTS.UTC(), Lat: lat, Lon: lon, HDOP: hdop, Heading: heading}
		}
		pending[name] = sv
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rollup scan: %w", err)
	}
	if err := flush(); err != nil {
		return 0, fmt.Errorf("merging subject %s: %w", curSubject, err)
	}
	tailSubjects, err := s.mergeTailSinceWatermark(ctx, db)
	if err != nil {
		return 0, err
	}
	s.log.Debug().Int("subjects", subjects).Int("tail_subjects", tailSubjects).Dur("took", time.Since(start)).
		Msg("latest-kv merge pass over lake.signals_latest complete")
	return subjects + tailSubjects, nil
}

// mergeTailSinceWatermark folds the per-(subject, name) latest of
// lake.signals rows stamped at or after the daily rollup watermark into the
// bucket (see mergeFromRollup for why). A zero watermark (no daily refresh has
// ever run) means the rollup is per-pass fresh and there is no tail. Returns
// the number of subjects the tail visited.
func (s *Store) mergeTailSinceWatermark(ctx context.Context, db *sql.DB) (int, error) {
	w, err := rollupWatermark(ctx, db)
	if err != nil {
		return 0, err
	}
	if w.IsZero() {
		return 0, nil
	}
	// Recency semantics pinned to the rollup fold's recency/locrec CTEs and to
	// Entry.newerThan: the value part wins by (timestamp DESC, cloud_event_id
	// ASC); the location part wins independently among nonzero fixes. The
	// constant make_timestamp bound is the whole point — it prunes the scan to
	// the settled partitions after the watermark instead of history.
	stmt := fmt.Sprintf(`
		WITH tail AS (
			SELECT subject, name, "timestamp" AS ts, cloud_event_id, value_number, value_string,
			       loc_lat, loc_lon, loc_hdop, loc_heading
			FROM lake.signals WHERE "timestamp" >= make_timestamp(%d)
		),
		val AS (
			SELECT subject, name, ts, cloud_event_id, value_number, value_string FROM tail
			QUALIFY row_number() OVER (PARTITION BY subject, name ORDER BY ts DESC, cloud_event_id ASC) = 1
		),
		loc AS (
			SELECT subject, name, ts, cloud_event_id, loc_lat, loc_lon, loc_hdop, loc_heading FROM tail
			WHERE loc_lat != 0 OR loc_lon != 0
			QUALIFY row_number() OVER (PARTITION BY subject, name ORDER BY ts DESC, cloud_event_id ASC) = 1
		)
		SELECT v.subject, v.name, v.ts, v.cloud_event_id, v.value_number, v.value_string,
		       l.ts, coalesce(l.cloud_event_id, ''), coalesce(l.loc_lat, 0), coalesce(l.loc_lon, 0),
		       coalesce(l.loc_hdop, 0), coalesce(l.loc_heading, 0)
		FROM val v
		LEFT JOIN loc l ON l.subject = v.subject AND l.name = v.name
		ORDER BY v.subject`, w.UnixMicro())
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return 0, fmt.Errorf("scanning signals tail since watermark: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	subjects := 0
	var curSubject string
	pending := map[string]SignalValue{}
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		batch := pending
		pending = map[string]SignalValue{}
		subjects++
		return s.updateEntry(ctx, KeyForSubject(curSubject), func(entry *Entry) bool {
			changed := false
			for name, sv := range batch {
				if entry.FoldValue(name, sv) {
					changed = true
				}
			}
			return changed
		})
	}
	for rows.Next() {
		var subject, name, ceid, locCEID, str string
		var ts time.Time
		var locTS sql.NullTime
		var num, lat, lon, hdop, heading float64
		if err := rows.Scan(&subject, &name, &ts, &ceid, &num, &str,
			&locTS, &locCEID, &lat, &lon, &hdop, &heading); err != nil {
			return 0, fmt.Errorf("scanning tail row: %w", err)
		}
		if subject != curSubject {
			if err := flush(); err != nil {
				return 0, fmt.Errorf("merging tail subject %s: %w", curSubject, err)
			}
			curSubject = subject
		}
		sv := SignalValue{TS: ts.UTC(), CEID: ceid, Num: num, Str: str}
		if locTS.Valid && (lat != 0 || lon != 0) {
			sv.Loc = &LocValue{TS: locTS.Time.UTC(), CEID: locCEID, Lat: lat, Lon: lon, HDOP: hdop, Heading: heading}
		}
		pending[name] = sv
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("tail scan: %w", err)
	}
	if err := flush(); err != nil {
		return 0, fmt.Errorf("merging tail subject %s: %w", curSubject, err)
	}
	return subjects, nil
}
