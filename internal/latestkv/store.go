package latestkv

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
}

// Open connects to NATS and ensures the bucket exists. The connection retries
// forever (like din's clients): a NATS outage must degrade to missed cache
// updates, never a crash-looping materializer.
func Open(ctx context.Context, url, bucket string, log zerolog.Logger) (*Store, error) {
	conn, err := nats.Connect(url,
		nats.Name("dq-latest-kv"),
		nats.MaxReconnects(-1),
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
	return &Store{conn: conn, kv: kv, log: log.With().Str("component", "latestkv").Logger()}, nil
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

// publishSubject folds rows into the subject's entry and Puts it back —
// skipping the Put when nothing changed (a replayed window folds to no-ops).
// Single writer, so plain Put: there is no concurrent updater to CAS against.
func (s *Store) publishSubject(ctx context.Context, subject string, rows []Row) error {
	key := KeyForSubject(subject)
	entry, err := s.getEntry(ctx, key)
	if err != nil {
		return err
	}
	changed := false
	for _, r := range rows {
		if entry.Fold(r) {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return s.putEntry(ctx, key, entry)
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

// getEntry loads and decodes the subject entry; a missing key or an
// undecodable value (schema damage) yields a fresh entry — the fold+Put then
// self-heals the key.
func (s *Store) getEntry(ctx context.Context, key string) (*Entry, error) {
	entry := &Entry{V: EntryVersion}
	kve, err := s.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return entry, nil
	}
	if err != nil {
		return nil, fmt.Errorf("kv get: %w", err)
	}
	if err := json.Unmarshal(kve.Value(), entry); err != nil {
		s.log.Warn().Err(err).Str("key", key).Msg("undecodable latest-kv entry; rebuilding it from this batch")
		return &Entry{V: EntryVersion}, nil
	}
	return entry, nil
}

func (s *Store) putEntry(ctx context.Context, key string, entry *Entry) error {
	entry.V = EntryVersion
	b, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}
	if _, err := s.kv.Put(ctx, key, b); err != nil {
		return fmt.Errorf("kv put: %w", err)
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
		return fmt.Errorf("scanning lake.signals_latest: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	subjects := 0
	var curSubject string
	pending := map[string]SignalValue{}
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		key := KeyForSubject(curSubject)
		entry, err := s.getEntry(ctx, key)
		if err != nil {
			return err
		}
		changed := false
		for name, sv := range pending {
			if entry.FoldValue(name, sv) {
				changed = true
			}
		}
		pending = map[string]SignalValue{}
		subjects++
		if !changed {
			return nil
		}
		return s.putEntry(ctx, key, entry)
	}
	for rows.Next() {
		var subject, name, str string
		var ts, locTS time.Time
		var num, lat, lon, hdop, heading float64
		if err := rows.Scan(&subject, &name, &ts, &num, &str, &lat, &lon, &hdop, &heading, &locTS); err != nil {
			return fmt.Errorf("scanning rollup row: %w", err)
		}
		if subject != curSubject {
			if err := flush(); err != nil {
				return fmt.Errorf("bootstrap subject %s: %w", curSubject, err)
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
		return fmt.Errorf("rollup scan: %w", err)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("bootstrap subject %s: %w", curSubject, err)
	}
	if _, err := s.kv.Put(ctx, bootstrapMarkerKey, []byte(time.Now().UTC().Format(time.RFC3339))); err != nil {
		return fmt.Errorf("writing bootstrap marker: %w", err)
	}
	bootstrapTimestamp.SetToCurrentTime()
	s.log.Info().Int("subjects", subjects).Dur("took", time.Since(start)).
		Msg("latest-kv bootstrap from lake.signals_latest complete")
	return nil
}
