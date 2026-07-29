package latestkv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
)

// The coverage contract (dq#42).
//
// A reader may answer signalsLatest with "no data" straight from a MISSING key
// — skipping the ~795ms rollup fallback that today returns nothing — only if
// the bucket is a COMPLETE mirror of lake.signals_latest. That is nearly true
// by construction: BootstrapFromRollup seeds every subject in the rollup, and
// afterwards a subject reaches the rollup only through a materializer commit
// whose publishLatest ran BEFORE it (see materializer/latest_publisher.go), so
// the cache leads the lake in time — the safe direction for a negative.
//
// The hole is that PublishSignals is best-effort by contract: a publish that
// fails while the commit succeeds puts a subject in the lake that is not in the
// bucket. Today that heals silently on the subject's next reading; under
// negative serving it would be a WRONG ANSWER ("no data" for a vehicle that has
// data). Closing it by making the publish blocking is the wrong trade — it
// would let a NATS outage stall ingest, which latestKVPublishTimeout exists to
// prevent.
//
// So the writer publishes a standing assertion instead, and the reader trusts
// misses only while that assertion is live. The correctness of the scheme rests
// on two properties, in this order:
//
//  1. TRUST DECAYS BY DEFAULT. Coverage carries VerifiedThrough and the reader
//     requires it to be within CoverageMaxStaleness. A writer that dies, wedges,
//     loses NATS, or simply stops heartbeating revokes negative serving on its
//     own within ~90s, with no successful write required at the moment things
//     break. Nothing here depends on an error path getting a message out.
//  2. THE DIRTY BIT IS THE ABSENCE OF A CLEAN ONE. A writer marks Clean only on
//     graceful shutdown. Any other exit — crash, OOM, SIGKILL — leaves the last
//     record un-clean, and the next writer to boot must re-prove coverage with a
//     full rollup→KV reconcile before it may heartbeat again. A failed shutdown
//     write fails toward re-proving, never toward asserting.
//
// Degraded is a third, weaker signal: an explicit "I know I lost a publish",
// written best-effort for humans and for cross-process cases (a backfill run
// publishing alongside the live materializer). It is a courtesy, not a
// dependency — everything stays correct if it never lands.
const (
	// CoverageVersion is the Coverage schema version. Bump on any
	// backward-incompatible change; a reader that does not recognize the version
	// treats coverage as absent (and so stops serving negatives).
	CoverageVersion = 1

	// coverageKey holds the writer's assertion. Lives in the meta. namespace
	// beside bootstrapMarkerKey, outside subjectKeyPrefix, so no encoded subject
	// can collide with it.
	coverageKey = "meta.coverage"

	// CoverageHeartbeatInterval is how often the live writer re-asserts
	// completeness. One small Put (plus one Get to notice an external degrade)
	// per interval is the entire steady-state cost of the contract.
	CoverageHeartbeatInterval = 30 * time.Second

	// CoverageMaxStaleness is how long a reader keeps trusting the last
	// assertion — three missed heartbeats. Long enough that an ordinary
	// materializer restart does not flap negative serving off and on; short
	// enough that a wedged writer stops being believed promptly.
	CoverageMaxStaleness = 90 * time.Second
)

// Coverage is the writer's assertion that the bucket mirrors
// lake.signals_latest completely as of VerifiedThrough.
type Coverage struct {
	V int `json:"v"`
	// VerifiedThrough is when the writer last re-asserted completeness. The
	// reader's staleness check reads this and nothing else, which is what makes
	// silence revoke trust.
	VerifiedThrough time.Time `json:"verified_through"`
	// Clean records a graceful writer shutdown. Its ABSENCE is the dirty bit:
	// the next writer re-proves coverage by reconcile before heartbeating.
	Clean bool `json:"clean,omitempty"`
	// Degraded is a known coverage break, written best-effort so operators and
	// a subsequent writer can see it. Readers honor it, but correctness does not
	// depend on it ever being written (see the package comment above).
	Degraded bool   `json:"degraded,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// trusted reports whether a reader may serve negatives against this record.
// Nil receiver (no key, undecodable, bucket recreated) is untrusted, which is
// the whole point of the nil-safe method.
func (c *Coverage) trusted(now time.Time, maxStaleness time.Duration) bool {
	if c == nil || c.V != CoverageVersion || c.Degraded || c.VerifiedThrough.IsZero() {
		return false
	}
	age := now.Sub(c.VerifiedThrough)
	// A record from the future is clock skew between the writer and this reader.
	// Bounding it symmetrically stops skew from silently EXTENDING trust past the
	// staleness window it is supposed to enforce.
	return age <= maxStaleness && age >= -maxStaleness
}

// GetCoverage reads the writer's assertion, or nil when the key is absent or
// carries a version this build does not know.
func (s *Store) GetCoverage(ctx context.Context) (*Coverage, error) {
	kve, err := s.kv.Get(ctx, coverageKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("kv get coverage: %w", err)
	}
	return decodeCoverage(kve.Value())
}

// decodeCoverage parses a coverage record; an unknown version decodes to nil
// (untrusted) rather than an error, since a newer writer's schema is a normal
// rollout state, not a fault.
func decodeCoverage(b []byte) (*Coverage, error) {
	var c Coverage
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("decoding coverage: %w", err)
	}
	if c.V != CoverageVersion {
		return nil, nil
	}
	return &c, nil
}

// PutCoverage writes the assertion.
func (s *Store) PutCoverage(ctx context.Context, c Coverage) error {
	c.V = CoverageVersion
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal coverage: %w", err)
	}
	if _, err := s.kv.Put(ctx, coverageKey, b); err != nil {
		return fmt.Errorf("kv put coverage: %w", err)
	}
	return nil
}

// CoverageWatcher keeps a query pod's view of the writer's assertion current
// over a KV watch, so the read path costs zero extra round trips: Trusted is an
// atomic load.
//
// The watch is a convenience, not the safety mechanism. A watcher whose
// connection drops stops receiving heartbeats silently — and that is fine,
// because the value it last saw then ages out of CoverageMaxStaleness on its
// own. Trust decays on silence whatever the cause.
type CoverageWatcher struct {
	cur atomic.Pointer[Coverage]
	// bucketTTL is nonzero when the bucket expires entries. Negative serving
	// assumes an entry disappears only when the subject never existed, so a TTL
	// would turn expiry into a confident wrong answer: refuse to trust anything.
	bucketTTL time.Duration
	log       zerolog.Logger
}

// WatchCoverage starts a watcher on the coverage key. It returns once the
// initial value (if any) has been delivered, so a caller that immediately asks
// Trusted gets the real answer rather than a spurious false.
func (s *Store) WatchCoverage(ctx context.Context) (*CoverageWatcher, error) {
	w := &CoverageWatcher{bucketTTL: s.bucketTTL, log: s.log.With().Str("component", "latestkv-coverage").Logger()}
	if s.bucketTTL != 0 {
		w.log.Error().Dur("bucket_ttl", s.bucketTTL).
			Msg("signals-latest bucket has a TTL: entries can expire, so a missing key no longer proves the subject has no data — negative serving disabled")
	}
	watcher, err := s.kv.Watch(ctx, coverageKey)
	if err != nil {
		return nil, fmt.Errorf("watching coverage key: %w", err)
	}
	ready := make(chan struct{})
	go w.consume(ctx, watcher, ready)
	select {
	case <-ready:
	case <-ctx.Done():
	}
	return w, nil
}

// consume applies watch updates to the atomic snapshot. It closes ready once
// the watcher has replayed the current value — signalled by the nil marker
// jetstream sends at the end of initial values.
func (w *CoverageWatcher) consume(ctx context.Context, watcher jetstream.KeyWatcher, ready chan struct{}) {
	defer func() { _ = watcher.Stop() }()
	closeReady := sync.OnceFunc(func() { close(ready) })
	defer closeReady()
	for {
		select {
		case <-ctx.Done():
			return
		case kve, ok := <-watcher.Updates():
			if !ok {
				// The watcher is done (context cancelled, or the underlying
				// consumer went away). Drop the snapshot rather than serve
				// negatives off a value nothing is refreshing.
				w.cur.Store(nil)
				return
			}
			if kve == nil {
				closeReady() // end of initial values
				continue
			}
			if kve.Operation() != jetstream.KeyValuePut {
				w.cur.Store(nil) // deleted or purged
				continue
			}
			c, err := decodeCoverage(kve.Value())
			if err != nil {
				w.log.Warn().Err(err).Msg("undecodable coverage record; not serving negatives until the next valid one")
			}
			w.cur.Store(c)
		}
	}
}

// Trusted reports whether the reader may answer a KV miss with "no data".
func (w *CoverageWatcher) Trusted() bool { return w.TrustedAt(time.Now()) }

// TrustedAt is Trusted at an explicit instant (tests pin the clock).
func (w *CoverageWatcher) TrustedAt(now time.Time) bool {
	if w == nil || w.bucketTTL != 0 {
		return false
	}
	return w.cur.Load().trusted(now, CoverageMaxStaleness)
}

// CoverageReporter is the writer half: it proves coverage, then re-asserts it
// on a heartbeat for as long as every publish keeps succeeding.
//
// Only the live decode loop should Run one. A process that publishes without
// being the live writer (RunBackfill) takes a reporter for ObservePublish alone,
// so its publish failures can degrade the assertion without its heartbeat
// competing with the materializer's.
type CoverageReporter struct {
	store *Store
	log   zerolog.Logger
	// reconcile re-proves coverage from the lake (mergeFromRollup over
	// lake.signals_latest). Nil in publish-only mode.
	reconcile func(context.Context) error

	mu sync.Mutex
	// proven is "this process currently holds a proof of completeness". False at
	// boot: a fresh writer has proved nothing yet.
	proven   bool
	degraded bool
	reason   string
	// adoptChecked records that the one-time "was the last shutdown clean?" probe
	// has run, so an unclean record cannot be re-probed into an adoption later.
	adoptChecked bool
	// retryAfter backs off failed reconciles. A reconcile that fails is usually a
	// NATS outage, and its per-subject round trips each wait out the JetStream
	// timeout — retrying every tick would pile those up for the whole outage.
	retryAfter time.Time
	backoff    time.Duration
}

const (
	reconcileMinBackoff = 30 * time.Second
	reconcileMaxBackoff = 10 * time.Minute
	// coverageWriteTimeout bounds one coverage Put/Get. Short: these run on the
	// reporter's own goroutine, but a wedged write must not hold the heartbeat
	// past the staleness window it is meant to refresh.
	coverageWriteTimeout = 5 * time.Second
)

// NewCoverageReporter builds a reporter. reconcile may be nil for a
// publish-only participant (see the type comment).
func NewCoverageReporter(store *Store, reconcile func(context.Context) error, log zerolog.Logger) *CoverageReporter {
	registerCoverageMetrics()
	return &CoverageReporter{
		store:     store,
		reconcile: reconcile,
		log:       log.With().Str("component", "latestkv-coverage").Logger(),
	}
}

// ObservePublish records one batch publish outcome. Called from the DECODE
// goroutine, so it does no I/O — it only drops the in-memory proof. The reader
// learns about it by the heartbeat going quiet, which is exactly the decay
// property the contract is built on; the reporter's own loop persists the
// Degraded courtesy flag later, if it can.
func (r *CoverageReporter) ObservePublish(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.degraded {
		r.log.Warn().Err(err).Msg("latest-kv publish failed: coverage is no longer proven, negative serving will lapse")
	}
	r.proven = false
	r.degraded = true
	r.reason = err.Error()
	coverageProven.Set(0)
}

// FlushDegraded persists a known gap, if one was observed. It exists for
// publish-only participants — RunBackfill publishes into the same bucket
// alongside the live materializer, and a backfilled batch can introduce a
// subject nobody else will publish again. Without this, that process's failure
// would be invisible to the live writer, whose heartbeat would go on asserting
// completeness it no longer has. The live writer reads the flag on its next
// heartbeat and re-proves by reconcile.
//
// Reports whether a degraded flag was written.
func (r *CoverageReporter) FlushDegraded(ctx context.Context) bool {
	r.mu.Lock()
	degraded, reason := r.degraded, r.reason
	r.mu.Unlock()
	if !degraded {
		return false
	}
	r.persistDegraded(ctx, reason)
	return true
}

// Run drives the reporter until ctx is cancelled, then marks a clean shutdown.
// It blocks, so callers run it on its own goroutine — the boot reconcile is a
// full scan of lake.signals_latest and must not sit in front of decode.
func (r *CoverageReporter) Run(ctx context.Context) {
	ticker := time.NewTicker(CoverageHeartbeatInterval)
	defer ticker.Stop()
	r.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			r.markClean()
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// tick establishes a proof if the reporter lacks one, then heartbeats.
func (r *CoverageReporter) tick(ctx context.Context) {
	if !r.isProven() {
		if !r.establish(ctx) {
			return
		}
	}
	r.heartbeat(ctx)
}

func (r *CoverageReporter) isProven() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.proven
}

// establish proves coverage, either by adopting a cleanly-shut-down predecessor's
// assertion or by reconciling from the lake. Reports whether a proof now exists.
func (r *CoverageReporter) establish(ctx context.Context) bool {
	r.mu.Lock()
	degraded, reason, checked := r.degraded, r.reason, r.adoptChecked
	r.adoptChecked = true
	if !r.retryAfter.IsZero() && time.Now().Before(r.retryAfter) {
		r.mu.Unlock()
		return false
	}
	r.mu.Unlock()

	// One-time adoption: a predecessor that shut down gracefully left a complete
	// bucket behind, so an ordinary deploy skips the scan. Only at boot, and only
	// if this process has not itself seen a publish failure.
	if !checked && !degraded {
		if r.adoptClean(ctx) {
			return true
		}
	}
	if degraded {
		r.persistDegraded(ctx, reason)
	}
	return r.runReconcile(ctx)
}

// adoptClean reports whether the stored record proves the bucket is complete
// without a scan: written by a writer that exited gracefully and knew of no gap.
func (r *CoverageReporter) adoptClean(ctx context.Context) bool {
	cctx, cancel := context.WithTimeout(ctx, coverageWriteTimeout)
	defer cancel()
	cur, err := r.store.GetCoverage(cctx)
	if err != nil {
		r.log.Warn().Err(err).Msg("could not read coverage at boot; proving it by reconcile instead")
		return false
	}
	if cur == nil || !cur.Clean || cur.Degraded {
		return false
	}
	r.mu.Lock()
	r.proven = true
	r.mu.Unlock()
	coverageProven.Set(1)
	r.log.Info().Time("verified_through", cur.VerifiedThrough).
		Msg("adopted coverage from a clean predecessor shutdown; skipping the boot reconcile")
	return true
}

// persistDegraded records the known gap for operators, best-effort. NATS is
// usually the reason we are here, so a failure is logged at debug and dropped:
// the heartbeat has already stopped, which is what actually protects readers.
func (r *CoverageReporter) persistDegraded(ctx context.Context, reason string) {
	cctx, cancel := context.WithTimeout(ctx, coverageWriteTimeout)
	defer cancel()
	if err := r.store.PutCoverage(cctx, Coverage{Degraded: true, Reason: reason}); err != nil {
		r.log.Debug().Err(err).Msg("could not persist the degraded coverage flag (readers already lapse on the stalled heartbeat)")
	}
}

// runReconcile re-proves coverage by merging lake.signals_latest into the
// bucket. Reports whether a proof now exists.
func (r *CoverageReporter) runReconcile(ctx context.Context) bool {
	if r.reconcile == nil {
		return false
	}
	start := time.Now()
	err := r.reconcile(ctx)
	reconcileSeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		reconcileTotal.WithLabelValues("error").Inc()
		r.mu.Lock()
		defer r.mu.Unlock()
		r.backoff = nextBackoff(r.backoff)
		r.retryAfter = time.Now().Add(r.backoff)
		r.log.Error().Err(err).Dur("retry_in", r.backoff).
			Msg("coverage reconcile failed; negative serving stays off until it succeeds")
		return false
	}
	reconcileTotal.WithLabelValues("ok").Inc()
	// Retract the degraded flag the revocation left in the bucket BEFORE the
	// heartbeat runs. Without this the heartbeat reads back this process's own
	// revocation, cannot tell it from another publisher's, and drops the proof
	// the reconcile just established — a livelock that reconciles forever and
	// never asserts.
	clearCtx, cancel := context.WithTimeout(ctx, coverageWriteTimeout)
	defer cancel()
	if perr := r.store.PutCoverage(clearCtx, Coverage{VerifiedThrough: time.Now()}); perr != nil {
		r.log.Warn().Err(perr).Msg("coverage reconcile succeeded but the assertion could not be written; readers stay on the rollup fallback")
		r.mu.Lock()
		defer r.mu.Unlock()
		r.backoff = nextBackoff(r.backoff)
		r.retryAfter = time.Now().Add(r.backoff)
		return false
	}
	coverageVerifiedTimestamp.SetToCurrentTime()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proven, r.degraded, r.reason = true, false, ""
	r.backoff, r.retryAfter = 0, time.Time{}
	coverageProven.Set(1)
	r.log.Info().Dur("took", time.Since(start)).Msg("coverage proven by reconcile from lake.signals_latest")
	return true
}

func nextBackoff(cur time.Duration) time.Duration {
	if cur == 0 {
		return reconcileMinBackoff
	}
	if next := cur * 2; next < reconcileMaxBackoff {
		return next
	}
	return reconcileMaxBackoff
}

// heartbeat re-asserts completeness. It reads before writing so that a Degraded
// flag set by ANOTHER publisher on this bucket (a concurrent RunBackfill) drops
// this process's proof instead of being overwritten by its next assertion.
func (r *CoverageReporter) heartbeat(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, coverageWriteTimeout)
	defer cancel()
	cur, err := r.store.GetCoverage(cctx)
	if err != nil {
		r.log.Warn().Err(err).Msg("coverage heartbeat read failed; readers lapse to the rollup fallback if this persists")
		return
	}
	if cur != nil && cur.Degraded {
		r.mu.Lock()
		r.proven, r.degraded, r.reason = false, true, cur.Reason
		r.mu.Unlock()
		coverageProven.Set(0)
		r.log.Warn().Str("reason", cur.Reason).
			Msg("another publisher marked coverage degraded; re-proving by reconcile")
		return
	}
	now := time.Now()
	if err := r.store.PutCoverage(cctx, Coverage{VerifiedThrough: now}); err != nil {
		r.log.Warn().Err(err).Msg("coverage heartbeat write failed; readers lapse to the rollup fallback if this persists")
		return
	}
	coverageVerifiedTimestamp.SetToCurrentTime()
}

// markClean writes the graceful-shutdown record so the next writer can adopt
// coverage without a full reconcile. Uses its own context: the caller's is
// already cancelled by definition. A failure here is safe — the successor
// simply reconciles.
func (r *CoverageReporter) markClean() {
	if !r.isProven() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), coverageWriteTimeout)
	defer cancel()
	if err := r.store.PutCoverage(ctx, Coverage{VerifiedThrough: time.Now(), Clean: true}); err != nil {
		r.log.Warn().Err(err).Msg("could not mark coverage clean on shutdown; the next writer will re-prove it by reconcile")
		return
	}
	r.log.Info().Msg("coverage marked clean for shutdown")
}
