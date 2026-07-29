package latestkv

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReporter(t *testing.T, s *Store, reconcile func(context.Context) error) *CoverageReporter {
	t.Helper()
	return NewCoverageReporter(s, reconcile, zerolog.Nop())
}

// TestCoverage_TrustDecaysOnSilence pins the property the whole contract rests
// on: a reader stops trusting an assertion nobody is refreshing, without anyone
// having to successfully write anything at the moment things break.
func TestCoverage_TrustDecaysOnSilence(t *testing.T) {
	now := time.Now()
	fresh := &Coverage{V: CoverageVersion, VerifiedThrough: now.Add(-10 * time.Second)}
	assert.True(t, fresh.trusted(now, CoverageMaxStaleness), "a recent assertion is trusted")

	stale := &Coverage{V: CoverageVersion, VerifiedThrough: now.Add(-CoverageMaxStaleness - time.Second)}
	assert.False(t, stale.trusted(now, CoverageMaxStaleness), "an assertion older than the staleness window is not")

	// Every other way of having no live assertion must also read as untrusted.
	assert.False(t, (*Coverage)(nil).trusted(now, CoverageMaxStaleness), "absent key")
	assert.False(t, (&Coverage{V: CoverageVersion + 1, VerifiedThrough: now}).trusted(now, CoverageMaxStaleness), "unknown schema version")
	assert.False(t, (&Coverage{V: CoverageVersion, VerifiedThrough: now, Degraded: true}).trusted(now, CoverageMaxStaleness), "degraded")
	assert.False(t, (&Coverage{V: CoverageVersion}).trusted(now, CoverageMaxStaleness), "zero timestamp")

	// Clock skew must not EXTEND trust: a writer running fast could otherwise
	// hand out an assertion that stays trusted long past the staleness window.
	skewed := &Coverage{V: CoverageVersion, VerifiedThrough: now.Add(2 * CoverageMaxStaleness)}
	assert.False(t, skewed.trusted(now, CoverageMaxStaleness), "a far-future assertion is not trusted")
}

func TestCoverage_WatcherTracksWriter(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-watch")

	w, err := s.WatchCoverage(ctx)
	require.NoError(t, err)
	assert.False(t, w.Trusted(), "no assertion has been written yet")

	require.NoError(t, s.PutCoverage(ctx, Coverage{VerifiedThrough: time.Now()}))
	requireEventually(t, func() bool { return w.Trusted() }, "watcher should pick up the assertion")

	// A degraded record revokes trust without waiting for staleness.
	require.NoError(t, s.PutCoverage(ctx, Coverage{VerifiedThrough: time.Now(), Degraded: true, Reason: "publish failed"}))
	requireEventually(t, func() bool { return !w.Trusted() }, "watcher should honor the degraded flag")
}

// TestCoverage_WatcherStartsWithCurrentValue guards the readiness handshake: a
// query pod that comes up while coverage is already live must not spend its
// first requests falling back because the watch had not replayed yet.
func TestCoverage_WatcherStartsWithCurrentValue(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-initial")
	require.NoError(t, s.PutCoverage(ctx, Coverage{VerifiedThrough: time.Now()}))

	w, err := s.WatchCoverage(ctx)
	require.NoError(t, err)
	assert.True(t, w.Trusted(), "WatchCoverage must return with the current value already applied")
}

// TestCoverage_BucketTTLDisablesTrust covers the premise behind a negative: a
// missing key means "never had data" only if entries cannot expire.
func TestCoverage_BucketTTLDisablesTrust(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-ttl")
	require.NoError(t, s.PutCoverage(ctx, Coverage{VerifiedThrough: time.Now()}))
	s.bucketTTL = time.Hour // as if an operator had set MaxAge on the bucket

	w, err := s.WatchCoverage(ctx)
	require.NoError(t, err)
	assert.False(t, w.Trusted(), "a bucket that expires entries must never support negative serving")
}

func TestCoverageReporter_ProvesByReconcileThenHeartbeats(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-prove")
	reconciled := 0
	r := newReporter(t, s, func(context.Context) error { reconciled++; return nil })

	r.tick(ctx)
	assert.Equal(t, 1, reconciled, "a fresh writer proves coverage before asserting it")
	cur, err := s.GetCoverage(ctx)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.True(t, cur.trusted(time.Now(), CoverageMaxStaleness))

	// Subsequent ticks heartbeat without re-scanning the lake.
	r.tick(ctx)
	assert.Equal(t, 1, reconciled, "an established proof is not re-proved every tick")
}

// TestCoverageReporter_PublishFailureRevokesThenRecovers is the core failure
// path: a lost publish may have put a subject in the lake that is not in the
// bucket, so the assertion must stop until a reconcile re-establishes it.
func TestCoverageReporter_PublishFailureRevokesThenRecovers(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-revoke")
	reconciled := 0
	r := newReporter(t, s, func(context.Context) error { reconciled++; return nil })
	r.tick(ctx)
	require.Equal(t, 1, reconciled)

	r.ObservePublish(errors.New("kv put: connection refused"))
	assert.False(t, r.isProven(), "a failed publish drops the proof immediately")

	cur, err := s.GetCoverage(ctx)
	require.NoError(t, err)
	// The heartbeat has already stopped; the stored record has not been rewritten
	// yet, which is exactly the case the reader's staleness check covers.
	require.NotNil(t, cur)

	r.tick(ctx)
	assert.Equal(t, 2, reconciled, "recovery re-proves coverage from the lake")
	assert.True(t, r.isProven())
	cur, err = s.GetCoverage(ctx)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.False(t, cur.Degraded, "a successful reconcile clears the degraded flag")
	assert.True(t, cur.trusted(time.Now(), CoverageMaxStaleness))
}

// TestCoverageReporter_FailedReconcileBacksOff: a reconcile fails because NATS
// is down, and each attempt walks 300+ subjects waiting out timeouts. Retrying
// every 30s tick for the length of an outage would pile those up.
func TestCoverageReporter_FailedReconcileBacksOff(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-backoff")
	attempts := 0
	r := newReporter(t, s, func(context.Context) error { attempts++; return errors.New("nats unreachable") })

	r.tick(ctx)
	assert.Equal(t, 1, attempts)
	assert.False(t, r.isProven())

	r.tick(ctx)
	assert.Equal(t, 1, attempts, "the retry is deferred, not run on the very next tick")

	r.mu.Lock()
	r.retryAfter = time.Now().Add(-time.Second) // as if the backoff had elapsed
	r.mu.Unlock()
	r.tick(ctx)
	assert.Equal(t, 2, attempts, "it does retry once the backoff elapses")
}

// TestCoverageReporter_AdoptsCleanShutdown: an ordinary deploy should not pay a
// full lake scan, because the predecessor left the bucket complete and said so.
func TestCoverageReporter_AdoptsCleanShutdown(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-adopt")
	require.NoError(t, s.PutCoverage(ctx, Coverage{VerifiedThrough: time.Now(), Clean: true}))

	reconciled := 0
	r := newReporter(t, s, func(context.Context) error { reconciled++; return nil })
	r.tick(ctx)
	assert.Equal(t, 0, reconciled, "a clean predecessor's assertion is adopted without a scan")
	assert.True(t, r.isProven())
}

// TestCoverageReporter_UncleanExitForcesReconcile is the other half of the
// dirty-bit rule: a crash leaves the last record un-clean, and the successor
// must re-prove rather than resume asserting.
func TestCoverageReporter_UncleanExitForcesReconcile(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-unclean")
	// A heartbeat record — the writer was alive and then died without marking clean.
	require.NoError(t, s.PutCoverage(ctx, Coverage{VerifiedThrough: time.Now()}))

	reconciled := 0
	r := newReporter(t, s, func(context.Context) error { reconciled++; return nil })
	r.tick(ctx)
	assert.Equal(t, 1, reconciled, "an un-clean predecessor record forces a reconcile")
	assert.True(t, r.isProven())
}

// TestCoverageReporter_ExternalDegradeDropsProof covers a concurrent
// RunBackfill: its publish failure must pull the live writer's assertion down
// rather than be overwritten by the next heartbeat.
func TestCoverageReporter_ExternalDegradeDropsProof(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-external")
	reconciled := 0
	r := newReporter(t, s, func(context.Context) error { reconciled++; return nil })
	r.tick(ctx)
	require.True(t, r.isProven())

	// Another publisher marks the bucket degraded.
	backfill := newReporter(t, s, nil)
	backfill.ObservePublish(errors.New("kv put: timeout"))
	require.True(t, backfill.FlushDegraded(ctx))

	r.tick(ctx) // heartbeat reads before writing
	assert.False(t, r.isProven(), "the live writer honors another publisher's degrade")

	r.tick(ctx)
	assert.Equal(t, 2, reconciled, "and re-proves by reconcile")
	assert.True(t, r.isProven())
}

func TestCoverageReporter_MarksCleanOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newStore(t, startNATS(t), "t-cov-clean")
	r := newReporter(t, s, func(context.Context) error { return nil })

	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	requireEventually(t, func() bool { return r.isProven() }, "reporter should prove coverage at start")
	cancel()
	<-done

	cur, err := s.GetCoverage(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.True(t, cur.Clean, "a graceful shutdown lets the next boot skip the reconcile")
}

// TestCoverageReporter_UnprovenShutdownIsNotClean: a writer that never held a
// proof must not hand its successor a reason to skip the reconcile.
func TestCoverageReporter_UnprovenShutdownIsNotClean(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cov-unproven")
	r := newReporter(t, s, func(context.Context) error { return errors.New("nats unreachable") })
	r.tick(ctx)
	require.False(t, r.isProven())

	r.markClean()
	cur, err := s.GetCoverage(ctx)
	require.NoError(t, err)
	assert.Nil(t, cur, "an unproven writer writes no clean mark at all")
}

// TestStore_ConcurrentWritersDoNotLoseUpdates is the reason entry writes are
// compare-and-swap rather than a plain Put. The coverage reconcile folds
// lake.signals_latest into the bucket from its own goroutine while the decode
// loop keeps publishing, and RunBackfill publishes alongside the live
// materializer — so "single writer" no longer holds. Under a blind Put, a
// reading that lands between another writer's read and write is dropped, and
// does not reappear until that signal reports again.
func TestStore_ConcurrentWritersDoNotLoseUpdates(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, startNATS(t), "t-cas")
	subject := "did:erc721:137:0xAB:7"

	// Many writers advancing DIFFERENT names on the SAME subject: every one of
	// them read-modify-writes the same key, so a lost update loses a whole name.
	const writers = 12
	errs := make(chan error, writers)
	start := make(chan struct{})
	for i := range writers {
		go func() {
			<-start
			errs <- s.PublishSignals(ctx, []Row{{
				Subject:      subject,
				Name:         fmt.Sprintf("signal%02d", i),
				Timestamp:    t0.Add(time.Duration(i) * time.Second),
				CloudEventID: fmt.Sprintf("ce%02d", i),
				ValueNumber:  float64(i),
			}})
		}()
	}
	close(start)
	for range writers {
		require.NoError(t, <-errs)
	}

	entry, _, err := s.getEntry(ctx, KeyForSubject(subject))
	require.NoError(t, err)
	assert.Len(t, entry.Signals, writers, "every concurrent writer's name must survive")
	for i := range writers {
		name := fmt.Sprintf("signal%02d", i)
		sv, ok := entry.Signals[name]
		require.True(t, ok, "%s was lost to a concurrent write", name)
		assert.Equal(t, float64(i), sv.Num, name)
	}
}

func requireEventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 5*time.Second, 10*time.Millisecond, msg)
}
