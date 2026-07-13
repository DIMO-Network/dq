package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPoisonClassifiers pins the error classes ported from the materializer
// (1736873): only the aborted-session cascade recycles and only full instance
// invalidation escalates straight to restart; ordinary errors match neither.
func TestPoisonClassifiers(t *testing.T) {
	aborted := errors.New("failed querying duckdb: TransactionContext Error: Failed to get table insertion file list from DuckLake: Current transaction is aborted (please ROLLBACK)")
	invalidated := errors.New("FATAL Error: Failed: database has been invalidated because of a previous fatal error")

	assert.True(t, isAbortedSession(aborted))
	assert.True(t, isDatabaseInvalidated(invalidated))

	for _, err := range []error{nil, errors.New("some transient s3 blip"), context.Canceled} {
		assert.False(t, isAbortedSession(err), "%v must not classify as aborted session", err)
		assert.False(t, isDatabaseInvalidated(err), "%v must not classify as invalidated", err)
	}
	// The collision's own INTERNAL error is a per-request failure, not pool
	// poison: the retry may succeed on another connection. Only its aftermath
	// (aborted session / invalidated instance) triggers recovery.
	internal := errors.New("failed querying duckdb: INTERNAL Error: Attempted to access index 0 within vector of size 0")
	assert.False(t, isAbortedSession(internal))
	assert.False(t, isDatabaseInvalidated(internal))
}

// poisonHarness builds a poisonRecovery over a real in-memory DuckDB pool with
// the process-exit hook captured instead of executed.
func poisonHarness(t *testing.T) (*poisonRecovery, *string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	var fatalReason string
	rec := newPoisonRecovery()
	rec.db = db
	rec.fatal = func(reason string, err error) { fatalReason = reason }
	return rec, &fatalReason
}

// TestPoisonRecoveryTwoStrike pins the connection-age two-strike calibration:
// the first aborted session burns the idle pool; a straggler still on a
// pre-recycle session re-burns but never escalates; only a session created
// AFTER the recycle that is still aborted proves instance-level poison and
// exits. A healthy post-recycle session disarms, so the next collision (din
// maintenance fires every ~15m) starts over at strike one.
func TestPoisonRecoveryTwoStrike(t *testing.T) {
	aborted := errors.New("Current transaction is aborted (please ROLLBACK)")

	t.Run("ordinary errors do nothing", func(t *testing.T) {
		rec, fatalReason := poisonHarness(t)
		rec.observe(errors.New("some transient s3 blip"), time.Now())
		assert.False(t, rec.armed)
		assert.Empty(t, *fatalReason)
	})

	t.Run("second strike on a fresh session escalates", func(t *testing.T) {
		rec, fatalReason := poisonHarness(t)
		rec.observe(aborted, time.Now()) // strike one: recycle + arm
		require.True(t, rec.armed)
		require.Empty(t, *fatalReason)

		// A straggler that was already in flight on a PRE-recycle session says
		// nothing about whether the recycle worked — re-burn, never escalate.
		rec.observe(aborted, rec.lastRecycle.Add(-time.Second))
		require.Empty(t, *fatalReason)

		// A session born AFTER the recycle still aborted: instance-level.
		rec.observe(aborted, rec.lastRecycle.Add(time.Second))
		assert.Contains(t, *fatalReason, "survived a connection recycle")
	})

	t.Run("healthy fresh session disarms", func(t *testing.T) {
		rec, fatalReason := poisonHarness(t)
		rec.observe(aborted, time.Now())
		require.True(t, rec.armed)

		rec.observe(nil, rec.lastRecycle.Add(time.Second)) // post-recycle success
		require.False(t, rec.armed, "a healthy post-recycle session proves the pool recovered")

		// The NEXT collision recycles again instead of killing the pod.
		rec.observe(aborted, rec.lastRecycle.Add(2*time.Second))
		assert.Empty(t, *fatalReason)
		assert.True(t, rec.armed)
	})

	t.Run("pre-recycle success does not disarm", func(t *testing.T) {
		rec, fatalReason := poisonHarness(t)
		rec.observe(aborted, time.Now())
		rec.observe(nil, rec.lastRecycle.Add(-time.Second)) // old healthy conn
		require.True(t, rec.armed, "an old session's success proves nothing about fresh sessions")

		rec.observe(aborted, rec.lastRecycle.Add(time.Second))
		assert.Contains(t, *fatalReason, "survived a connection recycle")
	})

	t.Run("database invalidated escalates immediately", func(t *testing.T) {
		rec, fatalReason := poisonHarness(t)
		rec.observe(errors.New("Invalidated database instance: database has been invalidated"), time.Now())
		assert.Contains(t, *fatalReason, "database invalidated")
	})
}

// TestPoisonRecoveryServiceTransparent proves NewService with PoisonRecovery
// installs the wrapper without disturbing healthy traffic: plain queries,
// error passthrough, and prepared statements (which forward through the
// embedded conn's optional interfaces) all behave as without it.
func TestPoisonRecoveryServiceTransparent(t *testing.T) {
	svc, err := NewService(Config{PoisonRecovery: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })

	rec := svc.poison
	require.NotNil(t, rec, "PoisonRecovery must install the driver wrapper")
	var fatalReason string
	rec.fatal = func(reason string, err error) { fatalReason = reason }

	db := svc.DB()
	var one int
	require.NoError(t, db.QueryRow("SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)

	_, err = db.Exec("SELECT * FROM this_table_does_not_exist")
	require.Error(t, err, "query errors must pass through the wrapper unchanged")

	stmt, err := db.Prepare("SELECT ?::INT")
	require.NoError(t, err, "prepared statements must forward through the wrapper")
	require.NoError(t, stmt.QueryRow(7).Scan(&one))
	assert.Equal(t, 7, one)
	require.NoError(t, stmt.Close())

	assert.False(t, rec.armed, "healthy traffic and ordinary errors must not arm recovery")
	assert.Empty(t, fatalReason)
}

// abortedStubConn stands in for a duckdb conn whose catalog session din's
// maintenance left permanently aborted: every query fails with the aborted
// class. A real aborted session can't be induced on a plain in-memory DuckDB
// (the FATAL needs the ducklake extension + a concurrent flush), so the
// wrapper's observation path is pinned against a stub.
type abortedStubConn struct {
	duckDriverConn // nil: panics if the wrapper forwards instead of observing
	err            error
}

func (c *abortedStubConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, c.err
}

func (c *abortedStubConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, c.err
}

// TestRecoveringConnObserves proves the driver wrapper feeds both query paths
// into the two-strike recovery: an aborted-session error on QueryContext or
// ExecContext registers a strike against the wrapped connection's birth time.
func TestRecoveringConnObserves(t *testing.T) {
	aborted := errors.New("Current transaction is aborted (please ROLLBACK)")

	for _, path := range []string{"query", "exec"} {
		t.Run(path, func(t *testing.T) {
			rec, fatalReason := poisonHarness(t)
			conn := &recoveringConn{
				duckDriverConn: &abortedStubConn{err: aborted},
				rec:            rec,
				born:           time.Now(),
			}
			var err error
			if path == "query" {
				_, err = conn.QueryContext(context.Background(), "SELECT 1", nil)
			} else {
				_, err = conn.ExecContext(context.Background(), "SELECT 1", nil)
			}
			require.ErrorIs(t, err, aborted, "the wrapper must return the original error")
			assert.True(t, rec.armed, "an aborted session must register a strike")
			assert.Empty(t, *fatalReason, "one strike must recycle, not exit")

			// A second failure on a connection born after the recycle proves the
			// recycle didn't help — instance-level poison, escalate.
			fresh := &recoveringConn{
				duckDriverConn: &abortedStubConn{err: aborted},
				rec:            rec,
				born:           rec.lastRecycle.Add(time.Second),
			}
			_, _ = fresh.QueryContext(context.Background(), "SELECT 1", nil)
			assert.Contains(t, *fatalReason, "survived a connection recycle")
		})
	}
}
