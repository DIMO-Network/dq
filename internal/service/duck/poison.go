package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	zlog "github.com/rs/zerolog/log"
)

// This file ports the materializer's two-strike ducklake poison recovery
// (commit 1736873) to the query pool (#21). The trigger is the same on both
// paths: din's catalog maintenance (flush_inlined_data / merge) rewriting the
// ducklake_inlined_data_* tables out from under an in-flight read makes the
// ducklake extension throw a FATAL ("Attempted to access index 0 within
// vector of size 0" in DuckLakeInlinedDataReader::TryInitializeScan).
// Depending on depth that either leaves the connection's catalog transaction
// permanently aborted — the pool re-serves it and every later query fails
// with "Current transaction is aborted" — or invalidates the ENTIRE embedded
// DuckDB instance. The materializer's decode loop recovers from both; the
// query fleet had no recovery at all, so a poisoned query pod parked
// Running-but-NotReady until someone deleted it (liveness probes the mon
// port and always passes).
//
// The query path has no single retry loop to hook, so recovery observes
// errors at the driver level instead: recoveringConnector wraps every pooled
// connection, funneling QueryContext/ExecContext outcomes — GraphQL reads,
// fetch RPCs, and the /ready probe alike — into one poisonRecovery.

// isDatabaseInvalidated reports the unrecoverable in-process failure mode: a
// ducklake-extension crash invalidates the whole embedded DuckDB instance and
// every future query on every connection fails with "database has been
// invalidated". No pool recycling can heal it; only a process restart can.
// Same classifier as the materializer's (1736873).
func isDatabaseInvalidated(err error) bool {
	return err != nil && strings.Contains(err.Error(), "database has been invalidated")
}

// isAbortedSession reports the recoverable failure mode: the connection's
// catalog transaction is permanently aborted ("Current transaction is
// aborted (please ROLLBACK)") and the session is useless, but a fresh
// connection re-bootstraps cleanly. Same classifier as the materializer's
// recoverPoisonedSession (1736873).
func isAbortedSession(err error) bool {
	return err != nil && strings.Contains(err.Error(), "transaction is aborted")
}

// poisonRecycleTotal makes query-pool poison recoveries alertable: a nonzero
// rate means the din-maintenance/read collision is firing in prod (expected
// occasionally under read load on the freshest ≤15m of data); a rising rate
// means the collision surface needs shrinking on the din side (#21 fix 4).
var poisonRecycleTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "dq_duckdb_poison_recycles_total",
	Help: "Idle-pool recycles triggered by an aborted DuckLake catalog session (din maintenance collision, #21).",
})

// poisonRecovery is the query-pool port of the materializer's two-strike
// recovery, adapted for a concurrent pool where "consecutive pass" doesn't
// exist. Strikes are judged by connection age instead: the first aborted
// session burns the idle pool; if a session created AFTER that recycle is
// still aborted, no connection can help — the poison is instance-level (the
// ducklake attach itself) — so terminate the process and let the pod
// supervisor restart it in seconds. Errors from stragglers still running on
// pre-recycle sessions only re-burn the idle pool (cheap, idempotent), never
// escalate: they say nothing about whether the recycle worked.
type poisonRecovery struct {
	db *sql.DB
	// fatal terminates the process (zerolog Fatal → os.Exit(1)); injectable
	// so tests can observe escalation instead of dying.
	fatal func(reason string, err error)

	mu          sync.Mutex
	lastRecycle time.Time // when the idle pool was last burned
	armed       bool      // recycled, and no post-recycle session has succeeded yet
}

func newPoisonRecovery() *poisonRecovery {
	return &poisonRecovery{
		fatal: func(reason string, err error) {
			zlog.Fatal().Err(err).Msg(reason + "; exiting so the pod supervisor restarts with a fresh DuckDB instance")
		},
	}
}

// observe classifies one query outcome from a connection created at born.
func (p *poisonRecovery) observe(err error, born time.Time) {
	if err == nil {
		p.noteHealthy(born)
		return
	}
	if isDatabaseInvalidated(err) {
		p.fatal("embedded database invalidated (ducklake crash under din maintenance)", err)
		return
	}
	if !isAbortedSession(err) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.armed && born.After(p.lastRecycle) {
		// This session was created after the pool was burned and is STILL
		// aborted: recycling can't heal it, the poison is instance-level.
		p.fatal("catalog session poison survived a connection recycle (instance-level ducklake invalidation)", err)
		return
	}
	// First strike, or a straggler on a pre-recycle session: burn the idle
	// pool so fresh sessions re-bootstrap (re-ATTACH). SetMaxIdleConns(0)
	// closes all idle conns — the poisoned one was just returned by the
	// failed query; restoring the limit lets fresh sessions re-open lazily.
	// Mirrors the materializer's recoverPoisonedSession (1736873).
	max := p.db.Stats().MaxOpenConnections
	p.db.SetMaxIdleConns(0)
	if max > 0 {
		p.db.SetMaxIdleConns(max)
	}
	p.lastRecycle = time.Now()
	p.armed = true
	poisonRecycleTotal.Inc()
	zlog.Warn().Err(err).Msg("poisoned catalog session detected on the query pool; recycled idle connections")
}

// noteHealthy disarms the escalation once a post-recycle session completes a
// query: the recycle healed the pool, so the NEXT collision (din maintenance
// fires every ~15m under read load) starts over at strike one instead of
// killing the pod.
func (p *poisonRecovery) noteHealthy(born time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.armed && born.After(p.lastRecycle) {
		p.armed = false
	}
}

// duckDriverConn is the exact optional-interface set *duckdb.Conn implements.
// The recovery wrapper must forward ALL of them — database/sql silently
// degrades when an optional interface is missing (e.g. no QueryerContext
// means an extra Prepare round-trip per query). The compile-time assertion
// below breaks the build if a duckdb-go upgrade changes the set; if the set
// GROWS (say driver.Validator), add the new interface here so the wrapper
// doesn't mask it.
type duckDriverConn interface {
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.ExecerContext
	driver.QueryerContext
	driver.NamedValueChecker
}

var _ duckDriverConn = (*duckdb.Conn)(nil)

// recoveringConnector wraps the duckdb connector so every pooled connection
// reports query outcomes to rec. Driver() is forwarded by embedding.
type recoveringConnector struct {
	driver.Connector
	rec *poisonRecovery
}

func (c *recoveringConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		// A dead instance can surface at connect time too: the bootstrap
		// (ATTACH) runs on connect and fails with "database has been
		// invalidated" once the ducklake crash killed the instance.
		if isDatabaseInvalidated(err) {
			c.rec.fatal("embedded database invalidated (ducklake crash under din maintenance)", err)
		}
		return nil, err
	}
	return &recoveringConn{duckDriverConn: conn.(duckDriverConn), rec: c.rec, born: time.Now()}, nil
}

// recoveringConn observes QueryContext/ExecContext outcomes — the only paths
// dq queries through (no prepared-statement reuse, no explicit txs on the
// query path) — and forwards everything else to the embedded conn. born
// orders this session against pool recycles for the two-strike logic.
type recoveringConn struct {
	duckDriverConn
	rec  *poisonRecovery
	born time.Time
}

func (c *recoveringConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.duckDriverConn.QueryContext(ctx, query, args)
	c.rec.observe(err, c.born)
	return rows, err
}

func (c *recoveringConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.duckDriverConn.ExecContext(ctx, query, args)
	c.rec.observe(err, c.born)
	return res, err
}
