package materializer

import "context"

// LatestPublisher receives every decoded signal batch so an external
// latest-value cache (the NATS KV signals-latest bucket, internal/latestkv)
// can fold it in. Implementations MUST be best-effort: swallow/record failures
// and return promptly — a cache outage must degrade to staleness, never block
// or fail decode. Publishes are called with batches that may replay (crash
// recovery, redelivery, backfill), so implementations must be idempotent
// (last-write-wins by (timestamp, cloud_event_id)).
type LatestPublisher interface {
	PublishLatest(ctx context.Context, rows []SignalRow)
}

// WithLatestPublisher wires p to receive every decoded signal batch. nil (the
// default) disables publishing. Returns m for chaining.
func (m *DuckLakeMaterializer) WithLatestPublisher(p LatestPublisher) *DuckLakeMaterializer {
	m.latestPub = p
	return m
}

// publishLatest hands the batch's signals to the publisher, if any. Called
// BEFORE the catalog transaction, deliberately:
//   - Ordering: publish-then-commit means a crash can lose the COMMIT (the
//     un-advanced cursor re-decodes the span and re-publishes, a fold no-op)
//     but never a committed batch's publish. Commit-then-publish would have
//     the reverse failure: a committed batch whose cache update is silently
//     gone until that subject's next reading.
//   - Latency: the publish does its NATS round trips without holding the
//     DuckDB connection or stretching the catalog transaction's conflict
//     window against din's maintenance.
//
// The transient cost is a cache momentarily AHEAD of the lake when the commit
// then fails — harmless for a latest-value read, and the retried span
// converges the lake to the published values.
func (m *DuckLakeMaterializer) publishLatest(ctx context.Context, dec *decodedBatch) {
	if m.latestPub == nil || len(dec.signals) == 0 {
		return
	}
	m.latestPub.PublishLatest(ctx, dec.signals)
}
