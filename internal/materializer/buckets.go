package materializer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"

	"golang.org/x/sync/errgroup"
)

// numBuckets is the number of latest/summary hash buckets.
const numBuckets = 256

// lastSeenFieldName is the virtual signal name carrying the
// per-(subject, source) max(timestamp) across all signals, replicating
// getLastSeenQuery in internal/service/ch/queries.go. Must stay equal to
// model.LastSeenField (internal/graph/model/signalArgs.go).
const lastSeenFieldName = "lastSeen"

// hashBucket maps a subject to its latest/summary bucket.
// NOTE: this must match duck.HashBucket (built in parallel on the
// query/compactor side); both are fnv32a(subject) % 256.
func hashBucket(subject string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(subject))
	return h.Sum32() % numBuckets
}

type latestKey struct {
	subject string
	source  string
	name    string
}

func (k latestKey) compare(o latestKey) int {
	return cmp.Or(
		cmp.Compare(k.subject, o.subject),
		cmp.Compare(k.source, o.source),
		cmp.Compare(k.name, o.name),
	)
}

func (r *Runner) latestBucketKey(bucket uint32) string {
	return fmt.Sprintf("%slatest/bucket=%03d/latest.parquet", r.cfg.DecodedPrefix, bucket)
}

func (r *Runner) summaryBucketKey(bucket uint32) string {
	return fmt.Sprintf("%ssummary/bucket=%03d/summary.parquet", r.cfg.DecodedPrefix, bucket)
}

// updateLatestBuckets read-merge-writes every latest bucket touched by the
// batch. The merge is max-by-timestamp, so re-applying the same batch is a
// no-op; buckets already stamped with this batchID are skipped outright.
func (r *Runner) updateLatestBuckets(ctx context.Context, batchID string, signals []SignalRow) error {
	byBucket := make(map[uint32][]SignalRow)
	for _, row := range signals {
		b := hashBucket(row.Subject)
		byBucket[b] = append(byBucket[b], row)
	}

	// Buckets are disjoint files — read-merge-write them concurrently.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.cfg.Workers)
	for _, bucket := range sortedBuckets(byBucket) {
		g.Go(func() error {
			key := r.latestBucketKey(bucket)
			existing, stamp, err := loadBucket[LatestRow](gctx, r.store, key)
			if err != nil {
				return fmt.Errorf("reading latest bucket %s: %w", key, err)
			}
			if stamp == batchID {
				return nil // already applied by a crashed run of this batch
			}

			merged := make(map[latestKey]*LatestRow, len(existing))
			for i := range existing {
				row := existing[i]
				merged[latestKey{subject: row.Subject, source: row.Source, name: row.Name}] = &row
			}
			for _, sig := range byBucket[bucket] {
				applySignalToLatest(merged, sig)
			}

			rows := sortedRowValues(merged)
			body, err := writeBucketParquet(rows, batchID)
			if err != nil {
				return fmt.Errorf("encoding latest bucket %s: %w", key, err)
			}
			if err := r.store.PutObject(gctx, key, body); err != nil {
				return fmt.Errorf("writing latest bucket %s: %w", key, err)
			}
			return nil
		})
	}
	return g.Wait()
}

func sortedBuckets[V any](m map[uint32]V) []uint32 {
	buckets := make([]uint32, 0, len(m))
	for b := range m {
		buckets = append(buckets, b)
	}
	slices.Sort(buckets)
	return buckets
}

// applySignalToLatest merges one decoded signal row into the latest map.
// Plain columns follow argMax(value, timestamp) semantics; the
// loc_*_nonzero columns follow argMaxIf/maxIf with latestLocationCond
// (latitude != 0 OR longitude != 0); and the virtual lastSeen row tracks
// max(timestamp) per (subject, source) regardless of name.
func applySignalToLatest(m map[latestKey]*LatestRow, row SignalRow) {
	k := latestKey{subject: row.Subject, source: row.Source, name: row.Name}
	cur, ok := m[k]
	if !ok {
		cur = &LatestRow{Name: row.Name, Subject: row.Subject, Source: row.Source}
		// Timestamp is zero, so the first apply below always wins.
		m[k] = cur
	}
	if row.Timestamp.After(cur.Timestamp) {
		cur.Timestamp = row.Timestamp
		cur.ValueNumber = row.ValueNumber
		cur.ValueString = row.ValueString
		cur.LocLat = row.LocLat
		cur.LocLon = row.LocLon
		cur.LocHDOP = row.LocHDOP
		cur.LocHeading = row.LocHeading
	}
	// latestLocationCond: exclude (0, 0) coordinates from the
	// latest-location tracking, mirroring the ClickHouse argMaxIf/maxIf.
	if (row.LocLat != 0 || row.LocLon != 0) && row.Timestamp.After(cur.LocNonzeroTS) {
		cur.LocNonzeroTS = row.Timestamp
		cur.LocLatNonzero = row.LocLat
		cur.LocLonNonzero = row.LocLon
		cur.LocHDOPNonzero = row.LocHDOP
		cur.LocHeadingNonzero = row.LocHeading
	}

	// Virtual lastSeen row, replicating getLastSeenQuery.
	lk := latestKey{subject: row.Subject, source: row.Source, name: lastSeenFieldName}
	seen, ok := m[lk]
	if !ok {
		seen = &LatestRow{Name: lastSeenFieldName, Subject: row.Subject, Source: row.Source}
		m[lk] = seen
	}
	if row.Timestamp.After(seen.Timestamp) {
		seen.Timestamp = row.Timestamp
	}
}

// updateSummaryBuckets read-merge-writes every summary bucket touched by
// the batch: count += rows, first_seen = min, last_seen = max per
// (subject, source, name). Increments are idempotent across crash replays
// because each bucket file is stamped with the batch that last updated
// it; a bucket already stamped with this batchID is skipped.
func (r *Runner) updateSummaryBuckets(ctx context.Context, batchID string, signals []SignalRow) error {
	byBucket := make(map[uint32][]SignalRow)
	for _, row := range signals {
		b := hashBucket(row.Subject)
		byBucket[b] = append(byBucket[b], row)
	}

	// Buckets are disjoint files — read-merge-write them concurrently.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.cfg.Workers)
	for _, bucket := range sortedBuckets(byBucket) {
		g.Go(func() error {
			key := r.summaryBucketKey(bucket)
			existing, stamp, err := loadBucket[SummaryRow](gctx, r.store, key)
			if err != nil {
				return fmt.Errorf("reading summary bucket %s: %w", key, err)
			}
			if stamp == batchID {
				return nil // already applied by a crashed run of this batch
			}

			merged := make(map[latestKey]*SummaryRow, len(existing))
			for i := range existing {
				row := existing[i]
				merged[latestKey{subject: row.Subject, source: row.Source, name: row.Name}] = &row
			}
			for _, sig := range byBucket[bucket] {
				k := latestKey{subject: sig.Subject, source: sig.Source, name: sig.Name}
				cur, ok := merged[k]
				if !ok {
					merged[k] = &SummaryRow{
						Subject:   sig.Subject,
						Source:    sig.Source,
						Name:      sig.Name,
						Count:     1,
						FirstSeen: sig.Timestamp,
						LastSeen:  sig.Timestamp,
					}
					continue
				}
				cur.Count++
				if sig.Timestamp.Before(cur.FirstSeen) {
					cur.FirstSeen = sig.Timestamp
				}
				if sig.Timestamp.After(cur.LastSeen) {
					cur.LastSeen = sig.Timestamp
				}
			}

			rows := sortedRowValues(merged)
			body, err := writeBucketParquet(rows, batchID)
			if err != nil {
				return fmt.Errorf("encoding summary bucket %s: %w", key, err)
			}
			if err := r.store.PutObject(gctx, key, body); err != nil {
				return fmt.Errorf("writing summary bucket %s: %w", key, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// loadBucket reads a latest/summary bucket object, treating a missing
// object as an empty bucket.
func loadBucket[T any](ctx context.Context, store ObjectStore, key string) ([]T, string, error) {
	data, err := store.GetObject(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return readParquet[T](data)
}

func sortedRowValues[T any](m map[latestKey]*T) []T {
	keys := make([]latestKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, latestKey.compare)
	rows := make([]T, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, *m[k])
	}
	return rows
}
