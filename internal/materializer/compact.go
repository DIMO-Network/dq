// compact.go merges the per-batch decoded data objects into one file per
// closed date partition. The materializer writes one object per batch per
// day, so partitions accumulate small files; queries pay one GET per file.
//
// Aggregation queries do NOT dedup decoded rows (sum/avg would double
// count), so unlike raw compaction there is no grace window where sources
// and merged output are both visible. The protocol trades a brief
// under-count window for never double-counting:
//
//  1. PUT the merged file under <decodedPrefix>_compaction/ (staging —
//     outside every query glob).
//  2. PUT a manifest naming staged file, target key, and sources.
//  3. DELETE the sources.
//  4. PUT the target, DELETE staged + manifest.
//
// A crash leaves either: nothing applied (no manifest), or a manifest whose
// recovery is idempotent — finish deleting sources, publish staged to
// target, clean up. Readers never see sources and target together.
package materializer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

// CompactStore is ObjectStore plus deletion; decoded compaction rewrites
// partitions in place and must remove its merge inputs.
type CompactStore interface {
	ObjectStore
	DeleteObject(ctx context.Context, key string) error
}

const (
	compactionPrefix = "_compaction/"
	// compactedName is the merged output per partition. A fixed name makes
	// recompaction (late batches after a first compaction) overwrite
	// atomically instead of accumulating merge generations.
	compactedName = "compacted.parquet"
)

// compactionManifest records an in-flight partition merge for recovery.
type compactionManifest struct {
	Target  string   `json:"target"`
	Staged  string   `json:"staged"`
	Sources []string `json:"sources"`
}

// CompactOnce recovers any interrupted merge, then compacts every closed
// (past-date, UTC) signals/events partition holding at least
// CompactMinFiles objects. Returns the number of partitions compacted.
func (r *Runner) CompactOnce(ctx context.Context) (int, error) {
	deleter, ok := r.store.(CompactStore)
	if !ok {
		return 0, fmt.Errorf("store %T does not support deletion; decoded compaction disabled", r.store)
	}
	if err := r.recoverCompactions(ctx, deleter); err != nil {
		return 0, err
	}

	today := time.Now().UTC().Format(datePartitionFormat)
	compacted := 0
	for _, table := range []string{"signals", "events"} {
		prefix := r.cfg.DecodedPrefix + table + "/"
		objects, err := r.store.List(ctx, prefix)
		if err != nil {
			return compacted, fmt.Errorf("listing %s: %w", prefix, err)
		}
		byDate := make(map[string][]string)
		for _, obj := range objects {
			rest := strings.TrimPrefix(obj.Key, prefix)
			dir, file := path.Split(rest)
			date, found := strings.CutPrefix(strings.TrimSuffix(dir, "/"), "date=")
			if !found || !strings.HasSuffix(file, ".parquet") {
				continue
			}
			byDate[date] = append(byDate[date], obj.Key)
		}
		for _, date := range sortedKeys(byDate) {
			keys := byDate[date]
			if date >= today || len(keys) < r.cfg.CompactMinFiles {
				continue
			}
			if err := r.compactPartition(ctx, deleter, table, date, keys); err != nil {
				return compacted, fmt.Errorf("compacting %s date=%s: %w", table, date, err)
			}
			compacted++
		}
	}
	return compacted, nil
}

// compactPartition merges keys into one object at
// <decodedPrefix><table>/date=<date>/compacted.parquet.
func (r *Runner) compactPartition(ctx context.Context, store CompactStore, table, date string, keys []string) error {
	sort.Strings(keys)
	bodies := make([][]byte, len(keys))
	for i, key := range keys {
		body, err := r.store.GetObject(ctx, key)
		if err != nil {
			return fmt.Errorf("reading %s: %w", key, err)
		}
		bodies[i] = body
	}

	var merged []byte
	var err error
	switch table {
	case "signals":
		merged, err = mergeDecodedParquet(bodies, writeSignalParquet, signalRowKey)
	case "events":
		merged, err = mergeDecodedParquet(bodies, writeEventParquet, eventRowKey)
	default:
		return fmt.Errorf("unknown decoded table %q", table)
	}
	if err != nil {
		return err
	}

	target := fmt.Sprintf("%s%s/date=%s/%s", r.cfg.DecodedPrefix, table, date, compactedName)
	id := fmt.Sprintf("%s-%s", table, date)
	staged := r.cfg.DecodedPrefix + compactionPrefix + id + ".parquet"
	manifestKey := r.cfg.DecodedPrefix + compactionPrefix + id + ".json"

	// Sources may include a previous compacted.parquet (recompaction after
	// late batches); it must not be deleted after the new target lands.
	sources := make([]string, 0, len(keys))
	for _, key := range keys {
		if key != target {
			sources = append(sources, key)
		}
	}

	if err := store.PutObject(ctx, staged, merged); err != nil {
		return fmt.Errorf("staging merge: %w", err)
	}
	if err := r.putJSON(ctx, manifestKey, compactionManifest{Target: target, Staged: staged, Sources: sources}); err != nil {
		return fmt.Errorf("writing compaction manifest: %w", err)
	}
	if err := r.finishCompaction(ctx, store, compactionManifest{Target: target, Staged: staged, Sources: sources}, manifestKey, merged); err != nil {
		return err
	}
	compactionsTotal.Inc()
	r.log.Info().Str("table", table).Str("date", date).Int("sources", len(sources)).
		Msg("compacted decoded partition")
	return nil
}

// finishCompaction performs the visible part of a merge: delete sources,
// publish the target, clean up staging. body may be nil (recovery), in
// which case the staged object is read back.
func (r *Runner) finishCompaction(ctx context.Context, store CompactStore, m compactionManifest, manifestKey string, body []byte) error {
	if body == nil {
		var err error
		body, err = r.store.GetObject(ctx, m.Staged)
		if err != nil {
			return fmt.Errorf("reading staged merge %s: %w", m.Staged, err)
		}
	}
	for _, src := range m.Sources {
		if err := store.DeleteObject(ctx, src); err != nil {
			return fmt.Errorf("deleting source %s: %w", src, err)
		}
	}
	if err := store.PutObject(ctx, m.Target, body); err != nil {
		return fmt.Errorf("publishing %s: %w", m.Target, err)
	}
	if err := store.DeleteObject(ctx, m.Staged); err != nil {
		return fmt.Errorf("deleting staged %s: %w", m.Staged, err)
	}
	if err := store.DeleteObject(ctx, manifestKey); err != nil {
		return fmt.Errorf("deleting manifest %s: %w", manifestKey, err)
	}
	return nil
}

// recoverCompactions finishes merges interrupted by a crash. Safe to call
// on every cycle; with no leftover manifests it only costs one List.
func (r *Runner) recoverCompactions(ctx context.Context, store CompactStore) error {
	objects, err := r.store.List(ctx, r.cfg.DecodedPrefix+compactionPrefix)
	if err != nil {
		return fmt.Errorf("listing compaction staging: %w", err)
	}
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		data, err := r.store.GetObject(ctx, obj.Key)
		if err != nil {
			return fmt.Errorf("reading compaction manifest %s: %w", obj.Key, err)
		}
		var m compactionManifest
		if err := json.Unmarshal(data, &m); err != nil {
			return fmt.Errorf("decoding compaction manifest %s: %w", obj.Key, err)
		}
		// The staged object is the discriminator: it is deleted only after
		// the target is published, so staged-missing means the merge
		// completed and only manifest cleanup remains. (Checking the target
		// instead would be wrong for recompaction, where the target key
		// already exists from the previous generation.)
		body, err := r.store.GetObject(ctx, m.Staged)
		if errors.Is(err, ErrNotFound) {
			if err := store.DeleteObject(ctx, obj.Key); err != nil {
				return fmt.Errorf("cleaning up compaction manifest %s: %w", obj.Key, err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("reading staged merge %s: %w", m.Staged, err)
		}
		if err := r.finishCompaction(ctx, store, m, obj.Key, body); err != nil {
			return fmt.Errorf("recovering compaction %s: %w", obj.Key, err)
		}
		r.log.Info().Str("target", m.Target).Msg("recovered interrupted decoded compaction")
	}
	// Orphaned staged files (crash between staged PUT and manifest PUT)
	// are invisible to queries and harmlessly overwritten by the next
	// merge of the same partition; no cleanup needed for correctness.
	return nil
}

// mergeDecodedParquet decodes every body, dedups rows on key (cross-batch
// at-least-once duplicates), and re-encodes one sorted file.
func mergeDecodedParquet[T any](bodies [][]byte, write func([]T) ([]byte, error), key func(*T) string) ([]byte, error) {
	var all []T
	for _, body := range bodies {
		rows, _, err := readParquet[T](body)
		if err != nil {
			return nil, fmt.Errorf("decoding merge input: %w", err)
		}
		all = append(all, rows...)
	}
	seen := make(map[string]struct{}, len(all))
	out := all[:0]
	for i := range all {
		k := key(&all[i])
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, all[i])
	}
	return write(out)
}

func signalRowKey(r *SignalRow) string {
	return r.CloudEventID + "|" + r.Name + "|" + r.Timestamp.UTC().Format(time.RFC3339Nano)
}

func eventRowKey(r *EventRow) string {
	return r.CloudEventID + "|" + r.Name + "|" + r.Timestamp.UTC().Format(time.RFC3339Nano)
}

// maybeCompact runs CompactOnce when the interval has elapsed and the
// store supports deletion. Called from the Run loop.
func (r *Runner) maybeCompact(ctx context.Context, last *time.Time) {
	if r.cfg.CompactInterval <= 0 {
		return
	}
	if _, ok := r.store.(CompactStore); !ok {
		return
	}
	if time.Since(*last) < r.cfg.CompactInterval {
		return
	}
	*last = time.Now()
	n, err := r.CompactOnce(ctx)
	if err != nil {
		r.log.Error().Err(err).Msg("decoded compaction failed")
		return
	}
	if n > 0 {
		r.log.Info().Int("partitions", n).Msg("decoded compaction cycle complete")
	}
}
