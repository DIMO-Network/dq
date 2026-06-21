// Package materializer is the post-fact decode loop for the parse-on-read
// pipeline. It reads raw cloudevent parquet written by the din ingest
// service, decodes payloads through model-garage, and writes decoded
// signal/event parquet plus latest/summary bucket files back to the object
// store. A watermark cursor published after every batch tells the
// write-side compactor how far decoding has progressed.
//
// Commit protocol (crash safe, idempotent):
//
//  1. List the raw partition starting after watermark[partition]; take up
//     to BatchMaxFiles objects as one batch.
//  2. batchID = hex(sha256(join(sorted raw keys))). All output keys are
//     deterministic functions of the batch contents, so a replay
//     overwrites identical objects instead of duplicating rows.
//  3. Write decoded signal/event objects, then read-merge-write the
//     latest/summary buckets. Latest merges are max-by-timestamp and
//     naturally idempotent. Summary increments are gated twice: by
//     manifest existence (crash after manifest, before watermark) and by
//     a per-bucket batch stamp embedded in the parquet footer (crash in
//     the middle of the bucket update fan-out).
//  4. PUT the manifest, then PUT the updated watermark.json (single
//     atomic object write).
package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"path"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// ErrNotFound must be returned (possibly wrapped) by ObjectStore.GetObject
// when the requested key does not exist. Store adapters (e.g. S3) must
// translate their native not-found errors to this sentinel.
var ErrNotFound = errors.New("object not found")

// ObjectInfo describes a single object in the store.
type ObjectInfo struct {
	Key  string
	Size int64
}

// ObjectStore is the minimal blob-store surface the materializer needs.
type ObjectStore interface {
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	GetObject(ctx context.Context, key string) ([]byte, error)
	PutObject(ctx context.Context, key string, body []byte) error
}

// Config controls the materializer run loop.
type Config struct {
	// RawPrefix is the root of the raw cloudevent parquet layout
	// (raw/type=<ceType>/date=YYYY-MM-DD/<file>.parquet).
	RawPrefix string
	// DecodedPrefix is the root of the decoded output layout.
	DecodedPrefix string
	// PollInterval is how often Run polls for new raw objects.
	PollInterval time.Duration
	// ChainID and VehicleNFTAddress gate dimo.status signal decoding to
	// vehicle-NFT subjects, mirroring dis signalconvert and din's
	// decodestream. A zero VehicleNFTAddress disables the gate (tests).
	ChainID           uint64
	VehicleNFTAddress common.Address
	// BatchMaxFiles caps how many raw files are processed per batch.
	BatchMaxFiles int
	// BatchMaxBytes caps the total raw object size per batch; the whole
	// batch is decoded in memory, so this bounds the working set.
	BatchMaxBytes int64
	// Types is the list of cloudevent types to materialize.
	Types []string
	// SettleWindow excludes raw keys younger than this from batching. A
	// sink flush mints its object key before the PUT; a slow PUT could
	// otherwise land below an already-advanced cursor and never decode.
	// The window is the decode-lag floor. Defaults to 90s.
	SettleWindow time.Duration
	// Workers bounds the per-batch fan-out: concurrent raw-object
	// fetch+decode, model-garage conversion, data-object writes, and
	// bucket read-merge-writes. Defaults to GOMAXPROCS.
	Workers int
	// CompactInterval is how often closed decoded date partitions are
	// merged into one file each. Requires a store with DeleteObject;
	// negative disables. Defaults to 1h.
	CompactInterval time.Duration
	// CompactMinFiles is the per-partition file count that triggers a
	// merge. Defaults to 4.
	CompactMinFiles int
	// DecodedRetention deletes decoded rows older than this from the DuckLake
	// tables (CHD-38). Zero (default) disables it — a row-level TTL on customer
	// history is a product decision. Honored only on the ducklake path.
	DecodedRetention time.Duration
	// ShardIndex/ShardCount split raw partitions across N materializer
	// replicas by partition hash. Each shard owns disjoint partitions,
	// writes its own watermark file and its own latest/summary bucket
	// namespace, so shards never write the same object. 0/1 = the
	// single-replica layout, unchanged.
	ShardIndex int
	ShardCount int
}

// ownsPartition reports whether this shard processes the given raw
// partition ("type=T/date=D"). The same hash assigns decoded-compaction
// ownership (over "table/date=D" strings).
func (c Config) ownsPartition(partition string) bool {
	if c.ShardCount <= 1 {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(partition))
	return int(h.Sum32()%uint32(c.ShardCount)) == c.ShardIndex
}

const (
	defaultRawPrefix     = "raw/"
	defaultDecodedPrefix = "decoded/v1/"
	defaultPollInterval  = 15 * time.Second
	defaultBatchMaxFiles = 64
	defaultBatchMaxBytes = 1 << 30 // decode working set is a multiple of this

	// batchHashLen is how many hex chars of the batchID are embedded in
	// decoded data object names.
	batchHashLen = 16

	// kvBatchIDKey is the parquet footer key-value metadata key stamping
	// latest/summary bucket files with the last batch applied to them.
	// It makes bucket increments idempotent when a crash lands in the
	// middle of the bucket update fan-out.
	kvBatchIDKey = "dq:lastBatchID"

	datePartitionFormat = "2006-01-02"
)

func (c Config) withDefaults() Config {
	if c.RawPrefix == "" {
		c.RawPrefix = defaultRawPrefix
	}
	if c.DecodedPrefix == "" {
		c.DecodedPrefix = defaultDecodedPrefix
	}
	if !strings.HasSuffix(c.RawPrefix, "/") {
		c.RawPrefix += "/"
	}
	if !strings.HasSuffix(c.DecodedPrefix, "/") {
		c.DecodedPrefix += "/"
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.BatchMaxFiles <= 0 {
		c.BatchMaxFiles = defaultBatchMaxFiles
	}
	if c.BatchMaxBytes <= 0 {
		c.BatchMaxBytes = defaultBatchMaxBytes
	}
	if len(c.Types) == 0 {
		c.Types = []string{cloudevent.TypeStatus, cloudevent.TypeEvents}
	}
	if c.SettleWindow == 0 {
		c.SettleWindow = 90 * time.Second
	}
	if c.Workers <= 0 {
		c.Workers = runtime.GOMAXPROCS(0)
	}
	if c.CompactInterval == 0 {
		c.CompactInterval = time.Hour
	}
	if c.CompactMinFiles <= 0 {
		c.CompactMinFiles = 4
	}
	if c.ShardCount <= 0 {
		c.ShardCount = 1
	}
	if c.ShardIndex < 0 || c.ShardIndex >= c.ShardCount {
		c.ShardIndex = 0
	}
	return c
}

// Runner drives the decode loop.
type Runner struct {
	cfg   Config
	store ObjectStore
	log   zerolog.Logger
	// lake, when set, reads din's raw_events from the shared DuckLake
	// catalog via snapshot diffs and writes decoded tables there, bypassing
	// the bucket/hive path entirely (RunOnce delegates to it).
	lake *DuckLakeMaterializer
}

// New creates a Runner. Zero-valued config fields get defaults.
func New(cfg Config, store ObjectStore, log zerolog.Logger) *Runner {
	return &Runner{
		cfg:   cfg.withDefaults(),
		store: store,
		log:   log.With().Str("component", "materializer").Logger(),
	}
}

// WithDuckLake returns r configured to materialize from din's shared DuckLake
// catalog (raw_events → signals/events) via m. The bucket/hive read path and
// decoded-layer compaction are bypassed; the catalog transaction is the
// commit protocol.
func (r *Runner) WithDuckLake(m *DuckLakeMaterializer) *Runner {
	r.lake = m
	return r
}

// Run polls the raw layout until ctx is canceled. As long as a poll
// processed files without error it polls again immediately to drain the
// backlog; otherwise it waits PollInterval.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	// Start a full interval out so a restart loop doesn't compact every
	// boot; recovery of interrupted merges still runs inside CompactOnce.
	lastCompact := time.Now()
	lastPrune := time.Now()
	for {
		processed, err := r.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.log.Error().Err(err).Msg("materializer pass failed")
		} else if processed > 0 {
			continue // drain the backlog without waiting
		}
		if r.lake == nil {
			// Bucket layout only. In DuckLake mode the decoded tables
			// (lake.signals/events) are merged + expired by din's
			// catalog-wide maintenance (ducklake_merge_adjacent_files('lake')
			// etc. cover every table in the attachment) — exactly one
			// maintenance process per catalog, so dq runs none.
			r.maybeCompact(ctx, &lastCompact)
		} else {
			r.maybePrune(ctx, &lastPrune)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// pruneInterval is how often the ducklake path enforces DecodedRetention.
const pruneInterval = time.Hour

// maybePrune runs the decoded-retention prune at most once per pruneInterval
// when a retention window is configured (CHD-38). A no-op when DecodedRetention
// is zero (the default).
func (r *Runner) maybePrune(ctx context.Context, last *time.Time) {
	if r.cfg.DecodedRetention <= 0 || time.Since(*last) < pruneInterval {
		return
	}
	*last = time.Now()
	n, err := r.lake.PruneDecoded(ctx, r.cfg.DecodedRetention)
	if err != nil {
		pruneErrorsTotal.Inc()
		r.log.Error().Err(err).Msg("decoded retention prune failed")
		return
	}
	if n > 0 {
		r.log.Info().Int64("rows", n).Dur("retention", r.cfg.DecodedRetention).
			Msg("pruned decoded rows past retention")
	}
}

// RunOnce performs a single pass over every configured type: one batch per
// raw date partition with pending files. It returns the number of raw
// files fully processed.
func (r *Runner) RunOnce(ctx context.Context) (int, error) {
	if r.lake != nil {
		return r.lake.RunOnce(ctx, r)
	}
	watermark, err := r.loadWatermark(ctx)
	if err != nil {
		return 0, fmt.Errorf("loading watermark: %w", err)
	}

	processed := 0
	for _, ceType := range r.cfg.Types {
		batches, err := r.pendingBatches(ctx, ceType, watermark)
		if err != nil {
			return processed, err
		}
		observeLag(ceType, batches)
		for _, b := range batches {
			if ctx.Err() != nil {
				return processed, ctx.Err()
			}
			if err := r.processBatch(ctx, b, watermark); err != nil {
				return processed, fmt.Errorf("processing batch %s: %w", b.partition, err)
			}
			processed += len(b.keys)
			batchesTotal.WithLabelValues(ceType).Inc()
		}
		// Every pending batch for this type committed; anything newer
		// arrived after the list and gets measured next pass.
		lagSeconds.WithLabelValues(ceType).Set(0)
	}
	return processed, nil
}

// decodeEvents converts an already-reconstructed slice of raw events (e.g. a
// DuckLake snapshot delta over lake.raw_events) into a decodedBatch. Unlike
// decodeBatch it carries no batch-wide ceType: each event routes by its own
// Type (status→signals, events→events), and the vehicle gate applies per
// status event. Dedup is on the header uniqueness key; conversion fans out
// over Workers.
func (r *Runner) decodeEvents(ctx context.Context, events []cloudevent.RawEvent) *decodedBatch {
	dec := &decodedBatch{}
	seen := make(map[string]struct{}, len(events))
	jobs := make([]*cloudevent.RawEvent, 0, len(events))
	for i := range events {
		ev := &events[i]
		if _, dup := seen[ev.Key()]; dup {
			continue
		}
		seen[ev.Key()] = struct{}{}
		switch ev.Type {
		case cloudevent.TypeStatus:
			if !r.isVehicleSignalMessage(ev) {
				continue
			}
		case cloudevent.TypeEvents:
		default:
			continue // not a decoded type
		}
		jobs = append(jobs, ev)
	}

	r.convertJobs(ctx, dec, jobs, func(raw *cloudevent.RawEvent) string { return raw.Type })
	return dec
}

// convertJobs fans model-garage conversion across jobs — the CPU-heavy step,
// stateless per event — merging the per-job signal/event rows into dec in
// input order (writers re-sort anyway). routeType selects the converter for a
// job: the lake path keys on each event's own Type, the batch path on the
// partition's single ceType. The convert funcs never return an error (decode
// failures are counted into errorCount), so the errgroup wait is informational.
func (r *Runner) convertJobs(ctx context.Context, dec *decodedBatch, jobs []*cloudevent.RawEvent, routeType func(*cloudevent.RawEvent) string) {
	type convResult struct {
		signals []SignalRow
		events  []EventRow
		failed  int
	}
	results := make([]convResult, len(jobs))
	conv, convCtx := errgroup.WithContext(ctx)
	conv.SetLimit(r.cfg.Workers)
	for i, raw := range jobs {
		conv.Go(func() error {
			switch routeType(raw) {
			case cloudevent.TypeStatus:
				rows, failed := r.convertSignals(convCtx, raw)
				results[i] = convResult{signals: rows, failed: failed}
			case cloudevent.TypeEvents:
				rows, failed := r.convertEvents(convCtx, raw)
				results[i] = convResult{events: rows, failed: failed}
			}
			return nil
		})
	}
	_ = conv.Wait() // convert funcs never return error (failures are counted)
	for i := range results {
		dec.signals = append(dec.signals, results[i].signals...)
		dec.signalCount += len(results[i].signals)
		dec.events = append(dec.events, results[i].events...)
		dec.eventCount += len(results[i].events)
		dec.errorCount += results[i].failed
	}
}

// rawBatch is one unit of work: up to BatchMaxFiles raw objects from a
// single type/date partition, in lexicographic (= time) order.
type rawBatch struct {
	partition string // "type=<ceType>/date=YYYY-MM-DD"
	ceType    string
	keys      []string
}

// pendingBatches lists one raw type prefix and groups unprocessed keys by
// date partition. List+filter emulates S3 StartAfter using the watermark.
func (r *Runner) pendingBatches(ctx context.Context, ceType string, watermark map[string]string) ([]rawBatch, error) {
	prefix := r.cfg.RawPrefix + "type=" + ceType + "/"
	objects, err := r.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", prefix, err)
	}

	byPartition := make(map[string][]ObjectInfo)
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		rest := strings.TrimPrefix(obj.Key, prefix)
		datePart, _, ok := strings.Cut(rest, "/")
		if !ok || !strings.HasPrefix(datePart, "date=") {
			continue
		}
		partition := "type=" + ceType + "/" + datePart
		if !r.cfg.ownsPartition(partition) {
			continue // another shard's partition
		}
		if obj.Key <= watermark[partition] {
			continue // already processed
		}
		// Settle window: never advance the cursor over a key younger than
		// the slowest plausible sink flush — once the cursor passes a key,
		// that key can never be decoded.
		if ts := ingestKeyTime(obj.Key); !ts.IsZero() && time.Since(ts) < r.cfg.SettleWindow {
			continue
		}
		byPartition[partition] = append(byPartition[partition], obj)
	}

	partitions := make([]string, 0, len(byPartition))
	for p := range byPartition {
		partitions = append(partitions, p)
	}
	sort.Strings(partitions)

	batches := make([]rawBatch, 0, len(partitions))
	for _, p := range partitions {
		infos := byPartition[p]
		sort.Slice(infos, func(i, j int) bool { return infos[i].Key < infos[j].Key })
		// Cut on file count or aggregate bytes, whichever hits first — the
		// whole batch is decoded in memory. Always take at least one file
		// so an oversized object can't stall the watermark.
		var keys []string
		var bytesTotal int64
		for _, info := range infos {
			if len(keys) >= r.cfg.BatchMaxFiles {
				break
			}
			if len(keys) > 0 && bytesTotal+info.Size > r.cfg.BatchMaxBytes {
				break
			}
			keys = append(keys, info.Key)
			bytesTotal += info.Size
		}
		batches = append(batches, rawBatch{partition: p, ceType: ceType, keys: keys})
	}
	return batches, nil
}

// processBatch runs the full commit protocol for one batch and advances
// the watermark map in place on success.
func (r *Runner) processBatch(ctx context.Context, b rawBatch, watermark map[string]string) error {
	batchID := computeBatchID(b.keys)
	manifestKey := r.manifestKey(batchID)

	// Step 0: gate. A manifest written by a previous (crashed) run of this
	// exact batch means latest/summary increments were already applied.
	manifestExists, err := r.objectExists(ctx, manifestKey)
	if err != nil {
		return fmt.Errorf("checking manifest %s: %w", manifestKey, err)
	}

	dec, err := r.decodeBatch(ctx, b)
	if err != nil {
		return err
	}

	// Step 1: decoded data objects. Keys are deterministic from the batch,
	// so a replay overwrites byte-identical content instead of duplicating.
	outputs, err := r.writeDataObjects(ctx, b, batchID, dec)
	if err != nil {
		return err
	}

	if !manifestExists {
		// Step 2: latest/summary read-merge-write, idempotent per bucket
		// via the embedded batch stamp. The two bucket families are
		// disjoint key sets — update them concurrently.
		bg, bgCtx := errgroup.WithContext(ctx)
		bg.Go(func() error { return r.updateLatestBuckets(bgCtx, batchID, dec.signals) })
		bg.Go(func() error { return r.updateSummaryBuckets(bgCtx, batchID, dec.signals) })
		if err := bg.Wait(); err != nil {
			return err
		}

		// Step 3: manifest. From here on the batch counts as applied.
		manifest := batchManifest{
			BatchID:            batchID,
			Inputs:             b.keys,
			Outputs:            outputs,
			SignalCount:        dec.signalCount,
			EventCount:         dec.eventCount,
			ErrorCount:         dec.errorCount,
			ModelGarageVersion: modelGarageVersion(),
		}
		if err := r.putJSON(ctx, manifestKey, manifest); err != nil {
			return fmt.Errorf("writing manifest: %w", err)
		}
	} else {
		r.log.Info().Str("batchId", batchID).Str("partition", b.partition).
			Msg("manifest already present, skipping bucket increments (replay)")
	}

	// Step 4: watermark, single atomic PUT.
	watermark[b.partition] = b.keys[len(b.keys)-1]
	if err := r.putJSON(ctx, r.watermarkKey(), watermark); err != nil {
		return fmt.Errorf("writing watermark: %w", err)
	}

	rowsTotal.WithLabelValues("signals").Add(float64(dec.signalCount))
	rowsTotal.WithLabelValues("events").Add(float64(dec.eventCount))
	errorsTotal.Add(float64(dec.errorCount))

	r.log.Info().
		Str("partition", b.partition).
		Str("batchId", batchID).
		Int("files", len(b.keys)).
		Int("signals", dec.signalCount).
		Int("events", dec.eventCount).
		Int("errors", dec.errorCount).
		Msg("materialized batch")
	return nil
}

// decodedBatch holds everything decoded out of one raw batch.
type decodedBatch struct {
	signals []SignalRow
	events  []EventRow

	signalCount int
	eventCount  int
	errorCount  int
}

// decodeBatch downloads each raw object, decodes the stored cloudevents,
// and converts them with model-garage. Conversion failures never block the
// batch: partial decodes are salvaged and failures only bump errorCount.
func (r *Runner) decodeBatch(ctx context.Context, b rawBatch) (*decodedBatch, error) {
	dec := &decodedBatch{}

	// Stage 1: fetch + parquet-decode every raw object concurrently (I/O
	// bound against S3). Results stay indexed by input position so the
	// dedup pass below is deterministic.
	decoded := make([][]cloudevent.StoredEvent, len(b.keys))
	var undecodable atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.cfg.Workers)
	for i, key := range b.keys {
		g.Go(func() error {
			data, err := r.store.GetObject(gctx, key)
			if err != nil {
				return fmt.Errorf("getting raw object %s: %w", key, err)
			}
			events, err := ceparquet.Decode(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				// An unreadable raw file is terminal for its rows but must
				// not block the watermark; the bytes stay in place for
				// recovery.
				r.log.Error().Err(err).Str("key", key).Msg("undecodable raw parquet file")
				undecodable.Add(1)
				return nil
			}
			decoded[i] = events
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	dec.errorCount += int(undecodable.Load())

	// Stage 2: sequential dedup in key order (first occurrence wins,
	// deterministic). At-least-once ingest can land the same event in
	// multiple raw bundles within one batch; dedup on the header
	// uniqueness key so decoded rows (and summary counts) see each event
	// once. Cross-batch duplicates are rare (redelivery lands in adjacent
	// bundles) and collapse later in raw compaction.
	seen := make(map[string]struct{})
	var jobs []*cloudevent.RawEvent
	for i := range decoded {
		for j := range decoded[i] {
			ev := &decoded[i][j]
			if _, dup := seen[ev.Key()]; dup {
				continue
			}
			seen[ev.Key()] = struct{}{}
			if b.ceType == cloudevent.TypeStatus && !r.isVehicleSignalMessage(&ev.RawEvent) {
				continue
			}
			jobs = append(jobs, &ev.RawEvent)
		}
		// Events still referenced by jobs stay live through their backing
		// array; dropping the outer slice lets everything skipped (dups,
		// non-vehicle subjects) get collected before conversion.
		decoded[i] = nil
	}

	// Stage 3: model-garage conversion is the CPU-heavy step — fan out.
	// Conversion is stateless per event; results are merged in input order
	// so output content stays deterministic (writers re-sort anyway). Every
	// event in the batch shares the partition's ceType.
	r.convertJobs(ctx, dec, jobs, func(*cloudevent.RawEvent) string { return b.ceType })
	return dec, nil
}

// isVehicleSignalMessage mirrors dis signalconvert: only ERC-721 vehicle
// subjects on the configured chain decode to signals. Disabled when no
// vehicle contract is configured.
func (r *Runner) isVehicleSignalMessage(rawEvent *cloudevent.RawEvent) bool {
	if r.cfg.VehicleNFTAddress == (common.Address{}) {
		return true
	}
	did, err := cloudevent.DecodeERC721DID(rawEvent.Subject)
	if err != nil {
		return false
	}
	return did.ChainID == r.cfg.ChainID && did.ContractAddress.Cmp(r.cfg.VehicleNFTAddress) == 0
}

// writeDataObjects writes the decoded signal and event parquet objects,
// partitioned by the decoded row's own timestamp date (UTC). Returns the
// keys written, sorted.
func (r *Runner) writeDataObjects(ctx context.Context, b rawBatch, batchID string, dec *decodedBatch) ([]string, error) {
	firstBase := strings.TrimSuffix(path.Base(b.keys[0]), ".parquet")
	objectName := "batch-" + firstBase + "-" + batchID[:batchHashLen] + ".parquet"

	signalsByDate := make(map[string][]SignalRow)
	for _, row := range dec.signals {
		date := row.Timestamp.UTC().Format(datePartitionFormat)
		signalsByDate[date] = append(signalsByDate[date], row)
	}
	eventsByDate := make(map[string][]EventRow)
	for _, row := range dec.events {
		date := row.Timestamp.UTC().Format(datePartitionFormat)
		eventsByDate[date] = append(eventsByDate[date], row)
	}

	// Encode (zstd, CPU) and PUT (S3 latency) every date partition
	// concurrently; each output key is independent.
	var (
		mu      sync.Mutex
		outputs []string
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.cfg.Workers)
	for _, date := range sortedKeys(signalsByDate) {
		key := r.cfg.DecodedPrefix + "signals/date=" + date + "/" + objectName
		rows := signalsByDate[date]
		g.Go(func() error {
			body, err := writeSignalParquet(rows)
			if err != nil {
				return fmt.Errorf("encoding signal parquet: %w", err)
			}
			if err := r.store.PutObject(gctx, key, body); err != nil {
				return fmt.Errorf("writing %s: %w", key, err)
			}
			mu.Lock()
			outputs = append(outputs, key)
			mu.Unlock()
			return nil
		})
	}
	for _, date := range sortedKeys(eventsByDate) {
		key := r.cfg.DecodedPrefix + "events/date=" + date + "/" + objectName
		rows := eventsByDate[date]
		g.Go(func() error {
			body, err := writeEventParquet(rows)
			if err != nil {
				return fmt.Errorf("encoding event parquet: %w", err)
			}
			if err := r.store.PutObject(gctx, key, body); err != nil {
				return fmt.Errorf("writing %s: %w", key, err)
			}
			mu.Lock()
			outputs = append(outputs, key)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	sort.Strings(outputs)
	return outputs, nil
}

func (r *Runner) objectExists(ctx context.Context, key string) (bool, error) {
	_, err := r.store.GetObject(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *Runner) watermarkKey() string {
	if r.cfg.ShardCount <= 1 {
		return r.cfg.DecodedPrefix + "_state/watermark.json"
	}
	return fmt.Sprintf("%s_state/watermark-p%03dof%03d.json", r.cfg.DecodedPrefix, r.cfg.ShardIndex, r.cfg.ShardCount)
}

func (r *Runner) manifestKey(batchID string) string {
	return r.cfg.DecodedPrefix + "_state/manifests/" + batchID + ".json"
}

// computeBatchID derives the deterministic batch identity from the sorted
// raw input keys.
func computeBatchID(keys []string) string {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:])
}

// modelGarageVersion reports the model-garage module version compiled into
// this binary, or "" when build info is unavailable.
func modelGarageVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/DIMO-Network/model-garage" {
			if dep.Replace != nil {
				return dep.Replace.Version
			}
			return dep.Version
		}
	}
	return ""
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
