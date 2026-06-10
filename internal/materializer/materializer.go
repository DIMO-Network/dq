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
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
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
	// Types is the list of cloudevent types to materialize.
	Types []string
}

const (
	defaultRawPrefix     = "raw/"
	defaultDecodedPrefix = "decoded/v1/"
	defaultPollInterval  = 15 * time.Second
	defaultBatchMaxFiles = 64

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
	if len(c.Types) == 0 {
		c.Types = []string{cloudevent.TypeStatus, cloudevent.TypeEvents}
	}
	return c
}

// Runner drives the decode loop.
type Runner struct {
	cfg   Config
	store ObjectStore
	log   zerolog.Logger
}

// New creates a Runner. Zero-valued config fields get defaults.
func New(cfg Config, store ObjectStore, log zerolog.Logger) *Runner {
	return &Runner{
		cfg:   cfg.withDefaults(),
		store: store,
		log:   log.With().Str("component", "materializer").Logger(),
	}
}

// Run polls the raw layout until ctx is canceled. As long as a poll
// processed files without error it polls again immediately to drain the
// backlog; otherwise it waits PollInterval.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
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
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// RunOnce performs a single pass over every configured type: one batch per
// raw date partition with pending files. It returns the number of raw
// files fully processed.
func (r *Runner) RunOnce(ctx context.Context) (int, error) {
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
		for _, b := range batches {
			if ctx.Err() != nil {
				return processed, ctx.Err()
			}
			if err := r.processBatch(ctx, b, watermark); err != nil {
				return processed, fmt.Errorf("processing batch %s: %w", b.partition, err)
			}
			processed += len(b.keys)
		}
	}
	return processed, nil
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

	byPartition := make(map[string][]string)
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
		if obj.Key <= watermark[partition] {
			continue // already processed
		}
		byPartition[partition] = append(byPartition[partition], obj.Key)
	}

	partitions := make([]string, 0, len(byPartition))
	for p := range byPartition {
		partitions = append(partitions, p)
	}
	sort.Strings(partitions)

	batches := make([]rawBatch, 0, len(partitions))
	for _, p := range partitions {
		keys := byPartition[p]
		sort.Strings(keys)
		if len(keys) > r.cfg.BatchMaxFiles {
			keys = keys[:r.cfg.BatchMaxFiles]
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
		// via the embedded batch stamp.
		if err := r.updateLatestBuckets(ctx, batchID, dec.signals); err != nil {
			return err
		}
		if err := r.updateSummaryBuckets(ctx, batchID, dec.signals); err != nil {
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
	// At-least-once ingest can land the same event in multiple raw bundles
	// within one batch; dedup on the header uniqueness key so decoded rows
	// (and summary counts) see each event once. Cross-batch duplicates are
	// rare (redelivery lands in adjacent bundles) and collapse later in
	// raw compaction.
	seen := make(map[string]struct{})
	for _, key := range b.keys {
		data, err := r.store.GetObject(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("getting raw object %s: %w", key, err)
		}
		events, err := ceparquet.Decode(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			// An unreadable raw file is terminal for its rows but must not
			// block the watermark; the bytes stay in place for recovery.
			r.log.Error().Err(err).Str("key", key).Msg("undecodable raw parquet file")
			dec.errorCount++
			continue
		}
		for i := range events {
			eventKey := events[i].Key()
			if _, dup := seen[eventKey]; dup {
				continue
			}
			seen[eventKey] = struct{}{}
			switch b.ceType {
			case cloudevent.TypeStatus:
				if !r.isVehicleSignalMessage(&events[i].RawEvent) {
					continue
				}
				rows, failed := r.convertSignals(ctx, &events[i].RawEvent)
				dec.signals = append(dec.signals, rows...)
				dec.signalCount += len(rows)
				dec.errorCount += failed
			case cloudevent.TypeEvents:
				rows, failed := r.convertEvents(ctx, &events[i].RawEvent)
				dec.events = append(dec.events, rows...)
				dec.eventCount += len(rows)
				dec.errorCount += failed
			}
		}
	}
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

	var outputs []string

	signalsByDate := make(map[string][]SignalRow)
	for _, row := range dec.signals {
		date := row.Timestamp.UTC().Format(datePartitionFormat)
		signalsByDate[date] = append(signalsByDate[date], row)
	}
	for _, date := range sortedKeys(signalsByDate) {
		key := r.cfg.DecodedPrefix + "signals/date=" + date + "/" + objectName
		body, err := writeSignalParquet(signalsByDate[date])
		if err != nil {
			return nil, fmt.Errorf("encoding signal parquet: %w", err)
		}
		if err := r.store.PutObject(ctx, key, body); err != nil {
			return nil, fmt.Errorf("writing %s: %w", key, err)
		}
		outputs = append(outputs, key)
	}

	eventsByDate := make(map[string][]EventRow)
	for _, row := range dec.events {
		date := row.Timestamp.UTC().Format(datePartitionFormat)
		eventsByDate[date] = append(eventsByDate[date], row)
	}
	for _, date := range sortedKeys(eventsByDate) {
		key := r.cfg.DecodedPrefix + "events/date=" + date + "/" + objectName
		body, err := writeEventParquet(eventsByDate[date])
		if err != nil {
			return nil, fmt.Errorf("encoding event parquet: %w", err)
		}
		if err := r.store.PutObject(ctx, key, body); err != nil {
			return nil, fmt.Errorf("writing %s: %w", key, err)
		}
		outputs = append(outputs, key)
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
	return r.cfg.DecodedPrefix + "_state/watermark.json"
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
