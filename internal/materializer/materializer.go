// Package materializer is the post-fact decode loop for the parse-on-read
// pipeline. It reads din's raw cloudevents from the shared DuckLake catalog
// (lake.raw_events), decodes payloads through model-garage, and writes the
// decoded lake.signals / lake.events tables (plus the signals_latest rollup)
// back into the same catalog. A single snapshot cursor in lake.ingest_progress,
// advanced in the same transaction as the inserts, is the commit protocol:
// exactly-once by construction and safe under concurrent writers (a same-range
// race conflicts at commit and the loser retries from the new snapshot).
//
// The decode + materialize mechanics live on DuckLakeMaterializer (ducklake.go);
// this file holds the poll loop (Runner) and the model-garage decode/convert
// stages shared by it.
package materializer

import (
	"context"
	"runtime"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// Config controls the materializer run loop.
type Config struct {
	// PollInterval is how often Run polls for new raw events.
	PollInterval time.Duration
	// ChainID and VehicleNFTAddress gate dimo.status signal decoding to
	// vehicle-NFT subjects, mirroring dis signalconvert and din's
	// decodestream. A zero VehicleNFTAddress disables the gate (tests).
	ChainID           uint64
	VehicleNFTAddress common.Address
	// Workers bounds the per-batch model-garage conversion fan-out. Defaults
	// to GOMAXPROCS.
	Workers int
	// DecodedRetention deletes decoded rows older than this from the DuckLake
	// tables (CHD-38). Zero (default) disables it — a row-level TTL on customer
	// history is a product decision.
	DecodedRetention time.Duration
}

const defaultPollInterval = 15 * time.Second

func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.Workers <= 0 {
		c.Workers = runtime.GOMAXPROCS(0)
	}
	return c
}

// Runner drives the decode loop against the shared DuckLake catalog (lake), set
// via WithDuckLake.
type Runner struct {
	cfg Config
	log zerolog.Logger
	// lake reads din's raw_events from the shared DuckLake catalog via snapshot
	// diffs and writes the decoded tables there. RunOnce delegates to it.
	lake *DuckLakeMaterializer
}

// New creates a Runner. Zero-valued config fields get defaults. Wire the
// DuckLake catalog with WithDuckLake before running.
func New(cfg Config, log zerolog.Logger) *Runner {
	return &Runner{
		cfg: cfg.withDefaults(),
		log: log.With().Str("component", "materializer").Logger(),
	}
}

// WithDuckLake returns r configured to materialize from din's shared DuckLake
// catalog (raw_events → signals/events) via m. The catalog transaction is the
// commit protocol.
func (r *Runner) WithDuckLake(m *DuckLakeMaterializer) *Runner {
	r.lake = m
	return r
}

// Run polls raw_events until ctx is canceled. As long as a poll processed
// events without error it polls again immediately to drain the backlog;
// otherwise it waits PollInterval.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	lastPrune := time.Now()
	for {
		processed, err := r.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			passErrorsTotal.Inc()
			r.log.Error().Err(err).Msg("materializer pass failed")
		} else if processed > 0 {
			continue // drain the backlog without waiting
		}
		// The decoded tables (lake.signals/events) are merged + expired by din's
		// catalog-wide maintenance (one maintenance process per catalog), so dq
		// runs none — it only enforces the optional decoded-row retention.
		r.maybePrune(ctx, &lastPrune)
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

// RunOnce performs a single decode pass over the raw_events snapshot delta. It
// returns the number of raw events processed.
func (r *Runner) RunOnce(ctx context.Context) (int, error) {
	return r.lake.RunOnce(ctx, r)
}

// decodeEvents converts a reconstructed slice of raw events (a DuckLake
// snapshot delta over lake.raw_events) into a decodedBatch. Each event routes
// by its own Type (status→signals, events→events), and the vehicle gate applies
// per status event. Dedup is on the header uniqueness key; conversion fans out
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

	r.convertJobs(ctx, dec, jobs)
	return dec
}

// convertJobs fans model-garage conversion across jobs — the CPU-heavy step,
// stateless per event — merging the per-job signal/event rows into dec in input
// order (writers re-sort anyway). Each job routes by its own Type. The convert
// funcs never return an error (decode failures are counted into errorCount), so
// the errgroup wait is informational.
func (r *Runner) convertJobs(ctx context.Context, dec *decodedBatch, jobs []*cloudevent.RawEvent) {
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
			// The materializer is the single writer: a panic in a vendor decode
			// module on one malformed payload would crash the process and then
			// crash-loop on the same row. Contain it — count a decode failure and
			// drop the row instead.
			defer func() {
				if p := recover(); p != nil {
					r.log.Error().Interface("panic", p).Str("source", raw.Source).Str("id", raw.ID).
						Msg("recovered panic during decode; skipping event")
					results[i] = convResult{failed: 1}
				}
			}()
			switch raw.Type {
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

// decodedBatch holds everything decoded out of one raw batch.
type decodedBatch struct {
	signals []SignalRow
	events  []EventRow

	signalCount int
	eventCount  int
	errorCount  int
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
