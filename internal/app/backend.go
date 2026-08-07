package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/config"
	"github.com/DIMO-Network/dq/internal/latestkv"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/dq/pkg/blobcrypt"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
)

// appLogger returns the process logger installed by main, or a no-op logger
// in tests.
func appLogger() zerolog.Logger {
	if zerolog.DefaultContextLogger != nil {
		return *zerolog.DefaultContextLogger
	}
	return zerolog.Nop()
}

// parseSlowReadThreshold reads DUCKDB_SLOW_READ_THRESHOLD. An unset or
// unparseable value yields 0, which duck.Service reads as "use the default" —
// a typo must not silence the slow-read log, and must not fail startup over a
// diagnostic knob.
func parseSlowReadThreshold(v string) time.Duration {
	if v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l := appLogger()
		l.Warn().Str("value", v).Msg("invalid DUCKDB_SLOW_READ_THRESHOLD; using the default")
		return 0
	}
	return d
}

// duckConfigFromSettings maps the app settings into the DuckDB query-engine
// config, reusing the shared S3/bucket settings.
//
// DUCKDB_PROFILE_READS is deliberately NOT read here. This is the base config for
// the materializer's decode instance as well as the query backend, and profiling
// only ever pays for itself on reads that route through Service.queryLake — the
// decode loop issues none, so for it the flag is pure cost (a pinned connection
// checkout and a scratch file per read) for a tree nothing reads back. The flag
// is applied in queryDuckConfig instead, so it fails safe: a future duck.Service
// that forgets to opt in loses a diagnostic rather than silently paying for one.
// SlowReadThreshold stays here because it sets no PRAGMA and is inert for a
// service that issues no queryLake reads.
func duckConfigFromSettings(settings *config.Settings) duck.Config {
	bucket := settings.BlobBucket
	return duck.Config{
		DuckDBMemoryLimit:    settings.DuckDBMemoryLimit,
		DuckDBThreads:        settings.DuckDBThreads,
		DuckDBExtensionDir:   settings.DuckDBExtensionDir,
		TempDirectory:        settings.DuckDBTempDirectory,
		MaxConns:             settings.DuckDBMaxConns,
		SlowReadThreshold:    parseSlowReadThreshold(settings.DuckDBSlowReadThreshold),
		S3Enabled:            !isLocalBucket(bucket),
		S3AWSRegion:          settings.S3AWSRegion,
		S3AWSAccessKeyID:     settings.S3AWSAccessKeyID,
		S3AWSSecretAccessKey: settings.S3AWSSecretAccessKey,
		S3Endpoint:           settings.LakeS3Endpoint(),
		// DuckLake is the only backend — always attach the catalog.
		DuckLakeEnabled: true,
		CatalogDSN:      settings.DuckLakeCatalogDSN,
		CatalogReadDSN:  settings.DuckLakeCatalogReadDSN,
		DataPath:        settings.DuckLakeDataPath,
		// Only the single-writer materializer writes the lake. The read/query
		// fleet attaches read-only when asked, which also lets it read from a
		// Postgres read replica. Force read-only off for the materializer so the
		// writer can never come up read-only.
		ReadOnly: settings.DuckLakeReadOnly && !settings.MaterializerEnabled,
		// Attach ENCRYPTED to match din. attachDuckLakeSQL only emits it on the
		// writable (materializer) attach, so read-only query pods are unaffected.
		Encrypted: settings.LakeEncryptionEnabled,
		// Load spatial for the ST_* geofence filters. Default on for the query path;
		// the materializer overrides it off (its delta read crashes under spatial's
		// RTree optimizer) — see startDuckLakeMaterializer.
		LoadSpatial: true,
		// Recover from ducklake session/instance poison after a din maintenance
		// collision (#21). Default on for the query path; the materializer
		// overrides it off — its decode loop carries its own calibrated
		// two-strike recovery (1736873) and must not stack a second one.
		PoisonRecovery: true,
	}
}

// queryDuckConfig is the DuckDB config for the QUERY backend. It is
// duckConfigFromSettings with one materializer-pod adjustment: on a materializer
// release (MATERIALIZER_ENABLED) the query backend is always built but serves no
// traffic (no ingress; the query fleet is a separate release), yet still honors
// DUCKDB_MEMORY_LIMIT — so two same-limit DuckDB instances can co-reside on one pod
// and OOM (finding #7). When DUCKDB_QUERY_MEMORY_LIMIT is set on such a pod, cap this
// idle query instance with it so it plus the decode instance sum under the pod limit;
// the decode instance (startDuckLakeMaterializer) keeps the full DUCKDB_MEMORY_LIMIT.
// On a query pod the override is inert, so the query fleet is unchanged.
func queryDuckConfig(settings *config.Settings) duck.Config {
	cfg := duckConfigFromSettings(settings)
	if settings.MaterializerEnabled && settings.DuckDBQueryMemoryLimit != "" {
		cfg.DuckDBMemoryLimit = settings.DuckDBQueryMemoryLimit
	}
	// Read-profiling is a query-path diagnostic: only Service.queryLake reads the
	// tree back, and only the query backend calls it. Enabling it on the base
	// config would charge the materializer's decode loop for profiling it never
	// consumes — see duckConfigFromSettings.
	cfg.ProfileReads = settings.DuckDBProfileReads
	return cfg
}

// isLocalBucket reports whether the parquet bucket points at the local
// filesystem (file:// URL or absolute path) instead of S3, mirroring how
// duck.Service interprets its Bucket setting.
func isLocalBucket(bucket string) bool {
	return strings.HasPrefix(bucket, "file://") || strings.HasPrefix(bucket, "/")
}

// newQueryBackend builds the DuckLake-backed Repository backend (the only
// backend). It returns the backend, the DuckDB service, and a cleanup function
// (always non-nil). The returned duckSvc is owned by the cleanup — callers must
// not close it themselves.
func newQueryBackend(settings *config.Settings, logger zerolog.Logger) (repositories.QueryService, *duck.Service, func(), error) {
	// DuckLake is the only backend, so the catalog DSN is mandatory. Fail fast and
	// clear at boot rather than attaching an empty catalog and failing opaquely on
	// the first query (the materializer path guards the same setting).
	if settings.DuckLakeCatalogDSN == "" {
		return nil, nil, nil, fmt.Errorf("DUCKLAKE_CATALOG_DSN is empty: the DuckLake catalog is required for the query backend")
	}
	duckSvc, err := duck.NewService(queryDuckConfig(settings))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("couldn't create DuckDB service: %w", err)
	}
	queries := duck.NewLakeQueries(duckSvc)
	cleanup := closeDuck(duckSvc, logger)

	// signals-latest cache read path (phase 2, kv_latest.go): shadow compares
	// against the rollup on real traffic; serve answers from the cache with
	// rollup fallback. Fail loudly on a bad mode or a missing NATS_URL — a
	// half-wired cache read silently degrades to 100% fallback and looks fine.
	mode, valid := duck.ParseKVReadMode(settings.LatestKVReadMode)
	if !valid {
		_ = duckSvc.Close()
		return nil, nil, nil, fmt.Errorf("invalid LATEST_KV_READ_MODE %q (want off, shadow, or serve)", settings.LatestKVReadMode)
	}
	if extRaw := settings.LatestKVReadModeExtended; mode == duck.KVReadOff && extRaw != "" && extRaw != string(duck.KVReadOff) {
		// The extended paths ride the base mode's store; without it the setting
		// silently does nothing — refuse it like the negative-mode combination.
		_ = duckSvc.Close()
		return nil, nil, nil, fmt.Errorf("LATEST_KV_READ_MODE_EXTENDED=%s requires LATEST_KV_READ_MODE to not be off", extRaw)
	}
	// Daily-serving rollup (dq#55 step 4): summaries switch to the exact
	// (rollup ∪ tail) union. Independent of the KV wiring.
	queries = queries.WithDailyServingRollup(settings.LakeRollupDailyServing)
	if mode != duck.KVReadOff {
		if settings.NATSURL == "" {
			_ = duckSvc.Close()
			return nil, nil, nil, fmt.Errorf("LATEST_KV_READ_MODE=%s but NATS_URL is empty", mode)
		}
		bucket := settings.LatestKVBucket
		if bucket == "" {
			bucket = latestkv.DefaultBucket
		}
		store, err := latestkv.Open(context.Background(), settings.NATSURL, bucket, logger)
		if err != nil {
			_ = duckSvc.Close()
			return nil, nil, nil, fmt.Errorf("opening signals-latest KV for reads: %w", err)
		}
		queries = queries.WithLatestKV(store, mode, logger)
		inner := cleanup
		cleanup = func() {
			store.Close()
			inner()
		}

		// Extended KV serving: allLatest + availableSignals (dq#55 step 3),
		// dark-launched on its own ladder. Inside the mode != off block because
		// it rides the same store; a bad value fails loudly like the base mode.
		extMode, valid := duck.ParseKVReadMode(settings.LatestKVReadModeExtended)
		if !valid {
			cleanup()
			return nil, nil, nil, fmt.Errorf("invalid LATEST_KV_READ_MODE_EXTENDED %q (want off, shadow, or serve)", settings.LatestKVReadModeExtended)
		}
		queries = queries.WithLatestKVExtended(extMode)

		// Authoritative-miss serving (dq#42). The watcher runs for the process
		// lifetime, so it takes a cancellable context of its own rather than a
		// request context.
		negMode, valid := duck.ParseKVNegativeMode(settings.LatestKVNegative)
		if !valid {
			cleanup()
			return nil, nil, nil, fmt.Errorf("invalid LATEST_KV_NEGATIVE %q (want off, shadow, or serve)", settings.LatestKVNegative)
		}
		if negMode != duck.KVNegativeOff {
			if mode != duck.KVReadServe {
				// Under read mode shadow/off every query reads the rollup anyway, so
				// a negative could never save the read it exists to save. Refuse the
				// combination rather than accept a setting that silently does nothing.
				cleanup()
				return nil, nil, nil, fmt.Errorf("LATEST_KV_NEGATIVE=%s requires LATEST_KV_READ_MODE=serve (got %q)", negMode, mode)
			}
			watchCtx, cancelWatch := context.WithCancel(context.Background())
			watcher, err := store.WatchCoverage(watchCtx)
			if err != nil {
				cancelWatch()
				cleanup()
				return nil, nil, nil, fmt.Errorf("watching signals-latest coverage: %w", err)
			}
			queries = queries.WithLatestKVNegative(watcher, negMode)
			inner := cleanup
			cleanup = func() {
				cancelWatch()
				inner()
			}
			logger.Info().Str("mode", string(negMode)).Msg("signals-latest authoritative-miss serving enabled")
		}
	}

	// Reads and segment detection both come from the DuckLake catalog. Return
	// duckSvc so the caller can share it with newEventService.
	return repositories.ComposeBackend(queries, duck.NewLakeSegments(duckSvc)), duckSvc, cleanup, nil
}

// newEventService builds the DuckLake cloudevent fetch backend (lake.raw_events).
// duckSvc is the same catalog-attached service returned by newQueryBackend;
// s3Client must be non-nil.
func newEventService(settings *config.Settings, duckSvc *duck.Service, s3Client *s3.Client, _ zerolog.Logger) (eventrepo.EventService, error) {
	presigner := s3.NewPresignClient(s3Client)
	cipher, err := blobcrypt.NewCipher(settings.BlobEncryptionKey)
	if err != nil {
		return nil, err
	}
	return duck.NewLakeEventService(duckSvc, s3Client, presigner, settings.BlobBucket).WithBlobCipher(cipher), nil
}

func closeDuck(duckSvc *duck.Service, logger zerolog.Logger) func() {
	return func() {
		if err := duckSvc.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close DuckDB service")
		}
	}
}

// startMaterializer registers the vendor decode modules and starts the
// materializer poll loop in a goroutine. The returned stop function cancels
// the loop and waits for it to exit.
func startMaterializer(settings *config.Settings, logger zerolog.Logger) (func(), error) {
	var pollInterval time.Duration
	if settings.MaterializerPollInterval != "" {
		var err error
		pollInterval, err = time.ParseDuration(settings.MaterializerPollInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid MATERIALIZER_POLL_INTERVAL %q: %w", settings.MaterializerPollInterval, err)
		}
	}

	materializer.RegisterVendorModules(materializer.VendorConfig{
		ChainID:               settings.DIMORegistryChainID,
		VehicleNFTAddress:     common.HexToAddress(settings.VehicleNFTAddress),
		AftermarketNFTAddress: common.HexToAddress(settings.AftermarketNFTAddress),
		SyntheticNFTAddress:   common.HexToAddress(settings.SyntheticNFTAddress),
	})

	// DuckLake is the only backend: the materializer decodes din's raw_events
	// through the shared catalog (no S3 store, no bucket layout). The catalog DSN
	// is the same one the query backend requires, so it is always present in
	// practice; guard it so an enabled materializer fails loudly rather than
	// coming up wired to nothing.
	if settings.DuckLakeCatalogDSN == "" {
		return nil, fmt.Errorf("materializer enabled but DUCKLAKE_CATALOG_DSN is empty: the DuckLake catalog is required")
	}
	return startDuckLakeMaterializer(settings, pollInterval, logger)
}

// startDuckLakeMaterializer wires the materializer to read din's raw_events
// from the shared DuckLake catalog and write the decoded tables there. It
// owns its own DuckDB service (catalog attached) for the lifetime of the
// loop; the query backend opens a separate one.
func startDuckLakeMaterializer(settings *config.Settings, pollInterval time.Duration, logger zerolog.Logger) (func(), error) {
	duckSvc, mat, runner, err := buildDuckLakeMaterializer(settings, pollInterval, logger)
	if err != nil {
		return nil, err
	}

	// The signals-latest KV cache (phase-1 write side): fold every decoded
	// batch into the bucket, and bootstrap it from lake.signals_latest on the
	// loop goroutine (first enable / LATEST_KV_FORCE_BOOTSTRAP repair).
	kvStore, err := openLatestKV(settings, logger)
	if err != nil {
		_ = duckSvc.Close()
		return nil, err
	}
	var bootstrapKV func(context.Context)
	var coverage *latestkv.CoverageReporter
	if kvStore != nil {
		// The coverage reporter (dq#42) is the writer half of authoritative-miss
		// serving: it re-proves and re-asserts that the bucket mirrors
		// lake.signals_latest, and query pods answer a miss as "no data" only
		// while that assertion is live. The publisher feeds it every batch
		// outcome, so a publish failure drops the assertion at once.
		if settings.LatestKVCoverage {
			coverage = latestkv.NewCoverageReporter(kvStore, func(ctx context.Context) error {
				return kvStore.ReconcileFromRollup(ctx, duckSvc.DB())
			}, logger)
		}
		mat.WithLatestPublisher(NewLatestKVPublisher(kvStore, coverage, logger))
		bootstrapKV = func(ctx context.Context) {
			if err := kvStore.BootstrapFromRollup(ctx, duckSvc.DB(), settings.LatestKVForceBootstrap); err != nil {
				// Non-fatal: live publishes heal active subjects; rerun with
				// LATEST_KV_FORCE_BOOTSTRAP to repair dormant ones.
				logger.Error().Err(err).Msg("signals-latest KV bootstrap failed; continuing (rerun with LATEST_KV_FORCE_BOOTSTRAP to repair)")
			}
		}
	}

	// rebuildRollup is the opt-in disaster-recovery rebuild (LAKE_REBUILD_ROLLUP_ON_BOOT):
	// the per-batch recompute only touches a batch's subjects, so a dropped/truncated
	// rollup leaves dormant vehicles missing until rebuilt from the full base. It runs
	// in the loop goroutine BEFORE processing (never concurrently with it) — not
	// synchronously at boot — so this O(history) scan can't outlast the liveness probe
	// and CrashLoop the pod; a failure is logged and the loop proceeds (the per-batch
	// recompute still heals active vehicles).
	stop := runMaterializerLoop(runner, mat, settings.LakeRebuildRollupOnBoot, bootstrapKV, logger)

	// The reporter runs on its own goroutine, NOT the decode loop: proving
	// coverage at boot is a full scan of lake.signals_latest, and the decode loop
	// is the one place that must not wait on it. Stopped before the KV connection
	// closes so the clean-shutdown mark still has a connection to go out on —
	// that mark is what lets the next boot skip the reconcile.
	stopCoverage := func() {}
	if coverage != nil {
		covCtx, cancelCov := context.WithCancel(context.Background())
		covDone := make(chan struct{})
		go func() {
			defer close(covDone)
			coverage.Run(covCtx)
		}()
		stopCoverage = func() {
			cancelCov()
			<-covDone
		}
	}
	return func() {
		stop()
		stopCoverage()
		if kvStore != nil {
			kvStore.Close()
		}
		_ = duckSvc.Close()
	}, nil
}

// openLatestKV connects the signals-latest KV store when LATEST_KV_WRITE_ENABLED,
// returning (nil, nil) when disabled. Fails loudly on a missing NATS_URL — an
// enabled cache silently wired to nothing would look healthy while going stale.
func openLatestKV(settings *config.Settings, logger zerolog.Logger) (*latestkv.Store, error) {
	if !settings.LatestKVWriteEnabled {
		return nil, nil
	}
	if settings.NATSURL == "" {
		return nil, fmt.Errorf("LATEST_KV_WRITE_ENABLED is set but NATS_URL is empty")
	}
	bucket := settings.LatestKVBucket
	if bucket == "" {
		bucket = latestkv.DefaultBucket
	}
	store, err := latestkv.Open(context.Background(), settings.NATSURL, bucket, logger)
	if err != nil {
		return nil, fmt.Errorf("opening signals-latest KV: %w", err)
	}
	return store, nil
}

// latestKVPublisher adapts latestkv.Store to materializer.LatestPublisher,
// mapping SignalRow → latestkv.Row and downgrading publish errors to
// log+metric per the LatestPublisher contract (best-effort, never block decode).
type latestKVPublisher struct {
	store *latestkv.Store
	// coverage, when set, learns every publish outcome. A failure means a
	// subject may have reached the lake without reaching the bucket, which is
	// precisely the case that must revoke authoritative-miss serving (dq#42).
	// Nil disables the contract and leaves publishing exactly as it was.
	coverage *latestkv.CoverageReporter
	log      zerolog.Logger
}

// NewLatestKVPublisher wraps store as the materializer's LatestPublisher.
// coverage may be nil (LATEST_KV_COVERAGE unset, or a publish-only caller).
func NewLatestKVPublisher(store *latestkv.Store, coverage *latestkv.CoverageReporter, log zerolog.Logger) materializer.LatestPublisher {
	return &latestKVPublisher{store: store, coverage: coverage, log: log}
}

// latestKVPublishTimeout caps one batch's KV publish. The publish runs on the
// decode goroutine (before the catalog txn), and during a NATS outage every
// per-subject round trip waits out the JetStream request timeout — uncapped, a
// fleet-wide batch would stall decode for minutes. Missed subjects heal on
// their next reading (or a forced bootstrap), which is the documented
// degradation mode; stalling decode is not.
const latestKVPublishTimeout = 30 * time.Second

func (p *latestKVPublisher) PublishLatest(parent context.Context, rows []materializer.SignalRow) {
	ctx, cancel := context.WithTimeout(parent, latestKVPublishTimeout)
	defer cancel()
	kvRows := make([]latestkv.Row, len(rows))
	for i, r := range rows {
		kvRows[i] = latestkv.Row{
			Subject:      r.Subject,
			Name:         r.Name,
			Timestamp:    r.Timestamp,
			CloudEventID: r.CloudEventID,
			ValueNumber:  r.ValueNumber,
			ValueString:  r.ValueString,
			LocLat:       r.LocLat,
			LocLon:       r.LocLon,
			LocHDOP:      r.LocHDOP,
			LocHeading:   r.LocHeading,
		}
	}
	// Log unless the PARENT is done (shutdown): a publish-timeout expiry is
	// exactly the outage this error line exists to surface.
	err := p.store.PublishSignals(ctx, kvRows)
	if err != nil && parent.Err() == nil {
		p.log.Error().Err(err).Msg("signals-latest KV publish failed; cache staler until those subjects' next readings (or a forced bootstrap)")
	}
	if p.coverage != nil {
		// Report even during shutdown: a batch that failed on the way out still
		// leaves the bucket possibly incomplete, and suppressing it here would let
		// the clean-shutdown mark claim otherwise.
		p.coverage.ObservePublish(err)
	}
}

// BatchSettled forwards the end of the batch's catalog transaction to the
// coverage reporter, which cannot take a valid proof from the lake while a
// batch whose publish failed is still in flight (latestkv.settleBarrierMet).
func (p *latestKVPublisher) BatchSettled() {
	if p.coverage != nil {
		p.coverage.ObserveBatchSettled()
	}
}

// buildDuckLakeMaterializer constructs the materializer's DuckDB service, the
// DuckLakeMaterializer (blob store + cipher + temp dir), and the Runner (the decode
// surface) — the shared setup behind both the live decode loop (startDuckLakeMaterializer)
// and the one-shot re-decode tool (RunBackfill, finding #1a). On error it closes the
// service so the caller has nothing to clean up; on success the caller owns duckSvc.
func buildDuckLakeMaterializer(settings *config.Settings, pollInterval time.Duration, logger zerolog.Logger) (*duck.Service, *materializer.DuckLakeMaterializer, *materializer.Runner, error) {
	// Sharding is not honored on the DuckLake path — readDelta reads the whole
	// raw_events delta and the single global ingest_progress cursor makes exactly
	// one logical processor (extra replicas just lose the cursor CAS and roll
	// back). Refuse the config rather than silently ignore it: run the
	// materializer as a single replicaCount=1 release (SR review #8).
	if settings.MaterializerShardCount > 1 {
		return nil, nil, nil, fmt.Errorf(
			"MATERIALIZER_SHARD_COUNT=%d is not supported on the DuckLake path: the global ingest_progress cursor allows only one materializer; run a single replicaCount=1 release",
			settings.MaterializerShardCount)
	}
	cfg := duckConfigFromSettings(settings)
	// duckConfigFromSettings already sets DuckLakeEnabled. The materializer reads the
	// DuckLake delta (ducklake_table_changes); spatial's RTreeIndexScanOptimizer
	// crashes the planner on that read, so never load it here.
	cfg.LoadSpatial = false
	// The decode loop's Runner already carries the calibrated two-strike poison
	// recovery (1736873); the driver-level port (#21) would race it — recycling
	// under the loop's feet and exiting before the loop's own escalation.
	cfg.PoisonRecovery = false
	cfg.MetricsPoolLabel = "materializer" // separate dq_db_pool_* series from the query pool (H4)
	duckSvc, err := duck.NewService(cfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating DuckLake service: %w", err)
	}
	mat, err := materializer.NewDuckLakeMaterializer(context.Background(), duckSvc.DB(), logger)
	if err != nil {
		_ = duckSvc.Close()
		return nil, nil, nil, fmt.Errorf("creating DuckLake materializer: %w", err)
	}
	// Resolve externalized blob payloads from the same bucket the fetch path
	// presigns/downloads (settings.BlobBucket): din writes payloads larger
	// than the inline threshold to a blob and leaves only the key on the row.
	blobCipher, err := blobcrypt.NewCipher(settings.BlobEncryptionKey)
	if err != nil {
		_ = duckSvc.Close()
		return nil, nil, nil, fmt.Errorf("blob cipher: %w", err)
	}
	mat = mat.WithBlobStore(s3ClientFromSettings(settings), settings.BlobBucket).
		WithBlobCipher(blobCipher).
		WithTempDir(settings.DuckDBTempDirectory) // stage batch parquet on the sized spill volume, not the root fs
	// Only positive overrides: WithMaxSnapshotSpan treats non-positive as
	// UNBOUNDED, which must not be reachable from config (memory bound — see
	// the settings field doc).
	if settings.MaterializerMaxSnapshotSpan > 0 {
		mat = mat.WithMaxSnapshotSpan(int64(settings.MaterializerMaxSnapshotSpan))
	}
	dailyMode, ok := materializer.ParseDailyRollupMode(settings.MaterializerDailyRollupMode)
	if !ok {
		_ = duckSvc.Close()
		return nil, nil, nil, fmt.Errorf("invalid MATERIALIZER_DAILY_ROLLUP_MODE %q (off|shadow)", settings.MaterializerDailyRollupMode)
	}
	var dailyDelay time.Duration
	if settings.MaterializerDailyRollupDelay != "" {
		dailyDelay, err = time.ParseDuration(settings.MaterializerDailyRollupDelay)
		if err != nil {
			_ = duckSvc.Close()
			return nil, nil, nil, fmt.Errorf("invalid MATERIALIZER_DAILY_ROLLUP_DELAY %q: %w", settings.MaterializerDailyRollupDelay, err)
		}
	}
	mat = mat.WithDailyRollup(dailyMode, dailyDelay)

	var decodedRetention time.Duration
	if settings.LakeDecodedRetention != "" {
		decodedRetention, err = time.ParseDuration(settings.LakeDecodedRetention)
		if err != nil {
			_ = duckSvc.Close()
			return nil, nil, nil, fmt.Errorf("invalid LAKE_DECODED_RETENTION %q: %w", settings.LakeDecodedRetention, err)
		}
	}
	var rollupInterval time.Duration
	if settings.MaterializerRollupInterval != "" {
		rollupInterval, err = time.ParseDuration(settings.MaterializerRollupInterval)
		if err != nil {
			_ = duckSvc.Close()
			return nil, nil, nil, fmt.Errorf("invalid MATERIALIZER_ROLLUP_INTERVAL %q: %w", settings.MaterializerRollupInterval, err)
		}
	}
	runner := materializer.New(materializer.Config{
		PollInterval:      pollInterval,
		RollupInterval:    rollupInterval,
		ChainID:           settings.DIMORegistryChainID,
		VehicleNFTAddress: common.HexToAddress(settings.VehicleNFTAddress),
		Workers:           settings.MaterializerWorkers,
		DecodedRetention:  decodedRetention,
		BackfillMode:      settings.MaterializerBackfillMode,
	}, logger).WithDuckLake(mat)
	return duckSvc, mat, runner, nil
}

// RunBackfill re-decodes raw_events in [from, to) into the decoded lake tables and
// exits — the operator-run repair for a range the decode loop permanently skipped on
// cursor expiry (finding #1a; alerted by DQMaterializerCursorReset). It is idempotent
// (the same cloud_event_id anti-join), so it is safe to re-run and safe to run while
// the live materializer is up. It registers the vendor decode modules, decodes the
// range, then flushes both rollups so latest/summary reflect the backfilled rows.
func RunBackfill(settings config.Settings, from, to time.Time, logger zerolog.Logger) error {
	if settings.DuckLakeCatalogDSN == "" {
		return fmt.Errorf("DUCKLAKE_CATALOG_DSN is empty: the DuckLake catalog is required for backfill")
	}
	materializer.RegisterVendorModules(materializer.VendorConfig{
		ChainID:               settings.DIMORegistryChainID,
		VehicleNFTAddress:     common.HexToAddress(settings.VehicleNFTAddress),
		AftermarketNFTAddress: common.HexToAddress(settings.AftermarketNFTAddress),
		SyntheticNFTAddress:   common.HexToAddress(settings.SyntheticNFTAddress),
	})
	duckSvc, mat, runner, err := buildDuckLakeMaterializer(&settings, 0, logger)
	if err != nil {
		return err
	}
	defer func() { _ = duckSvc.Close() }()

	// Publish backfilled batches to the signals-latest KV too (when enabled):
	// the fold discards anything older than the cache, and a backfilled range
	// may carry a dormant subject's genuinely-latest reading.
	kvStore, err := openLatestKV(&settings, logger)
	if err != nil {
		return err
	}
	if kvStore != nil {
		defer kvStore.Close()
		// Publish-only coverage participant (nil reconcile, no heartbeat): the
		// live materializer owns the assertion, but a backfill publish failure can
		// leave a subject in the lake and not in the bucket just the same, so it
		// must be able to revoke it. Flushed after the run, when NATS has had a
		// chance to come back.
		coverage := latestkv.NewCoverageReporter(kvStore, nil, logger)
		mat.WithLatestPublisher(NewLatestKVPublisher(kvStore, coverage, logger))
		defer func() {
			if coverage.FlushDegraded(context.Background()) {
				logger.Warn().Msg("backfill had signals-latest publish failures: coverage marked degraded, the materializer will re-prove it by reconcile")
			}
		}()
	}

	ctx := context.Background()
	logger.Info().Time("from", from).Time("to", to).Msg("backfill: re-decoding raw_events range")
	n, err := mat.BackfillTimeRange(ctx, runner, from, to)
	if err != nil {
		return fmt.Errorf("backfill decode: %w", err)
	}
	// Backfilled rows rewrite history behind the daily rollup watermark (dq#55),
	// so hand the touched subjects to the daily refresh's late set BEFORE the
	// flush drains the dirty set. No-op unless MATERIALIZER_DAILY_ROLLUP_MODE
	// is configured for this process.
	if ferr := mat.PersistDailyLateSubjects(ctx); ferr != nil {
		logger.Error().Err(ferr).Msg("backfill: recording daily-rollup late subjects failed; drop the daily watermark row to force a reseed")
	}
	// Refresh the rollups for the subjects the backfill touched (dirtied above).
	if ferr := runner.FlushRollup(ctx); ferr != nil {
		logger.Error().Err(ferr).Msg("backfill: signals_latest flush failed; rerun with LAKE_REBUILD_ROLLUP_ON_BOOT if latest/summary looks stale")
	}
	if ferr := runner.FlushEventRollup(ctx); ferr != nil {
		logger.Error().Err(ferr).Msg("backfill: events_latest flush failed; rerun with LAKE_REBUILD_ROLLUP_ON_BOOT if event summaries look stale")
	}
	logger.Info().Int("raw_events", n).Msg("backfill complete")
	return nil
}

// runMaterializerLoop runs runner.Run in a goroutine and returns a stop
// function that cancels it and waits for exit. bootstrapKV (nil = disabled)
// runs on the loop goroutine before processing, like the rollup rebuild, so
// it never races the live publisher and can't outlast the liveness probe.
func runMaterializerLoop(runner *materializer.Runner, mat *materializer.DuckLakeMaterializer, rebuildRollup bool, bootstrapKV func(context.Context), logger zerolog.Logger) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if rebuildRollup {
			logger.Info().Msg("LAKE_REBUILD_ROLLUP_ON_BOOT set: rebuilding signals_latest + events_latest from full base (may take a while on deep history)")
			if err := mat.RecomputeRollup(ctx); err != nil {
				// Non-fatal: log and proceed — crashing here would CrashLoop the
				// pod; the per-batch recompute still heals active vehicles.
				logger.Error().Err(err).Msg("signals_latest rebuild failed; continuing with the normal loop")
			} else {
				logger.Info().Msg("signals_latest rebuild complete")
			}
			// Rebuild the events rollup too (finding #5a); independent of the signals
			// rebuild so one failing doesn't skip the other.
			if err := mat.RecomputeEventRollup(ctx); err != nil {
				logger.Error().Err(err).Msg("events_latest rebuild failed; continuing with the normal loop")
			} else {
				logger.Info().Msg("events_latest rebuild complete")
			}
		}
		if bootstrapKV != nil {
			bootstrapKV(ctx)
		}
		if err := runner.Run(ctx); err != nil {
			// Run returns an error only when the decode loop is durably broken (the
			// consecutive-failure backstop tripped) — never on graceful shutdown, which
			// returns nil. Exit so K8s restarts this dedicated materializer pod and
			// re-attaches the catalog, rather than leaving it Ready-but-not-decoding.
			logger.Fatal().Err(err).Msg("materializer durably failed; exiting to restart the pod")
		}
	}()
	logger.Info().Msg("materializer started")
	return func() {
		cancel()
		<-done
	}
}
