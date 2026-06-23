package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/config"
	"github.com/DIMO-Network/dq/internal/fsstore"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/ch"
	"github.com/DIMO-Network/dq/internal/service/duck"
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

// duckConfigFromSettings maps the app settings into the DuckDB query-engine
// config, reusing the shared S3/bucket settings.
func duckConfigFromSettings(settings *config.Settings) duck.Config {
	bucket := settings.ParquetBucket
	return duck.Config{
		DuckDBMemoryLimit:    settings.DuckDBMemoryLimit,
		DuckDBThreads:        settings.DuckDBThreads,
		DuckDBExtensionDir:   settings.DuckDBExtensionDir,
		TempDirectory:        settings.DuckDBTempDirectory,
		MaxConns:             settings.DuckDBMaxConns,
		S3Enabled:            !isLocalBucket(bucket),
		S3AWSRegion:          settings.S3AWSRegion,
		S3AWSAccessKeyID:     settings.S3AWSAccessKeyID,
		S3AWSSecretAccessKey: settings.S3AWSSecretAccessKey,
		S3Endpoint:           settings.DuckDBS3Endpoint,
		Bucket:               bucket,
		RawPrefix:            settings.RawPrefix,
		DecodedPrefix:        settings.DecodedPrefix,
		// Enable the DuckLake catalog when running ducklake mode or shadow mode
		// with a catalog DSN configured — shadow needs it to compare segment
		// detection results from the lake against the primary ClickHouse backend.
		DuckLakeEnabled: settings.QueryBackend == config.QueryBackendDuckLake ||
			(settings.QueryBackend == config.QueryBackendShadow && settings.DuckLakeCatalogDSN != ""),
		CatalogDSN:     settings.DuckLakeCatalogDSN,
		CatalogReadDSN: settings.DuckLakeCatalogReadDSN,
		DataPath:       settings.DuckLakeDataPath,
		// Only the single-writer materializer writes the lake. Any other role
		// (query fleet, shadow comparison) attaches read-only when asked, which
		// also lets it read from a Postgres read replica. Force read-only off
		// for the materializer so the writer can never come up read-only.
		ReadOnly: settings.DuckLakeReadOnly && !settings.MaterializerEnabled,
	}
}

// isLocalBucket reports whether the parquet bucket points at the local
// filesystem (file:// URL or absolute path) instead of S3, mirroring how
// duck.Service interprets its Bucket setting.
func isLocalBucket(bucket string) bool {
	return strings.HasPrefix(bucket, "file://") || strings.HasPrefix(bucket, "/")
}

// newQueryBackend selects the Repository backend per QUERY_BACKEND. It
// returns the backend, the DuckDB service (nil for clickhouse mode), a cleanup
// function (always non-nil), and an error for unknown backend values. The
// returned duckSvc (when non-nil) is owned by the cleanup — callers must not
// close it themselves.
func newQueryBackend(settings *config.Settings, chService *ch.Service, logger zerolog.Logger) (repositories.CHService, *duck.Service, func(), error) {
	backend := settings.QueryBackend
	if backend == "" {
		backend = config.QueryBackendClickHouse
	}
	if backend == config.QueryBackendClickHouse {
		return chService, nil, func() {}, nil
	}

	duckSvc, err := duck.NewService(duckConfigFromSettings(settings))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("couldn't create DuckDB service: %w", err)
	}
	switch backend {
	case config.QueryBackendDuckLake:
		// Reads and segment detection both come from the DuckLake catalog.
		// Return duckSvc so the caller can share it with newEventService.
		return repositories.ComposeBackend(duck.NewLakeQueries(duckSvc), duck.NewLakeSegments(duckSvc)), duckSvc, closeDuck(duckSvc, logger), nil
	}

	queries := duck.NewQueries(duckSvc, "")
	switch backend {
	case config.QueryBackendDuckDB:
		// Segment detection is not implemented on DuckDB; it stays on ClickHouse.
		return repositories.ComposeBackend(queries, chService), duckSvc, closeDuck(duckSvc, logger), nil
	case config.QueryBackendShadow:
		// Shadow must validate the SAME backend that goes live under
		// QUERY_BACKEND=ducklake — lake mode (NewLakeQueries), not bucket mode.
		// With a catalog DSN configured (the cutover scenario; DuckLakeEnabled is
		// set above for this case) the signal/latest/summary secondary reads
		// lake.signals exactly like the live ducklake path, and segments shadow
		// against the lake too. Bucket mode here would read decoded/v1/*.parquet
		// globs the ducklake materializer never writes — comparing ClickHouse
		// against empty results, so a green shadow would be false confidence on
		// the highest-traffic surface. With no catalog DSN there is no lake to
		// compare, so fall back to the legacy bucket secondary unchanged.
		secondary := repositories.Backend(queries)
		var secondarySegment repositories.SegmentsBackend
		if settings.DuckLakeCatalogDSN != "" {
			secondary = duck.NewLakeQueries(duckSvc)
			secondarySegment = duck.NewLakeSegments(duckSvc)
		}
		shadow := repositories.NewShadowBackend(chService, secondary, secondarySegment, logger)
		cleanup := func() {
			shadow.Wait()
			closeDuck(duckSvc, logger)()
		}
		return shadow, duckSvc, cleanup, nil
	default:
		_ = duckSvc.Close()
		return nil, nil, nil, fmt.Errorf("unknown QUERY_BACKEND %q (expected %s, %s, %s, or %s)",
			settings.QueryBackend, config.QueryBackendClickHouse, config.QueryBackendDuckDB, config.QueryBackendShadow, config.QueryBackendDuckLake)
	}
}

// newEventService selects the cloudevent fetch backend per QUERY_BACKEND.
// ducklake → lake.raw_events (no ClickHouse client constructed).
// shadow → ShadowEventService (CH primary + lake secondary).
// everything else → ClickHouse cloud_event index.
//
// duckSvc must be non-nil when the backend is ducklake or shadow (the same
// catalog-attached service returned by newQueryBackend). s3Client must be
// non-nil for all backends.
func newEventService(settings *config.Settings, duckSvc *duck.Service, s3Client *s3.Client, log zerolog.Logger) (eventrepo.EventService, error) {
	presigner := s3.NewPresignClient(s3Client)
	bucket := settings.ParquetBucket

	switch settings.QueryBackend {
	case config.QueryBackendDuckLake:
		// No ClickHouse client is created in this branch.
		return duck.NewLakeEventService(duckSvc, s3Client, presigner, bucket), nil

	case config.QueryBackendShadow:
		// Build a CH event service as primary.
		chConn, err := chClientFromSettings(&settings.ClickhouseFileCatalogue)
		if err != nil {
			return nil, fmt.Errorf("ClickHouse connection for shadow event repo: %w", err)
		}
		chEvtSvc := eventrepo.New(chConn, s3Client, presigner, bucket)
		if settings.DuckLakeCatalogDSN == "" {
			// No catalog DSN configured — shadow fetch not possible; serve from CH.
			return chEvtSvc, nil
		}
		lakeSvc := duck.NewLakeEventService(duckSvc, s3Client, presigner, bucket)
		return eventrepo.NewShadowEventService(chEvtSvc, lakeSvc, log), nil

	default:
		// clickhouse, duckdb, or unset: build a CH event service.
		chConn, err := chClientFromSettings(&settings.ClickhouseFileCatalogue)
		if err != nil {
			return nil, fmt.Errorf("ClickHouse connection for event repo: %w", err)
		}
		return eventrepo.New(chConn, s3Client, presigner, bucket), nil
	}
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

	// DuckLake mode: decode din's raw_events through the shared catalog
	// (no S3 store, no bucket layout). Selected by DUCKLAKE_CATALOG_DSN.
	if settings.DuckLakeCatalogDSN != "" {
		return startDuckLakeMaterializer(settings, pollInterval, logger)
	}

	// Local path → filesystem store (single-node); bucket name → S3.
	var store materializer.ObjectStore
	if isLocalBucket(settings.ParquetBucket) {
		var err error
		store, err = fsstore.New(strings.TrimPrefix(settings.ParquetBucket, "file://"))
		if err != nil {
			return nil, fmt.Errorf("creating filesystem store: %w", err)
		}
	} else {
		store = newS3ObjectStore(s3ClientFromSettings(settings), settings.ParquetBucket)
	}
	runner := materializer.New(materializer.Config{
		RawPrefix:         settings.RawPrefix,
		DecodedPrefix:     settings.DecodedPrefix,
		PollInterval:      pollInterval,
		ChainID:           settings.DIMORegistryChainID,
		VehicleNFTAddress: common.HexToAddress(settings.VehicleNFTAddress),
		Workers:           settings.MaterializerWorkers,
		BatchMaxFiles:     settings.MaterializerBatchFiles,
		BatchMaxBytes:     settings.MaterializerBatchBytes,
		CompactInterval:   time.Duration(settings.CompactIntervalSeconds) * time.Second,
		CompactMinFiles:   settings.CompactMinFiles,
		ShardIndex:        settings.MaterializerShardIndex,
		ShardCount:        settings.MaterializerShardCount,
	}, store, logger)

	return runMaterializerLoop(runner, nil, false, logger), nil // bucket mode: no DuckLake rollup to rebuild
}

// startDuckLakeMaterializer wires the materializer to read din's raw_events
// from the shared DuckLake catalog and write the decoded tables there. It
// owns its own DuckDB service (catalog attached) for the lifetime of the
// loop; the query backend opens a separate one.
func startDuckLakeMaterializer(settings *config.Settings, pollInterval time.Duration, logger zerolog.Logger) (func(), error) {
	// Sharding is not honored on the DuckLake path — readDelta reads the whole
	// raw_events delta and the single global ingest_progress cursor makes exactly
	// one logical processor (extra replicas just lose the cursor CAS and roll
	// back). Refuse the config rather than silently ignore it: run the
	// materializer as a single replicaCount=1 release (SR review #8).
	if settings.MaterializerShardCount > 1 {
		return nil, fmt.Errorf(
			"MATERIALIZER_SHARD_COUNT=%d is not supported on the DuckLake path: the global ingest_progress cursor allows only one materializer; run a single replicaCount=1 release",
			settings.MaterializerShardCount)
	}
	cfg := duckConfigFromSettings(settings)
	cfg.DuckLakeEnabled = true
	duckSvc, err := duck.NewService(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating DuckLake service: %w", err)
	}
	mat, err := materializer.NewDuckLakeMaterializer(context.Background(), duckSvc.DB(), logger)
	if err != nil {
		_ = duckSvc.Close()
		return nil, fmt.Errorf("creating DuckLake materializer: %w", err)
	}
	// Resolve externalized blob payloads from the same bucket the fetch path
	// presigns/downloads (settings.ParquetBucket): din writes payloads larger
	// than the inline threshold to a blob and leaves only the key on the row.
	mat = mat.WithBlobStore(s3ClientFromSettings(settings), settings.ParquetBucket).
		WithTempDir(settings.DuckDBTempDirectory) // stage batch parquet on the sized spill volume, not the root fs

	var decodedRetention time.Duration
	if settings.LakeDecodedRetention != "" {
		decodedRetention, err = time.ParseDuration(settings.LakeDecodedRetention)
		if err != nil {
			_ = duckSvc.Close()
			return nil, fmt.Errorf("invalid LAKE_DECODED_RETENTION %q: %w", settings.LakeDecodedRetention, err)
		}
	}
	runner := materializer.New(materializer.Config{
		PollInterval:      pollInterval,
		ChainID:           settings.DIMORegistryChainID,
		VehicleNFTAddress: common.HexToAddress(settings.VehicleNFTAddress),
		Workers:           settings.MaterializerWorkers,
		DecodedRetention:  decodedRetention,
	}, nil, logger).WithDuckLake(mat)

	// rebuildRollup is the opt-in disaster-recovery rebuild (LAKE_REBUILD_ROLLUP_ON_BOOT):
	// the per-batch recompute only touches a batch's subjects, so a dropped/truncated
	// rollup leaves dormant vehicles missing until rebuilt from the full base. It runs
	// in the loop goroutine BEFORE processing (never concurrently with it) — not
	// synchronously at boot — so this O(history) scan can't outlast the liveness probe
	// and CrashLoop the pod; a failure is logged and the loop proceeds (the per-batch
	// recompute still heals active vehicles).
	stop := runMaterializerLoop(runner, mat, settings.LakeRebuildRollupOnBoot, logger)
	return func() {
		stop()
		_ = duckSvc.Close()
	}, nil
}

// runMaterializerLoop runs runner.Run in a goroutine and returns a stop
// function that cancels it and waits for exit.
func runMaterializerLoop(runner *materializer.Runner, mat *materializer.DuckLakeMaterializer, rebuildRollup bool, logger zerolog.Logger) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if rebuildRollup {
			logger.Info().Msg("LAKE_REBUILD_ROLLUP_ON_BOOT set: rebuilding signals_latest from full base (may take a while on deep history)")
			if err := mat.RecomputeRollup(ctx); err != nil {
				// Non-fatal: log and proceed — crashing here would CrashLoop the
				// pod; the per-batch recompute still heals active vehicles.
				logger.Error().Err(err).Msg("signals_latest rebuild failed; continuing with the normal loop")
			} else {
				logger.Info().Msg("signals_latest rebuild complete")
			}
		}
		if err := runner.Run(ctx); err != nil {
			logger.Error().Err(err).Msg("materializer exited with error")
		}
	}()
	logger.Info().Msg("materializer started")
	return func() {
		cancel()
		<-done
	}
}
