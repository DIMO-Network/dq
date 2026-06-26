package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/config"
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

// duckConfigFromSettings maps the app settings into the DuckDB query-engine
// config, reusing the shared S3/bucket settings.
func duckConfigFromSettings(settings *config.Settings) duck.Config {
	bucket := settings.BlobBucket
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
	}
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
	duckSvc, err := duck.NewService(duckConfigFromSettings(settings))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("couldn't create DuckDB service: %w", err)
	}
	// Reads and segment detection both come from the DuckLake catalog. Return
	// duckSvc so the caller can share it with newEventService.
	return repositories.ComposeBackend(duck.NewLakeQueries(duckSvc), duck.NewLakeSegments(duckSvc)), duckSvc, closeDuck(duckSvc, logger), nil
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
	// duckConfigFromSettings already sets DuckLakeEnabled. The materializer reads the
	// DuckLake delta (ducklake_table_changes); spatial's RTreeIndexScanOptimizer
	// crashes the planner on that read, so never load it here.
	cfg.LoadSpatial = false
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
	// presigns/downloads (settings.BlobBucket): din writes payloads larger
	// than the inline threshold to a blob and leaves only the key on the row.
	blobCipher, err := blobcrypt.NewCipher(settings.BlobEncryptionKey)
	if err != nil {
		_ = duckSvc.Close()
		return nil, fmt.Errorf("blob cipher: %w", err)
	}
	mat = mat.WithBlobStore(s3ClientFromSettings(settings), settings.BlobBucket).
		WithBlobCipher(blobCipher).
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
		BackfillMode:      settings.MaterializerBackfillMode,
	}, logger).WithDuckLake(mat)

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
