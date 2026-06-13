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
		DuckLakeEnabled:      settings.QueryBackend == config.QueryBackendDuckLake,
		CatalogDSN:           settings.DuckLakeCatalogDSN,
		DataPath:             settings.DuckLakeDataPath,
	}
}

// isLocalBucket reports whether the parquet bucket points at the local
// filesystem (file:// URL or absolute path) instead of S3, mirroring how
// duck.Service interprets its Bucket setting.
func isLocalBucket(bucket string) bool {
	return strings.HasPrefix(bucket, "file://") || strings.HasPrefix(bucket, "/")
}

// newQueryBackend selects the Repository backend per QUERY_BACKEND. It
// returns the backend, a cleanup function (always non-nil), and an error for
// unknown backend values. The clickhouse backend (default) needs no DuckDB
// resources at all.
func newQueryBackend(settings *config.Settings, chService *ch.Service, logger zerolog.Logger) (repositories.CHService, func(), error) {
	backend := settings.QueryBackend
	if backend == "" {
		backend = config.QueryBackendClickHouse
	}
	if backend == config.QueryBackendClickHouse {
		return chService, func() {}, nil
	}

	duckSvc, err := duck.NewService(duckConfigFromSettings(settings))
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't create DuckDB service: %w", err)
	}
	switch backend {
	case config.QueryBackendDuckLake:
		// Reads come from the DuckLake catalog tables; segment detection
		// stays on ClickHouse.
		return repositories.ComposeBackend(duck.NewLakeQueries(duckSvc), chService), closeDuck(duckSvc, logger), nil
	}

	queries := duck.NewQueries(duckSvc, "")
	switch backend {
	case config.QueryBackendDuckDB:
		// Segment detection is not implemented on DuckDB; it stays on ClickHouse.
		return repositories.ComposeBackend(queries, chService), closeDuck(duckSvc, logger), nil
	case config.QueryBackendShadow:
		shadow := repositories.NewShadowBackend(chService, queries, logger)
		cleanup := func() {
			shadow.Wait()
			closeDuck(duckSvc, logger)()
		}
		return shadow, cleanup, nil
	default:
		_ = duckSvc.Close()
		return nil, nil, fmt.Errorf("unknown QUERY_BACKEND %q (expected %s, %s, %s, or %s)",
			settings.QueryBackend, config.QueryBackendClickHouse, config.QueryBackendDuckDB, config.QueryBackendShadow, config.QueryBackendDuckLake)
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := runner.Run(ctx); err != nil {
			logger.Error().Err(err).Msg("materializer exited with error")
		}
	}()
	logger.Info().Msg("materializer started")

	return func() {
		cancel()
		<-done
	}, nil
}
