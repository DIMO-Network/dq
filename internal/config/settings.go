// Package config holds application configuration settings.
package config

import (
	"github.com/DIMO-Network/clickhouse-infra/pkg/connect/config"
)

// Settings contains the application config.
type Settings struct {
	LogLevel                  string          `yaml:"LOG_LEVEL"`
	Port                      int             `yaml:"PORT"`
	GRPCPort                  int             `yaml:"GRPC_PORT"`
	MonPort                   int             `yaml:"MON_PORT"`
	EnablePprof               bool            `yaml:"ENABLE_PPROF"`
	MaxRequestDuration        string          `yaml:"MAX_REQUEST_DURATION"`
	ClickhouseSignal          config.Settings `yaml:"SIGNAL"`
	ClickhouseFileCatalogue   config.Settings `yaml:"FILE"`
	TokenExchangeJWTKeySetURL string          `yaml:"TOKEN_EXCHANGE_JWK_KEY_SET_URL"`
	TokenExchangeIssuer       string          `yaml:"TOKEN_EXCHANGE_ISSUER_URL"`
	// FetchGRPCRequireJWT makes a valid DIMO JWT mandatory on the fetch gRPC port.
	// The interceptor always rejects an *invalid* token; this flag controls
	// whether a *missing* one is rejected too. Default false eases rollout (admit
	// callers until they send a token); set true once callers are updated. Pair
	// with the NetworkPolicy — the fetch RPCs return any subject's raw data.
	FetchGRPCRequireJWT bool `yaml:"FETCH_GRPC_REQUIRE_JWT"`
	// S3 storage (cloud events)
	CloudEventBucket     string `yaml:"CLOUDEVENT_BUCKET"`
	EphemeralBucket      string `yaml:"EPHEMERAL_BUCKET"`
	ParquetBucket        string `yaml:"PARQUET_BUCKET"`
	S3AWSRegion          string `yaml:"S3_AWS_REGION"`
	S3AWSAccessKeyID     string `yaml:"S3_AWS_ACCESS_KEY_ID"`
	S3AWSSecretAccessKey string `yaml:"S3_AWS_SECRET_ACCESS_KEY"`
	// Identity API for device→vehicle DID resolution
	IdentityAPIURL string `yaml:"IDENTITY_API_URL"`
	// QueryBackend selects the signal/event query backend: clickhouse (default),
	// duckdb, ducklake (the shared DuckLake catalog — the cutover target), or
	// shadow (serve from clickhouse, mirror to the lake and compare).
	QueryBackend string `yaml:"QUERY_BACKEND"`
	// DuckLakeCatalogDSN is the shared DuckLake catalog (Postgres DSN in
	// prod, a catalog file path for single-node). Required when
	// QUERY_BACKEND=ducklake or the DuckLake materializer is enabled.
	DuckLakeCatalogDSN string `yaml:"DUCKLAKE_CATALOG_DSN"`
	// DuckLakeDataPath is where DuckLake parquet data files live (s3:// or
	// a local dir). Must match din's LAKE_DATA_PATH.
	DuckLakeDataPath string `yaml:"DUCKLAKE_DATA_PATH"`
	// DuckLakeReadOnly attaches the DuckLake catalog read-only. Set it on the
	// query/shadow fleet (which never writes the lake — only the single-writer
	// materializer does) so the reader fleet can sit on a Postgres read replica
	// and never opens the primary catalog read-write. Forced off when
	// MaterializerEnabled so the writer can never come up read-only.
	DuckLakeReadOnly bool `yaml:"DUCKLAKE_READ_ONLY"`
	// DuckLakeCatalogReadDSN is an optional Postgres read-replica DSN for the
	// read-only reader role. Empty reads the primary DuckLakeCatalogDSN.
	DuckLakeCatalogReadDSN string `yaml:"DUCKLAKE_CATALOG_READ_DSN"`
	// DuckDB parse-on-read query engine (maps into duck.Config).
	DuckDBMemoryLimit   string `yaml:"DUCKDB_MEMORY_LIMIT"`
	DuckDBThreads       int    `yaml:"DUCKDB_THREADS"`
	DuckDBExtensionDir  string `yaml:"DUCKDB_EXTENSION_DIR"`
	DuckDBTempDirectory string `yaml:"DUCKDB_TEMP_DIRECTORY"`
	DuckDBMaxConns      int    `yaml:"DUCKDB_MAX_CONNS"`
	// DuckDBS3Endpoint is a custom S3 endpoint for DuckDB's httpfs (e.g. MinIO).
	DuckDBS3Endpoint string `yaml:"DUCKDB_S3_ENDPOINT"`
	// RawPrefix is the key prefix for raw cloudevent parquet files in ParquetBucket.
	RawPrefix string `yaml:"RAW_PREFIX"`
	// DecodedPrefix is the key prefix for decoded signal/event/latest/summary parquet files.
	DecodedPrefix string `yaml:"DECODED_PREFIX"`
	// Materializer (post-fact decode loop raw -> decoded parquet).
	MaterializerEnabled      bool   `yaml:"MATERIALIZER_ENABLED"`
	MaterializerPollInterval string `yaml:"MATERIALIZER_POLL_INTERVAL"`
	MaterializerWorkers      int    `yaml:"MATERIALIZER_WORKERS"`
	MaterializerBatchFiles   int    `yaml:"MATERIALIZER_BATCH_FILES"`
	MaterializerBatchBytes   int64  `yaml:"MATERIALIZER_BATCH_BYTES"`
	CompactIntervalSeconds   int    `yaml:"COMPACT_INTERVAL_SECONDS"`
	CompactMinFiles          int    `yaml:"COMPACT_MIN_FILES"`
	// LakeDecodedRetention is a Go duration (e.g. "8760h"); decoded rows older
	// than this are pruned from lake.signals/events (CHD-38). Empty disables it.
	LakeDecodedRetention string `yaml:"LAKE_DECODED_RETENTION"`
	// LakeRebuildRollupOnBoot, when true, rebuilds lake.signals_latest from the
	// full base on materializer startup before the normal loop (RecomputeRollup) —
	// the disaster-recovery path for a dropped/truncated rollup. Default false:
	// it is O(history) and unnecessary in steady state. Set it for one boot to
	// repair, then unset.
	LakeRebuildRollupOnBoot bool `yaml:"LAKE_REBUILD_ROLLUP_ON_BOOT"`
	MaterializerShardIndex  int  `yaml:"MATERIALIZER_SHARD_INDEX"`
	MaterializerShardCount  int  `yaml:"MATERIALIZER_SHARD_COUNT"`
	// DIMO registry chain settings for vendor module DID construction.
	DIMORegistryChainID   uint64 `yaml:"DIMO_REGISTRY_CHAIN_ID"`
	VehicleNFTAddress     string `yaml:"VEHICLE_NFT_ADDRESS"`
	AftermarketNFTAddress string `yaml:"AFTERMARKET_NFT_ADDRESS"`
	SyntheticNFTAddress   string `yaml:"SYNTHETIC_NFT_ADDRESS"`
}

// Query backend values for QueryBackend.
const (
	// QueryBackendClickHouse serves all queries from ClickHouse (default).
	QueryBackendClickHouse = "clickhouse"
	// QueryBackendDuckDB serves signal/event queries from DuckDB over parquet.
	QueryBackendDuckDB = "duckdb"
	// QueryBackendShadow serves from ClickHouse while mirroring queries to
	// DuckDB in the background and comparing results.
	QueryBackendShadow = "shadow"
	// QueryBackendDuckLake serves signal/event queries from the DuckLake
	// catalog tables (lake.signals / lake.events).
	QueryBackendDuckLake = "ducklake"
)
