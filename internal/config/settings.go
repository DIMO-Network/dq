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
	// duckdb, or shadow (serve from clickhouse, mirror to duckdb and compare).
	QueryBackend string `yaml:"QUERY_BACKEND"`
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
)
