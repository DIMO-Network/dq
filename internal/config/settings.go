// Package config holds application configuration settings.
package config

import "fmt"

// Settings contains the application config.
type Settings struct {
	LogLevel           string `yaml:"LOG_LEVEL"`
	Port               int    `yaml:"PORT"`
	GRPCPort           int    `yaml:"GRPC_PORT"`
	MonPort            int    `yaml:"MON_PORT"`
	EnablePprof        bool   `yaml:"ENABLE_PPROF"`
	MaxRequestDuration string `yaml:"MAX_REQUEST_DURATION"`
	// MaxConcurrentPerSubject bounds in-flight HTTP requests per authenticated JWT
	// subject so one caller can't pin the whole DuckDB pool and starve co-tenants on
	// a replica. 0 (default) disables it — opt-in, since the right ceiling depends on
	// the pool size + real query mix. A rejected request gets 429.
	MaxConcurrentPerSubject   int    `yaml:"MAX_CONCURRENT_REQUESTS_PER_SUBJECT"`
	TokenExchangeJWTKeySetURL string `yaml:"TOKEN_EXCHANGE_JWK_KEY_SET_URL"`
	TokenExchangeIssuer       string `yaml:"TOKEN_EXCHANGE_ISSUER_URL"`
	// FetchGRPCRequireJWT makes a valid DIMO JWT mandatory on the fetch gRPC port.
	// The interceptor always rejects an *invalid* token; this flag controls
	// whether a *missing* one is rejected too. Default false eases rollout (admit
	// callers until they send a token); set true once callers are updated. Pair
	// with the NetworkPolicy — the fetch RPCs return any subject's raw data.
	FetchGRPCRequireJWT bool `yaml:"FETCH_GRPC_REQUIRE_JWT"`
	// BlobBucket is the S3 bucket holding externalized cloudevent payloads (large
	// blobs din writes); dq presigns/downloads from it. Same bucket as din's
	// BLOB_BUCKET (the lake parquet lives separately at DUCKLAKE_DATA_PATH).
	BlobBucket           string `yaml:"BLOB_BUCKET"`
	S3AWSRegion          string `yaml:"S3_AWS_REGION"`
	S3AWSAccessKeyID     string `yaml:"S3_AWS_ACCESS_KEY_ID"`
	S3AWSSecretAccessKey string `yaml:"S3_AWS_SECRET_ACCESS_KEY"`
	// Identity API for device→vehicle DID resolution
	IdentityAPIURL string `yaml:"IDENTITY_API_URL"`
	// DuckLakeCatalogDSN is the shared DuckLake catalog (Postgres DSN in
	// prod, a catalog file path for single-node). Required for the query fleet
	// and when the DuckLake materializer is enabled.
	DuckLakeCatalogDSN string `yaml:"DUCKLAKE_CATALOG_DSN"`
	// DuckLakeDataPath is where DuckLake parquet data files live (s3:// or
	// a local dir). Must match din's LAKE_DATA_PATH.
	DuckLakeDataPath string `yaml:"DUCKLAKE_DATA_PATH"`
	// DuckLakeReadOnly attaches the DuckLake catalog read-only. Set it on the
	// read/query fleet (which never writes the lake — only the single-writer
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
	// Materializer (post-fact decode loop: din raw_events -> decoded lake tables).
	MaterializerEnabled      bool   `yaml:"MATERIALIZER_ENABLED"`
	MaterializerPollInterval string `yaml:"MATERIALIZER_POLL_INTERVAL"`
	MaterializerWorkers      int    `yaml:"MATERIALIZER_WORKERS"`
	// MaterializerBackfillMode tunes the writer for a large one-time catch-up
	// (initial historical load, long downtime): it skips the cross-batch dedup
	// anti-join and flushes signals_latest once on catch-up instead of mid-drain.
	// Default false (steady state). Set it for the initial backfill, then unset.
	MaterializerBackfillMode bool `yaml:"MATERIALIZER_BACKFILL_MODE"`
	// LakeDecodedRetention is a Go duration (e.g. "8760h"); decoded rows older
	// than this are pruned from lake.signals/events (CHD-38). Empty disables it.
	LakeDecodedRetention string `yaml:"LAKE_DECODED_RETENTION"`
	// LakeRebuildRollupOnBoot, when true, rebuilds lake.signals_latest from the
	// full base on materializer startup before the normal loop (RecomputeRollup) —
	// the disaster-recovery path for a dropped/truncated rollup. Default false:
	// it is O(history) and unnecessary in steady state. Set it for one boot to
	// repair, then unset.
	LakeRebuildRollupOnBoot bool `yaml:"LAKE_REBUILD_ROLLUP_ON_BOOT"`
	// MaterializerShardCount is read only to REJECT sharding: the DuckLake path's
	// single global ingest_progress cursor allows exactly one materializer, so a
	// value > 1 is refused (run a single replicaCount=1 release). See
	// startDuckLakeMaterializer.
	MaterializerShardCount int `yaml:"MATERIALIZER_SHARD_COUNT"`
	// DIMO registry chain settings for vendor module DID construction.
	DIMORegistryChainID   uint64 `yaml:"DIMO_REGISTRY_CHAIN_ID"`
	VehicleNFTAddress     string `yaml:"VEHICLE_NFT_ADDRESS"`
	AftermarketNFTAddress string `yaml:"AFTERMARKET_NFT_ADDRESS"`
	SyntheticNFTAddress   string `yaml:"SYNTHETIC_NFT_ADDRESS"`
}

// Validate checks the boot-critical numeric settings. The shared env loader swallows
// per-field parse errors and silently leaves the field zero (e.g. DUCKDB_THREADS=four or
// MATERIALIZER_ENABLED=ture), so a malformed value never fails LoadConfig — these checks
// turn the dangerous ones into a loud boot failure. A zero port binds ":0" and black-holes
// traffic; a zero chain id decodes every vehicle DID wrong.
func (s *Settings) Validate() error {
	for name, port := range map[string]int{"PORT": s.Port, "GRPC_PORT": s.GRPCPort, "MON_PORT": s.MonPort} {
		if port <= 0 {
			return fmt.Errorf("%s must be > 0, got %d (missing or malformed env?)", name, port)
		}
	}
	if s.DIMORegistryChainID == 0 {
		return fmt.Errorf("DIMO_REGISTRY_CHAIN_ID must be non-zero (missing or malformed env?)")
	}
	return nil
}
