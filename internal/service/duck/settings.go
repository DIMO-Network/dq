package duck

// Default values applied by Config.withDefaults.
const (
	// DefaultRawPrefix is the default key prefix for raw cloudevent parquet files.
	DefaultRawPrefix = "raw"
	// DefaultDecodedPrefix is the default key prefix for decoded parquet files.
	DefaultDecodedPrefix = "decoded/v1"
	// DefaultMaxConns is the default maximum number of open DuckDB connections.
	DefaultMaxConns = 6
)

// Config holds DuckDB query-engine settings.
// It is kept separate from internal/config.Settings; wiring into the shared
// settings struct happens later.
type Config struct {
	// DuckDBMemoryLimit is the value for DuckDB's memory_limit pragma, e.g. "2GiB".
	// Empty means use the DuckDB default (80% of system memory).
	DuckDBMemoryLimit string `yaml:"DUCKDB_MEMORY_LIMIT"`
	// DuckDBThreads is the value for DuckDB's threads pragma. Zero means use the DuckDB default.
	DuckDBThreads int `yaml:"DUCKDB_THREADS"`
	// DuckDBExtensionDir is the directory DuckDB loads extensions from
	// (extension_directory pragma). Empty means use the DuckDB default (~/.duckdb).
	DuckDBExtensionDir string `yaml:"DUCKDB_EXTENSION_DIR"`
	// TempDirectory is where DuckDB spills data that exceeds the memory limit.
	TempDirectory string `yaml:"DUCKDB_TEMP_DIRECTORY"`
	// MaxConns caps sql.DB open connections. Zero means DefaultMaxConns.
	MaxConns int `yaml:"DUCKDB_MAX_CONNS"`

	// S3Enabled controls whether httpfs/aws extensions are loaded and an S3
	// secret is created. Disable for local-filesystem tests.
	S3Enabled bool `yaml:"DUCKDB_S3_ENABLED"`
	// S3AWSRegion is the AWS region for the S3 secret.
	S3AWSRegion string `yaml:"S3_AWS_REGION"`
	// S3AWSAccessKeyID is an explicit access key. Empty means use credential_chain.
	S3AWSAccessKeyID string `yaml:"S3_AWS_ACCESS_KEY_ID"`
	// S3AWSSecretAccessKey is the secret for S3AWSAccessKeyID.
	S3AWSSecretAccessKey string `yaml:"S3_AWS_SECRET_ACCESS_KEY"`
	// S3Endpoint is a custom S3 endpoint (e.g. MinIO). May include an
	// http:// or https:// scheme; http disables SSL. When set, url_style=path is used.
	S3Endpoint string `yaml:"S3_ENDPOINT"`

	// Bucket is the parquet bucket. Plain names are treated as s3://<name>;
	// file:// URLs and absolute paths are treated as local directories (tests).
	Bucket string `yaml:"PARQUET_BUCKET"`
	// RawPrefix is the key prefix for raw cloudevents. Empty means DefaultRawPrefix.
	RawPrefix string `yaml:"RAW_PREFIX"`
	// DecodedPrefix is the key prefix for decoded signals/events/latest. Empty means DefaultDecodedPrefix.
	DecodedPrefix string `yaml:"DECODED_PREFIX"`
}

// withDefaults returns a copy of the config with zero values replaced by defaults.
func (c Config) withDefaults() Config {
	if c.RawPrefix == "" {
		c.RawPrefix = DefaultRawPrefix
	}
	if c.DecodedPrefix == "" {
		c.DecodedPrefix = DefaultDecodedPrefix
	}
	if c.MaxConns <= 0 {
		c.MaxConns = DefaultMaxConns
	}
	return c
}
