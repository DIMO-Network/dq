package duck

import (
	"strings"
	"time"
)

// Default values applied by Config.withDefaults.
const (
	// DefaultRawPrefix is the default key prefix for raw cloudevent parquet files.
	DefaultRawPrefix = "raw"
	// DefaultDecodedPrefix is the default key prefix for decoded parquet files.
	DefaultDecodedPrefix = "decoded/v1"
	// DefaultMaxConns is the default maximum number of open DuckDB connections.
	DefaultMaxConns = 6
	// DefaultConnMaxLifetime caps how long a pooled DuckDB connection lives
	// before it is recycled. The DuckLake catalog is reached over a Postgres
	// attach inside each connection; recycling drops a connection whose attach
	// was poisoned by a catalog blip so it re-bootstraps (re-ATTACH) instead of
	// staying broken until pod restart (CHD-21).
	DefaultConnMaxLifetime = 30 * time.Minute
	// DefaultConnMaxIdleTime retires idle connections so a poisoned one is not
	// pinned in the pool indefinitely.
	DefaultConnMaxIdleTime = 5 * time.Minute
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
	// ConnMaxLifetime / ConnMaxIdleTime recycle pooled DuckDB connections so a
	// connection whose DuckLake→Postgres catalog attach is poisoned by a PG blip
	// is dropped and re-bootstrapped rather than staying broken (CHD-21). Zero
	// means the defaults; negative disables recycling.
	ConnMaxLifetime time.Duration `yaml:"DUCKDB_CONN_MAX_LIFETIME"`
	ConnMaxIdleTime time.Duration `yaml:"DUCKDB_CONN_MAX_IDLE_TIME"`

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

	// DuckLakeEnabled attaches a DuckLake catalog as schema "lake" on every
	// connection. The decoded layer then lives as DuckLake tables
	// (lake.signals / lake.events) instead of bucket parquet files.
	DuckLakeEnabled bool `yaml:"DUCKLAKE_ENABLED"`
	// CatalogDSN is the DuckLake catalog. A "postgres:" / "postgresql:" DSN
	// (prod, concurrent writers) loads the postgres extension; anything else
	// is treated as a catalog file path (single-writer; tests, single-node).
	CatalogDSN string `yaml:"DUCKLAKE_CATALOG_DSN"`
	// DataPath is where DuckLake writes parquet data files: an s3:// prefix
	// in prod, a local directory in tests.
	DataPath string `yaml:"DUCKLAKE_DATA_PATH"`
	// ReadOnly attaches the DuckLake catalog (and its meta side database) in
	// READ_ONLY mode. Only the single-writer materializer writes the lake; the
	// read/query fleet never does, so it attaches read-only. Besides being
	// defense-in-depth, a read-only attach lets the reader fleet point at a
	// Postgres read replica (CatalogReadDSN) so the catalog read load of many
	// query replicas never lands on the primary that din ingest, din
	// maintenance, and the materializer all write. DuckLake supports
	// `ATTACH 'ducklake:...' (READ_ONLY)`; backend.go forces this off whenever
	// the materializer is enabled so a writer can never come up read-only.
	ReadOnly bool `yaml:"DUCKLAKE_READ_ONLY"`
	// CatalogReadDSN is an optional Postgres read-replica DSN used only by the
	// read-only reader role. Empty reads the primary CatalogDSN. Ignored when
	// ReadOnly is false.
	CatalogReadDSN string `yaml:"DUCKLAKE_CATALOG_READ_DSN"`

	// LoadSpatial loads the spatial extension (for the ST_* geofence filters in
	// aggregations.go) on every connection. Read/query connections only: spatial's
	// RTreeIndexScanOptimizer hooks the planner and crashes (INTERNAL Error) on the
	// materializer's DuckLake delta read, so the materializer leaves this false.
	LoadSpatial bool `yaml:"DUCKDB_LOAD_SPATIAL"`
}

// effectiveCatalogDSN is the catalog DSN this role connects to: the read
// replica (CatalogReadDSN) for a read-only reader when one is configured,
// otherwise the primary CatalogDSN. Centralizing the choice keeps catalogURI,
// CatalogIsPostgres, and MetaTarget agreeing on a single target.
func (c Config) effectiveCatalogDSN() string {
	if c.ReadOnly && c.CatalogReadDSN != "" {
		return c.CatalogReadDSN
	}
	return c.CatalogDSN
}

// CatalogIsPostgres reports whether the DuckLake catalog DSN names a Postgres
// database (vs a local catalog file). The DSN is the RAW connection string
// (no "postgres:" prefix) — same convention as din's LAKE_CATALOG_DSN, so an
// operator sets one DSN format for both services attaching the catalog.
func (c Config) CatalogIsPostgres() bool {
	dsn := c.effectiveCatalogDSN()
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") ||
		strings.Contains(dsn, "host=") || strings.Contains(dsn, "dbname=")
}

// catalogURI maps the DSN onto a ducklake ATTACH URI, matching din.catalogURI.
func (c Config) catalogURI() string {
	if c.CatalogIsPostgres() {
		return "ducklake:postgres:" + c.effectiveCatalogDSN()
	}
	return "ducklake:" + c.effectiveCatalogDSN()
}

// MetaTarget is the ATTACH target for the side database holding consumer
// progress (din's meta.din_consumer_progress): the catalog Postgres DSN
// itself, or a DuckDB file beside a local catalog. Mirrors din lake.metaTarget
// exactly so both attach the same database.
func (c Config) MetaTarget() string {
	if c.CatalogIsPostgres() {
		return c.effectiveCatalogDSN()
	}
	return c.effectiveCatalogDSN() + ".progress.db"
}

// MetaAttachOpts is the ATTACH options clause for the meta database. A
// read-only reader attaches meta read-only too: it never writes consumer
// progress (only the materializer does), and a read-only attach is what lets
// the reader role sit on a Postgres read replica.
func (c Config) MetaAttachOpts() string {
	if c.CatalogIsPostgres() {
		if c.ReadOnly {
			return " (TYPE postgres, READ_ONLY)"
		}
		return " (TYPE postgres)"
	}
	if c.ReadOnly {
		return " (READ_ONLY)"
	}
	return ""
}

// withDefaults returns a copy of the config with zero values replaced by defaults.
func (c Config) withDefaults() Config {
	if c.RawPrefix == "" {
		c.RawPrefix = DefaultRawPrefix
	}
	if c.DecodedPrefix == "" {
		c.DecodedPrefix = DefaultDecodedPrefix
	}
	// The materializer normalizes its prefixes WITH a trailing slash; the
	// path builders here join with "/" themselves. Trim so an operator
	// setting "decoded/v1/" doesn't build double-slash keys that match
	// nothing.
	c.RawPrefix = strings.TrimSuffix(c.RawPrefix, "/")
	c.DecodedPrefix = strings.TrimSuffix(c.DecodedPrefix, "/")
	if c.MaxConns <= 0 {
		c.MaxConns = DefaultMaxConns
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = DefaultConnMaxLifetime
	}
	if c.ConnMaxIdleTime == 0 {
		c.ConnMaxIdleTime = DefaultConnMaxIdleTime
	}
	return c
}
