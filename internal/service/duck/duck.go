// Package duck provides an embedded DuckDB query engine over parquet files
// stored on S3 (or the local filesystem in tests).
package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// Service wraps a DuckDB-backed *sql.DB.
type Service struct {
	db  *sql.DB
	cfg Config
}

// NewService opens an in-memory DuckDB database and applies the bootstrap
// configuration (pragmas, extensions, S3 secret) on every new connection.
func NewService(cfg Config) (*Service, error) {
	cfg = cfg.withDefaults()
	bootstrap := bootstrapQueries(cfg)

	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		for _, query := range bootstrap {
			if _, err := execer.ExecContext(context.Background(), query, nil); err != nil {
				return fmt.Errorf("failed to run bootstrap query %q: %w", redactQuery(query), err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create duckdb connector: %w", err)
	}

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(cfg.MaxConns)
	db.SetMaxIdleConns(cfg.MaxConns)
	// Recycle connections so a DuckLake→Postgres catalog attach poisoned by a
	// PG blip is dropped and re-bootstrapped, not pinned in the pool (CHD-21).
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// Force one connection open so bootstrap errors surface at startup
	// instead of on the first query.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize duckdb connection: %w", err)
	}

	return &Service{db: db, cfg: cfg}, nil
}

// DB returns the underlying *sql.DB.
func (s *Service) DB() *sql.DB {
	return s.db
}

// Config returns the resolved (defaults applied) configuration.
func (s *Service) Config() Config {
	return s.cfg
}

// Close closes the underlying database and its connector.
func (s *Service) Close() error {
	return s.db.Close()
}

// bootstrapQueries builds the per-connection initialization statements.
// All statements are idempotent so they can safely run on every connection
// in the pool against the shared database instance.
func bootstrapQueries(cfg Config) []string {
	var queries []string
	if cfg.DuckDBMemoryLimit != "" {
		queries = append(queries, fmt.Sprintf("SET memory_limit = %s", sqlString(cfg.DuckDBMemoryLimit)))
	}
	if cfg.DuckDBThreads > 0 {
		queries = append(queries, fmt.Sprintf("SET threads = %d", cfg.DuckDBThreads))
	}
	if cfg.TempDirectory != "" {
		queries = append(queries, fmt.Sprintf("SET temp_directory = %s", sqlString(cfg.TempDirectory)))
	}
	queries = append(queries, "SET enable_object_cache = true")
	// Decoded parquet timestamps are TIMESTAMPTZ and our queries inline
	// naive make_timestamp literals, which DuckDB resolves in the session
	// TimeZone. Pin UTC so results don't depend on the host zone.
	queries = append(queries, "SET TimeZone = 'UTC'")

	// extension_directory must be set before INSTALL/LOAD so pre-baked
	// extensions (see Dockerfile) are found.
	if cfg.DuckDBExtensionDir != "" {
		queries = append(queries, fmt.Sprintf("SET extension_directory = %s", sqlString(cfg.DuckDBExtensionDir)))
	}

	if cfg.S3Enabled {
		queries = append(queries,
			"INSTALL httpfs",
			"LOAD httpfs",
			"INSTALL aws",
			"LOAD aws",
			createS3SecretSQL(cfg),
		)
	}

	if cfg.DuckLakeEnabled {
		queries = append(queries, "INSTALL ducklake", "LOAD ducklake")
		if cfg.CatalogIsPostgres() {
			// The catalog lives in Postgres; the postgres extension backs it.
			queries = append(queries, "INSTALL postgres", "LOAD postgres")
		}
		queries = append(queries, attachDuckLakeSQL(cfg))
		// Attach the catalog's side database as "meta" so the materializer can
		// report consumer progress into din's meta.din_consumer_progress
		// (the snapshot-expiry floor). Same target din attaches.
		queries = append(queries, fmt.Sprintf("ATTACH IF NOT EXISTS %s AS meta%s",
			sqlString(cfg.MetaTarget()), cfg.MetaAttachOpts()))
	}
	return queries
}

// attachDuckLakeSQL attaches the DuckLake catalog as schema "lake".
// IF NOT EXISTS makes it idempotent across pooled connections.
func attachDuckLakeSQL(cfg Config) string {
	if cfg.DataPath != "" {
		return fmt.Sprintf("ATTACH IF NOT EXISTS %s AS lake (DATA_PATH %s)",
			sqlString(cfg.catalogURI()), sqlString(cfg.DataPath))
	}
	return fmt.Sprintf("ATTACH IF NOT EXISTS %s AS lake", sqlString(cfg.catalogURI()))
}

// createS3SecretSQL builds the CREATE SECRET statement for S3 access.
// Explicit keys are used when provided; otherwise the AWS credential chain
// (env vars, IRSA, instance profile, ...) is used.
func createS3SecretSQL(cfg Config) string {
	params := []string{"TYPE s3"}

	if cfg.S3AWSAccessKeyID != "" {
		params = append(params,
			fmt.Sprintf("KEY_ID %s", sqlString(cfg.S3AWSAccessKeyID)),
			fmt.Sprintf("SECRET %s", sqlString(cfg.S3AWSSecretAccessKey)),
		)
	} else {
		params = append(params, "PROVIDER credential_chain")
	}

	if cfg.S3AWSRegion != "" {
		params = append(params, fmt.Sprintf("REGION %s", sqlString(cfg.S3AWSRegion)))
	}

	if cfg.S3Endpoint != "" {
		endpoint := cfg.S3Endpoint
		useSSL := true
		if rest, ok := strings.CutPrefix(endpoint, "http://"); ok {
			endpoint, useSSL = rest, false
		} else if rest, ok := strings.CutPrefix(endpoint, "https://"); ok {
			endpoint = rest
		}
		params = append(params,
			fmt.Sprintf("ENDPOINT %s", sqlString(endpoint)),
			"URL_STYLE 'path'",
			fmt.Sprintf("USE_SSL %t", useSSL),
		)
	}

	return fmt.Sprintf("CREATE OR REPLACE SECRET dq_s3 (%s)", strings.Join(params, ", "))
}

// sqlString quotes a value as a single-quoted SQL string literal.
func sqlString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

// redactQuery hides credential-bearing bootstrap statements from error messages
// and logs: CREATE SECRET inlines S3 keys and ATTACH carries the Postgres
// DSN/password. Truncates at the first paren/quote so the statement kind stays
// visible for debugging while the secret is dropped. Mirrors din's lake.redact
// (CHD-31).
func redactQuery(q string) string {
	if strings.Contains(q, "SECRET") || strings.Contains(q, "ATTACH") {
		if i := strings.IndexAny(q, "('"); i > 0 {
			return q[:i] + "(…)"
		}
	}
	return q
}
