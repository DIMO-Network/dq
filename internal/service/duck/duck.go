// Package duck provides an embedded DuckDB query engine over parquet files
// stored on S3 (or the local filesystem in tests).
package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	duckdb "github.com/marcboeker/go-duckdb/v2"
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
				return fmt.Errorf("failed to run bootstrap query %q: %w", query, err)
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
	return queries
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
