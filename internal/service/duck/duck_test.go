package duck

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLocalService creates a Service with S3 disabled, suitable for querying
// local parquet files. Closed automatically at test cleanup.
func newLocalService(t *testing.T, cfg Config) *Service {
	t.Helper()
	svc, err := NewService(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	return svc
}

func TestNewServiceAppliesPragmas(t *testing.T) {
	tempDir := t.TempDir()
	svc := newLocalService(t, Config{
		DuckDBMemoryLimit: "512MiB",
		DuckDBThreads:     2,
		TempDirectory:     tempDir,
	})

	var memLimit, tmp string
	var threads int64
	var objCache bool
	row := svc.DB().QueryRowContext(context.Background(),
		`SELECT current_setting('memory_limit'),
		        current_setting('threads'),
		        current_setting('temp_directory'),
		        current_setting('enable_object_cache')`)
	require.NoError(t, row.Scan(&memLimit, &threads, &tmp, &objCache))

	assert.Contains(t, memLimit, "512", "memory_limit pragma not applied, got %q", memLimit)
	assert.EqualValues(t, 2, threads)
	assert.Equal(t, tempDir, tmp)
	assert.True(t, objCache, "enable_object_cache not applied")
}

func TestNewServiceDefaults(t *testing.T) {
	// A zero-value config must still bootstrap a usable service (defaults
	// applied) and answer a query.
	svc := newLocalService(t, Config{})

	var one int
	require.NoError(t, svc.DB().QueryRow("SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)
}

func TestNewServiceBadConfigFailsFast(t *testing.T) {
	_, err := NewService(Config{DuckDBMemoryLimit: "not-a-size"})
	require.Error(t, err, "invalid memory_limit should fail at NewService, not first query")
	assert.Contains(t, err.Error(), "memory_limit")
}

func TestCreateS3SecretSQL(t *testing.T) {
	t.Run("explicit keys with minio endpoint", func(t *testing.T) {
		got := createS3SecretSQL(Config{
			S3AWSRegion:          "us-east-1",
			S3AWSAccessKeyID:     "AKIA123",
			S3AWSSecretAccessKey: "shh's",
			S3Endpoint:           "http://localhost:9000",
		})
		want := "CREATE OR REPLACE SECRET dq_s3 (TYPE s3, KEY_ID 'AKIA123', SECRET 'shh''s', REGION 'us-east-1', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false)"
		assert.Equal(t, want, got)
	})

	t.Run("credential chain", func(t *testing.T) {
		got := createS3SecretSQL(Config{S3AWSRegion: "us-east-1"})
		want := "CREATE OR REPLACE SECRET dq_s3 (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')"
		assert.Equal(t, want, got)
	})

	t.Run("https endpoint keeps ssl", func(t *testing.T) {
		got := createS3SecretSQL(Config{S3Endpoint: "https://minio.internal:9000"})
		assert.Contains(t, got, "ENDPOINT 'minio.internal:9000'")
		assert.Contains(t, got, "USE_SSL true")
	})
}

func TestServiceClose(t *testing.T) {
	svc, err := NewService(Config{})
	require.NoError(t, err)
	require.NoError(t, svc.Close())
	require.Error(t, svc.DB().Ping(), "queries after Close must fail")
}
