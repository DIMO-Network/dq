package duck

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	svc := newLocalService(t, Config{})
	cfg := svc.Config()
	assert.Equal(t, DefaultRawPrefix, cfg.RawPrefix)
	assert.Equal(t, DefaultDecodedPrefix, cfg.DecodedPrefix)
	assert.Equal(t, DefaultMaxConns, cfg.MaxConns)

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

// writeRawFixture writes a small parquet file at
// <root>/raw/type=<ceType>/date=<day>/part-0.parquet using DuckDB itself.
// Each row carries tag = "<ceType>|<day>" so tests can assert which
// partitions were actually read.
func writeRawFixture(t *testing.T, svc *Service, root, ceType, day string, rows int) {
	t.Helper()
	dir := filepath.Join(root, "raw", "type="+ceType, "date="+day)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, "part-0.parquet")
	tag := ceType + "|" + day
	query := fmt.Sprintf(
		`COPY (SELECT %s AS tag, CAST(r.range AS INTEGER) AS n FROM range(%d) r) TO %s (FORMAT PARQUET)`,
		sqlString(tag), rows, sqlString(path),
	)
	_, err := svc.DB().Exec(query)
	require.NoError(t, err)
}

func queryTags(t *testing.T, svc *Service, globs []string) []string {
	t.Helper()
	query := "SELECT DISTINCT tag FROM " + ReadParquetSQL(globs) + " ORDER BY tag"
	rows, err := svc.DB().QueryContext(context.Background(), query)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

	var tags []string
	for rows.Next() {
		var tag string
		require.NoError(t, rows.Scan(&tag))
		tags = append(tags, tag)
	}
	require.NoError(t, rows.Err())
	return tags
}

func TestServiceReadsLocalParquetWithDatePruning(t *testing.T) {
	root := t.TempDir()
	svc := newLocalService(t, Config{
		DuckDBThreads: 2,
		TempDirectory: t.TempDir(),
		Bucket:        root,
	})

	days := []string{"2026-06-01", "2026-06-02", "2026-06-03"}
	types := []string{"dimo.status", "dimo.event"}
	for _, day := range days {
		for _, ceType := range types {
			writeRawFixture(t, svc, root, ceType, day, 3)
		}
	}

	t.Run("single day single type reads only that partition", func(t *testing.T) {
		globs := RawGlobs(root, "raw", []string{"dimo.status"}, date(2026, time.June, 2), date(2026, time.June, 2))
		require.Len(t, globs, 1)
		tags := queryTags(t, svc, globs)
		assert.Equal(t, []string{"dimo.status|2026-06-02"}, tags,
			"a 1-day query must only read that day's files")
	})

	t.Run("multi day multi type reads all requested partitions", func(t *testing.T) {
		globs := RawGlobs(root, "raw", types, date(2026, time.June, 1), date(2026, time.June, 3))
		require.Len(t, globs, 6)
		tags := queryTags(t, svc, globs)
		want := []string{
			"dimo.event|2026-06-01", "dimo.event|2026-06-02", "dimo.event|2026-06-03",
			"dimo.status|2026-06-01", "dimo.status|2026-06-02", "dimo.status|2026-06-03",
		}
		assert.Equal(t, want, tags)
	})

	t.Run("partial range excludes out-of-range days", func(t *testing.T) {
		globs := RawGlobs(root, "raw", types, date(2026, time.June, 1), date(2026, time.June, 2))
		tags := queryTags(t, svc, globs)
		for _, tag := range tags {
			assert.False(t, strings.HasSuffix(tag, "2026-06-03"), "day 3 leaked into 2-day query: %s", tag)
		}
		assert.Len(t, tags, 4)
	})

	t.Run("hive partition columns are queryable", func(t *testing.T) {
		globs := RawGlobs(root, "raw", types, date(2026, time.June, 1), date(2026, time.June, 1))
		query := `SELECT DISTINCT type, CAST(date AS VARCHAR) FROM ` + ReadParquetSQL(globs) + ` ORDER BY type`
		rows, err := svc.DB().Query(query)
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck

		var got [][2]string
		for rows.Next() {
			var ceType, day string
			require.NoError(t, rows.Scan(&ceType, &day))
			got = append(got, [2]string{ceType, day})
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, [][2]string{{"dimo.event", "2026-06-01"}, {"dimo.status", "2026-06-01"}}, got)
	})

	t.Run("row counts and aggregation", func(t *testing.T) {
		globs := RawGlobs(root, "raw", []string{"dimo.status"}, date(2026, time.June, 1), date(2026, time.June, 3))
		var count, sum int
		query := "SELECT COUNT(*), SUM(n) FROM " + ReadParquetSQL(globs)
		require.NoError(t, svc.DB().QueryRow(query).Scan(&count, &sum))
		assert.Equal(t, 9, count, "3 days x 3 rows")
		assert.Equal(t, 9, sum, "3 days x (0+1+2)")
	})
}

func TestServiceReadsDecodedSignalGlobs(t *testing.T) {
	root := t.TempDir()
	svc := newLocalService(t, Config{Bucket: root})

	for _, day := range []string{"2026-06-01", "2026-06-02"} {
		dir := filepath.Join(root, "decoded", "v1", "signals", "date="+day)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		query := fmt.Sprintf(
			`COPY (SELECT %s AS subject, %s AS day, 42.0 AS value) TO %s (FORMAT PARQUET)`,
			sqlString(testSubject1), sqlString(day), sqlString(filepath.Join(dir, "part-0.parquet")),
		)
		_, err := svc.DB().Exec(query)
		require.NoError(t, err)
	}

	globs := DecodedSignalGlobs(root, "decoded/v1", date(2026, time.June, 1), date(2026, time.June, 1))
	require.Len(t, globs, 1)

	var subject, day string
	query := "SELECT subject, day FROM " + ReadParquetSQL(globs)
	require.NoError(t, svc.DB().QueryRow(query).Scan(&subject, &day))
	assert.Equal(t, testSubject1, subject)
	assert.Equal(t, "2026-06-01", day, "1-day decoded query must only read that day's file")
}

func TestServiceClose(t *testing.T) {
	svc, err := NewService(Config{})
	require.NoError(t, err)
	require.NoError(t, svc.Close())
	require.Error(t, svc.DB().Ping(), "queries after Close must fail")
}
