package config

import "testing"

// TestLakeS3Endpoint locks in the fallback that fixes the GCP materializer crash:
// a deploy that sets only S3_ENDPOINT must still configure the DuckDB lake path,
// otherwise the lake secret has no ENDPOINT and DuckDB resolves the AWS default
// host (s3.<region>.amazonaws.com).
func TestLakeS3Endpoint(t *testing.T) {
	cases := []struct {
		name     string
		duckDB   string
		s3       string
		expected string
	}{
		{"falls back to S3_ENDPOINT", "", "https://storage.googleapis.com", "https://storage.googleapis.com"},
		{"explicit override wins", "http://lake-minio:9000", "http://blob-minio:9000", "http://lake-minio:9000"},
		{"both empty", "", "", ""},
		{"only DUCKDB_S3_ENDPOINT", "https://storage.googleapis.com", "", "https://storage.googleapis.com"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := Settings{DuckDBS3Endpoint: c.duckDB, S3Endpoint: c.s3}
			if got := s.LakeS3Endpoint(); got != c.expected {
				t.Fatalf("LakeS3Endpoint() = %q, want %q", got, c.expected)
			}
		})
	}
}
