package duck

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSubject1 = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:1"
	testSubject2 = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:2"
)

func date(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestNewPathBuilder(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		want   string
	}{
		{name: "bare bucket name", bucket: "my-bucket", want: "s3://my-bucket/raw"},
		{name: "s3 url", bucket: "s3://my-bucket", want: "s3://my-bucket/raw"},
		{name: "s3 url trailing slash", bucket: "s3://my-bucket/", want: "s3://my-bucket/raw"},
		{name: "file url", bucket: "file:///tmp/data", want: "/tmp/data/raw"},
		{name: "absolute dir", bucket: "/tmp/data", want: "/tmp/data/raw"},
		{name: "relative dir", bucket: "./data", want: "./data/raw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NewPathBuilder(tt.bucket).Join("raw"))
		})
	}
}

func TestRawGlobs(t *testing.T) {
	from := time.Date(2026, time.June, 1, 10, 30, 0, 0, time.UTC)
	to := time.Date(2026, time.June, 3, 1, 0, 0, 0, time.UTC)

	globs := RawGlobs("my-bucket", "raw", []string{"dimo.status", "dimo.event"}, from, to)
	want := []string{
		"s3://my-bucket/raw/type=dimo.status/date=2026-06-01/*.parquet",
		"s3://my-bucket/raw/type=dimo.event/date=2026-06-01/*.parquet",
		"s3://my-bucket/raw/type=dimo.status/date=2026-06-02/*.parquet",
		"s3://my-bucket/raw/type=dimo.event/date=2026-06-02/*.parquet",
		"s3://my-bucket/raw/type=dimo.status/date=2026-06-03/*.parquet",
		"s3://my-bucket/raw/type=dimo.event/date=2026-06-03/*.parquet",
	}
	assert.Equal(t, want, globs)
}

func TestRawGlobsSingleDay(t *testing.T) {
	// Same calendar day with different times still yields the day's glob.
	from := time.Date(2026, time.June, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, time.June, 1, 23, 59, 59, 0, time.UTC)

	globs := RawGlobs("/data", "raw", []string{"dimo.status"}, from, to)
	assert.Equal(t, []string{"/data/raw/type=dimo.status/date=2026-06-01/*.parquet"}, globs)
}

func TestRawGlobsNonUTCInput(t *testing.T) {
	// 2026-06-02 01:00 +03:00 is 2026-06-01 22:00 UTC; days resolve in UTC.
	loc := time.FixedZone("UTC+3", 3*60*60)
	from := time.Date(2026, time.June, 2, 1, 0, 0, 0, loc)
	globs := RawGlobs("b", "raw", []string{"t"}, from, from)
	assert.Equal(t, []string{"s3://b/raw/type=t/date=2026-06-01/*.parquet"}, globs)
}

func TestRawGlobsEmptyRange(t *testing.T) {
	assert.Empty(t, RawGlobs("b", "raw", []string{"t"}, date(2026, time.June, 2), date(2026, time.June, 1)))
	assert.Empty(t, RawGlobs("b", "raw", nil, date(2026, time.June, 1), date(2026, time.June, 2)))
}

func TestDecodedSignalGlobs(t *testing.T) {
	globs := DecodedSignalGlobs("my-bucket", "decoded/v1", date(2026, time.May, 31), date(2026, time.June, 1))
	want := []string{
		"s3://my-bucket/decoded/v1/signals/date=2026-05-31/*.parquet",
		"s3://my-bucket/decoded/v1/signals/date=2026-06-01/*.parquet",
	}
	assert.Equal(t, want, globs)
}

func TestHashBucket(t *testing.T) {
	// Hardcoded FNV-1a 32-bit reference values. These pin the on-disk
	// bucket layout contract shared with the materializer — do not change.
	assert.Equal(t, 197, HashBucket(""))
	assert.Equal(t, 219, HashBucket(testSubject1))
	assert.Equal(t, 110, HashBucket(testSubject2))

	// Deterministic and in range for arbitrary subjects.
	for _, subject := range []string{testSubject1, testSubject2, "anything", "did:erc721:1:0x0:99999"} {
		got := HashBucket(subject)
		assert.Equal(t, got, HashBucket(subject))
		assert.GreaterOrEqual(t, got, 0)
		assert.Less(t, got, NumLatestBuckets)
	}
}

func TestLatestBucketPath(t *testing.T) {
	got := LatestBucketPath("my-bucket", "decoded/v1", testSubject1)
	assert.Equal(t, "s3://my-bucket/decoded/v1/latest/bucket=219/latest.parquet", got)

	local := LatestBucketPath("/data", "decoded/v1", testSubject2)
	assert.Equal(t, "/data/decoded/v1/latest/bucket=110/latest.parquet", local)
}

func TestReadParquetSQL(t *testing.T) {
	got := ReadParquetSQL([]string{"s3://b/raw/type=t/date=2026-06-01/*.parquet", "/local/o'brien.parquet"})
	want := "read_parquet(['s3://b/raw/type=t/date=2026-06-01/*.parquet', '/local/o''brien.parquet'], hive_partitioning=true, union_by_name=true)"
	require.Equal(t, want, got)
}
