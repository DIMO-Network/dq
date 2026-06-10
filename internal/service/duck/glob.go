package duck

import (
	"fmt"
	"hash/fnv"
	"strings"
	"time"
)

// NumLatestBuckets is the number of hash buckets for latest/summary parquet files.
const NumLatestBuckets = 256

const dateFormat = "2006-01-02"

// PathBuilder builds parquet object paths rooted at either an S3 bucket or a
// local directory. It exists so the same glob builders work against
// s3://bucket layouts in production and plain directories in tests.
type PathBuilder struct {
	root string
}

// NewPathBuilder normalizes a bucket reference into a path root:
//   - "my-bucket" or "s3://my-bucket" -> "s3://my-bucket"
//   - "file:///tmp/data"             -> "/tmp/data"
//   - "/tmp/data" or "./data"        -> used as-is (local directory)
func NewPathBuilder(bucket string) PathBuilder {
	root := strings.TrimSuffix(bucket, "/")
	switch {
	case strings.HasPrefix(root, "s3://"):
		// already an S3 URL
	case strings.HasPrefix(root, "file://"):
		root = strings.TrimPrefix(root, "file://")
	case strings.HasPrefix(root, "/"), strings.HasPrefix(root, "."):
		// local directory, used as-is
	default:
		root = "s3://" + root
	}
	return PathBuilder{root: root}
}

// Join joins path parts onto the root with "/" separators.
func (p PathBuilder) Join(parts ...string) string {
	return p.root + "/" + strings.Join(parts, "/")
}

// RawGlobs returns explicit per-day, per-type parquet globs for raw
// cloudevents: <root>/<rawPrefix>/type=<T>/date=<YYYY-MM-DD>/*.parquet.
// The day range [from, to] is inclusive in UTC. Listing each partition
// explicitly prunes S3 listing to only the requested days and types.
func RawGlobs(bucket, rawPrefix string, types []string, from, to time.Time) []string {
	pb := NewPathBuilder(bucket)
	days := daysBetween(from, to)
	globs := make([]string, 0, len(days)*len(types))
	for _, day := range days {
		for _, ceType := range types {
			globs = append(globs, pb.Join(rawPrefix, "type="+ceType, "date="+day, "*.parquet"))
		}
	}
	return globs
}

// DecodedSignalGlobs returns explicit per-day parquet globs for decoded
// signals: <root>/<decodedPrefix>/signals/date=<YYYY-MM-DD>/*.parquet.
// The day range [from, to] is inclusive in UTC.
func DecodedSignalGlobs(bucket, decodedPrefix string, from, to time.Time) []string {
	pb := NewPathBuilder(bucket)
	days := daysBetween(from, to)
	globs := make([]string, 0, len(days))
	for _, day := range days {
		globs = append(globs, pb.Join(decodedPrefix, "signals", "date="+day, "*.parquet"))
	}
	return globs
}

// LatestBucketPath returns the latest/summary parquet path for a subject:
// <root>/<decodedPrefix>/latest/bucket=<HashBucket(subjectDID)>/latest.parquet.
func LatestBucketPath(bucket, decodedPrefix, subjectDID string) string {
	pb := NewPathBuilder(bucket)
	return pb.Join(decodedPrefix, "latest", fmt.Sprintf("bucket=%03d", HashBucket(subjectDID)), "latest.parquet")
}

// HashBucket maps a subject DID to its latest-bucket number in
// [0, NumLatestBuckets) using FNV-1a 32-bit. The materializer MUST use this
// exact function when writing latest/summary files so reads and writes agree.
func HashBucket(subject string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(subject))
	return int(h.Sum32() % NumLatestBuckets)
}

// ReadParquetSQL renders a read_parquet table function over the given globs:
// read_parquet(['g1', 'g2'], hive_partitioning=true, union_by_name=true).
// Hive partition columns (type, date, bucket) become queryable columns and
// union_by_name tolerates schema evolution across files.
func ReadParquetSQL(globs []string) string {
	quoted := make([]string, len(globs))
	for i, g := range globs {
		quoted[i] = sqlString(g)
	}
	return fmt.Sprintf("read_parquet([%s], hive_partitioning=true, union_by_name=true)", strings.Join(quoted, ", "))
}

// daysBetween returns each UTC calendar day in [from, to] inclusive,
// formatted as YYYY-MM-DD. Returns nil when to predates from.
func daysBetween(from, to time.Time) []string {
	start := truncateToDay(from)
	end := truncateToDay(to)
	if end.Before(start) {
		return nil
	}
	var days []string
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		days = append(days, d.Format(dateFormat))
	}
	return days
}

func truncateToDay(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}
