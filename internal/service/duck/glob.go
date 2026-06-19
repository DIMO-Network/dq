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

// LatestBucketPaths returns the latest parquet patterns for a subject —
// the single-replica path plus the sharded-materializer namespace
// (latest/shard=*/bucket=NNN). Aggregations (max/arg_max) merge shard
// files natively, so both layouts and mid-migration mixes read correctly.
func LatestBucketPaths(bucket, decodedPrefix, subjectDID string) []string {
	pb := NewPathBuilder(bucket)
	b := fmt.Sprintf("bucket=%03d", HashBucket(subjectDID))
	return []string{
		pb.Join(decodedPrefix, "latest", b, "latest.parquet"),
		pb.Join(decodedPrefix, "latest", "shard=*", b, "latest.parquet"),
	}
}

// HashBucket maps a subject DID to its latest-bucket number in
// [0, NumLatestBuckets) using FNV-1a 32-bit. The materializer MUST use this
// exact function when writing latest/summary files so reads and writes agree.
func HashBucket(subject string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(subject))
	return int(h.Sum32() % NumLatestBuckets)
}

// subjectBucketPredicate returns the inlined partition-pruning predicate for a
// subject: "<prefix>subject_bucket = N" where N = HashBucket(subject). The
// decoded lake tables are PARTITIONED BY (subject_bucket, day(timestamp))
// (CHD-1), so pairing this with the subject filter lets DuckLake skip every
// partition but the subject's. The value is a small int stamped at decode time
// by the same HashBucket, so it is inlined (like the timestamp literals) rather
// than bound — no injection risk. Lake mode only; bucket-mode decoded files may
// predate the column.
func subjectBucketPredicate(prefix, subject string) string {
	return fmt.Sprintf("%ssubject_bucket = %d", prefix, HashBucket(subject))
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
