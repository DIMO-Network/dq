package duck

import (
	"fmt"
	"hash/fnv"
)

// NumLatestBuckets is the number of hash buckets the decoded tables are
// partitioned across (subject_bucket).
const NumLatestBuckets = 256

// HashBucket maps a subject DID to its subject_bucket number in
// [0, NumLatestBuckets) using FNV-1a 32-bit. The materializer MUST use this
// exact function when stamping subject_bucket on decoded rows so reads and
// writes agree.
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
// than bound — no injection risk.
func subjectBucketPredicate(prefix, subject string) string {
	return fmt.Sprintf("%ssubject_bucket = %d", prefix, HashBucket(subject))
}
