package duck

import (
	"fmt"
)

// NumLatestBuckets is the number of hash buckets the decoded tables are
// partitioned across (subject_bucket).
const NumLatestBuckets = 256

// HashBucket maps a subject DID to its subject_bucket number in
// [0, NumLatestBuckets) using FNV-1a 32-bit. The materializer MUST use this
// exact function when stamping subject_bucket on decoded rows so reads and
// writes agree. Inlined rather than hash/fnv to avoid a hasher allocation + a
// []byte(subject) copy on the per-row decode path and every read predicate;
// the output is identical to fnv.New32a (TestHashBucket_MatchesFNV pins it).
func HashBucket(subject string) int {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(subject); i++ {
		h = (h ^ uint32(subject[i])) * prime32
	}
	return int(h % NumLatestBuckets)
}

// subjectBucketPredicate returns the inlined partition-pruning predicate for a
// subject: "<prefix>subject_bucket = N" where N = HashBucket(subject). The
// decoded lake tables are PARTITIONED BY (subject_bucket, year/month/day(timestamp))
// (CHD-1), so pairing this with the subject filter lets DuckLake skip every
// partition but the subject's. The value is a small int stamped at decode time
// by the same HashBucket, so it is inlined (like the timestamp literals) rather
// than bound — no injection risk.
func subjectBucketPredicate(prefix, subject string) string {
	return fmt.Sprintf("%ssubject_bucket = %d", prefix, HashBucket(subject))
}
