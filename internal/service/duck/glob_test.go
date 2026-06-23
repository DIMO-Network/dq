package duck

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	testSubject1 = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:1"
	testSubject2 = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:2"
)

func date(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestHashBucket(t *testing.T) {
	// Hardcoded FNV-1a 32-bit reference values. These pin the on-disk
	// subject_bucket layout contract shared with the materializer — do not change.
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

// TestSubjectBucketPredicate pins the inlined partition-pruning predicate the
// lake queries pair with the subject filter: "<prefix>subject_bucket = N" where
// N = HashBucket(subject). The decoded tables are PARTITIONED BY subject_bucket
// (CHD-1), so a drift here would silently stop pruning.
func TestSubjectBucketPredicate(t *testing.T) {
	assert.Equal(t, "subject_bucket = 219", subjectBucketPredicate("", testSubject1))
	assert.Equal(t, "s.subject_bucket = 110", subjectBucketPredicate("s.", testSubject2))
}
