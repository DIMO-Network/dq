package duck

import (
	"hash/fnv"
	"strings"
	"testing"
)

// TestHashBucket_MatchesFNV pins the inlined FNV-1a to be byte-identical to the stdlib
// hash/fnv it replaced. This is a correctness invariant, not a style check: the
// materializer stamps subject_bucket with HashBucket on write and every read predicate
// recomputes it, so any divergence would silently route reads to the wrong partition
// and drop rows.
func TestHashBucket_MatchesFNV(t *testing.T) {
	inputs := []string{
		"",
		"a",
		"did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
		"did:erc721:1:0x0000000000000000000000000000000000000000:0",
		"🚗 unicode subject ☼",
		strings.Repeat("x", 1024),
		"\x00\xff\x80 binary-ish",
	}
	for _, s := range inputs {
		h := fnv.New32a()
		_, _ = h.Write([]byte(s))
		want := int(h.Sum32() % NumLatestBuckets)
		if got := HashBucket(s); got != want {
			t.Errorf("HashBucket(%q) = %d, fnv.New32a = %d", s, got, want)
		}
	}
}
