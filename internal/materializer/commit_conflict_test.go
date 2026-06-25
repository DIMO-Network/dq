package materializer

import (
	"errors"
	"fmt"
	"testing"
)

// TestIsCommitConflict pins the conflict classifier that decides retry-vs-fatal on the
// commit path. It matches DuckLake's specific wording on purpose: a regression that
// broadened it would swallow unrelated errors as retryable (infinite retry); one that
// narrowed/broke it would treat a real cursor race as fatal and trip the failure
// backstop, wedging the loop. (Guards our matcher logic; the live wording is exercised
// by the concurrent-materializer integration test.)
func TestIsCommitConflict(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"transaction conflict", errors.New("Transaction conflict - concurrent write"), true},
		{"failed to commit", errors.New("Failed to commit DuckLake transaction: snapshot moved"), true},
		{"wrapped conflict", fmt.Errorf("commit batch: %w", errors.New("Transaction conflict - x")), true},
		{"unrelated 'conflict' word", errors.New("merge conflict in patch"), false},
		{"generic db error", errors.New("connection refused"), false},
		{"empty", errors.New(""), false},
	}
	for _, c := range cases {
		if got := isCommitConflict(c.err); got != c.want {
			t.Errorf("%s: isCommitConflict(%q) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}
