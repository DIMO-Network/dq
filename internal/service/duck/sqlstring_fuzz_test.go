package duck

import (
	"strings"
	"testing"
)

// FuzzSQLString guards the no-breakout invariant of the SQL string escaper: for ANY
// input, the result must be a single-quoted literal in which every interior quote is
// part of a ” pair — so no input can terminate the literal early and inject SQL. The
// escaping was proven complete against the real DuckDB in the security review; this is a
// fast regression guard over arbitrary input (the query builders inline sqlString for
// signal names, sources, etc.).
func FuzzSQLString(f *testing.F) {
	for _, s := range []string{"", "'", "''", "'''", "a'b", "'; DROP TABLE x; --", "\x00", "🚗", `\'`, `\\'`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, v string) {
		out := sqlString(v)
		if len(out) < 2 || out[0] != '\'' || out[len(out)-1] != '\'' {
			t.Fatalf("sqlString(%q) = %q: not wrapped in single quotes", v, out)
		}
		// Collapsing every '' pair in the body must leave no lone quote — a lone quote
		// would close the literal early and let the remainder execute as SQL.
		body := out[1 : len(out)-1]
		if strings.Contains(strings.ReplaceAll(body, "''", ""), "'") {
			t.Fatalf("sqlString(%q) = %q: a lone quote escapes the literal", v, out)
		}
	})
}
