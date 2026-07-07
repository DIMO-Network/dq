package app

import (
	"testing"

	"github.com/DIMO-Network/dq/internal/config"
	"github.com/stretchr/testify/assert"
)

// TestQueryDuckConfig_MaterializerMemoryCap proves finding #7's fix: on a
// materializer pod the always-built (but idle) query DuckDB instance is capped by
// DUCKDB_QUERY_MEMORY_LIMIT so it plus the decode instance sum under the pod limit,
// while a query pod is unaffected.
func TestQueryDuckConfig_MaterializerMemoryCap(t *testing.T) {
	t.Parallel()
	base := config.Settings{
		DuckLakeCatalogDSN: "postgres://x", // non-empty so this models a real config
		DuckDBMemoryLimit:  "6GiB",
	}

	t.Run("query pod ignores the override", func(t *testing.T) {
		s := base
		s.MaterializerEnabled = false
		s.DuckDBQueryMemoryLimit = "1GiB"
		assert.Equal(t, "6GiB", queryDuckConfig(&s).DuckDBMemoryLimit,
			"a query pod keeps the full DUCKDB_MEMORY_LIMIT")
	})

	t.Run("materializer pod caps the idle query instance", func(t *testing.T) {
		s := base
		s.MaterializerEnabled = true
		s.DuckDBQueryMemoryLimit = "1GiB"
		assert.Equal(t, "1GiB", queryDuckConfig(&s).DuckDBMemoryLimit,
			"the idle query instance on a materializer pod must use the lower cap")
		// The decode instance keeps the full budget (it uses duckConfigFromSettings
		// directly, not queryDuckConfig).
		assert.Equal(t, "6GiB", duckConfigFromSettings(&s).DuckDBMemoryLimit,
			"the decode instance keeps DUCKDB_MEMORY_LIMIT")
	})

	t.Run("materializer pod without an override keeps the full budget", func(t *testing.T) {
		s := base
		s.MaterializerEnabled = true
		s.DuckDBQueryMemoryLimit = ""
		assert.Equal(t, "6GiB", queryDuckConfig(&s).DuckDBMemoryLimit,
			"no override set → unchanged (opt-in)")
	})
}
