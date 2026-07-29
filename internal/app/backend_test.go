package app

import (
	"testing"
	"time"

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

// TestProfileReads_QueryPathOnly pins where DUCKDB_PROFILE_READS is applied. The
// materializer builds its decode instance from duckConfigFromSettings and then
// switches off the query-path features that do not apply to it (LoadSpatial,
// PoisonRecovery). Read-profiling must never be on in that base: it adds
// per-connection PRAGMAs whose cost lands on every decode-loop query, and the
// loop never reads a profiling tree back — it does not route through
// Service.queryLake. Applying the flag in queryDuckConfig instead fails safe: a
// future duck.Service that forgets to opt in loses a diagnostic rather than
// silently paying for one.
func TestProfileReads_QueryPathOnly(t *testing.T) {
	t.Parallel()
	on := config.Settings{DuckDBProfileReads: true}

	assert.False(t, duckConfigFromSettings(&on).ProfileReads,
		"the materializer's base config must never enable read profiling")
	assert.True(t, queryDuckConfig(&on).ProfileReads,
		"the query backend must honor DUCKDB_PROFILE_READS")
	assert.False(t, queryDuckConfig(&config.Settings{}).ProfileReads,
		"profiling must stay off by default")
}

// SlowReadThreshold stays on the BASE config, unlike ProfileReads: it sets no
// PRAGMA and is inert for a service that issues no queryLake reads, so there is
// nothing to fail safe against.
func TestSlowReadThreshold_ParsedOnBaseConfig(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 3*time.Second,
		duckConfigFromSettings(&config.Settings{DuckDBSlowReadThreshold: "3s"}).SlowReadThreshold)

	// An unparseable value must neither fail startup nor silence the log: zero
	// means "use duck.DefaultSlowReadThreshold".
	assert.Zero(t,
		duckConfigFromSettings(&config.Settings{DuckDBSlowReadThreshold: "nope"}).SlowReadThreshold,
		"a typo falls back to the default rather than disabling the slow-read log")
}
