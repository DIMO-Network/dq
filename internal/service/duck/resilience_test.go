package duck

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConfigDefaults_ConnRecycling pins the connection-recycling defaults. The
// DuckLake catalog is reached over a Postgres attach inside each DuckDB
// connection; without a finite lifetime a connection whose attach is poisoned
// by a PG blip is never recycled and stays broken until pod restart (CHD-21).
func TestConfigDefaults_ConnRecycling(t *testing.T) {
	cfg := Config{}.withDefaults()
	assert.Positive(t, cfg.ConnMaxLifetime, "connections must have a finite lifetime so stale catalog attaches recycle")
	assert.Positive(t, cfg.ConnMaxIdleTime, "idle connections must be retired so a poisoned one is not pinned in the pool")
}
