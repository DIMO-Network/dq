package duck

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRedactQuery proves credential-bearing bootstrap statements are masked
// before they reach error messages or logs: CREATE SECRET inlines S3 keys and
// ATTACH carries the Postgres DSN/password, both of which leaked verbatim
// (CHD-31).
func TestRedactQuery(t *testing.T) {
	secret := "CREATE OR REPLACE SECRET dq_s3 (TYPE s3, KEY_ID 'AKIAEXAMPLE', SECRET 'topsecret')"
	got := redactQuery(secret)
	assert.NotContains(t, got, "topsecret")
	assert.NotContains(t, got, "AKIAEXAMPLE")

	attach := "ATTACH 'ducklake:postgres:host=db dbname=x password=hunter2' AS lake"
	assert.NotContains(t, redactQuery(attach), "hunter2")

	// Non-credential statements are passed through unchanged for debuggability.
	assert.Equal(t, "SET threads = 4", redactQuery("SET threads = 4"))
}

// TestConfigDefaults_ConnRecycling pins the connection-recycling defaults. The
// DuckLake catalog is reached over a Postgres attach inside each DuckDB
// connection; without a finite lifetime a connection whose attach is poisoned
// by a PG blip is never recycled and stays broken until pod restart (CHD-21).
func TestConfigDefaults_ConnRecycling(t *testing.T) {
	cfg := Config{}.withDefaults()
	assert.Positive(t, cfg.ConnMaxLifetime, "connections must have a finite lifetime so stale catalog attaches recycle")
	assert.Positive(t, cfg.ConnMaxIdleTime, "idle connections must be retired so a poisoned one is not pinned in the pool")
}
