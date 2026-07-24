package duck

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBootstrap_NewConnAfterSpill pins the 2026-07-23 materializer grind:
// temp_directory is DATABASE-scoped and DuckDB refuses to re-SET it (even to
// the same value) once temp has been used, so a connection minted after a
// spill — a poison recycle, ConnMaxLifetime rotation, or pool growth — failed
// bootstrap, and with it every later connection until the process restarted.
// Force a real spill under a tiny memory limit, then force the pool to mint
// fresh connections, which must bootstrap cleanly.
func TestBootstrap_NewConnAfterSpill(t *testing.T) {
	svc, err := NewService(Config{
		TempDirectory:     t.TempDir(),
		DuckDBMemoryLimit: "64MiB",
		DuckDBThreads:     1,
		MaxConns:          2,
	})
	require.NoError(t, err)
	defer svc.Close() //nolint:errcheck
	db := svc.DB()

	// A full-materialization sort well past the 64MiB budget. ORDER BY inside
	// CTAS defeats top-N shortcuts, so the sort spills to temp_directory and
	// marks it "used" for the life of the database.
	_, err = db.Exec(`CREATE TABLE spill AS SELECT r AS i, hash(r) AS h FROM range(30000000) t(r) ORDER BY h`)
	require.NoError(t, err)

	// Retire every pooled connection; the next queries must mint fresh ones,
	// re-running the bootstrap against the now-spilled database.
	db.SetMaxIdleConns(0)
	db.SetMaxIdleConns(2)

	for range 3 {
		var n int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM spill").Scan(&n),
			"a connection minted after a spill must bootstrap cleanly")
		require.Equal(t, 30000000, n)
	}
}
