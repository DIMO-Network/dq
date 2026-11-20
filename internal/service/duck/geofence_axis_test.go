package duck

import (
	"database/sql"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHaversineWithinSQL_AxisOrder pins inCircle to real great-circle distance in
// BOTH directions. The equator-adjacent points in TestGetAggregatedSignalsEdgeCases
// can't tell (lon, lat) from (lat, lon) apart; at Detroit's latitude a swapped axis
// order shrinks a 1 km north offset to ~121 m and stretches 1 km east to ~1,353 m,
// which is what dq served before geometry_always_xy was set in bootstrapQueries.
// It runs the production bootstrap, so dropping that SET fails this test.
func TestHaversineWithinSQL_AxisOrder(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck
	db.SetMaxOpenConns(1)
	for _, q := range bootstrapQueries(Config{LoadSpatial: true}) {
		_, err := db.Exec(q)
		require.NoError(t, err, q)
	}

	const clat, clon = 42.3314, -83.0458 // Detroit
	const metresPerDegLat = 111195.0
	cases := []struct {
		name     string
		lat, lon float64
	}{
		{"1km north", clat + 1000/metresPerDegLat, clon},
		{"1km east", clat, clon + 1000/(metresPerDegLat*math.Cos(clat*math.Pi/180))},
	}
	for _, tc := range cases {
		for _, r := range []struct {
			km   float64
			want bool
		}{{1.05, true}, {0.95, false}} {
			var got bool
			q := fmt.Sprintf("SELECT %s FROM (SELECT %s AS lat, %s AS lon)",
				haversineWithinSQL("lat", "lon", clat, clon, r.km), floatLit(tc.lat), floatLit(tc.lon))
			require.NoError(t, db.QueryRow(q).Scan(&got), q)
			require.Equalf(t, r.want, got, "%s within %.2f km", tc.name, r.km)
		}
	}
}
