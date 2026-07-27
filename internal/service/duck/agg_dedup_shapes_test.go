// agg_dedup_shapes_test.go compares the candidate FROM shapes for the range
// aggregation path (GetAggregatedSignals) on one synthetic vehicle-week:
//
//	A "join-only"   — the pre-PR shape: dedup window over every name the vehicle
//	                  reports, requested names applied by the outer JOIN only.
//	B "name-pushed" — the shipped shape: requested names also restrict the dedup
//	                  subquery, so they reach the scan.
//	C "hash-dedup"  — B, with the QUALIFY ROW_NUMBER window replaced by a
//	                  GROUP BY (subject, name, timestamp) + arg_min(…, cloud_event_id).
//
// TestAggDedupShapes_Equivalent runs always and is the safety net: all three
// shapes must return byte-identical rows over data containing duplicate
// (subject,name,timestamp) rows, so "faster" can never mean "different".
// TestAggDedupShapes_Timing is the measurement and is opt-in (DQ_PERF_BENCH=1) —
// it seeds ~1M rows, which is far too slow for the normal suite.
//
// VERDICT (M4 Pro, 1.2M rows = 3 vehicles x 7 days x 20 signals at 30s, local
// parquet + file catalog; best of 5, stable across repeat runs):
//
//	                             dashboard (3 names)   report (20 names)
//	A join-only (pre-PR)                 74ms                 87ms
//	B name-pushed (shipped)              31ms                 68ms
//	C hash-dedup + name-pushed           24ms                 85ms
//
// B ships. C is deliberately NOT shipped and is kept here as the evidence: it
// wins the narrow query by ~25% but gives it all back on the wide one, because
// one (subject,name,timestamp) group per row means the hash table gains a group
// per input row and six arg_min states with it, while the window's sort does not
// grow the same way. A shape that is faster only when few signals are requested
// is the wrong trade for an API serving both dashboards and reports. Re-run the
// timing before proposing it again.
//
// Caveat on the numbers: local parquet with a file catalog, so they measure scan
// + dedup work only. They say nothing about DuckLake planning against the
// Postgres catalog, which is the dominant cost in production for the latest
// path (see the dq_lake_latest_query_seconds work) and is unaffected by any of
// these shapes.
package duck

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/require"
)

// perfSubject3 is a third vehicle (bucket_test.go defines only two), so the
// timing fixture holds more than one bucket's worth of data and partition
// pruning is doing real work.
const perfSubject3 = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:3"

// perfNames is a plausible per-vehicle signal catalogue: the dashboard asks for
// one or two of these, the dedup window in shape A processes all of them.
var perfNames = []string{
	"speed", "powertrainTransmissionTravelledDistance", "powertrainFuelSystemRelativeLevel",
	"powertrainType", "obdEngineLoad", "obdIntakeTemp", "obdRunTime", "obdBarometricPressure",
	"exteriorAirTemperature", "lowVoltageBatteryCurrentVoltage", "powertrainCombustionEngineECT",
	"powertrainCombustionEngineMAF", "powertrainCombustionEngineSpeed", "powertrainCombustionEngineTPS",
	"chassisAxleRow1WheelLeftTirePressure", "chassisAxleRow1WheelRightTirePressure",
	"chassisAxleRow2WheelLeftTirePressure", "chassisAxleRow2WheelRightTirePressure",
	"currentLocationAltitude", "currentLocationCoordinates",
}

// seedPerfLake lays lake.signals out the way the materializer does
// (setupStatements: partitioned by bucket + y/m/d, sorted by subject/timestamp)
// and fills it with `days` days of every perfNames signal at `every` cadence for
// each of `subjects`. dupEvery rows also get a second copy under a HIGHER
// cloud_event_id and a wrong value: dedup must drop it, so any shape that skips
// dedup fails TestAggDedupShapes_Equivalent instead of looking fast.
func seedPerfLake(t *testing.T, svc *Service, subjects []string, from time.Time, days int, every time.Duration, dupEvery int) {
	t.Helper()
	ctx := context.Background()
	for _, stmt := range []string{
		`ALTER TABLE lake.signals SET PARTITIONED BY (subject_bucket, year("timestamp"), month("timestamp"), day("timestamp"))`,
		`ALTER TABLE lake.signals SET SORTED BY (subject, "timestamp")`,
	} {
		_, err := svc.db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	nameRows := make([]string, len(perfNames))
	for i, n := range perfNames {
		nameRows[i] = "(" + sqlString(n) + ")"
	}
	to := from.AddDate(0, 0, days)

	for si, subject := range subjects {
		// value_number/loc_* are deterministic functions of the row so the
		// equivalence assertions compare real values, not noise.
		gen := fmt.Sprintf(`
			SELECT %s AS subject, %d AS subject_bucket, n.name, g.ts AS timestamp,
				'src-1' AS source, 'prod-1' AS producer,
				'ce-%d-' || n.name || '-' || CAST(epoch_us(g.ts) AS VARCHAR) AS cloud_event_id,
				CAST((epoch_us(g.ts) / 1000000) %% 97 AS DOUBLE) AS value_number,
				'' AS value_string,
				CAST(40 + ((epoch_us(g.ts) / 1000000) %% 13) AS DOUBLE) AS loc_lat,
				CAST(-70 - ((epoch_us(g.ts) / 1000000) %% 11) AS DOUBLE) AS loc_lon,
				CAST(0.5 AS DOUBLE) AS loc_hdop, CAST(90 AS DOUBLE) AS loc_heading
			FROM generate_series(%s, %s, INTERVAL %d SECOND) g(ts)
			CROSS JOIN (VALUES %s) n(name)`,
			sqlString(subject), HashBucket(subject), si,
			tsMicroLiteral(from), tsMicroLiteral(to.Add(-every)), int(every.Seconds()),
			strings.Join(nameRows, ", "))

		_, err := svc.db.ExecContext(ctx, `INSERT INTO lake.signals (subject, subject_bucket, name, timestamp,
			source, producer, cloud_event_id, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading) `+gen)
		require.NoError(t, err)

		// At-least-once duplicates: same (subject,name,timestamp), higher
		// cloud_event_id ('zz-' sorts after 'ce-'), poisoned value. The dedup
		// keeps the lower id, so these must never reach an aggregate.
		_, err = svc.db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO lake.signals (subject, subject_bucket, name, timestamp,
			source, producer, cloud_event_id, value_number, value_string, loc_lat, loc_lon, loc_hdop, loc_heading)
			SELECT subject, subject_bucket, name, timestamp, source, producer,
				'zz-dup-' || cloud_event_id, CAST(-999 AS DOUBLE), value_string,
				CAST(-999 AS DOUBLE), CAST(-999 AS DOUBLE), loc_hdop, loc_heading
			FROM lake.signals WHERE subject = %s AND (epoch_us(timestamp) / 1000000) %% %d = 0`,
			sqlString(subject), dupEvery))
		require.NoError(t, err)
	}
}

// dedupSourceJoinOnly is shape A: the pre-PR dedup source, with no name
// restriction — the window sorts every name the vehicle reports.
func dedupSourceJoinOnly(subject string) string {
	return `(SELECT * FROM lake.signals WHERE ` + subjectBucketPredicate("", subject) + ` ` + signalDedupQualify + `)`
}

// dedupSourceHashAgg is shape C — measured and rejected, see the file header;
// it lives here so the rejection stays reproducible. Same dedup semantics as signalDedupQualify
// (lowest cloud_event_id per (subject,name,timestamp) wins) expressed as a hash
// aggregate instead of a window, so DuckDB never has to sort the input. Ties on
// cloud_event_id within a key would let each column come from a different row,
// where ROW_NUMBER picks one whole row — that requires the SAME event id stored
// twice for one (name,timestamp), which the materializer's anti-join
// (subject_bucket, cloud_event_id, name, timestamp) already excludes.
func dedupSourceHashAgg(subject string, names []string) string {
	pred := subjectBucketPredicate("", subject)
	if nameCond := signalNameInCond("name", names); nameCond != "" {
		pred += " AND " + nameCond
	}
	return `(SELECT subject, name, timestamp,
			arg_min(value_number, cloud_event_id) AS value_number,
			arg_min(value_string, cloud_event_id) AS value_string,
			arg_min(loc_lat, cloud_event_id) AS loc_lat,
			arg_min(loc_lon, cloud_event_id) AS loc_lon,
			arg_min(loc_hdop, cloud_event_id) AS loc_hdop,
			arg_min(loc_heading, cloud_event_id) AS loc_heading
		FROM lake.signals WHERE ` + pred + `
		GROUP BY subject, name, timestamp)`
}

// aggStmtOverSource renders GetAggregatedSignals' statement verbatim except for
// the dedup source, so the shapes differ ONLY in their FROM. Keep in lockstep
// with GetAggregatedSignals (aggregations.go).
func aggStmtOverSource(src, subject string, aggArgs *model.AggregatedSignalArgs) (string, []any) {
	conds := []string{
		"s.subject = ?",
		"s.timestamp >= " + tsMicroLiteral(aggArgs.FromTS),
		"s.timestamp < " + tsMicroLiteral(aggArgs.ToTS),
	}
	var args []any
	args = append(args, subject)
	perSignal, perSignalArgs := perSignalFilterSQL(aggArgs)
	conds = append(conds, perSignal)
	args = append(args, perSignalArgs...)

	inner := "SELECT " + signalSrcColumns +
		" FROM " + src + " AS s JOIN " + aggValuesTable(aggArgs.FloatArgs, aggArgs.StringArgs, aggArgs.LocationArgs) +
		" ON s.name = agg_table.name WHERE " + strings.Join(conds, " AND ")

	originUs := aggArgs.FromTS.UnixMicro()
	bucketExpr := fmt.Sprintf("make_timestamp(((epoch_us(timestamp) - %d) // %d) * %d + %d)",
		originUs, aggArgs.Interval, aggArgs.Interval, originUs)

	stmt := "SELECT CAST(signal_type AS UTINYINT) AS out_type, CAST(signal_index AS USMALLINT) AS out_index, " +
		bucketExpr + " AS group_timestamp, " +
		floatCaseSQL(aggArgs.FloatArgs) + ", " +
		stringCaseSQL(aggArgs.StringArgs) + ", " +
		locationCaseSQL("loc_lat", "agg_lat", aggArgs.LocationArgs) + ", " +
		locationCaseSQL("loc_lon", "agg_lon", aggArgs.LocationArgs) + ", " +
		locationCaseSQL("loc_hdop", "agg_hdop", aggArgs.LocationArgs) + ", " +
		locationCaseSQL("loc_heading", "agg_heading", aggArgs.LocationArgs) +
		" FROM (" + inner + ")" +
		" GROUP BY group_timestamp, signal_type, signal_index" +
		" ORDER BY group_timestamp ASC, signal_type ASC, signal_index ASC"
	return stmt, args
}

// aggShapeRows runs one shape and renders its rows as comparable strings.
func aggShapeRows(t *testing.T, svc *Service, src, subject string, aggArgs *model.AggregatedSignalArgs) []string {
	t.Helper()
	stmt, args := aggStmtOverSource(src, subject, aggArgs)
	rows, err := svc.db.QueryContext(context.Background(), stmt, args...)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var out []string
	for rows.Next() {
		var stype uint8
		var index uint16
		var ts time.Time
		var num float64
		var str string
		var lat, lon, hdop, heading float64
		require.NoError(t, rows.Scan(&stype, &index, &ts, &num, &str, &lat, &lon, &hdop, &heading))
		out = append(out, fmt.Sprintf("%d/%d/%s: %.6f %q %.6f %.6f %.6f %.6f",
			stype, index, ts.UTC().Format(time.RFC3339Nano), num, str, lat, lon, hdop, heading))
	}
	require.NoError(t, rows.Err())
	return out
}

// dailyAggArgs is the "average speed per day this week" dashboard query, plus a
// location and a string aggregation so every output column is exercised.
func dailyAggArgs(subject string, from time.Time, days int) *model.AggregatedSignalArgs {
	return &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: subject},
		FromTS:     from,
		ToTS:       from.AddDate(0, 0, days),
		Interval:   (24 * time.Hour).Microseconds(),
		FloatArgs: []model.FloatSignalArgs{
			{Name: "speed", Agg: model.FloatAggregationAvg},
			{Name: "speed", Agg: model.FloatAggregationMax},
			{Name: "obdEngineLoad", Agg: model.FloatAggregationAvg},
		},
		StringArgs:   []model.StringSignalArgs{{Name: "powertrainType", Agg: model.StringAggregationTop}},
		LocationArgs: []model.LocationSignalArgs{{Name: "currentLocationCoordinates", Agg: model.LocationAggregationAvg}},
	}
}

// TestAggDedupShapes_Equivalent pins the correctness precondition for the
// pushdown: restricting the dedup subquery to the requested names (B), and
// re-expressing the dedup as a hash aggregate (C), must both return exactly what
// the pre-PR join-only shape (A) returns — including dropping the poisoned
// duplicate rows.
func TestAggDedupShapes_Equivalent(t *testing.T) {
	_, svc, _ := newQueriesHarness(t)
	subject, other := testSubject1, testSubject2
	require.NotEqual(t, HashBucket(subject), HashBucket(other), "fixture needs two buckets")
	from := mkts(t, "2026-06-01T00:00:00Z")
	// 2 days at 1 reading/minute over 20 names, every 7th second duplicated.
	seedPerfLake(t, svc, []string{subject, other}, from, 2, time.Minute, 7)

	aggArgs := dailyAggArgs(subject, from, 2)
	names := aggNames(aggArgs.FloatArgs, aggArgs.StringArgs, aggArgs.LocationArgs)

	want := aggShapeRows(t, svc, dedupSourceJoinOnly(subject), subject, aggArgs)
	require.NotEmpty(t, want, "fixture produced no aggregation rows")
	require.NotContains(t, strings.Join(want, "\n"), "-999",
		"baseline shape let a duplicate row through: the fixture, not the shapes, is wrong")

	require.Equal(t, want, aggShapeRows(t, svc, LakeSignalsDeduped(subject, "", names...), subject, aggArgs),
		"name-pushed dedup source changed results")
	require.Equal(t, want, aggShapeRows(t, svc, dedupSourceHashAgg(subject, names), subject, aggArgs),
		"hash-aggregate dedup source changed results")

	// The shipped path (GetAggregatedSignals) must agree with the shapes above:
	// the reference comparison is only worth something if it covers the SQL
	// production actually issues, not just the hand-rendered copies.
	q := NewLakeQueries(svc)
	got, err := q.GetAggregatedSignals(context.Background(), subject, aggArgs)
	require.NoError(t, err)
	rendered := make([]string, 0, len(got))
	for _, s := range got {
		rendered = append(rendered, fmt.Sprintf("%d/%d/%s: %.6f %q %.6f %.6f %.6f %.6f",
			s.SignalType, s.SignalIndex, s.Timestamp.UTC().Format(time.RFC3339Nano),
			s.ValueNumber, s.ValueString, s.ValueLocation.Latitude, s.ValueLocation.Longitude,
			s.ValueLocation.HDOP, s.ValueLocation.Heading))
	}
	require.Equal(t, want, rendered, "GetAggregatedSignals disagrees with the reference shape")
}

// TestAggDedupShapes_Timing is the measurement behind this PR. Opt-in: it seeds
// ~1M rows (a 20-signal vehicle-week at 30s cadence, x3 vehicles) which takes
// far longer than the unit suite tolerates.
//
//	DQ_PERF_BENCH=1 go test ./internal/service/duck/ -run TestAggDedupShapes_Timing -v -timeout 30m
func TestAggDedupShapes_Timing(t *testing.T) {
	if os.Getenv("DQ_PERF_BENCH") == "" {
		t.Skip("set DQ_PERF_BENCH=1 to run the aggregation shape timing")
	}
	_, svc, _ := newQueriesHarness(t)
	subject := testSubject1
	others := []string{testSubject2, perfSubject3}
	from := mkts(t, "2026-06-01T00:00:00Z")

	seedStart := time.Now()
	seedPerfLake(t, svc, append([]string{subject}, others...), from, 7, 30*time.Second, 601)
	var rowCount int64
	require.NoError(t, svc.db.QueryRowContext(context.Background(),
		"SELECT count(*) FROM lake.signals").Scan(&rowCount))
	t.Logf("seeded %d rows in %s", rowCount, time.Since(seedStart).Round(time.Millisecond))

	// Both halves of the workload: the dashboard asks for a few signals, the
	// report asks for all of them. The pushdown is worth nothing to the report
	// (it already wants every name) — that case is here to prove it costs
	// nothing either.
	queries := []struct {
		label string
		args  *model.AggregatedSignalArgs
	}{
		{"dashboard: 3 names, daily, 7d", dailyAggArgs(subject, from, 7)},
		{"report: all 20 names, hourly, 7d", wideAggArgs(subject, from, 7)},
	}

	const runs = 5
	for _, qc := range queries {
		names := aggNames(qc.args.FloatArgs, qc.args.StringArgs, qc.args.LocationArgs)
		shapes := []struct {
			label string
			src   string
		}{
			{"A join-only (pre-PR)", dedupSourceJoinOnly(subject)},
			{"B name-pushed (this PR)", LakeSignalsDeduped(subject, "", names...)},
			{"C hash-dedup + name-pushed", dedupSourceHashAgg(subject, names)},
		}
		t.Logf("--- %s", qc.label)
		for _, sh := range shapes {
			var best, total time.Duration
			for i := 0; i < runs; i++ {
				start := time.Now()
				rows := aggShapeRows(t, svc, sh.src, subject, qc.args)
				require.NotEmpty(t, rows)
				elapsed := time.Since(start)
				total += elapsed
				if best == 0 || elapsed < best {
					best = elapsed
				}
			}
			t.Logf("    %-28s best %8s   mean %8s (%d runs)",
				sh.label, best.Round(time.Millisecond), (total / runs).Round(time.Millisecond), runs)
		}
	}
}

// wideAggArgs is the report shape: every stored signal, hourly, over the same
// window. perfNames' location signal is requested as a location aggregation and
// the string-typed one as a string aggregation, so the projection matches the
// column types the fixture writes.
func wideAggArgs(subject string, from time.Time, days int) *model.AggregatedSignalArgs {
	args := &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: subject},
		FromTS:     from,
		ToTS:       from.AddDate(0, 0, days),
		Interval:   time.Hour.Microseconds(),
	}
	for _, n := range perfNames {
		switch n {
		case "currentLocationCoordinates":
			args.LocationArgs = append(args.LocationArgs, model.LocationSignalArgs{Name: n, Agg: model.LocationAggregationAvg})
		case "powertrainType":
			args.StringArgs = append(args.StringArgs, model.StringSignalArgs{Name: n, Agg: model.StringAggregationTop})
		default:
			args.FloatArgs = append(args.FloatArgs, model.FloatSignalArgs{Name: n, Agg: model.FloatAggregationAvg})
		}
	}
	return args
}
