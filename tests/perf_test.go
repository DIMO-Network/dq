// perf_test.go measures the DuckDB query path against the plan's perf gate:
// one vehicle / 7-day aggregation over a synthetic month of fleet data,
// target sub-second warm. Local NVMe stands in for S3, so these numbers are
// engine cost without network; S3 adds GET latency on cold reads.
//
// Run: go test ./tests/ -run TestQueryPerformance -v -perf
package tests

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var runPerf = flag.Bool("perf", false, "run the query performance gate")

func TestQueryPerformance(t *testing.T) {
	if !*runPerf {
		t.Skip("pass -perf to run the performance gate")
	}
	ctx := context.Background()
	root := t.TempDir()

	svc, err := duck.NewService(duck.Config{S3Enabled: false})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	// Synthetic fleet: 200 vehicles x 30 days x 1 signal/minute x 3 names
	// ≈ 26M rows, laid out exactly like the materializer writes them:
	// per-day hive partitions sorted by (subject, name, timestamp).
	const (
		vehicles  = 200
		days      = 30
		perDayPer = 1440 // one per minute
	)
	start := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)

	genStart := time.Now()
	db := svc.DB()
	for d := range days {
		day := start.AddDate(0, 0, d)
		dir := fmt.Sprintf("%s/decoded/v1/signals/date=%s", root, day.Format("2006-01-02"))
		require.NoError(t, mkdirAll(dir))
		stmt := fmt.Sprintf(`
			COPY (
				SELECT
					'did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:' || CAST(v AS VARCHAR) AS subject,
					name,
					TIMESTAMP '%s' + INTERVAL (m) MINUTE AS timestamp,
					'0xConn' AS source,
					'producer' AS producer,
					'ce-' || CAST(v AS VARCHAR) || '-' || CAST(m AS VARCHAR) AS cloud_event_id,
					random() * 120 AS value_number,
					'' AS value_string,
					0.0 AS loc_lat, 0.0 AS loc_lon, 0.0 AS loc_hdop, 0.0 AS loc_heading
				FROM range(%d) t(v), range(%d) r(m),
					(VALUES ('speed'), ('powertrainTransmissionTravelledDistance'), ('powertrainCombustionEngineECT')) names(name)
				ORDER BY subject, name, timestamp
			) TO '%s/data.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)`,
			day.Format("2006-01-02 15:04:05"), vehicles, perDayPer, dir)
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}
	totalRows := vehicles * days * perDayPer * 3
	t.Logf("generated %d rows across %d day partitions in %s", totalRows, days, time.Since(genStart).Round(time.Millisecond))

	queries := duck.NewQueries(svc, root)
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42"

	// The plan's perf gate: one vehicle, 7-day range, hourly buckets.
	aggArgs := &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: subject},
		FromTS:     start.AddDate(0, 0, 20),
		ToTS:       start.AddDate(0, 0, 27),
		Interval:   time.Hour.Microseconds(),
		FloatArgs: []model.FloatSignalArgs{
			{Name: "speed", Agg: model.FloatAggregationAvg},
			{Name: "speed", Agg: model.FloatAggregationMax},
			{Name: "powertrainTransmissionTravelledDistance", Agg: model.FloatAggregationLast},
		},
	}

	cold := time.Now()
	rows, err := queries.GetAggregatedSignals(ctx, subject, aggArgs)
	require.NoError(t, err)
	coldDur := time.Since(cold)
	require.NotEmpty(t, rows)

	const warmRuns = 5
	warm := time.Now()
	for range warmRuns {
		_, err = queries.GetAggregatedSignals(ctx, subject, aggArgs)
		require.NoError(t, err)
	}
	warmDur := time.Since(warm) / warmRuns

	t.Logf("7-day hourly aggregation over %d-row store: cold=%s warm=%s (%d buckets x 3 aggs = %d rows)",
		totalRows, coldDur.Round(time.Millisecond), warmDur.Round(time.Millisecond), 7*24, len(rows))

	// Full-month scan, same vehicle.
	aggArgs.FromTS = start
	aggArgs.ToTS = start.AddDate(0, 0, days)
	month := time.Now()
	rows, err = queries.GetAggregatedSignals(ctx, subject, aggArgs)
	require.NoError(t, err)
	monthDur := time.Since(month)
	t.Logf("30-day hourly aggregation: %s (%d rows)", monthDur.Round(time.Millisecond), len(rows))

	require.Less(t, warmDur, time.Second, "perf gate: warm 7-day aggregation must be sub-second")
	require.Less(t, coldDur, 3*time.Second, "perf gate: cold 7-day aggregation must be under 3s")
}

func mkdirAll(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

// TestMaterializerPerformance measures post-fact decode throughput: raw
// dimo.status bundles (written exactly like din's sink) through one
// materializer pass to decoded parquet + latest/summary buckets.
//
// Run: go test ./tests/ -run TestMaterializerPerformance -v -perf
func TestMaterializerPerformance(t *testing.T) {
	if !*runPerf {
		t.Skip("pass -perf to run the performance gate")
	}
	ctx := context.Background()
	root := t.TempDir()
	store := newFSStore(t, root)

	const (
		bundles         = 20
		eventsPerBundle = 2_000
		signalsPerEvent = 5
		vehicles        = 100
	)
	day := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour)

	genStart := time.Now()
	seq := 0
	for b := range bundles {
		events := make([]cloudevent.StoredEvent, 0, eventsPerBundle)
		for i := range eventsPerBundle {
			n := b*eventsPerBundle + i
			subject := fmt.Sprintf("did:erc721:137:%s:%d", vehicleNFT.Hex(), n%vehicles)
			ts := day.Add(time.Duration(n) * 50 * time.Millisecond)
			signals := make([]map[string]any, signalsPerEvent)
			for s := range signalsPerEvent {
				signals[s] = speedAt(ts.Add(time.Duration(s)*time.Second), float64(n%130))
			}
			events = append(events, deviceStatus(fmt.Sprintf("perf-%d", n), subject, ts, signals...))
		}
		seq++
		writeRawBundle(t, store, day, seq, events...)
	}
	totalEvents := bundles * eventsPerBundle
	totalSignals := totalEvents * signalsPerEvent
	t.Logf("generated %d raw events (%d signals) in %d bundles in %s",
		totalEvents, totalSignals, bundles, time.Since(genStart).Round(time.Millisecond))

	runner := materializer.New(materializer.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
	}, store, zerolog.Nop())

	matStart := time.Now()
	processed, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	for processed != 0 {
		processed, err = runner.RunOnce(ctx)
		require.NoError(t, err)
	}
	matDur := time.Since(matStart)

	eps := float64(totalEvents) / matDur.Seconds()
	sps := float64(totalSignals) / matDur.Seconds()
	t.Logf("materialized %d events (%d signals) in %s — %.0f events/s, %.0f signals/s",
		totalEvents, totalSignals, matDur.Round(time.Millisecond), eps, sps)

	require.Greater(t, eps, 1_000.0, "perf gate: materializer must sustain >1k events/s")
}
