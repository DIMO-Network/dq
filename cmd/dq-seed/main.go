// Command dq-seed writes dimo.status raw events into a DuckLake catalog the
// way din's sink does, then runs the materializer over them once, so a local
// dq boots with decoded signals already in place. It is a development tool:
// it stands in for din when the whole DIMO stack is run on one machine
// (did-directory's scripts/demo.sh) and for nothing else.
//
// The catalog is a DuckDB file with one writer at a time, so run this before
// starting dq, or against a catalog no dq holds open. Decoding here rather
// than leaving it to dq's own materializer also means the decoded tables
// exist before dq's query connections attach the catalog, which is the state
// a production query pod finds.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/rs/zerolog"
)

func main() {
	catalog := flag.String("catalog", "", "DuckLake catalog file (dq's DUCKLAKE_CATALOG_DSN)")
	dataPath := flag.String("data-path", "", "DuckLake data directory (dq's DUCKLAKE_DATA_PATH)")
	subject := flag.String("subject", "", "vehicle DID the events are about")
	fromFlag := flag.String("from", "", "first event time, RFC 3339")
	toFlag := flag.String("to", "", "last event time, RFC 3339 (default now)")
	every := flag.Duration("every", 5*time.Second, "spacing between events")
	source := flag.String("source", "0xDemoConnection", "cloud event source")
	flag.Parse()

	if *catalog == "" || *dataPath == "" || *subject == "" || *fromFlag == "" {
		fmt.Fprintln(os.Stderr, "Error: -catalog, -data-path, -subject and -from are required")
		os.Exit(1)
	}
	from, err := time.Parse(time.RFC3339, *fromFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: -from: %v\n", err)
		os.Exit(1)
	}
	to := time.Now().UTC()
	if *toFlag != "" {
		if to, err = time.Parse(time.RFC3339, *toFlag); err != nil {
			fmt.Fprintf(os.Stderr, "Error: -to: %v\n", err)
			os.Exit(1)
		}
	}
	if !to.After(from) || *every <= 0 {
		fmt.Fprintln(os.Stderr, "Error: -to must be after -from and -every must be positive")
		os.Exit(1)
	}

	n, decoded, err := seed(context.Background(), *catalog, *dataPath, *subject, *source, from, to, *every)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("seeded %d dimo.status events for %s from %s to %s every %s; decoded %d into lake.signals\n",
		n, *subject, from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339), *every, decoded)
}

// seed opens the catalog, creates lake.raw_events in din's shape if it is not
// there, inserts one status event per tick carrying a speed signal that
// swings between 0 and 100 km/h, so a time-series query has something to
// show, and then decodes everything the materializer has not seen yet.
// Returns the number of events written and the number of raw rows decoded.
func seed(ctx context.Context, catalog, dataPath, subject, source string, from, to time.Time, every time.Duration) (int, int, error) {
	svc, err := duck.NewService(duck.Config{DuckLakeEnabled: true, CatalogDSN: catalog, DataPath: dataPath})
	if err != nil {
		return 0, 0, fmt.Errorf("open catalog: %w", err)
	}
	defer func() { _ = svc.Close() }()
	db := svc.DB()

	// din's DDL: RawEventColumns in internal/service/duck is the projection dq
	// reads back, in this column order.
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS lake.raw_events (
		subject VARCHAR, "time" TIMESTAMP WITH TIME ZONE, type VARCHAR, id VARCHAR,
		source VARCHAR, producer VARCHAR, data_content_type VARCHAR, data_version VARCHAR,
		extras VARCHAR, data VARCHAR, data_base64 BLOB, data_index_key VARCHAR, voids_id VARCHAR)`); err != nil {
		return 0, 0, fmt.Errorf("create lake.raw_events: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	n := 0
	for ts := from.UTC(); !ts.After(to); ts = ts.Add(every) {
		// One swing from 0 to 100 km/h and back over about an hour.
		speed := 50 + 50*math.Sin(float64(ts.Unix())/600)
		payload, err := json.Marshal(map[string]any{"signals": []map[string]any{{
			"name": "speed", "timestamp": ts.Format(time.RFC3339Nano), "value": math.Round(speed*10) / 10,
		}}})
		if err != nil {
			return 0, 0, err
		}
		id := fmt.Sprintf("seed-%s-%d", ts.Format("20060102T150405"), n)
		// din's appender writes empty strings, not NULLs, for the header columns.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO lake.raw_events (subject, "time", type, id, source, producer, data_content_type, data_version, extras, data)
			 VALUES (?, ?, ?, ?, ?, ?, '', ?, '{}', ?)`,
			subject, ts, cloudevent.TypeStatus, id, source, subject, "default/v1.0", string(payload)); err != nil {
			return 0, 0, fmt.Errorf("insert event %s: %w", id, err)
		}
		n++
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}

	// Decode as dq's materializer would: the decoded tables are created if
	// they are missing, every new raw row becomes signal rows, and the latest
	// rollups are flushed so signalsLatest answers too.
	log := zerolog.New(os.Stderr).Level(zerolog.WarnLevel)
	mat, err := materializer.NewDuckLakeMaterializer(ctx, db, log)
	if err != nil {
		return 0, 0, fmt.Errorf("materializer: %w", err)
	}
	// No vehicle contract configured: every subject decodes, did:dimo included.
	runner := materializer.New(materializer.Config{}, log).WithDuckLake(mat)
	decoded := 0
	for {
		got, err := runner.RunOnce(ctx)
		if err != nil {
			return 0, 0, fmt.Errorf("decode: %w", err)
		}
		decoded += got
		if got == 0 {
			break
		}
	}
	if err := runner.FlushRollup(ctx); err != nil {
		return 0, 0, fmt.Errorf("flush signals_latest: %w", err)
	}
	if err := runner.FlushEventRollup(ctx); err != nil {
		return 0, 0, fmt.Errorf("flush events_latest: %w", err)
	}
	return n, decoded, nil
}
