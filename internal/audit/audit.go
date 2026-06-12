// Package audit verifies parse-on-read pipeline invariants directly against
// an object store. It is the assertion half of chaos testing today and is
// store-agnostic so the same checks can run against production S3 before
// cutover: point it at a bucket, get violations or a clean bill.
//
// Invariants checked:
//   - watermark cursors are well-formed and point into their own partition
//   - no decoded signal row is duplicated: (cloud_event_id, name, timestamp)
//     is unique within every decoded date partition (aggregations do not
//     dedup, so a duplicate here double-counts in query results)
//   - every decoded cloud_event_id exists in the raw layer (nothing invented)
//   - no leftover compaction manifests (interrupted merges must converge)
package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/dq/internal/materializer"
	pq "github.com/parquet-go/parquet-go"
)

// Violation is one invariant breach.
type Violation struct {
	Kind   string
	Detail string
}

// Report summarizes one audit pass.
type Report struct {
	RawEvents      int
	DecodedRows    int
	DecodedIDs     map[string]struct{}
	StagedOrphans  int
	Violations     []Violation
	WatermarkKeys  int
	RawPartitions  int
	DecodedDates   int
	CoveredCursors int
}

func (r *Report) violate(kind, format string, args ...any) {
	r.Violations = append(r.Violations, Violation{Kind: kind, Detail: fmt.Sprintf(format, args...)})
}

// signalKeyRow is the projection of decoded signal rows the audit needs.
type signalKeyRow struct {
	CloudEventID string    `parquet:"cloud_event_id"`
	Name         string    `parquet:"name"`
	Timestamp    time.Time `parquet:"timestamp,timestamp(microsecond)"`
}

// CheckPipeline runs every invariant over rawPrefix/decodedPrefix in store.
func CheckPipeline(ctx context.Context, store materializer.ObjectStore, rawPrefix, decodedPrefix string) (*Report, error) {
	if !strings.HasSuffix(rawPrefix, "/") {
		rawPrefix += "/"
	}
	if !strings.HasSuffix(decodedPrefix, "/") {
		decodedPrefix += "/"
	}
	report := &Report{DecodedIDs: map[string]struct{}{}}

	rawIDs, err := collectRawEventIDs(ctx, store, rawPrefix, report)
	if err != nil {
		return nil, err
	}
	if err := checkDecodedSignals(ctx, store, decodedPrefix, rawIDs, report); err != nil {
		return nil, err
	}
	if err := checkWatermark(ctx, store, rawPrefix, decodedPrefix, report); err != nil {
		return nil, err
	}
	if err := checkStaging(ctx, store, decodedPrefix, report); err != nil {
		return nil, err
	}
	return report, nil
}

// collectRawEventIDs decodes every raw bundle and returns the full event-ID
// set (the "nothing invented" reference).
func collectRawEventIDs(ctx context.Context, store materializer.ObjectStore, rawPrefix string, report *Report) (map[string]struct{}, error) {
	objects, err := store.List(ctx, rawPrefix+"type=")
	if err != nil {
		return nil, fmt.Errorf("listing raw layer: %w", err)
	}
	ids := map[string]struct{}{}
	partitions := map[string]struct{}{}
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		partitions[path.Dir(obj.Key)] = struct{}{}
		body, err := store.GetObject(ctx, obj.Key)
		if err != nil {
			return nil, fmt.Errorf("reading raw %s: %w", obj.Key, err)
		}
		events, err := ceparquet.Decode(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			report.violate("raw-undecodable", "%s: %v", obj.Key, err)
			continue
		}
		for i := range events {
			ids[events[i].ID] = struct{}{}
		}
		report.RawEvents += len(events)
	}
	report.RawPartitions = len(partitions)
	return ids, nil
}

// checkDecodedSignals enforces per-partition row uniqueness and the
// decoded ⊆ raw relation.
func checkDecodedSignals(ctx context.Context, store materializer.ObjectStore, decodedPrefix string, rawIDs map[string]struct{}, report *Report) error {
	objects, err := store.List(ctx, decodedPrefix+"signals/")
	if err != nil {
		return fmt.Errorf("listing decoded signals: %w", err)
	}
	type rowKey struct {
		id, name string
		ts       int64
	}
	byDate := map[string]map[rowKey]int{}
	dates := map[string]struct{}{}
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		date := path.Base(path.Dir(obj.Key))
		dates[date] = struct{}{}
		body, err := store.GetObject(ctx, obj.Key)
		if err != nil {
			return fmt.Errorf("reading decoded %s: %w", obj.Key, err)
		}
		rows, err := readSignalKeys(body)
		if err != nil {
			report.violate("decoded-unreadable", "%s: %v", obj.Key, err)
			continue
		}
		seen := byDate[date]
		if seen == nil {
			seen = map[rowKey]int{}
			byDate[date] = seen
		}
		for _, row := range rows {
			k := rowKey{id: row.CloudEventID, name: row.Name, ts: row.Timestamp.UnixMicro()}
			seen[k]++
			report.DecodedRows++
			report.DecodedIDs[row.CloudEventID] = struct{}{}
			if _, ok := rawIDs[row.CloudEventID]; !ok {
				report.violate("decoded-not-in-raw", "cloud_event_id %s (file %s)", row.CloudEventID, obj.Key)
			}
		}
	}
	report.DecodedDates = len(dates)
	for date, seen := range byDate {
		for k, n := range seen {
			if n > 1 {
				report.violate("decoded-duplicate-row",
					"date=%s id=%s name=%s ts=%d appears %d times — aggregations double-count",
					date, k.id, k.name, k.ts, n)
			}
		}
	}
	return nil
}

// checkWatermark validates cursor key shapes and partition agreement
// across every cursor file (watermark.json single replica, or one
// watermark-pNNNofMMM.json per shard). Sharded cursors must also be
// disjoint: two shards claiming the same partition is a split-brain.
func checkWatermark(ctx context.Context, store materializer.ObjectStore, rawPrefix, decodedPrefix string, report *Report) error {
	objects, err := store.List(ctx, decodedPrefix+"_state/watermark")
	if err != nil {
		return fmt.Errorf("listing watermarks: %w", err)
	}
	owners := map[string]string{}
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		body, err := store.GetObject(ctx, obj.Key)
		if err != nil {
			return fmt.Errorf("reading watermark %s: %w", obj.Key, err)
		}
		var watermark map[string]string
		if err := json.Unmarshal(body, &watermark); err != nil {
			report.violate("watermark-corrupt", "%s: %v", obj.Key, err)
			continue
		}
		report.WatermarkKeys += len(watermark)
		for partition, cursor := range watermark {
			if prev, ok := owners[partition]; ok {
				report.violate("watermark-split-brain", "partition %q claimed by %s and %s", partition, prev, obj.Key)
			}
			owners[partition] = obj.Key
			wantPrefix := rawPrefix + partition + "/"
			if !strings.HasPrefix(cursor, wantPrefix) {
				report.violate("watermark-cursor-mismatch", "partition %q cursor %q not under %q", partition, cursor, wantPrefix)
				continue
			}
			report.CoveredCursors++
		}
	}
	return nil
}

// checkStaging flags interrupted compactions that never recovered.
func checkStaging(ctx context.Context, store materializer.ObjectStore, decodedPrefix string, report *Report) error {
	objects, err := store.List(ctx, decodedPrefix+"_compaction/")
	if err != nil {
		return fmt.Errorf("listing compaction staging: %w", err)
	}
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".json") {
			report.violate("compaction-manifest-leftover", "%s — interrupted merge did not recover", obj.Key)
			continue
		}
		report.StagedOrphans++ // orphaned staged parquet: allowed by design, counted
	}
	return nil
}

func readSignalKeys(body []byte) ([]signalKeyRow, error) {
	f, err := pq.OpenFile(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, err
	}
	reader := pq.NewGenericReader[signalKeyRow](f)
	defer func() { _ = reader.Close() }()
	n := reader.NumRows()
	if n == 0 {
		return nil, nil
	}
	rows := make([]signalKeyRow, n)
	read, err := reader.Read(rows)
	if err != nil && read < int(n) {
		return nil, err
	}
	return rows[:read], nil
}
