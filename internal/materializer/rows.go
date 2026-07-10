package materializer

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"time"

	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// subjectBloomFilterBitsPerValue sizes the subject bloom filter at ~1%
// false positives, matching cloudevent/parquet's encoder.
const subjectBloomFilterBitsPerValue = 10

// SignalRow is the decoded signal parquet schema
// (decoded/v1/signals/date=YYYY-MM-DD/...). vss.Signal's
// Data.ValueLocation is flattened into the four loc_* columns.
type SignalRow struct {
	Subject string `parquet:"subject"`
	// SubjectBucket is HashBucket(Subject): the decoded tables are PARTITIONED
	// BY (subject_bucket, year/month/day(timestamp)) so per-vehicle reads prune to one
	// bucket instead of scanning the fleet (CHD-1). Stamped at decode time so
	// it always agrees with the read-side duck.HashBucket.
	SubjectBucket int32     `parquet:"subject_bucket"`
	Name          string    `parquet:"name"`
	Timestamp     time.Time `parquet:"timestamp,timestamp(microsecond)"`
	Source        string    `parquet:"source"`
	Producer      string    `parquet:"producer"`
	CloudEventID  string    `parquet:"cloud_event_id"`
	ValueNumber   float64   `parquet:"value_number"`
	ValueString   string    `parquet:"value_string"`
	LocLat        float64   `parquet:"loc_lat"`
	LocLon        float64   `parquet:"loc_lon"`
	LocHDOP       float64   `parquet:"loc_hdop"`
	LocHeading    float64   `parquet:"loc_heading"`
}

// EventRow is the decoded event parquet schema
// (decoded/v1/events/date=YYYY-MM-DD/...) carrying all 11
// vss.EventToSlice columns.
type EventRow struct {
	Subject string `parquet:"subject"`
	// SubjectBucket mirrors SignalRow.SubjectBucket for lake.events partitioning.
	SubjectBucket int32     `parquet:"subject_bucket"`
	Source        string    `parquet:"source"`
	Producer      string    `parquet:"producer"`
	CloudEventID  string    `parquet:"cloud_event_id"`
	Type          string    `parquet:"type"`
	DataVersion   string    `parquet:"data_version"`
	Name          string    `parquet:"name"`
	Timestamp     time.Time `parquet:"timestamp,timestamp(microsecond)"`
	DurationNs    uint64    `parquet:"duration_ns"`
	Metadata      string    `parquet:"metadata"`
	Tags          []string  `parquet:"tags,list"`
}

// writeSignalParquet encodes signal rows sorted by (subject, name,
// timestamp) with zstd compression and a bloom filter on subject.
//
// The explicit slices.SortFunc is the ONE thing that actually orders the rows:
// pq.NewGenericWriter's SortingColumns config is metadata-only — parquet-go
// documents that a GenericWriter "always writes rows in the order they were seen,
// no reordering is performed" (only SortingWriter[T] reorders). The old
// SortingWriterConfig therefore did NO sorting; it was a misleading no-op on the
// hot path, dropped here (M7). The lake's declared sort order is DuckLake's
// `ALTER TABLE … SET SORTED BY`, applied when these rows are read back and
// INSERTed — the temp parquet's own metadata never reaches the lake.
func writeSignalParquet(rows []SignalRow) ([]byte, error) {
	slices.SortFunc(rows, func(a, b SignalRow) int {
		return cmp.Or(
			cmp.Compare(a.Subject, b.Subject),
			cmp.Compare(a.Name, b.Name),
			a.Timestamp.Compare(b.Timestamp),
		)
	})
	return writeParquet(rows,
		pq.Compression(&zstd.Codec{}),
		pq.BloomFilters(pq.SplitBlockFilter(subjectBloomFilterBitsPerValue, "subject")),
	)
}

// writeEventParquet encodes event rows sorted by (subject, name,
// timestamp) with zstd compression and a bloom filter on subject. As in
// writeSignalParquet, the explicit sort is the real guarantee (SortingColumns on
// a GenericWriter is metadata-only, so it was dropped — M7).
func writeEventParquet(rows []EventRow) ([]byte, error) {
	slices.SortFunc(rows, func(a, b EventRow) int {
		return cmp.Or(
			cmp.Compare(a.Subject, b.Subject),
			cmp.Compare(a.Name, b.Name),
			a.Timestamp.Compare(b.Timestamp),
		)
	})
	return writeParquet(rows,
		pq.Compression(&zstd.Codec{}),
		pq.BloomFilters(pq.SplitBlockFilter(subjectBloomFilterBitsPerValue, "subject")),
	)
}

func writeParquet[T any](rows []T, opts ...pq.WriterOption) ([]byte, error) {
	var buf bytes.Buffer
	writer := pq.NewGenericWriter[T](&buf, opts...)
	if len(rows) > 0 {
		if _, err := writer.Write(rows); err != nil {
			return nil, fmt.Errorf("writing parquet rows: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("closing parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}
