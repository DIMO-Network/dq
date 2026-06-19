package materializer

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
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
	// BY (subject_bucket, day(timestamp)) so per-vehicle reads prune to one
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

// LatestRow is one row per (subject, source, name) in
// decoded/v1/latest/bucket=NNN/latest.parquet.
//
// Two ClickHouse business rules from internal/service/ch/queries.go are
// replicated here:
//
//   - A virtual per-(subject, source) row named lastSeenFieldName carries
//     max(timestamp) across ALL signals regardless of name
//     (getLastSeenQuery).
//   - The loc_*_nonzero columns mirror latestLocationCond/argMaxIf: they
//     track the latest location value where latitude or longitude is
//     non-zero, with loc_nonzero_ts as the matching maxIf timestamp.
//     The plain loc_* columns are the unconditional argMax values.
type LatestRow struct {
	Name              string    `parquet:"name"`
	Subject           string    `parquet:"subject"`
	Source            string    `parquet:"source"`
	Timestamp         time.Time `parquet:"timestamp,timestamp(microsecond)"`
	ValueNumber       float64   `parquet:"value_number"`
	ValueString       string    `parquet:"value_string"`
	LocLat            float64   `parquet:"loc_lat"`
	LocLon            float64   `parquet:"loc_lon"`
	LocHDOP           float64   `parquet:"loc_hdop"`
	LocHeading        float64   `parquet:"loc_heading"`
	LocLatNonzero     float64   `parquet:"loc_lat_nonzero"`
	LocLonNonzero     float64   `parquet:"loc_lon_nonzero"`
	LocHDOPNonzero    float64   `parquet:"loc_hdop_nonzero"`
	LocHeadingNonzero float64   `parquet:"loc_heading_nonzero"`
	LocNonzeroTS      time.Time `parquet:"loc_nonzero_ts,timestamp(microsecond)"`
}

// SummaryRow is one row per (subject, source, name) in
// decoded/v1/summary/bucket=NNN/summary.parquet.
type SummaryRow struct {
	Subject   string    `parquet:"subject"`
	Source    string    `parquet:"source"`
	Name      string    `parquet:"name"`
	Count     uint64    `parquet:"count"`
	FirstSeen time.Time `parquet:"first_seen,timestamp(microsecond)"`
	LastSeen  time.Time `parquet:"last_seen,timestamp(microsecond)"`
}

// writeSignalParquet encodes signal rows sorted by (subject, name,
// timestamp) with zstd compression and a bloom filter on subject.
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
		pq.SortingWriterConfig(pq.SortingColumns(
			pq.Ascending("subject"), pq.Ascending("name"), pq.Ascending("timestamp"),
		)),
		pq.BloomFilters(pq.SplitBlockFilter(subjectBloomFilterBitsPerValue, "subject")),
	)
}

// writeEventParquet encodes event rows sorted by (subject, name,
// timestamp) with zstd compression and a bloom filter on subject.
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
		pq.SortingWriterConfig(pq.SortingColumns(
			pq.Ascending("subject"), pq.Ascending("name"), pq.Ascending("timestamp"),
		)),
		pq.BloomFilters(pq.SplitBlockFilter(subjectBloomFilterBitsPerValue, "subject")),
	)
}

// writeBucketParquet encodes a latest/summary bucket file with zstd
// compression and stamps the parquet footer with the batch that produced
// it, making bucket updates idempotent across crash replays.
func writeBucketParquet[T any](rows []T, batchID string) ([]byte, error) {
	return writeParquet(rows,
		pq.Compression(&zstd.Codec{}),
		pq.KeyValueMetadata(kvBatchIDKey, batchID),
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

// readParquet decodes all rows of a parquet object and returns the batch
// stamp from the footer key-value metadata, if any.
func readParquet[T any](data []byte) (rows []T, batchID string, err error) {
	f, err := pq.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, "", fmt.Errorf("opening parquet file: %w", err)
	}
	batchID, _ = f.Lookup(kvBatchIDKey)

	reader := pq.NewGenericReader[T](f)
	defer func() { _ = reader.Close() }()

	numRows := reader.NumRows()
	if numRows == 0 {
		return nil, batchID, nil
	}
	rows = make([]T, numRows)
	n, err := reader.Read(rows)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, "", fmt.Errorf("reading parquet rows: %w", err)
	}
	return rows[:n], batchID, nil
}
