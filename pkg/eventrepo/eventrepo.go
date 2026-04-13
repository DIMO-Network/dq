// Package eventrepo contains service code for getting and managing cloudevent objects.
package eventrepo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/cloudevent"
	chindexer "github.com/DIMO-Network/cloudevent/clickhouse"
	"github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/fetch-api/pkg/grpc"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/volatiletech/sqlboiler/v4/drivers"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const tagsColumn = "JSONExtract(extras, 'tags', 'Array(String)')"

// Service manages and retrieves data messages from indexed objects in S3.
type Service struct {
	objGetter ObjectGetter
	presigner Presigner
	chConn    clickhouse.Conn
	// parquetBucket is the object storage bucket for Iceberg Parquet files.
	parquetBucket string
}

// ObjectInfo is the information about the object in S3.
type ObjectInfo struct {
	Key string
}

// ObjectGetter is an interface for getting an object from S3.
type ObjectGetter interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Presigner generates presigned S3 GET URLs.
type Presigner interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

// BlobKeyPrefix is the S3 key prefix used for large binary blob objects.
// Keys with this prefix are served via presigned URL instead of inline in the response.
const BlobKeyPrefix = "cloudevent/blobs/"

// presignTTL is the lifetime of generated presigned S3 URLs.
const presignTTL = 15 * time.Minute

// New creates a new instance of Service.
func New(chConn clickhouse.Conn, objGetter ObjectGetter, presigner Presigner, parquetBucket string) *Service {
	return &Service{
		objGetter:     objGetter,
		presigner:     presigner,
		chConn:        chConn,
		parquetBucket: parquetBucket,
	}
}

// PresignBlobURL returns a short-lived presigned GET URL for the given S3 key in the parquet bucket.
func (s *Service) PresignBlobURL(ctx context.Context, key string) (string, error) {
	if s.presigner == nil {
		return "", fmt.Errorf("presigner not configured")
	}
	if s.parquetBucket == "" {
		return "", fmt.Errorf("parquet bucket not configured")
	}
	req, err := s.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.parquetBucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(presignTTL))
	if err != nil {
		return "", fmt.Errorf("presign %s/%s: %w", s.parquetBucket, key, err)
	}
	return req.URL, nil
}

// GetLatestIndex returns the latest cloud event index that matches the given options.
func (s *Service) GetLatestIndex(ctx context.Context, opts *grpc.SearchOptions) (cloudevent.CloudEvent[ObjectInfo], error) {
	advancedOpts := convertSearchOptionsToAdvanced(opts)
	return s.GetLatestIndexAdvanced(ctx, advancedOpts)
}

// GetLatestIndexAdvanced returns the latest cloud event index that matches the given advanced options.
func (s *Service) GetLatestIndexAdvanced(ctx context.Context, advancedOpts *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[ObjectInfo], error) {
	// Only clone when we actually need to change TimestampAsc.
	opts := advancedOpts
	if advancedOpts != nil && advancedOpts.GetTimestampAsc().GetValue() {
		opts = proto.Clone(advancedOpts).(*grpc.AdvancedSearchOptions)
		opts.TimestampAsc = wrapperspb.Bool(false)
	}
	events, err := s.ListIndexesAdvanced(ctx, 1, opts)
	if err != nil {
		return cloudevent.CloudEvent[ObjectInfo]{}, err
	}
	return events[0], nil
}

// ListIndexes fetches and returns a list of index for cloud events that match the given options.
func (s *Service) ListIndexes(ctx context.Context, limit int, opts *grpc.SearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error) {
	advancedOpts := convertSearchOptionsToAdvanced(opts)
	return s.ListIndexesAdvanced(ctx, limit, advancedOpts)
}

// maxQueryLimit is the maximum number of rows a single query may return.
// Prevents unbounded result sets from malicious or buggy clients.
const maxQueryLimit = 1000

// ListIndexesAdvanced fetches and returns a list of index for cloud events that match the given advanced options.
func (s *Service) ListIndexesAdvanced(ctx context.Context, limit int, advancedOpts *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[ObjectInfo], error) {
	if limit <= 0 {
		limit = 1
	}
	if limit > maxQueryLimit {
		limit = maxQueryLimit
	}
	order := " DESC"
	if advancedOpts != nil && advancedOpts.GetTimestampAsc().GetValue() {
		order = " ASC"
	}
	mods := []qm.QueryMod{
		qm.Select(chindexer.SubjectColumn,
			chindexer.TimestampColumn,
			chindexer.TypeColumn,
			chindexer.IDColumn,
			chindexer.SourceColumn,
			chindexer.ProducerColumn,
			chindexer.DataContentTypeColumn,
			chindexer.DataVersionColumn,
			chindexer.ExtrasColumn,
			chindexer.IndexKeyColumn,
		),
		qm.From(chindexer.TableName),
		qm.OrderBy(chindexer.TimestampColumn + order),
		qm.Limit(limit),
	}

	// Apply advanced search options
	if advancedOpts != nil {
		advancedMods := AdvancedSearchOptionsToQueryMod(advancedOpts)
		mods = append(mods, advancedMods...)
	}
	query, args := newQuery(mods...)
	rows, err := s.chConn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get cloud events: %w", err)
	}

	cloudEvents := make([]cloudevent.CloudEvent[ObjectInfo], 0, limit)
	var extras string
	for rows.Next() {
		var event cloudevent.CloudEvent[ObjectInfo]
		err = rows.Scan(&event.Subject, &event.Time, &event.Type, &event.ID, &event.Source, &event.Producer, &event.DataContentType, &event.DataVersion, &extras, &event.Data.Key)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed to scan cloud event: %w", err)
		}
		if extras != "" && extras != "null" {
			if err = json.Unmarshal([]byte(extras), &event.Extras); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("failed to unmarshal extras: %w", err)
			}
			// Restore non-column fields from extras
			cloudevent.RestoreNonColumnFields(&event.CloudEventHeader)
		}
		cloudEvents = append(cloudEvents, event)
	}
	_ = rows.Close()
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over cloud events: %w", err)
	}
	if len(cloudEvents) == 0 {
		return nil, fmt.Errorf("no cloud events found %w", sql.ErrNoRows)
	}
	return cloudEvents, nil
}

// CloudEventTypeSummary holds per-type aggregate metadata for a subject.
type CloudEventTypeSummary struct {
	Type      string
	Count     uint64
	FirstSeen time.Time
	LastSeen  time.Time
}

// GetCloudEventTypeSummaries returns per-type counts and time ranges for the given search options.
func (s *Service) GetCloudEventTypeSummaries(ctx context.Context, opts *grpc.SearchOptions) ([]CloudEventTypeSummary, error) {
	return s.GetCloudEventTypeSummariesAdvanced(ctx, convertSearchOptionsToAdvanced(opts))
}

// GetCloudEventTypeSummariesAdvanced returns event type summaries filtered by advanced search options.
func (s *Service) GetCloudEventTypeSummariesAdvanced(ctx context.Context, advancedOpts *grpc.AdvancedSearchOptions) ([]CloudEventTypeSummary, error) {
	mods := []qm.QueryMod{
		qm.Select(
			chindexer.TypeColumn,
			"count(*) AS count",
			"MIN("+chindexer.TimestampColumn+") AS first_seen",
			"MAX("+chindexer.TimestampColumn+") AS last_seen",
		),
		qm.From(chindexer.TableName),
		qm.GroupBy(chindexer.TypeColumn),
		qm.OrderBy(chindexer.TypeColumn),
	}

	if advancedOpts != nil {
		mods = append(mods, AdvancedSearchOptionsToQueryMod(advancedOpts)...)
	}

	query, args := newQuery(mods...)
	rows, err := s.chConn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get event type summaries: %w", err)
	}

	var summaries []CloudEventTypeSummary
	for rows.Next() {
		var s CloudEventTypeSummary
		if err := rows.Scan(&s.Type, &s.Count, &s.FirstSeen, &s.LastSeen); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed to scan event type summary: %w", err)
		}
		summaries = append(summaries, s)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate event type summaries: %w", err)
	}
	if summaries == nil {
		summaries = []CloudEventTypeSummary{}
	}
	return summaries, nil
}

// ListCloudEvents fetches and returns the cloud events that match the given options.
func (s *Service) ListCloudEvents(ctx context.Context, bucketName string, limit int, opts *grpc.SearchOptions) ([]cloudevent.RawEvent, error) {
	advancedOpts := convertSearchOptionsToAdvanced(opts)
	return s.ListCloudEventsAdvanced(ctx, bucketName, limit, advancedOpts)
}

// ListCloudEventsAdvanced fetches and returns the cloud events that match the given advanced options.
func (s *Service) ListCloudEventsAdvanced(ctx context.Context, bucketName string, limit int, advancedOpts *grpc.AdvancedSearchOptions) ([]cloudevent.RawEvent, error) {
	events, err := s.ListIndexesAdvanced(ctx, limit, advancedOpts)
	if err != nil {
		return nil, err
	}
	data, err := s.ListCloudEventsFromIndexes(ctx, events, bucketName)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetLatestCloudEvent fetches and returns the latest cloud event that matches the given options.
func (s *Service) GetLatestCloudEvent(ctx context.Context, bucketName string, opts *grpc.SearchOptions) (cloudevent.RawEvent, error) {
	advancedOpts := convertSearchOptionsToAdvanced(opts)
	return s.GetLatestCloudEventAdvanced(ctx, bucketName, advancedOpts)
}

// GetLatestCloudEventAdvanced fetches and returns the latest cloud event that matches the given advanced options.
func (s *Service) GetLatestCloudEventAdvanced(ctx context.Context, bucketName string, advancedOpts *grpc.AdvancedSearchOptions) (cloudevent.RawEvent, error) {
	cloudIdx, err := s.GetLatestIndexAdvanced(ctx, advancedOpts)
	if err != nil {
		return cloudevent.RawEvent{}, err
	}

	data, err := s.GetCloudEventFromIndex(ctx, &cloudIdx, bucketName)
	if err != nil {
		return cloudevent.RawEvent{}, err
	}

	return data, nil
}

// fetchConcurrency is the maximum number of concurrent S3 fetches.
const fetchConcurrency = 50

// ListCloudEventsFromIndexes fetches and returns the cloud events for the given index.
// Parquet refs sharing the same object key are grouped so they share one S3ReaderAt
// (avoiding duplicate downloads). Groups and JSON fetches run concurrently.
func (s *Service) ListCloudEventsFromIndexes(ctx context.Context, indexes []cloudevent.CloudEvent[ObjectInfo], bucketName string) ([]cloudevent.RawEvent, error) {
	events := make([]cloudevent.RawEvent, len(indexes))

	// Group parquet indexes by object key so rows from the same file share a reader.
	type parquetItem struct {
		idx       int
		bucket    string
		objectKey string
		rowOffset int64
	}
	parquetGroups := make(map[string][]parquetItem, len(indexes))

	type jsonItem struct {
		idx int
		key string
	}
	var jsonItems []jsonItem

	// Classify each index into parquet groups or JSON items.
	for i := range indexes {
		key := indexes[i].Data.Key
		if parquet.IsParquetRef(key) {
			bucket, objectKey, rowOffset, err := parseParquetRef(key)
			if err != nil {
				return nil, fmt.Errorf("parse parquet ref: %w", err)
			}
			if bucket == "" {
				bucket = s.parquetBucket
			}
			if bucket == "" {
				return nil, fmt.Errorf("parquet bucket not configured and index_key has no s3:// URI: %s", key)
			}
			parquetGroups[objectKey] = append(parquetGroups[objectKey], parquetItem{
				idx: i, bucket: bucket, objectKey: objectKey, rowOffset: rowOffset,
			})
		} else {
			jsonItems = append(jsonItems, jsonItem{idx: i, key: key})
		}
	}

	group, errCtx := errgroup.WithContext(ctx)
	group.SetLimit(fetchConcurrency)

	// Process each parquet group concurrently (rows within a group share one
	// opened parquet Reader, avoiding repeated footer reads from S3).
	for _, items := range parquetGroups {
		group.Go(func() error {
			s3r, err := NewS3ReaderAt(errCtx, s.objGetter, items[0].bucket, items[0].objectKey)
			if err != nil {
				return fmt.Errorf("create s3 reader for %s: %w", items[0].objectKey, err)
			}
			pr, err := parquet.OpenReader(s3r, s3r.Size())
			if err != nil {
				return fmt.Errorf("open parquet reader for %s: %w", items[0].objectKey, err)
			}
			defer func() { _ = pr.Close() }()
			for _, item := range items {
				event, err := pr.SeekToRow(item.rowOffset)
				if err != nil {
					return fmt.Errorf("seek to row %d in %s: %w", item.rowOffset, item.objectKey, err)
				}
				event.Tags = grpc.TagsOrEmpty(event.Tags)
				events[item.idx] = event
			}
			return nil
		})
	}

	// Process JSON items concurrently, deduplicating in-flight fetches by key.
	var sf singleflight.Group

	for _, item := range jsonItems {
		group.Go(func() error {
			v, err, _ := sf.Do(item.key, func() (any, error) {
				return s.GetCloudEventFromIndex(errCtx, &indexes[item.idx], bucketName)
			})
			if err != nil {
				return err
			}
			ev := v.(cloudevent.RawEvent)
			// Apply this index's header; shared result may have a different one.
			hdr := indexes[item.idx].CloudEventHeader
			hdr.Tags = grpc.TagsOrEmpty(hdr.Tags)
			events[item.idx] = cloudevent.RawEvent{CloudEventHeader: hdr, Data: ev.Data, DataBase64: ev.DataBase64}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}
	return events, nil
}

// GetCloudEventFromIndex fetches and returns the cloud event for the given index.
func (s *Service) GetCloudEventFromIndex(ctx context.Context, index *cloudevent.CloudEvent[ObjectInfo], bucketName string) (cloudevent.RawEvent, error) {
	if parquet.IsParquetRef(index.Data.Key) {
		return s.getCloudEventFromParquet(ctx, index.Data.Key)
	}
	// Legacy JSON path
	rawData, err := s.getObjectFromS3(ctx, index.Data.Key, bucketName)
	if err != nil {
		return cloudevent.RawEvent{}, err
	}
	return toCloudEvent(&index.CloudEventHeader, rawData)
}

// ListObjectsFromKeys fetches and returns the objects for the given keys concurrently.
func (s *Service) ListObjectsFromKeys(ctx context.Context, keys []string, bucketName string) ([][]byte, error) {
	data := make([][]byte, len(keys))
	group, errCtx := errgroup.WithContext(ctx)
	group.SetLimit(fetchConcurrency)
	for i, key := range keys {
		group.Go(func() error {
			obj, err := s.GetObjectFromKey(errCtx, key, bucketName)
			if err != nil {
				return fmt.Errorf("failed to get data from key '%s': %w", key, err)
			}
			data[i] = obj
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return data, nil
}

// GetObjectFromKey fetches and returns the raw object for the given key.
// Routes based on index_key format:
//   - If key contains "#": Parquet reference -- reads the event via SeekToRow and returns data.
//   - Otherwise: legacy S3 path -- fetches the entire object as before.
func (s *Service) GetObjectFromKey(ctx context.Context, key, bucketName string) ([]byte, error) {
	if parquet.IsParquetRef(key) {
		ev, err := s.getCloudEventFromParquet(ctx, key)
		if err != nil {
			return nil, err
		}
		return ev.Data, nil
	}
	return s.getObjectFromS3(ctx, key, bucketName)
}

// getCloudEventFromParquet retrieves a full RawEvent from a parquet file via SeekToRow.
func (s *Service) getCloudEventFromParquet(ctx context.Context, key string) (cloudevent.RawEvent, error) {
	bucket, objectKey, rowOffset, err := parseParquetRef(key)
	if err != nil {
		return cloudevent.RawEvent{}, fmt.Errorf("failed to parse parquet index_key: %w", err)
	}

	if bucket == "" {
		bucket = s.parquetBucket
	}
	if bucket == "" {
		return cloudevent.RawEvent{}, fmt.Errorf("parquet bucket not configured and index_key has no s3:// URI: %s", key)
	}

	reader, err := NewS3ReaderAt(ctx, s.objGetter, bucket, objectKey)
	if err != nil {
		return cloudevent.RawEvent{}, fmt.Errorf("create s3 reader for %s: %w", objectKey, err)
	}

	event, err := parquet.SeekToRow(reader, reader.Size(), rowOffset)
	if err != nil {
		return cloudevent.RawEvent{}, fmt.Errorf("seek to row %d in %s: %w", rowOffset, objectKey, err)
	}
	event.Tags = grpc.TagsOrEmpty(event.Tags)
	return event, nil
}

// parseParquetRef parses a parquet index key into bucket, object key, and row offset.
// Handles s3://bucket/key#row URIs by extracting the bucket from the URI.
func parseParquetRef(indexKey string) (bucket, objectKey string, rowOffset int64, err error) {
	objKey, rowOffset, err := parquet.ParseIndexKey(indexKey)
	if err != nil {
		return "", "", 0, err
	}
	if strings.HasPrefix(objKey, "s3://") {
		rest := objKey[5:]
		bucket, objectKey, found := strings.Cut(rest, "/")
		if !found {
			return "", "", 0, fmt.Errorf("invalid s3 URI: %s", objKey)
		}
		return bucket, objectKey, rowOffset, nil
	}
	return "", objKey, rowOffset, nil
}

// maxObjectSize is the maximum size of a single S3 object we'll read (50 MiB).
// Objects larger than this are rejected to prevent OOM from corrupted or
// malicious index keys pointing to oversized objects.
const maxObjectSize = 50 << 20

// getObjectFromS3 fetches the entire object from S3 (legacy per-file JSON path).
func (s *Service) getObjectFromS3(ctx context.Context, key, bucketName string) ([]byte, error) {
	return downloadS3Object(ctx, s.objGetter, bucketName, key, maxObjectSize)
}

// StoreObject stores the given data in S3 with the given cloudevent header.
func (s *Service) StoreObject(ctx context.Context, bucketName string, cloudHeader *cloudevent.CloudEventHeader, data []byte) error {
	key := chindexer.CloudEventToObjectKey(cloudHeader)
	_, err := s.objGetter.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to store object in S3: %w", err)
	}

	values := chindexer.CloudEventToSlice(cloudHeader)

	err = s.chConn.Exec(ctx, chindexer.InsertStmt, values...)
	if err != nil {
		return fmt.Errorf("failed to store index in ClickHouse: %w", err)
	}

	return nil
}

// toCloudEvent extracts only the data and data_base64 fields from the stored
// CloudEvent JSON, then overlays the index header. We skip parsing header fields
// since they come from the DB index. When data_base64 is present, Data is left nil.
func toCloudEvent(dbHdr *cloudevent.CloudEventHeader, raw []byte) (cloudevent.RawEvent, error) {
	var partial struct {
		Data       json.RawMessage `json:"data"`
		DataBase64 string          `json:"data_base64,omitempty"`
	}
	if err := json.Unmarshal(raw, &partial); err != nil {
		return cloudevent.RawEvent{}, err
	}
	ev := cloudevent.RawEvent{
		CloudEventHeader: *dbHdr,
	}
	if partial.DataBase64 != "" {
		ev.DataBase64 = partial.DataBase64
	} else {
		ev.Data = partial.Data
	}
	ev.Tags = grpc.TagsOrEmpty(ev.Tags)
	return ev, nil
}

// convertSearchOptionsToAdvanced converts basic SearchOptions to AdvancedSearchOptions
func convertSearchOptionsToAdvanced(opts *grpc.SearchOptions) *grpc.AdvancedSearchOptions {
	if opts == nil {
		return nil
	}

	advanced := &grpc.AdvancedSearchOptions{
		After:        opts.GetAfter(),
		Before:       opts.GetBefore(),
		TimestampAsc: opts.GetTimestampAsc(),
	}

	// Convert each field to StringFilterOption with has_any logic
	if opts.GetType() != nil {
		advanced.Type = &grpc.StringFilterOption{
			In: []string{opts.GetType().GetValue()},
		}
	}
	if opts.GetDataVersion() != nil {
		advanced.DataVersion = &grpc.StringFilterOption{
			In: []string{opts.GetDataVersion().GetValue()},
		}
	}
	if opts.GetSubject() != nil {
		advanced.Subject = &grpc.StringFilterOption{
			In: []string{opts.GetSubject().GetValue()},
		}
	}
	if opts.GetSource() != nil {
		advanced.Source = &grpc.StringFilterOption{
			In: []string{opts.GetSource().GetValue()},
		}
	}
	if opts.GetProducer() != nil {
		advanced.Producer = &grpc.StringFilterOption{
			In: []string{opts.GetProducer().GetValue()},
		}
	}
	if opts.GetExtras() != nil {
		advanced.Extras = &grpc.StringFilterOption{
			In: []string{opts.GetExtras().GetValue()},
		}
	}
	if opts.GetId() != nil {
		advanced.Id = &grpc.StringFilterOption{
			In: []string{opts.GetId().GetValue()},
		}
	}

	return advanced
}

func AdvancedSearchOptionsToQueryMod(opts *grpc.AdvancedSearchOptions) []qm.QueryMod {
	if opts == nil {
		return nil
	}
	var mods []qm.QueryMod

	// Handle timestamp filtering (same as SearchOptions)
	if opts.GetAfter() != nil {
		mods = append(mods, qm.Where(chindexer.TimestampColumn+" > ?", opts.GetAfter().AsTime()))
	}
	if opts.GetBefore() != nil {
		mods = append(mods, qm.Where(chindexer.TimestampColumn+" < ?", opts.GetBefore().AsTime()))
	}

	// Handle advanced filtering for each field
	if opts.GetType() != nil {
		mods = append(mods, qm.Expr(stringFilterMods(opts.GetType(), chindexer.TypeColumn)...))
	}

	if opts.GetDataVersion() != nil {
		mods = append(mods, qm.Expr(stringFilterMods(opts.GetDataVersion(), chindexer.DataVersionColumn)...))
	}

	if opts.GetSubject() != nil {
		mods = append(mods, qm.Expr(stringFilterMods(opts.GetSubject(), chindexer.SubjectColumn)...))
	}

	if opts.GetSource() != nil {
		mods = append(mods, qm.Expr(stringFilterMods(opts.GetSource(), chindexer.SourceColumn)...))
	}

	if opts.GetProducer() != nil {
		mods = append(mods, qm.Expr(stringFilterMods(opts.GetProducer(), chindexer.ProducerColumn)...))
	}

	if opts.GetExtras() != nil {
		mods = append(mods, qm.Expr(stringFilterMods(opts.GetExtras(), chindexer.ExtrasColumn)...))
	}

	if opts.GetId() != nil {
		mods = append(mods, qm.Expr(stringFilterMods(opts.GetId(), chindexer.IDColumn)...))
	}

	if opts.GetTags() != nil {
		mods = append(mods, qm.Expr(arrayFilterMods(opts.GetTags(), tagsColumn)...))
	}

	return mods
}

// maxFilterDepth is the maximum nesting depth for recursive Or filter conditions.
// Prevents stack overflow and oversized queries from malicious gRPC clients.
const maxFilterDepth = 5

// stringFilterMods converts a StringFilterOption to query modifications.
func stringFilterMods(filter *grpc.StringFilterOption, columnName string) []qm.QueryMod {
	return stringFilterModsDepth(filter, columnName, 0)
}

func stringFilterModsDepth(filter *grpc.StringFilterOption, columnName string, depth int) []qm.QueryMod {
	if filter == nil || depth > maxFilterDepth {
		return nil
	}
	var mods []qm.QueryMod

	if len(filter.GetIn()) > 0 {
		mods = append(mods, qm.Where(columnName+" IN (?)", filter.GetIn()))
	}
	if len(filter.GetNotIn()) > 0 {
		mods = append(mods, qm.Where(columnName+" NOT IN (?)", filter.GetNotIn()))
	}
	for _, cond := range filter.GetOr() {
		clauseMods := stringFilterModsDepth(cond, columnName, depth+1)
		if len(clauseMods) != 0 {
			mods = append(mods, qm.Or2(qm.Expr(clauseMods...)))
		}
	}

	if len(filter.GetOr()) != 0 {
		mods = []qm.QueryMod{qm.Expr(mods...)}
	}
	return mods
}

// arrayFilterMods converts an ArrayFilterOption to query modifications.
func arrayFilterMods(filter *grpc.ArrayFilterOption, columnName string) []qm.QueryMod {
	return arrayFilterModsDepth(filter, columnName, 0)
}

func arrayFilterModsDepth(filter *grpc.ArrayFilterOption, columnName string, depth int) []qm.QueryMod {
	if filter == nil || depth > maxFilterDepth {
		return nil
	}
	var mods []qm.QueryMod

	if len(filter.GetContainsAny()) > 0 {
		mods = append(mods, qm.Where("hasAny("+columnName+", ?)", filter.GetContainsAny()))
	}
	if len(filter.GetContainsAll()) > 0 {
		mods = append(mods, qm.Where("hasAll("+columnName+", ?)", filter.GetContainsAll()))
	}
	if len(filter.GetNotContainsAny()) > 0 {
		mods = append(mods, qm.Where("NOT hasAny("+columnName+", ?)", filter.GetNotContainsAny()))
	}
	if len(filter.GetNotContainsAll()) > 0 {
		mods = append(mods, qm.Where("NOT hasAll("+columnName+", ?)", filter.GetNotContainsAll()))
	}
	for _, cond := range filter.GetOr() {
		clauseMods := arrayFilterModsDepth(cond, columnName, depth+1)
		if len(clauseMods) != 0 {
			mods = append(mods, qm.Or2(qm.Expr(clauseMods...)))
		}
	}
	if len(filter.GetOr()) != 0 {
		mods = []qm.QueryMod{qm.Expr(mods...)}
	}

	return mods
}

var dialect = drivers.Dialect{
	LQ:                      '`',
	RQ:                      '`',
	UseIndexPlaceholders:    false,
	UseLastInsertID:         false,
	UseSchema:               false,
	UseDefaultKeyword:       false,
	UseAutoColumns:          false,
	UseTopClause:            false,
	UseOutputClause:         false,
	UseCaseWhenExistsClause: false,
}

// newQuery initializes a new Query using the passed in QueryMods.
func newQuery(mods ...qm.QueryMod) (string, []any) {
	q := &queries.Query{}
	queries.SetDialect(q, &dialect)
	qm.Apply(q, mods...)
	return queries.BuildQuery(q)
}
