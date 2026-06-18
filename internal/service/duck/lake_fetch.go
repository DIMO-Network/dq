package duck

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// errOrClauseUnsupported is returned when an advanced filter contains an Or
// clause that the lake path cannot translate to SQL. Returning an error rather
// than silently over-returning preserves correctness until Or is fully
// implemented.
var errOrClauseUnsupported = errors.New("lake fetch: Or clauses in advanced filter are not yet supported")

const lakeRawEvents = "lake.raw_events"

// lakeRawColumns matches rawColumns and scanStoredEvent's scan order.
const lakeRawColumns = "subject, time, type, id, source, producer, data_content_type, data_version, extras, data, data_base64, data_index_key, voids_id"

// LakeEventService serves the eventrepo.EventService surface from
// lake.raw_events. Index lookups return a header + an ObjectInfo locator;
// payload resolution reads inline data (or presigns a blob).
type LakeEventService struct {
	svc       *Service
	presigner eventrepo.Presigner
	bucket    string // parquet bucket for presigning
}

// NewLakeEventService constructs a LakeEventService backed by svc (which must
// have the DuckLake catalog attached as schema "lake"). presigner and bucket
// are used to presign blob payloads stored in S3; both may be nil/empty when
// large-payload blobs are not expected.
func NewLakeEventService(svc *Service, presigner eventrepo.Presigner, bucket string) *LakeEventService {
	return &LakeEventService{svc: svc, presigner: presigner, bucket: bucket}
}

var _ eventrepo.EventService = (*LakeEventService)(nil)

// queryLakeRaw returns at most limit events matching filter, newest first,
// deduped on the header key. When filter.ExcludeVoided is set, tombstones
// (voids_id != '') and events referenced by a tombstone are excluded.
func (l *LakeEventService) queryLakeRaw(ctx context.Context, filter RawFilter, limit int) ([]cloudevent.StoredEvent, error) {
	where, args := whereClauseQ(filter, "e.")
	voiding := ""
	if filter.ExcludeVoided {
		// Exclude tombstones themselves (voids_id != '') and events whose id is
		// referenced by a tombstone's voids_id for the same subject.
		voiding = fmt.Sprintf(
			" AND (e.voids_id IS NULL OR e.voids_id = '')"+
				" AND NOT EXISTS (SELECT 1 FROM %s t WHERE t.subject = e.subject AND t.voids_id = e.id)",
			lakeRawEvents)
	}
	q := fmt.Sprintf(
		"SELECT %s FROM %s e WHERE %s%s ORDER BY e.time DESC LIMIT %d",
		lakeRawColumns, lakeRawEvents, where, voiding, limit*2)

	rows, err := l.svc.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("querying lake raw_events: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	seen := map[string]struct{}{}
	var events []cloudevent.StoredEvent
	for rows.Next() && len(events) < limit {
		ev, err := scanStoredEvent(rows)
		if err != nil {
			return nil, err
		}
		key := ev.Key()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// ListIndexesAdvanced returns index entries (header + ObjectInfo locator) for
// events matching opts, newest first, capped at limit.
func (l *LakeEventService) ListIndexesAdvanced(ctx context.Context, limit int, opts *grpc.AdvancedSearchOptions) ([]cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	if limit <= 0 {
		limit = 1
	}
	f, err := filterFromAdvanced(opts)
	if err != nil {
		return nil, err
	}
	evs, err := l.queryLakeRaw(ctx, f, limit)
	if err != nil {
		return nil, err
	}
	out := make([]cloudevent.CloudEvent[eventrepo.ObjectInfo], len(evs))
	for i, e := range evs {
		out[i] = toIndex(e)
	}
	return out, nil
}

// GetLatestIndexAdvanced returns the single newest index entry matching opts,
// or ErrNotFound when no events exist.
func (l *LakeEventService) GetLatestIndexAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	list, err := l.ListIndexesAdvanced(ctx, 1, opts)
	if err != nil {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, err
	}
	if len(list) == 0 {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, ErrNotFound
	}
	return list[0], nil
}

// ListIndexes is the SearchOptions variant; it converts to AdvancedSearchOptions
// and delegates.
func (l *LakeEventService) ListIndexes(ctx context.Context, limit int, opts *grpc.SearchOptions) ([]cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return l.ListIndexesAdvanced(ctx, limit, toAdvanced(opts))
}

// GetLatestIndex is the SearchOptions variant.
func (l *LakeEventService) GetLatestIndex(ctx context.Context, opts *grpc.SearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	return l.GetLatestIndexAdvanced(ctx, toAdvanced(opts))
}

// GetCloudEventTypeSummariesAdvanced returns per-type counts and time ranges
// matching opts. Voided events are excluded (ExcludeVoided always true here).
func (l *LakeEventService) GetCloudEventTypeSummariesAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) ([]eventrepo.CloudEventTypeSummary, error) {
	f, err := filterFromAdvanced(opts)
	if err != nil {
		return nil, err
	}
	// Build base WHERE with unqualified columns for the aggregate query.
	where, args := whereClauseQ(f, "")
	// Exclude tombstones and voided events in the aggregate.
	q := fmt.Sprintf(`SELECT type, count(*) AS cnt, min(time) AS first_seen, max(time) AS last_seen`+
		` FROM %s`+
		` WHERE %s`+
		`   AND (voids_id IS NULL OR voids_id = '')`+
		`   AND NOT EXISTS (SELECT 1 FROM %s t WHERE t.subject = %s.subject AND t.voids_id = %s.id)`+
		` GROUP BY type ORDER BY type`,
		lakeRawEvents, where, lakeRawEvents, lakeRawEvents, lakeRawEvents)

	rows, err := l.svc.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("lake type summaries: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var out []eventrepo.CloudEventTypeSummary
	for rows.Next() {
		var s eventrepo.CloudEventTypeSummary
		if err := rows.Scan(&s.Type, &s.Count, &s.FirstSeen, &s.LastSeen); err != nil {
			return nil, fmt.Errorf("scanning type summary: %w", err)
		}
		s.FirstSeen = s.FirstSeen.UTC()
		s.LastSeen = s.LastSeen.UTC()
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []eventrepo.CloudEventTypeSummary{}
	}
	return out, nil
}

// GetCloudEventFromIndex re-reads the event row by (subject, id) and returns
// the inline data payload. Blob events (data_index_key set) are also returned
// inline when the row carries data; for pure blob references the caller should
// use PresignBlobURL.
func (l *LakeEventService) GetCloudEventFromIndex(ctx context.Context, index *cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) (cloudevent.RawEvent, error) {
	evs, err := l.queryLakeRaw(ctx, RawFilter{
		Subject:       index.Subject,
		IDs:           []string{index.ID},
		ExcludeVoided: true,
	}, 1)
	if err != nil {
		return cloudevent.RawEvent{}, err
	}
	if len(evs) == 0 {
		return cloudevent.RawEvent{}, ErrNotFound
	}
	return toRawEvent(evs[0]), nil
}

// ListCloudEventsFromIndexes fetches the payload for each index entry.
func (l *LakeEventService) ListCloudEventsFromIndexes(ctx context.Context, indexes []cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) ([]cloudevent.RawEvent, error) {
	out := make([]cloudevent.RawEvent, 0, len(indexes))
	for i := range indexes {
		ev, err := l.GetCloudEventFromIndex(ctx, &indexes[i], "")
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}

// PresignBlobURL returns a short-lived presigned GET URL for the given S3 key.
func (l *LakeEventService) PresignBlobURL(ctx context.Context, key string) (string, error) {
	if l.presigner == nil {
		return "", fmt.Errorf("presigner not configured")
	}
	if l.bucket == "" {
		return "", fmt.Errorf("bucket not configured")
	}
	req, err := l.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(presignTTL))
	if err != nil {
		return "", fmt.Errorf("presign %s/%s: %w", l.bucket, key, err)
	}
	return req.URL, nil
}

// --- Helpers ---

// filterFromAdvanced translates gRPC AdvancedSearchOptions to a RawFilter,
// mirroring eventrepo.AdvancedSearchOptionsToQueryMod field-for-field.
// ExcludeVoided is always set to true (fetch path hides tombstones).
//
// Or clauses (StringFilterOption.Or / ArrayFilterOption.Or) are not yet
// supported. When present, errOrClauseUnsupported is returned so callers get a
// clear failure rather than silently over-returning (which would break
// shadow-mode correctness checks).
func filterFromAdvanced(opts *grpc.AdvancedSearchOptions) (RawFilter, error) {
	f := RawFilter{ExcludeVoided: true}
	if opts == nil {
		return f, nil
	}

	// Subject — multi-value IN supported; Or returns error.
	if s := opts.GetSubject(); s != nil {
		if len(s.GetOr()) > 0 {
			return RawFilter{}, errOrClauseUnsupported
		}
		f.Subjects = s.GetIn()
	}

	// String fields: In + NotIn; Or → error.
	applyString := func(opt *grpc.StringFilterOption, in, notIn *[]string) error {
		if opt == nil {
			return nil
		}
		if len(opt.GetOr()) > 0 {
			return errOrClauseUnsupported
		}
		*in = opt.GetIn()
		*notIn = opt.GetNotIn()
		return nil
	}

	if err := applyString(opts.GetType(), &f.Types, &f.TypesNotIn); err != nil {
		return RawFilter{}, err
	}
	if err := applyString(opts.GetSource(), &f.Sources, &f.SourcesNotIn); err != nil {
		return RawFilter{}, err
	}
	if err := applyString(opts.GetProducer(), &f.Producers, &f.ProducersNotIn); err != nil {
		return RawFilter{}, err
	}
	if err := applyString(opts.GetId(), &f.IDs, &f.IDsNotIn); err != nil {
		return RawFilter{}, err
	}
	if err := applyString(opts.GetDataVersion(), &f.DataVersions, &f.DataVersionsNotIn); err != nil {
		return RawFilter{}, err
	}
	if err := applyString(opts.GetExtras(), &f.Extras, &f.ExtrasNotIn); err != nil {
		return RawFilter{}, err
	}

	// Tags: all four array operators; Or → error.
	if t := opts.GetTags(); t != nil {
		if len(t.GetOr()) > 0 {
			return RawFilter{}, errOrClauseUnsupported
		}
		f.Tags = t.GetContainsAny()
		f.TagsAll = t.GetContainsAll()
		f.TagsNotContainAny = t.GetNotContainsAny()
		f.TagsNotContainAll = t.GetNotContainsAll()
	}

	if opts.GetAfter() != nil {
		f.After = opts.GetAfter().AsTime()
	}
	if opts.GetBefore() != nil {
		f.Before = opts.GetBefore().AsTime()
	}
	return f, nil
}

// toAdvanced converts basic SearchOptions to AdvancedSearchOptions,
// mirroring eventrepo.convertSearchOptionsToAdvanced.
func toAdvanced(opts *grpc.SearchOptions) *grpc.AdvancedSearchOptions {
	if opts == nil {
		return nil
	}
	advanced := &grpc.AdvancedSearchOptions{
		After:        opts.GetAfter(),
		Before:       opts.GetBefore(),
		TimestampAsc: opts.GetTimestampAsc(),
	}
	if opts.GetType() != nil {
		advanced.Type = &grpc.StringFilterOption{In: []string{opts.GetType().GetValue()}}
	}
	if opts.GetDataVersion() != nil {
		advanced.DataVersion = &grpc.StringFilterOption{In: []string{opts.GetDataVersion().GetValue()}}
	}
	if opts.GetSubject() != nil {
		advanced.Subject = &grpc.StringFilterOption{In: []string{opts.GetSubject().GetValue()}}
	}
	if opts.GetSource() != nil {
		advanced.Source = &grpc.StringFilterOption{In: []string{opts.GetSource().GetValue()}}
	}
	if opts.GetProducer() != nil {
		advanced.Producer = &grpc.StringFilterOption{In: []string{opts.GetProducer().GetValue()}}
	}
	if opts.GetExtras() != nil {
		advanced.Extras = &grpc.StringFilterOption{In: []string{opts.GetExtras().GetValue()}}
	}
	if opts.GetId() != nil {
		advanced.Id = &grpc.StringFilterOption{In: []string{opts.GetId().GetValue()}}
	}
	return advanced
}

// toIndex builds a CloudEvent[ObjectInfo] index entry from a StoredEvent.
// ObjectInfo.Key is set to the blob key (data_index_key) when the payload is
// a blob reference (starts with BlobKeyPrefix), or a lake-scheme locator
// otherwise (inline data is read back via GetCloudEventFromIndex).
func toIndex(ev cloudevent.StoredEvent) cloudevent.CloudEvent[eventrepo.ObjectInfo] {
	key := ev.DataIndexKey
	if key == "" {
		// Inline data: encode a lake locator so GetCloudEventFromIndex can re-fetch.
		key = "lake://" + ev.Subject + "/" + ev.ID
	}
	return cloudevent.CloudEvent[eventrepo.ObjectInfo]{
		CloudEventHeader: ev.CloudEventHeader,
		Data:             eventrepo.ObjectInfo{Key: key},
	}
}

// toRawEvent converts a StoredEvent to a RawEvent (the payload type).
func toRawEvent(ev cloudevent.StoredEvent) cloudevent.RawEvent {
	return cloudevent.RawEvent{
		CloudEventHeader: ev.CloudEventHeader,
		Data:             ev.Data,
		DataBase64:       ev.DataBase64,
	}
}

// presignTTL is the lifetime of generated presigned S3 URLs (matches eventrepo).
const presignTTL = 15 * time.Minute
