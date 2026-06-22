package duck

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

// errOrClauseUnsupported is returned when an advanced filter contains an Or
// clause that the lake path cannot translate to SQL. Returning an error rather
// than silently over-returning preserves correctness until Or is fully
// implemented.
var errOrClauseUnsupported = errors.New("lake fetch: Or clauses in advanced filter are not yet supported")

const lakeRawEvents = "lake.raw_events"

// defaultFetchScanWindow bounds a subject-less, id-less lake fetch as a DoS
// guard against scanning all of raw_events (CHD-34). It is NOT a parity bound:
// ClickHouse applies no lookback floor when the caller supplies no `after` (it
// just ORDER BY time DESC LIMIT n over its index), so a subject-scoped fetch
// must reach arbitrarily old events too (SR review #4). queryLakeRaw therefore
// applies this window only when neither an id nor a subject narrows the scan.
const defaultFetchScanWindow = 400 * 24 * time.Hour

// maxLakeQueryLimit caps the number of rows a single lake fetch query may
// return, matching ClickHouse eventrepo.maxQueryLimit. Without it an oversized
// caller-supplied limit (the gRPC layer passes it through unguarded) forces an
// unbounded scan plus a Go-side dedup map — a memory/latency DoS ClickHouse is
// immune to.
const maxLakeQueryLimit = 1000

// RawEventColumns is the raw_events SELECT projection in din's DDL column order
// (matching scanStoredEvent / scanRawEvent's scan order). Single source of truth
// for the fetch path, the legacy raw reader (raw.go), and the materializer
// change-feed reader (materializer/ducklake.go), which previously hand-kept three
// identical copies. "time" is quoted (a DuckDB keyword); the quoted form is valid
// in every projection context.
const RawEventColumns = `subject, "time", type, id, source, producer, ` +
	`data_content_type, data_version, extras, data, data_base64, data_index_key, voids_id`

// voidingClause builds the tombstone-exclusion predicate for raw_events: drop
// tombstones (voids_id set) and any event a same-subject tombstone voids. ref is
// the row alias/qualifier ("e" in a search, the table name in an aggregate).
// Single source of truth so the search and summary paths cannot drift (CHD-30).
func voidingClause(ref string) string {
	return fmt.Sprintf(
		" AND (%[1]s.voids_id IS NULL OR %[1]s.voids_id = '')"+
			" AND NOT EXISTS (SELECT 1 FROM %[2]s t WHERE t.subject = %[1]s.subject AND t.voids_id = %[1]s.id)",
		ref, lakeRawEvents)
}

// LakeEventService serves the eventrepo.EventService surface from
// lake.raw_events. Index lookups return a header + an ObjectInfo locator;
// payload resolution reads inline data (or presigns a blob).
type LakeEventService struct {
	svc       *Service
	getter    eventrepo.ObjectGetter // fetches blob payload bytes from S3 (gRPC path)
	presigner eventrepo.Presigner
	bucket    string // parquet/blob bucket for presigning and blob download
}

// NewLakeEventService constructs a LakeEventService backed by svc (which must
// have the DuckLake catalog attached as schema "lake"). getter downloads blob
// payloads for the gRPC fetch path; presigner and bucket are used to presign
// blob payloads stored in S3 for the GraphQL path. getter/presigner may be
// nil and bucket empty when large-payload blobs are not expected.
func NewLakeEventService(svc *Service, getter eventrepo.ObjectGetter, presigner eventrepo.Presigner, bucket string) *LakeEventService {
	return &LakeEventService{svc: svc, getter: getter, presigner: presigner, bucket: bucket}
}

var _ eventrepo.EventService = (*LakeEventService)(nil)

// queryLakeRaw returns at most limit events matching filter, deduped on the
// header key. When filter.ExcludeVoided is set, tombstones (voids_id != ”)
// and events referenced by a tombstone are excluded. ORDER BY direction
// matches ClickHouse eventrepo: DESC (newest-first) by default, ASC
// (oldest-first) when filter.TimestampAsc is true.
func (l *LakeEventService) queryLakeRaw(ctx context.Context, filter RawFilter, limit int) ([]cloudevent.StoredEvent, error) {
	// Apply the default lookback only when nothing else narrows the scan: not a
	// point lookup by id (GetCloudEventFromIndex), and not a subject-scoped
	// fetch. A subject prunes via raw_events' (subject, time) sort + zone maps to
	// that one vehicle's files, so latestCloudEvent / cloudEvents can reach
	// arbitrarily old events without a full scan — and must, to match ClickHouse,
	// which imposes no floor when the caller supplies no `after`. A dormant
	// vehicle whose newest event predates the window otherwise wrongly looked
	// empty (SR review #4). Only a subject-less, id-less search keeps the guard.
	if filter.After.IsZero() && len(filter.IDs) == 0 && filter.Subject == "" && len(filter.Subjects) == 0 {
		filter.After = time.Now().Add(-defaultFetchScanWindow)
	}
	where, args := whereClauseQ(filter, "e.")
	voiding := ""
	if filter.ExcludeVoided {
		voiding = voidingClause("e")
	}
	order := "DESC"
	if filter.TimestampAsc {
		order = "ASC"
	}
	// Collapse redelivery duplicates in SQL on the cloudevent header key
	// (subject, second-precision time, type, source, id) — date_trunc('second')
	// matches cloudevent.Key's RFC3339 second precision exactly. This replaces
	// the old "fetch limit*2, dedup in a Go map, truncate to limit" pattern,
	// which silently returned a short page when over half the window was
	// duplicates and materialized a dedup map per query (SR-11).
	q := fmt.Sprintf(
		"SELECT %s FROM %s e WHERE %s%s "+
			"QUALIFY ROW_NUMBER() OVER ("+
			"PARTITION BY e.subject, date_trunc('second', e.time), e.type, e.source, e.id "+
			"ORDER BY e.time) = 1 "+
			"ORDER BY e.time %s LIMIT %d",
		RawEventColumns, lakeRawEvents, where, voiding, order, limit)

	rows, err := l.svc.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("querying lake raw_events: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var events []cloudevent.StoredEvent
	for rows.Next() {
		ev, err := scanStoredEvent(rows)
		if err != nil {
			return nil, err
		}
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
	if limit > maxLakeQueryLimit {
		limit = maxLakeQueryLimit
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
// or ErrNotFound when no events exist. TimestampAsc is forced to false (DESC)
// so that "latest" always means newest, mirroring ClickHouse's
// GetLatestIndexAdvanced which explicitly sets TimestampAsc=false before
// delegating to ListIndexesAdvanced.
func (l *LakeEventService) GetLatestIndexAdvanced(ctx context.Context, opts *grpc.AdvancedSearchOptions) (cloudevent.CloudEvent[eventrepo.ObjectInfo], error) {
	// Force DESC regardless of any caller-supplied TimestampAsc so we always
	// retrieve the most-recent event, not the oldest.
	f, err := filterFromAdvanced(opts)
	if err != nil {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, err
	}
	f.TimestampAsc = false // always newest-first for "get latest"

	evs, err := l.queryLakeRaw(ctx, f, 1)
	if err != nil {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, err
	}
	if len(evs) == 0 {
		return cloudevent.CloudEvent[eventrepo.ObjectInfo]{}, ErrNotFound
	}
	return toIndex(evs[0]), nil
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
//
// This intentionally does NOT apply defaultFetchScanWindow: first_seen/last_seen
// are all-time min/max per type, so a lookback bound would corrupt them and
// diverge from ClickHouse's getEventSummariesQuery, which is also unbounded
// (SR-10). raw_events is partitioned by (type, day), so a type filter still
// prunes; an unfiltered summary is an inherent full scan by definition.
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
		` WHERE %s%s`+
		` GROUP BY type ORDER BY type`,
		lakeRawEvents, where, voidingClause(lakeRawEvents))

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
// its payload. Inline events return their stored data directly; blob events
// (data_index_key under BlobKeyPrefix, no inline data) have their raw bytes
// downloaded from S3 so the gRPC fetch path returns a non-empty payload. The
// GraphQL path presigns blobs via PresignBlobURL instead and never reaches the
// download here.
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
	return l.resolvePayload(ctx, evs[0])
}

// resolvePayload returns ev's payload, downloading the blob bytes from S3 when
// the event externalized its payload (data_index_key under BlobKeyPrefix with
// no inline data). din stores the raw decoded payload at the blob key, so the
// downloaded bytes go straight into Data — the gRPC proto (CloudEventToProto)
// carries only Data, so this is the field that reaches blob consumers.
func (l *LakeEventService) resolvePayload(ctx context.Context, ev cloudevent.StoredEvent) (cloudevent.RawEvent, error) {
	raw := toRawEvent(ev)
	if len(raw.Data) > 0 || raw.DataBase64 != "" {
		return raw, nil // inline payload present
	}
	if !strings.HasPrefix(ev.DataIndexKey, eventrepo.BlobKeyPrefix) {
		return raw, nil // no blob reference: genuinely empty payload
	}
	if l.getter == nil {
		return cloudevent.RawEvent{}, fmt.Errorf("blob payload %s requires an object store but none is configured", ev.DataIndexKey)
	}
	data, err := eventrepo.DownloadObject(ctx, l.getter, l.bucket, ev.DataIndexKey)
	if err != nil {
		return cloudevent.RawEvent{}, fmt.Errorf("fetch blob payload %s: %w", ev.DataIndexKey, err)
	}
	raw.Data = data
	return raw, nil
}

// indexBlobConcurrency bounds the parallel blob resolution in
// ListCloudEventsFromIndexes.
const indexBlobConcurrency = 25

// BatchesAllIndexes is true for the lake backend: ListCloudEventsFromIndexes
// groups any index by subject and issues one query per subject, so internal/fetch
// routes every index through it (a 1000-key single-subject fetch is one query,
// not 1000 per-key queries).
func (l *LakeEventService) BatchesAllIndexes() bool { return true }

// ListCloudEventsFromIndexes fetches the payload for each index entry, in input
// order. It groups the requested ids by subject and issues one query per
// subject instead of one per index (SR-4) — a list of N indexes for one vehicle
// is a single raw_events query, not N. Blob payloads are resolved concurrently
// (each needs its own bytes). A requested index with no row is ErrNotFound,
// matching the old per-index path.
func (l *LakeEventService) ListCloudEventsFromIndexes(ctx context.Context, indexes []cloudevent.CloudEvent[eventrepo.ObjectInfo], _ string) ([]cloudevent.RawEvent, error) {
	if len(indexes) == 0 {
		return nil, nil
	}
	idsBySubject := make(map[string][]string)
	for i := range indexes {
		s := indexes[i].Subject
		idsBySubject[s] = append(idsBySubject[s], indexes[i].ID)
	}

	type key struct{ subject, id string }
	found := make(map[key]cloudevent.StoredEvent, len(indexes))
	for subject, ids := range idsBySubject {
		evs, err := l.queryLakeRaw(ctx, RawFilter{Subject: subject, IDs: ids, ExcludeVoided: true}, len(ids))
		if err != nil {
			return nil, err
		}
		for _, ev := range evs {
			found[key{ev.Subject, ev.ID}] = ev
		}
	}

	// Order the matched rows; a missing index is ErrNotFound (before launching any
	// resolution, so no goroutine leaks on the error path).
	evs := make([]cloudevent.StoredEvent, len(indexes))
	for i := range indexes {
		ev, ok := found[key{indexes[i].Subject, indexes[i].ID}]
		if !ok {
			return nil, ErrNotFound
		}
		evs[i] = ev
	}

	out := make([]cloudevent.RawEvent, len(indexes))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(indexBlobConcurrency)
	for i := range evs {
		g.Go(func() error {
			raw, err := l.resolvePayload(gctx, evs[i])
			if err != nil {
				return err
			}
			out[i] = raw
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
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
	// Mirror ClickHouse eventrepo.ListIndexesAdvanced: ASC only when explicitly
	// true; unset (nil) or false → DESC (newest first).
	f.TimestampAsc = opts.GetTimestampAsc().GetValue()
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
