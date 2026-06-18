// Package fetch provides functions for fetching objects from the index service.
package fetch

import (
	"context"
	"errors"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const fetchers = 25

// ListCloudEventsFromIndexes fetches cloud events by splitting indexes into
// parquet refs (handled efficiently via eventrepo with reader caching) and
// legacy JSON refs (fetched concurrently with multi-bucket fallback).
// Both paths run concurrently.
func ListCloudEventsFromIndexes(ctx context.Context, evtSvc eventrepo.EventService, indexKeys []cloudevent.CloudEvent[eventrepo.ObjectInfo], buckets []string) ([]cloudevent.RawEvent, error) {
	if len(indexKeys) == 0 {
		return nil, nil
	}

	events := make([]cloudevent.RawEvent, len(indexKeys))

	// Split into parquet and JSON indexes, preserving original positions.
	var parquetIndexes []cloudevent.CloudEvent[eventrepo.ObjectInfo]
	var parquetPositions []int
	type jsonItem struct {
		pos  int
		info cloudevent.CloudEvent[eventrepo.ObjectInfo]
	}
	var jsonItems []jsonItem

	for i, idx := range indexKeys {
		if parquet.IsParquetRef(idx.Data.Key) {
			parquetIndexes = append(parquetIndexes, idx)
			parquetPositions = append(parquetPositions, i)
		} else {
			jsonItems = append(jsonItems, jsonItem{pos: i, info: idx})
		}
	}

	group, errCtx := errgroup.WithContext(ctx)

	// Parquet refs: delegate to eventrepo which shares S3ReaderAt and parquet
	// Reader across rows from the same file. Bucket is embedded in the parquet
	// ref or comes from config — the bucketName parameter is unused.
	if len(parquetIndexes) > 0 {
		group.Go(func() error {
			pqEvents, err := evtSvc.ListCloudEventsFromIndexes(errCtx, parquetIndexes, "")
			if err != nil {
				return fmt.Errorf("fetch parquet events: %w", err)
			}
			for i, ev := range pqEvents {
				events[parquetPositions[i]] = ev
			}
			return nil
		})
	}

	// JSON refs: concurrent fetch with multi-bucket fallback and dedup.
	if len(jsonItems) > 0 {
		group.Go(func() error {
			var sf singleflight.Group

			jsonGroup, jsonCtx := errgroup.WithContext(errCtx)
			jsonGroup.SetLimit(fetchers)

			for _, item := range jsonItems {
				jsonGroup.Go(func() error {
					v, err, _ := sf.Do(item.info.Data.Key, func() (any, error) {
						return GetCloudEventFromIndex(jsonCtx, evtSvc, &item.info, buckets)
					})
					if err != nil {
						return err
					}
					obj := v.(cloudevent.RawEvent)
					// Apply this index's header; shared result may have a different one.
					events[item.pos] = cloudevent.RawEvent{
						CloudEventHeader: item.info.CloudEventHeader,
						Data:             obj.Data,
						DataBase64:       obj.DataBase64,
					}
					return nil
				})
			}

			return jsonGroup.Wait()
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return events, nil
}

// GetCloudEventFromIndex gets an object from the index service by trying each
// bucket in order, returning the first successful result.
func GetCloudEventFromIndex(ctx context.Context, evtSvc eventrepo.EventService, indexKeys *cloudevent.CloudEvent[eventrepo.ObjectInfo], buckets []string) (cloudevent.RawEvent, error) {
	if len(buckets) == 0 {
		return cloudevent.RawEvent{}, fmt.Errorf("no buckets configured")
	}
	var lastErr error
	for _, bucket := range buckets {
		obj, err := evtSvc.GetCloudEventFromIndex(ctx, indexKeys, bucket)
		if err != nil {
			var notFound *types.NoSuchKey
			if errors.As(err, &notFound) {
				lastErr = err
				continue
			}
			return cloudevent.RawEvent{}, fmt.Errorf("failed to get object: %w", err)
		}
		return obj, nil
	}
	return cloudevent.RawEvent{}, fmt.Errorf("object not found in any bucket: %w", lastErr)
}
