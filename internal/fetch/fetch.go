// Package fetch resolves index references to cloud events via the lake backend.
package fetch

import (
	"context"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
)

// ListCloudEventsFromIndexes resolves the given indexes to cloud events. The lake
// backend groups them by subject and resolves the whole list in one query per
// subject; the bucket is taken from the parquet ref / config, so none is passed.
func ListCloudEventsFromIndexes(ctx context.Context, evtSvc eventrepo.EventService, indexKeys []cloudevent.CloudEvent[eventrepo.ObjectInfo]) ([]cloudevent.RawEvent, error) {
	if len(indexKeys) == 0 {
		return nil, nil
	}
	return evtSvc.ListCloudEventsFromIndexes(ctx, indexKeys)
}

// GetCloudEventFromIndex resolves a single index to a cloud event.
func GetCloudEventFromIndex(ctx context.Context, evtSvc eventrepo.EventService, indexKey *cloudevent.CloudEvent[eventrepo.ObjectInfo]) (cloudevent.RawEvent, error) {
	return evtSvc.GetCloudEventFromIndex(ctx, indexKey)
}
