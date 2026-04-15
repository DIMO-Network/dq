package grpc

import (
	"encoding/json"

	"github.com/DIMO-Network/cloudevent"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// emptyTags is a shared empty slice returned by TagsOrEmpty to avoid per-call allocation.
var emptyTags = []string{}

// TagsOrEmpty returns the slice if non-nil and non-empty, otherwise a non-nil empty slice.
// Ensures API always returns an array for tags, never null.
func TagsOrEmpty(tags []string) []string {
	if len(tags) > 0 {
		return tags
	}
	return emptyTags
}

// AsRawCloudEvent converts the CloudEvent to a cloudevent.RawEvent.
func (c *CloudEvent) AsRawCloudEvent() cloudevent.RawEvent {
	return cloudevent.RawEvent{
		CloudEventHeader: c.GetHeader().AsCloudEventHeader(),
		Data:             c.GetData(),
	}
}

// AsCloudEventHeader converts the CloudEventHeader to a cloudevent.CloudEventHeader.
func (c *CloudEventHeader) AsCloudEventHeader() cloudevent.CloudEventHeader {
	if c == nil {
		return cloudevent.CloudEventHeader{}
	}
	extras := make(map[string]any, len(c.GetExtras()))
	for k, v := range c.GetExtras() {
		var val any
		if err := json.Unmarshal(v, &val); err != nil {
			val = v
		}
		extras[k] = val
	}
	return cloudevent.CloudEventHeader{
		ID:              c.GetId(),
		Source:          c.GetSource(),
		Producer:        c.GetProducer(),
		SpecVersion:     c.GetSpecVersion(),
		Subject:         c.GetSubject(),
		Time:            c.GetTime().AsTime(),
		Type:            c.GetType(),
		DataContentType: c.GetDataContentType(),
		DataSchema:      c.GetDataSchema(),
		DataVersion:     c.GetDataVersion(),
		Extras:          extras,
		Signature:       c.GetSignature(),
		RawEventID:      c.GetRawEventId(),
		Tags:            TagsOrEmpty(c.GetTags()),
	}
}

// CloudEventHeaderToProto converts a cloudevent.CloudEventHeader to a grpc.CloudEventHeader.
func CloudEventHeaderToProto(event *cloudevent.CloudEventHeader) *CloudEventHeader {
	if event == nil {
		return nil
	}
	extras := make(map[string][]byte)
	for k, v := range event.Extras {
		v, err := json.Marshal(v)
		if err != nil {
			// Skip the extra if it can't be marshaled
			continue
		}
		extras[k] = v
	}
	return &CloudEventHeader{
		Id:              event.ID,
		Source:          event.Source,
		Producer:        event.Producer,
		Subject:         event.Subject,
		SpecVersion:     event.SpecVersion,
		Time:            timestamppb.New(event.Time),
		Type:            event.Type,
		DataContentType: event.DataContentType,
		DataSchema:      event.DataSchema,
		DataVersion:     event.DataVersion,
		Extras:          extras,
		Signature:       event.Signature,
		RawEventId:      event.RawEventID,
		Tags:            TagsOrEmpty(event.Tags),
	}
}

// CloudEventToProto converts a cloudevent.RawEvent to a grpc.CloudEvent.
func CloudEventToProto(event cloudevent.RawEvent) *CloudEvent {
	return &CloudEvent{
		Header: CloudEventHeaderToProto(&event.CloudEventHeader),
		Data:   event.Data,
	}
}
