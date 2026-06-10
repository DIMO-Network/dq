package materializer

// Port of github.com/DIMO-Network/model-garage/pkg/defaultmodule's signal
// and event conversion. See registry.go for why the package cannot be
// imported directly. Two deliberate deviations: partial signal decodes are
// wrapped in convert.ConversionError (so the salvage path is uniform), and
// decoded events do not get a fresh ksuid header ID — the header ID is
// never persisted; cloud_event_id comes from EventData.CloudEventID.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/schema"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// defaultSignalData is the wire shape of a default-module status payload.
type defaultSignalData struct {
	Signals []*defaultSignal `json:"signals"`
}

type defaultSignal struct {
	// Timestamp is when this data was collected. (format: RFC3339)
	Timestamp time.Time `json:"timestamp"`
	// Name is the name of the signal collected.
	Name string `json:"name"`
	// Value is the value of the signal collected.
	Value any `json:"value"`
}

// defaultEventsData is the wire shape of a default-module events payload.
type defaultEventsData struct {
	Events []defaultEvent `json:"events"`
}

type defaultEvent struct {
	Name       string    `json:"name"`
	Timestamp  time.Time `json:"timestamp"`
	DurationNs uint64    `json:"durationNs,omitempty"`
	Metadata   string    `json:"metadata,omitempty"`
	Tags       []string  `json:"tags"`
}

// defaultModule decodes the standard DIMO payload shape using the embedded
// VSS schema, like model-garage's defaultmodule.Module.
type defaultModule struct {
	once         sync.Once
	signalMap    map[string]*schema.SignalInfo
	eventNameMap map[string]*schema.EventNameInfo
	loadErr      error
}

func (m *defaultModule) load() error {
	m.once.Do(func() {
		definedSignals, err := schema.GetDefaultSignals()
		if err != nil {
			m.loadErr = fmt.Errorf("failed to load default signals: %w", err)
			return
		}
		m.signalMap = make(map[string]*schema.SignalInfo, len(definedSignals))
		for _, signal := range definedSignals {
			m.signalMap[signal.JSONName] = signal
		}

		eventNames, err := schema.GetDefaultEventNames()
		if err != nil {
			m.loadErr = fmt.Errorf("failed to load default event names: %w", err)
			return
		}
		m.eventNameMap = make(map[string]*schema.EventNameInfo, len(eventNames))
		for _, eventName := range eventNames {
			m.eventNameMap[eventName.Name] = eventName
		}
	})
	return m.loadErr
}

// SignalConvert converts a default CloudEvent to DIMO's vss signals.
func (m *defaultModule) SignalConvert(_ context.Context, event cloudevent.RawEvent) ([]vss.Signal, error) {
	if err := m.load(); err != nil {
		return nil, err
	}

	var sigData defaultSignalData
	if err := json.Unmarshal(event.Data, &sigData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal signal data: %w", err)
	}
	hdr := event.CloudEventHeader
	hdr.Type = cloudevent.TypeSignal

	var decodeErrs []error
	vssSignals := make([]vss.Signal, 0, len(sigData.Signals))
	for _, signal := range sigData.Signals {
		vssSig, err := defaultSignalToVSS(signal, m.signalMap)
		if err != nil {
			// We want to return decoded signals even if some fail.
			decodeErrs = append(decodeErrs, err)
			continue
		}
		vssSig.CloudEventHeader = hdr
		vssSig.Data.CloudEventID = event.ID
		vssSignals = append(vssSignals, vssSig)
	}
	if len(decodeErrs) > 0 {
		return nil, convert.ConversionError{
			DecodedSignals: vssSignals,
			Errors:         decodeErrs,
			Subject:        event.Subject,
			Source:         event.Source,
		}
	}
	return vssSignals, nil
}

func defaultSignalToVSS(signal *defaultSignal, signalMap map[string]*schema.SignalInfo) (vss.Signal, error) {
	signalInfo, ok := signalMap[signal.Name]
	if !ok {
		return vss.Signal{}, fmt.Errorf("signal %s is not a defined signal name", signal.Name)
	}
	if signal.Value == nil {
		return vss.Signal{}, fmt.Errorf("signal %s is missing a value", signal.Name)
	}
	vssSig := vss.Signal{
		Data: vss.SignalData{
			Timestamp: signal.Timestamp,
			Name:      signal.Name,
		},
	}
	switch signalInfo.BaseGoType {
	case "float64":
		num, ok := signal.Value.(float64)
		if ok {
			vssSig.Data.ValueNumber = num
		} else if str, ok := signal.Value.(string); ok {
			v, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return vss.Signal{}, fmt.Errorf("signal %s can not be converted to a float64: %w", signal.Name, err)
			}
			vssSig.Data.ValueNumber = v
		} else {
			return vss.Signal{}, fmt.Errorf("signal %s is not a float64", signal.Name)
		}
	case "string":
		str, ok := signal.Value.(string)
		if !ok {
			return vss.Signal{}, fmt.Errorf("signal %s is not a string", signal.Name)
		}
		vssSig.Data.ValueString = str
	case "vss.Location":
		m, ok := signal.Value.(map[string]any)
		if !ok {
			return vss.Signal{}, fmt.Errorf("signal %s is not a location object", signal.Name)
		}
		var loc vss.Location
		if v, exists := m["latitude"]; exists {
			loc.Latitude, ok = v.(float64)
			if !ok {
				return vss.Signal{}, fmt.Errorf("signal %s has a non-float64 latitude", signal.Name)
			}
		}
		if v, exists := m["longitude"]; exists {
			loc.Longitude, ok = v.(float64)
			if !ok {
				return vss.Signal{}, fmt.Errorf("signal %s has a non-float64 longitude", signal.Name)
			}
		}
		if v, exists := m["hdop"]; exists {
			loc.HDOP, ok = v.(float64)
			if !ok {
				return vss.Signal{}, fmt.Errorf("signal %s has a non-float64 hdop", signal.Name)
			}
		}
		vssSig.Data.ValueLocation = loc
	default:
		return vss.Signal{}, fmt.Errorf("signal %s has an unsupported base type %s", signal.Name, signalInfo.BaseGoType)
	}

	return vssSig, nil
}

// EventConvert converts a default CloudEvent to vehicle events.
func (m *defaultModule) EventConvert(_ context.Context, event cloudevent.RawEvent) ([]vss.Event, error) {
	if err := m.load(); err != nil {
		return nil, err
	}

	var eventsData defaultEventsData
	if err := json.Unmarshal(event.Data, &eventsData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal events data: %w", err)
	}

	vssEvents := make([]vss.Event, 0, len(eventsData.Events))
	var decodeErrs []error
	for _, ev := range eventsData.Events {
		if ev.Name == "" {
			decodeErrs = append(decodeErrs, fmt.Errorf("event.name is empty"))
			continue
		}
		if ev.Timestamp.IsZero() {
			decodeErrs = append(decodeErrs, fmt.Errorf("event.timestamp is zero for event.name %s", ev.Name))
			continue
		}
		if _, ok := m.eventNameMap[ev.Name]; !ok {
			decodeErrs = append(decodeErrs, fmt.Errorf("unknown event name: %s", ev.Name))
			continue
		}
		if len(ev.Metadata) > 0 && !json.Valid([]byte(ev.Metadata)) {
			decodeErrs = append(decodeErrs, fmt.Errorf("metadata for event.name %s, event.timestamp %s is not valid json", ev.Name, ev.Timestamp))
			continue
		}
		invalidTag := false
		for _, tag := range ev.Tags {
			if tag != strings.ToLower(tag) {
				decodeErrs = append(decodeErrs, fmt.Errorf("tag %q for event.name %s must be lowercase", tag, ev.Name))
				invalidTag = true
				break
			}
		}
		if invalidTag {
			continue
		}

		vssEvents = append(vssEvents, vss.Event{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: "1.0",
				Subject:     event.Subject,
				Source:      event.Source,
				Producer:    event.Producer,
				Time:        event.Time,
				Type:        cloudevent.TypeEvent,
				DataVersion: event.DataVersion,
			},
			Data: vss.EventData{
				Name:         ev.Name,
				Timestamp:    ev.Timestamp,
				DurationNs:   ev.DurationNs,
				Metadata:     ev.Metadata,
				CloudEventID: event.ID,
				Tags:         ensureTags(ev.Tags),
			},
		})
	}

	if len(decodeErrs) > 0 {
		return nil, convert.ConversionError{
			DecodedEvents: vssEvents,
			Errors:        decodeErrs,
			Subject:       event.Subject,
			Source:        event.Source,
		}
	}
	return vssEvents, nil
}

// ensureTags returns tags as-is if non-nil, or an empty slice otherwise.
func ensureTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}
