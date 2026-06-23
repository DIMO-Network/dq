package repositories

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/qtypes"
	"github.com/DIMO-Network/model-garage/pkg/schema"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/server-garage/pkg/gql/errorhandler"
	"github.com/uber/h3-go/v4"
)

const approximateLocationResolution = 6

var unixEpoch = time.Unix(0, 0).UTC()

// Backend is the signal/latest/summary/event query surface served by the
// DuckLake parse-on-read query layer (internal/service/duck). *duck.Queries
// satisfies it (see assertions in backend.go).
type Backend interface {
	GetAggregatedSignals(ctx context.Context, subject string, aggArgs *model.AggregatedSignalArgs) ([]*qtypes.AggSignal, error)
	GetAggregatedSignalsForRanges(ctx context.Context, subject string, ranges []qtypes.TimeRange, globalFrom, globalTo time.Time, floatArgs []model.FloatSignalArgs, locationArgs []model.LocationSignalArgs) ([]*qtypes.AggSignalForRange, error)
	GetLatestSignals(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, error)
	GetAllLatestSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]*vss.Signal, error)
	GetAvailableSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]string, error)
	GetSignalSummaries(ctx context.Context, subject string, filter *model.SignalFilter) ([]*model.SignalDataSummary, error)
	GetEvents(ctx context.Context, subject string, from, to time.Time, filter *model.EventFilter) ([]*vss.Event, error)
	GetEventCounts(ctx context.Context, subject string, from, to time.Time, eventNames []string) ([]*qtypes.EventCount, error)
	GetEventCountsForRanges(ctx context.Context, subject string, ranges []qtypes.TimeRange, eventNames []string) ([]*qtypes.EventCountForRange, error)
	GetEventSummaries(ctx context.Context, subject string) ([]*qtypes.EventSummary, error)
}

// SegmentsBackend is the segment-detection surface, implemented by the
// lake backend.
type SegmentsBackend interface {
	GetSegments(ctx context.Context, subject string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig) ([]*model.Segment, error)
}

// CHService is the full query surface the Repository depends on.
type CHService interface {
	Backend
	SegmentsBackend
}

// Repository is the base repository for all repositories.
type Repository struct {
	queryableSignals map[string]struct{}
	signalPrivileges map[string][]string
	chService        CHService
}

// NewRepository creates a new base repository.
func NewRepository(chService CHService) (*Repository, error) {
	definitions, err := schema.LoadDefinitionFile(strings.NewReader(schema.DefaultDefinitionsYAML()))
	if err != nil {
		return nil, fmt.Errorf("error reading definition file: %w", err)
	}
	queryableSignals := make(map[string]struct{}, len(definitions.FromName))
	signalPrivileges := make(map[string][]string, len(definitions.FromName))
	for vssName, info := range definitions.FromName {
		jsonName := schema.VSSToJSONName(vssName)
		queryableSignals[jsonName] = struct{}{}
		signalPrivileges[jsonName] = info.RequiredPrivileges
	}
	return &Repository{
		chService:        chService,
		queryableSignals: queryableSignals,
		signalPrivileges: signalPrivileges,
	}, nil
}

// RequiredPrivileges returns the privilege enum values required to read the named
// signal. The second return is false for signals not in the definitions file
// (e.g. derived signals like currentLocationApproximateCoordinates) — callers
// should fail closed in that case.
func (r *Repository) RequiredPrivileges(signalName string) ([]string, bool) {
	privs, ok := r.signalPrivileges[signalName]
	return privs, ok
}

// GetSignal returns the aggregated signals for the given DID, interval, from, to and filter.
func (r *Repository) GetSignal(ctx context.Context, aggArgs *model.AggregatedSignalArgs) ([]*model.SignalAggregations, error) {
	if err := validateAggSigArgs(aggArgs); err != nil {
		return nil, errorhandler.NewBadRequestError(ctx, err)
	}

	signals, err := r.chService.GetAggregatedSignals(ctx, aggArgs.Subject, aggArgs)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}

	var allAggs []*model.SignalAggregations
	var currAggs *model.SignalAggregations
	lastTS := time.Time{}

	for _, signal := range signals {
		if !lastTS.Equal(signal.Timestamp) {
			lastTS = signal.Timestamp
			currAggs = &model.SignalAggregations{
				Timestamp:      signal.Timestamp,
				ValueNumbers:   make(map[string]float64),
				ValueStrings:   make(map[string]string),
				ValueLocations: make(map[string]vss.Location),
			}
			allAggs = append(allAggs, currAggs)
		}

		switch signal.SignalType {
		case qtypes.FloatType:
			if len(aggArgs.FloatArgs) <= int(signal.SignalIndex) {
				return nil, fmt.Errorf("only %d float signal requests, but the query returned index %d", len(aggArgs.FloatArgs), signal.SignalIndex)
			}
			currAggs.ValueNumbers[aggArgs.FloatArgs[signal.SignalIndex].Alias] = signal.ValueNumber
		case qtypes.StringType:
			if len(aggArgs.StringArgs) <= int(signal.SignalIndex) {
				return nil, fmt.Errorf("only %d string signal requests, but the query returned index %d", len(aggArgs.StringArgs), signal.SignalIndex)
			}
			currAggs.ValueStrings[aggArgs.StringArgs[signal.SignalIndex].Alias] = signal.ValueString
		case qtypes.LocType:
			if len(aggArgs.LocationArgs) <= int(signal.SignalIndex) {
				return nil, fmt.Errorf("only %d location signal requests, but the query returned index %d", len(aggArgs.LocationArgs), signal.SignalIndex)
			}
			currAggs.ValueLocations[aggArgs.LocationArgs[signal.SignalIndex].Alias] = signal.ValueLocation
		default:
			return nil, fmt.Errorf("scanned a row with unrecognized type number %d", signal.SignalType)
		}
	}

	return allAggs, nil
}

// GetSignalLatest returns the latest signals for the given DID and filter.
func (r *Repository) GetSignalLatest(ctx context.Context, latestArgs *model.LatestSignalsArgs) (*model.SignalCollection, error) {
	if err := validateLatestSigArgs(latestArgs); err != nil {
		return nil, errorhandler.NewBadRequestError(ctx, err)
	}
	signals, err := r.chService.GetLatestSignals(ctx, latestArgs.Subject, latestArgs)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}
	coll := &model.SignalCollection{}
	for _, signal := range signals {
		if signal.Data.Name == model.LastSeenField && !signal.Data.Timestamp.Equal(unixEpoch) {
			coll.LastSeen = &signal.Data.Timestamp
			continue
		}
		model.SetCollectionField(coll, signal)
	}
	setApproximateLocationInCollection(coll)
	return coll, nil
}

// GetAvailableSignals returns the available signals for the given DID and filter.
func (r *Repository) GetAvailableSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]string, error) {
	allSignals, err := r.chService.GetAvailableSignals(ctx, subject, filter)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}
	var retSignals []string
	for _, signal := range allSignals {
		if _, ok := r.queryableSignals[signal]; ok {
			retSignals = append(retSignals, signal)
		}
	}
	return retSignals, nil
}

// GetDataSummary returns the signal and event metadata for the given DID and filter.
// foldTimeRange widens [minTime, maxTime] to include first and last.
func foldTimeRange(minTime, maxTime, first, last time.Time) (time.Time, time.Time) {
	if first.Before(minTime) {
		minTime = first
	}
	if last.After(maxTime) {
		maxTime = last
	}
	return minTime, maxTime
}

func (r *Repository) GetDataSummary(ctx context.Context, subject string, filter *model.SignalFilter) (*model.DataSummary, error) {
	signalDataSummary, err := r.chService.GetSignalSummaries(ctx, subject, filter)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}
	eventSummaries, err := r.chService.GetEventSummaries(ctx, subject)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}
	eventDataSummary := make([]*model.EventDataSummary, len(eventSummaries))
	for i, es := range eventSummaries {
		eventDataSummary[i] = &model.EventDataSummary{
			Name:           es.Name,
			NumberOfEvents: es.Count,
			FirstSeen:      es.FirstSeen,
			LastSeen:       es.LastSeen,
		}
	}
	totalCount := uint64(0)
	minTimestamp := time.Now().UTC()
	maxTimestamp := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	availableSignals := make([]string, len(signalDataSummary))
	for i, metadata := range signalDataSummary {
		availableSignals[i] = metadata.Name
		totalCount += metadata.NumberOfSignals
		minTimestamp, maxTimestamp = foldTimeRange(minTimestamp, maxTimestamp, metadata.FirstSeen, metadata.LastSeen)
	}
	for _, es := range eventSummaries {
		minTimestamp, maxTimestamp = foldTimeRange(minTimestamp, maxTimestamp, es.FirstSeen, es.LastSeen)
	}
	return &model.DataSummary{
		NumberOfSignals:   totalCount,
		FirstSeen:         minTimestamp,
		LastSeen:          maxTimestamp,
		AvailableSignals:  availableSignals,
		SignalDataSummary: signalDataSummary,
		EventDataSummary:  eventDataSummary,
	}, nil
}

// GetEvents returns the events for the given DID, from, to and filter.
func (r *Repository) GetEvents(ctx context.Context, did string, from, to time.Time, filter *model.EventFilter) ([]*model.Event, error) {
	if err := validateEventArgs(did, from, to, filter); err != nil {
		return nil, errorhandler.NewBadRequestError(ctx, err)
	}
	allEvents, err := r.chService.GetEvents(ctx, did, from, to, filter)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}
	retEvents := make([]*model.Event, len(allEvents))
	for i, event := range allEvents {
		retEvents[i] = &model.Event{
			Timestamp:  event.Data.Timestamp,
			Name:       event.Data.Name,
			Source:     event.Source,
			DurationNs: int(event.Data.DurationNs),
		}
		if event.Data.Metadata != "" {
			retEvents[i].Metadata = &event.Data.Metadata
		}
	}
	return retEvents, nil
}

// GetSignalSnapshot returns the latest value for every available signal for the given subject.
func (r *Repository) GetSignalSnapshot(ctx context.Context, subject string, filter *model.SignalFilter) (*model.SignalsSnapshotResponse, error) {
	signals, err := r.chService.GetAllLatestSignals(ctx, subject, filter)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}
	resp := &model.SignalsSnapshotResponse{Signals: []*model.LatestSignal{}}
	var rawLocationSignal *vss.Signal
	for _, signal := range signals {
		if signal.Data.Name == model.LastSeenField && !signal.Data.Timestamp.Equal(unixEpoch) {
			resp.LastSeen = &signal.Data.Timestamp
			continue
		}
		if _, ok := r.queryableSignals[signal.Data.Name]; !ok {
			continue
		}
		resp.Signals = append(resp.Signals, signalToLatestSignal(signal))
		if signal.Data.Name == vss.FieldCurrentLocationCoordinates {
			rawLocationSignal = signal
		}
	}
	if rawLocationSignal != nil {
		loc := rawLocationSignal.Data.ValueLocation
		if approx := GetApproximateLoc(loc.Latitude, loc.Longitude); approx != nil {
			resp.Signals = append(resp.Signals, &model.LatestSignal{
				Name:      model.ApproximateCoordinatesField,
				Timestamp: rawLocationSignal.Data.Timestamp,
				ValueLocation: &model.Location{
					Latitude:  approx.Lat,
					Longitude: approx.Lng,
					Hdop:      loc.HDOP,
				},
			})
		}
	}
	return resp, nil
}

func signalToLatestSignal(signal *vss.Signal) *model.LatestSignal {
	ls := &model.LatestSignal{
		Name:      signal.Data.Name,
		Timestamp: signal.Data.Timestamp,
	}
	loc := signal.Data.ValueLocation
	if loc.Latitude != 0 || loc.Longitude != 0 {
		ls.ValueLocation = &model.Location{
			Latitude:  loc.Latitude,
			Longitude: loc.Longitude,
			Hdop:      loc.HDOP,
		}
	} else if signal.Data.ValueString != "" {
		s := signal.Data.ValueString
		ls.ValueString = &s
	} else {
		n := signal.Data.ValueNumber
		ls.ValueNumber = &n
	}
	return ls
}

// handleDBError logs the error and returns a generic error message.
func handleDBError(ctx context.Context, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errorhandler.NewBadRequestErrorWithMsg(ctx, err, "request exceeded or is estimated to exceed the maximum execution time")
	}
	return errorhandler.NewInternalErrorWithMsg(ctx, err, "failed to query db")
}

// GetApproximateLoc returns the approximate location for the given latitude and longitude.
func GetApproximateLoc(lat, long float64) *h3.LatLng {
	h3LatLng := h3.NewLatLng(lat, long)
	cell, err := h3.LatLngToCell(h3LatLng, approximateLocationResolution)
	if err != nil {
		return nil
	}
	latLong, err := h3.CellToLatLng(cell)
	if err != nil {
		return nil
	}
	return &latLong
}

func setApproximateLocationInCollection(coll *model.SignalCollection) {
	if coll == nil || coll.CurrentLocationCoordinates == nil {
		return
	}
	loc := coll.CurrentLocationCoordinates
	latLong := GetApproximateLoc(loc.Value.Latitude, loc.Value.Longitude)
	if latLong == nil {
		return
	}
	coll.CurrentLocationApproximateCoordinates = &model.SignalLocation{
		Timestamp: loc.Timestamp,
		Value: &model.Location{
			Latitude:  latLong.Lat,
			Longitude: latLong.Lng,
			Hdop:      loc.Value.Hdop,
		},
	}
}
