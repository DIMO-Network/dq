package materializer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/DIMO-Network/cloudevent"
	mgconvert "github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// pruneSignalName marks signals scheduled for removal during pruning and
// coordinate merging. Ported from din internal/decodestream.
const pruneSignalName = "___prune"

// allowableTimeSkew matches din internal/convert defaultSkew: timestamps
// further than this into the future are pruned.
const allowableTimeSkew = 5 * time.Minute

var (
	errFutureTimestamp = errors.New("future timestamp")
	errLatLongMismatch = errors.New("latitude and longitude mismatch")
	pruneSignal        = vss.Signal{Data: vss.SignalData{Name: pruneSignalName}}
)

// convertSignals decodes one raw status event into flattened signal rows.
// It mirrors dis/din signal handling exactly: convert via model-garage,
// salvage partial decodes from *convert.ConversionError, prune
// future/duplicate signals, and merge coordinate pairs into location
// signals. failed is 1 when the conversion reported errors.
func (r *Runner) convertSignals(ctx context.Context, rawEvent *cloudevent.RawEvent) (rows []SignalRow, failed int) {
	signals, err := convertToSignals(ctx, rawEvent.Source, *rawEvent)
	if err != nil {
		if convertErr, ok := asConversionError(err); ok {
			signals = convertErr.DecodedSignals
		}
		failed = 1
		r.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("signal conversion errors")
	}
	if len(signals) == 0 {
		return nil, failed
	}

	signals, pruneErr := pruneFutureAndDuplicateSignals(signals)
	signals, locErr := handleCoordinates(signals)
	if err := errors.Join(pruneErr, locErr); err != nil {
		r.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("signal pruning errors")
	}

	rows = make([]SignalRow, 0, len(signals))
	for i := range signals {
		rows = append(rows, signalRowFromVSS(rawEvent, &signals[i]))
	}
	return rows, failed
}

// convertEvents decodes one raw events cloudevent into flattened event
// rows, salvaging partial decodes the same way convertSignals does.
func (r *Runner) convertEvents(ctx context.Context, rawEvent *cloudevent.RawEvent) (rows []EventRow, failed int) {
	events, err := convertToEvents(ctx, rawEvent.Source, *rawEvent)
	if err != nil {
		if convertErr, ok := asConversionError(err); ok {
			events = convertErr.DecodedEvents
		}
		failed = 1
		r.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("event conversion errors")
	}

	rows = make([]EventRow, 0, len(events))
	for i := range events {
		rows = append(rows, eventRowFromVSS(rawEvent, &events[i]))
	}
	return rows, failed
}

// signalRowFromVSS flattens a vss.Signal into the decoded parquet schema.
// Header fields come from the raw event envelope, matching how dis
// persists decoded signals.
func signalRowFromVSS(rawEvent *cloudevent.RawEvent, sig *vss.Signal) SignalRow {
	cloudEventID := sig.Data.CloudEventID
	if cloudEventID == "" {
		cloudEventID = rawEvent.ID
	}
	return SignalRow{
		Subject:      rawEvent.Subject,
		Name:         sig.Data.Name,
		Timestamp:    sig.Data.Timestamp.UTC().Truncate(time.Microsecond),
		Source:       rawEvent.Source,
		Producer:     rawEvent.Producer,
		CloudEventID: cloudEventID,
		ValueNumber:  sig.Data.ValueNumber,
		ValueString:  sig.Data.ValueString,
		LocLat:       sig.Data.ValueLocation.Latitude,
		LocLon:       sig.Data.ValueLocation.Longitude,
		LocHDOP:      sig.Data.ValueLocation.HDOP,
		LocHeading:   sig.Data.ValueLocation.Heading,
	}
}

// eventRowFromVSS flattens a vss.Event into the decoded parquet schema
// with the exact 11 vss.EventToSlice columns.
func eventRowFromVSS(rawEvent *cloudevent.RawEvent, ev *vss.Event) EventRow {
	cloudEventID := ev.Data.CloudEventID
	if cloudEventID == "" {
		cloudEventID = rawEvent.ID
	}
	tags := ev.Data.Tags
	if tags == nil {
		tags = []string{}
	}
	return EventRow{
		Subject:      rawEvent.Subject,
		Source:       rawEvent.Source,
		Producer:     rawEvent.Producer,
		CloudEventID: cloudEventID,
		Type:         cmp.Or(ev.Type, cloudevent.TypeEvent),
		DataVersion:  cmp.Or(ev.DataVersion, rawEvent.DataVersion),
		Name:         ev.Data.Name,
		Timestamp:    ev.Data.Timestamp.UTC().Truncate(time.Microsecond),
		DurationNs:   ev.Data.DurationNs,
		Metadata:     ev.Data.Metadata,
		Tags:         tags,
	}
}

// asConversionError extracts a model-garage ConversionError carrying
// partial decodes. Modules return both pointer and value forms (the
// default module returns a value, ruptela et al. return pointers), so
// check for both.
func asConversionError(err error) (*mgconvert.ConversionError, bool) {
	var ptrErr *mgconvert.ConversionError
	if errors.As(err, &ptrErr) {
		return ptrErr, true
	}
	var valErr mgconvert.ConversionError
	if errors.As(err, &valErr) {
		return &valErr, true
	}
	return nil, false
}

// isFutureTimestamp is ported from din internal/convert.IsFutureTimestamp.
func isFutureTimestamp(ts time.Time) bool {
	return ts.After(time.Now().Add(allowableTimeSkew))
}

// pruneFutureAndDuplicateSignals is ported from din
// internal/decodestream (itself ported from dis signalconvert) so the
// decoded tables match what dis produced.
func pruneFutureAndDuplicateSignals(signals []vss.Signal) ([]vss.Signal, error) {
	var errs error
	slices.SortFunc(signals, func(a, b vss.Signal) int {
		return cmp.Or(a.Data.Timestamp.Compare(b.Data.Timestamp), cmp.Compare(a.Data.Name, b.Data.Name))
	})
	for i := range signals {
		signal := &signals[i]
		if isFutureTimestamp(signal.Data.Timestamp) {
			errs = errors.Join(errs, fmt.Errorf("%w, signal '%s' has timestamp: %v",
				errFutureTimestamp, signal.Data.Name, signal.Data.Timestamp))
			signals[i] = pruneSignal
			continue
		}
		if i < len(signals)-1 && signalEqual(signals[i], signals[i+1]) {
			signals[i] = pruneSignal
		}
	}

	var pruned []vss.Signal
	for _, signal := range signals {
		if signal.Data.Name != pruneSignalName {
			pruned = append(pruned, signal)
		}
	}
	return pruned, errs
}

func signalEqual(a, b vss.Signal) bool {
	return a.Data.Name == b.Data.Name && a.Data.Timestamp.Equal(b.Data.Timestamp)
}
