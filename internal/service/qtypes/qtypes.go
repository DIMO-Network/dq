// Package qtypes holds the backend-agnostic query result and argument types
// shared by the query layer (the DuckLake backend) and its repository consumers.
// These were originally defined in the ClickHouse service; they outlived it and
// now live in a neutral package so no consumer depends on a specific backend.
package qtypes

import (
	"fmt"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// FieldType indicates the type of values in an aggregation.
type FieldType uint8

const (
	// FloatType is the type for rows with numeric values that are in the VSS spec.
	FloatType FieldType = 1
	// StringType is the type for rows with string values.
	StringType FieldType = 2
	// LocType is the type for rows with location values.
	LocType FieldType = 3
)

// Scan implements sql.Scanner so a driver-returned uint8 maps to a FieldType.
func (t *FieldType) Scan(value any) error {
	w, ok := value.(uint8)
	if !ok {
		return fmt.Errorf("expected value of type uint8, but got type %T", value)
	}
	if w < uint8(FloatType) || w > uint8(LocType) {
		return fmt.Errorf("invalid value %d for field type", w)
	}
	*t = FieldType(w)
	return nil
}

// AggSignal is one aggregation result row for a time bucket.
type AggSignal struct {
	// SignalType describes the type of values in the aggregation:
	// float, string, or location.
	SignalType FieldType
	// SignalIndex is an identifier for the aggregation within its SignalType
	// (an index into the corresponding argument array).
	SignalIndex uint16
	// Timestamp is the timestamp for the bucket, the leftmost point.
	Timestamp time.Time
	// ValueNumber is the value for this row if it is of float or approximate
	// location type.
	ValueNumber float64
	// ValueString is the value for this row if it is of string type.
	ValueString string
	// ValueLocation is the value for this row if it is of location type.
	ValueLocation vss.Location
}

// AggSignalForRange is AggSignal with a segment index (from GetAggregatedSignalsForRanges).
type AggSignalForRange struct {
	SegIndex      int
	SignalType    FieldType
	SignalIndex   uint16
	ValueNumber   float64
	ValueString   string
	ValueLocation vss.Location
}

// EventCount is the count of events by name in a time range.
type EventCount struct {
	Name  string
	Count int
}

// EventCountForRange is event count by name for one segment index (from GetEventCountsForRanges).
type EventCountForRange struct {
	SegIndex int
	Name     string
	Count    int
}

// EventSummary is the per-event summary for a vehicle (all time): name, count, first/last seen.
type EventSummary struct {
	Name      string
	Count     uint64
	FirstSeen time.Time
	LastSeen  time.Time
}

// TimeRange is a [From, To) interval for batch event-count queries.
type TimeRange struct {
	From, To time.Time
}
