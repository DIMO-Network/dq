package model

import (
	"time"
)

const (
	// LastSeenField is the field name for the last seen timestamp.
	LastSeenField = "lastSeen"
	// ApproximateCoordinatesField is the field name for the approximate current location.
	// This is treated specially because there is no underlying ClickHouse table row carrying
	// this name.
	ApproximateCoordinatesField = "currentLocationApproximateCoordinates"
)

// SignalArgs is the base arguments for querying signals.
type SignalArgs struct {
	// Filter is an optional filter for the signals.
	Filter *SignalFilter
	// Subject is the vehicle DID (e.g. "did:erc721:137:0x...:1").
	Subject string
}

// LatestSignalsArgs is the arguments for querying the latest signals.
type LatestSignalsArgs struct {
	SignalArgs
	// SignalNames is the list of signal names to query.
	SignalNames map[string]struct{}
	// LocationSignalNames is the list of location signal names to query.
	LocationSignalNames map[string]struct{}
	// IncludeLastSeen is a flag to include a new signal for the last seen signal.
	IncludeLastSeen bool
}

// AggregatedSignalArgs is the arguments for querying aggregated signals.
type AggregatedSignalArgs struct {
	SignalArgs
	// FromTS is the start timestamp for the data range.
	FromTS time.Time
	// ToTS is the end timestamp for the data range.
	ToTS time.Time
	// Interval in which the data is aggregated in microseconds.
	Interval int64
	// FloatArgs represents arguments for each float signal.
	FloatArgs []FloatSignalArgs
	// StringArgs represents arguments for each string signal.
	StringArgs []StringSignalArgs
	// LocationArgs represents arguments for each location signal.
	LocationArgs []LocationSignalArgs
}

type LocationSignalArgs struct {
	Name   string
	Agg    LocationAggregation
	Alias  string
	Filter *SignalLocationFilter
}

// FloatSignalArgs is the arguments for querying a float signals.
type FloatSignalArgs struct {
	Name   string
	Agg    FloatAggregation
	Alias  string
	Filter *SignalFloatFilter
}

// StringSignalArgs is the arguments for querying a string signals.
type StringSignalArgs struct {
	Name  string
	Agg   StringAggregation
	Alias string
}
