package repositories

import (
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/graph/model"
)

// eventNamePattern matches exactly 2 dotted segments, e.g. "behavior.harshBraking".
var eventNamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9]*\.[a-zA-Z][a-zA-Z0-9]*$`)

// eventNamePrefixPattern matches a category with optional name segment, e.g. "behavior." or "behavior.harsh".
var eventNamePrefixPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9]*\.([a-zA-Z][a-zA-Z0-9]*)?$`)

// ValidationError is an error type for validation errors.
type ValidationError string

func (v ValidationError) Error() string { return "invalid argument: " + string(v) }

func validateAggSigArgs(args *model.AggregatedSignalArgs) error {
	if args == nil {
		return ValidationError("aggregated signal args not provided")
	}

	if args.FromTS.IsZero() {
		return ValidationError("from timestamp is zero")
	}
	if args.ToTS.IsZero() {
		return ValidationError("to timestamp is zero")
	}
	if args.FromTS.After(args.ToTS) {
		return ValidationError("from timestamp is after to timestamp")
	}

	if args.Interval < 1 {
		return ValidationError("interval is not a positive integer")
	}
	// Bound the GROUP-BY bucket cardinality: a tiny interval (microseconds) over a
	// wide window would otherwise materialize an unbounded number of buckets and
	// OOM the per-replica DuckDB. 100k buckets is far beyond any real dashboard.
	const maxAggBuckets = 100_000
	if windowMicros := args.ToTS.Sub(args.FromTS).Microseconds(); windowMicros > 0 && args.Interval < windowMicros/maxAggBuckets {
		return ValidationError("interval too small for the requested window (exceeds the aggregation bucket limit)")
	}

	if len(args.FloatArgs) > math.MaxUint16 {
		return ValidationError("too many float aggregations")
	}
	if len(args.StringArgs) > math.MaxUint16 {
		return ValidationError("too many string aggregations")
	}
	if len(args.LocationArgs) > math.MaxUint16 {
		return ValidationError("too many location aggregations")
	}

	for _, locArg := range args.LocationArgs {
		if fil := locArg.Filter; fil != nil {
			if len(fil.InPolygon) != 0 && len(fil.InPolygon) < 3 {
				return ValidationError("not enough points in geofence filter")
			}
			for _, pt := range fil.InPolygon {
				// Validate every vertex. A GraphQL Float! accepts the JSON strings
				// "NaN"/"Inf", and an unfinite or out-of-range coordinate would
				// otherwise be inlined into the ST_GeomFromText WKT and 500 at the DB
				// (or silently match nothing).
				if !isFilterLocationValid(pt) {
					return ValidationError("invalid geofence polygon vertex")
				}
			}

			if fil.InCircle != nil {
				if !isFilterLocationValid(fil.InCircle.Center) {
					return ValidationError("invalid circle filter location")
				}
				// Radius is inlined into the haversine SQL: a NaN/Inf radius 500s, a
				// negative one silently matches nothing.
				if r := fil.InCircle.Radius; math.IsNaN(r) || math.IsInf(r, 0) || r < 0 {
					return ValidationError("invalid circle filter radius")
				}
			}
		}
	}

	return validateSignalArgs(&args.SignalArgs)
}

// isFilterLocationValid reports whether loc is non-nil with finite, in-range
// coordinates. NaN/Inf fail the range comparisons (e.g. -90 <= NaN is false), so this
// also rejects the unfinite values a GraphQL Float! can carry.
func isFilterLocationValid(loc *model.FilterLocation) bool {
	return loc != nil && -90 <= loc.Latitude && loc.Latitude <= 90 && -180 <= loc.Longitude && loc.Longitude <= 180
}

func validateLatestSigArgs(args *model.LatestSignalsArgs) error {
	if args == nil {
		return ValidationError("latest signal args not provided")
	}
	return validateSignalArgs(&args.SignalArgs)
}

func validateSignalArgs(args *model.SignalArgs) error {
	if args == nil {
		return ValidationError("signal args not provided")
	}

	if args.Subject == "" {
		return ValidationError("subject is required")
	}

	return validateFilter(args.Filter)
}

func validateFilter(filter *model.SignalFilter) error {
	if filter == nil {
		return nil
	}
	if filter.Source != nil {
		if _, err := cloudevent.DecodeEthrDID(*filter.Source); err != nil {
			return ValidationError(fmt.Sprintf("source '%s' is not a valid ethr DID", *filter.Source))
		}
	}
	return nil
}

func validateEventArgs(did string, from, to time.Time, filter *model.EventFilter) error {
	if did == "" {
		return ValidationError("subject is required")
	}
	if from.IsZero() {
		return ValidationError("from timestamp is zero")
	}
	if to.IsZero() {
		return ValidationError("to timestamp is zero")
	}
	if from.After(to) {
		return ValidationError("from timestamp is after to timestamp")
	}
	if filter != nil {
		if err := validateEventNameFilter(filter.Name); err != nil {
			return err
		}
	}
	return nil
}

func validateEventNameFilter(filter *model.StringValueFilter) error {
	if filter == nil {
		return nil
	}
	if filter.Eq != nil {
		if !eventNamePattern.MatchString(*filter.Eq) {
			return ValidationError(fmt.Sprintf("event name '%s' does not match namespace pattern (e.g. 'behavior.harshBraking')", *filter.Eq))
		}
	}
	for _, name := range filter.In {
		if !eventNamePattern.MatchString(name) {
			return ValidationError(fmt.Sprintf("event name '%s' does not match namespace pattern (e.g. 'behavior.harshBraking')", name))
		}
	}
	if filter.StartsWith != nil {
		if !eventNamePrefixPattern.MatchString(*filter.StartsWith) {
			return ValidationError(fmt.Sprintf("event name prefix '%s' does not match namespace pattern (e.g. 'behavior.' or 'behavior.harsh')", *filter.StartsWith))
		}
	}
	for _, orFilter := range filter.Or {
		if err := validateEventNameFilter(orFilter); err != nil {
			return err
		}
	}
	return nil
}
