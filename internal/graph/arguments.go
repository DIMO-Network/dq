package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// aggregationArgsFromContext creates aggregated signals arguments from the context and provided arguments.
func aggregationArgsFromContext(ctx context.Context, did string, interval string, from time.Time, to time.Time, filter *model.SignalFilter) (*model.AggregatedSignalArgs, error) {
	intervalInt, err := getIntervalMicroseconds(interval)
	if err != nil {
		return nil, err
	}
	aggArgs := model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{
			Subject: did,
			Filter:  filter,
		},
		FromTS:   from,
		ToTS:     to,
		Interval: intervalInt,
	}

	fields := graphql.CollectFieldsCtx(ctx, nil)
	parentCtx := graphql.GetFieldContext(ctx)
	for _, field := range fields {
		if !isSignal(field) || !hasAggregations(field) {
			continue
		}
		child, err := parentCtx.Child(ctx, field)
		if err != nil {
			return nil, fmt.Errorf("failed to get child field: %w", err)
		}

		if err := addSignalAggregation(&aggArgs, child, child.Field.Name); err != nil {
			return nil, err
		}
	}
	return &aggArgs, nil
}

// addSignalAggregation gets the aggregation arguments from the child field and adds them to the aggregated signal arguments.
func addSignalAggregation(aggArgs *model.AggregatedSignalArgs, child *graphql.FieldContext, name string) error {
	agg := child.Args["agg"]
	alias := child.Field.Alias
	switch typedAgg := agg.(type) {
	case model.FloatAggregation:
		filter, _ := child.Args["filter"].(*model.SignalFloatFilter)
		aggArgs.FloatArgs = append(aggArgs.FloatArgs, model.FloatSignalArgs{
			Name:   name,
			Agg:    typedAgg,
			Alias:  alias,
			Filter: filter,
		})
	case model.StringAggregation:
		aggArgs.StringArgs = append(aggArgs.StringArgs, model.StringSignalArgs{
			Name:  name,
			Agg:   typedAgg,
			Alias: alias,
		})
	case model.LocationAggregation:
		var filter *model.SignalLocationFilter
		dbSignalName := name
		if name == model.ApproximateCoordinatesField {
			dbSignalName = vss.FieldCurrentLocationCoordinates
		} else {
			filter, _ = child.Args["filter"].(*model.SignalLocationFilter)
		}
		aggArgs.LocationArgs = append(aggArgs.LocationArgs, model.LocationSignalArgs{
			Name:   dbSignalName,
			Agg:    typedAgg,
			Alias:  alias,
			Filter: filter,
		})
	default:
		return fmt.Errorf("unknown aggregation type: %T", agg)
	}
	return nil
}

// latestArgsFromContext creates latest signals arguments from the context and provided arguments.
func latestArgsFromContext(ctx context.Context, did string, filter *model.SignalFilter) (*model.LatestSignalsArgs, error) {
	fields := graphql.CollectFieldsCtx(ctx, nil)
	latestArgs := model.LatestSignalsArgs{
		SignalArgs: model.SignalArgs{
			Subject: did,
			Filter:  filter,
		},
		SignalNames:         make(map[string]struct{}),
		LocationSignalNames: make(map[string]struct{}),
	}
	for _, field := range fields {
		if !isSignal(field) {
			if field.Name == model.LastSeenField {
				latestArgs.IncludeLastSeen = true
			}
			continue
		}

		if field.Name == model.ApproximateCoordinatesField {
			latestArgs.LocationSignalNames[vss.FieldCurrentLocationCoordinates] = struct{}{}
		} else if field.Definition.Type.Name() == "SignalLocation" {
			latestArgs.LocationSignalNames[field.Name] = struct{}{}
		} else {
			latestArgs.SignalNames[field.Name] = struct{}{}
		}
	}
	return &latestArgs, nil
}

// Field names of the LatestSignals type that select a signal by name.
const (
	latestNumberField   = "number"
	latestStringField   = "string"
	latestLocationField = "location"
)

// latestByNameArgsFromContext creates latest signals arguments from the number,
// string and location selections of a latestSignals query, so that every selected
// name is fetched at once. Names the caller may not read are left out; their field
// resolvers report the error.
func latestByNameArgsFromContext(ctx context.Context, repo *repositories.Repository, did string, filter *model.SignalFilter) (*model.LatestSignalsArgs, error) {
	latestArgs := model.LatestSignalsArgs{
		SignalArgs: model.SignalArgs{
			Subject: did,
			Filter:  filter,
		},
		SignalNames:         make(map[string]struct{}),
		LocationSignalNames: make(map[string]struct{}),
	}
	parentCtx := graphql.GetFieldContext(ctx)
	for _, field := range graphql.CollectFieldsCtx(ctx, nil) {
		switch field.Name {
		case model.LastSeenField:
			latestArgs.IncludeLastSeen = true
		case latestNumberField, latestStringField, latestLocationField:
			child, err := parentCtx.Child(ctx, field)
			if err != nil {
				return nil, fmt.Errorf("failed to get child field: %w", err)
			}
			name, _ := child.Args["name"].(string)
			if !canReadSignal(ctx, repo, did, name) {
				continue
			}
			if field.Name == latestLocationField {
				latestArgs.LocationSignalNames[storedLocationName(name)] = struct{}{}
			} else {
				latestArgs.SignalNames[name] = struct{}{}
			}
		}
	}
	return &latestArgs, nil
}

// getIntervalMicroseconds parses the interval string and returns the number
// of microseconds the interval contains.
//
// We use microseconds for sub-second timestamp precision.
func getIntervalMicroseconds(interval string) (int64, error) {
	dur, err := time.ParseDuration(interval)
	if err != nil {
		return 0, fmt.Errorf("failed parsing interval: %w", err)
	}
	return dur.Microseconds(), nil
}

// isSignal checks if the field has the isSignal directive.
func isSignal(field graphql.CollectedField) bool {
	return field.Definition.Directives.ForName("isSignal") != nil
}

// hasAggregations checks if the field has the hasAggregation directive.
func hasAggregations(field graphql.CollectedField) bool {
	return field.Definition.Directives.ForName("hasAggregation") != nil
}
