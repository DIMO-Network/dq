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
func aggregationArgsFromContext(ctx context.Context, repo *repositories.Repository, did string, interval string, from time.Time, to time.Time, filter *model.SignalFilter) (*model.AggregatedSignalArgs, error) {
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

	tok := tokenFromCtx(ctx)
	fields := graphql.CollectFieldsCtx(ctx, nil)
	parentCtx := graphql.GetFieldContext(ctx)
	for _, field := range fields {
		if !isSignal(field) || !hasAggregations(field) {
			continue
		}
		// Possession is checked by the field's privilege directive; this rejects
		// requested ranges outside a HELD scoped permission's data window.
		// Rejection — not silent clamping — because an aggregate computed over
		// a narrower range than requested would be mislabeled as covering the
		// full range.
		if hasScopedPermissions(tok) && !signalRangeWithinWindows(repo, field.Name, tok, from, to) {
			if desc := signalWindowDescription(repo, field.Name, tok); desc != "" {
				return nil, fmt.Errorf("unauthorized: requested range for signal %s is outside the token's data window: %s", field.Name, desc)
			}
			return nil, fmt.Errorf("unauthorized: token does not allow signal %s over the requested range", field.Name)
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
func latestArgsFromContext(ctx context.Context, repo *repositories.Repository, did string, filter *model.SignalFilter) (*model.LatestSignalsArgs, error) {
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
	if tok := tokenFromCtx(ctx); hasScopedPermissions(tok) {
		// Latest values are point queries: rather than rejecting, they are
		// evaluated under the window — a value recorded outside it is withheld,
		// which is indistinguishable from the vehicle not having transmitted
		// then. Possession stays with the field directives; these hooks only
		// enforce the windows.
		latestArgs.RowAllowed = func(name string, ts time.Time) bool {
			return signalValueVisible(repo, name, tok, ts)
		}
		latestArgs.ApproxLocationAllowed = func(ts time.Time) bool {
			return approxLocationVisible(tok, ts)
		}
		// lastSeen is computed across every signal the vehicle has, so it can
		// reveal activity outside the window; suppressed for scoped tokens
		// until it is window-aware.
		latestArgs.IncludeLastSeen = false
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
