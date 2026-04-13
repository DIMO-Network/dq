package graph

import (
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/fetch-api/pkg/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// filterToAdvancedSearchOptions converts a GraphQL filter and subject DID directly to
// grpc.AdvancedSearchOptions. The type and types fields are unioned: if both are set,
// results match events whose type is any of the combined values.
func filterToAdvancedSearchOptions(filter *model.CloudEventFilter, subject string) *grpc.AdvancedSearchOptions {
	opts := &grpc.AdvancedSearchOptions{
		Subject: &grpc.StringFilterOption{In: []string{subject}},
	}
	if filter == nil {
		return opts
	}
	// Merge type (single) and types (list) with OR semantics.
	var allTypes []string
	if filter.Type != nil {
		allTypes = append(allTypes, *filter.Type)
	}
	allTypes = append(allTypes, filter.Types...)
	if len(allTypes) > 0 {
		opts.Type = &grpc.StringFilterOption{In: allTypes}
	}
	if filter.ID != nil {
		opts.Id = &grpc.StringFilterOption{In: []string{*filter.ID}}
	}
	if filter.Dataversion != nil {
		opts.DataVersion = &grpc.StringFilterOption{In: []string{*filter.Dataversion}}
	}
	if filter.Source != nil {
		opts.Source = &grpc.StringFilterOption{In: []string{*filter.Source}}
	}
	if filter.Producer != nil {
		opts.Producer = &grpc.StringFilterOption{In: []string{*filter.Producer}}
	}
	if filter.Before != nil {
		opts.Before = timestamppb.New(*filter.Before)
	}
	if filter.After != nil {
		opts.After = timestamppb.New(*filter.After)
	}
	return opts
}

const defaultLimit = 10

// emptyCloudEventList is a preallocated empty slice for list resolvers to avoid
// allocating on sql.ErrNoRows.
var emptyCloudEventList = []*CloudEventWrapper{}

func resolveLimit(limit *int) int {
	if limit != nil && *limit > 0 {
		return *limit
	}
	return defaultLimit
}
