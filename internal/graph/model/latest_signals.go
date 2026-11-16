package model

import (
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// LatestSignals backs the LatestSignals GraphQL type: the rows fetched for one
// latestSignals query, keyed by stored signal name. The number, string and
// location field resolvers project their value out of these rows.
type LatestSignals struct {
	// Subject is the vehicle DID the rows belong to.
	Subject string
	// LastSeen is the last time any signal was seen matching the filter.
	LastSeen *time.Time
	// Scalars holds the latest row of each name requested through number or string.
	Scalars map[string]*vss.Signal
	// Locations holds the latest nonzero fix of each name requested through location.
	Locations map[string]*vss.Signal
}
