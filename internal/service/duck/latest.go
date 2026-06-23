package duck

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// GetLatestSignals returns the latest value for the requested signal names for
// the subject, mirroring ch.Service.GetLatestSignals:
//
//   - non-location names: max(timestamp), arg_max(value_*) over all sources
//   - location names: arg_max excluding (0, 0) fixes
//   - IncludeLastSeen adds the virtual lastSeen row (max timestamp over all
//     signals, per (subject, source) under model.LastSeenField)
func (q *Queries) GetLatestSignals(ctx context.Context, subject string, latestArgs *model.LatestSignalsArgs) ([]*vss.Signal, error) {
	// The rollup serves named latest in O(distinct-names), but it stores
	// only the per-name latest value + its overall-max timestamp — not the
	// separate nonzero-location timestamp the location-name path needs — so
	// location names and source-filtered queries fall back to the full
	// deduped scan (SR-5).
	if noSourceFilter(latestArgs.Filter) && len(latestArgs.LocationSignalNames) == 0 {
		observeLakePath(true)
		return q.getLatestSignalsRollup(ctx, subject, latestArgs)
	}
	observeLakePath(false)
	return q.getLatestSignalsLake(ctx, subject, latestArgs)
}

// GetAllLatestSignals returns the latest value for every signal name stored
// for the subject, plus the virtual lastSeen row, mirroring
// ch.Service.GetAllLatestSignals: the timestamp is the unconditional
// max(timestamp) while the location value comes from the nonzero columns.
func (q *Queries) GetAllLatestSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]*vss.Signal, error) {
	if noSourceFilter(filter) {
		observeLakePath(true)
		return q.getAllLatestSignalsRollup(ctx, subject) // O(distinct-names) rollup (CHD-3)
	}
	observeLakePath(false)
	return q.getAllLatestSignalsLake(ctx, subject, filter)
}

// GetAvailableSignals returns the distinct signal names stored for a subject,
// sorted ascending. Returns nil when none.
func (q *Queries) GetAvailableSignals(ctx context.Context, subject string, filter *model.SignalFilter) ([]string, error) {
	if noSourceFilter(filter) {
		observeLakePath(true)
		return q.getAvailableSignalsRollup(ctx, subject) // rollup (CHD-3)
	}
	observeLakePath(false)
	return q.getAvailableSignalsLake(ctx, subject, filter)
}

// GetSignalSummaries returns per-name signal counts and first/last seen
// timestamps for a subject, aggregated across sources, mirroring
// ch.Service.GetSignalSummaries.
func (q *Queries) GetSignalSummaries(ctx context.Context, subject string, filter *model.SignalFilter) ([]*model.SignalDataSummary, error) {
	if noSourceFilter(filter) {
		observeLakePath(true)
		return q.getSignalSummariesRollup(ctx, subject) // rollup (CHD-3)
	}
	observeLakePath(false)
	return q.getSignalSummariesLake(ctx, subject, filter)
}

// scanSignalSummary scans one summary row (name, count, first_seen, last_seen)
// and normalizes both timestamps to UTC. The column order is shared by every
// signal-summary query (lake, rollup), so it lives in one place.
func scanSignalSummary(rows rowScanner) (*model.SignalDataSummary, error) {
	var s model.SignalDataSummary
	if err := rows.Scan(&s.Name, &s.NumberOfSignals, &s.FirstSeen, &s.LastSeen); err != nil {
		return nil, err
	}
	s.FirstSeen = s.FirstSeen.UTC()
	s.LastSeen = s.LastSeen.UTC()
	return &s, nil
}

// querySignals runs a signal-shaped query (name, ts, value_number,
// value_string, loc_lat, loc_lon, loc_hdop, loc_heading) and scans rows into
// vss.Signal values like ch's getSignals.
func (q *Queries) querySignals(ctx context.Context, stmt string, args []any) ([]*vss.Signal, error) {
	rows, err := q.svc.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duckdb: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	signals := []*vss.Signal{}
	for rows.Next() {
		var signal vss.Signal
		var ts time.Time
		var loc vss.Location
		err := rows.Scan(&signal.Data.Name, &ts, &signal.Data.ValueNumber, &signal.Data.ValueString,
			&loc.Latitude, &loc.Longitude, &loc.HDOP, &loc.Heading)
		if err != nil {
			return nil, fmt.Errorf("failed scanning duckdb row: %w", err)
		}
		signal.Data.Timestamp = ts.UTC()
		signal.Data.ValueLocation = loc
		signals = append(signals, &signal)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("duckdb row error: %w", rows.Err())
	}
	return signals, nil
}

func mapKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
