package repositories

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/qtypes"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/server-garage/pkg/gql/errorhandler"
	"golang.org/x/sync/errgroup"
)

const (
	maxDateRangeDays     = 32
	maxDateRangeDuration = maxDateRangeDays*24*time.Hour + time.Second
	maxSegmentLimit      = 200
)

func validateSegmentDateRange(from, to time.Time) error {
	if to.Sub(from) > maxDateRangeDuration {
		return fmt.Errorf("date range exceeds maximum of %d days", maxDateRangeDays)
	}
	return nil
}

func validateSegmentArgs(did string, from, to time.Time) error {
	if did == "" {
		return fmt.Errorf("subject is required")
	}
	if from.After(to) {
		return fmt.Errorf("from time must be before to time")
	}
	if from.Equal(to) {
		return fmt.Errorf("from and to times cannot be equal")
	}
	if err := validateSegmentDateRange(from, to); err != nil {
		return err
	}
	return nil
}

// checkRange validates an optional bounded int: nil passes, otherwise the
// value must lie within [lo, hi]. name appears in the out-of-range error.
func checkRange(name string, v *int, lo, hi int) error {
	if v != nil && (*v < lo || *v > hi) {
		return fmt.Errorf("%s must be between %d and %d", name, lo, hi)
	}
	return nil
}

func validateSegmentConfig(config *model.SegmentConfig, mechanism model.DetectionMechanism) error {
	if config == nil {
		return nil
	}
	if err := checkRange("maxGapSeconds", config.MaxGapSeconds, 60, 3600); err != nil {
		return err
	}
	if err := checkRange("minSegmentDurationSeconds", config.MinSegmentDurationSeconds, 60, 3600); err != nil {
		return err
	}
	if err := checkRange("signalCountThreshold", config.SignalCountThreshold, 1, 3600); err != nil {
		return err
	}
	if mechanism == model.DetectionMechanismIdling {
		if err := checkRange("maxIdleRpm", config.MaxIdleRpm, 300, 3000); err != nil {
			return err
		}
	}
	if mechanism == model.DetectionMechanismRefuel || mechanism == model.DetectionMechanismRecharge {
		if err := checkRange("minIncreasePercent", config.MinIncreasePercent, 1, 100); err != nil {
			return err
		}
	}
	return nil
}

func validateSegmentLimit(limit *int) error {
	if limit == nil {
		return nil
	}
	if *limit < 1 || *limit > maxSegmentLimit {
		return fmt.Errorf("limit must be between 1 and %d", maxSegmentLimit)
	}
	return nil
}

var (
	sigSpeed     = &model.SegmentSignalRequest{Name: vss.FieldSpeed, Agg: model.FloatAggregationMax}
	sigFuelFirst = &model.SegmentSignalRequest{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Agg: model.FloatAggregationFirst}
	sigFuelLast  = &model.SegmentSignalRequest{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Agg: model.FloatAggregationLast}
	sigSoCFirst  = &model.SegmentSignalRequest{Name: vss.FieldPowertrainTractionBatteryStateOfChargeCurrent, Agg: model.FloatAggregationFirst}
	sigSoCLast   = &model.SegmentSignalRequest{Name: vss.FieldPowertrainTractionBatteryStateOfChargeCurrent, Agg: model.FloatAggregationLast}
	sigOdoFirst  = &model.SegmentSignalRequest{Name: vss.FieldPowertrainTransmissionTravelledDistance, Agg: model.FloatAggregationFirst}
	sigOdoLast   = &model.SegmentSignalRequest{Name: vss.FieldPowertrainTransmissionTravelledDistance, Agg: model.FloatAggregationLast}

	mechanismSignalSets = map[model.DetectionMechanism][]*model.SegmentSignalRequest{
		model.DetectionMechanismIdling:   {sigSpeed, sigFuelFirst, sigFuelLast, sigOdoFirst, sigOdoLast},
		model.DetectionMechanismRefuel:   {sigSpeed, sigFuelFirst, sigFuelLast, sigOdoFirst, sigOdoLast},
		model.DetectionMechanismRecharge: {sigSpeed, sigSoCFirst, sigSoCLast, sigOdoFirst, sigOdoLast},
	}

	baseSignalSet = []*model.SegmentSignalRequest{sigSpeed, sigFuelFirst, sigFuelLast, sigSoCFirst, sigSoCLast, sigOdoFirst, sigOdoLast}
)

func defaultSegmentSignalSet(mechanism model.DetectionMechanism) []*model.SegmentSignalRequest {
	if set, ok := mechanismSignalSets[mechanism]; ok {
		return set
	}
	return baseSignalSet
}

func buildAggArgs(signalReqs []*model.SegmentSignalRequest) ([]model.FloatSignalArgs, []model.LocationSignalArgs) {
	floatArgs := make([]model.FloatSignalArgs, 0, len(signalReqs))
	for _, req := range signalReqs {
		floatArgs = append(floatArgs, model.FloatSignalArgs{
			Name:  req.Name,
			Agg:   req.Agg,
			Alias: req.Name + "_" + string(req.Agg),
		})
	}
	locationArgs := []model.LocationSignalArgs{
		{Name: vss.FieldCurrentLocationCoordinates, Agg: model.LocationAggregationFirst, Alias: "startLoc"},
		{Name: vss.FieldCurrentLocationCoordinates, Agg: model.LocationAggregationLast, Alias: "endLoc"},
	}
	return floatArgs, locationArgs
}

func noDataLocation() *model.Location {
	return &model.Location{Latitude: 0, Longitude: 0, Hdop: 0}
}

// batchLocationsAt resolves the nearest prior fix for each timestamp in one query
// when the backend supports it, returning an index-aligned slice (entry nil = no fix
// within the lookback floor). This replaces the per-boundary LocationAt fan-out
// (finding #8): one as-of join instead of O(boundaries) reverse-scan point queries.
// A transient backend error is swallowed (best-effort, like the prior per-point path):
// every lookup counts toward segmentGapFillErrorsTotal and the caller substitutes
// (0,0), so a flaky location backend is visible without failing the segment.
func (r *Repository) batchLocationsAt(ctx context.Context, subject string, tss []time.Time) []*model.Location {
	if len(tss) == 0 {
		return nil
	}
	las, ok := r.query.(BatchLocationAtSource)
	if !ok {
		return nil
	}
	locs, err := las.LocationsAt(ctx, subject, tss)
	if err != nil {
		segmentGapFillErrorsTotal.Add(float64(len(tss)))
		return nil
	}
	return locs
}

// fillBoundaryLocations resolves, in ONE batched LocationsAt call, every boundary
// whose Value is still nil (no GPS fix from the windowed aggregate), then substitutes
// the (0,0) no-data sentinel for any that still have no prior fix. Shared by segment
// and daily-activity gap-fill; the (0,0) fallback preserves the prior per-point chain
// (windowed aggregate → nearest prior fix → (0,0)).
func (r *Repository) fillBoundaryLocations(ctx context.Context, subject string, boundaries []*model.SignalLocation) {
	if len(boundaries) == 0 {
		return
	}
	tss := make([]time.Time, len(boundaries))
	for i, b := range boundaries {
		tss[i] = b.Timestamp
	}
	locs := r.batchLocationsAt(ctx, subject, tss)
	for i, b := range boundaries {
		var got *model.Location
		if i < len(locs) {
			got = locs[i]
		}
		if got == nil {
			got = noDataLocation()
		}
		b.Value = got
	}
}

func mergeSegmentSignalRequests(defaultSet []*model.SegmentSignalRequest, clientRequests []*model.SegmentSignalRequest) []*model.SegmentSignalRequest {
	key := func(r *model.SegmentSignalRequest) string {
		if r == nil {
			return ""
		}
		return r.Name + ":" + string(r.Agg)
	}
	seen := make(map[string]struct{})
	var out []*model.SegmentSignalRequest
	for _, r := range append(defaultSet, clientRequests...) {
		if r == nil {
			continue
		}
		k := key(r)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, r)
	}
	return out
}

func sortSegmentSignals(signals []*model.SignalAggregationValue) {
	sort.Slice(signals, func(i, j int) bool {
		if signals[i].Name != signals[j].Name {
			return signals[i].Name < signals[j].Name
		}
		return signals[i].Agg < signals[j].Agg
	})
}

// GetSegments returns segments detected using the specified mechanism in the time range.
func (r *Repository) GetSegments(ctx context.Context, did string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig, signalRequests []*model.SegmentSignalRequest, eventRequests []*model.SegmentEventRequest, limit *int, after *time.Time) ([]*model.Segment, error) {
	if now := time.Now(); to.After(now) {
		to = now
	}
	if err := validateSegmentArgs(did, from, to); err != nil {
		return nil, errorhandler.NewBadRequestError(ctx, err)
	}
	if err := validateSegmentConfig(config, mechanism); err != nil {
		return nil, errorhandler.NewBadRequestError(ctx, err)
	}
	if err := validateSegmentLimit(limit); err != nil {
		return nil, errorhandler.NewBadRequestError(ctx, err)
	}
	segments, err := r.query.GetSegments(ctx, did, from, to, mechanism, config)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}
	// Cursor pagination: keep only segments starting strictly after the cursor. We must
	// NOT advance `from` to do this. A segment whose true start is at or before `from`
	// (an ongoing trip, or one already in progress at `from` — the common first segment of
	// any range) is reported with its start CLIPPED to `from`. Advancing `from` to
	// after+1ns would re-clip that same segment to the new `from`, giving it a start just
	// past the cursor that re-passes this filter forever — the same segment re-emitted
	// every page, 1ns later each time (infinite loop). Detecting from the original `from`
	// keeps the clipped start stable, so the filter retires the segment once the cursor
	// reaches it.
	if after != nil {
		segments = segmentsStartingAfter(segments, *after)
	}
	// For IDLING, the post-summary speed filter (below) drops moving segments, so
	// truncating here would under-return and, under cursor pagination, permanently
	// skip real idle segments past the cutoff. Defer idling's truncation until after
	// that filter; other mechanisms truncate now to bound the per-segment summary fetch.
	if mechanism != model.DetectionMechanismIdling {
		segments = truncateToLimit(segments, limit)
	} else {
		segments = capIdlingCandidates(segments)
	}

	defaultReqs := defaultSegmentSignalSet(mechanism)
	signalReqs := mergeSegmentSignalRequests(defaultReqs, signalRequests)
	wantSummary := len(signalReqs) > 0 || len(eventRequests) > 0
	eventNames := eventNamesOf(eventRequests)

	extendSummaryEnd := mechanism == model.DetectionMechanismRefuel || mechanism == model.DetectionMechanismRecharge

	var eventCountsBySeg map[int]map[string]int
	var aggsBySeg map[int][]*qtypes.AggSignal
	if wantSummary && len(segments) > 0 {
		ranges, aggRanges, globalFrom, globalTo := buildSegmentRanges(segments, to, extendSummaryEnd)
		floatArgs, locationArgs := buildAggArgs(signalReqs)
		var batchCounts []*qtypes.EventCountForRange
		var batchAggs []*qtypes.AggSignalForRange
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			var err error
			batchCounts, err = r.query.GetEventCountsForRanges(gctx, did, ranges, eventNames)
			return err
		})
		g.Go(func() error {
			var err error
			batchAggs, err = r.query.GetAggregatedSignalsForRanges(gctx, did, aggRanges, globalFrom, globalTo, floatArgs, locationArgs)
			return err
		})
		if err := g.Wait(); err != nil {
			return nil, handleDBError(ctx, err)
		}
		eventCountsBySeg = scatterEventCountsByIndex(batchCounts)
		aggsBySeg = scatterAggsByIndex(batchAggs)
	}

	for i, seg := range segments {
		var eventCounts []*qtypes.EventCount
		var preFetchedAggs []*qtypes.AggSignal
		if wantSummary {
			if eventCountsBySeg != nil {
				m := eventCountsBySeg[i]
				eventCounts = make([]*qtypes.EventCount, 0, len(m))
				for name, count := range m {
					eventCounts = append(eventCounts, &qtypes.EventCount{Name: name, Count: count})
				}
			}
			if aggsBySeg != nil {
				preFetchedAggs = aggsBySeg[i]
				if preFetchedAggs == nil {
					preFetchedAggs = []*qtypes.AggSignal{}
				}
			}
		}
		if err := r.enrichSegment(ctx, did, seg, to, wantSummary, signalReqs, eventNames, eventCounts, preFetchedAggs); err != nil {
			return nil, err
		}
	}

	// Batched location gap-fill for every boundary the aggregate left fix-less: one
	// LocationsAt as-of join for the whole page instead of 2 LocationAt point queries
	// per segment (finding #8).
	r.gapFillSegmentLocations(ctx, did, segments)

	if mechanism == model.DetectionMechanismIdling {
		segments = filterIdlingSegmentsBySpeed(segments, 0)
		// Truncate only now that moving segments are removed (deferred from above).
		segments = truncateToLimit(segments, limit)
	}
	return segments, nil
}

// segmentsStartingAfter drops segments whose start is at or before the cursor. Paired
// with detecting from the original (un-advanced) `from`, this gives stable cursor
// pagination: a started-before-range / ongoing segment has a clipped-but-stable start, so
// it is returned once and then retired once the cursor passes it — never re-emitted.
func segmentsStartingAfter(segments []*model.Segment, after time.Time) []*model.Segment {
	out := segments[:0]
	for _, s := range segments {
		if s.Start != nil && s.Start.Timestamp.After(after) {
			out = append(out, s)
		}
	}
	return out
}

// truncateToLimit caps segments at *limit (nil = no cap). For IDLING this must run
// only AFTER the speed filter (see GetSegments) so a page returns up to `limit` real
// idle segments instead of under-returning.
func truncateToLimit(segments []*model.Segment, limit *int) []*model.Segment {
	if limit != nil && len(segments) > *limit {
		return segments[:*limit]
	}
	return segments
}

// idlingEnrichCandidateCap bounds how many idle-run candidates the IDLING path
// enriches before its deferred speed filter + truncation (Q3). 8 × the max page
// size: generous enough that the surviving set matches the unbounded path unless an
// implausible fraction of the EARLIEST idle runs are also moving.
const idlingEnrichCandidateCap = 8 * maxSegmentLimit

// capIdlingCandidates bounds the IDLING candidate list before enrichment (Q3).
// Every detected idle run in the window is otherwise summary-fetched before the
// deferred truncation — unbounded work for a wide window. (Location gap-fill is no
// longer per-candidate: it is a single batched LocationsAt as-of join for the whole
// page, finding #8.) Segments arrive ordered ascending by start (mergeTimeRanges) and
// the final truncate keeps the first *limit idle runs that survive the speed filter,
// so capping the FIRST idlingEnrichCandidateCap (same ascending order) yields the
// EXACT surviving set as long as at least *limit of those candidates are non-moving.
// A page could under-return only if >(cap-1)/cap of the earliest idle-RPM runs also
// show movement (>87.5% at 8×) — pathological. 8× buys the soundness headroom cheaply.
func capIdlingCandidates(segments []*model.Segment) []*model.Segment {
	if len(segments) > idlingEnrichCandidateCap {
		return segments[:idlingEnrichCandidateCap]
	}
	return segments
}

// buildSegmentRanges builds, for the batched summary fetch, each segment's
// count range and agg range plus the global [from, to] envelope. aggRanges extend
// the end by summaryEndBuffer for refuel/recharge, whose level signal can land just
// after the trip ends.
func buildSegmentRanges(segments []*model.Segment, to time.Time, extendSummaryEnd bool) (ranges, aggRanges []qtypes.TimeRange, globalFrom, globalTo time.Time) {
	const summaryEndBuffer = 2 * time.Minute
	ranges = make([]qtypes.TimeRange, len(segments))
	aggRanges = make([]qtypes.TimeRange, len(segments))
	for i, seg := range segments {
		segTo := to
		if seg.End != nil {
			segTo = seg.End.Timestamp
		}
		ranges[i] = qtypes.TimeRange{From: seg.Start.Timestamp, To: segTo}
		summaryTo := segTo
		if extendSummaryEnd {
			summaryTo = segTo.Add(summaryEndBuffer)
			// Don't let the +buffer overrun the next segment's start. segmentIndexCaseSQL's
			// CASE returns the FIRST matching range, so an overlap row would be assigned to
			// this segment instead of the next — corrupting the next segment's FIRST values
			// and start location. Segments are sorted ascending by start (mergeTimeRanges).
			if i+1 < len(segments) && segments[i+1].Start != nil && summaryTo.After(segments[i+1].Start.Timestamp) {
				summaryTo = segments[i+1].Start.Timestamp
			}
		}
		aggRanges[i] = qtypes.TimeRange{From: seg.Start.Timestamp, To: summaryTo}
		if i == 0 {
			globalFrom, globalTo = seg.Start.Timestamp, summaryTo
			continue
		}
		if seg.Start.Timestamp.Before(globalFrom) {
			globalFrom = seg.Start.Timestamp
		}
		if summaryTo.After(globalTo) {
			globalTo = summaryTo
		}
	}
	return ranges, aggRanges, globalFrom, globalTo
}

// enrichSegment fills a segment's signals/eventCounts and its start/end location
// from the windowed aggregate (when wantSummary). Boundaries the aggregate leaves
// without a fix stay nil here and are resolved in a single batched pass afterwards
// (gapFillSegmentLocations) rather than one LocationAt point query each (finding #8).
func (r *Repository) enrichSegment(ctx context.Context, did string, seg *model.Segment, to time.Time, wantSummary bool, signalReqs []*model.SegmentSignalRequest, eventNames []string, eventCounts []*qtypes.EventCount, preFetchedAggs []*qtypes.AggSignal) error {
	if !wantSummary {
		return nil
	}
	summary, err := r.segmentSummary(ctx, did, seg, to, signalReqs, eventNames, eventCounts, preFetchedAggs)
	if err != nil {
		return err
	}
	seg.Signals = summary.Signals
	seg.EventCounts = summary.EventCounts
	if summary.StartLocation != nil {
		seg.Start.Value = summary.StartLocation
	}
	if seg.End != nil && summary.EndLocation != nil {
		seg.End.Value = summary.EndLocation
	}
	return nil
}

// gapFillSegmentLocations resolves every segment boundary (start, and end when
// present) that the windowed aggregate left without a GPS fix, in ONE batched
// LocationsAt as-of join instead of one reverse-scan point query per boundary
// (finding #8). Boundaries still nil after the lookup fall back to the (0,0) no-data
// sentinel — the exact fallback chain the prior per-segment path applied.
func (r *Repository) gapFillSegmentLocations(ctx context.Context, subject string, segments []*model.Segment) {
	var boundaries []*model.SignalLocation
	for _, seg := range segments {
		if seg.Start != nil && seg.Start.Value == nil {
			boundaries = append(boundaries, seg.Start)
		}
		if seg.End != nil && seg.End.Value == nil {
			boundaries = append(boundaries, seg.End)
		}
	}
	r.fillBoundaryLocations(ctx, subject, boundaries)
}

func segmentMaxSpeed(signals []*model.SignalAggregationValue) float64 {
	for _, s := range signals {
		if s != nil && s.Name == vss.FieldSpeed && s.Agg == string(model.FloatAggregationMax) {
			return s.Value
		}
	}
	return -1
}

func filterIdlingSegmentsBySpeed(segments []*model.Segment, maxSpeedKph float64) []*model.Segment {
	out := make([]*model.Segment, 0, len(segments))
	for _, seg := range segments {
		maxSpeed := segmentMaxSpeed(seg.Signals)
		if maxSpeed < 0 || maxSpeed <= maxSpeedKph {
			out = append(out, seg)
		}
	}
	return out
}

type segmentSummaryResult struct {
	Signals       []*model.SignalAggregationValue
	StartLocation *model.Location
	EndLocation   *model.Location
	EventCounts   []*model.EventCount
}

func buildSummaryFromAggs(aggs []*qtypes.AggSignal, floatArgs []model.FloatSignalArgs) ([]*model.SignalAggregationValue, *model.Location, *model.Location) {
	signals := make([]*model.SignalAggregationValue, 0, len(floatArgs))
	var startLoc, endLoc *model.Location
	for _, a := range aggs {
		if a.SignalType == qtypes.FloatType && int(a.SignalIndex) < len(floatArgs) {
			signals = append(signals, &model.SignalAggregationValue{
				Name:  floatArgs[a.SignalIndex].Name,
				Agg:   string(floatArgs[a.SignalIndex].Agg),
				Value: a.ValueNumber,
			})
		}
		// (0,0) is the "no GPS fix" sentinel, not a real location. The ranges aggregation
		// (GetAggregatedSignalsForRanges) does NOT exclude it (unlike the single-window
		// path), so skip it here and leave the slot nil — the batched location gap-fill
		// then substitutes the nearest prior real fix instead of pinning to null island.
		if a.SignalType == qtypes.LocType && (a.ValueLocation.Latitude != 0 || a.ValueLocation.Longitude != 0) {
			loc := &model.Location{
				Latitude:  a.ValueLocation.Latitude,
				Longitude: a.ValueLocation.Longitude,
				Hdop:      a.ValueLocation.HDOP,
			}
			if a.SignalIndex == 0 {
				startLoc = loc
			} else {
				endLoc = loc
			}
		}
	}
	sortSegmentSignals(signals)
	return signals, startLoc, endLoc
}

func buildEventSummary(eventCountMap map[string]int, eventNames []string) []*model.EventCount {
	if len(eventNames) > 0 {
		out := make([]*model.EventCount, len(eventNames))
		for i, name := range eventNames {
			out[i] = &model.EventCount{Name: name, Count: eventCountMap[name]}
		}
		return out
	}
	out := make([]*model.EventCount, 0, len(eventCountMap))
	for name, count := range eventCountMap {
		out = append(out, &model.EventCount{Name: name, Count: count})
	}
	return out
}

func eventCountsToMap(counts []*qtypes.EventCount) map[string]int {
	m := make(map[string]int, len(counts))
	for _, ec := range counts {
		m[ec.Name] = ec.Count
	}
	return m
}

func (r *Repository) segmentSummary(ctx context.Context, did string, seg *model.Segment, queryTo time.Time, signalReqs []*model.SegmentSignalRequest, eventNames []string, preFetchedEventCounts []*qtypes.EventCount, preFetchedAggs []*qtypes.AggSignal) (*segmentSummaryResult, error) {
	segFrom := seg.Start.Timestamp
	segTo := queryTo
	if seg.End != nil {
		segTo = seg.End.Timestamp
	}
	intervalMicro := segTo.Sub(segFrom).Microseconds()
	if intervalMicro <= 0 {
		intervalMicro = 1
	}

	floatArgs, locationArgs := buildAggArgs(signalReqs)
	var aggs []*qtypes.AggSignal
	if preFetchedAggs != nil {
		aggs = preFetchedAggs
	} else {
		aggArgs := &model.AggregatedSignalArgs{
			SignalArgs:   model.SignalArgs{Subject: did},
			FromTS:       segFrom,
			ToTS:         segTo,
			Interval:     intervalMicro,
			FloatArgs:    floatArgs,
			LocationArgs: locationArgs,
		}
		var err error
		aggs, err = r.query.GetAggregatedSignals(ctx, did, aggArgs)
		if err != nil {
			return nil, handleDBError(ctx, err)
		}
	}

	signalSummary, startLoc, endLoc := buildSummaryFromAggs(aggs, floatArgs)

	var eventCountMap map[string]int
	if preFetchedEventCounts != nil {
		eventCountMap = eventCountsToMap(preFetchedEventCounts)
	} else {
		eventCounts, err := r.query.GetEventCounts(ctx, did, segFrom, segTo, eventNames)
		if err != nil {
			return nil, handleDBError(ctx, err)
		}
		eventCountMap = eventCountsToMap(eventCounts)
	}

	return &segmentSummaryResult{
		Signals:       signalSummary,
		StartLocation: startLoc,
		EndLocation:   endLoc,
		EventCounts:   buildEventSummary(eventCountMap, eventNames),
	}, nil
}

// GetDailyActivity returns one record per calendar day in the requested date range.
func (r *Repository) GetDailyActivity(ctx context.Context, did string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig, signalRequests []*model.SegmentSignalRequest, eventRequests []*model.SegmentEventRequest, timezone *string) ([]*model.DailyActivity, error) {
	if mechanism == model.DetectionMechanismIdling || mechanism == model.DetectionMechanismRefuel || mechanism == model.DetectionMechanismRecharge {
		return nil, errorhandler.NewBadRequestError(ctx, fmt.Errorf("dailyActivity does not accept mechanism %s; use IGNITION_DETECTION, FREQUENCY_ANALYSIS, or CHANGE_POINT_DETECTION", mechanism))
	}
	loc := time.UTC
	if timezone != nil && *timezone != "" {
		var err error
		loc, err = time.LoadLocation(*timezone)
		if err != nil {
			return nil, errorhandler.NewBadRequestError(ctx, fmt.Errorf("invalid timezone %q: %w", *timezone, err))
		}
	}
	fromInLoc := from.In(loc)
	toInLoc := to.In(loc)
	fromDate := time.Date(fromInLoc.Year(), fromInLoc.Month(), fromInLoc.Day(), 0, 0, 0, 0, loc)
	toDate := time.Date(toInLoc.Year(), toInLoc.Month(), toInLoc.Day(), 0, 0, 0, 0, loc)
	if fromDate.After(toDate) {
		return nil, errorhandler.NewBadRequestError(ctx, fmt.Errorf("from date must be before to date"))
	}
	if toDate.After(time.Now().In(loc)) {
		return nil, errorhandler.NewBadRequestError(ctx, fmt.Errorf("to date cannot be in the future"))
	}
	if err := validateSegmentDateRange(fromDate, toDate); err != nil {
		return nil, errorhandler.NewBadRequestError(ctx, err)
	}
	rangeStart := fromDate
	// AddDate(0,0,1) advances one CALENDAR day in loc — a DST day is 23h/25h, so a flat
	// Add(24h) would drift off local midnight and clip/overrun the trailing day.
	rangeEnd := toDate.AddDate(0, 0, 1)

	defaultReqs := defaultSegmentSignalSet(mechanism)
	signalReqs := mergeSegmentSignalRequests(defaultReqs, signalRequests)
	eventNames := eventNamesOf(eventRequests)

	segments, err := r.GetSegments(ctx, did, rangeStart, rangeEnd, mechanism, config, signalReqs, eventRequests, nil, nil)
	if err != nil {
		return nil, err
	}

	// One batched ForRanges call covers every calendar day, replacing the old
	// per-day GetAggregatedSignals + GetEventCounts — up to 64 serialized
	// round-trips for a 32-day window, which blew the request timeout (SR-3).
	// Each result's SegIndex maps back to the day's position in `days`.
	floatArgs, locationArgs := buildAggArgs(signalReqs)
	var days []dayWindow
	// Iterate by CALENDAR day in loc (AddDate), not a flat 24h: across a DST transition a
	// civil day is 23h or 25h. A flat Add(24h) drifts d off local midnight after the
	// transition, makes the DST day's window overlap the next day, and terminates a day
	// early — dropping the last calendar day and mis-bucketing post-transition segments.
	for d := fromDate; !d.After(toDate); d = d.AddDate(0, 0, 1) {
		days = append(days, dayWindow{start: d.UTC(), end: d.AddDate(0, 0, 1).UTC()})
	}
	aggsByDay, eventCountsByDay, err := r.batchDaySummaries(ctx, did, days, floatArgs, locationArgs, eventNames)
	if err != nil {
		return nil, err
	}

	var out []*model.DailyActivity
	for i, w := range days {
		dayStartUTC := w.start
		dayEndUTC := w.end

		segmentCount, totalActiveSeconds, firstSeg, lastSeg := daySegmentStats(segments, dayStartUTC, dayEndUTC)

		signalSummary, startLoc, endLoc := buildSummaryFromAggs(aggsByDay[i], floatArgs)
		eventSummary := buildEventSummary(eventCountsByDay[i], eventNames)
		if firstSeg != nil && firstSeg.Start != nil && firstSeg.Start.Value != nil {
			startLoc = firstSeg.Start.Value
		}
		if lastSeg != nil && lastSeg.End != nil && lastSeg.End.Value != nil {
			endLoc = lastSeg.End.Value
		}
		// Boundaries left fix-less (no segment, no windowed aggregate) stay nil here and
		// are resolved in one batched LocationsAt pass below instead of a LocationAt
		// point query per day (finding #8).
		startSignalLoc := &model.SignalLocation{Timestamp: dayStartUTC, Value: startLoc}
		endSignalLoc := &model.SignalLocation{Timestamp: dayEndUTC, Value: endLoc}
		out = append(out, &model.DailyActivity{
			SegmentCount: segmentCount,
			Duration:     totalActiveSeconds,
			Start:        startSignalLoc,
			End:          endSignalLoc,
			Signals:      signalSummary,
			EventCounts:  eventSummary,
		})
	}
	// One batched gap-fill for every day boundary the aggregate/segments left fix-less;
	// any still without a prior fix falls back to (0,0).
	r.gapFillDailyLocations(ctx, did, out)
	if out == nil {
		out = []*model.DailyActivity{}
	}
	return out, nil
}

// gapFillDailyLocations resolves every day-activity boundary the aggregate and its
// overlapping segments left fix-less in one batched LocationsAt call (finding #8),
// substituting the (0,0) no-data sentinel for any without a prior fix.
func (r *Repository) gapFillDailyLocations(ctx context.Context, subject string, days []*model.DailyActivity) {
	var boundaries []*model.SignalLocation
	for _, d := range days {
		if d.Start != nil && d.Start.Value == nil {
			boundaries = append(boundaries, d.Start)
		}
		if d.End != nil && d.End.Value == nil {
			boundaries = append(boundaries, d.End)
		}
	}
	r.fillBoundaryLocations(ctx, subject, boundaries)
}

// daySegmentStats accounts one calendar day's [dayStart, dayEnd) window against all
// segments: the count overlapping the day, the total active seconds (each segment's
// overlap clamped to the day), and the first/last overlapping segment (which supply
// the day's boundary locations). A segment touches the day only if it both starts
// before dayEnd and has a positive-length overlap ending after dayStart.
func daySegmentStats(segments []*model.Segment, dayStart, dayEnd time.Time) (count, activeSeconds int, first, last *model.Segment) {
	for _, seg := range segments {
		segEnd := dayEnd
		if seg.End != nil && seg.End.Timestamp.Before(dayEnd) {
			segEnd = seg.End.Timestamp
		}
		if seg.Start.Timestamp.After(dayEnd) || segEnd.Before(dayStart) || !segEnd.After(seg.Start.Timestamp) {
			continue
		}
		count++
		overlapStart := seg.Start.Timestamp
		if overlapStart.Before(dayStart) {
			overlapStart = dayStart
		}
		overlapEnd := segEnd
		if overlapEnd.After(dayEnd) {
			overlapEnd = dayEnd
		}
		activeSeconds += int(overlapEnd.Sub(overlapStart).Seconds())
		if first == nil {
			first = seg
		}
		last = seg
	}
	return count, activeSeconds, first, last
}

// dayWindow is one calendar day's UTC [start, end) bounds, in the order
// GetDailyActivity iterates days so a ForRanges SegIndex maps back by position.
type dayWindow struct {
	start, end time.Time
}

// batchDaySummaries fetches every day's signal aggregation and event counts in
// a single GetAggregatedSignalsForRanges + GetEventCountsForRanges pair (run
// concurrently), then scatters the results by day index. This replaces the old
// per-day daySummary (one GetAggregatedSignals + one GetEventCounts each), whose
// serialized round-trips blew the request timeout for wide ranges (SR-3).
func (r *Repository) batchDaySummaries(ctx context.Context, did string, days []dayWindow, floatArgs []model.FloatSignalArgs, locationArgs []model.LocationSignalArgs, eventNames []string) (map[int][]*qtypes.AggSignal, map[int]map[string]int, error) {
	if len(days) == 0 {
		return map[int][]*qtypes.AggSignal{}, map[int]map[string]int{}, nil
	}
	ranges := make([]qtypes.TimeRange, len(days))
	for i, w := range days {
		ranges[i] = qtypes.TimeRange{From: w.start, To: w.end}
	}
	globalFrom := days[0].start
	globalTo := days[len(days)-1].end

	var batchAggs []*qtypes.AggSignalForRange
	var batchCounts []*qtypes.EventCountForRange
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		batchAggs, err = r.query.GetAggregatedSignalsForRanges(gctx, did, ranges, globalFrom, globalTo, floatArgs, locationArgs)
		return err
	})
	g.Go(func() error {
		var err error
		batchCounts, err = r.query.GetEventCountsForRanges(gctx, did, ranges, eventNames)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, nil, handleDBError(ctx, err)
	}

	return scatterAggsByIndex(batchAggs), scatterEventCountsByIndex(batchCounts), nil
}

// eventNamesOf extracts the requested event names, or nil when none are requested.
func eventNamesOf(eventRequests []*model.SegmentEventRequest) []string {
	if len(eventRequests) == 0 {
		return nil
	}
	names := make([]string, len(eventRequests))
	for i, e := range eventRequests {
		names[i] = e.Name
	}
	return names
}

// scatterAggsByIndex groups per-range aggregated signals by SegIndex — the
// position of the originating segment/day in the request slice.
func scatterAggsByIndex(aggs []*qtypes.AggSignalForRange) map[int][]*qtypes.AggSignal {
	out := make(map[int][]*qtypes.AggSignal)
	for _, a := range aggs {
		out[a.SegIndex] = append(out[a.SegIndex], &qtypes.AggSignal{
			SignalType:    a.SignalType,
			SignalIndex:   a.SignalIndex,
			ValueNumber:   a.ValueNumber,
			ValueString:   a.ValueString,
			ValueLocation: a.ValueLocation,
		})
	}
	return out
}

// scatterEventCountsByIndex groups per-range event counts by SegIndex into a
// name→count map.
func scatterEventCountsByIndex(counts []*qtypes.EventCountForRange) map[int]map[string]int {
	out := make(map[int]map[string]int)
	for _, ec := range counts {
		if out[ec.SegIndex] == nil {
			out[ec.SegIndex] = make(map[string]int)
		}
		out[ec.SegIndex][ec.Name] = ec.Count
	}
	return out
}
