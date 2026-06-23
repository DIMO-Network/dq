package repositories

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/service/ch"
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
	if after != nil && after.Before(to) {
		cursorFrom := (*after).Add(time.Nanosecond)
		if cursorFrom.After(from) {
			from = cursorFrom
		}
	}

	chSegments, err := r.chService.GetSegments(ctx, did, from, to, mechanism, config)
	if err != nil {
		return nil, handleDBError(ctx, err)
	}
	// For IDLING, the post-summary speed filter (below) drops moving segments, so
	// truncating to `limit` here would under-return — and under cursor pagination
	// permanently skip real idle segments past the cutoff. Defer idling's
	// truncation until after that filter; other mechanisms truncate now to bound
	// the per-segment summary fetch.
	if limit != nil && mechanism != model.DetectionMechanismIdling && len(chSegments) > *limit {
		chSegments = chSegments[:*limit]
	}

	defaultReqs := defaultSegmentSignalSet(mechanism)
	signalReqs := mergeSegmentSignalRequests(defaultReqs, signalRequests)
	wantSummary := len(signalReqs) > 0 || len(eventRequests) > 0
	eventNames := eventNamesOf(eventRequests)

	const summaryEndBuffer = 2 * time.Minute
	extendSummaryEnd := mechanism == model.DetectionMechanismRefuel || mechanism == model.DetectionMechanismRecharge

	var eventCountsBySeg map[int]map[string]int
	var aggsBySeg map[int][]*ch.AggSignal
	if wantSummary && len(chSegments) > 0 {
		ranges := make([]ch.TimeRange, len(chSegments))
		aggRanges := make([]ch.TimeRange, len(chSegments))
		var globalFrom, globalTo time.Time
		for i, seg := range chSegments {
			segTo := to
			if seg.End != nil {
				segTo = seg.End.Timestamp
			}
			ranges[i] = ch.TimeRange{From: seg.Start.Timestamp, To: segTo}
			summaryTo := segTo
			if extendSummaryEnd {
				summaryTo = segTo.Add(summaryEndBuffer)
			}
			aggRanges[i] = ch.TimeRange{From: seg.Start.Timestamp, To: summaryTo}
			if i == 0 {
				globalFrom, globalTo = seg.Start.Timestamp, summaryTo
			} else {
				if seg.Start.Timestamp.Before(globalFrom) {
					globalFrom = seg.Start.Timestamp
				}
				if summaryTo.After(globalTo) {
					globalTo = summaryTo
				}
			}
		}
		floatArgs, locationArgs := buildAggArgs(signalReqs)
		var batchCounts []*ch.EventCountForRange
		var batchAggs []*ch.AggSignalForRange
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			var err error
			batchCounts, err = r.chService.GetEventCountsForRanges(gctx, did, ranges, eventNames)
			return err
		})
		g.Go(func() error {
			var err error
			batchAggs, err = r.chService.GetAggregatedSignalsForRanges(gctx, did, aggRanges, globalFrom, globalTo, floatArgs, locationArgs)
			return err
		})
		if err := g.Wait(); err != nil {
			return nil, handleDBError(ctx, err)
		}
		eventCountsBySeg = scatterEventCountsByIndex(batchCounts)
		aggsBySeg = scatterAggsByIndex(batchAggs)
	}

	// locAt (lake backend only) gap-fills a trip's start/end location from the
	// nearest fix before the boundary when no GPS fix landed inside the window.
	locAt, hasLocAt := r.chService.(LocationAtSource)

	segments := chSegments
	for i, seg := range segments {
		if wantSummary {
			var eventCounts []*ch.EventCount
			if eventCountsBySeg != nil {
				m := eventCountsBySeg[i]
				eventCounts = make([]*ch.EventCount, 0, len(m))
				for name, count := range m {
					eventCounts = append(eventCounts, &ch.EventCount{Name: name, Count: count})
				}
			}
			var preFetchedAggs []*ch.AggSignal
			if aggsBySeg != nil {
				preFetchedAggs = aggsBySeg[i]
				if preFetchedAggs == nil {
					preFetchedAggs = []*ch.AggSignal{}
				}
			}
			summary, err := r.segmentSummary(ctx, did, seg, to, signalReqs, eventNames, eventCounts, preFetchedAggs)
			if err != nil {
				return nil, err
			}
			seg.Signals = summary.Signals
			seg.EventCounts = summary.EventCounts
			if summary.StartLocation != nil {
				seg.Start.Value = summary.StartLocation
			}
			if seg.End != nil && summary.EndLocation != nil {
				seg.End.Value = summary.EndLocation
			}
		}
		// Gap-fill a still-missing location from the nearest fix before the boundary
		// (lake backend only) — strictly additive, only when the windowed aggregate
		// found nothing in the trip window.
		if hasLocAt {
			if seg.Start.Value == nil {
				if loc, err := locAt.LocationAt(ctx, did, seg.Start.Timestamp); err == nil && loc != nil {
					seg.Start.Value = loc
				}
			}
			if seg.End != nil && seg.End.Value == nil {
				if loc, err := locAt.LocationAt(ctx, did, seg.End.Timestamp); err == nil && loc != nil {
					seg.End.Value = loc
				}
			}
		}
		if seg.Start.Value == nil {
			seg.Start.Value = noDataLocation()
		}
		if seg.End != nil && seg.End.Value == nil {
			seg.End.Value = noDataLocation()
		}
	}

	if mechanism == model.DetectionMechanismIdling {
		segments = filterIdlingSegmentsBySpeed(segments, 0)
		// Truncate to `limit` only now that moving segments are removed, so a page
		// returns up to `limit` real idle segments (deferred from above).
		if limit != nil && len(segments) > *limit {
			segments = segments[:*limit]
		}
	}
	return segments, nil
}

func segmentMaxSpeed(signals []*model.SignalAggregationValue) float64 {
	for _, s := range signals {
		if s != nil && s.Name == vss.FieldSpeed && s.Agg == "MAX" {
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

func buildSummaryFromAggs(aggs []*ch.AggSignal, floatArgs []model.FloatSignalArgs) ([]*model.SignalAggregationValue, *model.Location, *model.Location) {
	signals := make([]*model.SignalAggregationValue, 0, len(floatArgs))
	var startLoc, endLoc *model.Location
	for _, a := range aggs {
		if a.SignalType == ch.FloatType && int(a.SignalIndex) < len(floatArgs) {
			signals = append(signals, &model.SignalAggregationValue{
				Name:  floatArgs[a.SignalIndex].Name,
				Agg:   string(floatArgs[a.SignalIndex].Agg),
				Value: a.ValueNumber,
			})
		}
		if a.SignalType == ch.LocType {
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

func eventCountsToMap(counts []*ch.EventCount) map[string]int {
	m := make(map[string]int, len(counts))
	for _, ec := range counts {
		m[ec.Name] = ec.Count
	}
	return m
}

func (r *Repository) segmentSummary(ctx context.Context, did string, seg *model.Segment, queryTo time.Time, signalReqs []*model.SegmentSignalRequest, eventNames []string, preFetchedEventCounts []*ch.EventCount, preFetchedAggs []*ch.AggSignal) (*segmentSummaryResult, error) {
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
	var aggs []*ch.AggSignal
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
		aggs, err = r.chService.GetAggregatedSignals(ctx, did, aggArgs)
		if err != nil {
			return nil, handleDBError(ctx, err)
		}
	}

	signalSummary, startLoc, endLoc := buildSummaryFromAggs(aggs, floatArgs)

	var eventCountMap map[string]int
	if preFetchedEventCounts != nil {
		eventCountMap = eventCountsToMap(preFetchedEventCounts)
	} else {
		eventCounts, err := r.chService.GetEventCounts(ctx, did, segFrom, segTo, eventNames)
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
	rangeEnd := toDate.Add(24 * time.Hour)

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
	for d := fromDate; !d.After(toDate); d = d.Add(24 * time.Hour) {
		days = append(days, dayWindow{start: d.UTC(), end: d.Add(24 * time.Hour).UTC()})
	}
	aggsByDay, eventCountsByDay, err := r.batchDaySummaries(ctx, did, days, floatArgs, locationArgs, eventNames)
	if err != nil {
		return nil, err
	}

	var out []*model.DailyActivity
	for i, w := range days {
		dayStartUTC := w.start
		dayEndUTC := w.end

		var segmentCount int
		var totalActiveSeconds int
		var firstSeg, lastSeg *model.Segment
		for _, seg := range segments {
			segEnd := dayEndUTC
			if seg.End != nil && seg.End.Timestamp.Before(dayEndUTC) {
				segEnd = seg.End.Timestamp
			}
			if seg.Start.Timestamp.After(dayEndUTC) || segEnd.Before(dayStartUTC) || !segEnd.After(seg.Start.Timestamp) {
				continue
			}
			segmentCount++
			overlapStart := seg.Start.Timestamp
			if overlapStart.Before(dayStartUTC) {
				overlapStart = dayStartUTC
			}
			overlapEnd := segEnd
			if overlapEnd.After(dayEndUTC) {
				overlapEnd = dayEndUTC
			}
			totalActiveSeconds += int(overlapEnd.Sub(overlapStart).Seconds())
			if firstSeg == nil {
				firstSeg = seg
			}
			lastSeg = seg
		}

		signalSummary, startLoc, endLoc := buildSummaryFromAggs(aggsByDay[i], floatArgs)
		eventSummary := buildEventSummary(eventCountsByDay[i], eventNames)
		if firstSeg != nil && firstSeg.Start != nil && firstSeg.Start.Value != nil {
			startLoc = firstSeg.Start.Value
		}
		if lastSeg != nil && lastSeg.End != nil && lastSeg.End.Value != nil {
			endLoc = lastSeg.End.Value
		}
		// Gap-fill the day's start/end location from the nearest prior fix (lake
		// backend only) when neither a segment nor the windowed aggregate supplied one.
		if las, ok := r.chService.(LocationAtSource); ok {
			if startLoc == nil {
				if loc, err := las.LocationAt(ctx, did, dayStartUTC); err == nil && loc != nil {
					startLoc = loc
				}
			}
			if endLoc == nil {
				if loc, err := las.LocationAt(ctx, did, dayEndUTC); err == nil && loc != nil {
					endLoc = loc
				}
			}
		}
		if startLoc == nil {
			startLoc = noDataLocation()
		}
		if endLoc == nil {
			endLoc = noDataLocation()
		}

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
	if out == nil {
		out = []*model.DailyActivity{}
	}
	return out, nil
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
func (r *Repository) batchDaySummaries(ctx context.Context, did string, days []dayWindow, floatArgs []model.FloatSignalArgs, locationArgs []model.LocationSignalArgs, eventNames []string) (map[int][]*ch.AggSignal, map[int]map[string]int, error) {
	if len(days) == 0 {
		return map[int][]*ch.AggSignal{}, map[int]map[string]int{}, nil
	}
	ranges := make([]ch.TimeRange, len(days))
	for i, w := range days {
		ranges[i] = ch.TimeRange{From: w.start, To: w.end}
	}
	globalFrom := days[0].start
	globalTo := days[len(days)-1].end

	var batchAggs []*ch.AggSignalForRange
	var batchCounts []*ch.EventCountForRange
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		batchAggs, err = r.chService.GetAggregatedSignalsForRanges(gctx, did, ranges, globalFrom, globalTo, floatArgs, locationArgs)
		return err
	})
	g.Go(func() error {
		var err error
		batchCounts, err = r.chService.GetEventCountsForRanges(gctx, did, ranges, eventNames)
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
func scatterAggsByIndex(aggs []*ch.AggSignalForRange) map[int][]*ch.AggSignal {
	out := make(map[int][]*ch.AggSignal)
	for _, a := range aggs {
		out[a.SegIndex] = append(out[a.SegIndex], &ch.AggSignal{
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
func scatterEventCountsByIndex(counts []*ch.EventCountForRange) map[int]map[string]int {
	out := make(map[int]map[string]int)
	for _, ec := range counts {
		if out[ec.SegIndex] == nil {
			out[ec.SegIndex] = make(map[string]int)
		}
		out[ec.SegIndex][ec.Name] = ec.Count
	}
	return out
}
