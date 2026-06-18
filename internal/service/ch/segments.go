package ch

import (
	"context"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/segments"
	"github.com/prometheus/client_golang/prometheus"
)

// GetSegments returns segments detected using the specified mechanism.
func (s *Service) GetSegments(
	ctx context.Context,
	subject string,
	from, to time.Time,
	mechanism model.DetectionMechanism,
	config *model.SegmentConfig,
) ([]*model.Segment, error) {
	det, err := segments.NewDetector(chSignalSource{conn: s.conn}, mechanism)
	if err != nil {
		return nil, err
	}

	timer := prometheus.NewTimer(GetSegmentsLatency.WithLabelValues(mechanism.String()))
	segs, err := det.DetectSegments(ctx, subject, from, to, config)
	timer.ObserveDuration()
	if err != nil {
		return nil, err
	}
	return segs, nil
}
