package duck

import (
	"context"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/segments"
)

// LakeSegments serves segment detection from lake.signals. It satisfies
// repositories.SegmentsBackend.
type LakeSegments struct {
	src *LakeSignalSource
}

// NewLakeSegments builds a SegmentsBackend over the catalog-attached svc.
func NewLakeSegments(svc *Service) *LakeSegments {
	return &LakeSegments{src: NewLakeSignalSource(svc)}
}

// GetSegments dispatches to the mechanism's detector over the lake source.
func (l *LakeSegments) GetSegments(ctx context.Context, subject string, from, to time.Time, mechanism model.DetectionMechanism, config *model.SegmentConfig) ([]*model.Segment, error) {
	det, err := segments.NewDetector(l.src, mechanism)
	if err != nil {
		return nil, err
	}
	return det.DetectSegments(ctx, subject, from, to, config)
}
