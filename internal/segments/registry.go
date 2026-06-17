package segments

import (
	"fmt"

	"github.com/DIMO-Network/dq/internal/graph/model"
)

// NewDetector returns the SegmentDetector for a mechanism, bound to src.
func NewDetector(src SignalSource, mechanism model.DetectionMechanism) (SegmentDetector, error) {
	switch mechanism {
	case model.DetectionMechanismIgnitionDetection:
		return NewIgnitionDetector(src), nil
	case model.DetectionMechanismFrequencyAnalysis:
		return NewFrequencyDetector(src), nil
	case model.DetectionMechanismChangePointDetection:
		return NewChangePointDetector(src), nil
	case model.DetectionMechanismIdling:
		return NewIdlingDetector(src), nil
	case model.DetectionMechanismRefuel:
		return NewRefuelDetector(src), nil
	case model.DetectionMechanismRecharge:
		return NewRechargeDetector(src), nil
	default:
		return nil, fmt.Errorf("unknown detection mechanism: %s", mechanism)
	}
}
