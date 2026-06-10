package materializer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// batchManifest records what one batch consumed and produced. Its
// existence gates summary increments on replay: a manifest written before
// a crash means the latest/summary buckets were already updated.
type batchManifest struct {
	BatchID            string   `json:"batchId"`
	Inputs             []string `json:"inputs"`
	Outputs            []string `json:"outputs"`
	SignalCount        int      `json:"signalCount"`
	EventCount         int      `json:"eventCount"`
	ErrorCount         int      `json:"errorCount"`
	ModelGarageVersion string   `json:"modelGarageVersion,omitempty"`
}

// loadWatermark reads decoded/v1/_state/watermark.json: a map of raw
// partition ("type=T/date=D") to the last fully-processed raw object key.
// A missing object means no batch has ever committed.
func (r *Runner) loadWatermark(ctx context.Context) (map[string]string, error) {
	data, err := r.store.GetObject(ctx, r.watermarkKey())
	if errors.Is(err, ErrNotFound) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	watermark := map[string]string{}
	if err := json.Unmarshal(data, &watermark); err != nil {
		return nil, fmt.Errorf("unmarshaling watermark: %w", err)
	}
	return watermark, nil
}

func (r *Runner) putJSON(ctx context.Context, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling %s: %w", key, err)
	}
	return r.store.PutObject(ctx, key, body)
}
