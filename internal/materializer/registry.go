package materializer

// This file mirrors github.com/DIMO-Network/model-garage/pkg/modules'
// registry just closely enough for the decode loop. We cannot import
// pkg/modules directly today: its init() registers the AutoPi/Tesla/
// Ruptela vendor modules, which pull module dependencies (segmentio/ksuid,
// teslamotors/fleet-telemetry) that are not in this repo's go.sum, and the
// repo builds with -mod=readonly. The registry below self-initializes via
// init() with a port of model-garage's default module (empty source
// fallback), and callers can Override vendor sources with
// modules.SignalRegistry-compatible implementations once those
// dependencies land in go.sum.

import (
	"context"
	"fmt"
	"sync"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// SignalModule converts raw events to signals. It matches
// modules.SignalModule so model-garage modules can be registered directly.
type SignalModule interface {
	SignalConvert(ctx context.Context, event cloudevent.RawEvent) ([]vss.Signal, error)
}

// EventModule converts raw events to vehicle events. It matches
// modules.EventModule.
type EventModule interface {
	EventConvert(ctx context.Context, event cloudevent.RawEvent) ([]vss.Event, error)
}

// SignalRegistry stores signal modules by source, with "" as the default.
var SignalRegistry = newRegistry[SignalModule]()

// EventRegistry stores event modules by source, with "" as the default.
var EventRegistry = newRegistry[EventModule]()

func init() {
	def := &defaultModule{}
	SignalRegistry.Override("", def)
	EventRegistry.Override("", def)
}

type registry[T any] struct {
	mu      sync.RWMutex
	modules map[string]T
}

func newRegistry[T any]() *registry[T] {
	return &registry[T]{modules: make(map[string]T)}
}

// Override adds or replaces the module for a source.
func (r *registry[T]) Override(source string, module T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.modules[source] = module
}

// Get returns the module for a source.
func (r *registry[T]) Get(source string) (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.modules[source]
	return m, ok
}

// get returns the module for source, falling back to the default ("").
func (r *registry[T]) get(source string) (T, error) {
	module, ok := r.Get(source)
	if !ok {
		module, ok = r.Get("")
		if !ok {
			var zero T
			return zero, fmt.Errorf("module '%s' not found and no default module registered", source)
		}
	}
	return module, nil
}

// convertToSignals dispatches like modules.ConvertToSignals.
func convertToSignals(ctx context.Context, source string, event cloudevent.RawEvent) ([]vss.Signal, error) {
	module, err := SignalRegistry.get(source)
	if err != nil {
		return nil, err
	}
	signals, err := module.SignalConvert(ctx, event)
	if err != nil {
		return nil, fmt.Errorf("failed to convert signals with module '%s': %w", source, err)
	}
	return signals, nil
}

// convertToEvents dispatches like modules.ConvertToEvents.
func convertToEvents(ctx context.Context, source string, event cloudevent.RawEvent) ([]vss.Event, error) {
	module, err := EventRegistry.get(source)
	if err != nil {
		return nil, err
	}
	events, err := module.EventConvert(ctx, event)
	if err != nil {
		return nil, fmt.Errorf("failed to convert events with module '%s': %w", source, err)
	}
	return events, nil
}
