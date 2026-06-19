package repositories

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TestShadow_DropCountedSeparatelyFromError proves a shadow call dropped under
// backpressure increments a dedicated dropped counter, not the error counter.
// Conflating the two let "clean" mean "didn't look": a saturated shadow could
// drop most comparisons while the error rate stayed low (CHD-15).
func TestShadow_DropCountedSeparatelyFromError(t *testing.T) {
	s := NewShadowBackend(nil, nil, nil, zerolog.Nop())
	// Saturate the semaphore so the next shadow call is dropped.
	for i := 0; i < cap(s.sem); i++ {
		s.sem <- struct{}{}
	}

	const method = "GetSignals"
	dropBefore := testutil.ToFloat64(shadowDroppedTotal.WithLabelValues(method))
	errBefore := testutil.ToFloat64(shadowErrorTotal.WithLabelValues(method))

	s.shadow(method, "", nil, nil, func(context.Context) (any, error) { return nil, nil })

	assert.Equal(t, dropBefore+1, testutil.ToFloat64(shadowDroppedTotal.WithLabelValues(method)),
		"a dropped shadow call increments the dropped counter")
	assert.Equal(t, errBefore, testutil.ToFloat64(shadowErrorTotal.WithLabelValues(method)),
		"a dropped shadow call does NOT inflate the error counter")
}
