package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSource serves canned summaries per subject.
type fakeSource struct {
	bySubject map[string][]*model.SignalDataSummary
}

func (f fakeSource) GetSignalSummaries(_ context.Context, subject string, _ *model.SignalFilter) ([]*model.SignalDataSummary, error) {
	return f.bySubject[subject], nil
}

// TestReconcile_DetectsCountMismatch proves the harness flags a per-name count
// difference between the primary (ClickHouse) and secondary (lake) — the bulk
// pre-flip gate the migration lacked, which had only organic query coverage
// (CHD-15 / R2.3).
func TestReconcile_DetectsCountMismatch(t *testing.T) {
	ts := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	subj := "did:erc721:137:0xabc:1"
	primary := fakeSource{bySubject: map[string][]*model.SignalDataSummary{
		subj: {{Name: "speed", NumberOfSignals: 100, FirstSeen: ts, LastSeen: ts}},
	}}
	secondary := fakeSource{bySubject: map[string][]*model.SignalDataSummary{
		subj: {{Name: "speed", NumberOfSignals: 99, FirstSeen: ts, LastSeen: ts}}, // lost one
	}}

	rep, err := Reconcile(context.Background(), primary, secondary, []string{subj})
	require.NoError(t, err)
	require.Len(t, rep.Mismatches, 1)
	assert.Equal(t, subj, rep.Mismatches[0].Subject)
	assert.Equal(t, "speed", rep.Mismatches[0].Name)
	assert.Contains(t, rep.Mismatches[0].Detail, "count")
}

// TestReconcile_CleanWhenEqual confirms identical summaries produce no
// mismatches.
func TestReconcile_CleanWhenEqual(t *testing.T) {
	ts := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	subj := "did:erc721:137:0xabc:2"
	sums := []*model.SignalDataSummary{{Name: "speed", NumberOfSignals: 10, FirstSeen: ts, LastSeen: ts}}
	src := fakeSource{bySubject: map[string][]*model.SignalDataSummary{subj: sums}}

	rep, err := Reconcile(context.Background(), src, src, []string{subj})
	require.NoError(t, err)
	assert.Empty(t, rep.Mismatches)
	assert.Equal(t, 1, rep.SubjectsChecked)
}
