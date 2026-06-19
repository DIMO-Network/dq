// Package reconcile is the bulk CH↔lake pre-flip gate: it samples vehicles and
// compares per-signal summaries (count, first/last seen) between the ClickHouse
// primary and the DuckLake secondary. The migration previously had only organic
// query coverage via shadow mode; this gives an explicit, exhaustive check to
// run before flipping QUERY_BACKEND=ducklake (CHD-15 / R2.3).
package reconcile

import (
	"context"
	"fmt"

	"github.com/DIMO-Network/dq/internal/graph/model"
)

// SummarySource is the per-subject signal-summary surface both backends expose
// (repositories.CHService and duck.Queries both satisfy it).
type SummarySource interface {
	GetSignalSummaries(ctx context.Context, subject string, filter *model.SignalFilter) ([]*model.SignalDataSummary, error)
}

// Mismatch is one per-(subject,name) disagreement between the two backends.
type Mismatch struct {
	Subject string
	Name    string
	Detail  string
}

// Report is the outcome of a reconciliation run. An empty Mismatches slice is
// the green light to flip.
type Report struct {
	SubjectsChecked int
	Mismatches      []Mismatch
}

// Reconcile compares primary (ClickHouse) and secondary (lake) signal summaries
// for each subject and reports per-name disagreements. Run it over a sample of
// vehicles and require an empty report before cutover.
func Reconcile(ctx context.Context, primary, secondary SummarySource, subjects []string) (Report, error) {
	var rep Report
	for _, subject := range subjects {
		p, err := primary.GetSignalSummaries(ctx, subject, nil)
		if err != nil {
			return rep, fmt.Errorf("primary summaries for %s: %w", subject, err)
		}
		s, err := secondary.GetSignalSummaries(ctx, subject, nil)
		if err != nil {
			return rep, fmt.Errorf("secondary summaries for %s: %w", subject, err)
		}
		rep.SubjectsChecked++
		rep.Mismatches = append(rep.Mismatches, diffSummaries(subject, p, s)...)
	}
	return rep, nil
}

func diffSummaries(subject string, primary, secondary []*model.SignalDataSummary) []Mismatch {
	pBy := indexByName(primary)
	sBy := indexByName(secondary)
	var out []Mismatch
	for name, p := range pBy {
		s, ok := sBy[name]
		if !ok {
			out = append(out, Mismatch{Subject: subject, Name: name, Detail: "missing in secondary (lake)"})
			continue
		}
		if p.NumberOfSignals != s.NumberOfSignals {
			out = append(out, Mismatch{Subject: subject, Name: name,
				Detail: fmt.Sprintf("count primary=%d secondary=%d", p.NumberOfSignals, s.NumberOfSignals)})
		}
		if !p.LastSeen.Equal(s.LastSeen) {
			out = append(out, Mismatch{Subject: subject, Name: name,
				Detail: fmt.Sprintf("lastSeen primary=%s secondary=%s", p.LastSeen, s.LastSeen)})
		}
		if !p.FirstSeen.Equal(s.FirstSeen) {
			out = append(out, Mismatch{Subject: subject, Name: name,
				Detail: fmt.Sprintf("firstSeen primary=%s secondary=%s", p.FirstSeen, s.FirstSeen)})
		}
	}
	for name := range sBy {
		if _, ok := pBy[name]; !ok {
			out = append(out, Mismatch{Subject: subject, Name: name, Detail: "missing in primary (ClickHouse)"})
		}
	}
	return out
}

func indexByName(sums []*model.SignalDataSummary) map[string]*model.SignalDataSummary {
	m := make(map[string]*model.SignalDataSummary, len(sums))
	for _, s := range sums {
		m[s.Name] = s
	}
	return m
}
