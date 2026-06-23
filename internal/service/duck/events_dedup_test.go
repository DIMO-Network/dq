package duck

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLakeEvents_DedupOnRead proves the decoded-event read path collapses at-rest
// duplicates the way the signal path does (SR review #3). Two rows with the same
// (subject, timestamp, name, source) but different cloud_event_id — an
// at-least-once / re-decode duplicate the materializer's anti-join keeps because
// its key includes cloud_event_id — must count once after duplicates are
// collapsed. A row differing only in source is a distinct event.
func TestLakeEvents_DedupOnRead(t *testing.T) {
	ctx := context.Background()
	svc := newLakeServiceForTest(t)

	// lake.events matching materializer.EventRow (rows.go); only the columns the
	// query and dedup key touch are populated.
	_, err := svc.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS lake.events (
			subject VARCHAR, subject_bucket INTEGER, source VARCHAR, producer VARCHAR,
			cloud_event_id VARCHAR, type VARCHAR, data_version VARCHAR, name VARCHAR,
			timestamp TIMESTAMPTZ, duration_ns BIGINT, metadata VARCHAR, tags VARCHAR[])`)
	require.NoError(t, err)

	subject := testSubject1
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insertEvent := func(ceID, name, source string) {
		_, err := svc.db.ExecContext(ctx,
			`INSERT INTO lake.events (subject, subject_bucket, source, producer, cloud_event_id, type, name, timestamp, duration_ns)
			 VALUES (?, ?, ?, 'prod', ?, 'dimo.event', ?, ?, 0)`,
			subject, HashBucket(subject), source, ceID, name, ts.UTC())
		require.NoError(t, err)
	}

	insertEvent("ce-1", "harsh.brake", "src-a")
	insertEvent("ce-1-dup", "harsh.brake", "src-a") // dup key, different cloud_event_id
	insertEvent("ce-2", "harsh.brake", "src-b")     // distinct event (different source)

	q := NewLakeQueries(svc)
	counts, err := q.GetEventCounts(ctx, subject, ts.Add(-time.Hour), ts.Add(time.Hour), nil)
	require.NoError(t, err)
	require.Len(t, counts, 1)
	assert.Equal(t, "harsh.brake", counts[0].Name)
	assert.Equal(t, 2, counts[0].Count, "duplicate cloud_event_id collapses; distinct source is kept (not 3)")
}
