// ducklake_schema_test.go pins the decoded-table column contract. The schema is
// otherwise implicitly defined by the first materializer write (the SignalRow /
// EventRow parquet template), so a model-garage change to those structs would
// silently alter the table shape and break appends against an existing lake
// (CHD-32). This test fails on any such drift; update the expected columns
// deliberately and plan a migration of the live lake tables when you do.
package tests

import (
	"context"
	"testing"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckLake_DecodedSchemaContract(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	svc := newLakeService(t, dir)
	db := svc.DB()

	// Constructing the materializer runs ensureSchema, creating the tables.
	_, err := materializer.NewDuckLakeMaterializer(ctx, db, zerolog.Nop())
	require.NoError(t, err)

	cols := func(table string) []string {
		rows, err := db.QueryContext(ctx, "SELECT * FROM "+table+" LIMIT 0")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck
		names, err := rows.Columns()
		require.NoError(t, err)
		return names
	}

	assert.Equal(t, []string{
		"subject", "subject_bucket", "name", "timestamp", "source", "producer",
		"cloud_event_id", "value_number", "value_string",
		"loc_lat", "loc_lon", "loc_hdop", "loc_heading",
	}, cols("lake.signals"), "lake.signals column contract (CHD-32)")

	assert.Equal(t, []string{
		"subject", "subject_bucket", "source", "producer", "cloud_event_id",
		"type", "data_version", "name", "timestamp", "duration_ns", "metadata", "tags",
	}, cols("lake.events"), "lake.events column contract (CHD-32)")
}
