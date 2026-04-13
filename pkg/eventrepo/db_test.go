package eventrepo_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	chconfig "github.com/DIMO-Network/clickhouse-infra/pkg/connect/config"
	"github.com/DIMO-Network/clickhouse-infra/pkg/container"
	"github.com/DIMO-Network/cloudevent"
	chindexer "github.com/DIMO-Network/cloudevent/clickhouse"
	"github.com/DIMO-Network/cloudevent/clickhouse/migrations"
	"github.com/stretchr/testify/require"
)

type TestContainer struct {
	container *container.Container
	onceSetup sync.Once
	refs      atomic.Int64
}

var globalTestContainer TestContainer

func (tc *TestContainer) TeardownIfLastTest(t *testing.T) {
	tc.refs.Add(1)
	t.Cleanup(func() {
		refs := tc.refs.Add(-1)
		if refs != 0 {
			return
		}
		tc.container.Terminate(context.Background())
		globalTestContainer.onceSetup = sync.Once{}
		globalTestContainer.refs = atomic.Int64{}
		globalTestContainer.container = nil
	})
}

// setupClickHouseContainer starts a ClickHouse container for testing and returns the connection.
func setupClickHouseContainer(t *testing.T) *container.Container {
	globalTestContainer.onceSetup.Do(func() {
		ctx := context.Background()
		settings := chconfig.Settings{
			User:     "default",
			Database: "dimo",
		}

		chContainer, err := container.CreateClickHouseContainer(ctx, settings)
		require.NoError(t, err)

		chDB, err := chContainer.GetClickhouseAsDB()
		require.NoError(t, err)

		err = migrations.RunGoose(ctx, []string{"up"}, chDB)
		require.NoError(t, err)
		globalTestContainer.container = chContainer
	})
	globalTestContainer.TeardownIfLastTest(t)
	return globalTestContainer.container
}

// insertTestData inserts test data into ClickHouse.
func insertTestData(t *testing.T, ctx context.Context, conn clickhouse.Conn, index *cloudevent.CloudEventHeader) string {
	values := chindexer.CloudEventToSlice(index)

	err := conn.Exec(ctx, chindexer.InsertStmt, values...)
	require.NoError(t, err)
	return values[len(values)-1].(string)
}
