package app

import (
	"testing"

	"github.com/DIMO-Network/dq/internal/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestStartMaterializer_LocalBucket proves the single-node wiring: a
// path-like parquet bucket selects the filesystem store instead of S3.
func TestStartMaterializer_LocalBucket(t *testing.T) {
	settings := &config.Settings{
		ParquetBucket:            t.TempDir(),
		MaterializerPollInterval: "1h",
		DIMORegistryChainID:      137,
		VehicleNFTAddress:        "0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
	}

	stop, err := startMaterializer(settings, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, stop)
	stop()
}

func TestStartMaterializer_RelativeBucketRejected(t *testing.T) {
	settings := &config.Settings{
		ParquetBucket: "file://relative/path",
	}
	_, err := startMaterializer(settings, zerolog.Nop())
	require.Error(t, err)
}
