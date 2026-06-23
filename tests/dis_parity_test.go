// dis_parity_test.go proves GraphQL-level output parity with the legacy
// dis pipeline using a REAL Ruptela device payload.
//
// The fixture (testdata/ruptela_full_status.json) is the verbatim
// `fullInputJSON` from model-garage's pkg/ruptela conversion tests — the
// same payload the dis integration suite pushes through mTLS. Its expected
// decoded values (speed 31.24609375, powertrainType "COMBUSTION", oil level
// "LOW", tire pressures, …) are hand-verified in model-garage and are
// exactly what dis persisted and served through dq's GraphQL
// API. Here the same payload flows through the NEW pipeline — raw parquet →
// materializer → DuckDB — and a real gqlgen execution of signalsLatest must
// return those exact numbers.
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/99designs/gqlgen/client"
	"github.com/99designs/gqlgen/graphql"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/ruptela"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ruptelaVehicle is the subject for the parity fixture on the test chain.
var ruptelaVehicle = fmt.Sprintf("did:erc721:137:%s:33", vehicleNFT.Hex())

// disExpected are the hand-verified conversion results from model-garage's
// TestFullFromDataConversion — the values dis stored and served.
var disExpected = struct {
	timestamp        time.Time
	speed            float64
	powertrainType   string
	batteryVoltage   float64
	tirePressureFL   float64
	dtcCount         float64
	engineOilLevel   string
	relativeFuel     float64
	travelledDistKm  float64
	engineOilRelPct  float64
	locationLat      float64
	locationLon      float64
	locationHDOP     float64
	currentAltitude  float64
	headingDegrees   float64
	tractionSOHPct   float64
	chargingPowerKW  float64
	isIgnitionOn     float64
	combustionECTTPS float64
}{
	timestamp:        time.Date(2024, 9, 27, 8, 33, 26, 0, time.UTC),
	speed:            31.24609375,
	powertrainType:   "COMBUSTION",
	batteryVoltage:   14.335,
	tirePressureFL:   262.00088,
	dtcCount:         18,
	engineOilLevel:   "LOW",
	relativeFuel:     19.200000000000003,
	travelledDistKm:  8,
	engineOilRelPct:  36.800000000000004,
	locationLat:      52.2721466,
	locationLon:      -0.9014316,
	locationHDOP:     0.6,
	currentAltitude:  104.8,
	headingDegrees:   73.7,
	tractionSOHPct:   98.5,
	chargingPowerKW:  24.400000000000002,
	isIgnitionOn:     1,
	combustionECTTPS: 18,
}

// loadRuptelaFixture builds the raw cloudevent exactly as dis stored it:
// the device's data section verbatim, headers as dis's cloudeventconvert
// derived them (type dimo.status, dataversion r/v0/s, source = the Ruptela
// connection address, subject = the paired vehicle DID).
func loadRuptelaFixture(t *testing.T) cloudevent.StoredEvent {
	t.Helper()
	body, err := os.ReadFile("testdata/ruptela_full_status.json")
	require.NoError(t, err)
	var doc struct {
		Time time.Time       `json:"time"`
		Data json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &doc))

	return cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        cloudevent.TypeStatus,
			Subject:     ruptelaVehicle,
			Source:      modules.RuptelaSource.String(),
			Producer:    ruptelaVehicle,
			ID:          "ruptela-parity-1",
			Time:        doc.Time,
			DataVersion: ruptela.StatusEventDS,
		},
		Data: doc.Data,
	}}
}

// passDirective lets every directive through: auth is exercised by dq's
// own middleware tests, parity here is about data values.
func passDirective(ctx context.Context, _ any, next graphql.Resolver) (any, error) {
	return next(ctx)
}

func passPrivilegeDirective(ctx context.Context, _ any, next graphql.Resolver, _ []string) (any, error) {
	return next(ctx)
}

func newGraphQLClient(t *testing.T, root string) *client.Client {
	t.Helper()
	svc, err := duck.NewService(duck.Config{S3Enabled: false})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	repo, err := repositories.NewRepository(repositories.ComposeBackend(duck.NewQueries(svc, root), nil))
	require.NoError(t, err)

	cfg := graph.Config{Resolvers: &graph.Resolver{SignalRepo: repo}}
	cfg.Directives.RequiresVehicleToken = passDirective
	cfg.Directives.IsSignal = passDirective
	cfg.Directives.HasAggregation = passDirective
	cfg.Directives.McpHide = passDirective
	cfg.Directives.RequiresAllOfPrivileges = passPrivilegeDirective
	cfg.Directives.RequiresOneOfPrivilege = passPrivilegeDirective

	srv := gqlhandler.New(graph.NewExecutableSchema(cfg))
	srv.AddTransport(transport.POST{})
	return client.New(srv)
}

func TestDISParity_RuptelaPayloadThroughGraphQL(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := newFSStore(t, root)

	// Stage 1: the real device payload lands as a raw bundle, exactly as
	// din's sink writes it.
	fixture := loadRuptelaFixture(t)
	writeRawBundle(t, store, fixture.Time, 1, fixture)

	// Stage 2: materializer decodes post fact with the real Ruptela module.
	materializer.RegisterVendorModules(materializer.VendorConfig{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
	})
	runner := materializer.New(materializer.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
	}, store, zerolog.Nop())
	processed, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Positive(t, processed)

	// Stage 3: a REAL gqlgen execution of signalsLatest must return the
	// exact values dis produced for this payload.
	gql := newGraphQLClient(t, root)

	var resp struct {
		SignalsLatest struct {
			LastSeen string
			Speed    struct {
				Timestamp string
				Value     float64
			}
			PowertrainType struct {
				Value string
			}
			LowVoltageBatteryCurrentVoltage struct {
				Value float64
			}
			ChassisAxleRow1WheelLeftTirePressure struct {
				Value float64
			}
			ObdStatusDTCCount struct {
				Value float64
			}
			PowertrainCombustionEngineEngineOilLevel struct {
				Value string
			}
			PowertrainFuelSystemRelativeLevel struct {
				Value float64
			}
			PowertrainTransmissionTravelledDistance struct {
				Value float64
			}
			PowertrainTractionBatteryStateOfHealth struct {
				Value float64
			}
			PowertrainTractionBatteryChargingPower struct {
				Value float64
			}
			IsIgnitionOn struct {
				Value float64
			}
			CurrentLocationAltitude struct {
				Value float64
			}
			CurrentLocationHeading struct {
				Value float64
			}
		}
	}

	query := fmt.Sprintf(`{
		signalsLatest(subject: %q) {
			lastSeen
			speed { timestamp value }
			powertrainType { value }
			lowVoltageBatteryCurrentVoltage { value }
			chassisAxleRow1WheelLeftTirePressure { value }
			obdStatusDTCCount { value }
			powertrainCombustionEngineEngineOilLevel { value }
			powertrainFuelSystemRelativeLevel { value }
			powertrainTransmissionTravelledDistance { value }
			powertrainTractionBatteryStateOfHealth { value }
			powertrainTractionBatteryChargingPower { value }
			isIgnitionOn { value }
			currentLocationAltitude { value }
			currentLocationHeading { value }
		}
	}`, ruptelaVehicle)

	require.NoError(t, gql.Post(query, &resp))

	got := resp.SignalsLatest
	lastSeen, err := time.Parse(time.RFC3339, got.LastSeen)
	require.NoError(t, err)
	speedTS, err := time.Parse(time.RFC3339, got.Speed.Timestamp)
	require.NoError(t, err)
	assert.True(t, lastSeen.Equal(disExpected.timestamp), "lastSeen: got %v", lastSeen)
	assert.True(t, speedTS.Equal(disExpected.timestamp), "speed timestamp")
	assert.Equal(t, disExpected.speed, got.Speed.Value, "speed must match dis bit-for-bit")
	assert.Equal(t, disExpected.powertrainType, got.PowertrainType.Value)
	assert.Equal(t, disExpected.batteryVoltage, got.LowVoltageBatteryCurrentVoltage.Value)
	assert.Equal(t, disExpected.tirePressureFL, got.ChassisAxleRow1WheelLeftTirePressure.Value)
	assert.Equal(t, disExpected.dtcCount, got.ObdStatusDTCCount.Value)
	assert.Equal(t, disExpected.engineOilLevel, got.PowertrainCombustionEngineEngineOilLevel.Value)
	assert.Equal(t, disExpected.relativeFuel, got.PowertrainFuelSystemRelativeLevel.Value)
	assert.Equal(t, disExpected.travelledDistKm, got.PowertrainTransmissionTravelledDistance.Value)
	assert.Equal(t, disExpected.tractionSOHPct, got.PowertrainTractionBatteryStateOfHealth.Value)
	assert.Equal(t, disExpected.chargingPowerKW, got.PowertrainTractionBatteryChargingPower.Value)
	assert.Equal(t, disExpected.isIgnitionOn, got.IsIgnitionOn.Value)
	assert.Equal(t, disExpected.currentAltitude, got.CurrentLocationAltitude.Value)
	assert.Equal(t, disExpected.headingDegrees, got.CurrentLocationHeading.Value)
}

// TestDISParity_LocationSignals checks the coordinate path: the Ruptela
// payload's GPS block must surface through the decoded location columns
// with the exact lat/lon/hdop dis produced.
func TestDISParity_LocationSignals(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := newFSStore(t, root)

	fixture := loadRuptelaFixture(t)
	writeRawBundle(t, store, fixture.Time, 1, fixture)

	materializer.RegisterVendorModules(materializer.VendorConfig{ChainID: 137, VehicleNFTAddress: vehicleNFT})
	runner := materializer.New(materializer.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, store, zerolog.Nop())
	_, err := runner.RunOnce(ctx)
	require.NoError(t, err)

	gql := newGraphQLClient(t, root)
	var resp struct {
		SignalsLatest struct {
			CurrentLocationCoordinates struct {
				Timestamp string
				Value     struct {
					Latitude  float64
					Longitude float64
					Hdop      float64
				}
			}
		}
	}
	query := fmt.Sprintf(`{
		signalsLatest(subject: %q) {
			currentLocationCoordinates { timestamp value { latitude longitude hdop } }
		}
	}`, ruptelaVehicle)
	require.NoError(t, gql.Post(query, &resp))

	loc := resp.SignalsLatest.CurrentLocationCoordinates
	assert.Equal(t, disExpected.locationLat, loc.Value.Latitude, "latitude bit-for-bit")
	assert.Equal(t, disExpected.locationLon, loc.Value.Longitude, "longitude bit-for-bit")
	assert.Equal(t, disExpected.locationHDOP, loc.Value.Hdop, "hdop bit-for-bit")
	locTS, err := time.Parse(time.RFC3339, loc.Timestamp)
	require.NoError(t, err)
	assert.True(t, locTS.Equal(disExpected.timestamp))
}
