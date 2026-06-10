package materializer

import (
	"github.com/DIMO-Network/model-garage/pkg/autopi"
	"github.com/DIMO-Network/model-garage/pkg/hashdog"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/ruptela"
	"github.com/DIMO-Network/model-garage/pkg/tesla"
	"github.com/ethereum/go-ethereum/common"
)

// VendorConfig holds the chain settings for the model-garage vendor modules,
// mirroring din's convert.Config (DIMO_REGISTRY_CHAIN_ID, VEHICLE_NFT_ADDRESS,
// AFTERMARKET_NFT_ADDRESS, SYNTHETIC_NFT_ADDRESS).
type VendorConfig struct {
	ChainID               uint64
	VehicleNFTAddress     common.Address
	AftermarketNFTAddress common.Address
	SyntheticNFTAddress   common.Address
}

// RegisterVendorModules wires the real model-garage vendor modules into the
// local registries so post-fact decoding matches the ingest pipeline for
// every source. All four registrations must stay in sync with din/dis;
// dropping any silently breaks decoding for that oracle's raw data. Unknown
// sources still fall back to the ported default module, whose partial-decode
// salvage is stricter than upstream's.
func RegisterVendorModules(cfg VendorConfig) {
	autoPiModule := &autopi.Module{
		AftermarketContractAddr: cfg.AftermarketNFTAddress,
		VehicleContractAddr:     cfg.VehicleNFTAddress,
		ChainID:                 cfg.ChainID,
	}
	SignalRegistry.Override(modules.AutoPiSource.String(), autoPiModule)

	ruptelaModule := &ruptela.Module{
		AftermarketContractAddr: cfg.AftermarketNFTAddress,
		VehicleContractAddr:     cfg.VehicleNFTAddress,
		ChainID:                 cfg.ChainID,
	}
	SignalRegistry.Override(modules.RuptelaSource.String(), ruptelaModule)
	EventRegistry.Override(modules.RuptelaSource.String(), ruptelaModule)

	// Ruptela protocol via Kaufmann oracle: synthetic devices.
	ruptelaSyntheticModule := &ruptela.Module{
		AftermarketContractAddr: cfg.SyntheticNFTAddress,
		VehicleContractAddr:     cfg.VehicleNFTAddress,
		ChainID:                 cfg.ChainID,
	}
	SignalRegistry.Override(modules.KaufmannSource.String(), ruptelaSyntheticModule)
	EventRegistry.Override(modules.KaufmannSource.String(), ruptelaSyntheticModule)

	hashDogModule := &hashdog.Module{
		AftermarketContractAddr: cfg.AftermarketNFTAddress,
		VehicleContractAddr:     cfg.VehicleNFTAddress,
		ChainID:                 cfg.ChainID,
	}
	SignalRegistry.Override(modules.HashDogSource.String(), hashDogModule)

	// Tesla: stateless module, registered upstream by model-garage's own
	// init(). Without it Tesla raw status would fall through to the default
	// module and decode to nothing — live triggers would see Tesla signals
	// while decoded tables silently miss them.
	SignalRegistry.Override(modules.TeslaSource.String(), &tesla.Module{})
}
