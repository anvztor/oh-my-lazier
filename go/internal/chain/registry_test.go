package chain

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/config"
)

func TestRegistryIndexesChainsAndPathways(t *testing.T) {
	registry, err := NewRegistry(testChains(), testPathways())
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}

	ethereum, err := registry.Get(40161)
	if err != nil {
		t.Fatalf("Get(40161) error = %v", err)
	}
	if ethereum.EndpointAddress != common.HexToAddress("0x1111111111111111111111111111111111111111") {
		t.Fatalf("endpoint address = %s", ethereum.EndpointAddress)
	}
	if ethereum.TxRoles.Executor.SignerID != "0x9999999999999999999999999999999999999999" {
		t.Fatalf("executor signer = %s", ethereum.TxRoles.Executor.SignerID)
	}
	if ethereum.StartBlockNumber != 12345 {
		t.Fatalf("StartBlockNumber = %d, want 12345", ethereum.StartBlockNumber)
	}
	if ethereum.IndexerQueryBlockRange != 250 {
		t.Fatalf("IndexerQueryBlockRange = %d, want 250", ethereum.IndexerQueryBlockRange)
	}
	if ethereum.IndexerPollInterval != 7*time.Second {
		t.Fatalf("IndexerPollInterval = %s, want 7s", ethereum.IndexerPollInterval)
	}
	pathway, err := registry.Pathway(
		40161,
		40449,
		common.HexToAddress("0x7777777777777777777777777777777777777777"),
		common.HexToAddress("0x8888888888888888888888888888888888888888"),
	)
	if err != nil {
		t.Fatalf("Pathway() error = %v", err)
	}
	if !pathway.Enabled {
		t.Fatal("pathway.Enabled = false")
	}
	if pathway.MaxMessageSize != 10000 {
		t.Fatalf("MaxMessageSize = %d", pathway.MaxMessageSize)
	}
	if pathway.SourceWorkers.OpenExecutor != common.HexToAddress("0x2222222222222222222222222222222222222222") {
		t.Fatalf("pathway open executor address = %s", pathway.SourceWorkers.OpenExecutor)
	}
	if pathway.SourceWorkers.PriceFeed != common.HexToAddress("0x4444444444444444444444444444444444444444") {
		t.Fatalf("pathway price feed address = %s", pathway.SourceWorkers.PriceFeed)
	}
	if pathway.DestinationWorkers.OpenDVN != common.HexToAddress("0x6666666666666666666666666666666666666666") {
		t.Fatalf("pathway destination open dvn address = %s", pathway.DestinationWorkers.OpenDVN)
	}
	if pathway.DVNMode != config.DVNModeShadow {
		t.Fatalf("pathway dvn mode = %q", pathway.DVNMode)
	}
}

func TestRegistryRejectsUnknownPathway(t *testing.T) {
	registry, err := NewRegistry(testChains(), testPathways())
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	_, err = registry.Pathway(
		40161,
		40449,
		common.HexToAddress("0x9999999999999999999999999999999999999999"),
		common.HexToAddress("0x8888888888888888888888888888888888888888"),
	)
	if err == nil {
		t.Fatal("Pathway() error = nil, want unknown pathway error")
	}
}

func testChains() []config.ChainConfig {
	return []config.ChainConfig{
		{
			EID:                        40161,
			Name:                       "ethereum-sepolia",
			Family:                     config.ChainFamilyEVM,
			ChainID:                    11155111,
			EndpointAddress:            config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
			Confirmations:              12,
			StartBlockNumber:           12345,
			IndexerQueryBlockRange:     250,
			IndexerPollIntervalSeconds: 7,
			RPCURLs:                    []string{"http://localhost:8545"},
			TxRoles: config.ChainTxRolesConfig{
				Executor: testExecutorRole(),
				DVN: config.DVNTxRoleConfig{
					Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
					MaxFeePerGasWei:         "2000000000",
					MaxPriorityFeePerGasWei: "1000000000",
					MinNativeBalanceWei:     "100000000000000000",
				},
			},
		},
		{
			EID:                        40449,
			Name:                       "hoodi",
			Family:                     config.ChainFamilyEVM,
			ChainID:                    560048,
			EndpointAddress:            config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
			Confirmations:              12,
			IndexerPollIntervalSeconds: 11,
			RPCURLs:                    []string{"http://localhost:8546"},
			TxRoles: config.ChainTxRolesConfig{
				Executor: testExecutorRole(),
				DVN: config.DVNTxRoleConfig{
					Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
					MaxFeePerGasWei:         "2000000000",
					MaxPriorityFeePerGasWei: "1000000000",
					MinNativeBalanceWei:     "100000000000000000",
				},
			},
		},
	}
}

func testExecutorRole() config.ExecutorTxRoleConfig {
	return config.ExecutorTxRoleConfig{
		Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func testPathways() []config.PathwayConfig {
	return []config.PathwayConfig{
		{
			SrcEID:     40161,
			DstEID:     40449,
			SrcOApp:    config.MustEVMAddress("0x7777777777777777777777777777777777777777"),
			DstOApp:    config.MustEVMAddress("0x8888888888888888888888888888888888888888"),
			SendLib:    config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
			ReceiveLib: config.MustEVMAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			SourceWorkers: config.WorkerContractsConfig{
				OpenExecutor: config.MustEVMAddress("0x2222222222222222222222222222222222222222"),
				OpenDVN:      config.MustEVMAddress("0x3333333333333333333333333333333333333333"),
				PriceFeed:    config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
			},
			DestinationWorkers: config.DestinationWorkerContractsConfig{
				OpenDVN: config.MustEVMAddress("0x6666666666666666666666666666666666666666"),
			},
			DVN:            config.PathwayDVNConfig{Mode: config.DVNModeShadow},
			Enabled:        true,
			MaxMessageSize: 10000,
		},
	}
}
