package chain

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

// Chain is a configured LayerZero endpoint chain plus its RPC quorum client.
type Chain struct {
	EID                    uint32
	Name                   string
	ChainID                *big.Int
	EndpointAddress        common.Address
	Confirmations          uint64
	StartBlockNumber       uint64
	IndexerQueryBlockRange uint64
	IndexerPollInterval    time.Duration
	TxRoles                TxRoles
	RPC                    *rpcquorum.Client
}

// TxRoles identifies local transaction signers and fee caps for one chain.
type TxRoles struct {
	Executor ExecutorTxRole
	DVN      DVNTxRole
}

// ExecutorTxRole identifies the executor transaction signer for one chain.
type ExecutorTxRole struct {
	SignerID                string
	MaxFeePerGasWei         string
	MaxPriorityFeePerGasWei string
	MinNativeBalanceWei     string
}

// DVNTxRole identifies active DVN transaction settings for one chain.
type DVNTxRole struct {
	SignerID                string
	MaxFeePerGasWei         string
	MaxPriorityFeePerGasWei string
	MinNativeBalanceWei     string
}

// WorkerContracts identifies the self-hosted worker contracts selected for one source pathway.
type WorkerContracts struct {
	OpenExecutor common.Address
	OpenDVN      common.Address
	PriceFeed    common.Address
}

// DestinationWorkerContracts identifies target-chain worker contracts selected for one pathway.
type DestinationWorkerContracts struct {
	OpenDVN common.Address
}

// Pathway is one configured source-to-destination OApp pathway.
type Pathway struct {
	SrcEID             uint32
	DstEID             uint32
	SrcOApp            common.Address
	DstOApp            common.Address
	SendLib            common.Address
	ReceiveLib         common.Address
	SourceWorkers      WorkerContracts
	DestinationWorkers DestinationWorkerContracts
	DVNMode            config.DVNMode
	Pricing            config.PathwayPricingConfig
	Enabled            bool
	MaxMessageSize     uint64
	MinLzReceiveGas    uint64
	MaxLzReceiveGas    uint64
}

// Registry indexes configured chains by endpoint ID.
type Registry struct {
	byEID    map[uint32]Chain
	pathways map[string]Pathway
}

// NewRegistry builds chain and pathway indexes from validated startup configuration.
func NewRegistry(chains []config.ChainConfig, pathways []config.PathwayConfig) (*Registry, error) {
	registry := &Registry{
		byEID:    make(map[uint32]Chain, len(chains)),
		pathways: make(map[string]Pathway, len(pathways)),
	}
	for _, cfg := range chains {
		registry.byEID[cfg.EID] = Chain{
			EID:                    cfg.EID,
			Name:                   cfg.Name,
			ChainID:                new(big.Int).SetUint64(cfg.ChainID),
			EndpointAddress:        cfg.EndpointAddress.Common(),
			Confirmations:          cfg.Confirmations,
			StartBlockNumber:       cfg.StartBlockNumber,
			IndexerQueryBlockRange: cfg.IndexerQueryBlockRange,
			IndexerPollInterval:    time.Duration(cfg.IndexerPollIntervalSeconds) * time.Second,
			TxRoles: TxRoles{
				Executor: ExecutorTxRole{
					SignerID:                cfg.TxRoles.Executor.Signer.Hex(),
					MaxFeePerGasWei:         cfg.TxRoles.Executor.MaxFeePerGasWei,
					MaxPriorityFeePerGasWei: cfg.TxRoles.Executor.MaxPriorityFeePerGasWei,
					MinNativeBalanceWei:     cfg.TxRoles.Executor.MinNativeBalanceWei,
				},
				DVN: DVNTxRole{
					SignerID:                optionalSignerID(cfg.TxRoles.DVN.Signer),
					MaxFeePerGasWei:         cfg.TxRoles.DVN.MaxFeePerGasWei,
					MaxPriorityFeePerGasWei: cfg.TxRoles.DVN.MaxPriorityFeePerGasWei,
					MinNativeBalanceWei:     cfg.TxRoles.DVN.MinNativeBalanceWei,
				},
			},
			RPC: rpcquorum.New(cfg.Name, cfg.RPCURLs),
		}
	}
	for _, cfg := range pathways {
		pathway := Pathway{
			SrcEID:     cfg.SrcEID,
			DstEID:     cfg.DstEID,
			SrcOApp:    cfg.SrcOApp.Common(),
			DstOApp:    cfg.DstOApp.Common(),
			SendLib:    cfg.SendLib.Common(),
			ReceiveLib: cfg.ReceiveLib.Common(),
			SourceWorkers: WorkerContracts{
				OpenExecutor: cfg.SourceWorkers.OpenExecutor.Common(),
				OpenDVN:      cfg.SourceWorkers.OpenDVN.Common(),
				PriceFeed:    cfg.SourceWorkers.PriceFeed.Common(),
			},
			DestinationWorkers: DestinationWorkerContracts{
				OpenDVN: cfg.DestinationWorkers.OpenDVN.Common(),
			},
			DVNMode:         cfg.DVN.Mode,
			Pricing:         cfg.Pricing,
			Enabled:         cfg.Enabled,
			MaxMessageSize:  cfg.MaxMessageSize,
			MinLzReceiveGas: cfg.MinLzReceiveGas,
			MaxLzReceiveGas: cfg.MaxLzReceiveGas,
		}
		registry.pathways[pathwayKey(pathway.SrcEID, pathway.DstEID, pathway.SrcOApp, pathway.DstOApp)] = pathway
	}
	return registry, nil
}

// Get returns the configured chain for an endpoint ID.
func (r *Registry) Get(eid uint32) (Chain, error) {
	chain, ok := r.byEID[eid]
	if !ok {
		return Chain{}, fmt.Errorf("unknown chain eid %d", eid)
	}
	return chain, nil
}

// All returns every configured chain.
func (r *Registry) All() []Chain {
	chains := make([]Chain, 0, len(r.byEID))
	for _, chain := range r.byEID {
		chains = append(chains, chain)
	}
	return chains
}

// Close releases RPC clients owned by configured chains.
func (r *Registry) Close() {
	if r == nil {
		return
	}
	for _, chain := range r.byEID {
		if chain.RPC != nil {
			chain.RPC.Close()
		}
	}
}

// Pathway returns the configured pathway for a source/destination OApp pair.
func (r *Registry) Pathway(srcEID, dstEID uint32, srcOApp, dstOApp common.Address) (Pathway, error) {
	pathway, ok := r.pathways[pathwayKey(srcEID, dstEID, srcOApp, dstOApp)]
	if !ok {
		return Pathway{}, fmt.Errorf("unknown pathway %d -> %d %s -> %s", srcEID, dstEID, srcOApp, dstOApp)
	}
	return pathway, nil
}

// Pathways returns every configured pathway.
func (r *Registry) Pathways() []Pathway {
	pathways := make([]Pathway, 0, len(r.pathways))
	for _, pathway := range r.pathways {
		pathways = append(pathways, pathway)
	}
	return pathways
}

func pathwayKey(srcEID, dstEID uint32, srcOApp, dstOApp common.Address) string {
	return fmt.Sprintf("%d:%d:%s:%s", srcEID, dstEID, srcOApp, dstOApp)
}

func optionalSignerID(value config.EVMAddress) string {
	if value.IsZero() {
		return ""
	}
	return value.Hex()
}
