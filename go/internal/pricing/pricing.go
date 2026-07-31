package pricing

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/abiutil"
	"github.com/islishude/oh-my-lazier/go/internal/bigutil"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/db"
)

const (
	// TxPurposeSetPriceSnapshot identifies OpenPriceFeed.setPriceSnapshot updates.
	TxPurposeSetPriceSnapshot = "pricing_set_price_snapshot"
)

var (
	//go:embed abis/price_snapshot.json
	priceSnapshotABIJSON string

	priceSnapshotABI = abiutil.MustParse(priceSnapshotABIJSON)
	maxUint256       = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
)

// Bot updates shared worker price snapshots.
type Bot struct {
	store         Store
	registry      *chain.Registry
	settings      Settings
	sources       map[uint32]ChainSources
	snapshots     PriceSnapshotReader
	lastGasPrices map[string]*big.Int
	lastWritten   map[string]writtenPrice
	loadedWritten map[string]bool
	now           func() time.Time
	logger        *slog.Logger
}

// New creates a price bot.
func New(logger *slog.Logger) *Bot {
	return &Bot{logger: logger, now: time.Now}
}

// NewWithDependencies creates an enabled price bot with explicit sources.
func NewWithDependencies(store Store, registry *chain.Registry, settings Settings, sources map[uint32]ChainSources, snapshots PriceSnapshotReader, logger *slog.Logger) (*Bot, error) {
	if !settings.Enabled {
		return New(logger), nil
	}
	if store == nil {
		return nil, errors.New("pricing store is required")
	}
	if registry == nil {
		return nil, errors.New("pricing registry is required")
	}
	if snapshots == nil {
		return nil, errors.New("pricing snapshot reader is required")
	}
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	copied := make(map[uint32]ChainSources, len(sources))
	maps.Copy(copied, sources)
	return &Bot{
		store:         store,
		registry:      registry,
		settings:      settings,
		sources:       copied,
		snapshots:     snapshots,
		lastGasPrices: make(map[string]*big.Int),
		lastWritten:   make(map[string]writtenPrice),
		loadedWritten: make(map[string]bool),
		now:           time.Now,
		logger:        logger,
	}, nil
}

// Run starts the price update loop until the context is canceled.
func (b *Bot) Run(ctx context.Context) error {
	if b == nil || !b.settings.Enabled {
		b.logger.Info("price bot disabled")
		<-ctx.Done()
		return ctx.Err()
	}
	b.logger.Info("price bot loop started")
	if err := b.EnqueueOnce(ctx); err != nil {
		return err
	}
	interval := time.NewTicker(b.settings.Interval)
	defer interval.Stop()
	gasCheckInterval := min(b.settings.Interval, 15*time.Second)
	gasCheck := time.NewTicker(gasCheckInterval)
	defer gasCheck.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-interval.C:
			if err := b.EnqueueOnce(ctx); err != nil {
				return err
			}
		case <-gasCheck.C:
			if err := b.EnqueueOnGasSpike(ctx); err != nil {
				return err
			}
		}
	}
}

// Store persists price update transactions.
type Store interface {
	EnqueueTx(ctx context.Context, request db.TxRequest) (int64, error)
}

// GasPriceReader reads a destination-chain gas price.
type GasPriceReader interface {
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
}

// PriceReader reads USD/native prices.
type PriceReader interface {
	PriceUSD(ctx context.Context) (SourcePrice, error)
}

// PriceSnapshotReader reads the currently stored snapshot for one source-chain feed pathway.
type PriceSnapshotReader interface {
	PriceSnapshot(ctx context.Context, srcEID uint32, priceFeed common.Address, dstEID uint32) (PriceSnapshot, error)
}

type onChainPriceSnapshotReader struct {
	registry *chain.Registry
}

// NewOnChainPriceSnapshotReader creates a snapshot reader backed by configured source-chain RPCs.
func NewOnChainPriceSnapshotReader(registry *chain.Registry) PriceSnapshotReader {
	return &onChainPriceSnapshotReader{registry: registry}
}

func (r *onChainPriceSnapshotReader) PriceSnapshot(ctx context.Context, srcEID uint32, priceFeed common.Address, dstEID uint32) (PriceSnapshot, error) {
	if r == nil || r.registry == nil {
		return PriceSnapshot{}, errors.New("pricing snapshot registry is required")
	}
	srcChain, err := r.registry.Get(srcEID)
	if err != nil {
		return PriceSnapshot{}, err
	}
	if srcChain.RPC == nil {
		return PriceSnapshot{}, fmt.Errorf("pricing snapshot RPC for chain %d is not configured", srcEID)
	}
	snapshot, err := readPriceSnapshot(ctx, srcChain.RPC, priceFeed, dstEID)
	if err != nil {
		return PriceSnapshot{}, fmt.Errorf("read price snapshot for %d -> %d: %w", srcEID, dstEID, err)
	}
	return snapshot, nil
}

func readPriceSnapshot(ctx context.Context, caller CallContractReader, priceFeed common.Address, dstEID uint32) (PriceSnapshot, error) {
	calldata, err := priceSnapshotABI.Pack("priceSnapshot", dstEID)
	if err != nil {
		return PriceSnapshot{}, err
	}
	result, err := caller.CallContract(ctx, ethereum.CallMsg{To: &priceFeed, Data: calldata}, nil)
	if err != nil {
		return PriceSnapshot{}, err
	}
	values, err := priceSnapshotABI.Unpack("priceSnapshot", result)
	if err != nil {
		return PriceSnapshot{}, err
	}
	if len(values) != 4 {
		return PriceSnapshot{}, fmt.Errorf("priceSnapshot returned %d values", len(values))
	}
	dstGasPrice, gasOK := values[0].(*big.Int)
	dstDataFee, dataOK := values[1].(*big.Int)
	updatedAt, updatedOK := values[2].(uint64)
	staleAfter, staleOK := values[3].(uint64)
	if !gasOK || !dataOK || !updatedOK || !staleOK {
		return PriceSnapshot{}, errors.New("priceSnapshot returned unexpected ABI values")
	}
	return PriceSnapshot{
		DstGasPriceInSrcToken:       bigutil.Clone(dstGasPrice),
		DstDataFeePerByteInSrcToken: bigutil.Clone(dstDataFee),
		UpdatedAt:                   updatedAt,
		StaleAfter:                  staleAfter,
	}, nil
}

// Settings controls price update generation.
type Settings struct {
	Enabled              bool
	SignerID             string
	Interval             time.Duration
	StaleAfter           time.Duration
	MaxDeviation         uint64
	MinUpdateDeviation   uint64
	Heartbeat            time.Duration
	SourceRequestTimeout time.Duration
	GasSpikeBps          uint64
}

// FeeModel controls one worker role's source-chain quote inputs.
type FeeModel struct {
	FixedFee              *big.Int
	DstGasOverhead        uint64
	DataSizeOverheadBytes uint64
	MarginBps             uint16
}

// Validate checks settings required for enabled price updates.
func (s Settings) Validate() error {
	if !s.Enabled {
		return nil
	}
	if s.SignerID == "" {
		return errors.New("pricing signer id is required")
	}
	if s.Interval <= 0 {
		return errors.New("pricing interval must be positive")
	}
	if s.StaleAfter <= 0 {
		return errors.New("pricing stale_after must be positive")
	}
	if s.StaleAfter > time.Duration(config.MaxPriceSnapshotStaleAfterSeconds)*time.Second {
		return fmt.Errorf("pricing stale_after exceeds OpenPriceFeed maximum %s", time.Duration(config.MaxPriceSnapshotStaleAfterSeconds)*time.Second)
	}
	if s.MaxDeviation == 0 {
		return errors.New("pricing max deviation bps is required")
	}
	if s.MinUpdateDeviation == 0 || s.MinUpdateDeviation > 10_000 {
		return errors.New("pricing min update deviation bps must be between 1 and 10000")
	}
	if s.Heartbeat <= 0 {
		return errors.New("pricing heartbeat must be positive")
	}
	if s.Heartbeat >= s.StaleAfter {
		return errors.New("pricing heartbeat must be less than stale_after")
	}
	if s.Interval >= s.StaleAfter-s.Heartbeat {
		return errors.New("pricing heartbeat plus interval must be less than stale_after")
	}
	if s.SourceRequestTimeout <= 0 {
		return errors.New("pricing source request timeout must be positive")
	}
	if s.GasSpikeBps == 0 {
		return errors.New("pricing gas spike bps is required")
	}
	return nil
}

// Validate checks one worker fee model.
func (m FeeModel) Validate(prefix string) error {
	if m.FixedFee == nil || m.FixedFee.Sign() < 0 {
		return fmt.Errorf("%s fixed fee must be non-negative", prefix)
	}
	if m.MarginBps > 10_000 {
		return fmt.Errorf("%s margin bps exceeds 10000", prefix)
	}
	return nil
}

// ChainSources are the price and gas inputs for one configured chain.
type ChainSources struct {
	Primary           ConfiguredPriceReader
	Sanity            []ConfiguredPriceReader
	Gas               GasPriceReader
	DataFeePerByteWei *big.Int
	NativeAssetID     string
}

// ConfiguredPriceReader binds one source reader to its freshness policy.
type ConfiguredPriceReader struct {
	Name   string
	Reader PriceReader
	MaxAge time.Duration
}

// PriceSelectionPolicy controls primary/sanity price-source selection.
type PriceSelectionPolicy struct {
	MaxDeviationBps      uint64
	SourceRequestTimeout time.Duration
	Now                  func() time.Time
	OnSourceFailure      func(PriceSourceFailure)
}

// PriceSourceFailure describes one rejected primary or sanity observation.
type PriceSourceFailure struct {
	EID          uint32
	Source       string
	Role         string
	Category     string
	DeviationBps uint64
	Err          error
}

// EnqueueOnce computes current price snapshots and enqueues shared price-feed update batches.
func (b *Bot) EnqueueOnce(ctx context.Context) error {
	if b == nil || !b.settings.Enabled {
		return nil
	}
	if b.now == nil {
		b.now = time.Now
	}
	updates, err := b.uniquePriceUpdates()
	if err != nil {
		return err
	}
	if len(updates) == 0 {
		b.logger.Debug("skipped price update batch", "reason", "no_pathways")
		return nil
	}
	cycle, err := b.preparePriceCycle(ctx, updates, nil)
	if err != nil {
		return err
	}
	for _, batch := range priceUpdateBatches(updates) {
		if _, _, err := b.enqueuePriceUpdateBatch(ctx, batch, cycle); err != nil {
			return err
		}
	}
	return nil
}

// EnqueueOnGasSpike enqueues updates for pathways whose destination gas price rose past the configured threshold.
func (b *Bot) EnqueueOnGasSpike(ctx context.Context) error {
	if b == nil || !b.settings.Enabled {
		return nil
	}
	if b.lastGasPrices == nil {
		b.lastGasPrices = make(map[string]*big.Int)
	}
	updates, err := b.uniquePriceUpdates()
	if err != nil {
		return err
	}
	if len(updates) == 0 {
		b.logger.Debug("skipped price gas-spike check", "reason", "no_pathways")
		return nil
	}
	gasPrices, err := b.readDestinationGasPrices(ctx, updates)
	if err != nil {
		return err
	}
	spikes := make([]pricedGasSpike, 0, len(updates))
	for _, update := range updates {
		key := priceUpdateKey(update)
		current := gasPrices[update.DstEID]
		previous := b.lastGasPrices[key]
		if previous == nil {
			b.lastGasPrices[key] = bigutil.Clone(current)
			continue
		}
		if GasIncreaseBps(previous, current) < b.settings.GasSpikeBps {
			continue
		}
		spikes = append(spikes, pricedGasSpike{
			update:   update,
			previous: bigutil.Clone(previous),
			current:  bigutil.Clone(current),
		})
	}
	selectedUpdates := spikeUpdates(spikes)
	cycle, err := b.preparePriceCycle(ctx, selectedUpdates, gasPrices)
	if err != nil {
		return err
	}
	for _, batch := range priceUpdateBatches(selectedUpdates) {
		txOutboxID, enqueued, err := b.enqueuePriceUpdateBatch(ctx, batch, cycle)
		if err != nil {
			return err
		}
		if txOutboxID == 0 {
			// Nothing was enqueued: the chain is paused/disabled, or every
			// update in the batch was suppressed by deviation gating.
			continue
		}
		for _, target := range enqueued {
			for _, selected := range spikes {
				if selected.update != target {
					continue
				}
				b.logger.Warn("price bot enqueued gas-spike update", "src_eid", selected.update.SrcEID, "dst_eid", selected.update.DstEID, "price_feed", selected.update.PriceFeed, "previous_gas_wei", selected.previous, "current_gas_wei", selected.current, "tx_outbox_id", txOutboxID)
				break
			}
		}
	}
	return nil
}

type pricedUpdate struct {
	SrcEID    uint32
	DstEID    uint32
	PriceFeed common.Address
}

type pricedGasSpike struct {
	update   pricedUpdate
	previous *big.Int
	current  *big.Int
}

type pricedUpdateBatch struct {
	SrcEID    uint32
	PriceFeed common.Address
	Targets   []pricedUpdate
}

type priceCycleInputs struct {
	nativeUSD map[uint32]*big.Rat
	gasWei    map[uint32]*big.Int
}

type writtenPrice struct {
	price     *big.Int
	writtenAt time.Time
}

func (b *Bot) enqueuePriceUpdateBatch(ctx context.Context, batch pricedUpdateBatch, cycle priceCycleInputs) (int64, []pricedUpdate, error) {
	srcChain, err := b.registry.Get(batch.SrcEID)
	if err != nil {
		return 0, nil, err
	}
	updates := make([]PriceSnapshotUpdate, 0, len(batch.Targets))
	gasByKey := make(map[string]*big.Int, len(batch.Targets))
	snapshotsByKey := make(map[string]PriceSnapshot, len(batch.Targets))
	enqueuedTargets := make([]pricedUpdate, 0, len(batch.Targets))
	dstEIDs := make([]uint32, 0, len(batch.Targets))
	for _, target := range batch.Targets {
		dstChain, err := b.registry.Get(target.DstEID)
		if err != nil {
			return 0, nil, err
		}
		srcPrice, dstPrice, err := b.cyclePathwayPrices(cycle, target.SrcEID, target.DstEID)
		if err != nil {
			return 0, nil, err
		}
		dstGasPrice := cycle.gasWei[target.DstEID]
		if dstGasPrice == nil || dstGasPrice.Sign() <= 0 {
			return 0, nil, fmt.Errorf("pricing gas source for chain %d is missing from prepared cycle", target.DstEID)
		}
		dstDataFeePerByte, err := b.currentDstDataFeePerByte(target.DstEID)
		if err != nil {
			return 0, nil, err
		}
		snapshot, err := BuildPriceSnapshot(PriceInputs{
			SrcNativeUSD:         srcPrice,
			DstNativeUSD:         dstPrice,
			DstGasPriceWei:       dstGasPrice,
			DstDataFeePerByteWei: dstDataFeePerByte,
			UpdatedAtUnix:        uint64(b.now().Unix()),
			StaleAfterSeconds:    uint64(b.settings.StaleAfter.Seconds()),
		})
		if err != nil {
			return 0, nil, err
		}
		write, deviation, elapsed, err := b.shouldWriteSnapshot(ctx, target, snapshot)
		if err != nil {
			return 0, nil, err
		}
		if !write {
			b.logger.Debug("skipped price snapshot update", "reason", "below_deviation_and_heartbeat", "src_eid", target.SrcEID, "dst_eid", target.DstEID, "price_feed", target.PriceFeed, "deviation_bps", deviation, "min_update_deviation_bps", b.settings.MinUpdateDeviation, "seconds_since_last_write", uint64(elapsed.Seconds()), "heartbeat_seconds", uint64(b.settings.Heartbeat.Seconds()))
			continue
		}
		updates = append(updates, PriceSnapshotUpdate{DstEid: dstChain.EID, Snapshot: snapshot})
		key := priceUpdateKey(target)
		gasByKey[key] = bigutil.Clone(dstGasPrice)
		snapshotsByKey[key] = snapshot
		enqueuedTargets = append(enqueuedTargets, target)
		dstEIDs = append(dstEIDs, dstChain.EID)
	}
	if len(updates) == 0 {
		return 0, nil, nil
	}
	tx, err := BuildSetPriceSnapshotTx(srcChain.EID, batch.PriceFeed, b.settings.SignerID, updates)
	if err != nil {
		return 0, nil, err
	}
	id, err := b.store.EnqueueTx(ctx, tx)
	if errors.Is(err, db.ErrTxSendScopeInactive) {
		// The chain was paused/disabled; skip this cycle's update for it instead
		// of queueing snapshots that would all go stale behind the pause.
		b.logger.Debug("skipped price update tx enqueue", "reason", "send_scope_inactive", "src_eid", srcChain.EID, "price_feed", batch.PriceFeed)
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}
	if b.lastGasPrices == nil {
		b.lastGasPrices = make(map[string]*big.Int)
	}
	for key, gas := range gasByKey {
		b.lastGasPrices[key] = bigutil.Clone(gas)
		snapshot := snapshotsByKey[key]
		b.lastWritten[key] = writtenPrice{price: bigutil.Clone(snapshot.DstGasPriceInSrcToken), writtenAt: time.Unix(int64(snapshot.UpdatedAt), 0)}
	}
	b.logger.Info("price update tx enqueued", "tx_outbox_id", id, "purpose", TxPurposeSetPriceSnapshot, "src_eid", srcChain.EID, "dst_count", len(dstEIDs), "dst_eids", dstEIDs, "price_feed", batch.PriceFeed)
	return id, enqueuedTargets, nil
}

func (b *Bot) shouldWriteSnapshot(ctx context.Context, update pricedUpdate, snapshot PriceSnapshot) (bool, uint64, time.Duration, error) {
	previous, exists, err := b.loadWrittenPrice(ctx, update)
	if err != nil {
		return false, 0, 0, err
	}
	if !exists {
		return true, ^uint64(0), 0, nil
	}
	deviation := PriceChangeBps(previous.price, snapshot.DstGasPriceInSrcToken)
	now := b.now()
	elapsed := now.Sub(previous.writtenAt)
	if elapsed < 0 {
		elapsed = 0
	}
	if deviation >= b.settings.MinUpdateDeviation || elapsed >= b.settings.Heartbeat {
		return true, deviation, elapsed, nil
	}
	return false, deviation, elapsed, nil
}

func (b *Bot) loadWrittenPrice(ctx context.Context, update pricedUpdate) (writtenPrice, bool, error) {
	key := priceUpdateKey(update)
	if b.loadedWritten[key] {
		previous, ok := b.lastWritten[key]
		return previous, ok, nil
	}
	snapshot, err := b.snapshots.PriceSnapshot(ctx, update.SrcEID, update.PriceFeed, update.DstEID)
	if err != nil {
		return writtenPrice{}, false, err
	}
	if snapshot.UpdatedAt == 0 && snapshot.DstGasPriceInSrcToken != nil && snapshot.DstGasPriceInSrcToken.Sign() == 0 {
		b.loadedWritten[key] = true
		return writtenPrice{}, false, nil
	}
	if err := snapshot.Validate(); err != nil {
		return writtenPrice{}, false, fmt.Errorf("on-chain price snapshot for %d -> %d is invalid: %w", update.SrcEID, update.DstEID, err)
	}
	previous := writtenPrice{
		price:     bigutil.Clone(snapshot.DstGasPriceInSrcToken),
		writtenAt: time.Unix(int64(snapshot.UpdatedAt), 0),
	}
	b.loadedWritten[key] = true
	b.lastWritten[key] = previous
	return previous, true, nil
}

func (b *Bot) preparePriceCycle(ctx context.Context, updates []pricedUpdate, knownGas map[uint32]*big.Int) (priceCycleInputs, error) {
	cycle := priceCycleInputs{nativeUSD: make(map[uint32]*big.Rat), gasWei: make(map[uint32]*big.Int)}
	if len(updates) == 0 {
		return cycle, nil
	}
	priceEIDs := make(map[uint32]struct{})
	gasEIDs := make(map[uint32]struct{})
	for _, update := range updates {
		src, srcOK := b.sources[update.SrcEID]
		dst, dstOK := b.sources[update.DstEID]
		if !srcOK || !dstOK {
			return priceCycleInputs{}, fmt.Errorf("pricing sources for pathway %d -> %d are not configured", update.SrcEID, update.DstEID)
		}
		if src.NativeAssetID == "" || src.NativeAssetID != dst.NativeAssetID {
			priceEIDs[update.SrcEID] = struct{}{}
			priceEIDs[update.DstEID] = struct{}{}
		}
		gasEIDs[update.DstEID] = struct{}{}
	}
	policy := PriceSelectionPolicy{
		MaxDeviationBps:      b.settings.MaxDeviation,
		SourceRequestTimeout: b.settings.SourceRequestTimeout,
		Now:                  b.now,
		OnSourceFailure:      b.logSourceFailure,
	}
	for eid := range priceEIDs {
		price, err := ChainNativePrice(ctx, b.sources, eid, policy)
		if err != nil {
			return priceCycleInputs{}, err
		}
		cycle.nativeUSD[eid] = price
	}
	for eid := range gasEIDs {
		if gas := knownGas[eid]; gas != nil {
			cycle.gasWei[eid] = bigutil.Clone(gas)
			continue
		}
		gas, err := b.currentDstGasPrice(ctx, eid)
		if err != nil {
			return priceCycleInputs{}, err
		}
		cycle.gasWei[eid] = gas
	}
	return cycle, nil
}

func (b *Bot) logSourceFailure(failure PriceSourceFailure) {
	attributes := []any{
		"eid", failure.EID,
		"source", failure.Source,
		"role", failure.Role,
		"category", failure.Category,
	}
	if failure.DeviationBps > 0 {
		attributes = append(attributes, "deviation_bps", failure.DeviationBps)
	}
	if failure.Err != nil {
		attributes = append(attributes, "error", failure.Err.Error())
	}
	b.logger.Warn("price source rejected", attributes...)
}

func (b *Bot) cyclePathwayPrices(cycle priceCycleInputs, srcEID, dstEID uint32) (*big.Rat, *big.Rat, error) {
	src, srcOK := b.sources[srcEID]
	dst, dstOK := b.sources[dstEID]
	if !srcOK || !dstOK {
		return nil, nil, fmt.Errorf("pricing sources for pathway %d -> %d are not configured", srcEID, dstEID)
	}
	if src.NativeAssetID != "" && src.NativeAssetID == dst.NativeAssetID {
		return big.NewRat(1, 1), big.NewRat(1, 1), nil
	}
	srcPrice, dstPrice := cycle.nativeUSD[srcEID], cycle.nativeUSD[dstEID]
	if srcPrice == nil || dstPrice == nil {
		return nil, nil, fmt.Errorf("pricing market inputs for pathway %d -> %d are missing from prepared cycle", srcEID, dstEID)
	}
	return bigutil.CloneRat(srcPrice), bigutil.CloneRat(dstPrice), nil
}

func (b *Bot) readDestinationGasPrices(ctx context.Context, updates []pricedUpdate) (map[uint32]*big.Int, error) {
	prices := make(map[uint32]*big.Int)
	for _, update := range updates {
		if prices[update.DstEID] != nil {
			continue
		}
		price, err := b.currentDstGasPrice(ctx, update.DstEID)
		if err != nil {
			return nil, err
		}
		prices[update.DstEID] = price
	}
	return prices, nil
}

func (b *Bot) currentDstGasPrice(ctx context.Context, dstEID uint32) (*big.Int, error) {
	dstSources, ok := b.sources[dstEID]
	if !ok || dstSources.Gas == nil {
		return nil, fmt.Errorf("pricing gas source for chain %d is not configured", dstEID)
	}
	dstGasPrice, err := dstSources.Gas.SuggestGasPrice(ctx)
	if err != nil {
		return nil, err
	}
	if dstGasPrice == nil || dstGasPrice.Sign() <= 0 {
		return nil, fmt.Errorf("pricing gas source for chain %d returned non-positive gas price", dstEID)
	}
	return dstGasPrice, nil
}

func (b *Bot) currentDstDataFeePerByte(dstEID uint32) (*big.Int, error) {
	dstSources, ok := b.sources[dstEID]
	if !ok || dstSources.DataFeePerByteWei == nil {
		return nil, fmt.Errorf("pricing data fee source for chain %d is not configured", dstEID)
	}
	if dstSources.DataFeePerByteWei.Sign() < 0 {
		return nil, fmt.Errorf("pricing data fee source for chain %d returned negative data fee", dstEID)
	}
	return bigutil.Clone(dstSources.DataFeePerByteWei), nil
}

func (b *Bot) uniquePriceUpdates() ([]pricedUpdate, error) {
	if b == nil || b.registry == nil {
		return nil, nil
	}
	seen := make(map[string]pricedUpdate)
	seenFeeModels := make(map[string]FeeModel)
	for _, pathway := range b.registry.Pathways() {
		executorFee, err := feeModelFromConfig(pathway.Pricing.ExecutorFee)
		if err != nil {
			return nil, fmt.Errorf("pathway %d -> %d pricing.executor_fee: %w", pathway.SrcEID, pathway.DstEID, err)
		}
		dvnFee, err := feeModelFromConfig(pathway.Pricing.DVNFee)
		if err != nil {
			return nil, fmt.Errorf("pathway %d -> %d pricing.dvn_fee: %w", pathway.SrcEID, pathway.DstEID, err)
		}
		if err := rememberFeeModel(seenFeeModels, pathway.SrcEID, pathway.DstEID, "executor", pathway.SourceWorkers.OpenExecutor, executorFee); err != nil {
			return nil, err
		}
		if err := rememberFeeModel(seenFeeModels, pathway.SrcEID, pathway.DstEID, "dvn", pathway.SourceWorkers.OpenDVN, dvnFee); err != nil {
			return nil, err
		}
		update := pricedUpdate{SrcEID: pathway.SrcEID, DstEID: pathway.DstEID, PriceFeed: pathway.SourceWorkers.PriceFeed}
		key := priceUpdateKey(update)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = update
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	updates := make([]pricedUpdate, 0, len(keys))
	for _, key := range keys {
		updates = append(updates, seen[key])
	}
	return updates, nil
}

func priceUpdateKey(update pricedUpdate) string {
	return fmt.Sprintf("%d:%d:%s", update.SrcEID, update.DstEID, update.PriceFeed)
}

func priceUpdateBatchKey(update pricedUpdate) string {
	return fmt.Sprintf("%d:%s", update.SrcEID, update.PriceFeed)
}

func priceUpdateBatches(updates []pricedUpdate) []pricedUpdateBatch {
	seen := make(map[string]pricedUpdateBatch)
	for _, update := range updates {
		key := priceUpdateBatchKey(update)
		batch := seen[key]
		if batch.Targets == nil {
			batch.SrcEID = update.SrcEID
			batch.PriceFeed = update.PriceFeed
		}
		batch.Targets = append(batch.Targets, update)
		seen[key] = batch
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	batches := make([]pricedUpdateBatch, 0, len(keys))
	for _, key := range keys {
		batch := seen[key]
		sort.Slice(batch.Targets, func(i, j int) bool {
			if batch.Targets[i].DstEID != batch.Targets[j].DstEID {
				return batch.Targets[i].DstEID < batch.Targets[j].DstEID
			}
			return priceUpdateKey(batch.Targets[i]) < priceUpdateKey(batch.Targets[j])
		})
		batches = append(batches, batch)
	}
	return batches
}

func spikeUpdates(spikes []pricedGasSpike) []pricedUpdate {
	updates := make([]pricedUpdate, 0, len(spikes))
	for _, spike := range spikes {
		updates = append(updates, spike.update)
	}
	return updates
}

func rememberFeeModel(seen map[string]FeeModel, srcEID, dstEID uint32, role string, worker common.Address, fee FeeModel) error {
	key := fmt.Sprintf("%d:%d:%s:%s", srcEID, dstEID, role, worker)
	if existing, ok := seen[key]; ok {
		if !feeModelsEqual(existing, fee) {
			return fmt.Errorf("conflicting %s fee model for %d -> %d worker %s", role, srcEID, dstEID, worker)
		}
		return nil
	}
	seen[key] = fee
	return nil
}

func feeModelFromConfig(cfg config.WorkerFeeModelConfig) (FeeModel, error) {
	if cfg.FixedFeeWei == "" {
		return FeeModel{}, errors.New("fixed_fee_wei is required")
	}
	fixedFee, err := bigutil.ParseNonNegativeDecimal("fixed_fee_wei", cfg.FixedFeeWei)
	if err != nil {
		return FeeModel{}, errors.New("fixed_fee_wei must be a non-negative integer")
	}
	if cfg.DataSizeOverheadBytes == nil {
		return FeeModel{}, errors.New("data_size_overhead_bytes is required")
	}
	model := FeeModel{
		FixedFee:              fixedFee,
		DstGasOverhead:        cfg.DstGasOverhead,
		DataSizeOverheadBytes: *cfg.DataSizeOverheadBytes,
		MarginBps:             cfg.MarginBps,
	}
	if err := model.Validate("fee model"); err != nil {
		return FeeModel{}, err
	}
	return model, nil
}

func feeModelsEqual(left, right FeeModel) bool {
	if left.FixedFee == nil || right.FixedFee == nil {
		return left.FixedFee == right.FixedFee &&
			left.DstGasOverhead == right.DstGasOverhead &&
			left.DataSizeOverheadBytes == right.DataSizeOverheadBytes &&
			left.MarginBps == right.MarginBps
	}
	return left.FixedFee.Cmp(right.FixedFee) == 0 &&
		left.DstGasOverhead == right.DstGasOverhead &&
		left.DataSizeOverheadBytes == right.DataSizeOverheadBytes &&
		left.MarginBps == right.MarginBps
}

// SourcePrice is one USD/native price observed from a configured data source.
type SourcePrice struct {
	Source     string
	USD        *big.Rat
	ObservedAt time.Time
}

// SelectPrice applies the primary/sanity policy for one sanity source.
func SelectPrice(primary, sanity SourcePrice, maxDeviationBps uint64) (*big.Rat, error) {
	return SelectPriceWithSanity(primary, []SourcePrice{sanity}, maxDeviationBps)
}

// SelectPriceWithSanity validates every healthy sanity price without ever replacing the primary.
func SelectPriceWithSanity(primary SourcePrice, sanityPrices []SourcePrice, maxDeviationBps uint64) (*big.Rat, error) {
	if primary.USD == nil || primary.USD.Sign() <= 0 {
		return nil, errors.New("primary price source is unhealthy")
	}
	for _, sanity := range sanityPrices {
		if sanity.USD == nil || sanity.USD.Sign() <= 0 {
			return nil, fmt.Errorf("sanity price source %s is unhealthy", sanity.Source)
		}
		deviation := DeviationBps(primary.USD, sanity.USD)
		if deviation > maxDeviationBps {
			return nil, fmt.Errorf("price deviation between %s and %s is %d bps, exceeds limit %d bps", primary.Source, sanity.Source, deviation, maxDeviationBps)
		}
	}
	return bigutil.CloneRat(primary.USD), nil
}

const sourceFutureTolerance = 30 * time.Second

type configuredPriceResult struct {
	configured ConfiguredPriceReader
	price      SourcePrice
	err        error
}

// ChainNativePrice reads one chain's selected USD/native price with the configured source-selection policy.
func ChainNativePrice(ctx context.Context, sources map[uint32]ChainSources, eid uint32, policy PriceSelectionPolicy) (*big.Rat, error) {
	chainSources, ok := sources[eid]
	if !ok {
		return nil, fmt.Errorf("pricing sources for chain %d are not configured", eid)
	}
	if chainSources.Primary.Reader == nil {
		return nil, fmt.Errorf("primary price source for chain %d is not configured", eid)
	}
	if policy.SourceRequestTimeout <= 0 {
		return nil, errors.New("price source request timeout must be positive")
	}
	now := time.Now
	if policy.Now != nil {
		now = policy.Now
	}
	configured := make([]ConfiguredPriceReader, 0, len(chainSources.Sanity)+1)
	configured = append(configured, chainSources.Primary)
	configured = append(configured, chainSources.Sanity...)
	sourceNames := make(map[string]struct{}, len(configured))
	for _, source := range configured {
		if source.Name == "" || source.Reader == nil {
			return nil, fmt.Errorf("pricing chain %d contains an incomplete price source", eid)
		}
		if source.MaxAge <= 0 {
			return nil, fmt.Errorf("%s max age must be positive", source.Name)
		}
		if _, duplicate := sourceNames[source.Name]; duplicate {
			return nil, fmt.Errorf("pricing chain %d contains duplicate source %s", eid, source.Name)
		}
		sourceNames[source.Name] = struct{}{}
	}
	requestCtx, cancel := context.WithTimeout(ctx, policy.SourceRequestTimeout)
	defer cancel()
	results := make(chan configuredPriceResult, len(configured))
	for _, source := range configured {
		go func(source ConfiguredPriceReader) {
			price, err := source.Reader.PriceUSD(requestCtx)
			results <- configuredPriceResult{configured: source, price: price, err: err}
		}(source)
	}
	completed := make(map[string]configuredPriceResult, len(configured))
	for len(completed) < len(configured) {
		select {
		case result := <-results:
			if _, duplicate := completed[result.configured.Name]; duplicate {
				continue
			}
			completed[result.configured.Name] = result
		case <-requestCtx.Done():
			for _, source := range configured {
				if _, ok := completed[source.Name]; ok {
					continue
				}
				completed[source.Name] = configuredPriceResult{configured: source, err: requestCtx.Err()}
			}
		}
	}
	validationNow := now()
	primaryResult := completed[chainSources.Primary.Name]
	primary := primaryResult.price
	primaryErr := primaryResult.err
	if primaryErr == nil {
		primaryErr = validateObservedPrice(chainSources.Primary, primary, validationNow)
	}
	sanityPrices := make([]SourcePrice, 0, len(chainSources.Sanity))
	var sanityErrs []error
	for _, source := range chainSources.Sanity {
		result := completed[source.Name]
		err := result.err
		if err == nil {
			err = validateObservedPrice(source, result.price, validationNow)
		}
		if err != nil {
			sanityErrs = append(sanityErrs, sanitySourceError{source: source.Name, err: err})
			continue
		}
		sanityPrices = append(sanityPrices, result.price)
	}
	if primaryErr != nil {
		notifyPriceSourceFailure(policy, PriceSourceFailure{
			EID: eid, Source: chainSources.Primary.Name, Role: "primary", Category: priceSourceFailureCategory(primaryErr), Err: primaryErr,
		})
		return nil, fmt.Errorf("%s primary source for chain %d: %w", chainSources.Primary.Name, eid, primaryErr)
	}
	for _, err := range sanityErrs {
		if sourceErr, ok := errors.AsType[sanitySourceError](err); ok {
			notifyPriceSourceFailure(policy, PriceSourceFailure{
				EID: eid, Source: sourceErr.source, Role: "sanity", Category: priceSourceFailureCategory(sourceErr.err), Err: sourceErr.err,
			})
		}
	}
	if len(chainSources.Sanity) > 0 && len(sanityPrices) == 0 {
		err := fmt.Errorf("no healthy sanity price source for chain %d", eid)
		if len(sanityErrs) > 0 {
			return nil, errors.Join(append([]error{err}, sanityErrs...)...)
		}
		return nil, err
	}
	for _, sanity := range sanityPrices {
		deviation := DeviationBps(primary.USD, sanity.USD)
		if deviation > policy.MaxDeviationBps {
			err := fmt.Errorf("price deviation between %s and %s is %d bps, exceeds limit %d bps", primary.Source, sanity.Source, deviation, policy.MaxDeviationBps)
			notifyPriceSourceFailure(policy, PriceSourceFailure{
				EID: eid, Source: sanity.Source, Role: "sanity", Category: "deviation", DeviationBps: deviation, Err: err,
			})
			return nil, err
		}
	}
	return SelectPriceWithSanity(primary, sanityPrices, policy.MaxDeviationBps)
}

type sanitySourceError struct {
	source string
	err    error
}

func (e sanitySourceError) Error() string {
	return fmt.Sprintf("%s sanity source: %v", e.source, e.err)
}
func (e sanitySourceError) Unwrap() error { return e.err }

func notifyPriceSourceFailure(policy PriceSelectionPolicy, failure PriceSourceFailure) {
	if policy.OnSourceFailure != nil {
		policy.OnSourceFailure(failure)
	}
}

func priceSourceFailureCategory(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case strings.Contains(err.Error(), "stale"):
		return "stale"
	case strings.Contains(err.Error(), "future"):
		return "future"
	case strings.Contains(err.Error(), "non-positive"):
		return "non_positive"
	case strings.Contains(err.Error(), "missing observation time"):
		return "missing_time"
	default:
		return "unavailable"
	}
}

func validateObservedPrice(source ConfiguredPriceReader, price SourcePrice, now time.Time) error {
	if source.Name == "" || source.Reader == nil {
		return errors.New("price source is not configured")
	}
	if source.MaxAge <= 0 {
		return fmt.Errorf("%s max age must be positive", source.Name)
	}
	if price.Source == "" {
		return fmt.Errorf("%s returned missing source name", source.Name)
	}
	if price.Source != source.Name {
		return fmt.Errorf("%s returned unexpected source %q", source.Name, price.Source)
	}
	if price.USD == nil || price.USD.Sign() <= 0 {
		return fmt.Errorf("%s returned non-positive price", source.Name)
	}
	if price.ObservedAt.IsZero() {
		return fmt.Errorf("%s returned missing observation time", source.Name)
	}
	if price.ObservedAt.After(now.Add(sourceFutureTolerance)) {
		return fmt.Errorf("%s observation time is too far in the future", source.Name)
	}
	if now.Sub(price.ObservedAt) > source.MaxAge {
		return fmt.Errorf("%s observation is stale", source.Name)
	}
	return nil
}

// PathwayNativePrices returns source and destination USD/native prices, using 1:1 for shared native assets.
func PathwayNativePrices(ctx context.Context, sources map[uint32]ChainSources, srcEID, dstEID uint32, policy PriceSelectionPolicy) (*big.Rat, *big.Rat, error) {
	srcSources, ok := sources[srcEID]
	if !ok {
		return nil, nil, fmt.Errorf("pricing sources for chain %d are not configured", srcEID)
	}
	dstSources, ok := sources[dstEID]
	if !ok {
		return nil, nil, fmt.Errorf("pricing sources for chain %d are not configured", dstEID)
	}
	if srcSources.NativeAssetID != "" && srcSources.NativeAssetID == dstSources.NativeAssetID {
		return big.NewRat(1, 1), big.NewRat(1, 1), nil
	}
	srcPrice, err := ChainNativePrice(ctx, sources, srcEID, policy)
	if err != nil {
		return nil, nil, err
	}
	dstPrice, err := ChainNativePrice(ctx, sources, dstEID, policy)
	if err != nil {
		return nil, nil, err
	}
	return srcPrice, dstPrice, nil
}

// ConvertDstWeiToSrcWei converts destination-chain native wei into source-chain native wei.
func ConvertDstWeiToSrcWei(dstWei *big.Int, srcNativeUSD, dstNativeUSD *big.Rat) (*big.Int, error) {
	if dstWei == nil || dstWei.Sign() < 0 {
		return nil, errors.New("destination wei must be non-negative")
	}
	if srcNativeUSD == nil || srcNativeUSD.Sign() <= 0 {
		return nil, errors.New("source native USD price must be positive")
	}
	if dstNativeUSD == nil || dstNativeUSD.Sign() <= 0 {
		return nil, errors.New("destination native USD price must be positive")
	}
	cost := new(big.Rat).SetInt(dstWei)
	cost.Mul(cost, dstNativeUSD)
	cost.Quo(cost, srcNativeUSD)
	return bigutil.CeilRat(cost), nil
}

// GasIncreaseBps returns max((current-previous)/previous, 0) in basis points.
func GasIncreaseBps(previous, current *big.Int) uint64 {
	if previous == nil || current == nil || previous.Sign() <= 0 || current.Sign() <= 0 {
		return ^uint64(0)
	}
	if current.Cmp(previous) <= 0 {
		return 0
	}
	diff := new(big.Int).Sub(current, previous)
	ratio := new(big.Rat).SetFrac(diff, previous)
	ratio.Mul(ratio, big.NewRat(10_000, 1))
	bps := bigutil.CeilRat(ratio)
	if !bps.IsUint64() {
		return ^uint64(0)
	}
	return bps.Uint64()
}

// PriceChangeBps returns abs(current-previous)/previous in basis points.
func PriceChangeBps(previous, current *big.Int) uint64 {
	if previous == nil || current == nil || previous.Sign() <= 0 || current.Sign() <= 0 {
		return ^uint64(0)
	}
	diff := new(big.Int).Sub(current, previous)
	if diff.Sign() < 0 {
		diff.Neg(diff)
	}
	diff.Mul(diff, big.NewInt(10_000))
	bps := diff.Quo(diff, previous)
	if !bps.IsUint64() {
		return ^uint64(0)
	}
	return bps.Uint64()
}

// DeviationBps returns abs(left-right)/min(left,right) in basis points.
func DeviationBps(left, right *big.Rat) uint64 {
	if left == nil || right == nil || left.Sign() <= 0 || right.Sign() <= 0 {
		return ^uint64(0)
	}
	diff := new(big.Rat).Sub(left, right)
	if diff.Sign() < 0 {
		diff.Neg(diff)
	}
	diff.Mul(diff, big.NewRat(10_000, 1))
	denominator := left
	if right.Cmp(left) < 0 {
		denominator = right
	}
	diff.Quo(diff, denominator)
	bps := bigutil.CeilRat(diff)
	if !bps.IsUint64() {
		return ^uint64(0)
	}
	return bps.Uint64()
}

// PriceInputs are the source data used to construct WorkerTypes.PriceSnapshot.
type PriceInputs struct {
	SrcNativeUSD         *big.Rat
	DstNativeUSD         *big.Rat
	DstGasPriceWei       *big.Int
	DstDataFeePerByteWei *big.Int
	UpdatedAtUnix        uint64
	StaleAfterSeconds    uint64
}

// PriceSnapshot mirrors WorkerTypes.PriceSnapshot for ABI encoding.
type PriceSnapshot struct {
	DstGasPriceInSrcToken       *big.Int `abi:"dstGasPriceInSrcToken"`
	DstDataFeePerByteInSrcToken *big.Int `abi:"dstDataFeePerByteInSrcToken"`
	UpdatedAt                   uint64   `abi:"updatedAt"`
	StaleAfter                  uint64   `abi:"staleAfter"`
}

// PriceSnapshotUpdate mirrors WorkerTypes.PriceSnapshotUpdate for ABI encoding.
type PriceSnapshotUpdate struct {
	DstEid   uint32        `abi:"dstEid"`
	Snapshot PriceSnapshot `abi:"snapshot"`
}

// BuildPriceSnapshot converts destination gas cost into source native-token units.
func BuildPriceSnapshot(inputs PriceInputs) (PriceSnapshot, error) {
	if inputs.SrcNativeUSD == nil || inputs.SrcNativeUSD.Sign() <= 0 {
		return PriceSnapshot{}, errors.New("source native USD price must be positive")
	}
	if inputs.DstNativeUSD == nil || inputs.DstNativeUSD.Sign() <= 0 {
		return PriceSnapshot{}, errors.New("destination native USD price must be positive")
	}
	if inputs.DstGasPriceWei == nil || inputs.DstGasPriceWei.Sign() <= 0 {
		return PriceSnapshot{}, errors.New("destination gas price must be positive")
	}
	if inputs.DstDataFeePerByteWei == nil || inputs.DstDataFeePerByteWei.Sign() < 0 {
		return PriceSnapshot{}, errors.New("destination data fee per byte must be non-negative")
	}
	if inputs.UpdatedAtUnix == 0 {
		return PriceSnapshot{}, errors.New("updated_at is required")
	}
	if inputs.StaleAfterSeconds == 0 {
		return PriceSnapshot{}, errors.New("stale_after is required")
	}
	if inputs.StaleAfterSeconds > config.MaxPriceSnapshotStaleAfterSeconds {
		return PriceSnapshot{}, fmt.Errorf("stale_after exceeds OpenPriceFeed maximum %d", config.MaxPriceSnapshotStaleAfterSeconds)
	}
	dstGasPriceInSrcToken := new(big.Rat).SetInt(inputs.DstGasPriceWei)
	dstGasPriceInSrcToken.Mul(dstGasPriceInSrcToken, inputs.DstNativeUSD)
	dstGasPriceInSrcToken.Quo(dstGasPriceInSrcToken, inputs.SrcNativeUSD)
	dstDataFeePerByteInSrcToken := new(big.Rat).SetInt(inputs.DstDataFeePerByteWei)
	dstDataFeePerByteInSrcToken.Mul(dstDataFeePerByteInSrcToken, inputs.DstNativeUSD)
	dstDataFeePerByteInSrcToken.Quo(dstDataFeePerByteInSrcToken, inputs.SrcNativeUSD)
	snapshot := PriceSnapshot{
		DstGasPriceInSrcToken:       bigutil.CeilRat(dstGasPriceInSrcToken),
		DstDataFeePerByteInSrcToken: bigutil.CeilRat(dstDataFeePerByteInSrcToken),
		UpdatedAt:                   inputs.UpdatedAtUnix,
		StaleAfter:                  inputs.StaleAfterSeconds,
	}
	if err := snapshot.Validate(); err != nil {
		return PriceSnapshot{}, err
	}
	return snapshot, nil
}

// BuildSetPriceSnapshotCalldata ABI-encodes OpenPriceFeed setPriceSnapshot.
func BuildSetPriceSnapshotCalldata(updates []PriceSnapshotUpdate) ([]byte, error) {
	if len(updates) == 0 {
		return nil, errors.New("price snapshot updates are required")
	}
	for _, update := range updates {
		if update.DstEid == 0 {
			return nil, errors.New("destination eid is required")
		}
		if err := update.Snapshot.Validate(); err != nil {
			return nil, err
		}
	}
	return priceSnapshotABI.Pack("setPriceSnapshot", updates)
}

// BuildSetPriceSnapshotTx creates an outbox transaction for a shared price feed update.
func BuildSetPriceSnapshotTx(chainEID uint32, priceFeed common.Address, signerID string, updates []PriceSnapshotUpdate) (db.TxRequest, error) {
	if chainEID == 0 {
		return db.TxRequest{}, errors.New("chain eid is required")
	}
	if priceFeed == (common.Address{}) {
		return db.TxRequest{}, errors.New("price feed address is required")
	}
	if signerID == "" {
		return db.TxRequest{}, errors.New("signer id is required")
	}
	calldata, err := BuildSetPriceSnapshotCalldata(updates)
	if err != nil {
		return db.TxRequest{}, err
	}
	return db.TxRequest{
		ChainEID: chainEID,
		Purpose:  TxPurposeSetPriceSnapshot,
		To:       priceFeed,
		Calldata: calldata,
		Value:    new(big.Int),
		SignerID: signerID,
	}, nil
}

// Validate checks the on-chain price snapshot invariants the worker can know before sending.
func (s PriceSnapshot) Validate() error {
	if s.DstGasPriceInSrcToken == nil || s.DstGasPriceInSrcToken.Sign() <= 0 {
		return errors.New("price snapshot destination gas price must be positive")
	}
	if s.DstGasPriceInSrcToken.Cmp(maxUint256) > 0 {
		return errors.New("price snapshot destination gas price exceeds uint256")
	}
	if s.DstDataFeePerByteInSrcToken == nil || s.DstDataFeePerByteInSrcToken.Sign() < 0 {
		return errors.New("price snapshot destination data fee per byte must be non-negative")
	}
	if s.DstDataFeePerByteInSrcToken.Cmp(maxUint256) > 0 {
		return errors.New("price snapshot destination data fee per byte exceeds uint256")
	}
	if s.UpdatedAt == 0 {
		return errors.New("price snapshot updated_at is required")
	}
	if s.StaleAfter == 0 {
		return errors.New("price snapshot stale_after is required")
	}
	if s.StaleAfter > config.MaxPriceSnapshotStaleAfterSeconds {
		return fmt.Errorf("price snapshot stale_after exceeds OpenPriceFeed maximum %d", config.MaxPriceSnapshotStaleAfterSeconds)
	}
	return nil
}
