package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/islishude/oh-my-lazier/go/internal/bigutil"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/configcheck"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/dvn"
	"github.com/islishude/oh-my-lazier/go/internal/executor"
	"github.com/islishude/oh-my-lazier/go/internal/feeaccounting"
	"github.com/islishude/oh-my-lazier/go/internal/indexer"
	"github.com/islishude/oh-my-lazier/go/internal/metrics"
	"github.com/islishude/oh-my-lazier/go/internal/pricing"
	"github.com/islishude/oh-my-lazier/go/internal/readiness"
	"github.com/islishude/oh-my-lazier/go/internal/signer"
	"github.com/islishude/oh-my-lazier/go/internal/signer/keystore"
	"github.com/islishude/oh-my-lazier/go/internal/signer/kms"
	"github.com/islishude/oh-my-lazier/go/internal/txmgr"
	"github.com/islishude/oh-my-lazier/go/internal/workerloop"
)

var checkOnChainConfig = func(ctx context.Context, registry *chain.Registry, opts ...configcheck.Option) (configcheck.Report, error) {
	return configcheck.Check(ctx, registry, opts...)
}

var readTxHeader = func(ctx context.Context, client txmgr.ChainClient) (*gethtypes.Header, error) {
	return client.HeaderByNumber(ctx, nil)
}

func configCheckOptions(cfg config.Config) []configcheck.Option {
	if !cfg.Pricing.Enabled {
		return nil
	}
	return []configcheck.Option{configcheck.WithPricingSigner(cfg.Pricing.Signer.Common())}
}

const loopRestartDelay = 5 * time.Second

// DefaultIndexerProgressLogInterval is the default cadence for indexer progress Info logs.
const DefaultIndexerProgressLogInterval = indexer.DefaultProgressLogInterval

type loopRetryRecorder interface {
	RecordLoopRetry(name string)
}

// Options controls process-local worker runtime behavior outside the loaded config.
type Options struct {
	IndexerProgressLogInterval    time.Duration
	IndexerProgressLogIntervalSet bool
	SkipOnchainCheck              bool
}

// App owns the configured worker process and its durable service loops.
type App struct {
	cfg               config.Config
	logger            *slog.Logger
	options           Options
	pricingHTTPClient *http.Client
}

// New builds an App from already-validated configuration.
func New(cfg config.Config, logger *slog.Logger) (*App, error) {
	return NewWithOptions(cfg, logger, Options{})
}

// NewWithOptions builds an App with process-local runtime options.
func NewWithOptions(cfg config.Config, logger *slog.Logger, options Options) (*App, error) {
	if logger == nil {
		return nil, errors.New("app logger is required")
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return &App{cfg: cfg, logger: logger, options: normalized, pricingHTTPClient: http.DefaultClient}, nil
}

func normalizeOptions(options Options) (Options, error) {
	if !options.IndexerProgressLogIntervalSet {
		options.IndexerProgressLogInterval = DefaultIndexerProgressLogInterval
		options.IndexerProgressLogIntervalSet = true
	}
	if options.IndexerProgressLogInterval < 0 {
		return Options{}, errors.New("indexer progress log interval must be non-negative")
	}
	return options, nil
}

// Run connects dependencies and runs all worker loops until cancellation.
func (a *App) Run(ctx context.Context) error {
	if err := a.cfg.Validate(); err != nil {
		return err
	}
	registry, err := chain.NewRegistry(a.cfg.Chains, a.cfg.Pathways)
	if err != nil {
		return err
	}
	defer registry.Close()

	if a.options.SkipOnchainCheck {
		a.logger.Warn("worker starting with on-chain config check skipped")
	} else {
		a.logger.Info("worker starting and checking on-chain config...")
		if report, err := checkOnChainConfig(ctx, registry, configCheckOptions(a.cfg)...); err != nil {
			return err
		} else if !report.OK {
			return fmt.Errorf("on-chain config check failed: %s", configcheck.RenderText(report))
		}
	}
	var priceSources map[uint32]pricing.ChainSources
	if a.cfg.Pricing.Enabled {
		priceSources, err = a.pricingSources(registry)
		if err != nil {
			return err
		}
		if a.options.SkipOnchainCheck {
			// -skip-onchain-check bypasses the config check that normally
			// establishes each chain's head quorum before its first on-chain
			// read. On-chain source validation below (Chainlink, Uniswap)
			// reads contract identity over RPC, and a fresh quorum client
			// would serve that from whichever provider is listed first —
			// establish the majority trust state for those chains so a
			// minority fork cannot bias startup validation. Chains with only
			// HTTP market-data sources make no startup RPC reads and are
			// skipped.
			for _, pricingChain := range a.cfg.Pricing.Chains {
				if !pricingChainUsesSource(pricingChain, "chainlink") && !pricingChainUsesSource(pricingChain, "uniswap") {
					continue
				}
				configuredChain, err := registry.Get(pricingChain.EID)
				if err != nil {
					return err
				}
				if configuredChain.RPC == nil {
					continue
				}
				if _, err := configuredChain.RPC.CheckHead(ctx); err != nil {
					return fmt.Errorf("check chain %d rpc head quorum: %w", pricingChain.EID, err)
				}
			}
		}
		if err := pricing.ValidateSourceConfigurations(ctx, priceSources, a.priceSelectionPolicy()); err != nil {
			return err
		}
	}

	a.logger.Info("connecting to database and running migrations...")
	store, err := db.Connect(ctx, a.cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Ping(ctx); err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	if a.options.SkipOnchainCheck {
		// The bypassed config check is what normally establishes every chain's
		// head quorum before its first on-chain read. The pricing chains with
		// on-chain sources were probed before source validation above; probe
		// every remaining configured chain here, before the durable loops
		// start, so DVN destination-config validation, executor readiness
		// reads, and gas polls never run against a fresh all-healthy
		// classification that a forked first provider could exploit.
		for _, configuredChainConfig := range a.cfg.Chains {
			configuredChain, err := registry.Get(configuredChainConfig.EID)
			if err != nil {
				return err
			}
			if configuredChain.RPC == nil {
				continue
			}
			if _, err := configuredChain.RPC.CheckHead(ctx); err != nil {
				return fmt.Errorf("check chain %d rpc head quorum: %w", configuredChainConfig.EID, err)
			}
		}
	}

	a.logger.Info("synchronizing registry configuration...")
	if err := store.SyncConfig(ctx, registry); err != nil {
		return err
	}

	runtimeMetrics := metrics.NewRegistry()
	pathways := registry.Pathways()
	indexerStreams := indexer.StreamsForRoles(a.cfg.ExecutorEnabled(), a.cfg.DVNEnabled())
	txTargets, err := a.txTargets(ctx, registry, store)
	if err != nil {
		return err
	}
	var executorWorker *executor.Worker
	if a.cfg.ExecutorEnabled() {
		executorWorker = executor.New(store, registry, a.logger)
	}
	var dvnWorker *dvn.Worker
	if a.cfg.DVNEnabled() {
		dvnWorker, err = a.dvnWorker(store, registry)
		if err != nil {
			return err
		}
	}
	var priceBot *pricing.Bot
	var feeReconciler *feeaccounting.Reconciler
	if a.cfg.Pricing.Enabled {
		priceBot, err = a.priceBotWithSources(store, registry, priceSources)
		if err != nil {
			return err
		}
		priceBot.WithMetrics(runtimeMetrics)
		feeReconciler, err = feeaccounting.New(store, priceSources, feeaccounting.Settings{
			PriceSelection: a.priceSelectionPolicy(),
		}, a.logger)
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	start := func(name string, run func(context.Context) error) {
		wg.Go(func() {
			if err := superviseLoop(ctx, name, loopRestartDelay, a.logger, runtimeMetrics, run); err != nil {
				select {
				case errCh <- fmt.Errorf("%s loop failed fatally: %w", name, err):
				default:
				}
			}
		})
	}

	start("metrics", metrics.NewWithReadiness(a.cfg.Metrics.ListenAddress, store, a.logger, readiness.Services{
		ExecutorEnabled: a.cfg.ExecutorEnabled(),
		DVNEnabled:      a.cfg.DVNEnabled(),
	}, runtimeMetrics).Run)
	if !indexerStreams.Empty() {
		for _, c := range registry.All() {
			start("indexer."+c.Name, indexer.New(c, pathways, store, a.logger).
				WithStreams(indexerStreams).
				WithMetrics(runtimeMetrics).
				WithProgressLogInterval(a.options.IndexerProgressLogInterval).
				Run)
		}
	}
	if len(txTargets) > 0 {
		start("signer_balance", txmgr.NewBalanceMonitor(txTargets, runtimeMetrics, a.logger).Run)
		start("txmgr", txmgr.NewWithTargetsAndOptions(store, txTargets, a.logger, a.txManagerOptions()).Run)
	}
	if a.cfg.ExecutorEnabled() {
		start("executor.committer", executorWorker.RunCommitter)
		start("executor.deliverer", executorWorker.RunDeliverer)
	}
	if a.cfg.DVNEnabled() {
		start("dvn", dvnWorker.Run)
	}
	if a.cfg.Pricing.Enabled {
		start("fee_accounting", feeReconciler.Run)
		start("pricing", priceBot.Run)
	}

	select {
	case <-ctx.Done():
		wg.Wait()
		return nil
	case err := <-errCh:
		cancel()
		wg.Wait()
		return err
	}
}

func (a *App) txManagerOptions() txmgr.Options {
	return txmgr.Options{
		StaleBroadcastReplacementAfter: time.Duration(a.cfg.TxManager.StaleBroadcastReplacementAfterSeconds) * time.Second,
	}
}

func superviseLoop(ctx context.Context, name string, restartDelay time.Duration, logger *slog.Logger, retryMetrics loopRetryRecorder, run func(context.Context) error) error {
	for {
		err := run(ctx)
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			logger.Info("loop stopped", "name", name)
			return nil
		}
		if workerloop.IsFatal(err) {
			logger.Error("loop failed fatally", "name", name, "error", err)
			return err
		}
		if err != nil {
			logger.Error("loop failed; restarting", "name", name, "error", err)
			if retryMetrics != nil {
				retryMetrics.RecordLoopRetry(name)
			}
		} else {
			logger.Warn("loop stopped unexpectedly; restarting", "name", name)
		}
		if !waitLoopRestart(ctx, restartDelay) {
			logger.Info("loop stopped", "name", name)
			return nil
		}
	}
}

func waitLoopRestart(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// RunPriceOnce computes current shared price snapshots and enqueues one update batch.
func (a *App) RunPriceOnce(ctx context.Context) error {
	if !a.cfg.Pricing.Enabled {
		return errors.New("pricing is disabled")
	}
	registry, err := chain.NewRegistry(a.cfg.Chains, a.cfg.Pathways)
	if err != nil {
		return err
	}
	defer registry.Close()
	if report, err := checkOnChainConfig(ctx, registry, configCheckOptions(a.cfg)...); err != nil {
		return err
	} else if !report.OK {
		return fmt.Errorf("on-chain config check failed: %s", configcheck.RenderText(report))
	}

	store, err := db.Connect(ctx, a.cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Ping(ctx); err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	if err := store.SyncConfig(ctx, registry); err != nil {
		return err
	}

	priceBot, err := a.priceBot(store, registry)
	if err != nil {
		return err
	}
	return priceBot.EnqueueOnce(ctx)
}

func (a *App) priceBot(store *db.Store, registry *chain.Registry) (*pricing.Bot, error) {
	if !a.cfg.Pricing.Enabled {
		return pricing.New(a.logger), nil
	}
	sources, err := a.pricingSources(registry)
	if err != nil {
		return nil, err
	}
	return a.priceBotWithSources(store, registry, sources)
}

func (a *App) priceBotWithSources(store *db.Store, registry *chain.Registry, sources map[uint32]pricing.ChainSources) (*pricing.Bot, error) {
	if !a.cfg.Pricing.Enabled {
		return pricing.New(a.logger), nil
	}
	settings := pricing.Settings{
		Enabled:              true,
		SignerID:             a.cfg.Pricing.Signer.Hex(),
		Interval:             time.Duration(a.cfg.Pricing.IntervalSeconds) * time.Second,
		StaleAfter:           time.Duration(a.cfg.Pricing.StaleAfterSeconds) * time.Second,
		MaxDeviation:         a.cfg.Pricing.MaxDeviationBps,
		MinUpdateDeviation:   a.cfg.Pricing.MinUpdateDeviationBps,
		Heartbeat:            time.Duration(a.cfg.Pricing.HeartbeatSeconds) * time.Second,
		SourceRequestTimeout: time.Duration(a.cfg.Pricing.SourceRequestTimeoutSeconds) * time.Second,
		GasSpikeBps:          a.cfg.Pricing.GasSpikeBps,
	}
	return pricing.NewWithDependencies(store, registry, settings, sources, pricing.NewOnChainPriceSnapshotReader(registry), a.logger)
}

func (a *App) priceSelectionPolicy() pricing.PriceSelectionPolicy {
	return pricing.PriceSelectionPolicy{
		MaxDeviationBps:      a.cfg.Pricing.MaxDeviationBps,
		SourceRequestTimeout: time.Duration(a.cfg.Pricing.SourceRequestTimeoutSeconds) * time.Second,
		OnSourceFailure: func(failure pricing.PriceSourceFailure) {
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
			a.logger.Warn("price source rejected", attributes...)
		},
	}
}

func (a *App) pricingSources(registry *chain.Registry) (map[uint32]pricing.ChainSources, error) {
	var coinMarketCapClient *pricing.CoinMarketCapClient
	if pricingUsesSource(a.cfg.Pricing.Chains, "coinmarketcap") {
		var err error
		coinMarketCapClient, err = pricing.NewCoinMarketCapClient(a.cfg.Pricing.CoinMarketCapBaseURL, a.cfg.Pricing.CoinMarketCapAPIKeyEnv, a.pricingHTTPClient)
		if err != nil {
			return nil, err
		}
	}
	var coinGeckoClient *pricing.CoinGeckoClient
	if pricingUsesSource(a.cfg.Pricing.Chains, "coingecko") {
		var err error
		coinGeckoClient, err = pricing.NewCoinGeckoClient(a.cfg.Pricing.CoinGeckoBaseURL, a.cfg.Pricing.CoinGeckoAPIKeyEnv, a.pricingHTTPClient)
		if err != nil {
			return nil, err
		}
	}
	sources := make(map[uint32]pricing.ChainSources, len(a.cfg.Pricing.Chains))
	for _, cfg := range a.cfg.Pricing.Chains {
		configuredChain, err := registry.Get(cfg.EID)
		if err != nil {
			return nil, err
		}
		readers := make(map[string]pricing.ConfiguredPriceReader)
		if pricingChainUsesSource(cfg, "coinmarketcap") && coinMarketCapClient != nil {
			reader, err := pricing.NewCoinMarketCapPriceReader(coinMarketCapClient, cfg.CoinMarketCap.ID)
			if err != nil {
				return nil, err
			}
			readers["coinmarketcap"] = pricing.ConfiguredPriceReader{Name: "coinmarketcap", Reader: reader, MaxAge: time.Duration(cfg.CoinMarketCap.MaxAgeSeconds) * time.Second}
		}
		if pricingChainUsesSource(cfg, "coingecko") && coinGeckoClient != nil {
			reader, err := pricing.NewCoinGeckoPriceReader(coinGeckoClient, cfg.CoinGecko.ID)
			if err != nil {
				return nil, err
			}
			readers["coingecko"] = pricing.ConfiguredPriceReader{Name: "coingecko", Reader: reader, MaxAge: time.Duration(cfg.CoinGecko.MaxAgeSeconds) * time.Second}
		}
		if pricingChainUsesSource(cfg, "chainlink") {
			reader, err := pricing.NewChainlinkClient(configuredChain.RPC, configuredChain.RPC, pricing.ChainlinkConfig{
				FeedAddress:         cfg.Chainlink.FeedAddress.Common(),
				ExpectedDescription: cfg.Chainlink.ExpectedDescription,
			})
			if err != nil {
				return nil, err
			}
			readers["chainlink"] = pricing.ConfiguredPriceReader{Name: "chainlink", Reader: reader, MaxAge: time.Duration(cfg.Chainlink.MaxAgeSeconds) * time.Second}
		}
		if pricingChainUsesSource(cfg, "uniswap") {
			minimumLiquidity, err := bigutil.ParsePositiveDecimal("uniswap min_harmonic_mean_liquidity", cfg.Uniswap.MinHarmonicMeanLiquidity)
			if err != nil {
				return nil, err
			}
			sanity, err := pricing.NewUniswapV3Client(configuredChain.RPC, configuredChain.RPC, pricing.UniswapV3Config{
				PoolAddress:              cfg.Uniswap.PoolAddress.Common(),
				TokenIn:                  cfg.Uniswap.TokenIn.Common(),
				TokenOut:                 cfg.Uniswap.TokenOut.Common(),
				TWAPWindowSeconds:        uint32(cfg.Uniswap.TWAPWindowSeconds),
				MinHarmonicMeanLiquidity: minimumLiquidity,
			})
			if err != nil {
				return nil, err
			}
			readers["uniswap"] = pricing.ConfiguredPriceReader{Name: "uniswap", Reader: sanity, MaxAge: time.Duration(cfg.Uniswap.MaxBlockAgeSeconds) * time.Second}
		}
		var primary pricing.ConfiguredPriceReader
		var sanityReaders []pricing.ConfiguredPriceReader
		if cfg.PrimarySource != "" || len(cfg.SanitySources) != 0 {
			primary = readers[cfg.PrimarySource]
			if primary.Reader == nil {
				return nil, fmt.Errorf("pricing chain %d primary source %s is not configured", cfg.EID, cfg.PrimarySource)
			}
			sanityReaders = make([]pricing.ConfiguredPriceReader, 0, len(cfg.SanitySources))
			for _, source := range cfg.SanitySources {
				reader := readers[source]
				if reader.Reader == nil {
					return nil, fmt.Errorf("pricing chain %d sanity source %s is not configured", cfg.EID, source)
				}
				sanityReaders = append(sanityReaders, reader)
			}
		}
		dataFeePerByte, err := bigutil.ParseNonNegativeDecimal("data_fee_per_byte_wei", cfg.DataFeePerByteWei)
		if err != nil {
			return nil, fmt.Errorf("pricing chain %d data fee per byte: %w", cfg.EID, err)
		}
		sources[cfg.EID] = pricing.ChainSources{
			Primary:           primary,
			Sanity:            sanityReaders,
			Gas:               configuredChain.RPC,
			DataFeePerByteWei: dataFeePerByte,
			NativeAssetID:     cfg.NativeAssetID,
		}
	}
	return sources, nil
}

func pricingUsesSource(chains []config.PricingChainConfig, source string) bool {
	for _, chain := range chains {
		if chain.PrimarySource == source {
			return true
		}
		if slices.Contains(chain.SanitySources, source) {
			return true
		}
	}
	return false
}

func pricingChainUsesSource(chain config.PricingChainConfig, source string) bool {
	if chain.PrimarySource == source {
		return true
	}
	return slices.Contains(chain.SanitySources, source)
}

func (a *App) dvnWorker(store *db.Store, registry *chain.Registry) (*dvn.Worker, error) {
	settings := make(map[uint32]dvn.Settings)
	for _, pathway := range registry.Pathways() {
		if pathway.DVNMode != config.DVNModeActive {
			continue
		}
		if _, ok := settings[pathway.DstEID]; ok {
			continue
		}
		dstChain, err := registry.Get(pathway.DstEID)
		if err != nil {
			return nil, err
		}
		settings[pathway.DstEID] = dvn.Settings{
			SignerID: dstChain.TxRoles.DVN.SignerID,
		}
	}
	return dvn.NewWithSettings(store, registry, settings, a.logger), nil
}

func (a *App) txTargets(ctx context.Context, registry *chain.Registry, store *db.Store) ([]txmgr.Target, error) {
	type targetKey struct {
		chainEID uint32
		signerID string
	}
	type targetRequirement struct {
		policies            map[string]txmgr.FeePolicy
		minNativeBalanceWei *big.Int
	}
	required := make(map[targetKey]targetRequirement)
	addPolicy := func(chainEID uint32, signerID, purpose string, policy txmgr.FeePolicy, minNativeBalanceWei *big.Int) {
		key := targetKey{chainEID: chainEID, signerID: signerID}
		requirement := required[key]
		if requirement.policies == nil {
			requirement.policies = make(map[string]txmgr.FeePolicy)
		}
		requirement.policies[purpose] = policy
		requirement.minNativeBalanceWei = bigutil.Max(requirement.minNativeBalanceWei, minNativeBalanceWei)
		required[key] = requirement
	}
	for _, configuredChain := range registry.All() {
		if a.cfg.ExecutorEnabled() {
			executorPolicy, err := feePolicy(configuredChain.TxRoles.Executor.MaxFeePerGasWei, configuredChain.TxRoles.Executor.MaxPriorityFeePerGasWei, configuredChain.LegacyTransactions)
			if err != nil {
				return nil, fmt.Errorf("chain %s executor fee policy: %w", configuredChain.Name, err)
			}
			minBalance, err := bigutil.ParsePositiveDecimal("min_native_balance_wei", configuredChain.TxRoles.Executor.MinNativeBalanceWei)
			if err != nil {
				return nil, fmt.Errorf("chain %s executor min native balance: %w", configuredChain.Name, err)
			}
			addPolicy(configuredChain.EID, configuredChain.TxRoles.Executor.SignerID, executor.TxPurposeCommitVerification, executorPolicy, minBalance)
			addPolicy(configuredChain.EID, configuredChain.TxRoles.Executor.SignerID, executor.TxPurposeLzReceive, executorPolicy, minBalance)
		}
	}
	if a.cfg.Pricing.Enabled {
		for _, pricingChain := range a.cfg.Pricing.Chains {
			configuredChain, err := registry.Get(pricingChain.EID)
			if err != nil {
				return nil, err
			}
			pricingPolicy, err := feePolicy(pricingChain.TxPolicy.MaxFeePerGasWei, pricingChain.TxPolicy.MaxPriorityFeePerGasWei, configuredChain.LegacyTransactions)
			if err != nil {
				return nil, fmt.Errorf("chain %s pricing fee policy: %w", configuredChain.Name, err)
			}
			minBalance, err := bigutil.ParsePositiveDecimal("min_native_balance_wei", pricingChain.TxPolicy.MinNativeBalanceWei)
			if err != nil {
				return nil, fmt.Errorf("chain %s pricing min native balance: %w", configuredChain.Name, err)
			}
			addPolicy(configuredChain.EID, a.cfg.Pricing.Signer.Hex(), pricing.TxPurposeSetPriceSnapshot, pricingPolicy, minBalance)
			// Rows enqueued by a previously configured pricing signer must keep
			// converging after a rotation: they gate their feed until they
			// resolve, and without a target nothing would ever sign, broadcast,
			// or receipt-poll them. The rotation runbook keeps the old signer
			// configured until its rows drain; fail fast when it was removed
			// too early instead of gating the feed forever.
			if store == nil {
				// Targets built without a database (config-only tests) cannot
				// check the outbox; Run always passes the connected store.
				continue
			}
			legacySigners, err := store.ListPendingPricingSigners(ctx, configuredChain.EID)
			if err != nil {
				return nil, err
			}
			for _, legacy := range legacySigners {
				if strings.EqualFold(legacy, a.cfg.Pricing.Signer.Hex()) {
					continue
				}
				if !a.signerConfigured(legacy) {
					return nil, fmt.Errorf("chain %s has pending pricing txs from previous signer %s; keep that signer configured until they drain or resolve the rows manually", configuredChain.Name, legacy)
				}
				addPolicy(configuredChain.EID, legacy, pricing.TxPurposeSetPriceSnapshot, pricingPolicy, minBalance)
			}
		}
	}
	if a.cfg.DVNEnabled() {
		for _, pathway := range registry.Pathways() {
			if pathway.DVNMode != config.DVNModeActive {
				continue
			}
			dstChain, err := registry.Get(pathway.DstEID)
			if err != nil {
				return nil, err
			}
			dvnPolicy, err := feePolicy(dstChain.TxRoles.DVN.MaxFeePerGasWei, dstChain.TxRoles.DVN.MaxPriorityFeePerGasWei, dstChain.LegacyTransactions)
			if err != nil {
				return nil, fmt.Errorf("chain %s dvn fee policy: %w", dstChain.Name, err)
			}
			minBalance, err := bigutil.ParsePositiveDecimal("min_native_balance_wei", dstChain.TxRoles.DVN.MinNativeBalanceWei)
			if err != nil {
				return nil, fmt.Errorf("chain %s dvn min native balance: %w", dstChain.Name, err)
			}
			addPolicy(dstChain.EID, dstChain.TxRoles.DVN.SignerID, dvn.TxPurposeVerify, dvnPolicy, minBalance)
		}
	}
	if len(required) == 0 {
		return nil, nil
	}
	signers, err := a.loadSigners(ctx)
	if err != nil {
		return nil, err
	}
	targets := make([]txmgr.Target, 0, len(required))
	for key, requirement := range required {
		configuredChain, err := registry.Get(key.chainEID)
		if err != nil {
			return nil, err
		}
		if err := validateRuntimeFeePolicies(ctx, configuredChain, requirement.policies); err != nil {
			return nil, err
		}
		configuredSigner, ok := signers[key.signerID]
		if !ok {
			return nil, errors.New("configured signer was not loaded")
		}
		targets = append(targets, txmgr.Target{
			ChainEID:            configuredChain.EID,
			ChainName:           configuredChain.Name,
			ChainID:             new(big.Int).Set(configuredChain.ChainID),
			Signer:              configuredSigner,
			Client:              configuredChain.RPC,
			Confirmations:       configuredChain.Confirmations,
			FeePolicies:         cloneFeePolicies(requirement.policies),
			MinNativeBalanceWei: bigutil.Clone(requirement.minNativeBalanceWei),
		})
	}
	return targets, nil
}

// signerConfigured reports whether a signer address is present in the loaded
// signers configuration, compared case-insensitively on the hex form.
func (a *App) signerConfigured(signerID string) bool {
	for _, cfg := range a.cfg.Signers {
		if strings.EqualFold(cfg.ID.Hex(), signerID) {
			return true
		}
	}
	return false
}

func (a *App) loadSigners(ctx context.Context) (map[string]signer.Signer, error) {
	signers := make(map[string]signer.Signer, len(a.cfg.Signers))
	for _, cfg := range a.cfg.Signers {
		id := cfg.ID.Hex()
		switch cfg.Type {
		case "keystore":
			loaded, err := keystore.LoadWithPasswordSource(cfg.Keystore.Path, keystore.PasswordSource{
				Env:  cfg.Keystore.PasswordEnv,
				File: cfg.Keystore.PasswordFile,
			})
			if err != nil {
				return nil, err
			}
			if loaded.Address().Hex() != id {
				return nil, errors.New("keystore signer address does not match configured signer id")
			}
			signers[id] = loaded
		case "kms":
			awsConfig, err := loadKMSAWSConfig(ctx, cfg.KMS)
			if err != nil {
				return nil, err
			}
			client := kms.NewSDKClient(awsConfig)
			if cfg.KMS.Endpoint != "" {
				client = kms.NewSDKClientWithEndpoint(awsConfig, cfg.KMS.Endpoint)
			}
			loaded := kms.New(client, cfg.KMS.KeyID, cfg.KMS.Address.Common())
			if err := loaded.ValidateKey(ctx); err != nil {
				return nil, err
			}
			signers[id] = loaded
		default:
			return nil, errors.New("unsupported signer type")
		}
	}
	return signers, nil
}

func loadKMSAWSConfig(ctx context.Context, cfg config.KMSSignerConfig) (aws.Config, error) {
	return awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
}

func feePolicy(maxFeePerGasWei, maxPriorityFeePerGasWei string, legacyTransactions bool) (txmgr.FeePolicy, error) {
	maxFeePerGas, err := bigutil.ParseDecimal("max_fee_per_gas_wei", maxFeePerGasWei)
	if err != nil {
		return txmgr.FeePolicy{}, err
	}
	var maxPriorityFeePerGas *big.Int
	if maxPriorityFeePerGasWei != "" {
		maxPriorityFeePerGas, err = bigutil.ParseDecimal("max_priority_fee_per_gas_wei", maxPriorityFeePerGasWei)
		if err != nil {
			return txmgr.FeePolicy{}, err
		}
	}
	return txmgr.FeePolicy{
		ConfiguredMaxFeePerGas:         maxFeePerGas,
		ConfiguredMaxPriorityFeePerGas: maxPriorityFeePerGas,
		ForceLegacyTransactions:        legacyTransactions,
	}, nil
}

func validateRuntimeFeePolicies(ctx context.Context, configuredChain chain.Chain, policies map[string]txmgr.FeePolicy) error {
	header, err := readTxHeader(ctx, configuredChain.RPC)
	if err != nil {
		return fmt.Errorf("read latest header for chain %s: %w", configuredChain.Name, err)
	}
	if header == nil || header.BaseFee == nil || configuredChain.LegacyTransactions {
		// A legacy-forced chain never quotes dynamic fees, so it does not
		// need a priority fee cap even when the header carries a base fee.
		return nil
	}
	for purpose, policy := range policies {
		if policy.ConfiguredMaxPriorityFeePerGas == nil || policy.ConfiguredMaxPriorityFeePerGas.Sign() <= 0 {
			return fmt.Errorf("chain %s purpose %s max_priority_fee_per_gas_wei is required because latest header has base fee", configuredChain.Name, purpose)
		}
	}
	return nil
}

func cloneFeePolicies(policies map[string]txmgr.FeePolicy) map[string]txmgr.FeePolicy {
	out := make(map[string]txmgr.FeePolicy, len(policies))
	for purpose, policy := range policies {
		out[purpose] = txmgr.FeePolicy{
			ConfiguredMaxFeePerGas:         bigutil.Clone(policy.ConfiguredMaxFeePerGas),
			ConfiguredMaxPriorityFeePerGas: bigutil.Clone(policy.ConfiguredMaxPriorityFeePerGas),
			ForceLegacyTransactions:        policy.ForceLegacyTransactions,
		}
	}
	return out
}
