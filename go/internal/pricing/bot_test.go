package pricing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/db"
)

func TestBotEnqueueOnceQueuesSharedPriceFeedUpdates(t *testing.T) {
	registry := testRegistry(t)
	store := &fakeStore{}
	logger, logs := captureLogger(slog.LevelInfo)
	bot, err := NewWithDependencies(store, registry, testSettings(), testSources(), emptySnapshotReader{}, logger)
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	if len(store.requests) != 2 {
		t.Fatalf("enqueued requests = %d, want 2", len(store.requests))
	}
	wantPurposes := map[string]int{
		TxPurposeSetPriceSnapshot: 2,
	}
	for _, request := range store.requests {
		wantPurposes[request.Purpose]--
		if len(request.Calldata) == 0 {
			t.Fatal("request calldata is empty")
		}
		if request.SignerID != "0x9999999999999999999999999999999999999999" {
			t.Fatalf("signer = %q", request.SignerID)
		}
	}
	for purpose, remaining := range wantPurposes {
		if remaining != 0 {
			t.Fatalf("purpose %s remaining count = %d", purpose, remaining)
		}
	}
	assertLogContains(t, logs.String(),
		`msg="price update tx enqueued"`,
		`tx_outbox_id=1`,
		`purpose=pricing_set_price_snapshot`,
		`src_eid=40161`,
		`dst_count=1`,
		`price_feed=0x4444444444444444444444444444444444444444`,
	)
	assertRequestMatchesSnapshot(t, store.requests, common.HexToAddress("0x4444444444444444444444444444444444444444"), 40449, PriceSnapshot{
		DstGasPriceInSrcToken:       big.NewInt(1_000_000_000),
		DstDataFeePerByteInSrcToken: big.NewInt(0),
		UpdatedAt:                   1_700_000_000,
		StaleAfter:                  1800,
	})
}

func TestBotEnqueueOnceSkipsInactiveSendScope(t *testing.T) {
	registry := testRegistry(t)
	store := &fakeStore{enqueueErr: db.ErrTxSendScopeInactive}
	logger, logs := captureLogger(slog.LevelDebug)
	bot, err := NewWithDependencies(store, registry, testSettings(), testSources(), emptySnapshotReader{}, logger)
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	// A paused/disabled send chain skips this cycle's update without failing
	// the pricing loop; the next cycle after unpause enqueues fresh prices.
	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v, want skipped without error", err)
	}
	if len(store.requests) != 0 {
		t.Fatalf("enqueued requests = %d, want none", len(store.requests))
	}
	assertLogContains(t, logs.String(),
		`msg="skipped price update tx enqueue"`,
		`reason=send_scope_inactive`,
	)
}

func TestBotEnqueueOnceRejectsDeviationWithoutEnqueue(t *testing.T) {
	registry := testRegistry(t)
	store := &fakeStore{}
	sources := testSources()
	sources[40161] = ChainSources{
		Primary:           testConfiguredPrice("primary", big.NewRat(2000, 1)),
		Sanity:            []ConfiguredPriceReader{testConfiguredPrice("sanity", big.NewRat(2300, 1))},
		Gas:               fixedGas{price: big.NewInt(1_000_000_000)},
		DataFeePerByteWei: big.NewInt(0),
	}
	bot, err := NewWithDependencies(store, registry, testSettings(), sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}

	if err := bot.EnqueueOnce(context.Background()); err == nil {
		t.Fatal("EnqueueOnce() error = nil, want deviation error")
	}
	if len(store.requests) != 0 {
		t.Fatalf("enqueued requests = %d, want 0", len(store.requests))
	}
}

func TestBotEnqueueOnceGatesWritesByDeviationAndHeartbeat(t *testing.T) {
	const nowUnix = uint64(1_700_000_000)
	pathways := testPathways()
	tests := []struct {
		name             string
		destinationGas   int64
		lastUpdatedAt    uint64
		wantRequests     int
		wantDeviationBps uint64
	}{
		{
			name:             "unchanged before heartbeat skips",
			destinationGas:   2_000_000_000,
			lastUpdatedAt:    nowUnix - 300,
			wantRequests:     0,
			wantDeviationBps: 0,
		},
		{
			name:             "deviation threshold writes",
			destinationGas:   2_020_000_000,
			lastUpdatedAt:    nowUnix - 300,
			wantRequests:     1,
			wantDeviationBps: 100,
		},
		{
			name:             "heartbeat writes unchanged price",
			destinationGas:   2_000_000_000,
			lastUpdatedAt:    nowUnix - 900,
			wantRequests:     1,
			wantDeviationBps: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]})
			store := &fakeStore{}
			sources := testSources()
			sources[40449] = ChainSources{
				Primary:           testConfiguredPrice("primary", big.NewRat(1000, 1)),
				Gas:               fixedGas{price: big.NewInt(test.destinationGas)},
				DataFeePerByteWei: big.NewInt(0),
			}
			logger, logs := captureLogger(slog.LevelDebug)
			reader := fixedSnapshotReader{snapshot: PriceSnapshot{
				DstGasPriceInSrcToken:       big.NewInt(1_000_000_000),
				DstDataFeePerByteInSrcToken: big.NewInt(0),
				UpdatedAt:                   test.lastUpdatedAt,
				StaleAfter:                  1800,
			}}
			bot, err := NewWithDependencies(store, registry, testSettings(), sources, reader, logger)
			if err != nil {
				t.Fatalf("NewWithDependencies() error = %v", err)
			}
			bot.now = func() time.Time { return time.Unix(int64(nowUnix), 0) }

			if err := bot.EnqueueOnce(context.Background()); err != nil {
				t.Fatalf("EnqueueOnce() error = %v", err)
			}
			if len(store.requests) != test.wantRequests {
				t.Fatalf("enqueued requests = %d, want %d", len(store.requests), test.wantRequests)
			}
			if test.wantRequests == 0 {
				assertLogContains(t, logs.String(),
					`msg="skipped price snapshot update"`,
					`reason=below_deviation_and_heartbeat`,
					`deviation_bps=0`,
					`heartbeat_seconds=900`,
				)
				return
			}
			wantPrice := new(big.Int).Quo(big.NewInt(test.destinationGas), big.NewInt(2))
			assertRequestMatchesSnapshot(t, store.requests, pathways[0].SourceWorkers.PriceFeed.Common(), pathways[0].DstEID, PriceSnapshot{
				DstGasPriceInSrcToken:       wantPrice,
				DstDataFeePerByteInSrcToken: big.NewInt(0),
				UpdatedAt:                   nowUnix,
				StaleAfter:                  1800,
			})
			if got := PriceChangeBps(big.NewInt(1_000_000_000), wantPrice); got != test.wantDeviationBps {
				t.Fatalf("PriceChangeBps() = %d, want %d", got, test.wantDeviationBps)
			}
		})
	}
}

func TestBotEnqueueOnceUsesSameNativeAssetConversionWithoutPriceReaders(t *testing.T) {
	pathways := testPathways()
	registry := testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]})
	store := &fakeStore{}
	bot, err := NewWithDependencies(store, registry, testSettings(), map[uint32]ChainSources{
		40161: {
			Gas:               fixedGas{price: big.NewInt(1_000_000_000)},
			DataFeePerByteWei: big.NewInt(0),
			NativeAssetID:     "eth",
		},
		40449: {
			Gas:               fixedGas{price: big.NewInt(2_000_000_000)},
			DataFeePerByteWei: big.NewInt(123),
			NativeAssetID:     "eth",
		},
	}, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	if len(store.requests) != 1 {
		t.Fatalf("enqueued requests = %d, want 1", len(store.requests))
	}
	assertRequestMatchesSnapshot(t, store.requests, common.HexToAddress("0x4444444444444444444444444444444444444444"), 40449, PriceSnapshot{
		DstGasPriceInSrcToken:       big.NewInt(2_000_000_000),
		DstDataFeePerByteInSrcToken: big.NewInt(123),
		UpdatedAt:                   1_700_000_000,
		StaleAfter:                  1800,
	})
}

func TestBotEnqueueOnGasSpikeQueuesOnlyAboveThreshold(t *testing.T) {
	registry := testRegistry(t)
	store := &fakeStore{}
	sourceGas := &mutableGas{price: big.NewInt(1_000_000_000)}
	destinationGas := &mutableGas{price: big.NewInt(2_000_000_000)}
	logger, logs := captureLogger(slog.LevelInfo)
	bot, err := NewWithDependencies(store, registry, testSettings(), map[uint32]ChainSources{
		40161: {Primary: testConfiguredPrice("primary", big.NewRat(2000, 1)), Gas: sourceGas, DataFeePerByteWei: big.NewInt(0)},
		40449: {Primary: testConfiguredPrice("primary", big.NewRat(1000, 1)), Gas: destinationGas, DataFeePerByteWei: big.NewInt(0)},
	}, emptySnapshotReader{}, logger)
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	if len(store.requests) != 2 {
		t.Fatalf("initial enqueued requests = %d, want 2", len(store.requests))
	}
	// Steady state: the successful writes landed and a prior spike check
	// already consumed the pending-observed flag with a deviation-suppressed
	// forced evaluation.
	bot.pendingFeeds = nil

	destinationGas.price = big.NewInt(2_100_000_000)
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike() below threshold error = %v", err)
	}
	if len(store.requests) != 2 {
		t.Fatalf("below-threshold enqueued requests = %d, want 2", len(store.requests))
	}

	destinationGas.price = big.NewInt(2_300_000_000)
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike() above threshold error = %v", err)
	}
	if len(store.requests) != 3 {
		t.Fatalf("above-threshold enqueued requests = %d, want 3", len(store.requests))
	}
	assertLogContains(t, logs.String(),
		`msg="price bot enqueued gas-spike update"`,
		`src_eid=40161`,
		`dst_eid=40449`,
		`previous_gas_wei=2000000000`,
		`current_gas_wei=2300000000`,
		`tx_outbox_id=`,
	)
}

func TestBotEnqueueOnGasSpikeAdvancesBaselineWhenSuppressed(t *testing.T) {
	registry := testRegistry(t)
	store := &fakeStore{}
	sourceGas := &mutableGas{price: big.NewInt(1_000_000_000)}
	destinationGas := &mutableGas{price: big.NewInt(2_000_000_000)}
	bot, err := NewWithDependencies(store, registry, testSettings(), map[uint32]ChainSources{
		40161: {Primary: testConfiguredPrice("primary", big.NewRat(2000, 1)), Gas: sourceGas, DataFeePerByteWei: big.NewInt(0)},
		40449: {Primary: testConfiguredPrice("primary", big.NewRat(1000, 1)), Gas: destinationGas, DataFeePerByteWei: big.NewInt(0)},
	}, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	seedCalls := store.enqueueCalls
	// Steady state: the successful writes landed and a prior spike check
	// already consumed the pending-observed flag with a deviation-suppressed
	// forced evaluation.
	bot.pendingFeeds = nil

	// The chain pauses, so the spike evaluation runs but nothing is enqueued.
	store.enqueueErr = db.ErrTxSendScopeInactive
	destinationGas.price = big.NewInt(2_300_000_000)
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike() suppressed error = %v", err)
	}
	if store.enqueueCalls != seedCalls+1 {
		t.Fatalf("enqueue calls after suppressed spike = %d, want %d", store.enqueueCalls, seedCalls+1)
	}

	// The same gas level must not re-trigger evaluation every check interval.
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike() repeat error = %v", err)
	}
	if store.enqueueCalls != seedCalls+1 {
		t.Fatalf("enqueue calls after repeated spike check = %d, want unchanged %d", store.enqueueCalls, seedCalls+1)
	}

	// A further rise past the threshold from the advanced baseline re-triggers.
	store.enqueueErr = nil
	destinationGas.price = big.NewInt(2_600_000_000)
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike() further rise error = %v", err)
	}
	if store.enqueueCalls != seedCalls+2 {
		t.Fatalf("enqueue calls after further rise = %d, want %d", store.enqueueCalls, seedCalls+2)
	}
}

func TestBotEnqueueOnceDeduplicatesSharedPriceFeed(t *testing.T) {
	pathways := testPathways()
	duplicate := pathways[0]
	duplicate.SrcOApp = config.MustEVMAddress("0x9999999999999999999999999999999999999998")
	duplicate.DstOApp = config.MustEVMAddress("0x9999999999999999999999999999999999999997")
	duplicate.SourceWorkers.OpenDVN = config.MustEVMAddress("0x9999999999999999999999999999999999999996")
	duplicate.Pricing.DVNFee = config.WorkerFeeModelConfig{FixedFeeWei: "3000", DstGasOverhead: 250_000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 300}
	pathways = []config.PathwayConfig{pathways[0], duplicate}
	registry := testRegistryWithPathways(t, pathways)
	store := &fakeStore{}
	bot, err := NewWithDependencies(store, registry, testSettings(), testSources(), emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	if len(store.requests) != 1 {
		t.Fatalf("enqueued requests = %d, want 1", len(store.requests))
	}
	if got := countRequests(store.requests, TxPurposeSetPriceSnapshot); got != 1 {
		t.Fatalf("price snapshot requests = %d, want 1", got)
	}
}

func TestBotEnqueueOnceBatchesSameSourcePriceFeedTargets(t *testing.T) {
	pathways := testPathways()
	secondTarget := pathways[0]
	secondTarget.DstEID = 40500
	secondTarget.DstOApp = config.MustEVMAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	secondTarget.ReceiveLib = config.MustEVMAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	secondTarget.DestinationWorkers.OpenDVN = config.MustEVMAddress("0xcccccccccccccccccccccccccccccccccccccccc")
	registry := testRegistryWithPathways(t, []config.PathwayConfig{pathways[0], secondTarget})
	store := &fakeStore{}
	sourcePrice := &countingPrice{source: "primary", price: big.NewRat(2000, 1), observedAt: time.Unix(1_700_000_000, 0)}
	destinationPrice := &countingPrice{source: "primary", price: big.NewRat(1000, 1), observedAt: time.Unix(1_700_000_000, 0)}
	alternatePrice := &countingPrice{source: "primary", price: big.NewRat(500, 1), observedAt: time.Unix(1_700_000_000, 0)}
	sources := map[uint32]ChainSources{
		40161: {Primary: ConfiguredPriceReader{Name: "primary", Reader: sourcePrice, MaxAge: time.Minute}, Gas: fixedGas{price: big.NewInt(1_000_000_000)}, DataFeePerByteWei: big.NewInt(0)},
		40449: {Primary: ConfiguredPriceReader{Name: "primary", Reader: destinationPrice, MaxAge: time.Minute}, Gas: fixedGas{price: big.NewInt(2_000_000_000)}, DataFeePerByteWei: big.NewInt(0)},
	}
	sources[40500] = ChainSources{
		Primary:           ConfiguredPriceReader{Name: "primary", Reader: alternatePrice, MaxAge: time.Minute},
		Gas:               fixedGas{price: big.NewInt(3_000_000_000)},
		DataFeePerByteWei: big.NewInt(0),
	}
	bot, err := NewWithDependencies(store, registry, testSettings(), sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	if len(store.requests) != 1 {
		t.Fatalf("enqueued requests = %d, want 1 batch", len(store.requests))
	}
	for name, reader := range map[string]*countingPrice{"source": sourcePrice, "destination": destinationPrice, "alternate": alternatePrice} {
		if reads := reader.count.Load(); reads != 1 {
			t.Fatalf("%s price reads = %d, want one per EID per cycle", name, reads)
		}
	}
	assertRequestMatchesUpdates(t, store.requests, common.HexToAddress("0x4444444444444444444444444444444444444444"), []PriceSnapshotUpdate{
		{
			DstEid: 40449,
			Snapshot: PriceSnapshot{
				DstGasPriceInSrcToken:       big.NewInt(1_000_000_000),
				DstDataFeePerByteInSrcToken: big.NewInt(0),
				UpdatedAt:                   1_700_000_000,
				StaleAfter:                  1800,
			},
		},
		{
			DstEid: 40500,
			Snapshot: PriceSnapshot{
				DstGasPriceInSrcToken:       big.NewInt(750_000_000),
				DstDataFeePerByteInSrcToken: big.NewInt(0),
				UpdatedAt:                   1_700_000_000,
				StaleAfter:                  1800,
			},
		},
	})
}

func TestBotEnqueueOnGasSpikeBatchesSameSourcePriceFeedTargets(t *testing.T) {
	pathways := testPathways()
	secondTarget := pathways[0]
	secondTarget.DstEID = 40500
	secondTarget.DstOApp = config.MustEVMAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	secondTarget.ReceiveLib = config.MustEVMAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	secondTarget.DestinationWorkers.OpenDVN = config.MustEVMAddress("0xcccccccccccccccccccccccccccccccccccccccc")
	registry := testRegistryWithPathways(t, []config.PathwayConfig{pathways[0], secondTarget})
	store := &fakeStore{}
	destinationGas := &mutableGas{price: big.NewInt(2_000_000_000)}
	alternateGas := &mutableGas{price: big.NewInt(3_000_000_000)}
	sources := testSources()
	sources[40449] = ChainSources{Primary: testConfiguredPrice("primary", big.NewRat(1000, 1)), Gas: destinationGas, DataFeePerByteWei: big.NewInt(0)}
	sources[40500] = ChainSources{Primary: testConfiguredPrice("primary", big.NewRat(500, 1)), Gas: alternateGas, DataFeePerByteWei: big.NewInt(0)}
	bot, err := NewWithDependencies(store, registry, testSettings(), sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	if len(store.requests) != 1 {
		t.Fatalf("initial enqueued requests = %d, want 1 batch", len(store.requests))
	}

	destinationGas.price = big.NewInt(2_300_000_000)
	alternateGas.price = big.NewInt(3_600_000_000)
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike() error = %v", err)
	}
	if len(store.requests) != 2 {
		t.Fatalf("gas-spike enqueued requests = %d, want 2 total batches", len(store.requests))
	}
	assertRequestMatchesUpdates(t, store.requests[1:], common.HexToAddress("0x4444444444444444444444444444444444444444"), []PriceSnapshotUpdate{
		{
			DstEid: 40449,
			Snapshot: PriceSnapshot{
				DstGasPriceInSrcToken:       big.NewInt(1_150_000_000),
				DstDataFeePerByteInSrcToken: big.NewInt(0),
				UpdatedAt:                   1_700_000_000,
				StaleAfter:                  1800,
			},
		},
		{
			DstEid: 40500,
			Snapshot: PriceSnapshot{
				DstGasPriceInSrcToken:       big.NewInt(900_000_000),
				DstDataFeePerByteInSrcToken: big.NewInt(0),
				UpdatedAt:                   1_700_000_000,
				StaleAfter:                  1800,
			},
		},
	})
}

func TestBotEnqueueOnceRejectsConflictingSharedRoleFeeModel(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.PathwayConfig)
	}{
		{
			name: "executor",
			mutate: func(pathway *config.PathwayConfig) {
				pathway.SourceWorkers.OpenDVN = config.MustEVMAddress("0x9999999999999999999999999999999999999996")
				pathway.Pricing.ExecutorFee.FixedFeeWei = "9999"
			},
		},
		{
			name: "dvn",
			mutate: func(pathway *config.PathwayConfig) {
				pathway.SourceWorkers.OpenExecutor = config.MustEVMAddress("0x9999999999999999999999999999999999999995")
				pathway.Pricing.DVNFee.MarginBps = 999
			},
		},
		{
			name: "executor",
			mutate: func(pathway *config.PathwayConfig) {
				pathway.SourceWorkers.OpenDVN = config.MustEVMAddress("0x9999999999999999999999999999999999999996")
				pathway.Pricing.ExecutorFee.DataSizeOverheadBytes = new(uint64(1))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pathways := testPathways()
			duplicate := pathways[0]
			duplicate.SrcOApp = config.MustEVMAddress("0x9999999999999999999999999999999999999998")
			duplicate.DstOApp = config.MustEVMAddress("0x9999999999999999999999999999999999999997")
			test.mutate(&duplicate)
			registry := testRegistryWithPathways(t, []config.PathwayConfig{pathways[0], duplicate})
			store := &fakeStore{}
			bot, err := NewWithDependencies(store, registry, testSettings(), testSources(), emptySnapshotReader{}, discardLogger())
			if err != nil {
				t.Fatalf("NewWithDependencies() error = %v", err)
			}

			err = bot.EnqueueOnce(context.Background())
			if err == nil {
				t.Fatal("EnqueueOnce() error = nil, want conflicting fee model error")
			}
			if !strings.Contains(err.Error(), "conflicting "+test.name+" fee model") {
				t.Fatalf("EnqueueOnce() error = %v, want conflicting fee model", err)
			}
			if len(store.requests) != 0 {
				t.Fatalf("enqueued requests = %d, want 0", len(store.requests))
			}
		})
	}
}

type fakeStore struct {
	requests     []db.TxRequest
	enqueueErr   error
	enqueueCalls int
	pending      []db.PendingPricingTx
	pendingErr   error
}

type emptySnapshotReader struct{}

func (emptySnapshotReader) PriceSnapshot(context.Context, uint32, common.Address, uint32) (PriceSnapshot, error) {
	return PriceSnapshot{
		DstGasPriceInSrcToken:       big.NewInt(0),
		DstDataFeePerByteInSrcToken: big.NewInt(0),
	}, nil
}

type fixedSnapshotReader struct {
	snapshot PriceSnapshot
}

func (r fixedSnapshotReader) PriceSnapshot(context.Context, uint32, common.Address, uint32) (PriceSnapshot, error) {
	return PriceSnapshot{
		DstGasPriceInSrcToken:       new(big.Int).Set(r.snapshot.DstGasPriceInSrcToken),
		DstDataFeePerByteInSrcToken: new(big.Int).Set(r.snapshot.DstDataFeePerByteInSrcToken),
		UpdatedAt:                   r.snapshot.UpdatedAt,
		StaleAfter:                  r.snapshot.StaleAfter,
	}, nil
}

func (s *fakeStore) EnqueuePricingSnapshotTx(_ context.Context, request db.TxRequest) (int64, error) {
	s.enqueueCalls++
	if s.enqueueErr != nil {
		return 0, s.enqueueErr
	}
	s.requests = append(s.requests, request)
	return int64(len(s.requests)), nil
}

func (s *fakeStore) ListPendingPricingTxs(_ context.Context, chainEID uint32) ([]db.PendingPricingTx, error) {
	if s.pendingErr != nil {
		return nil, s.pendingErr
	}
	return s.pending, nil
}

type fixedPrice struct {
	source     string
	price      *big.Rat
	observedAt time.Time
}

type countingPrice struct {
	count      atomic.Int32
	source     string
	price      *big.Rat
	observedAt time.Time
}

func (p *countingPrice) PriceUSD(context.Context) (SourcePrice, error) {
	p.count.Add(1)
	return SourcePrice{Source: p.source, USD: p.price, ObservedAt: p.observedAt}, nil
}

func (p fixedPrice) PriceUSD(context.Context) (SourcePrice, error) {
	return SourcePrice{Source: p.source, USD: p.price, ObservedAt: p.observedAt}, nil
}

type failingPrice struct{}

func (failingPrice) PriceUSD(context.Context) (SourcePrice, error) {
	return SourcePrice{}, context.Canceled
}

type fixedGas struct {
	price *big.Int
}

func (g fixedGas) SuggestGasPrice(context.Context) (*big.Int, error) {
	return new(big.Int).Set(g.price), nil
}

type mutableGas struct {
	price *big.Int
	err   error
}

func (g *mutableGas) SuggestGasPrice(context.Context) (*big.Int, error) {
	if g.err != nil {
		return nil, g.err
	}
	return new(big.Int).Set(g.price), nil
}

func testSettings() Settings {
	return Settings{
		Enabled:              true,
		SignerID:             "0x9999999999999999999999999999999999999999",
		Interval:             time.Minute,
		StaleAfter:           30 * time.Minute,
		MaxDeviation:         500,
		MinUpdateDeviation:   50,
		Heartbeat:            15 * time.Minute,
		SourceRequestTimeout: time.Second,
		GasSpikeBps:          1000,
	}
}

func testRegistry(t *testing.T) *chain.Registry {
	t.Helper()
	return testRegistryWithPathways(t, testPathways())
}

func testRegistryWithPathways(t *testing.T, pathways []config.PathwayConfig) *chain.Registry {
	t.Helper()
	registry, err := chain.NewRegistry([]config.ChainConfig{
		{
			EID:             40161,
			Name:            "ethereum-sepolia",
			Family:          config.ChainFamilyEVM,
			ChainID:         11155111,
			EndpointAddress: config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
			Confirmations:   12,
			RPCURLs:         []string{"http://localhost:8545"},
			TxRoles: config.ChainTxRolesConfig{
				Executor: testExecutorRole(),
			},
		},
		{
			EID:             40449,
			Name:            "hoodi",
			Family:          config.ChainFamilyEVM,
			ChainID:         560048,
			EndpointAddress: config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
			Confirmations:   12,
			RPCURLs:         []string{"http://localhost:8546"},
			TxRoles: config.ChainTxRolesConfig{
				Executor: testExecutorRole(),
			},
		},
		{
			EID:             40500,
			Name:            "alt-destination",
			Family:          config.ChainFamilyEVM,
			ChainID:         40500,
			EndpointAddress: config.MustEVMAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			Confirmations:   12,
			RPCURLs:         []string{"http://localhost:8547"},
			TxRoles: config.ChainTxRolesConfig{
				Executor: testExecutorRole(),
			},
		},
	}, pathways)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	return registry
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
			Pricing:        testPathwayPricingConfig("1000", 50_000, 100, "2000", 150_000, 200),
			Enabled:        true,
			MaxMessageSize: 10000,
		},
		{
			SrcEID:     40449,
			DstEID:     40161,
			SrcOApp:    config.MustEVMAddress("0x8888888888888888888888888888888888888888"),
			DstOApp:    config.MustEVMAddress("0x7777777777777777777777777777777777777777"),
			SendLib:    config.MustEVMAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			ReceiveLib: config.MustEVMAddress("0xcccccccccccccccccccccccccccccccccccccccc"),
			SourceWorkers: config.WorkerContractsConfig{
				OpenExecutor: config.MustEVMAddress("0x5555555555555555555555555555555555555555"),
				OpenDVN:      config.MustEVMAddress("0x6666666666666666666666666666666666666666"),
				PriceFeed:    config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
			},
			DestinationWorkers: config.DestinationWorkerContractsConfig{
				OpenDVN: config.MustEVMAddress("0x3333333333333333333333333333333333333333"),
			},
			DVN:            config.PathwayDVNConfig{Mode: config.DVNModeShadow},
			Pricing:        testPathwayPricingConfig("4000", 80_000, 400, "5000", 180_000, 500),
			Enabled:        true,
			MaxMessageSize: 10000,
		},
	}
}

func testPathwayPricingConfig(executorBase string, executorOverhead uint64, executorMargin uint16, dvnBase string, dvnOverhead uint64, dvnMargin uint16) config.PathwayPricingConfig {
	return config.PathwayPricingConfig{
		ExecutorFee: config.WorkerFeeModelConfig{FixedFeeWei: executorBase, DstGasOverhead: executorOverhead, DataSizeOverheadBytes: new(uint64(0)), MarginBps: executorMargin},
		DVNFee:      config.WorkerFeeModelConfig{FixedFeeWei: dvnBase, DstGasOverhead: dvnOverhead, DataSizeOverheadBytes: new(uint64(0)), MarginBps: dvnMargin},
	}
}

func testSources() map[uint32]ChainSources {
	return map[uint32]ChainSources{
		40161: {Primary: testConfiguredPrice("primary", big.NewRat(2000, 1)), Gas: fixedGas{price: big.NewInt(1_000_000_000)}, DataFeePerByteWei: big.NewInt(0)},
		40449: {Primary: testConfiguredPrice("primary", big.NewRat(1000, 1)), Gas: fixedGas{price: big.NewInt(2_000_000_000)}, DataFeePerByteWei: big.NewInt(0)},
	}
}

func testConfiguredPrice(source string, price *big.Rat) ConfiguredPriceReader {
	return ConfiguredPriceReader{
		Name:   source,
		Reader: fixedPrice{source: source, price: price, observedAt: time.Unix(1_700_000_000, 0)},
		MaxAge: 100 * 365 * 24 * time.Hour,
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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func captureLogger(level slog.Leveler) (*slog.Logger, *bytes.Buffer) {
	var logs bytes.Buffer
	return slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: level})), &logs
}

func assertLogContains(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Fatalf("logs missing %q in:\n%s", want, output)
		}
	}
}

func assertRequestMatchesSnapshot(t *testing.T, requests []db.TxRequest, priceFeed common.Address, dstEID uint32, snapshot PriceSnapshot) {
	t.Helper()
	assertRequestMatchesUpdates(t, requests, priceFeed, []PriceSnapshotUpdate{{DstEid: dstEID, Snapshot: snapshot}})
}

func assertRequestMatchesUpdates(t *testing.T, requests []db.TxRequest, priceFeed common.Address, updates []PriceSnapshotUpdate) {
	t.Helper()
	want, err := BuildSetPriceSnapshotCalldata(updates)
	if err != nil {
		t.Fatalf("BuildSetPriceSnapshotCalldata() error = %v", err)
	}
	for _, request := range requests {
		if request.To == priceFeed && request.Purpose == TxPurposeSetPriceSnapshot {
			if !bytes.Equal(request.Calldata, want) {
				t.Fatalf("price snapshot calldata for %s does not match expected snapshot", priceFeed)
			}
			return
		}
	}
	t.Fatalf("missing price snapshot request for %s", priceFeed)
}

func countRequests(requests []db.TxRequest, purpose string) int {
	count := 0
	for _, request := range requests {
		if request.Purpose == purpose {
			count++
		}
	}
	return count
}

func TestBotEnqueueOnceSkipsPendingUpdates(t *testing.T) {
	// The pending batch carries a DIFFERENT destination than the pathway under
	// test: gating is per feed (one snapshot tx per feed in flight), so every
	// destination sharing the pending feed must stay gated.
	pendingCalldata, err := BuildSetPriceSnapshotCalldata([]PriceSnapshotUpdate{{
		DstEid: 40999,
		Snapshot: PriceSnapshot{
			DstGasPriceInSrcToken:       big.NewInt(1_000_000_000),
			DstDataFeePerByteInSrcToken: big.NewInt(0),
			UpdatedAt:                   1_699_999_000,
			StaleAfter:                  1800,
		},
	}})
	if err != nil {
		t.Fatalf("BuildSetPriceSnapshotCalldata() error = %v", err)
	}
	store := &fakeStore{pending: []db.PendingPricingTx{{
		ID:       7,
		SignerID: "0x9999999999999999999999999999999999999999",
		To:       common.HexToAddress("0x4444444444444444444444444444444444444444"),
		Calldata: pendingCalldata,
		Status:   db.TxStatusBroadcast,
	}}}
	logger, logs := captureLogger(slog.LevelDebug)
	pathways := testPathways()
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]}), testSettings(), testSources(), emptySnapshotReader{}, logger)
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	// The only pathway's update is covered by the pending row, so nothing is
	// enqueued and no market data is requested.
	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	if store.enqueueCalls != 0 {
		t.Fatalf("enqueue calls = %d, want 0 while pending", store.enqueueCalls)
	}
	assertLogContains(t, logs.String(),
		`msg="skipped price snapshot update"`,
		`reason=pending`,
		`msg="skipped price update batch"`,
		`reason=all_updates_pending`,
	)
}

func TestBotEnqueueOnceFailsOnMalformedPendingRow(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{pending: []db.PendingPricingTx{{
		ID:       9,
		To:       common.HexToAddress("0x4444444444444444444444444444444444444444"),
		Calldata: []byte{0x01, 0x02, 0x03, 0x04, 0x05},
		Status:   db.TxStatusQueued,
	}}}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]}), testSettings(), testSources(), emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}

	// A pending row the bot cannot attribute gates nothing while still able to
	// spend on chain; the cycle must fail loudly instead of ignoring it.
	if err := bot.EnqueueOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "undecodable") {
		t.Fatalf("EnqueueOnce() error = %v, want undecodable pending row error", err)
	}
	if store.enqueueCalls != 0 {
		t.Fatalf("enqueue calls = %d, want 0", store.enqueueCalls)
	}
}

func TestBotEnqueueOnceSkipsWhenPendingRaceLost(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{enqueueErr: db.ErrPricingPendingExists}
	logger, logs := captureLogger(slog.LevelDebug)
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]}), testSettings(), testSources(), emptySnapshotReader{}, logger)
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v, want race skipped without error", err)
	}
	if len(store.requests) != 0 {
		t.Fatalf("stored requests = %d, want 0", len(store.requests))
	}
	assertLogContains(t, logs.String(),
		`msg="skipped price update tx enqueue"`,
		`reason=pending_exists`,
	)
}

func TestDecodeSetPriceSnapshotCalldata(t *testing.T) {
	valid := []PriceSnapshotUpdate{{
		DstEid: 40449,
		Snapshot: PriceSnapshot{
			DstGasPriceInSrcToken:       big.NewInt(123),
			DstDataFeePerByteInSrcToken: big.NewInt(0),
			UpdatedAt:                   1_700_000_000,
			StaleAfter:                  1800,
		},
	}}
	validCalldata, err := BuildSetPriceSnapshotCalldata(valid)
	if err != nil {
		t.Fatalf("BuildSetPriceSnapshotCalldata() error = %v", err)
	}
	decoded, err := decodeSetPriceSnapshotCalldata(validCalldata)
	if err != nil {
		t.Fatalf("decodeSetPriceSnapshotCalldata(valid) error = %v", err)
	}
	if len(decoded) != 1 || decoded[0].DstEid != 40449 || decoded[0].Snapshot.DstGasPriceInSrcToken.Cmp(big.NewInt(123)) != 0 {
		t.Fatalf("decoded = %+v, want the original entry", decoded)
	}

	duplicate := append(append([]PriceSnapshotUpdate(nil), valid...), valid...)
	duplicateCalldata, err := BuildSetPriceSnapshotCalldata(duplicate)
	if err != nil {
		t.Fatalf("BuildSetPriceSnapshotCalldata(duplicate) error = %v", err)
	}
	emptyCalldata, err := priceSnapshotABI.Pack("setPriceSnapshot", []PriceSnapshotUpdate{})
	if err != nil {
		t.Fatalf("pack empty batch: %v", err)
	}
	for name, calldata := range map[string][]byte{
		"tooShort":      {0x01, 0x02},
		"wrongSelector": append([]byte{0xde, 0xad, 0xbe, 0xef}, validCalldata[4:]...),
		"trailingByte":  append(append([]byte(nil), validCalldata...), 0x00),
		"emptyBatch":    emptyCalldata,
		"duplicateEid":  duplicateCalldata,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSetPriceSnapshotCalldata(calldata); err == nil {
				t.Fatal("decodeSetPriceSnapshotCalldata() error = nil, want strict rejection")
			}
		})
	}
}

type fakeMetricsRecorder struct {
	samples int
}

func (f *fakeMetricsRecorder) RecordPricingSnapshot(uint32, uint32, common.Address, time.Time, time.Duration) {
	f.samples++
}

func TestBotEnqueueOnceSamplesSnapshotMetricsBeforeMarketData(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{}
	sources := testSources()
	// The cross-asset pathway's primary source fails persistently.
	sources[40161] = ChainSources{
		Primary:           ConfiguredPriceReader{Name: "failed", Reader: failingPrice{}, MaxAge: time.Minute},
		Gas:               fixedGas{price: big.NewInt(1_000_000_000)},
		DataFeePerByteWei: big.NewInt(0),
	}
	recorder := &fakeMetricsRecorder{}
	reader := fixedSnapshotReader{snapshot: PriceSnapshot{
		DstGasPriceInSrcToken:       big.NewInt(1_000_000_000),
		DstDataFeePerByteInSrcToken: big.NewInt(0),
		UpdatedAt:                   1_699_999_000,
		StaleAfter:                  1800,
	}}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]}), testSettings(), sources, reader, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.WithMetrics(recorder)
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	// The cycle fails on the market source, but the on-chain snapshot metric
	// series must already exist so the near-stale alert can fire during the
	// very outage that blocks new writes.
	if err := bot.EnqueueOnce(context.Background()); err == nil {
		t.Fatal("EnqueueOnce() error = nil, want market source failure")
	}
	if recorder.samples == 0 {
		t.Fatal("snapshot metrics were not sampled before the market-data failure")
	}
}

func TestBotEnqueueOnGasSpikeKeepsBaselineWhenRaceLost(t *testing.T) {
	registry := testRegistry(t)
	store := &fakeStore{}
	sourceGas := &mutableGas{price: big.NewInt(1_000_000_000)}
	destinationGas := &mutableGas{price: big.NewInt(2_000_000_000)}
	bot, err := NewWithDependencies(store, registry, testSettings(), map[uint32]ChainSources{
		40161: {Primary: testConfiguredPrice("primary", big.NewRat(2000, 1)), Gas: sourceGas, DataFeePerByteWei: big.NewInt(0)},
		40449: {Primary: testConfiguredPrice("primary", big.NewRat(1000, 1)), Gas: destinationGas, DataFeePerByteWei: big.NewInt(0)},
	}, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	seedCalls := store.enqueueCalls
	// Steady state: the successful writes landed and a prior spike check
	// already consumed the pending-observed flag with a deviation-suppressed
	// forced evaluation.
	bot.pendingFeeds = nil

	// Another instance wins the enqueue race for the feed: the spike stays
	// unreacted (baseline unchanged) so it re-evaluates once pending clears.
	store.enqueueErr = db.ErrPricingPendingExists
	destinationGas.price = big.NewInt(2_300_000_000)
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike(race lost) error = %v", err)
	}
	if store.enqueueCalls != seedCalls+1 {
		t.Fatalf("enqueue calls after race lost = %d, want %d", store.enqueueCalls, seedCalls+1)
	}
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike(repeat) error = %v", err)
	}
	if store.enqueueCalls != seedCalls+2 {
		t.Fatalf("enqueue calls after repeated race-lost spike = %d, want %d (baseline must not advance)", store.enqueueCalls, seedCalls+2)
	}

	// Once the pending row resolves, the still-elevated gas enqueues normally.
	store.enqueueErr = nil
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike(resolved) error = %v", err)
	}
	if len(store.requests) != 3 {
		t.Fatalf("stored requests = %d, want 3 after the pending row resolved", len(store.requests))
	}
}

func TestBotEnqueueOnGasSpikeForcesEvaluationAfterPendingDrains(t *testing.T) {
	pathways := testPathways()
	pendingCalldata, err := BuildSetPriceSnapshotCalldata([]PriceSnapshotUpdate{{
		DstEid: 40449,
		Snapshot: PriceSnapshot{
			DstGasPriceInSrcToken:       big.NewInt(1_000_000_000),
			DstDataFeePerByteInSrcToken: big.NewInt(0),
			UpdatedAt:                   1_699_999_000,
			StaleAfter:                  1800,
		},
	}})
	if err != nil {
		t.Fatalf("BuildSetPriceSnapshotCalldata() error = %v", err)
	}
	store := &fakeStore{pending: []db.PendingPricingTx{{
		ID:       11,
		To:       pathways[0].SourceWorkers.PriceFeed.Common(),
		Calldata: pendingCalldata,
		Status:   db.TxStatusBroadcast,
	}}}
	destinationGas := &mutableGas{price: big.NewInt(2_300_000_000)}
	sources := testSources()
	sources[40449] = ChainSources{
		Primary:           testConfiguredPrice("primary", big.NewRat(1000, 1)),
		Gas:               destinationGas,
		DataFeePerByteWei: big.NewInt(0),
	}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]}), testSettings(), sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	// Restart shape: baselines are empty while a pending row is in flight, so
	// the pending feed is neither seeded nor evaluated.
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike(pending) error = %v", err)
	}
	if store.enqueueCalls != 0 {
		t.Fatalf("enqueue calls while pending = %d, want 0", store.enqueueCalls)
	}

	// The pending row drains; gas is flat but was elevated the whole time. The
	// first check after draining must force a full evaluation instead of
	// silently seeding the elevated value as the baseline.
	store.pending = nil
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike(drained) error = %v", err)
	}
	if len(store.requests) != 1 {
		t.Fatalf("stored requests after drain = %d, want a forced evaluation write", len(store.requests))
	}
}

func TestBotEnqueueOnGasSpikeForcesEvaluationAfterFastTerminalization(t *testing.T) {
	registry := testRegistry(t)
	store := &fakeStore{}
	sourceGas := &mutableGas{price: big.NewInt(1_000_000_000)}
	destinationGas := &mutableGas{price: big.NewInt(2_000_000_000)}
	bot, err := NewWithDependencies(store, registry, testSettings(), map[uint32]ChainSources{
		40161: {Primary: testConfiguredPrice("primary", big.NewRat(2000, 1)), Gas: sourceGas, DataFeePerByteWei: big.NewInt(0)},
		40449: {Primary: testConfiguredPrice("primary", big.NewRat(1000, 1)), Gas: destinationGas, DataFeePerByteWei: big.NewInt(0)},
	}, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	seedCalls := store.enqueueCalls

	// The enqueued rows terminalize before any pending poll observes them (the
	// fake never reports pending): the successful enqueue remembered the feeds
	// as pending-observed, so the next spike check — gas unchanged, baselines
	// already advanced — must still force a full evaluation to converge with
	// whatever actually landed on chain.
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike(after fast terminalization) error = %v", err)
	}
	if store.enqueueCalls <= seedCalls {
		t.Fatalf("enqueue calls = %d, want a forced evaluation beyond the seed %d", store.enqueueCalls, seedCalls)
	}
}

type blockingGas struct{}

func (blockingGas) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type erroringGas struct{}

func (erroringGas) SuggestGasPrice(context.Context) (*big.Int, error) {
	return nil, errors.New("gas provider unavailable")
}

func TestBotEnqueueOnceBoundsBlackHoledChainReads(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{}
	settings := testSettings()
	settings.SourceRequestTimeout = 100 * time.Millisecond
	sources := testSources()
	sources[40449] = ChainSources{
		Primary:           testConfiguredPrice("primary", big.NewRat(1000, 1)),
		Gas:               blockingGas{},
		DataFeePerByteWei: big.NewInt(0),
	}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]}), settings, sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	// A black-holed gas provider must delay one bounded read, not hang the
	// whole pricing loop with no supervisor recourse.
	started := time.Now()
	err = bot.EnqueueOnce(context.Background())
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("EnqueueOnce() error = nil, want bounded chain-read failure")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("EnqueueOnce() took %s, want bounded by the per-read deadline", elapsed)
	}
	if len(store.requests) != 0 {
		t.Fatalf("stored requests = %d, want 0", len(store.requests))
	}
}

func TestBotEnqueueOnceIsolatesFailingChainFromOtherFeeds(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{}
	logger, logs := captureLogger(slog.LevelWarn)
	sources := testSources()
	// Pathway 40161->40449 needs 40449 gas (fails); pathway 40449->40161 needs
	// 40161 gas (healthy) and both native prices.
	sources[40449] = ChainSources{
		Primary:           testConfiguredPrice("primary", big.NewRat(1000, 1)),
		Gas:               erroringGas{},
		DataFeePerByteWei: big.NewInt(0),
	}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, pathways), testSettings(), sources, emptySnapshotReader{}, logger)
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v, want partial success without error", err)
	}
	if len(store.requests) != 1 {
		t.Fatalf("stored requests = %d, want the healthy feed's write only", len(store.requests))
	}
	assertLogContains(t, logs.String(),
		`msg="price update batch failed; continuing with remaining feeds"`,
	)
}

func TestBotEnqueueOnceReturnsErrorWhenEveryFeedFails(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{}
	sources := testSources()
	sources[40449] = ChainSources{
		Primary:           testConfiguredPrice("primary", big.NewRat(1000, 1)),
		Gas:               erroringGas{},
		DataFeePerByteWei: big.NewInt(0),
	}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]}), testSettings(), sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	if err := bot.EnqueueOnce(context.Background()); err == nil {
		t.Fatal("EnqueueOnce() error = nil, want total-failure error")
	}
	if len(store.requests) != 0 {
		t.Fatalf("stored requests = %d, want 0", len(store.requests))
	}
}

func TestBotEnqueueOnceMemoizesMarketReadsAcrossFeeds(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{}
	price161 := &countingPrice{source: "counting", price: big.NewRat(2000, 1), observedAt: time.Unix(1_700_000_000, 0)}
	price449 := &countingPrice{source: "counting", price: big.NewRat(1000, 1), observedAt: time.Unix(1_700_000_000, 0)}
	sources := map[uint32]ChainSources{
		40161: {
			Primary:           ConfiguredPriceReader{Name: "counting", Reader: price161, MaxAge: time.Minute},
			Gas:               fixedGas{price: big.NewInt(1_000_000_000)},
			DataFeePerByteWei: big.NewInt(0),
		},
		40449: {
			Primary:           ConfiguredPriceReader{Name: "counting", Reader: price449, MaxAge: time.Minute},
			Gas:               fixedGas{price: big.NewInt(2_000_000_000)},
			DataFeePerByteWei: big.NewInt(0),
		},
	}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, pathways), testSettings(), sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	// Two feed batches share both chains; the memoizing cycle must ask each
	// chain's market source exactly once.
	if err := bot.EnqueueOnce(context.Background()); err != nil {
		t.Fatalf("EnqueueOnce() error = %v", err)
	}
	if len(store.requests) != 2 {
		t.Fatalf("stored requests = %d, want 2", len(store.requests))
	}
	if price161.count.Load() != 1 || price449.count.Load() != 1 {
		t.Fatalf("market calls = %d/%d, want exactly one per chain", price161.count.Load(), price449.count.Load())
	}
}

func TestBotEnqueueOnGasSpikeCarriesDrainMarkerAcrossFailedGasRead(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{}
	gas := &mutableGas{price: big.NewInt(2_300_000_000)}
	sources := testSources()
	sources[40449] = ChainSources{
		Primary:           testConfiguredPrice("primary", big.NewRat(1000, 1)),
		Gas:               gas,
		DataFeePerByteWei: big.NewInt(0),
	}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, []config.PathwayConfig{pathways[0]}), testSettings(), sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	// The feed was pending-observed; the pending row drains, but the first
	// check after the drain fails its gas read. The marker must survive.
	bot.pendingFeeds = map[string]struct{}{pendingFeedKey(40161, pathways[0].SourceWorkers.PriceFeed.Common()): {}}
	gas.err = errors.New("gas provider unavailable")
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike(failed gas) error = %v", err)
	}
	if store.enqueueCalls != 0 {
		t.Fatalf("enqueue calls during failed gas read = %d, want 0", store.enqueueCalls)
	}

	// Gas recovers flat: the carried marker still forces the evaluation.
	gas.err = nil
	if err := bot.EnqueueOnGasSpike(context.Background()); err != nil {
		t.Fatalf("EnqueueOnGasSpike(recovered) error = %v", err)
	}
	if len(store.requests) != 1 {
		t.Fatalf("stored requests = %d, want the forced post-drain write", len(store.requests))
	}
}

type cancellingGas struct {
	cancel context.CancelFunc
}

func (g cancellingGas) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	g.cancel()
	return nil, context.Canceled
}

func TestBotEnqueueOncePropagatesCancellationAfterPartialSuccess(t *testing.T) {
	pathways := testPathways()
	store := &fakeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sources := testSources()
	// Pathway 40449->40161 (healthy) enqueues first or second depending on
	// batch order; the other batch's gas read cancels the caller context.
	sources[40449] = ChainSources{
		Primary:           testConfiguredPrice("primary", big.NewRat(1000, 1)),
		Gas:               cancellingGas{cancel: cancel},
		DataFeePerByteWei: big.NewInt(0),
	}
	bot, err := NewWithDependencies(store, testRegistryWithPathways(t, pathways), testSettings(), sources, emptySnapshotReader{}, discardLogger())
	if err != nil {
		t.Fatalf("NewWithDependencies() error = %v", err)
	}
	bot.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	// Whatever the batch order, a canceled caller context must surface as an
	// error even when another feed batch already succeeded.
	if err := bot.EnqueueOnce(ctx); err == nil {
		t.Fatal("EnqueueOnce() error = nil, want propagated cancellation")
	}
}
