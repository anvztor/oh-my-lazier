package pricing

import (
	"bytes"
	"context"
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
	requests   []db.TxRequest
	enqueueErr error
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

func (s *fakeStore) EnqueueTx(_ context.Context, request db.TxRequest) (int64, error) {
	if s.enqueueErr != nil {
		return 0, s.enqueueErr
	}
	s.requests = append(s.requests, request)
	return int64(len(s.requests)), nil
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
}

func (g *mutableGas) SuggestGasPrice(context.Context) (*big.Int, error) {
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
