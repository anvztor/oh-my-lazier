package pricing

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/config"
)

func TestSelectPriceRejectsDeviationAboveThreshold(t *testing.T) {
	_, err := SelectPrice(
		SourcePrice{Source: "primary", USD: big.NewRat(2000, 1)},
		SourcePrice{Source: "uniswap", USD: big.NewRat(2101, 1)},
		500,
	)
	if err == nil {
		t.Fatal("SelectPrice() error = nil, want deviation error")
	}
}

func TestSelectPriceRejectsUnhealthyPrimary(t *testing.T) {
	_, err := SelectPrice(
		SourcePrice{Source: "primary"},
		SourcePrice{Source: "uniswap", USD: big.NewRat(2000, 1)},
		500,
	)
	if err == nil {
		t.Fatal("SelectPrice() error = nil, want unhealthy primary error")
	}
}

func TestSelectPriceWithNoSanityReturnsPrimary(t *testing.T) {
	price, err := SelectPriceWithSanity(SourcePrice{Source: "primary", USD: big.NewRat(2000, 1)}, nil, 500)
	if err != nil {
		t.Fatalf("SelectPriceWithSanity() error = %v", err)
	}
	if price.Cmp(big.NewRat(2000, 1)) != 0 {
		t.Fatalf("price = %s, want 2000", price)
	}
}

func TestSelectPriceWithSanityRejectsAnyDeviatingSource(t *testing.T) {
	_, err := SelectPriceWithSanity(
		SourcePrice{Source: "coinmarketcap", USD: big.NewRat(2000, 1)},
		[]SourcePrice{
			{Source: "coingecko", USD: big.NewRat(2000, 1)},
			{Source: "uniswap", USD: big.NewRat(2200, 1)},
		},
		500,
	)
	if err == nil {
		t.Fatal("SelectPriceWithSanity() error = nil, want deviation error")
	}
}

func TestChainNativePriceAllowsEmptySanity(t *testing.T) {
	price, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
		1: {Primary: testConfiguredPrice("primary", big.NewRat(2000, 1))},
	}, 1, testSelectionPolicy())
	if err != nil {
		t.Fatalf("ChainNativePrice() error = %v", err)
	}
	if price.Cmp(big.NewRat(2000, 1)) != 0 {
		t.Fatalf("price = %s, want 2000", price)
	}
}

func TestChainNativePriceAllowsOneFailedSanityWhenAnotherIsHealthy(t *testing.T) {
	price, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
		1: {
			Primary: testConfiguredPrice("primary", big.NewRat(2000, 1)),
			Sanity: []ConfiguredPriceReader{
				{Name: "failed", Reader: failingPrice{}, MaxAge: time.Minute},
				testConfiguredPrice("healthy", big.NewRat(2001, 1)),
			},
		},
	}, 1, testSelectionPolicy())
	if err != nil {
		t.Fatalf("ChainNativePrice() error = %v", err)
	}
	if price.Cmp(big.NewRat(2000, 1)) != 0 {
		t.Fatalf("price = %s, want primary 2000", price)
	}
}

func TestChainNativePriceRejectsAllFailedSanity(t *testing.T) {
	_, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
		1: {
			Primary: testConfiguredPrice("primary", big.NewRat(2000, 1)),
			Sanity:  []ConfiguredPriceReader{{Name: "failed", Reader: failingPrice{}, MaxAge: time.Minute}},
		},
	}, 1, testSelectionPolicy())
	if err == nil {
		t.Fatal("ChainNativePrice() error = nil, want no healthy sanity error")
	}
}

func TestChainNativePriceNeverFallsBackFromPrimary(t *testing.T) {
	_, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
		1: {
			Primary: ConfiguredPriceReader{Name: "primary", Reader: failingPrice{}, MaxAge: time.Minute},
			Sanity:  []ConfiguredPriceReader{testConfiguredPrice("sanity", big.NewRat(2000, 1))},
		},
	}, 1, testSelectionPolicy())
	if err == nil {
		t.Fatal("ChainNativePrice() error = nil, want primary error")
	}
}

func TestChainNativePriceRejectsInvalidObservationMetadata(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tests := []struct {
		name       string
		price      SourcePrice
		wantReason string
	}{
		{name: "stale", price: SourcePrice{Source: "primary", USD: big.NewRat(2000, 1), ObservedAt: now.Add(-2 * time.Minute)}, wantReason: "stale"},
		{name: "future", price: SourcePrice{Source: "primary", USD: big.NewRat(2000, 1), ObservedAt: now.Add(31 * time.Second)}, wantReason: "future"},
		{name: "missing time", price: SourcePrice{Source: "primary", USD: big.NewRat(2000, 1)}, wantReason: "missing observation time"},
		{name: "non-positive", price: SourcePrice{Source: "primary", USD: big.NewRat(0, 1), ObservedAt: now}, wantReason: "non-positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
				1: {Primary: ConfiguredPriceReader{Name: "primary", Reader: staticSourcePrice{price: test.price}, MaxAge: time.Minute}},
			}, 1, PriceSelectionPolicy{MaxDeviationBps: 500, SourceRequestTimeout: time.Second, Now: func() time.Time { return now }})
			if err == nil || !strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("ChainNativePrice() error = %v, want %q", err, test.wantReason)
			}
		})
	}
}

func TestChainNativePriceRejectsObservationThatExpiresDuringRead(t *testing.T) {
	requestStartedAt := time.Unix(1_700_000_000, 0)
	selectionTime := requestStartedAt
	reader := advancingPrice{
		price: SourcePrice{Source: "primary", USD: big.NewRat(2000, 1), ObservedAt: requestStartedAt},
		advance: func() {
			selectionTime = requestStartedAt.Add(2 * time.Minute)
		},
	}
	_, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
		1: {Primary: ConfiguredPriceReader{Name: "primary", Reader: reader, MaxAge: time.Minute}},
	}, 1, PriceSelectionPolicy{
		MaxDeviationBps:      500,
		SourceRequestTimeout: time.Second,
		Now:                  func() time.Time { return selectionTime },
	})
	if err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("ChainNativePrice() error = %v, want stale observation", err)
	}
}

func TestChainNativePriceTimesOutAndReportsFailureCategory(t *testing.T) {
	var failure PriceSourceFailure
	_, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
		1: {Primary: ConfiguredPriceReader{Name: "primary", Reader: blockingPrice{}, MaxAge: time.Minute}},
	}, 1, PriceSelectionPolicy{
		MaxDeviationBps:      500,
		SourceRequestTimeout: time.Millisecond,
		OnSourceFailure:      func(got PriceSourceFailure) { failure = got },
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ChainNativePrice() error = %v, want deadline exceeded", err)
	}
	if failure.EID != 1 || failure.Source != "primary" || failure.Role != "primary" || failure.Category != "timeout" {
		t.Fatalf("failure = %+v", failure)
	}
}

func TestChainNativePriceEnforcesTimeoutForNonCooperativeReader(t *testing.T) {
	started := time.Now()
	_, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
		1: {Primary: ConfiguredPriceReader{Name: "primary", Reader: nonCooperativePrice{}, MaxAge: time.Minute}},
	}, 1, PriceSelectionPolicy{MaxDeviationBps: 500, SourceRequestTimeout: 10 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ChainNativePrice() error = %v, want deadline exceeded", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("ChainNativePrice() exceeded enforced timeout: %s", time.Since(started))
	}
}

func TestChainNativePriceReadsPrimaryAndSanityConcurrently(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	started := make(chan string, 2)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
			1: {
				Primary: ConfiguredPriceReader{Name: "primary", Reader: gatedPrice{source: "primary", started: started, release: release, observedAt: now}, MaxAge: time.Minute},
				Sanity:  []ConfiguredPriceReader{{Name: "sanity", Reader: gatedPrice{source: "sanity", started: started, release: release, observedAt: now}, MaxAge: time.Minute}},
			},
		}, 1, PriceSelectionPolicy{MaxDeviationBps: 500, SourceRequestTimeout: time.Second, Now: func() time.Time { return now }})
		done <- err
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("primary and sanity readers did not start concurrently")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("ChainNativePrice() error = %v", err)
	}
}

func TestChainNativePriceReportsIgnoredSanityFailure(t *testing.T) {
	var failures []PriceSourceFailure
	_, err := ChainNativePrice(context.Background(), map[uint32]ChainSources{
		1: {
			Primary: testConfiguredPrice("primary", big.NewRat(2000, 1)),
			Sanity: []ConfiguredPriceReader{
				{Name: "failed", Reader: failingPrice{}, MaxAge: time.Minute},
				testConfiguredPrice("healthy", big.NewRat(2000, 1)),
			},
		},
	}, 1, PriceSelectionPolicy{
		MaxDeviationBps:      500,
		SourceRequestTimeout: time.Second,
		Now:                  func() time.Time { return time.Unix(1_700_000_000, 0) },
		OnSourceFailure:      func(got PriceSourceFailure) { failures = append(failures, got) },
	})
	if err != nil {
		t.Fatalf("ChainNativePrice() error = %v", err)
	}
	if len(failures) != 1 || failures[0].Source != "failed" || failures[0].Role != "sanity" {
		t.Fatalf("failures = %+v", failures)
	}
}

type staticSourcePrice struct{ price SourcePrice }

func (s staticSourcePrice) PriceUSD(context.Context) (SourcePrice, error) { return s.price, nil }

type advancingPrice struct {
	price   SourcePrice
	advance func()
}

func (p advancingPrice) PriceUSD(context.Context) (SourcePrice, error) {
	p.advance()
	return p.price, nil
}

type blockingPrice struct{}

func (blockingPrice) PriceUSD(ctx context.Context) (SourcePrice, error) {
	<-ctx.Done()
	return SourcePrice{}, ctx.Err()
}

type nonCooperativePrice struct{}

func (nonCooperativePrice) PriceUSD(context.Context) (SourcePrice, error) {
	time.Sleep(200 * time.Millisecond)
	return SourcePrice{}, errors.New("late failure")
}

type gatedPrice struct {
	source     string
	started    chan<- string
	release    <-chan struct{}
	observedAt time.Time
}

func (p gatedPrice) PriceUSD(context.Context) (SourcePrice, error) {
	p.started <- p.source
	<-p.release
	return SourcePrice{Source: p.source, USD: big.NewRat(2000, 1), ObservedAt: p.observedAt}, nil
}

func testSelectionPolicy() PriceSelectionPolicy {
	return PriceSelectionPolicy{
		MaxDeviationBps:      500,
		SourceRequestTimeout: time.Second,
		Now:                  func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
}

func TestBuildPriceSnapshotConvertsDestinationGasPriceToSourceToken(t *testing.T) {
	snapshot, err := BuildPriceSnapshot(PriceInputs{
		SrcNativeUSD:         big.NewRat(2000, 1),
		DstNativeUSD:         big.NewRat(1000, 1),
		DstGasPriceWei:       big.NewInt(2_000_000_000),
		DstDataFeePerByteWei: big.NewInt(0),
		UpdatedAtUnix:        1_700_000_000,
		StaleAfterSeconds:    1800,
	})
	if err != nil {
		t.Fatalf("BuildPriceSnapshot() error = %v", err)
	}
	if snapshot.DstGasPriceInSrcToken.Cmp(big.NewInt(1_000_000_000)) != 0 {
		t.Fatalf("dst gas price = %s, want 1000000000", snapshot.DstGasPriceInSrcToken)
	}
	if snapshot.DstDataFeePerByteInSrcToken.Sign() != 0 {
		t.Fatalf("dst data fee per byte = %s, want 0", snapshot.DstDataFeePerByteInSrcToken)
	}
}

func TestBuildPriceSnapshotRoundsUpFractionalWei(t *testing.T) {
	snapshot, err := BuildPriceSnapshot(PriceInputs{
		SrcNativeUSD:         big.NewRat(3, 1),
		DstNativeUSD:         big.NewRat(2, 1),
		DstGasPriceWei:       big.NewInt(10),
		DstDataFeePerByteWei: big.NewInt(10),
		UpdatedAtUnix:        1,
		StaleAfterSeconds:    2,
	})
	if err != nil {
		t.Fatalf("BuildPriceSnapshot() error = %v", err)
	}
	if snapshot.DstGasPriceInSrcToken.Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("dst gas price = %s, want rounded-up 7", snapshot.DstGasPriceInSrcToken)
	}
	if snapshot.DstDataFeePerByteInSrcToken.Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("dst data fee per byte = %s, want rounded-up 7", snapshot.DstDataFeePerByteInSrcToken)
	}
}

func TestBuildPriceSnapshotRejectsStaleAfterAboveContractMaximum(t *testing.T) {
	_, err := BuildPriceSnapshot(PriceInputs{
		SrcNativeUSD:         big.NewRat(1, 1),
		DstNativeUSD:         big.NewRat(1, 1),
		DstGasPriceWei:       big.NewInt(1),
		DstDataFeePerByteWei: big.NewInt(0),
		UpdatedAtUnix:        1,
		StaleAfterSeconds:    config.MaxPriceSnapshotStaleAfterSeconds + 1,
	})
	if err == nil {
		t.Fatal("BuildPriceSnapshot() error = nil, want OpenPriceFeed stale-after maximum error")
	}
}

func TestBuildPriceSnapshotRejectsUint256Overflow(t *testing.T) {
	tinySourcePrice, ok := new(big.Rat).SetString("1e-70")
	if !ok {
		t.Fatal("failed to parse tiny source price")
	}
	_, err := BuildPriceSnapshot(PriceInputs{
		SrcNativeUSD:         tinySourcePrice,
		DstNativeUSD:         big.NewRat(2000, 1),
		DstGasPriceWei:       big.NewInt(1_000_000_000),
		DstDataFeePerByteWei: big.NewInt(0),
		UpdatedAtUnix:        1,
		StaleAfterSeconds:    2,
	})
	if err == nil || !strings.Contains(err.Error(), "destination gas price exceeds uint256") {
		t.Fatalf("BuildPriceSnapshot() error = %v, want uint256 overflow error", err)
	}
}

func TestSettingsRejectStaleAfterAboveContractMaximum(t *testing.T) {
	settings := Settings{
		Enabled:              true,
		SignerID:             "0x9999999999999999999999999999999999999999",
		Interval:             time.Minute,
		StaleAfter:           time.Duration(config.MaxPriceSnapshotStaleAfterSeconds+1) * time.Second,
		MaxDeviation:         500,
		MinUpdateDeviation:   50,
		Heartbeat:            15 * time.Minute,
		SourceRequestTimeout: time.Second,
		GasSpikeBps:          1000,
	}
	if err := settings.Validate(); err == nil {
		t.Fatal("Settings.Validate() error = nil, want OpenPriceFeed stale-after maximum error")
	}
}

func TestGasIncreaseBpsOnlyCountsUpwardMoves(t *testing.T) {
	if got := GasIncreaseBps(big.NewInt(100), big.NewInt(110)); got != 1000 {
		t.Fatalf("GasIncreaseBps() = %d, want 1000", got)
	}
	if got := GasIncreaseBps(big.NewInt(100), big.NewInt(90)); got != 0 {
		t.Fatalf("GasIncreaseBps() = %d, want 0 for gas decrease", got)
	}
}

func TestBasisPointCalculationsSaturateAboveUint64(t *testing.T) {
	// An unchecked Uint64 conversion truncates this exact result to 500 bps.
	overflowBps := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(500))
	previous := big.NewInt(10_000)
	current := new(big.Int).Add(new(big.Int).Set(previous), overflowBps)
	if got := GasIncreaseBps(previous, current); got != ^uint64(0) {
		t.Fatalf("GasIncreaseBps() = %d, want saturated uint64", got)
	}

	rightNumerator := new(big.Int).Add(new(big.Int).Set(overflowBps), big.NewInt(10_000))
	right := new(big.Rat).SetFrac(rightNumerator, big.NewInt(10_000))
	if got := DeviationBps(big.NewRat(1, 1), right); got != ^uint64(0) {
		t.Fatalf("DeviationBps() = %d, want saturated uint64", got)
	}
}

func TestBuildSetPriceSnapshotCalldata(t *testing.T) {
	snapshot := testPriceSnapshot()
	calldata, err := BuildSetPriceSnapshotCalldata([]PriceSnapshotUpdate{{DstEid: 40449, Snapshot: snapshot}})
	if err != nil {
		t.Fatalf("BuildSetPriceSnapshotCalldata() error = %v", err)
	}
	if len(calldata) == 0 {
		t.Fatal("calldata is empty")
	}
	method := priceSnapshotABI.Methods["setPriceSnapshot"]
	if string(calldata[:4]) != string(method.ID) {
		t.Fatalf("method id = 0x%x, want 0x%x", calldata[:4], method.ID)
	}
}

func TestReadPriceSnapshot(t *testing.T) {
	want := testPriceSnapshot()
	response, err := priceSnapshotABI.Methods["priceSnapshot"].Outputs.Pack(
		want.DstGasPriceInSrcToken,
		want.DstDataFeePerByteInSrcToken,
		want.UpdatedAt,
		want.StaleAfter,
	)
	if err != nil {
		t.Fatalf("priceSnapshot output Pack() error = %v", err)
	}

	got, err := readPriceSnapshot(context.Background(), staticEVMResponseReader{response: response}, common.HexToAddress("0x4444444444444444444444444444444444444444"), 40449, nil)
	if err != nil {
		t.Fatalf("readPriceSnapshot() error = %v", err)
	}
	if got.DstGasPriceInSrcToken.Cmp(want.DstGasPriceInSrcToken) != 0 ||
		got.DstDataFeePerByteInSrcToken.Cmp(want.DstDataFeePerByteInSrcToken) != 0 ||
		got.UpdatedAt != want.UpdatedAt || got.StaleAfter != want.StaleAfter {
		t.Fatalf("readPriceSnapshot() = %+v, want %+v", got, want)
	}
}

func TestBuildSetPriceSnapshotCalldataAcceptsUint256Maximum(t *testing.T) {
	snapshot := testPriceSnapshot()
	snapshot.DstGasPriceInSrcToken = new(big.Int).Set(maxUint256)
	snapshot.DstDataFeePerByteInSrcToken = new(big.Int).Set(maxUint256)
	if _, err := BuildSetPriceSnapshotCalldata([]PriceSnapshotUpdate{{DstEid: 40449, Snapshot: snapshot}}); err != nil {
		t.Fatalf("BuildSetPriceSnapshotCalldata() error = %v, want uint256 maximum accepted", err)
	}
}

func TestBuildSetPriceSnapshotCalldataRejectsEmptyUpdates(t *testing.T) {
	if _, err := BuildSetPriceSnapshotCalldata(nil); err == nil {
		t.Fatal("BuildSetPriceSnapshotCalldata() error = nil, want empty updates error")
	}
}

func TestBuildSetPriceSnapshotCalldataRejectsUint256Overflow(t *testing.T) {
	overflow := new(big.Int).Lsh(big.NewInt(1), 256)
	tests := []struct {
		name   string
		mutate func(*PriceSnapshot)
		want   string
	}{
		{
			name: "destination gas price",
			mutate: func(snapshot *PriceSnapshot) {
				snapshot.DstGasPriceInSrcToken = new(big.Int).Set(overflow)
			},
			want: "destination gas price exceeds uint256",
		},
		{
			name: "destination data fee",
			mutate: func(snapshot *PriceSnapshot) {
				snapshot.DstDataFeePerByteInSrcToken = new(big.Int).Set(overflow)
			},
			want: "destination data fee per byte exceeds uint256",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testPriceSnapshot()
			test.mutate(&snapshot)
			_, err := BuildSetPriceSnapshotCalldata([]PriceSnapshotUpdate{{DstEid: 40449, Snapshot: snapshot}})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("BuildSetPriceSnapshotCalldata() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBuildSetPriceSnapshotTx(t *testing.T) {
	request, err := BuildSetPriceSnapshotTx(
		40161,
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
		"0x9999999999999999999999999999999999999999",
		[]PriceSnapshotUpdate{{DstEid: 40449, Snapshot: testPriceSnapshot()}},
	)
	if err != nil {
		t.Fatalf("BuildSetPriceSnapshotTx() error = %v", err)
	}
	if request.ChainEID != 40161 {
		t.Fatalf("chain eid = %d, want 40161", request.ChainEID)
	}
	if request.Purpose != TxPurposeSetPriceSnapshot {
		t.Fatalf("purpose = %q", request.Purpose)
	}
	if len(request.Calldata) == 0 {
		t.Fatal("calldata is empty")
	}
	if request.Value.Sign() != 0 {
		t.Fatalf("value = %s, want 0", request.Value)
	}
}

func testPriceSnapshot() PriceSnapshot {
	return PriceSnapshot{
		DstGasPriceInSrcToken:       big.NewInt(2_000_000_000),
		DstDataFeePerByteInSrcToken: big.NewInt(0),
		UpdatedAt:                   1_700_000_000,
		StaleAfter:                  1800,
	}
}
