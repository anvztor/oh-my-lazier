package txmgr

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/islishude/oh-my-lazier/go/internal/db"
)

func TestQuoteDynamicFeeClampsPriorityTip(t *testing.T) {
	client := &fakeChainClient{
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_500_000_000),
	}
	quote, err := quoteFee(context.Background(), db.QueuedOutboxTx{ID: 1}, FeePolicy{
		ConfiguredMaxFeePerGas:         big.NewInt(3_000_000_000),
		ConfiguredMaxPriorityFeePerGas: big.NewInt(500_000_000),
	}, client)
	if err != nil {
		t.Fatalf("quoteFee() error = %v", err)
	}
	if !quote.Dynamic {
		t.Fatal("quote.Dynamic = false, want true")
	}
	if quote.MaxPriorityFeePerGas.Cmp(big.NewInt(500_000_000)) != 0 {
		t.Fatalf("priority fee = %s, want clamp to 500000000", quote.MaxPriorityFeePerGas)
	}
	if quote.MaxFeePerGas.Cmp(big.NewInt(1_500_000_000)) != 0 {
		t.Fatalf("max fee = %s, want 1500000000", quote.MaxFeePerGas)
	}
}

func TestQuoteFeeForceLegacyIgnoresBaseFee(t *testing.T) {
	// A legacy-forced chain quotes type-0 fees even when the header reports a
	// base fee: its mempool drops EIP-1559 transactions.
	client := &fakeChainClient{
		header:            dynamicHeader(),
		suggestedGasPrice: big.NewInt(1_000_000_000),
	}
	quote, err := quoteFee(context.Background(), db.QueuedOutboxTx{ID: 1}, FeePolicy{
		ConfiguredMaxFeePerGas:  big.NewInt(3_000_000_000),
		ForceLegacyTransactions: true,
	}, client)
	if err != nil {
		t.Fatalf("quoteFee() error = %v", err)
	}
	if quote.Dynamic {
		t.Fatal("quote.Dynamic = true, want a legacy quote on a legacy-forced chain")
	}
	if quote.MaxFeePerGas.Cmp(big.NewInt(1_000_000_000)) != 0 {
		t.Fatalf("gas price = %s, want the suggested legacy price", quote.MaxFeePerGas)
	}
}

func TestQuoteFeeDefersOverCap(t *testing.T) {
	client := &fakeChainClient{
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	_, err := quoteFee(context.Background(), db.QueuedOutboxTx{ID: 1}, FeePolicy{
		ConfiguredMaxFeePerGas:         big.NewInt(1_500_000_000),
		ConfiguredMaxPriorityFeePerGas: big.NewInt(1_000_000_000),
	}, client)
	if !errors.Is(err, ErrTxDeferred) {
		t.Fatalf("quoteFee() error = %v, want ErrTxDeferred", err)
	}
}

func TestQuoteFeeAssignedNonceWithoutPreviousFeesUsesFreshQuote(t *testing.T) {
	nonce := uint64(7)
	t.Run("dynamic", func(t *testing.T) {
		client := &fakeChainClient{
			header:             dynamicHeader(),
			suggestedGasTipCap: big.NewInt(1_000_000_000),
		}
		quote, err := quoteFee(context.Background(), db.QueuedOutboxTx{ID: 1, Nonce: &nonce}, defaultFeePolicy(), client)
		if err != nil {
			t.Fatalf("quoteFee() error = %v", err)
		}
		if quote.MaxFeePerGas.Cmp(big.NewInt(2_000_000_000)) != 0 {
			t.Fatalf("max fee = %s, want fresh quote", quote.MaxFeePerGas)
		}
		if quote.MaxPriorityFeePerGas.Cmp(big.NewInt(1_000_000_000)) != 0 {
			t.Fatalf("priority fee = %s, want fresh quote", quote.MaxPriorityFeePerGas)
		}
	})
	t.Run("legacy", func(t *testing.T) {
		client := &fakeChainClient{
			header:            legacyHeader(),
			suggestedGasPrice: big.NewInt(5_000_000_000),
		}
		quote, err := quoteFee(context.Background(), db.QueuedOutboxTx{ID: 1, Nonce: &nonce}, defaultFeePolicy(), client)
		if err != nil {
			t.Fatalf("quoteFee() error = %v", err)
		}
		if quote.MaxFeePerGas.Cmp(big.NewInt(5_000_000_000)) != 0 {
			t.Fatalf("gas price = %s, want fresh quote", quote.MaxFeePerGas)
		}
	})
}
