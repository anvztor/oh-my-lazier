package txmgr

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	gethkeystore "github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	signeriface "github.com/islishude/oh-my-lazier/go/internal/signer"
	"github.com/islishude/oh-my-lazier/go/internal/signer/keystore"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProcessNextSignsAndBroadcastsDynamicFeeTx(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 10, estimatedGas: 123_456, header: dynamicHeader(), suggestedGasTipCap: big.NewInt(1_000_000_000)}
	logger, logs := captureLogger(slog.LevelInfo)
	manager := New(store, logger)

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count = %d, want 1", len(client.sent))
	}
	sent := client.sent[0]
	if sent.Type() != types.DynamicFeeTxType {
		t.Fatalf("sent tx type = %d, want dynamic fee", sent.Type())
	}
	if sent.Nonce() != 10 {
		t.Fatalf("sent nonce = %d, want 10", sent.Nonce())
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want 1", client.pendingNonceCalls)
	}
	if sent.Gas() != 123_456 {
		t.Fatalf("sent gas = %d, want estimated gas", sent.Gas())
	}
	if sent.GasFeeCap().Cmp(big.NewInt(2_000_000_000)) != 0 {
		t.Fatalf("sent gas fee cap = %s", sent.GasFeeCap())
	}
	if sent.GasTipCap().Cmp(big.NewInt(1_000_000_000)) != 0 {
		t.Fatalf("sent gas tip cap = %s", sent.GasTipCap())
	}
	if client.suggestGasPriceCalls != 0 {
		t.Fatalf("SuggestGasPrice() calls = %d, want 0", client.suggestGasPriceCalls)
	}
	if client.suggestGasTipCapCalls != 1 {
		t.Fatalf("SuggestGasTipCap() calls = %d, want 1", client.suggestGasTipCapCalls)
	}
	assertEstimateGasCall(t, client, signer.Address(), common.HexToAddress("0x2222222222222222222222222222222222222222"), big.NewInt(123), []byte{0x01, 0x02, 0x03})
	from, err := types.Sender(types.LatestSignerForChainID(big.NewInt(11155111)), sent)
	if err != nil {
		t.Fatalf("Sender() error = %v", err)
	}
	if from != signer.Address() {
		t.Fatalf("sender = %s, want %s", from, signer.Address())
	}

	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Status != db.TxStatusBroadcast {
		t.Fatalf("outbox status = %q, want %q", outboxTx.Status, db.TxStatusBroadcast)
	}
	if outboxTx.MaxFeePerGas.Cmp(big.NewInt(2_000_000_000)) != 0 {
		t.Fatalf("recorded max fee = %s", outboxTx.MaxFeePerGas)
	}
	if outboxTx.MaxPriorityFeePerGas.Cmp(big.NewInt(1_000_000_000)) != 0 {
		t.Fatalf("recorded priority fee = %s", outboxTx.MaxPriorityFeePerGas)
	}
	if outboxTx.GasLimit != 123_456 {
		t.Fatalf("recorded gas limit = %d, want estimated gas", outboxTx.GasLimit)
	}
	assertLogContains(t, logs.String(),
		`msg="bootstrapped tx nonce cursor"`,
		`msg="claimed tx nonce"`,
		`nonce=10`,
		`msg="signed tx outbox row"`,
		`gas_limit=123456`,
		`dynamic_fee=true`,
		`msg="broadcast tx outbox row"`,
		`purpose=commit-verification`,
	)
}

func TestProcessNextSignsLegacyTxWithSuggestedGasPrice(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 12, estimatedGas: 98_765, header: legacyHeader(), suggestedGasPrice: big.NewInt(7_000_000_000)}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if id == 0 {
		t.Fatal("ProcessNext() id = 0")
	}
	if client.suggestGasPriceCalls != 1 {
		t.Fatalf("SuggestGasPrice() calls = %d, want 1", client.suggestGasPriceCalls)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count = %d, want 1", len(client.sent))
	}
	sent := client.sent[0]
	if sent.Type() != types.LegacyTxType {
		t.Fatalf("sent tx type = %d, want legacy", sent.Type())
	}
	if sent.Nonce() != 12 {
		t.Fatalf("sent nonce = %d, want 12", sent.Nonce())
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want 1", client.pendingNonceCalls)
	}
	if sent.GasPrice().Cmp(big.NewInt(7_000_000_000)) != 0 {
		t.Fatalf("sent gas price = %s, want 7000000000", sent.GasPrice())
	}
	if sent.Gas() != 98_765 {
		t.Fatalf("sent gas = %d, want estimated gas", sent.Gas())
	}
	assertEstimateGasCall(t, client, signer.Address(), common.HexToAddress("0x2222222222222222222222222222222222222222"), big.NewInt(123), []byte{0x01, 0x02, 0x03})
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.MaxFeePerGas.Cmp(big.NewInt(7_000_000_000)) != 0 {
		t.Fatalf("recorded gas price = %s, want 7000000000", outboxTx.MaxFeePerGas)
	}
	if outboxTx.MaxPriorityFeePerGas != nil {
		t.Fatalf("recorded priority fee = %s, want nil", outboxTx.MaxPriorityFeePerGas)
	}
	if outboxTx.GasLimit != 98_765 {
		t.Fatalf("recorded gas limit = %d, want estimated gas", outboxTx.GasLimit)
	}
	from, err := types.Sender(types.LatestSignerForChainID(big.NewInt(11155111)), sent)
	if err != nil {
		t.Fatalf("Sender() error = %v", err)
	}
	if from != signer.Address() {
		t.Fatalf("sender = %s, want %s", from, signer.Address())
	}
}

func TestProcessNextUsesExistingCursorWithoutPendingNonceAt(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 99, estimatedGas: 123_456, header: dynamicHeader(), suggestedGasTipCap: big.NewInt(1_000_000_000)}
	manager := New(store, discardLogger())

	inserted, err := store.BootstrapTxNonceCursor(t.Context(), 40161, signer.Address().Hex(), 7)
	if err != nil {
		t.Fatalf("BootstrapTxNonceCursor() error = %v", err)
	}
	if !inserted {
		t.Fatal("BootstrapTxNonceCursor() inserted = false, want true")
	}
	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	if _, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if client.pendingNonceCalls != 0 {
		t.Fatalf("PendingNonceAt() calls = %d, want 0", client.pendingNonceCalls)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count = %d, want 1", len(client.sent))
	}
	if client.sent[0].Nonce() != 7 {
		t.Fatalf("sent nonce = %d, want 7", client.sent[0].Nonce())
	}
}

func TestProcessNextRecoversNonceAssignedRow(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 99, estimatedGas: 123_456, header: dynamicHeader(), suggestedGasTipCap: big.NewInt(1_000_000_000)}
	manager := New(store, discardLogger())

	inserted, err := store.BootstrapTxNonceCursor(t.Context(), 40161, signer.Address().Hex(), 71)
	if err != nil {
		t.Fatalf("BootstrapTxNonceCursor() error = %v", err)
	}
	if !inserted {
		t.Fatal("BootstrapTxNonceCursor() inserted = false, want true")
	}
	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	claimed, err := store.ClaimTxNonce(t.Context(), id, 40161, signer.Address().Hex())
	if err != nil {
		t.Fatalf("ClaimTxNonce() error = %v", err)
	}
	if claimed.Nonce != 71 {
		t.Fatalf("claimed nonce = %d, want 71", claimed.Nonce)
	}

	processedID, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if processedID != id {
		t.Fatalf("processed id = %d, want %d", processedID, id)
	}
	if client.pendingNonceCalls != 0 {
		t.Fatalf("PendingNonceAt() calls = %d, want 0", client.pendingNonceCalls)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count = %d, want 1", len(client.sent))
	}
	if client.sent[0].Nonce() != 71 {
		t.Fatalf("sent nonce = %d, want 71", client.sent[0].Nonce())
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want %q", outboxTx.Status, db.TxStatusBroadcast)
	}
}

func TestProcessNextDefersFeeOverCapBeforeNonceAssignment(t *testing.T) {
	tests := []struct {
		name   string
		client *fakeChainClient
		policy FeePolicy
	}{
		{
			name: "dynamic",
			client: &fakeChainClient{
				pendingNonce:       13,
				header:             dynamicHeader(),
				suggestedGasTipCap: big.NewInt(1_000_000_000),
			},
			policy: FeePolicy{
				ConfiguredMaxFeePerGas:         big.NewInt(1_500_000_000),
				ConfiguredMaxPriorityFeePerGas: big.NewInt(1_000_000_000),
			},
		},
		{
			name: "legacy",
			client: &fakeChainClient{
				pendingNonce:      13,
				header:            legacyHeader(),
				suggestedGasPrice: big.NewInt(2_000_000_000),
			},
			policy: FeePolicy{
				ConfiguredMaxFeePerGas: big.NewInt(1_500_000_000),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			signer := newTestKeystoreSigner(t)
			manager := New(store, discardLogger())

			queuedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
				ChainEID: 40161,
				Purpose:  "commit-verification",
				To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
				Calldata: []byte{0x01, 0x02, 0x03},
				Value:    big.NewInt(123),
				SignerID: signer.Address().Hex(),
			})
			if err != nil {
				t.Fatalf("EnqueueTx() error = %v", err)
			}

			_, err = manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, tt.client, tt.policy))
			if !errors.Is(err, ErrTxDeferred) {
				t.Fatalf("ProcessNext() error = %v, want ErrTxDeferred", err)
			}
			outboxTx, err := store.GetOutboxTx(t.Context(), queuedID)
			if err != nil {
				t.Fatalf("GetOutboxTx() error = %v", err)
			}
			if outboxTx.Status != db.TxStatusQueued {
				t.Fatalf("outbox status = %q, want %q", outboxTx.Status, db.TxStatusQueued)
			}
			if outboxTx.Nonce != 0 {
				t.Fatalf("outbox nonce = %d, want unassigned zero value", outboxTx.Nonce)
			}
			if outboxTx.Attempts != 0 {
				t.Fatalf("outbox attempts = %d, want 0", outboxTx.Attempts)
			}
			if outboxTx.MaxFeePerGas != nil || outboxTx.MaxPriorityFeePerGas != nil {
				t.Fatalf("recorded fees = %v/%v, want nil", outboxTx.MaxFeePerGas, outboxTx.MaxPriorityFeePerGas)
			}
			if tt.client.pendingNonceCalls != 0 {
				t.Fatalf("PendingNonceAt() calls = %d, want 0", tt.client.pendingNonceCalls)
			}
			if len(tt.client.estimateGasCalls) != 0 {
				t.Fatalf("EstimateGas() calls = %d, want 0", len(tt.client.estimateGasCalls))
			}
			if len(tt.client.sent) != 0 {
				t.Fatalf("sent tx count = %d, want 0", len(tt.client.sent))
			}
		})
	}
}

func TestProcessNextDefersEstimateGasNonRevertErrorBeforeNonceAssignment(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       13,
		estimateGasErr:     errors.New("rpc unavailable"),
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	logger, logs := captureLogger(slog.LevelDebug)
	manager := New(store, logger)

	queuedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	_, err = manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if !errors.Is(err, ErrTxDeferred) {
		t.Fatalf("ProcessNext() error = %v, want ErrTxDeferred", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), queuedID)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Status != db.TxStatusQueued {
		t.Fatalf("outbox status = %q, want %q", outboxTx.Status, db.TxStatusQueued)
	}
	if outboxTx.Nonce != 0 {
		t.Fatalf("outbox nonce = %d, want unassigned zero value", outboxTx.Nonce)
	}
	if outboxTx.Attempts != 0 {
		t.Fatalf("outbox attempts = %d, want 0", outboxTx.Attempts)
	}
	if client.pendingNonceCalls != 0 {
		t.Fatalf("PendingNonceAt() calls = %d, want 0", client.pendingNonceCalls)
	}
	if len(client.sent) != 0 {
		t.Fatalf("sent tx count = %d, want 0", len(client.sent))
	}
	assertEstimateGasCall(t, client, signer.Address(), common.HexToAddress("0x2222222222222222222222222222222222222222"), big.NewInt(123), []byte{0x01, 0x02, 0x03})
	assertLogContains(t, logs.String(),
		`level=DEBUG`,
		`msg="deferred tx outbox row"`,
		`reason=estimate_gas_error`,
		`error="rpc unavailable"`,
	)
}

func TestProcessNextMarksEstimateGasRevertFailedBeforeNonceAssignment(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       13,
		estimateGasErr:     fakeRPCDataError{message: "execution reverted: denied", data: "0x08c379a0"},
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())

	queuedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	processedID, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if processedID != queuedID {
		t.Fatalf("processed id = %d, want %d", processedID, queuedID)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), queuedID)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Status != db.TxStatusFailed {
		t.Fatalf("outbox status = %q, want %q", outboxTx.Status, db.TxStatusFailed)
	}
	if outboxTx.Nonce != 0 {
		t.Fatalf("outbox nonce = %d, want unassigned zero value", outboxTx.Nonce)
	}
	if outboxTx.Attempts != 0 {
		t.Fatalf("outbox attempts = %d, want 0", outboxTx.Attempts)
	}
	if outboxTx.FailureKind != db.TxFailureEstimateGasRevert || outboxTx.NextRetryAt == nil {
		t.Fatalf("failure metadata = %q/%v, want retryable estimate gas revert", outboxTx.FailureKind, outboxTx.NextRetryAt)
	}
	if client.pendingNonceCalls != 0 {
		t.Fatalf("PendingNonceAt() calls = %d, want 0", client.pendingNonceCalls)
	}
	if len(client.sent) != 0 {
		t.Fatalf("sent tx count = %d, want 0", len(client.sent))
	}
	assertEstimateGasCall(t, client, signer.Address(), common.HexToAddress("0x2222222222222222222222222222222222222222"), big.NewInt(123), []byte{0x01, 0x02, 0x03})
}

func TestIsEstimateGasRevertChecksRedactedErrorCause(t *testing.T) {
	cause := fakeRPCError{message: "execution reverted: denied", code: -32000}
	err := redactedProviderError{cause: cause}
	if err.Error() != "provider[0] eth_estimateGas failed" {
		t.Fatalf("error = %q, want redacted provider error", err)
	}
	if !isEstimateGasRevert(err) {
		t.Fatal("isEstimateGasRevert() = false, want wrapped revert classification")
	}
}

func TestProcessNextLegacyGasPriceFailuresLeaveOutboxQueued(t *testing.T) {
	tests := []struct {
		name              string
		suggestedGasPrice *big.Int
		suggestErr        error
	}{
		{name: "rpc error", suggestErr: errors.New("gas price unavailable")},
		{name: "nil gas price"},
		{name: "zero gas price", suggestedGasPrice: new(big.Int)},
		{name: "negative gas price", suggestedGasPrice: big.NewInt(-1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			signer := newTestKeystoreSigner(t)
			client := &fakeChainClient{
				pendingNonce:       13,
				header:             legacyHeader(),
				suggestedGasPrice:  tt.suggestedGasPrice,
				suggestGasPriceErr: tt.suggestErr,
			}
			manager := New(store, discardLogger())

			queuedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
				ChainEID: 40161,
				Purpose:  "commit-verification",
				To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
				Calldata: []byte{0x01, 0x02, 0x03},
				Value:    big.NewInt(123),
				SignerID: signer.Address().Hex(),
			})
			if err != nil {
				t.Fatalf("EnqueueTx() error = %v", err)
			}

			if _, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err == nil {
				t.Fatal("ProcessNext() error = nil, want gas price error")
			}
			outboxTx, err := store.GetOutboxTx(t.Context(), queuedID)
			if err != nil {
				t.Fatalf("GetOutboxTx() error = %v", err)
			}
			if outboxTx.Status != db.TxStatusQueued {
				t.Fatalf("outbox status = %q, want %q", outboxTx.Status, db.TxStatusQueued)
			}
			if outboxTx.Nonce != 0 {
				t.Fatalf("outbox nonce = %d, want unassigned zero value", outboxTx.Nonce)
			}
			if len(client.sent) != 0 {
				t.Fatalf("sent tx count = %d, want 0", len(client.sent))
			}
		})
	}
}

func TestProcessNextSendFailureConsumesNonceAndNextTxUsesCursor(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       31,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		sendErr:            errors.New("broadcast timeout"),
	}
	logger, logs := captureLogger(slog.LevelInfo)
	manager := New(store, logger)

	firstID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(first) error = %v", err)
	}

	processedID, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext(first) error = %v", err)
	}
	if processedID != firstID {
		t.Fatalf("processed id = %d, want %d", processedID, firstID)
	}
	failedTx, err := store.GetOutboxTx(t.Context(), firstID)
	if err != nil {
		t.Fatalf("GetOutboxTx(first) error = %v", err)
	}
	if failedTx.Status != db.TxStatusFailed {
		t.Fatalf("failed status = %q, want %q", failedTx.Status, db.TxStatusFailed)
	}
	if failedTx.Nonce != 31 {
		t.Fatalf("failed nonce = %d, want 31", failedTx.Nonce)
	}
	if failedTx.TxHash == (common.Hash{}) {
		t.Fatal("failed tx hash = zero, want signed hash retained")
	}
	if failedTx.GasLimit != 0 || failedTx.MaxFeePerGas != nil || failedTx.MaxPriorityFeePerGas != nil {
		t.Fatalf("failed gas/fees = %d/%v/%v, want cleared", failedTx.GasLimit, failedTx.MaxFeePerGas, failedTx.MaxPriorityFeePerGas)
	}
	if failedTx.FailureKind != db.TxFailureBroadcastFailed || failedTx.NextRetryAt == nil {
		t.Fatalf("failure metadata = %q/%v, want retryable broadcast failure", failedTx.FailureKind, failedTx.NextRetryAt)
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want 1", client.pendingNonceCalls)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count = %d, want 1", len(client.sent))
	}
	assertLogContains(t, logs.String(),
		`msg="failed tx broadcast"`,
		`failure_kind=broadcast_failed`,
		`error="broadcast timeout"`,
	)

	client.sendErr = nil
	secondID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05, 0x06},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(second) error = %v", err)
	}
	processedID, err = manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext(second) error = %v", err)
	}
	if processedID != secondID {
		t.Fatalf("processed id = %d, want %d", processedID, secondID)
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want 1", client.pendingNonceCalls)
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	if client.sent[1].Nonce() != 32 {
		t.Fatalf("second sent nonce = %d, want 32", client.sent[1].Nonce())
	}
}

func TestProcessOnceRetriesDueBroadcastFailureBeforeQueuedTx(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       31,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		sendErr:            errors.New("broadcast timeout"),
	}
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	manager := NewWithTargets(store, []Target{target}, discardLogger())

	failedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(failed) error = %v", err)
	}
	processedID, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if processedID != failedID {
		t.Fatalf("processed id = %d, want %d", processedID, failedID)
	}
	forceRetryDue(t, failedID)
	queuedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x3333333333333333333333333333333333333333"),
		Calldata: []byte{0x04, 0x05, 0x06},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(queued) error = %v", err)
	}

	processed, err := manager.processOnce(t.Context())
	if err != nil {
		t.Fatalf("processOnce(retry) error = %v", err)
	}
	if !processed {
		t.Fatal("processOnce(retry) processed = false, want true")
	}
	retryTx, err := store.GetOutboxTx(t.Context(), failedID)
	if err != nil {
		t.Fatalf("GetOutboxTx(retry) error = %v", err)
	}
	if retryTx.Status != db.TxStatusQueued {
		t.Fatalf("retry status = %q, want %q", retryTx.Status, db.TxStatusQueued)
	}
	if retryTx.Attempts != 1 {
		t.Fatalf("retry attempts = %d, want 1", retryTx.Attempts)
	}
	if retryTx.FailureKind != "" || retryTx.NextRetryAt != nil {
		t.Fatalf("retry failure metadata = %q/%v, want cleared", retryTx.FailureKind, retryTx.NextRetryAt)
	}
	if retryTx.GasLimit != 0 || retryTx.MaxFeePerGas != nil || retryTx.MaxPriorityFeePerGas != nil {
		t.Fatalf("retry gas/fees = %d/%v/%v, want cleared for fresh quote", retryTx.GasLimit, retryTx.MaxFeePerGas, retryTx.MaxPriorityFeePerGas)
	}
	stillQueued, err := store.GetOutboxTx(t.Context(), queuedID)
	if err != nil {
		t.Fatalf("GetOutboxTx(queued) error = %v", err)
	}
	if stillQueued.Status != db.TxStatusQueued {
		t.Fatalf("later queued status = %q, want queued", stillQueued.Status)
	}

	client.sendErr = nil
	processed, err = manager.processOnce(t.Context())
	if err != nil {
		t.Fatalf("processOnce(send retry) error = %v", err)
	}
	if !processed {
		t.Fatal("processOnce(send retry) processed = false, want true")
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	if client.sent[1].Nonce() != 31 {
		t.Fatalf("replacement nonce = %d, want 31", client.sent[1].Nonce())
	}
	if client.sent[1].GasFeeCap().Cmp(big.NewInt(2_000_000_000)) != 0 {
		t.Fatalf("replacement max fee = %s, want fresh quote", client.sent[1].GasFeeCap())
	}
	if client.sent[1].GasTipCap().Cmp(big.NewInt(1_000_000_000)) != 0 {
		t.Fatalf("replacement priority fee = %s, want fresh quote", client.sent[1].GasTipCap())
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want only bootstrap call", client.pendingNonceCalls)
	}
}

func TestProcessOnceReplacesStaleBroadcastBeforeQueuedTx(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       31,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	manager := NewWithTargets(store, []Target{target}, discardLogger())

	staleID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(stale) error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext(stale) error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), staleID)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	forceBroadcastStale(t, staleID)
	queuedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x3333333333333333333333333333333333333333"),
		Calldata: []byte{0x04, 0x05, 0x06},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(queued) error = %v", err)
	}

	processed, err := manager.processOnce(t.Context())
	if err != nil {
		t.Fatalf("processOnce(stale replacement) error = %v", err)
	}
	if !processed {
		t.Fatal("processOnce(stale replacement) processed = false, want true")
	}
	replacement, err := store.GetOutboxTx(t.Context(), staleID)
	if err != nil {
		t.Fatalf("GetOutboxTx(stale) error = %v", err)
	}
	if replacement.Status != db.TxStatusBroadcast {
		t.Fatalf("replacement status = %q, want broadcast", replacement.Status)
	}
	if replacement.Nonce != 31 {
		t.Fatalf("replacement nonce = %d, want 31", replacement.Nonce)
	}
	if replacement.Attempts != 1 {
		t.Fatalf("replacement attempts = %d, want 1", replacement.Attempts)
	}
	if replacement.TxHash == original.TxHash || replacement.TxHash == (common.Hash{}) {
		t.Fatalf("replacement tx hash = %s, want non-zero hash distinct from original %s", replacement.TxHash, original.TxHash)
	}
	if replacement.GasLimit != 123_456 {
		t.Fatalf("replacement gas limit = %d, want 123456", replacement.GasLimit)
	}
	if replacement.MaxFeePerGas.Cmp(big.NewInt(2_200_000_000)) != 0 {
		t.Fatalf("replacement max fee = %s, want 2200000000", replacement.MaxFeePerGas)
	}
	if replacement.MaxPriorityFeePerGas.Cmp(big.NewInt(1_100_000_000)) != 0 {
		t.Fatalf("replacement priority fee = %s, want 1100000000", replacement.MaxPriorityFeePerGas)
	}
	stillQueued, err := store.GetOutboxTx(t.Context(), queuedID)
	if err != nil {
		t.Fatalf("GetOutboxTx(queued) error = %v", err)
	}
	if stillQueued.Status != db.TxStatusQueued {
		t.Fatalf("later queued status = %q, want queued", stillQueued.Status)
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	sentReplacement := client.sent[1]
	if sentReplacement.Nonce() != 31 {
		t.Fatalf("replacement nonce = %d, want 31", sentReplacement.Nonce())
	}
	if sentReplacement.Gas() != 123_456 {
		t.Fatalf("replacement gas = %d, want original signed gas limit", sentReplacement.Gas())
	}
	if sentReplacement.GasFeeCap().Cmp(big.NewInt(2_200_000_000)) != 0 {
		t.Fatalf("replacement max fee = %s, want 2200000000", sentReplacement.GasFeeCap())
	}
	if sentReplacement.GasTipCap().Cmp(big.NewInt(1_100_000_000)) != 0 {
		t.Fatalf("replacement priority fee = %s, want 1100000000", sentReplacement.GasTipCap())
	}
}

func TestProcessOnceReceiptWinsOverStaleBroadcastReplacement(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       32,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
	}
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	manager := NewWithTargets(store, []Target{target}, discardLogger())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	forceBroadcastStale(t, id)
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusSuccessful)

	processed, err := manager.processOnce(t.Context())
	if err != nil {
		t.Fatalf("processOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processOnce() processed = false, want true")
	}
	confirmed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(confirmed) error = %v", err)
	}
	if confirmed.Status != db.TxStatusConfirmed {
		t.Fatalf("status = %q, want confirmed", confirmed.Status)
	}
	if confirmed.Attempts != 0 {
		t.Fatalf("attempts = %d, want 0", confirmed.Attempts)
	}
}

func TestStaleBroadcastReplacementDefersWhenBumpExceedsCap(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       33,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	forceBroadcastStale(t, id)

	lowCapPolicy := FeePolicy{
		ConfiguredMaxFeePerGas:         big.NewInt(2_100_000_000),
		ConfiguredMaxPriorityFeePerGas: big.NewInt(2_000_000_000),
	}
	_, err = manager.ProcessStaleBroadcastReplacement(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, lowCapPolicy))
	if !errors.Is(err, ErrTxDeferred) {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v, want ErrTxDeferred", err)
	}
	deferred, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if deferred.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want broadcast", deferred.Status)
	}
	if deferred.TxHash != original.TxHash {
		t.Fatalf("tx hash = %s, want original %s", deferred.TxHash, original.TxHash)
	}
	if deferred.Attempts != 0 {
		t.Fatalf("attempts = %d, want 0", deferred.Attempts)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count = %d, want original only", len(client.sent))
	}

	client.receipts = map[common.Hash]*types.Receipt{
		original.TxHash: testReceipt(original.TxHash, types.ReceiptStatusSuccessful),
	}
	if _, err := manager.ProcessReceipts(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, lowCapPolicy), 1); err != nil {
		t.Fatalf("ProcessReceipts(original) error = %v", err)
	}
	confirmed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(confirmed) error = %v", err)
	}
	if confirmed.Status != db.TxStatusConfirmed {
		t.Fatalf("status after original receipt = %q, want confirmed", confirmed.Status)
	}
}

func TestStaleBroadcastReplacementUsesConfiguredDuration(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       35,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := NewWithOptions(store, discardLogger(), Options{
		StaleBroadcastReplacementAfter: 2 * time.Second,
	})

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	forceBroadcastAgeSeconds(t, id, 3)
	client.estimateGasErr = errors.New("pending state already applied original tx")

	replacedID, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v", err)
	}
	if replacedID != id {
		t.Fatalf("replacement id = %d, want %d", replacedID, id)
	}
	replacement, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(replacement) error = %v", err)
	}
	if replacement.TxHash == original.TxHash {
		t.Fatalf("replacement tx hash = original %s, want distinct replacement", original.TxHash)
	}
	if replacement.GasLimit != original.GasLimit {
		t.Fatalf("replacement gas limit = %d, want original %d", replacement.GasLimit, original.GasLimit)
	}
}

func TestStaleBroadcastReplacementSkipsMinedTxAwaitingConfirmations(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       35,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
	}
	manager := NewWithOptions(store, discardLogger(), Options{
		StaleBroadcastReplacementAfter: 2 * time.Second,
	})

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	// The tx is mined but not yet deep, so its receipt exists while the row is
	// still stale by time. It must not be replaced.
	client.receipts[original.TxHash] = testReceipt(original.TxHash, types.ReceiptStatusSuccessful)
	forceBroadcastAgeSeconds(t, id, 3)

	skippedID, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v", err)
	}
	if skippedID != id {
		t.Fatalf("skipped id = %d, want %d", skippedID, id)
	}
	after, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(after) error = %v", err)
	}
	if after.TxHash != original.TxHash {
		t.Fatal("replaced a mined tx that was only awaiting confirmation depth")
	}
}

func TestStaleBroadcastReplacementRecoversSignedRows(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       36,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	forceOutboxStatus(t, id, db.TxStatusSigned)
	forceBroadcastStale(t, id)

	replacedID, err := manager.ProcessStaleBroadcastReplacement(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v", err)
	}
	if replacedID != id {
		t.Fatalf("replacement id = %d, want %d", replacedID, id)
	}
	replacement, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(replacement) error = %v", err)
	}
	if replacement.Status != db.TxStatusBroadcast {
		t.Fatalf("replacement status = %q, want %q", replacement.Status, db.TxStatusBroadcast)
	}
	if replacement.Nonce != original.Nonce {
		t.Fatalf("replacement nonce = %d, want %d", replacement.Nonce, original.Nonce)
	}
	if replacement.TxHash == original.TxHash || replacement.TxHash == (common.Hash{}) {
		t.Fatalf("replacement tx hash = %s, want non-zero distinct from original %s", replacement.TxHash, original.TxHash)
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	if client.sent[1].Nonce() != original.Nonce {
		t.Fatalf("replacement tx nonce = %d, want %d", client.sent[1].Nonce(), original.Nonce)
	}
}

func TestStaleBroadcastReplacementStopsAtAttemptCap(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       34,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	forceBroadcastStale(t, id)
	forceOutboxAttempts(t, id, db.TxAutoRetryMaxAttempts)

	_, err = manager.ProcessStaleBroadcastReplacement(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if !errors.Is(err, db.ErrNoStaleBroadcastReplacement) {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	capped, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if capped.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want broadcast", capped.Status)
	}
	if capped.Attempts != db.TxAutoRetryMaxAttempts {
		t.Fatalf("attempts = %d, want %d", capped.Attempts, db.TxAutoRetryMaxAttempts)
	}
}

func TestProcessNextSignFailureRetainsAssignedNonce(t *testing.T) {
	store := openTestStore(t)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	signer := failingSigner{address: crypto.PubkeyToAddress(key.PublicKey)}
	client := &fakeChainClient{
		pendingNonce:       41,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	logger, logs := captureLogger(slog.LevelInfo)
	manager := New(store, logger)

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "commit-verification",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	processedID, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if processedID != id {
		t.Fatalf("processed id = %d, want %d", processedID, id)
	}
	failedTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if failedTx.Status != db.TxStatusFailed {
		t.Fatalf("status = %q, want %q", failedTx.Status, db.TxStatusFailed)
	}
	if failedTx.Nonce != 41 {
		t.Fatalf("nonce = %d, want 41", failedTx.Nonce)
	}
	if failedTx.TxHash != (common.Hash{}) {
		t.Fatalf("tx hash = %s, want zero hash", failedTx.TxHash)
	}
	if failedTx.GasLimit != 0 || failedTx.MaxFeePerGas != nil || failedTx.MaxPriorityFeePerGas != nil {
		t.Fatalf("failed gas/fees = %d/%v/%v, want cleared", failedTx.GasLimit, failedTx.MaxFeePerGas, failedTx.MaxPriorityFeePerGas)
	}
	if failedTx.FailureKind != db.TxFailureSignFailed || failedTx.NextRetryAt == nil {
		t.Fatalf("failure metadata = %q/%v, want retryable sign failure", failedTx.FailureKind, failedTx.NextRetryAt)
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want 1", client.pendingNonceCalls)
	}
	if len(client.sent) != 0 {
		t.Fatalf("sent tx count = %d, want 0", len(client.sent))
	}
	assertLogContains(t, logs.String(),
		`msg="failed tx signing"`,
		`failure_kind=sign_failed`,
		`error="sign tx failed"`,
	)
}

func TestPrepareReplacementTxPreservesNonceAndBumpsFees(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 21, estimatedGas: 111_111, header: dynamicHeader(), suggestedGasTipCap: big.NewInt(1_000_000_000)}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "lz-receive",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if err := store.PrepareReplacementTx(t.Context(), id); err != nil {
		t.Fatalf("PrepareReplacementTx() error = %v", err)
	}
	replacement, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if replacement.Nonce != 21 {
		t.Fatalf("replacement nonce = %d, want 21", replacement.Nonce)
	}
	if replacement.Status != db.TxStatusQueued {
		t.Fatalf("replacement status = %q, want %q", replacement.Status, db.TxStatusQueued)
	}
	if replacement.MaxFeePerGas.Cmp(big.NewInt(2_000_000_000)) != 0 {
		t.Fatalf("replacement max fee = %s", replacement.MaxFeePerGas)
	}
	if replacement.MaxPriorityFeePerGas.Cmp(big.NewInt(1_000_000_000)) != 0 {
		t.Fatalf("replacement priority fee = %s", replacement.MaxPriorityFeePerGas)
	}
	if replacement.Attempts != 1 {
		t.Fatalf("replacement attempts = %d, want 1", replacement.Attempts)
	}
	client.estimatedGas = 222_222
	replacementID, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() replacement error = %v", err)
	}
	if replacementID != id {
		t.Fatalf("replacement id = %d, want %d", replacementID, id)
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want 1", client.pendingNonceCalls)
	}
	replacementTx := client.sent[1]
	if replacementTx.Nonce() != 21 {
		t.Fatalf("replacement tx nonce = %d, want 21", replacementTx.Nonce())
	}
	if replacementTx.Gas() != 222_222 {
		t.Fatalf("replacement tx gas = %d, want re-estimated gas", replacementTx.Gas())
	}
	if replacementTx.GasFeeCap().Cmp(big.NewInt(2_200_000_000)) != 0 {
		t.Fatalf("replacement tx max fee = %s", replacementTx.GasFeeCap())
	}
	if replacementTx.GasTipCap().Cmp(big.NewInt(1_100_000_000)) != 0 {
		t.Fatalf("replacement tx priority fee = %s", replacementTx.GasTipCap())
	}
	if len(client.estimateGasCalls) != 2 {
		t.Fatalf("EstimateGas() calls = %d, want 2", len(client.estimateGasCalls))
	}
}

func TestPrepareReplacementTxPreservesNonceAndRefreshesGasPrice(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 22, estimatedGas: 111_111, header: legacyHeader(), suggestedGasPrice: big.NewInt(4_000_000_000)}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "lz-receive",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if err := store.PrepareReplacementTx(t.Context(), id); err != nil {
		t.Fatalf("PrepareReplacementTx() error = %v", err)
	}
	replacement, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if replacement.Nonce != 22 {
		t.Fatalf("replacement nonce = %d, want 22", replacement.Nonce)
	}
	if replacement.Status != db.TxStatusQueued {
		t.Fatalf("replacement status = %q, want %q", replacement.Status, db.TxStatusQueued)
	}
	if replacement.Attempts != 1 {
		t.Fatalf("replacement attempts = %d, want 1", replacement.Attempts)
	}

	client.suggestedGasPrice = big.NewInt(5_000_000_000)
	client.estimatedGas = 222_222
	replacementID, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() replacement error = %v", err)
	}
	if replacementID != id {
		t.Fatalf("replacement id = %d, want %d", replacementID, id)
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want 1", client.pendingNonceCalls)
	}
	replacementTx := client.sent[1]
	if replacementTx.Type() != types.LegacyTxType {
		t.Fatalf("replacement tx type = %d, want legacy", replacementTx.Type())
	}
	if replacementTx.Nonce() != 22 {
		t.Fatalf("replacement tx nonce = %d, want 22", replacementTx.Nonce())
	}
	if replacementTx.Gas() != 222_222 {
		t.Fatalf("replacement tx gas = %d, want re-estimated gas", replacementTx.Gas())
	}
	if replacementTx.GasPrice().Cmp(big.NewInt(5_000_000_000)) != 0 {
		t.Fatalf("replacement tx gas price = %s", replacementTx.GasPrice())
	}
	if len(client.estimateGasCalls) != 2 {
		t.Fatalf("EstimateGas() calls = %d, want 2", len(client.estimateGasCalls))
	}
}

func TestProcessReceiptsMarksBroadcastTxConfirmed(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 33,
		receipts:     make(map[common.Hash]*types.Receipt),
	}
	logger, logs := captureLogger(slog.LevelInfo)
	manager := New(store, logger)

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "lz-receive",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Nonce >= client.pendingNonce {
		client.pendingNonce = outboxTx.Nonce + 1
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusSuccessful)

	processedID, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: 40161,
		ChainID:  big.NewInt(11155111),
		Signer:   signer,
		Client:   client,
	}, 1)
	if err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	if processedID != id {
		t.Fatalf("processed id = %d, want %d", processedID, id)
	}
	confirmed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() after receipt error = %v", err)
	}
	if confirmed.Status != db.TxStatusConfirmed {
		t.Fatalf("status = %q, want %q", confirmed.Status, db.TxStatusConfirmed)
	}
	if confirmed.ReceiptTxHash != outboxTx.TxHash {
		t.Fatalf("receipt tx hash = %s, want %s", confirmed.ReceiptTxHash, outboxTx.TxHash)
	}
	if confirmed.ReceiptStatus == nil || *confirmed.ReceiptStatus != types.ReceiptStatusSuccessful {
		t.Fatalf("receipt status = %v, want successful", confirmed.ReceiptStatus)
	}
	if confirmed.ReceiptBlockNumber == nil || *confirmed.ReceiptBlockNumber != 1_234_567 {
		t.Fatalf("receipt block number = %v, want 1234567", confirmed.ReceiptBlockNumber)
	}
	if confirmed.ReceiptGasUsed == nil || *confirmed.ReceiptGasUsed != 21_000 {
		t.Fatalf("receipt gas used = %v, want 21000", confirmed.ReceiptGasUsed)
	}
	if confirmed.ReceiptEffectiveGasPrice == nil || confirmed.ReceiptEffectiveGasPrice.Cmp(big.NewInt(2_000_000_000)) != 0 {
		t.Fatalf("receipt effective gas price = %v, want 2000000000", confirmed.ReceiptEffectiveGasPrice)
	}
	if confirmed.ReceiptGasCostDstWei == nil || confirmed.ReceiptGasCostDstWei.Cmp(big.NewInt(42_000_000_000_000)) != 0 {
		t.Fatalf("receipt destination gas cost = %v, want 42000000000000", confirmed.ReceiptGasCostDstWei)
	}
	if confirmed.ReceiptObservedAt == nil {
		t.Fatal("receipt observed at = nil, want timestamp")
	}
	assertLogContains(t, logs.String(),
		`msg="confirmed tx receipt"`,
		`chain_eid=40161`,
		`purpose=lz-receive`,
		`receipt_status=1`,
	)
}

func TestProcessReceiptsDefersReceiptBelowConfirmationDepth(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 33,
		receipts:     make(map[common.Hash]*types.Receipt),
		// Head is the receipt's own block, so the receipt is only 1 block deep.
		header: &types.Header{Number: big.NewInt(1_234_567)},
	}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "lz-receive",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Nonce >= client.pendingNonce {
		client.pendingNonce = outboxTx.Nonce + 1
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusSuccessful)

	target := Target{ChainEID: 40161, ChainID: big.NewInt(11155111), Signer: signer, Client: client, Confirmations: 12}
	if _, err := manager.ProcessReceipts(t.Context(), target, 1); !errors.Is(err, ErrNoReceiptUpdate) {
		t.Fatalf("ProcessReceipts() error = %v, want ErrNoReceiptUpdate (receipt below confirmation depth)", err)
	}
	pending, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() after receipt error = %v", err)
	}
	if pending.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want %q (must stay broadcast until confirmed)", pending.Status, db.TxStatusBroadcast)
	}
	if pending.ReceiptTxHash != (common.Hash{}) {
		t.Fatal("receipt facts recorded before reaching confirmation depth")
	}
}

func TestProcessReceiptsConfirmsReceiptAtConfirmationDepth(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 34,
		receipts:     make(map[common.Hash]*types.Receipt),
		// Head is 11 blocks past the receipt, so it is exactly 12 blocks deep.
		header: &types.Header{Number: big.NewInt(1_234_567 + 11)},
	}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "lz-receive",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Nonce >= client.pendingNonce {
		client.pendingNonce = outboxTx.Nonce + 1
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusSuccessful)

	target := Target{ChainEID: 40161, ChainID: big.NewInt(11155111), Signer: signer, Client: client, Confirmations: 12}
	processedID, err := manager.ProcessReceipts(t.Context(), target, 1)
	if err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	if processedID != id {
		t.Fatalf("processed id = %d, want %d", processedID, id)
	}
	confirmed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() after receipt error = %v", err)
	}
	if confirmed.Status != db.TxStatusConfirmed {
		t.Fatalf("status = %q, want %q", confirmed.Status, db.TxStatusConfirmed)
	}
}

func TestProcessReceiptsRejectsMismatchedReceiptTxHash(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 34,
		receipts:     make(map[common.Hash]*types.Receipt),
	}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  "lz-receive",
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	wrongHash := common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	client.receipts[outboxTx.TxHash] = testReceipt(wrongHash, types.ReceiptStatusSuccessful)

	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: 40161,
		ChainID:  big.NewInt(11155111),
		Signer:   signer,
		Client:   client,
	}, 1); err == nil || !strings.Contains(err.Error(), "receipt tx hash") {
		t.Fatalf("ProcessReceipts() error = %v, want receipt hash mismatch", err)
	}
	got, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() after mismatch error = %v", err)
	}
	if got.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want %q", got.Status, db.TxStatusBroadcast)
	}
	if got.ReceiptTxHash != (common.Hash{}) {
		t.Fatalf("receipt tx hash = %s, want zero hash", got.ReceiptTxHash)
	}
}

func TestProcessReceiptsMarksExecutorLzReceiveDelivered(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 44,
		receipts:     make(map[common.Hash]*types.Receipt),
	}
	manager := New(store, discardLogger())
	packet := testExecutorPacket(t)
	packet.Status = string(packets.ExecutorExecutable)
	if err := store.UpsertPacket(t.Context(), packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(t.Context(), db.ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if _, err := store.EnqueueExecutorTx(
		t.Context(),
		packet.GUID,
		string(packets.ExecutorExecutable),
		string(packets.ExecutorLzReceiveTxEnqueued),
		db.TxRequest{
			ChainEID: packet.DstEID,
			Purpose:  executorLzReceivePurpose,
			GUID:     packet.GUID.Bytes(),
			To:       packet.Receiver,
			Calldata: []byte{0x04, 0x05},
			Value:    big.NewInt(0),
			SignerID: signer.Address().Hex(),
		},
	); err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Nonce >= client.pendingNonce {
		client.pendingNonce = outboxTx.Nonce + 1
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusSuccessful)

	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	delivered, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if delivered.Status != string(packets.ExecutorDelivered) {
		t.Fatalf("packet status = %q, want %q", delivered.Status, packets.ExecutorDelivered)
	}
}

func TestProcessReceiptsMarksExecutorLzReceiveFailed(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 55,
		receipts:     make(map[common.Hash]*types.Receipt),
	}
	logger, logs := captureLogger(slog.LevelInfo)
	manager := New(store, logger)
	packet := testExecutorPacket(t)
	packet.Status = string(packets.ExecutorExecutable)
	if err := store.UpsertPacket(t.Context(), packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(t.Context(), db.ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if _, err := store.EnqueueExecutorTx(
		t.Context(),
		packet.GUID,
		string(packets.ExecutorExecutable),
		string(packets.ExecutorLzReceiveTxEnqueued),
		db.TxRequest{
			ChainEID: packet.DstEID,
			Purpose:  executorLzReceivePurpose,
			GUID:     packet.GUID.Bytes(),
			To:       packet.Receiver,
			Calldata: []byte{0x04, 0x05},
			Value:    big.NewInt(0),
			SignerID: signer.Address().Hex(),
		},
	); err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusFailed)

	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	failedPacket, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if failedPacket.Status != string(packets.ExecutorLzReceiveFailed) {
		t.Fatalf("packet status = %q, want %q", failedPacket.Status, packets.ExecutorLzReceiveFailed)
	}
	failedTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() after receipt error = %v", err)
	}
	if failedTx.Status != db.TxStatusFailed {
		t.Fatalf("tx status = %q, want %q", failedTx.Status, db.TxStatusFailed)
	}
	assertLogContains(t, logs.String(),
		`msg="failed tx receipt"`,
		`purpose=executor_lz_receive`,
		`receipt_status=0`,
		`failure_kind=receipt_failed`,
	)
}

func TestProcessReceiptsResolvesRevertedLzReceiveAfterThirdPartyDelivery(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 77,
		receipts:     make(map[common.Hash]*types.Receipt),
	}
	manager := New(store, discardLogger())
	packet := testExecutorPacket(t)
	packet.Status = string(packets.ExecutorExecutable)
	if err := store.UpsertPacket(t.Context(), packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(t.Context(), db.ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if _, err := store.EnqueueExecutorTx(
		t.Context(),
		packet.GUID,
		string(packets.ExecutorExecutable),
		string(packets.ExecutorLzReceiveTxEnqueued),
		db.TxRequest{
			ChainEID: packet.DstEID,
			Purpose:  executorLzReceivePurpose,
			GUID:     packet.GUID.Bytes(),
			To:       packet.Receiver,
			Calldata: []byte{0x04, 0x05},
			Value:    big.NewInt(0),
			SignerID: signer.Address().Hex(),
		},
	); err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	// A third party delivered the packet while our lzReceive tx was in flight,
	// so the job is already DELIVERED when our own tx mines reverted.
	if err := store.MarkExecutorDelivered(t.Context(), packet.GUID, common.HexToHash("0xabc")); err != nil {
		t.Fatalf("MarkExecutorDelivered() error = %v", err)
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusFailed)

	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	delivered, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if delivered.Status != string(packets.ExecutorDelivered) {
		t.Fatalf("packet status = %q, want %q", delivered.Status, packets.ExecutorDelivered)
	}
	resolvedTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() after receipt error = %v", err)
	}
	if resolvedTx.Status != db.TxStatusFailed {
		t.Fatalf("tx status = %q, want %q (row must not stay a zombie broadcast)", resolvedTx.Status, db.TxStatusFailed)
	}
}

func TestProcessFailedRetryClonesLzReceiveReceiptFailureAndRestoresWorkflow(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       56,
		receipts:           make(map[common.Hash]*types.Receipt),
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())
	packet := testExecutorPacket(t)
	packet.Status = string(packets.ExecutorExecutable)
	if err := store.UpsertPacket(t.Context(), packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(t.Context(), db.ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if _, err := store.EnqueueExecutorTx(
		t.Context(),
		packet.GUID,
		string(packets.ExecutorExecutable),
		string(packets.ExecutorLzReceiveTxEnqueued),
		db.TxRequest{
			ChainEID: packet.DstEID,
			Purpose:  executorLzReceivePurpose,
			GUID:     packet.GUID.Bytes(),
			To:       packet.Receiver,
			Calldata: []byte{0x04, 0x05},
			Value:    big.NewInt(0),
			SignerID: signer.Address().Hex(),
		},
	); err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusFailed)
	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	forceRetryDue(t, id)

	retryID, err := manager.ProcessFailedRetry(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessFailedRetry() error = %v", err)
	}
	if retryID == id {
		t.Fatalf("retry id = %d, want cloned row", retryID)
	}
	retryTx, err := store.GetOutboxTx(t.Context(), retryID)
	if err != nil {
		t.Fatalf("GetOutboxTx(retry) error = %v", err)
	}
	if retryTx.RetryOfID == nil || *retryTx.RetryOfID != id {
		t.Fatalf("retry_of_id = %v, want %d", retryTx.RetryOfID, id)
	}
	restored, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket(restored) error = %v", err)
	}
	if restored.Status != string(packets.ExecutorLzReceiveTxEnqueued) {
		t.Fatalf("packet status = %q, want %q", restored.Status, packets.ExecutorLzReceiveTxEnqueued)
	}

	retryProcessedID, err := manager.ProcessNext(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext(retry) error = %v", err)
	}
	if retryProcessedID != retryID {
		t.Fatalf("processed retry id = %d, want %d", retryProcessedID, retryID)
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want only bootstrap call", client.pendingNonceCalls)
	}
	retryTx, err = store.GetOutboxTx(t.Context(), retryID)
	if err != nil {
		t.Fatalf("GetOutboxTx(retry broadcast) error = %v", err)
	}
	if retryTx.Nonce != 57 {
		t.Fatalf("fresh retry nonce = %d, want next local nonce 57", retryTx.Nonce)
	}
	client.receipts[retryTx.TxHash] = testReceipt(retryTx.TxHash, types.ReceiptStatusSuccessful)
	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts(retry) error = %v", err)
	}
	delivered, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket(delivered) error = %v", err)
	}
	if delivered.Status != string(packets.ExecutorDelivered) {
		t.Fatalf("packet status = %q, want %q", delivered.Status, packets.ExecutorDelivered)
	}
}

func TestProcessFailedRetryIgnoresStaleLzReceiveFailureAfterWorkflowAdvanced(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       58,
		receipts:           make(map[common.Hash]*types.Receipt),
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())
	packet := testExecutorPacket(t)
	packet.Status = string(packets.ExecutorExecutable)
	if err := store.UpsertPacket(t.Context(), packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(t.Context(), db.ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if _, err := store.EnqueueExecutorTx(
		t.Context(),
		packet.GUID,
		string(packets.ExecutorExecutable),
		string(packets.ExecutorLzReceiveTxEnqueued),
		db.TxRequest{
			ChainEID: packet.DstEID,
			Purpose:  executorLzReceivePurpose,
			GUID:     packet.GUID.Bytes(),
			To:       packet.Receiver,
			Calldata: []byte{0x04, 0x05},
			Value:    big.NewInt(0),
			SignerID: signer.Address().Hex(),
		},
	); err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusFailed)
	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	if _, err := store.EnqueueExecutorTx(
		t.Context(),
		packet.GUID,
		string(packets.ExecutorLzReceiveFailed),
		string(packets.ExecutorLzReceiveTxEnqueued),
		db.TxRequest{
			ChainEID: packet.DstEID,
			Purpose:  executorLzReceivePurpose,
			GUID:     packet.GUID.Bytes(),
			To:       packet.Receiver,
			Calldata: []byte{0x06, 0x07},
			Value:    big.NewInt(0),
			SignerID: signer.Address().Hex(),
		},
	); err != nil {
		t.Fatalf("EnqueueExecutorTx(reenqueue) error = %v", err)
	}
	forceRetryDue(t, id)

	_, err = manager.ProcessFailedRetry(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if !errors.Is(err, db.ErrNoFailedTxRetry) {
		t.Fatalf("ProcessFailedRetry() error = %v, want ErrNoFailedTxRetry", err)
	}
	failedTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(failed) error = %v", err)
	}
	if failedTx.NextRetryAt != nil {
		t.Fatalf("next retry at = %v, want nil", failedTx.NextRetryAt)
	}
	if failedTx.FailureKind != "" {
		t.Fatalf("failure kind = %q, want cleared", failedTx.FailureKind)
	}
	advanced, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket(advanced) error = %v", err)
	}
	if advanced.Status != string(packets.ExecutorLzReceiveTxEnqueued) {
		t.Fatalf("packet status = %q, want %q", advanced.Status, packets.ExecutorLzReceiveTxEnqueued)
	}
}

func TestProcessReceiptsMarksDVNVerifyTxVerified(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 66,
		receipts:     make(map[common.Hash]*types.Receipt),
	}
	manager := New(store, discardLogger())
	packet := testExecutorPacket(t)
	if err := store.UpsertPacket(t.Context(), packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertDVNJob(t.Context(), db.DVNJobRecord{
		GUID:                  packet.GUID,
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNQuorumChecking),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	if _, err := store.EnqueueDVNVerifyTx(
		t.Context(),
		packet.GUID,
		string(packets.DVNQuorumChecking),
		string(packets.DVNVerifyTxEnqueued),
		db.TxRequest{
			ChainEID: packet.DstEID,
			Purpose:  dvnVerifyPurpose,
			GUID:     packet.GUID.Bytes(),
			To:       packet.Receiver,
			Calldata: []byte{0x06, 0x07},
			Value:    big.NewInt(0),
			SignerID: signer.Address().Hex(),
		},
		[]byte(`{"status":"ready"}`),
	); err != nil {
		t.Fatalf("EnqueueDVNVerifyTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusSuccessful)

	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	job, err := store.GetDVNJob(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetDVNJob() error = %v", err)
	}
	if job.Status != string(packets.DVNVerified) {
		t.Fatalf("dvn job status = %q, want %q", job.Status, packets.DVNVerified)
	}
}

func TestProcessReceiptsFailedDVNVerifyOnlyFailsOutbox(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 67,
		receipts:     make(map[common.Hash]*types.Receipt),
	}
	manager := New(store, discardLogger())
	packet := testExecutorPacket(t)
	if err := store.UpsertPacket(t.Context(), packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertDVNJob(t.Context(), db.DVNJobRecord{
		GUID:                  packet.GUID,
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNReadyToVerify),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	if _, err := store.EnqueueDVNVerifyTx(t.Context(), packet.GUID, string(packets.DVNReadyToVerify), string(packets.DVNVerifyTxEnqueued), db.TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  dvnVerifyPurpose,
		GUID:     packet.GUID.Bytes(),
		To:       packet.Receiver,
		Calldata: []byte{0x06, 0x07},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}, []byte(`{"status":"ready"}`)); err != nil {
		t.Fatalf("EnqueueDVNVerifyTx() error = %v", err)
	}

	id, err := manager.ProcessNext(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusFailed)

	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	failedTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() after receipt error = %v", err)
	}
	if failedTx.Status != db.TxStatusFailed {
		t.Fatalf("tx status = %q, want %q", failedTx.Status, db.TxStatusFailed)
	}
	job, err := store.GetDVNJob(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetDVNJob() error = %v", err)
	}
	if job.Status != string(packets.DVNVerifyTxEnqueued) {
		t.Fatalf("dvn status = %q, want %q", job.Status, packets.DVNVerifyTxEnqueued)
	}
}

func TestSyntheticActiveFlowVerifiesCommitsAndDelivers(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 77,
		receipts:     make(map[common.Hash]*types.Receipt),
	}
	manager := New(store, discardLogger())
	packet := testExecutorPacket(t)
	packet.Status = string(packets.ExecutorAssigned)
	if err := store.UpsertPacket(t.Context(), packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(t.Context(), db.ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorAssigned),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if err := store.UpsertDVNJob(t.Context(), db.DVNJobRecord{
		GUID:                  packet.GUID,
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNReadyToVerify),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}

	if _, err := store.EnqueueDVNVerifyTx(t.Context(), packet.GUID, string(packets.DVNReadyToVerify), string(packets.DVNVerifyTxEnqueued), db.TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  dvnVerifyPurpose,
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Calldata: []byte{0x06, 0x07},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}, []byte(`{"status":"ready"}`)); err != nil {
		t.Fatalf("EnqueueDVNVerifyTx() error = %v", err)
	}
	processQueuedSuccess(t, manager, store, client, signer, packet.DstEID, big.NewInt(560048))
	job, err := store.GetDVNJob(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetDVNJob() error = %v", err)
	}
	if job.Status != string(packets.DVNVerified) {
		t.Fatalf("dvn status = %q, want %q", job.Status, packets.DVNVerified)
	}

	if err := store.MarkExecutorWaitingDVNVerification(t.Context(), packet.GUID, string(packets.ExecutorAssigned)); err != nil {
		t.Fatalf("MarkExecutorWaitingDVNVerification() error = %v", err)
	}
	if err := store.MarkExecutorVerifiable(t.Context(), packet.GUID, string(packets.ExecutorWaitingDVNVerification)); err != nil {
		t.Fatalf("MarkExecutorVerifiable() error = %v", err)
	}
	if _, err := store.EnqueueExecutorTx(t.Context(), packet.GUID, string(packets.ExecutorVerifiable), string(packets.ExecutorCommitTxEnqueued), db.TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  executorCommitVerificationPurpose,
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Calldata: []byte{0x08, 0x09},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueExecutorTx(commit) error = %v", err)
	}
	processQueuedSuccess(t, manager, store, client, signer, packet.DstEID, big.NewInt(560048))
	committed, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() after commit error = %v", err)
	}
	if committed.Status != string(packets.ExecutorCommitted) {
		t.Fatalf("packet status = %q, want %q", committed.Status, packets.ExecutorCommitted)
	}

	if err := store.MarkExecutorExecutable(t.Context(), packet.GUID); err != nil {
		t.Fatalf("MarkExecutorExecutable() error = %v", err)
	}
	if _, err := store.EnqueueExecutorTx(t.Context(), packet.GUID, string(packets.ExecutorExecutable), string(packets.ExecutorLzReceiveTxEnqueued), db.TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  executorLzReceivePurpose,
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0x4444444444444444444444444444444444444444"),
		Calldata: []byte{0x0a, 0x0b},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueExecutorTx(lzReceive) error = %v", err)
	}
	processQueuedSuccess(t, manager, store, client, signer, packet.DstEID, big.NewInt(560048))
	delivered, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() after delivery error = %v", err)
	}
	if delivered.Status != string(packets.ExecutorDelivered) {
		t.Fatalf("packet status = %q, want %q", delivered.Status, packets.ExecutorDelivered)
	}
}

func processQueuedSuccess(t *testing.T, manager *Manager, store *db.Store, client *fakeChainClient, signer *keystore.Signer, chainEID uint32, chainID *big.Int) {
	t.Helper()
	id, err := manager.ProcessNext(t.Context(), testTarget(chainEID, chainID, signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Nonce >= client.pendingNonce {
		client.pendingNonce = outboxTx.Nonce + 1
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusSuccessful)
	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: chainEID,
		ChainID:  chainID,
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
}

func testReceipt(txHash common.Hash, status uint64) *types.Receipt {
	return &types.Receipt{
		TxHash:            txHash,
		Status:            status,
		BlockNumber:       big.NewInt(1_234_567),
		GasUsed:           21_000,
		EffectiveGasPrice: big.NewInt(2_000_000_000),
	}
}

func assertEstimateGasCall(t *testing.T, client *fakeChainClient, from, to common.Address, value *big.Int, data []byte) {
	t.Helper()
	if len(client.estimateGasCalls) != 1 {
		t.Fatalf("EstimateGas() calls = %d, want 1", len(client.estimateGasCalls))
	}
	call := client.estimateGasCalls[0]
	if call.From != from {
		t.Fatalf("EstimateGas() from = %s, want %s", call.From, from)
	}
	if call.To == nil || *call.To != to {
		t.Fatalf("EstimateGas() to = %v, want %s", call.To, to)
	}
	if call.Value.Cmp(value) != 0 {
		t.Fatalf("EstimateGas() value = %s, want %s", call.Value, value)
	}
	if !bytes.Equal(call.Data, data) {
		t.Fatalf("EstimateGas() data = %x, want %x", call.Data, data)
	}
	if call.Gas != 0 {
		t.Fatalf("EstimateGas() gas = %d, want 0", call.Gas)
	}
	if call.GasPrice != nil || call.GasFeeCap != nil || call.GasTipCap != nil {
		t.Fatalf("EstimateGas() fee fields = %v/%v/%v, want nil", call.GasPrice, call.GasFeeCap, call.GasTipCap)
	}
}

func openTestStore(t *testing.T) *db.Store {
	t.Helper()
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	store, err := db.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	registry, err := chain.NewRegistry(testChains(), testPathways())
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	if err := store.SyncConfig(ctx, registry); err != nil {
		t.Fatalf("SyncConfig() error = %v", err)
	}
	return store
}

func forceRetryDue(t *testing.T, id int64) {
	t.Helper()
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	pool, err := pgxpool.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	tag, err := pool.Exec(t.Context(), `
		UPDATE tx_outbox
		SET next_retry_at = now() - interval '1 second'
		WHERE id = $1
	`, id)
	if err != nil {
		t.Fatalf("force retry due: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("force retry due rows = %d, want 1", tag.RowsAffected())
	}
}

func forceBroadcastStale(t *testing.T, id int64) {
	t.Helper()
	forceBroadcastAgeSeconds(t, id, 16*60)
}

func forceBroadcastAgeSeconds(t *testing.T, id int64, seconds int) {
	t.Helper()
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	pool, err := pgxpool.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	tag, err := pool.Exec(t.Context(), `
		UPDATE tx_outbox
		SET updated_at = now() - make_interval(secs => $2::int)
		WHERE id = $1
	`, id, seconds)
	if err != nil {
		t.Fatalf("force broadcast age: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("force broadcast age rows = %d, want 1", tag.RowsAffected())
	}
}

func forceOutboxAttempts(t *testing.T, id int64, attempts uint32) {
	t.Helper()
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	pool, err := pgxpool.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	tag, err := pool.Exec(t.Context(), `
		UPDATE tx_outbox
		SET attempts = $1
		WHERE id = $2
	`, attempts, id)
	if err != nil {
		t.Fatalf("force outbox attempts: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("force outbox attempts rows = %d, want 1", tag.RowsAffected())
	}
}

func forceOutboxStatus(t *testing.T, id int64, status string) {
	t.Helper()
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	pool, err := pgxpool.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	tag, err := pool.Exec(t.Context(), `
		UPDATE tx_outbox
		SET status = $1
		WHERE id = $2
	`, status, id)
	if err != nil {
		t.Fatalf("force outbox status: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("force outbox status rows = %d, want 1", tag.RowsAffected())
	}
}

func newTestKeystoreSigner(t *testing.T) *keystore.Signer {
	t.Helper()
	dir := t.TempDir()
	const password = "test-password"
	account, err := gethkeystore.StoreKey(dir, password, gethkeystore.StandardScryptN, gethkeystore.StandardScryptP)
	if err != nil {
		t.Fatalf("StoreKey() error = %v", err)
	}
	signer, err := keystore.LoadWithPasswordSource(filepath.Clean(account.URL.Path), keystore.PasswordSource{Value: password})
	if err != nil {
		t.Fatalf("LoadWithPasswordSource() error = %v", err)
	}
	return signer
}

func testExecutorPacket(t *testing.T) db.PacketRecord {
	t.Helper()
	message := []byte{0x03, 0x04}
	guid := crypto.Keccak256Hash([]byte(t.Name()))
	nonce := new(big.Int).SetBytes(guid[:8])
	return db.PacketRecord{
		GUID:           guid,
		SrcEID:         40161,
		DstEID:         40449,
		Nonce:          nonce,
		Sender:         common.HexToAddress("0x7777777777777777777777777777777777777777"),
		Receiver:       common.HexToAddress("0x8888888888888888888888888888888888888888"),
		SendLib:        common.HexToAddress("0x9999999999999999999999999999999999999999"),
		SrcTxHash:      crypto.Keccak256Hash([]byte(t.Name() + ":source")),
		SrcBlockNumber: 123,
		SrcLogIndex:    4,
		EncodedPacket:  append([]byte{0x01, 0x02}, message...),
		PacketHeader:   []byte{0x01, 0x02},
		Message:        message,
		PayloadHash:    crypto.Keccak256Hash(message),
		Options:        []byte{0x07, 0x08},
		Status:         string(packets.ExecutorNew),
	}
}

type fakeChainClient struct {
	pendingNonce          uint64
	pendingNonceCalls     int
	estimatedGas          uint64
	estimateGasErr        error
	estimateGasCalls      []ethereum.CallMsg
	header                *types.Header
	headerErr             error
	suggestedGasPrice     *big.Int
	suggestGasPriceErr    error
	suggestGasPriceCalls  int
	suggestedGasTipCap    *big.Int
	suggestGasTipCapErr   error
	suggestGasTipCapCalls int
	sendErr               error
	sent                  []*types.Transaction
	receipts              map[common.Hash]*types.Receipt
	balance               *big.Int
	balanceErr            error
}

func (f *fakeChainClient) BalanceAt(context.Context, common.Address, *big.Int) (*big.Int, error) {
	if f.balanceErr != nil {
		return nil, f.balanceErr
	}
	if f.balance == nil {
		return big.NewInt(0), nil
	}
	return new(big.Int).Set(f.balance), nil
}

func (f *fakeChainClient) EstimateGas(_ context.Context, call ethereum.CallMsg) (uint64, error) {
	f.estimateGasCalls = append(f.estimateGasCalls, call)
	if f.estimateGasErr != nil {
		return 0, f.estimateGasErr
	}
	if f.estimatedGas == 0 {
		return 150_000, nil
	}
	return f.estimatedGas, nil
}

func (f *fakeChainClient) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	if f.headerErr != nil {
		return nil, f.headerErr
	}
	if f.header != nil {
		copied := *f.header
		return &copied, nil
	}
	return dynamicHeader(), nil
}

func (f *fakeChainClient) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	f.pendingNonceCalls++
	return f.pendingNonce, nil
}

func (f *fakeChainClient) SuggestGasPrice(context.Context) (*big.Int, error) {
	f.suggestGasPriceCalls++
	if f.suggestGasPriceErr != nil {
		return nil, f.suggestGasPriceErr
	}
	if f.suggestedGasPrice == nil {
		return nil, nil
	}
	return new(big.Int).Set(f.suggestedGasPrice), nil
}

func (f *fakeChainClient) SuggestGasTipCap(context.Context) (*big.Int, error) {
	f.suggestGasTipCapCalls++
	if f.suggestGasTipCapErr != nil {
		return nil, f.suggestGasTipCapErr
	}
	if f.suggestedGasTipCap == nil {
		return big.NewInt(1_000_000_000), nil
	}
	return new(big.Int).Set(f.suggestedGasTipCap), nil
}

func (f *fakeChainClient) SendTransaction(_ context.Context, tx *types.Transaction) error {
	f.sent = append(f.sent, tx)
	if f.sendErr != nil {
		return f.sendErr
	}
	return nil
}

func (f *fakeChainClient) TransactionReceipt(_ context.Context, txHash common.Hash) (*types.Receipt, error) {
	receipt, ok := f.receipts[txHash]
	if !ok {
		return nil, ethereum.NotFound
	}
	return receipt, nil
}

type failingSigner struct {
	address common.Address
}

func (s failingSigner) Address() common.Address {
	return s.address
}

func (s failingSigner) SignHash(context.Context, common.Hash) ([]byte, error) {
	return nil, errors.New("sign hash failed")
}

func (s failingSigner) SignTx(context.Context, *types.Transaction, *big.Int) (*types.Transaction, error) {
	return nil, errors.New("sign tx failed")
}

func (s failingSigner) Type() string {
	return "failing"
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

func testChains() []config.ChainConfig {
	return []config.ChainConfig{
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
	}
}

func testTarget(chainEID uint32, chainID *big.Int, signer signeriface.Signer, client *fakeChainClient, policy FeePolicy) Target {
	return Target{
		ChainEID: chainEID,
		ChainID:  chainID,
		Signer:   signer,
		Client:   client,
		FeePolicies: map[string]FeePolicy{
			"commit-verification":             policy,
			"lz-receive":                      policy,
			executorCommitVerificationPurpose: policy,
			executorLzReceivePurpose:          policy,
			dvnVerifyPurpose:                  policy,
		},
	}
}

func defaultFeePolicy() FeePolicy {
	return FeePolicy{
		ConfiguredMaxFeePerGas:         big.NewInt(10_000_000_000),
		ConfiguredMaxPriorityFeePerGas: big.NewInt(2_000_000_000),
	}
}

func dynamicHeader() *types.Header {
	return &types.Header{BaseFee: big.NewInt(500_000_000)}
}

func legacyHeader() *types.Header {
	return &types.Header{}
}

func testExecutorRole() config.ExecutorTxRoleConfig {
	return config.ExecutorTxRoleConfig{
		Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

type fakeRPCDataError struct {
	message string
	data    any
}

type fakeRPCError struct {
	message string
	code    int
}

func (e fakeRPCError) Error() string {
	return e.message
}

func (e fakeRPCError) ErrorCode() int {
	return e.code
}

type redactedProviderError struct {
	cause error
}

func (e redactedProviderError) Error() string {
	return "provider[0] eth_estimateGas failed"
}

func (e redactedProviderError) Unwrap() error {
	return e.cause
}

func (e fakeRPCDataError) Error() string {
	return e.message
}

func (e fakeRPCDataError) ErrorCode() int {
	return 3
}

func (e fakeRPCDataError) ErrorData() any {
	return e.data
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
