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
	"github.com/google/uuid"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	signeriface "github.com/islishude/oh-my-lazier/go/internal/signer"
	"github.com/islishude/oh-my-lazier/go/internal/signer/keystore"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProcessNextSkipsPausedChainUntilUnpause(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 20, estimatedGas: 100_000, header: dynamicHeader(), suggestedGasTipCap: big.NewInt(1_000_000_000)}
	logger, _ := captureLogger(slog.LevelInfo)
	manager := New(store, logger)

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	// A pause committed after enqueue holds the queued row back from signing:
	// no nonce is reserved and no RPC preflight runs for it.
	setChainScopeFlags(t, 40161, true, true)
	t.Cleanup(func() { setChainScopeFlags(t, 40161, true, false) })

	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	if _, err := manager.ProcessNext(t.Context(), target); !errors.Is(err, ErrNoQueuedTx) {
		t.Fatalf("ProcessNext(paused chain) error = %v, want ErrNoQueuedTx", err)
	}

	setChainScopeFlags(t, 40161, true, false)
	id, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext(unpaused) error = %v", err)
	}
	signedTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if signedTx.Status != db.TxStatusSigned {
		t.Fatalf("status = %q, want signed after unpause", signedTx.Status)
	}
}

func TestProcessNextSignsAndBroadcastsDynamicFeeTx(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 10, estimatedGas: 123_456, header: dynamicHeader(), suggestedGasTipCap: big.NewInt(1_000_000_000)}
	logger, logs := captureLogger(slog.LevelInfo)
	manager := New(store, logger)

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	id, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	// Signing is durable-first: nothing is sent until ProcessBroadcast.
	if len(client.sent) != 0 {
		t.Fatalf("sent tx count after ProcessNext = %d, want 0", len(client.sent))
	}
	signedTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(signed) error = %v", err)
	}
	if signedTx.Status != db.TxStatusSigned {
		t.Fatalf("outbox status after ProcessNext = %q, want %q", signedTx.Status, db.TxStatusSigned)
	}
	broadcastID, err := manager.ProcessBroadcast(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	if broadcastID != id {
		t.Fatalf("broadcast id = %d, want %d", broadcastID, id)
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
	if outboxTx.TxHash != sent.Hash() {
		t.Fatalf("mirror tx hash = %s, want the sent hash %s", outboxTx.TxHash, sent.Hash())
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
		`msg="claimed tx outbox row for signing"`,
		`nonce=10`,
		`msg="signed tx attempt"`,
		`gas_limit=123456`,
		`dynamic_fee=true`,
		`msg="broadcast tx attempt"`,
		`purpose=pricing_set_price_snapshot`,
	)
}

func TestProcessNextSignsLegacyTxWithSuggestedGasPrice(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 12, estimatedGas: 98_765, header: legacyHeader(), suggestedGasPrice: big.NewInt(7_000_000_000)}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	id, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if id == 0 {
		t.Fatal("ProcessNext() id = 0")
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	// A crash after nonce assignment but before the attempt insert leaves a bare
	// nonce_assigned row (no lease, no attempt); ProcessNext must recover it.
	forceNonceAssigned(t, id, 71)

	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	processedID, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if processedID != id {
		t.Fatalf("processed id = %d, want %d", processedID, id)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
				Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
		`reason=preflight_error`,
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	// A pricing row is terminal on the deterministic revert: no retry window,
	// because its calldata carries a time-bound observation the retry paths
	// refuse; the bot rebuilds from a fresh observation instead.
	if outboxTx.FailureKind != db.TxFailureEstimateGasRevert || outboxTx.NextRetryAt != nil {
		t.Fatalf("failure metadata = %q/%v, want terminal estimate gas revert without a retry window", outboxTx.FailureKind, outboxTx.NextRetryAt)
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
				Purpose:  db.TxPurposePricingSetPriceSnapshot,
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

func TestProcessBroadcastAmbiguousSendKeepsAttemptTracked(t *testing.T) {
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
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())

	firstID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(first) error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext(first) error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(first) error = %v", err)
	}
	// An unrecognized send error may still have been accepted by the node: the
	// row stays broadcast for receipt polling and the signed raw is retained.
	ambiguousTx, err := store.GetOutboxTx(t.Context(), firstID)
	if err != nil {
		t.Fatalf("GetOutboxTx(first) error = %v", err)
	}
	if ambiguousTx.Status != db.TxStatusBroadcast {
		t.Fatalf("status after ambiguous send = %q, want %q", ambiguousTx.Status, db.TxStatusBroadcast)
	}
	if ambiguousTx.Nonce != 31 {
		t.Fatalf("nonce = %d, want 31", ambiguousTx.Nonce)
	}
	if ambiguousTx.TxHash == (common.Hash{}) {
		t.Fatal("tx hash = zero, want signed hash retained")
	}
	if ambiguousTx.GasLimit == 0 || ambiguousTx.MaxFeePerGas == nil {
		t.Fatalf("gas/fees = %d/%v, want retained for the persisted attempt", ambiguousTx.GasLimit, ambiguousTx.MaxFeePerGas)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count = %d, want 1", len(client.sent))
	}
	assertLogContains(t, logs.String(),
		`msg="tx broadcast not accepted"`,
		`send_class=ambiguous`,
		`send_detail="unrecognized broadcast error"`,
	)

	// The bounded replay resends the same persisted raw, never a new signature.
	client.sendErr = nil
	forceAttemptBroadcastDue(t, firstID)
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(replay) error = %v", err)
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	if client.sent[1].Hash() != client.sent[0].Hash() {
		t.Fatalf("replayed hash = %s, want the original %s", client.sent[1].Hash(), client.sent[0].Hash())
	}

	// The ambiguous lane reached broadcast, so the next queued tx takes the next
	// cursor nonce without any RPC nonce read.
	secondID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05, 0x06},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(second) error = %v", err)
	}
	processedID, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext(second) error = %v", err)
	}
	if processedID != secondID {
		t.Fatalf("processed id = %d, want %d", processedID, secondID)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(second) error = %v", err)
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want only the bootstrap call", client.pendingNonceCalls)
	}
	if len(client.sent) != 3 {
		t.Fatalf("sent tx count = %d, want 3", len(client.sent))
	}
	if client.sent[2].Nonce() != 32 {
		t.Fatalf("second tx nonce = %d, want 32", client.sent[2].Nonce())
	}
}

func TestProcessBroadcastDefinitiveErrorHoldsLane(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       51,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		sendErr:            errors.New("invalid sender"),
	}
	manager := New(store, discardLogger())
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())

	heldID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(held) error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	heldTx, err := store.GetOutboxTx(t.Context(), heldID)
	if err != nil {
		t.Fatalf("GetOutboxTx(held) error = %v", err)
	}
	if heldTx.Status != db.TxStatusHeld {
		t.Fatalf("status after definitive rejection = %q, want %q", heldTx.Status, db.TxStatusHeld)
	}
	if heldTx.Nonce != 51 {
		t.Fatalf("held nonce = %d, want 51 (nonce must stay owned)", heldTx.Nonce)
	}

	// The held nonce blocks every higher nonce until it is reconciled.
	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05, 0x06},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx(blocked) error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); !errors.Is(err, db.ErrSignerLaneBlocked) {
		t.Fatalf("ProcessNext(blocked) error = %v, want ErrSignerLaneBlocked", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); !errors.Is(err, db.ErrNoBroadcastCandidate) {
		t.Fatalf("ProcessBroadcast(blocked) error = %v, want ErrNoBroadcastCandidate", err)
	}
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count = %d, want the single rejected send", len(client.sent))
	}
}

func TestProcessOnceReplaysDueBroadcastBeforeQueuedTx(t *testing.T) {
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

	ambiguousID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(ambiguous) error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	forceAttemptBroadcastDue(t, ambiguousID)
	queuedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x3333333333333333333333333333333333333333"),
		Calldata: []byte{0x04, 0x05, 0x06},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(queued) error = %v", err)
	}

	// The due replay of the persisted raw must win the pass over signing the
	// later queued row.
	client.sendErr = nil
	processed, err := manager.processOnce(t.Context())
	if err != nil {
		t.Fatalf("processOnce(replay) error = %v", err)
	}
	if !processed {
		t.Fatal("processOnce(replay) processed = false, want true")
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	if client.sent[1].Hash() != client.sent[0].Hash() {
		t.Fatalf("replayed hash = %s, want the persisted raw %s", client.sent[1].Hash(), client.sent[0].Hash())
	}
	stillQueued, err := store.GetOutboxTx(t.Context(), queuedID)
	if err != nil {
		t.Fatalf("GetOutboxTx(queued) error = %v", err)
	}
	if stillQueued.Status != db.TxStatusQueued {
		t.Fatalf("later queued status = %q, want queued", stillQueued.Status)
	}
	replayed, err := store.GetOutboxTx(t.Context(), ambiguousID)
	if err != nil {
		t.Fatalf("GetOutboxTx(replayed) error = %v", err)
	}
	if replayed.Status != db.TxStatusBroadcast {
		t.Fatalf("replayed status = %q, want broadcast", replayed.Status)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(stale) error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), staleID)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	forceBroadcastStale(t, staleID)
	queuedID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x3333333333333333333333333333333333333333"),
		Calldata: []byte{0x04, 0x05, 0x06},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(queued) error = %v", err)
	}

	// Pass 1 signs the replacement attempt (durable-first, nothing sent yet).
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
	if len(client.sent) != 1 {
		t.Fatalf("sent tx count after signing pass = %d, want 1 (original only)", len(client.sent))
	}

	// Pass 2 broadcasts the persisted replacement before touching the queued row.
	processed, err = manager.processOnce(t.Context())
	if err != nil {
		t.Fatalf("processOnce(broadcast replacement) error = %v", err)
	}
	if !processed {
		t.Fatal("processOnce(broadcast replacement) processed = false, want true")
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
	if sentReplacement.Hash() != replacement.TxHash {
		t.Fatalf("sent replacement hash = %s, want persisted %s", sentReplacement.Hash(), replacement.TxHash)
	}
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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

func TestStaleBroadcastReplacementSkipsUnsentSignedAttempt(t *testing.T) {
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	forceBroadcastStale(t, id)

	// A persisted-but-unsent signed attempt is replayed by ProcessBroadcast, not
	// replaced: replacing an unsent raw would waste a signature and a hash.
	if _, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target); !errors.Is(err, db.ErrNoStaleBroadcastReplacement) {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	if len(client.sent) != 1 || client.sent[0].Hash() != original.TxHash {
		t.Fatalf("sent = %d txs, want exactly the persisted raw %s", len(client.sent), original.TxHash)
	}
}

func TestStaleBroadcastReplacementStopsAtReplacementCap(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       34,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())
	// A very high cap keeps the escalating replacement fee bumps under the policy
	// so every replacement up to the count cap actually signs.
	policy := FeePolicy{
		ConfiguredMaxFeePerGas:         big.NewInt(1_000_000_000_000),
		ConfiguredMaxPriorityFeePerGas: big.NewInt(1_000_000_000_000),
	}
	target := testTarget(40161, big.NewInt(11155111), signer, client, policy)

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	for i := 0; i < db.TxMaxReplacements; i++ {
		forceBroadcastStale(t, id)
		if _, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target); err != nil {
			t.Fatalf("ProcessStaleBroadcastReplacement(#%d) error = %v", i+1, err)
		}
		if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
			t.Fatalf("ProcessBroadcast(#%d) error = %v", i+1, err)
		}
	}

	forceBroadcastStale(t, id)
	_, err = manager.ProcessStaleBroadcastReplacement(t.Context(), target)
	if !errors.Is(err, db.ErrNoStaleBroadcastReplacement) {
		t.Fatalf("ProcessStaleBroadcastReplacement(at cap) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	capped, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if capped.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want broadcast (receipt polling continues at the cap)", capped.Status)
	}
	if len(client.sent) != 1+db.TxMaxReplacements {
		t.Fatalf("sent tx count = %d, want %d", len(client.sent), 1+db.TxMaxReplacements)
	}

	// An explicit operator request authorizes one replacement past the automatic cap.
	if err := store.RequestTxReplacement(t.Context(), id); err != nil {
		t.Fatalf("RequestTxReplacement() error = %v", err)
	}
	if _, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target); err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement(requested) error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(requested) error = %v", err)
	}
	if len(client.sent) != 2+db.TxMaxReplacements {
		t.Fatalf("sent tx count after manual override = %d, want %d", len(client.sent), 2+db.TxMaxReplacements)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02, 0x03},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	processedID, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if processedID != id {
		t.Fatalf("processed id = %d, want %d", processedID, id)
	}
	// A signing failure charges the pre-sign budget and keeps the nonce owned;
	// it never requeues the row (that would release the nonce and wedge the lane).
	chargedTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if chargedTx.Status != db.TxStatusNonceAssigned {
		t.Fatalf("status = %q, want %q", chargedTx.Status, db.TxStatusNonceAssigned)
	}
	if chargedTx.Nonce != 41 {
		t.Fatalf("nonce = %d, want 41", chargedTx.Nonce)
	}
	if chargedTx.TxHash != (common.Hash{}) {
		t.Fatalf("tx hash = %s, want zero hash", chargedTx.TxHash)
	}
	if chargedTx.FailureKind != "" {
		t.Fatalf("failure kind = %q, want none (pre-sign budget, not the failed path)", chargedTx.FailureKind)
	}
	count, nextSignAt := queryPreSignBudget(t, id)
	if count != 1 || nextSignAt == nil {
		t.Fatalf("pre-sign budget = %d/%v, want 1 charge with a backoff", count, nextSignAt)
	}
	if client.pendingNonceCalls != 1 {
		t.Fatalf("PendingNonceAt() calls = %d, want 1", client.pendingNonceCalls)
	}
	if len(client.sent) != 0 {
		t.Fatalf("sent tx count = %d, want 0", len(client.sent))
	}
	assertLogContains(t, logs.String(),
		`msg="failed tx pre-sign stage"`,
		`stage=sign`,
		`error="sign tx failed"`,
	)

	// The held nonce blocks fresh queued work while the backoff is pending.
	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(123),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx(blocked) error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); !errors.Is(err, db.ErrSignerLaneBlocked) {
		t.Fatalf("ProcessNext(blocked) error = %v, want ErrSignerLaneBlocked", err)
	}
}

func TestRequestTxReplacementPreservesNonceAndBumpsFees(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 21, estimatedGas: 111_111, header: dynamicHeader(), suggestedGasTipCap: big.NewInt(1_000_000_000)}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	id, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	if err := store.RequestTxReplacement(t.Context(), id); err != nil {
		t.Fatalf("RequestTxReplacement() error = %v", err)
	}
	replacedID, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v", err)
	}
	if replacedID != id {
		t.Fatalf("replacement id = %d, want %d", replacedID, id)
	}
	replacement, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if replacement.Nonce != 21 {
		t.Fatalf("replacement nonce = %d, want 21", replacement.Nonce)
	}
	if replacement.Status != db.TxStatusBroadcast {
		t.Fatalf("replacement status = %q, want %q", replacement.Status, db.TxStatusBroadcast)
	}
	if replacement.TxHash == original.TxHash {
		t.Fatal("replacement mirror hash still points at the original attempt")
	}
	if replacement.MaxFeePerGas.Cmp(big.NewInt(2_200_000_000)) != 0 {
		t.Fatalf("replacement max fee = %s, want 2200000000", replacement.MaxFeePerGas)
	}
	if replacement.MaxPriorityFeePerGas.Cmp(big.NewInt(1_100_000_000)) != 0 {
		t.Fatalf("replacement priority fee = %s, want 1100000000", replacement.MaxPriorityFeePerGas)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(replacement) error = %v", err)
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
	if replacementTx.Gas() != 111_111 {
		t.Fatalf("replacement tx gas = %d, want the mirrored signed gas limit", replacementTx.Gas())
	}
	if replacementTx.GasFeeCap().Cmp(big.NewInt(2_200_000_000)) != 0 {
		t.Fatalf("replacement tx max fee = %s", replacementTx.GasFeeCap())
	}
	if replacementTx.GasTipCap().Cmp(big.NewInt(1_100_000_000)) != 0 {
		t.Fatalf("replacement tx priority fee = %s", replacementTx.GasTipCap())
	}
	if len(client.estimateGasCalls) != 1 {
		t.Fatalf("EstimateGas() calls = %d, want 1 (replacement reuses the mirrored gas limit)", len(client.estimateGasCalls))
	}
}

func TestRequestTxReplacementPreservesNonceAndRefreshesGasPrice(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{pendingNonce: 22, estimatedGas: 111_111, header: legacyHeader(), suggestedGasPrice: big.NewInt(4_000_000_000)}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	id, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	if err := store.RequestTxReplacement(t.Context(), id); err != nil {
		t.Fatalf("RequestTxReplacement() error = %v", err)
	}

	client.suggestedGasPrice = big.NewInt(5_000_000_000)
	replacedID, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v", err)
	}
	if replacedID != id {
		t.Fatalf("replacement id = %d, want %d", replacedID, id)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(replacement) error = %v", err)
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
	if replacementTx.Gas() != 111_111 {
		t.Fatalf("replacement tx gas = %d, want the mirrored signed gas limit", replacementTx.Gas())
	}
	if replacementTx.GasPrice().Cmp(big.NewInt(5_000_000_000)) != 0 {
		t.Fatalf("replacement tx gas price = %s", replacementTx.GasPrice())
	}
	if len(client.estimateGasCalls) != 1 {
		t.Fatalf("EstimateGas() calls = %d, want 1 (replacement reuses the mirrored gas limit)", len(client.estimateGasCalls))
	}
}

func TestUnderpricedSendAutoRepricesAfterCooldown(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       23,
		estimatedGas:       111_111,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		sendErr:            errors.New("transaction underpriced"),
	}
	manager := New(store, discardLogger())
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	heldTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(held) error = %v", err)
	}
	if heldTx.Status != db.TxStatusHeld {
		t.Fatalf("status after underpriced = %q, want %q", heldTx.Status, db.TxStatusHeld)
	}

	// Inside the cooldown nothing is repriced; after it, the hold recovers with
	// no operator involvement.
	if _, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target); !errors.Is(err, db.ErrNoStaleBroadcastReplacement) {
		t.Fatalf("ProcessStaleBroadcastReplacement(cooldown) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	forceBroadcastAgeSeconds(t, id, 120)
	client.sendErr = nil
	if _, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target); err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement(auto reprice) error = %v", err)
	}
	repriced, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(repriced) error = %v", err)
	}
	if repriced.Status != db.TxStatusSigned {
		t.Fatalf("status after auto reprice = %q, want %q", repriced.Status, db.TxStatusSigned)
	}
	if repriced.MaxFeePerGas.Cmp(big.NewInt(2_200_000_000)) != 0 || repriced.MaxPriorityFeePerGas.Cmp(big.NewInt(1_100_000_000)) != 0 {
		t.Fatalf("repriced fees = %s/%s, want 10%% bump over the underpriced attempt", repriced.MaxFeePerGas, repriced.MaxPriorityFeePerGas)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(repriced) error = %v", err)
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want 2", len(client.sent))
	}
	if client.sent[1].Nonce() != client.sent[0].Nonce() {
		t.Fatalf("repriced nonce = %d, want the held nonce %d", client.sent[1].Nonce(), client.sent[0].Nonce())
	}
	if client.sent[1].GasFeeCap().Cmp(big.NewInt(2_200_000_000)) != 0 {
		t.Fatalf("repriced max fee = %s, want 2200000000", client.sent[1].GasFeeCap())
	}
	final, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(final) error = %v", err)
	}
	if final.Status != db.TxStatusBroadcast {
		t.Fatalf("final status = %q, want broadcast", final.Status)
	}
}

func TestCancelNonceEndToEnd(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       61,
		estimatedGas:       111_111,
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
	id, err := store.EnqueueExecutorTx(
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
	)
	if err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}
	target := testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}

	if err := store.RequestTxCancel(t.Context(), id); err != nil {
		t.Fatalf("RequestTxCancel() error = %v", err)
	}
	cancelID, err := manager.ProcessCancelRequest(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessCancelRequest() error = %v", err)
	}
	if cancelID != id {
		t.Fatalf("cancel id = %d, want %d", cancelID, id)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(cancel) error = %v", err)
	}
	if len(client.sent) != 2 {
		t.Fatalf("sent tx count = %d, want the task then the cancel", len(client.sent))
	}
	cancelTx := client.sent[1]
	if cancelTx.To() == nil || *cancelTx.To() != signer.Address() {
		t.Fatalf("cancel destination = %v, want the signer itself", cancelTx.To())
	}
	if cancelTx.Value().Sign() != 0 || len(cancelTx.Data()) != 0 {
		t.Fatalf("cancel tx value/data = %s/%d bytes, want zero-value dataless noop", cancelTx.Value(), len(cancelTx.Data()))
	}
	if cancelTx.Nonce() != original.Nonce {
		t.Fatalf("cancel nonce = %d, want the held nonce %d", cancelTx.Nonce(), original.Nonce)
	}
	if cancelTx.GasFeeCap().Cmp(big.NewInt(2_200_000_000)) != 0 {
		t.Fatalf("cancel max fee = %s, want a 10%% bump over the task attempt", cancelTx.GasFeeCap())
	}

	// The cancel mines: the row terminates as canceled, the job parks for review,
	// and the lane unlocks for the next nonce.
	client.receipts = map[common.Hash]*types.Receipt{cancelTx.Hash(): testReceipt(cancelTx.Hash(), types.ReceiptStatusSuccessful)}
	if _, err := manager.ProcessReceipts(t.Context(), Target{ChainEID: packet.DstEID, ChainID: big.NewInt(560048), Signer: signer, Client: client}, 1); err != nil {
		t.Fatalf("ProcessReceipts(cancel) error = %v", err)
	}
	canceled, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(canceled) error = %v", err)
	}
	if canceled.Status != db.TxStatusFailed || canceled.FailureKind != db.TxFailureCanceled {
		t.Fatalf("canceled row = %q/%q, want failed/canceled", canceled.Status, canceled.FailureKind)
	}
	parked, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if parked.Status != string(packets.ExecutorManualReview) {
		t.Fatalf("packet status = %q, want %q (operator abandoned the task)", parked.Status, packets.ExecutorManualReview)
	}
}

func TestFailedTaskReceiptUnderCancelIntentParksWorkflow(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       66,
		estimatedGas:       111_111,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
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
	id, err := store.EnqueueExecutorTx(
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
	)
	if err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}
	target := testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	if err := store.RequestTxCancel(t.Context(), id); err != nil {
		t.Fatalf("RequestTxCancel() error = %v", err)
	}
	// The original task mines with a failed status while cancel intent is set:
	// the full receipts pipeline must park the job for review, never write the
	// LZ_RECEIVE_FAILED auto-retry state the operator just abandoned.
	taskHash := client.sent[0].Hash()
	client.receipts[taskHash] = testReceipt(taskHash, types.ReceiptStatusFailed)
	if _, err := manager.ProcessReceipts(t.Context(), Target{ChainEID: packet.DstEID, ChainID: big.NewInt(560048), Signer: signer, Client: client}, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	parked, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if parked.Status != string(packets.ExecutorManualReview) {
		t.Fatalf("packet status = %q, want %q (cancel intent must reach the workflow)", parked.Status, packets.ExecutorManualReview)
	}
	canceled, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if canceled.Status != db.TxStatusFailed || canceled.FailureKind != db.TxFailureCanceled || canceled.NextRetryAt != nil {
		t.Fatalf("row = %q/%q/%v, want failed/canceled without retry", canceled.Status, canceled.FailureKind, canceled.NextRetryAt)
	}
}

func TestNonceReconciliationPartialRPCFailureChangesNothing(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       91,
		estimatedGas:       111_111,
		header:             &types.Header{Number: big.NewInt(6_000_000), BaseFee: big.NewInt(500_000_000)},
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		sendErr:            errors.New("nonce too low"),
		receipts:           make(map[common.Hash]*types.Receipt),
		receiptErrs:        make(map[common.Hash]error),
	}
	manager := New(store, discardLogger())
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())

	// Two ambiguous broadcasts on the same lane (still replayable), then both
	// replays hit stale-view nodes and hold for reconciliation.
	client.sendErr = errors.New("connection glitch before acknowledgement")
	var ids []int64
	for i := 0; i < 2; i++ {
		id, err := store.EnqueueTx(t.Context(), db.TxRequest{
			ChainEID: 40161,
			Purpose:  db.TxPurposePricingSetPriceSnapshot,
			To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Calldata: []byte{byte(i + 1)},
			Value:    big.NewInt(0),
			SignerID: signer.Address().Hex(),
		})
		if err != nil {
			t.Fatalf("EnqueueTx(#%d) error = %v", i, err)
		}
		ids = append(ids, id)
		if _, err := manager.ProcessNext(t.Context(), target); err != nil {
			t.Fatalf("ProcessNext(#%d) error = %v", i, err)
		}
		if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
			t.Fatalf("ProcessBroadcast(#%d) error = %v", i, err)
		}
	}
	// Two instances claim the two due replays concurrently (both rows are still
	// broadcast, so neither blocks the other), then both hit stale-view nodes.
	forceAttemptBroadcastDue(t, ids[0])
	forceAttemptBroadcastDue(t, ids[1])
	token1, token2 := uuid.New(), uuid.New()
	claim1, err := store.ClaimAttemptForBroadcast(t.Context(), 40161, signer.Address().Hex(), token1, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast(1) error = %v", err)
	}
	claim2, err := store.ClaimAttemptForBroadcast(t.Context(), 40161, signer.Address().Hex(), token2, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast(2) error = %v", err)
	}
	if err := store.MarkAttemptSendResult(t.Context(), claim1.AttemptID, token1, db.SendErrorNonceTooLow, "nonce too low"); err != nil {
		t.Fatalf("MarkAttemptSendResult(1) error = %v", err)
	}
	if err := store.MarkAttemptSendResult(t.Context(), claim2.AttemptID, token2, db.SendErrorNonceTooLow, "nonce too low"); err != nil {
		t.Fatalf("MarkAttemptSendResult(2) error = %v", err)
	}
	for _, id := range ids {
		held, err := store.GetOutboxTx(t.Context(), id)
		if err != nil {
			t.Fatalf("GetOutboxTx(%d) error = %v", id, err)
		}
		if held.Status != db.TxStatusHeld {
			t.Fatalf("row %d status = %q, want held", id, held.Status)
		}
	}

	reasonPool, err := pgxpool.New(t.Context(), os.Getenv("TEST_POSTGRES_URL"))
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(reasonPool.Close)
	heldReasonOf := func(id int64) string {
		t.Helper()
		row, err := store.GetOutboxTx(t.Context(), id)
		if err != nil {
			t.Fatalf("GetOutboxTx(%d) error = %v", id, err)
		}
		if row.Status != db.TxStatusHeld {
			t.Fatalf("row %d status = %q, want held", id, row.Status)
		}
		var reason string
		if err := reasonPool.QueryRow(t.Context(), `SELECT held_reason FROM tx_outbox WHERE id = $1`, id).Scan(&reason); err != nil {
			t.Fatalf("select held_reason(%d): %v", id, err)
		}
		return reason
	}

	// The second hold's receipt lookup fails mid-pass: that hold alone is
	// skipped and stays put, while the first hold's fully resolved reads still
	// publish — one failing hash must not starve the whole lane's progress.
	client.confirmedNonce = 200
	client.receiptErrs[client.sent[1].Hash()] = errors.New("rpc receipt outage")
	if _, err := manager.ProcessNonceReconciliation(t.Context(), target); err != nil {
		t.Fatalf("ProcessNonceReconciliation(partial outage) error = %v", err)
	}
	if reason := heldReasonOf(ids[0]); reason != db.HeldNonceConsumedExternally {
		t.Fatalf("resolved hold reason = %q, want nonce_consumed_externally", reason)
	}
	if reason := heldReasonOf(ids[1]); reason != db.HeldNonceReconcileRequired {
		t.Fatalf("skipped hold reason = %q, want untouched nonce_reconcile_required", reason)
	}

	// After the outage clears (and the backoff), the skipped hold catches up.
	forceReconcileDue(t, signer.Address().Hex())
	delete(client.receiptErrs, client.sent[1].Hash())
	if _, err := manager.ProcessNonceReconciliation(t.Context(), target); err != nil {
		t.Fatalf("ProcessNonceReconciliation(retry) error = %v", err)
	}
	if reason := heldReasonOf(ids[1]); reason != db.HeldNonceConsumedExternally {
		t.Fatalf("caught-up hold reason = %q, want nonce_consumed_externally", reason)
	}
}

func TestNonceReconciliationReleasesAndParksExternally(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       71,
		estimatedGas:       111_111,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		sendErr:            errors.New("nonce too low"),
		receipts:           make(map[common.Hash]*types.Receipt),
	}
	manager := New(store, discardLogger())
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	held, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(held) error = %v", err)
	}
	if held.Status != db.TxStatusHeld {
		t.Fatalf("status after nonce too low = %q, want held", held.Status)
	}

	// Transient case: the confirmed nonce has not passed the held nonce, so the
	// hold releases back to broadcast and the raw keeps replaying. The confirmed
	// nonce must be read at head - confirmations, the indexer's confirmed window.
	client.header = &types.Header{Number: big.NewInt(5_000_000), BaseFee: big.NewInt(500_000_000)}
	client.confirmedNonce = held.Nonce
	confirmedTarget := target
	confirmedTarget.Confirmations = 12
	if _, err := manager.ProcessNonceReconciliation(t.Context(), confirmedTarget); err != nil {
		t.Fatalf("ProcessNonceReconciliation(release) error = %v", err)
	}
	if client.lastNonceAtBlock == nil || client.lastNonceAtBlock.Int64() != 5_000_000-12 {
		t.Fatalf("NonceAt block = %v, want %d (head - confirmations)", client.lastNonceAtBlock, 5_000_000-12)
	}
	released, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(released) error = %v", err)
	}
	if released.Status != db.TxStatusBroadcast {
		t.Fatalf("released status = %q, want broadcast", released.Status)
	}

	// External consumption: the confirmed nonce moved past the held nonce with no
	// receipt on any attempt. The row parks for the operator and the cursor
	// fast-forwards so the next fresh nonce is beyond the consumed range.
	forceOutboxHeldNonceReconcile(t, id)
	forceReconcileDue(t, signer.Address().Hex())
	client.confirmedNonce = held.Nonce + 5
	if _, err := manager.ProcessNonceReconciliation(t.Context(), target); err != nil {
		t.Fatalf("ProcessNonceReconciliation(external) error = %v", err)
	}
	consumed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(consumed) error = %v", err)
	}
	if consumed.Status != db.TxStatusHeld {
		t.Fatalf("consumed status = %q, want held for operator resolution", consumed.Status)
	}
	// A pricing row refuses the retry resolution (its calldata is a time-bound
	// observation); the operator abandons it and the bot rebuilds. Fresh work
	// still signs at the fast-forwarded cursor, past the consumed range.
	if _, err := store.ResolveExternalNonceRetry(t.Context(), id); err == nil || !strings.Contains(err.Error(), "pricing observation") {
		t.Fatalf("ResolveExternalNonceRetry(pricing) error = %v, want pricing refusal", err)
	}
	if err := store.ResolveExternalNonceAbandon(t.Context(), id); err != nil {
		t.Fatalf("ResolveExternalNonceAbandon() error = %v", err)
	}
	freshID, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x0a},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx(fresh) error = %v", err)
	}
	client.sendErr = nil
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext(fresh) error = %v", err)
	}
	fresh, err := store.GetOutboxTx(t.Context(), freshID)
	if err != nil {
		t.Fatalf("GetOutboxTx(fresh) error = %v", err)
	}
	if fresh.Nonce != held.Nonce+5 {
		t.Fatalf("fresh nonce = %d, want the fast-forwarded confirmed nonce %d", fresh.Nonce, held.Nonce+5)
	}
}

func TestCancelBumpStaysCancelKind(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       81,
		estimatedGas:       111_111,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	if err := store.RequestTxCancel(t.Context(), id); err != nil {
		t.Fatalf("RequestTxCancel() error = %v", err)
	}
	if _, err := manager.ProcessCancelRequest(t.Context(), target); err != nil {
		t.Fatalf("ProcessCancelRequest() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(cancel) error = %v", err)
	}

	// The mempool-stuck cancel goes stale; its bump must be another cancel noop,
	// never a rebuild of the canceled task calldata.
	forceBroadcastStale(t, id)
	if _, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target); err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement(cancel bump) error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(cancel bump) error = %v", err)
	}
	if len(client.sent) != 3 {
		t.Fatalf("sent tx count = %d, want task, cancel, and cancel bump", len(client.sent))
	}
	bump := client.sent[2]
	if bump.To() == nil || *bump.To() != signer.Address() || bump.Value().Sign() != 0 || len(bump.Data()) != 0 {
		t.Fatalf("cancel bump to=%v value=%s data=%d bytes, want a self-transfer noop", bump.To(), bump.Value(), len(bump.Data()))
	}
	if bump.GasFeeCap().Cmp(client.sent[1].GasFeeCap()) <= 0 {
		t.Fatalf("cancel bump fee %s did not exceed the previous cancel fee %s", bump.GasFeeCap(), client.sent[1].GasFeeCap())
	}
	if bumpKind := queryActiveAttemptKind(t, id); bumpKind != db.TxAttemptCancel {
		t.Fatalf("bump kind = %q, want cancel", bumpKind)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
		`purpose=pricing_set_price_snapshot`,
		`receipt_status=1`,
	)
}

func TestProcessReceiptsDefersReceiptBelowConfirmationDepth(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 33,
		receipts:     make(map[common.Hash]*types.Receipt),
		// Head is 11 blocks past the receipt: one short of the 12-confirmation
		// boundary (head >= receipt + confirmations), so the receipt must wait.
		header:             &types.Header{Number: big.NewInt(1_234_567 + 11), BaseFee: big.NewInt(500_000_000)},
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
		// Head is 12 blocks past the receipt, exactly at the confirmation
		// boundary (head >= receipt + confirmations), matching the indexer's
		// confirmed window head - confirmations.
		header:             &types.Header{Number: big.NewInt(1_234_567 + 12), BaseFee: big.NewInt(500_000_000)},
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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

func TestProcessReceiptsDefersReceiptOffTheCanonicalChain(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       44,
		receipts:           make(map[common.Hash]*types.Receipt),
		header:             &types.Header{Number: big.NewInt(1_234_567 + 12), BaseFee: big.NewInt(500_000_000)},
		suggestedGasTipCap: big.NewInt(1_000_000_000),
	}
	manager := New(store, discardLogger())

	if _, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x06, 0x07},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	id, err := manager.ProcessNext(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy()))
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if outboxTx.Nonce >= client.pendingNonce {
		client.pendingNonce = outboxTx.Nonce + 1
	}
	// The receipt is depth-buried, but its block hash is not the majority
	// canonical hash at that height: the chain reorged between the receipt and
	// head reads. Terminalizing would pin an orphaned transaction.
	receipt := testReceipt(outboxTx.TxHash, types.ReceiptStatusSuccessful)
	receipt.BlockHash = common.HexToHash("0x0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e")
	client.receipts[outboxTx.TxHash] = receipt
	client.canonicalHashes = map[uint64]common.Hash{
		receipt.BlockNumber.Uint64(): common.HexToHash("0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c"),
	}

	target := Target{ChainEID: 40161, ChainID: big.NewInt(11155111), Signer: signer, Client: client, Confirmations: 12}
	if _, err := manager.ProcessReceipts(t.Context(), target, 1); !errors.Is(err, ErrNoReceiptUpdate) {
		t.Fatalf("ProcessReceipts() error = %v, want ErrNoReceiptUpdate (deferred)", err)
	}
	after, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(after) error = %v", err)
	}
	if after.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want broadcast (still polling, not terminalized)", after.Status)
	}

	// Once the canonical chain includes the receipt's block, it terminalizes.
	client.canonicalHashes[receipt.BlockNumber.Uint64()] = receipt.BlockHash
	if _, err := manager.ProcessReceipts(t.Context(), target, 1); err != nil {
		t.Fatalf("ProcessReceipts(canonical) error = %v", err)
	}
	confirmed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(final) error = %v", err)
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
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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

func TestProcessReceiptsReplayFinalizesAfterManualReview(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce: 78,
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

	target := testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())
	id, err := manager.ProcessNext(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	outboxTx, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	client.receipts[outboxTx.TxHash] = testReceipt(outboxTx.TxHash, types.ReceiptStatusFailed)

	// Simulate a crash between the workflow write and the receipt finalizer: the
	// workflow effect (LZ_RECEIVE_FAILED) already committed ...
	if err := store.MarkExecutorReceiveFailed(t.Context(), packet.GUID, outboxTx.TxHash, "lzReceive transaction reverted"); err != nil {
		t.Fatalf("MarkExecutorReceiveFailed() error = %v", err)
	}
	// ... and before the replay, the worker legally parks the job for operator
	// review (for example the delivery retry budget runs out).
	if err := store.MarkExecutorManualReview(t.Context(), packet.GUID, string(packets.ExecutorLzReceiveFailed), "delivery retry budget exhausted"); err != nil {
		t.Fatalf("MarkExecutorManualReview() error = %v", err)
	}

	// The replay must treat MANUAL_REVIEW as a legal successor and still reach
	// the receipt finalizer instead of wedging the receipt stage forever.
	if _, err := manager.ProcessReceipts(t.Context(), Target{
		ChainEID: packet.DstEID,
		ChainID:  big.NewInt(560048),
		Signer:   signer,
		Client:   client,
	}, 1); err != nil {
		t.Fatalf("ProcessReceipts(replay) error = %v", err)
	}
	parked, err := store.GetPacket(t.Context(), packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if parked.Status != string(packets.ExecutorManualReview) {
		t.Fatalf("packet status = %q, want %q (replay must not disturb the parked job)", parked.Status, packets.ExecutorManualReview)
	}
	finalized, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(finalized) error = %v", err)
	}
	if finalized.Status != db.TxStatusFailed || finalized.FailureKind != db.TxFailureReceiptFailed {
		t.Fatalf("tx = %q/%q, want failed/receipt_failed (finalizer must still run)", finalized.Status, finalized.FailureKind)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(packet.DstEID, big.NewInt(560048), signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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
	if _, err := manager.ProcessBroadcast(t.Context(), testTarget(chainEID, chainID, signer, client, defaultFeePolicy())); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
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

func setChainScopeFlags(t *testing.T, eid uint32, enabled, paused bool) {
	t.Helper()
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	// A fresh context so the restore also works from t.Cleanup, after the
	// test's own context is canceled.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "UPDATE chains SET enabled = $2, paused = $3 WHERE eid = $1", eid, enabled, paused); err != nil {
		t.Fatalf("set chain scope flags: %v", err)
	}
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

// cleanPacketWorkflowRows deletes every row keyed to one packet GUID so a test
// with a deterministic GUID is idempotent across runs against a shared database.
func cleanPacketWorkflowRows(t *testing.T, guid common.Hash) {
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
	for _, stmt := range []string{
		"DELETE FROM tx_outbox WHERE guid = $1",
		"DELETE FROM dvn_jobs WHERE guid = $1",
		"DELETE FROM executor_jobs WHERE guid = $1",
		"DELETE FROM packets WHERE guid = $1",
	} {
		if _, err := pool.Exec(t.Context(), stmt, guid.Bytes()); err != nil {
			t.Fatalf("clean packet workflow rows (%s): %v", stmt, err)
		}
	}
}

// forceOutboxHeldNonceReconcile puts a row back into the nonce-reconcile hold.
func forceOutboxHeldNonceReconcile(t *testing.T, id int64) {
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
	if _, err := pool.Exec(t.Context(), `
		UPDATE tx_outbox SET status = 'held', held_reason = 'nonce_reconcile_required', updated_at = now() WHERE id = $1
	`, id); err != nil {
		t.Fatalf("force nonce reconcile hold: %v", err)
	}
}

// forceReconcileDue clears the signer lane's reconciliation backoff and lease.
func forceReconcileDue(t *testing.T, signerID string) {
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
	if _, err := pool.Exec(t.Context(), `
		UPDATE tx_nonce_cursors
		SET next_reconcile_at = NULL, reconcile_lease_token = NULL, reconcile_lease_until = NULL
		WHERE signer_id = $1
	`, signerID); err != nil {
		t.Fatalf("force reconcile due: %v", err)
	}
}

func queryActiveAttemptKind(t *testing.T, outboxID int64) string {
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
	var kind string
	if err := pool.QueryRow(t.Context(), `
		SELECT a.kind FROM tx_attempts a JOIN tx_outbox o ON o.active_attempt_id = a.id WHERE o.id = $1
	`, outboxID).Scan(&kind); err != nil {
		t.Fatalf("select active attempt kind: %v", err)
	}
	return kind
}

// forceAttemptBroadcastDue clears the broadcast backoff and lease for every
// attempt of one outbox row so the next ProcessBroadcast claims it immediately.
func forceAttemptBroadcastDue(t *testing.T, outboxID int64) {
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
		UPDATE tx_attempts
		SET next_broadcast_at = now() - interval '1 second',
			broadcast_lease_token = NULL, broadcast_lease_until = NULL
		WHERE outbox_id = $1
	`, outboxID)
	if err != nil {
		t.Fatalf("force attempt broadcast due: %v", err)
	}
	if tag.RowsAffected() == 0 {
		t.Fatalf("force attempt broadcast due: no attempts for outbox %d", outboxID)
	}
}

func queryPreSignBudget(t *testing.T, outboxID int64) (int32, *time.Time) {
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
	var count int32
	var nextSignAt *time.Time
	if err := pool.QueryRow(t.Context(), `
		SELECT pre_sign_failure_count, next_sign_at FROM tx_outbox WHERE id = $1
	`, outboxID).Scan(&count, &nextSignAt); err != nil {
		t.Fatalf("query pre-sign budget: %v", err)
	}
	return count, nextSignAt
}

// forceNonceAssigned simulates a crash after nonce assignment but before the
// attempt insert: the row owns a nonce with no signing lease and no attempt.
func forceNonceAssigned(t *testing.T, id int64, nonce int64) {
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
		SET nonce = $2, status = 'nonce_assigned', lease_token = NULL, lease_until = NULL, updated_at = now()
		WHERE id = $1
	`, id, nonce)
	if err != nil {
		t.Fatalf("force nonce assigned: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("force nonce assigned rows = %d, want 1", tag.RowsAffected())
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
	// The GUID is deterministic per test name and the upserts below do not reset
	// workflow statuses, so scrub any rows a previous run of this test left over.
	cleanPacketWorkflowRows(t, guid)
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
	headerDelay           time.Duration
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
	receiptErrs           map[common.Hash]error
	confirmedNonce        uint64
	nonceAtErr            error
	nonceAtCalls          int
	lastNonceAtBlock      *big.Int
	balance               *big.Int
	balanceErr            error
	// canonicalHashes overrides CanonicalHashAt per height; unset heights fall
	// back to the stored receipt at that height, mimicking a consistent chain.
	canonicalHashes  map[uint64]common.Hash
	canonicalHashErr error
}

func (f *fakeChainClient) CanonicalHashAt(_ context.Context, blockNumber *big.Int) (common.Hash, error) {
	if f.canonicalHashErr != nil {
		return common.Hash{}, f.canonicalHashErr
	}
	if f.canonicalHashes != nil {
		if hash, ok := f.canonicalHashes[blockNumber.Uint64()]; ok {
			return hash, nil
		}
	}
	for _, receipt := range f.receipts {
		if receipt != nil && receipt.BlockNumber != nil && receipt.BlockNumber.Uint64() == blockNumber.Uint64() {
			return receipt.BlockHash, nil
		}
	}
	return common.Hash{}, nil
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
	if f.headerDelay > 0 {
		time.Sleep(f.headerDelay)
	}
	if f.headerErr != nil {
		return nil, f.headerErr
	}
	if f.header != nil {
		copied := *f.header
		return &copied, nil
	}
	return dynamicHeader(), nil
}

func (f *fakeChainClient) NonceAt(_ context.Context, _ common.Address, block *big.Int) (uint64, error) {
	f.nonceAtCalls++
	f.lastNonceAtBlock = block
	if f.nonceAtErr != nil {
		return 0, f.nonceAtErr
	}
	return f.confirmedNonce, nil
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

func (f *fakeChainClient) TransactionReceiptAt(ctx context.Context, txHash common.Hash, _ *big.Int) (*types.Receipt, error) {
	return f.TransactionReceipt(ctx, txHash)
}

func (f *fakeChainClient) TransactionReceipt(_ context.Context, txHash common.Hash) (*types.Receipt, error) {
	if err, ok := f.receiptErrs[txHash]; ok {
		return nil, err
	}
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
			db.TxPurposePricingSetPriceSnapshot: policy,
			executorCommitVerificationPurpose:   policy,
			executorLzReceivePurpose:            policy,
			dvnVerifyPurpose:                    policy,
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

func TestStaleBroadcastReplacementIgnoresOrphanedReceipt(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       47,
		estimatedGas:       123_456,
		header:             &types.Header{Number: big.NewInt(1_234_567 + 12), BaseFee: big.NewInt(500_000_000)},
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
	}
	manager := NewWithOptions(store, discardLogger(), Options{
		StaleBroadcastReplacementAfter: 2 * time.Second,
	})

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x0a, 0x0b},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	target.Confirmations = 12
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	// A provider keeps serving a receipt whose block the majority chain has
	// orphaned. It must not count as mined: nothing can build on this nonce,
	// so suppressing the replacement would wedge the lane forever.
	receipt := testReceipt(original.TxHash, types.ReceiptStatusSuccessful)
	receipt.BlockHash = common.HexToHash("0x0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e")
	client.receipts[original.TxHash] = receipt
	client.canonicalHashes = map[uint64]common.Hash{
		receipt.BlockNumber.Uint64(): common.HexToHash("0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c"),
	}
	forceBroadcastAgeSeconds(t, id, 3)

	// Mirror the real processOnce ordering: receipt polling runs first. The
	// orphaned receipt must defer WITHOUT refreshing the stale timer, or the
	// replacement below would never see a stale row and the lane would wedge.
	if _, err := manager.ProcessReceipts(t.Context(), target, 1); !errors.Is(err, ErrNoReceiptUpdate) {
		t.Fatalf("ProcessReceipts() error = %v, want ErrNoReceiptUpdate (deferred)", err)
	}

	replacedID, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target)
	if err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v", err)
	}
	if replacedID != id {
		t.Fatalf("replaced id = %d, want %d", replacedID, id)
	}
	after, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(after) error = %v", err)
	}
	if after.TxHash == original.TxHash {
		t.Fatal("orphaned receipt suppressed the same-nonce replacement")
	}
}

func TestNonceReconciliationIgnoresOrphanedReceipt(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       97,
		estimatedGas:       111_111,
		header:             &types.Header{Number: big.NewInt(6_000_000), BaseFee: big.NewInt(500_000_000)},
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
		receiptErrs:        make(map[common.Hash]error),
	}
	manager := New(store, discardLogger())
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	target.Confirmations = 12

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x0c},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	client.sendErr = errors.New("connection glitch before acknowledgement")
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	forceAttemptBroadcastDue(t, id)
	token := uuid.New()
	claim, err := store.ClaimAttemptForBroadcast(t.Context(), 40161, signer.Address().Hex(), token, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast() error = %v", err)
	}
	if err := store.MarkAttemptSendResult(t.Context(), claim.AttemptID, token, db.SendErrorNonceTooLow, "nonce too low"); err != nil {
		t.Fatalf("MarkAttemptSendResult() error = %v", err)
	}
	held, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(held) error = %v", err)
	}
	if held.Status != db.TxStatusHeld {
		t.Fatalf("status = %q, want held", held.Status)
	}

	// The stale-view node also serves an orphaned receipt for the attempt. It
	// must not defer the reconciliation forever: the nonce is unspent on the
	// majority chain, so the hold releases and the raw resumes replaying.
	receipt := testReceipt(held.TxHash, types.ReceiptStatusSuccessful)
	receipt.BlockHash = common.HexToHash("0x0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e")
	client.receipts[held.TxHash] = receipt
	client.canonicalHashes = map[uint64]common.Hash{
		receipt.BlockNumber.Uint64(): common.HexToHash("0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c"),
	}
	client.confirmedNonce = held.Nonce
	if _, err := manager.ProcessNonceReconciliation(t.Context(), target); err != nil {
		t.Fatalf("ProcessNonceReconciliation() error = %v", err)
	}
	released, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(released) error = %v", err)
	}
	if released.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want broadcast (orphaned receipt must not defer the release)", released.Status)
	}
}

func TestProcessReceiptsSkipsOrphanedCandidateForCanonicalWinner(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       57,
		estimatedGas:       123_456,
		header:             &types.Header{Number: big.NewInt(1_234_568 + 12), BaseFee: big.NewInt(500_000_000)},
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
	}
	manager := NewWithOptions(store, discardLogger(), Options{
		StaleBroadcastReplacementAfter: 2 * time.Second,
	})
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	target.Confirmations = 12

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x0d, 0x0e},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	forceBroadcastAgeSeconds(t, id, 3)
	if _, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target); err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(replacement) error = %v", err)
	}
	replaced, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(replacement) error = %v", err)
	}
	if replaced.TxHash == original.TxHash {
		t.Fatal("replacement did not switch the active attempt")
	}

	// The older attempt keeps a persistently served orphaned receipt while the
	// replacement's receipt is canonical one block later. The ascending scan
	// must skip the orphan and terminalize with the canonical winner.
	orphaned := testReceipt(original.TxHash, types.ReceiptStatusSuccessful)
	orphaned.BlockHash = common.HexToHash("0x0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e")
	client.receipts[original.TxHash] = orphaned
	canonical := testReceipt(replaced.TxHash, types.ReceiptStatusSuccessful)
	canonical.BlockNumber = big.NewInt(1_234_568)
	client.receipts[replaced.TxHash] = canonical
	client.canonicalHashes = map[uint64]common.Hash{
		orphaned.BlockNumber.Uint64():  common.HexToHash("0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c"),
		canonical.BlockNumber.Uint64(): canonical.BlockHash,
	}

	processedID, err := manager.ProcessReceipts(t.Context(), target, 1)
	if err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	if processedID != id {
		t.Fatalf("processed id = %d, want %d", processedID, id)
	}
	confirmed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(final) error = %v", err)
	}
	if confirmed.Status != db.TxStatusConfirmed {
		t.Fatalf("status = %q, want confirmed via the canonical replacement receipt", confirmed.Status)
	}
	if confirmed.ReceiptBlockNumber == nil || *confirmed.ReceiptBlockNumber != 1_234_568 {
		t.Fatalf("receipt block = %v, want the canonical replacement block 1234568", confirmed.ReceiptBlockNumber)
	}
}

func TestProcessReceiptsSkipsUnsentSignedAttempt(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       67,
		estimatedGas:       123_456,
		header:             dynamicHeader(),
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
		receiptErrs:        make(map[common.Hash]error),
	}
	manager := New(store, discardLogger())
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x0f},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	signed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	// The signed raw was never handed to a node; a broken receipt endpoint for
	// its hash must not be consulted at all, or a receipts-first processOnce
	// would starve the first broadcast forever.
	client.receiptErrs[signed.TxHash] = errors.New("receipt endpoint down")
	if _, err := manager.ProcessReceipts(t.Context(), target, 1); !errors.Is(err, ErrNoReceiptUpdate) {
		t.Fatalf("ProcessReceipts(unsent) error = %v, want ErrNoReceiptUpdate without touching the receipt endpoint", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	// Once sent the task is polled, but a failing endpoint only skips it —
	// aborting the pass would starve the broadcast/replacement stages behind
	// receipts in processOnce.
	if _, err := manager.ProcessReceipts(t.Context(), target, 1); !errors.Is(err, ErrNoReceiptUpdate) {
		t.Fatalf("ProcessReceipts(sent) error = %v, want ErrNoReceiptUpdate (task skipped, pass alive)", err)
	}
	// With the endpoint healthy again, the receipt lands normally.
	delete(client.receiptErrs, signed.TxHash)
	client.receipts[signed.TxHash] = testReceipt(signed.TxHash, types.ReceiptStatusSuccessful)
	if _, err := manager.ProcessReceipts(t.Context(), target, 1); err != nil {
		t.Fatalf("ProcessReceipts(recovered) error = %v", err)
	}
}

func TestProcessReceiptsScansPastFailingOldHash(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       77,
		estimatedGas:       123_456,
		header:             &types.Header{Number: big.NewInt(1_234_568 + 12), BaseFee: big.NewInt(500_000_000)},
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
		receiptErrs:        make(map[common.Hash]error),
	}
	manager := NewWithOptions(store, discardLogger(), Options{
		StaleBroadcastReplacementAfter: 2 * time.Second,
	})
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())
	target.Confirmations = 12

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x1a, 0x1b},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	original, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	forceBroadcastAgeSeconds(t, id, 3)
	if _, err := manager.ProcessStaleBroadcastReplacement(t.Context(), target); err != nil {
		t.Fatalf("ProcessStaleBroadcastReplacement() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast(replacement) error = %v", err)
	}
	replaced, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(replacement) error = %v", err)
	}

	// The superseded old hash answers with a persistent error while the
	// replacement's receipt is canonical: the scan must reach the later
	// attempt — one nonce mines at most once, so the canonical receipt is
	// authoritative regardless of the old hash's answer.
	client.receiptErrs[original.TxHash] = errors.New("hash-specific lookup failure")
	canonical := testReceipt(replaced.TxHash, types.ReceiptStatusSuccessful)
	canonical.BlockNumber = big.NewInt(1_234_568)
	client.receipts[replaced.TxHash] = canonical
	client.canonicalHashes = map[uint64]common.Hash{
		canonical.BlockNumber.Uint64(): canonical.BlockHash,
	}

	if _, err := manager.ProcessReceipts(t.Context(), target, 1); err != nil {
		t.Fatalf("ProcessReceipts() error = %v", err)
	}
	confirmed, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(final) error = %v", err)
	}
	if confirmed.Status != db.TxStatusConfirmed {
		t.Fatalf("status = %q, want confirmed via the canonical replacement receipt", confirmed.Status)
	}
}

// TestNonceReconciliationHeartbeatOutlivesSlowRPC pins the lease lifecycle:
// the heartbeat starts as soon as the lease is held, so slow RPC phases (a
// hanging head read here) cannot expire the lease before the final publish
// that CASes on it.
func TestNonceReconciliationHeartbeatOutlivesSlowRPC(t *testing.T) {
	store := openTestStore(t)
	signer := newTestKeystoreSigner(t)
	client := &fakeChainClient{
		pendingNonce:       117,
		estimatedGas:       111_111,
		header:             &types.Header{Number: big.NewInt(6_000_000), BaseFee: big.NewInt(500_000_000)},
		headerDelay:        600 * time.Millisecond,
		suggestedGasTipCap: big.NewInt(1_000_000_000),
		receipts:           make(map[common.Hash]*types.Receipt),
		receiptErrs:        make(map[common.Hash]error),
	}
	manager := NewWithOptions(store, discardLogger(), Options{
		SigningLeaseTTL: 200 * time.Millisecond,
	})
	target := testTarget(40161, big.NewInt(11155111), signer, client, defaultFeePolicy())

	id, err := store.EnqueueTx(t.Context(), db.TxRequest{
		ChainEID: 40161,
		Purpose:  db.TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x1c},
		Value:    big.NewInt(0),
		SignerID: signer.Address().Hex(),
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	client.sendErr = errors.New("connection glitch before acknowledgement")
	if _, err := manager.ProcessNext(t.Context(), target); err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if _, err := manager.ProcessBroadcast(t.Context(), target); err != nil {
		t.Fatalf("ProcessBroadcast() error = %v", err)
	}
	forceAttemptBroadcastDue(t, id)
	token := uuid.New()
	claim, err := store.ClaimAttemptForBroadcast(t.Context(), 40161, signer.Address().Hex(), token, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast() error = %v", err)
	}
	if err := store.MarkAttemptSendResult(t.Context(), claim.AttemptID, token, db.SendErrorNonceTooLow, "nonce too low"); err != nil {
		t.Fatalf("MarkAttemptSendResult() error = %v", err)
	}
	held, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(held) error = %v", err)
	}
	if held.Status != db.TxStatusHeld {
		t.Fatalf("status = %q, want held", held.Status)
	}

	// The 600ms head read alone outlives the 200ms lease; only the heartbeat
	// keeps the final publish's lease CAS satisfiable.
	client.confirmedNonce = held.Nonce
	if _, err := manager.ProcessNonceReconciliation(t.Context(), target); err != nil {
		t.Fatalf("ProcessNonceReconciliation() error = %v", err)
	}
	released, err := store.GetOutboxTx(t.Context(), id)
	if err != nil {
		t.Fatalf("GetOutboxTx(released) error = %v", err)
	}
	if released.Status != db.TxStatusBroadcast {
		t.Fatalf("status = %q, want broadcast (lease survived the slow RPC)", released.Status)
	}
}
