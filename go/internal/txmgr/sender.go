package txmgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/islishude/oh-my-lazier/go/internal/bigutil"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/islishude/oh-my-lazier/go/internal/signer"
	"github.com/jackc/pgx/v5"
)

const (
	executorCommitVerificationPurpose = "executor_commit_verification"
	executorLzReceivePurpose          = "executor_lz_receive"
	dvnVerifyPurpose                  = "dvn_verify"

	replacementBumpNumerator   = int64(110)
	replacementBumpDenominator = int64(100)
)

// ChainClient is the tx manager's RPC boundary for first-use nonce bootstrap, fee reads, and broadcasts.
type ChainClient interface {
	BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error)
	EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	SuggestGasTipCap(ctx context.Context) (*big.Int, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
}

// FeePolicy caps send-time gas fees for one outbox purpose.
type FeePolicy struct {
	ConfiguredMaxFeePerGas         *big.Int
	ConfiguredMaxPriorityFeePerGas *big.Int
}

type feeQuote struct {
	Dynamic              bool
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
}

// ErrNoQueuedTx indicates no queued outbox row exists for the signer on a chain.
var ErrNoQueuedTx = errors.New("no queued tx")

// ErrNoReceiptUpdate indicates no broadcast tx receipt changed durable state.
var ErrNoReceiptUpdate = errors.New("no receipt update")

// ErrTxDeferred indicates the queued outbox row should stay queued and be retried later.
var ErrTxDeferred = errors.New("tx deferred")

// ProcessNext signs and broadcasts one queued outbox transaction for a signer.
func (m *Manager) ProcessNext(ctx context.Context, target Target) (int64, error) {
	if target.ChainID == nil || target.ChainID.Sign() <= 0 {
		return 0, errors.New("chain id is required")
	}
	if target.Signer == nil {
		return 0, errors.New("target signer is required")
	}
	if target.Client == nil {
		return 0, errors.New("target client is required")
	}
	signerID := target.Signer.Address().Hex()
	queued, err := m.store.PeekSendableTx(ctx, target.ChainEID, signerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNoQueuedTx
	}
	if err != nil {
		return 0, err
	}
	policy, ok := target.FeePolicies[queued.Purpose]
	if !ok {
		return 0, fmt.Errorf("tx purpose %q has no fee policy for chain %d signer %s", queued.Purpose, target.ChainEID, signerID)
	}
	quote, err := quoteFee(ctx, queued, policy, target.Client)
	if err != nil {
		if errors.Is(err, ErrTxDeferred) {
			m.logger.Debug("deferred tx outbox row", "reason", "fee_cap", "id", queued.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", queued.Purpose)
		}
		return 0, err
	}
	gasLimit, err := estimateGas(ctx, queued, target.Signer.Address(), target.Client)
	if err != nil {
		if isEstimateGasRevert(err) {
			if markErr := m.store.MarkTxFailed(ctx, queued.ID, fmt.Errorf("estimate gas reverted: %w", err), db.TxFailureEstimateGasRevert); markErr != nil {
				return 0, markErr
			}
			m.logger.Warn("failed tx gas estimate", "reason", "estimate_gas_revert", "id", queued.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", queued.Purpose, "failure_kind", db.TxFailureEstimateGasRevert, "error", err.Error())
			return queued.ID, nil
		}
		m.logger.Debug("deferred tx outbox row", "reason", "estimate_gas_error", "id", queued.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", queued.Purpose, "error", err.Error())
		return 0, fmt.Errorf("%w: estimate gas for outbox tx %d: %w", ErrTxDeferred, queued.ID, err)
	}
	if queued.Status == db.TxStatusQueued {
		claimed, err := m.store.ClaimTxNonce(ctx, queued.ID, target.ChainEID, signerID)
		if errors.Is(err, db.ErrNonceCursorMissing) {
			rpcNonce, nonceErr := target.Client.PendingNonceAt(ctx, target.Signer.Address())
			if nonceErr != nil {
				return 0, nonceErr
			}
			if _, nonceErr := m.store.BootstrapTxNonceCursor(ctx, target.ChainEID, signerID, rpcNonce); nonceErr != nil {
				return 0, nonceErr
			}
			m.logger.Info("bootstrapped tx nonce cursor", "id", queued.ID, "chain_eid", target.ChainEID, "signer", signerID, "rpc_nonce", rpcNonce)
			claimed, err = m.store.ClaimTxNonce(ctx, queued.ID, target.ChainEID, signerID)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNoQueuedTx
		}
		if err != nil {
			return 0, err
		}
		m.logger.Info("claimed tx nonce", "id", claimed.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", queued.Purpose, "nonce", claimed.Nonce)
	} else if queued.Status != db.TxStatusNonceAssigned {
		return 0, fmt.Errorf("outbox tx %d has unsupported sendable status %s", queued.ID, queued.Status)
	}
	outboxTx, err := m.store.GetOutboxTx(ctx, queued.ID)
	if err != nil {
		return 0, err
	}
	signed, err := signOutboxTx(ctx, outboxTx, target.ChainID, gasLimit, quote, target.Signer)
	if err != nil {
		if markErr := m.store.MarkTxFailed(ctx, outboxTx.ID, err, db.TxFailureSignFailed); markErr != nil {
			return 0, markErr
		}
		m.logger.Warn("failed tx signing", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "failure_kind", db.TxFailureSignFailed, "error", err.Error())
		return outboxTx.ID, nil
	}
	if err := m.store.MarkTxSignedWithGasAndFees(ctx, outboxTx.ID, signed.Hash(), gasLimit, quote.MaxFeePerGas, quote.MaxPriorityFeePerGas); err != nil {
		return 0, err
	}
	m.logger.Info("signed tx outbox row", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", signed.Hash(), "gas_limit", gasLimit, "dynamic_fee", quote.Dynamic)
	if err := target.Client.SendTransaction(ctx, signed); err != nil {
		if markErr := m.store.MarkTxFailed(ctx, outboxTx.ID, err, db.TxFailureBroadcastFailed); markErr != nil {
			return 0, markErr
		}
		m.logger.Warn("failed tx broadcast", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", signed.Hash(), "failure_kind", db.TxFailureBroadcastFailed, "error", err.Error())
		return outboxTx.ID, nil
	}
	if err := m.store.MarkTxBroadcast(ctx, outboxTx.ID, signed.Hash()); err != nil {
		return 0, err
	}
	m.logger.Info("broadcast tx outbox row", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", signed.Hash())
	return outboxTx.ID, nil
}

// ProcessReceipts checks broadcast transactions and marks mined receipts as confirmed or failed.
func (m *Manager) ProcessReceipts(ctx context.Context, target Target, limit int) (int64, error) {
	if target.Signer == nil {
		return 0, errors.New("target signer is required")
	}
	if target.Client == nil {
		return 0, errors.New("target client is required")
	}
	broadcasts, err := m.store.ListBroadcastTx(ctx, target.ChainEID, target.Signer.Address().Hex(), limit)
	if err != nil {
		return 0, err
	}
	var head *types.Header
	for _, outboxTx := range broadcasts {
		receipt, err := target.Client.TransactionReceipt(ctx, outboxTx.TxHash)
		if errors.Is(err, ethereum.NotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if receipt.TxHash != outboxTx.TxHash {
			return 0, fmt.Errorf("receipt tx hash %s does not match outbox tx hash %s", receipt.TxHash, outboxTx.TxHash)
		}
		// Do not apply an irreversible terminal workflow state until the receipt
		// is buried under the chain's confirmation depth; a short reorg before
		// then could otherwise leave the database terminal for a rolled-back tx.
		if target.Confirmations > 0 {
			if head == nil {
				head, err = target.Client.HeaderByNumber(ctx, nil)
				if err != nil {
					return 0, err
				}
			}
			confirmed, err := receiptConfirmed(head, receipt, target.Confirmations)
			if err != nil {
				return 0, err
			}
			if !confirmed {
				// Refresh the row so the stale-broadcast replacement does not treat
				// this mined-but-shallow tx as stuck in the mempool.
				if err := m.store.RefreshBroadcastReceiptObservedAt(ctx, outboxTx.ID); err != nil {
					return 0, err
				}
				m.logger.Debug("deferred tx receipt below confirmation depth", "id", outboxTx.ID, "chain_eid", target.ChainEID, "purpose", outboxTx.Purpose, "tx_hash", outboxTx.TxHash, "confirmations", target.Confirmations)
				continue
			}
		}
		facts, err := txReceiptFacts(receipt)
		if err != nil {
			return 0, err
		}
		if err := m.store.RecordTxReceipt(ctx, outboxTx.ID, facts); err != nil {
			return 0, err
		}
		if receipt.Status == types.ReceiptStatusSuccessful {
			if err := m.applyWorkflowReceipt(ctx, outboxTx, true); err != nil {
				return 0, err
			}
			if err := m.store.MarkTxConfirmed(ctx, outboxTx.ID, outboxTx.TxHash); err != nil {
				return 0, err
			}
			m.logger.Info("confirmed tx receipt", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", target.Signer.Address(), "purpose", outboxTx.Purpose, "tx_hash", outboxTx.TxHash, "receipt_status", receipt.Status, "gas_used", facts.GasUsed, "effective_gas_price", facts.EffectiveGasPrice, "gas_cost_dst_wei", facts.GasCostDstWei)
			return outboxTx.ID, nil
		}
		if err := m.applyWorkflowReceipt(ctx, outboxTx, false); err != nil {
			return 0, err
		}
		if err := m.store.MarkTxFailed(ctx, outboxTx.ID, fmt.Errorf("transaction receipt status %d", receipt.Status), db.TxFailureReceiptFailed); err != nil {
			return 0, err
		}
		m.logger.Warn("failed tx receipt", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", target.Signer.Address(), "purpose", outboxTx.Purpose, "tx_hash", outboxTx.TxHash, "receipt_status", receipt.Status, "gas_used", facts.GasUsed, "effective_gas_price", facts.EffectiveGasPrice, "gas_cost_dst_wei", facts.GasCostDstWei, "failure_kind", db.TxFailureReceiptFailed)
		return outboxTx.ID, nil
	}
	return 0, ErrNoReceiptUpdate
}

// ProcessFailedRetry requeues one due failed transaction for a signer.
func (m *Manager) ProcessFailedRetry(ctx context.Context, target Target) (int64, error) {
	if target.Signer == nil {
		return 0, errors.New("target signer is required")
	}
	return m.store.PrepareNextFailedTxRetry(ctx, target.ChainEID, target.Signer.Address().Hex())
}

// ProcessStaleBroadcastReplacement signs and broadcasts one long-pending same-nonce replacement.
func (m *Manager) ProcessStaleBroadcastReplacement(ctx context.Context, target Target) (int64, error) {
	if target.ChainID == nil || target.ChainID.Sign() <= 0 {
		return 0, errors.New("chain id is required")
	}
	if target.Signer == nil {
		return 0, errors.New("target signer is required")
	}
	if target.Client == nil {
		return 0, errors.New("target client is required")
	}
	signerID := target.Signer.Address().Hex()
	outboxTx, err := m.store.PrepareNextStaleBroadcastReplacement(ctx, target.ChainEID, signerID, m.options.StaleBroadcastReplacementAfter)
	if err != nil {
		return 0, err
	}
	// A reserved stale row may actually be mined and only waiting to reach
	// confirmation depth (the receipt gate keeps it in broadcast). Replacing it
	// would broadcast a doomed same-nonce tx, so skip; PrepareNext already
	// refreshed updated_at, keeping it out of the next stale window.
	receipt, err := target.Client.TransactionReceipt(ctx, outboxTx.TxHash)
	if err != nil && !errors.Is(err, ethereum.NotFound) {
		return 0, err
	}
	if err == nil && receipt != nil {
		m.logger.Debug("skipped stale replacement for a mined tx awaiting confirmations", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", outboxTx.TxHash)
		return outboxTx.ID, nil
	}
	queued := queuedFromOutbox(outboxTx)
	policy, ok := target.FeePolicies[outboxTx.Purpose]
	if !ok {
		return 0, fmt.Errorf("missing fee policy for purpose %q", outboxTx.Purpose)
	}
	gasLimit := outboxTx.GasLimit
	if gasLimit == 0 {
		var err error
		gasLimit, err = estimateGas(ctx, queued, target.Signer.Address(), target.Client)
		if err != nil {
			if markErr := m.store.MarkTxReplacementAttemptFailed(ctx, outboxTx.ID, outboxTx.TxHash, err); markErr != nil {
				return 0, markErr
			}
			m.logger.Warn("failed stale broadcast replacement gas estimate", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", outboxTx.TxHash, "error", err.Error())
			return outboxTx.ID, nil
		}
	}
	quote, err := quoteFee(ctx, queued, policy, target.Client)
	if err != nil {
		if errors.Is(err, ErrTxDeferred) {
			if markErr := m.store.MarkTxReplacementDeferred(ctx, outboxTx.ID, outboxTx.TxHash, err); markErr != nil {
				return 0, markErr
			}
			return 0, ErrTxDeferred
		}
		if markErr := m.store.MarkTxReplacementAttemptFailed(ctx, outboxTx.ID, outboxTx.TxHash, err); markErr != nil {
			return 0, markErr
		}
		return 0, err
	}
	signed, err := signReplacementOutboxTx(ctx, outboxTx, target.ChainID, gasLimit, quote, target.Signer)
	if err != nil {
		if markErr := m.store.MarkTxReplacementAttemptFailed(ctx, outboxTx.ID, outboxTx.TxHash, err); markErr != nil {
			return 0, markErr
		}
		m.logger.Warn("failed stale broadcast replacement signing", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", outboxTx.TxHash, "error", err.Error())
		return outboxTx.ID, nil
	}
	if err := target.Client.SendTransaction(ctx, signed); err != nil {
		if markErr := m.store.MarkTxReplacementAttemptFailed(ctx, outboxTx.ID, outboxTx.TxHash, err); markErr != nil {
			return 0, markErr
		}
		m.logger.Warn("failed stale broadcast replacement broadcast", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", signed.Hash(), "previous_tx_hash", outboxTx.TxHash, "error", err.Error())
		return outboxTx.ID, nil
	}
	if err := m.store.MarkTxReplacementBroadcast(ctx, outboxTx.ID, outboxTx.TxHash, signed.Hash(), gasLimit, quote.MaxFeePerGas, quote.MaxPriorityFeePerGas); err != nil {
		return 0, err
	}
	m.logger.Info("broadcast stale tx replacement", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", signed.Hash(), "previous_tx_hash", outboxTx.TxHash)
	return outboxTx.ID, nil
}

// receiptConfirmed reports whether a receipt is buried under at least
// confirmations blocks relative to the latest head.
func receiptConfirmed(head *types.Header, receipt *types.Receipt, confirmations uint64) (bool, error) {
	if head == nil || head.Number == nil || !head.Number.IsUint64() {
		return false, errors.New("latest header block number is unavailable")
	}
	if receipt.BlockNumber == nil || !receipt.BlockNumber.IsUint64() {
		return false, errors.New("receipt block number is unavailable")
	}
	headNumber := head.Number.Uint64()
	receiptNumber := receipt.BlockNumber.Uint64()
	if headNumber < receiptNumber {
		return false, nil
	}
	return headNumber-receiptNumber+1 >= confirmations, nil
}

func (m *Manager) applyWorkflowReceipt(ctx context.Context, outboxTx db.OutboxTx, success bool) error {
	if len(outboxTx.GUID) != common.HashLength {
		return nil
	}
	guid := common.BytesToHash(outboxTx.GUID)
	switch outboxTx.Purpose {
	case executorCommitVerificationPurpose:
		if success {
			return m.markExecutorReceipt(ctx, guid, func() error {
				return m.store.MarkExecutorCommitted(ctx, guid, outboxTx.TxHash)
			}, packets.ExecutorCommitted, packets.ExecutorExecutable, packets.ExecutorLzReceiveTxEnqueued, packets.ExecutorLzReceiveFailed, packets.ExecutorDelivered)
		}
	case executorLzReceivePurpose:
		if success {
			return m.markExecutorReceipt(ctx, guid, func() error {
				return m.store.MarkExecutorDelivered(ctx, guid, outboxTx.TxHash)
			}, packets.ExecutorDelivered)
		}
		return m.markExecutorReceipt(ctx, guid, func() error {
			return m.store.MarkExecutorReceiveFailed(ctx, guid, outboxTx.TxHash, "lzReceive transaction reverted")
		}, packets.ExecutorLzReceiveFailed, packets.ExecutorDelivered)
	case dvnVerifyPurpose:
		if success {
			return m.markDVNReceipt(ctx, guid, func() error {
				return m.store.MarkDVNVerified(ctx, guid, outboxTx.TxHash)
			}, packets.DVNVerified)
		}
	}
	return nil
}

func txReceiptFacts(receipt *types.Receipt) (db.TxReceiptFacts, error) {
	if receipt == nil {
		return db.TxReceiptFacts{}, errors.New("tx receipt is required")
	}
	if receipt.TxHash == (common.Hash{}) {
		return db.TxReceiptFacts{}, errors.New("receipt tx hash is required")
	}
	if receipt.BlockNumber == nil || receipt.BlockNumber.Sign() < 0 || !receipt.BlockNumber.IsUint64() {
		return db.TxReceiptFacts{}, errors.New("receipt block number must be a non-negative uint64")
	}
	if receipt.EffectiveGasPrice == nil || receipt.EffectiveGasPrice.Sign() < 0 {
		return db.TxReceiptFacts{}, errors.New("receipt effective gas price must be non-negative")
	}
	gasCostDstWei := new(big.Int).Mul(new(big.Int).SetUint64(receipt.GasUsed), receipt.EffectiveGasPrice)
	return db.TxReceiptFacts{
		TxHash:            receipt.TxHash,
		Status:            receipt.Status,
		BlockNumber:       receipt.BlockNumber.Uint64(),
		GasUsed:           receipt.GasUsed,
		EffectiveGasPrice: bigutil.Clone(receipt.EffectiveGasPrice),
		GasCostDstWei:     gasCostDstWei,
	}, nil
}

func (m *Manager) markExecutorReceipt(ctx context.Context, guid common.Hash, mark func() error, alreadyApplied ...packets.ExecutorState) error {
	if m.executorStatusMatches(ctx, guid, alreadyApplied) {
		return nil
	}
	if err := mark(); err != nil {
		if m.executorStatusMatches(ctx, guid, alreadyApplied) {
			return nil
		}
		return err
	}
	return nil
}

func (m *Manager) executorStatusMatches(ctx context.Context, guid common.Hash, statuses []packets.ExecutorState) bool {
	packet, err := m.store.GetPacket(ctx, guid)
	if err != nil {
		return false
	}
	for _, status := range statuses {
		if packet.Status == string(status) {
			return true
		}
	}
	return false
}

func (m *Manager) markDVNReceipt(ctx context.Context, guid common.Hash, mark func() error, alreadyApplied ...packets.DVNState) error {
	if m.dvnStatusMatches(ctx, guid, alreadyApplied) {
		return nil
	}
	if err := mark(); err != nil {
		if m.dvnStatusMatches(ctx, guid, alreadyApplied) {
			return nil
		}
		return err
	}
	return nil
}

func (m *Manager) dvnStatusMatches(ctx context.Context, guid common.Hash, statuses []packets.DVNState) bool {
	job, err := m.store.GetDVNJob(ctx, guid)
	if err != nil {
		return false
	}
	for _, status := range statuses {
		if job.Status == string(status) {
			return true
		}
	}
	return false
}

func estimateGas(ctx context.Context, queued db.QueuedOutboxTx, from common.Address, client ChainClient) (uint64, error) {
	gasLimit, err := client.EstimateGas(ctx, ethereum.CallMsg{
		From:  from,
		To:    &queued.To,
		Value: queued.Value,
		Data:  queued.Calldata,
	})
	if err != nil {
		return 0, err
	}
	if gasLimit == 0 {
		return 0, fmt.Errorf("outbox tx %d estimated gas is zero", queued.ID)
	}
	return gasLimit, nil
}

func queuedFromOutbox(outboxTx db.OutboxTx) db.QueuedOutboxTx {
	nonce := outboxTx.Nonce
	return db.QueuedOutboxTx{
		ID:                   outboxTx.ID,
		ChainEID:             outboxTx.ChainEID,
		Purpose:              outboxTx.Purpose,
		GUID:                 bytes.Clone(outboxTx.GUID),
		To:                   outboxTx.To,
		Calldata:             bytes.Clone(outboxTx.Calldata),
		Value:                bigutil.Clone(outboxTx.Value),
		GasLimit:             outboxTx.GasLimit,
		MaxFeePerGas:         bigutil.Clone(outboxTx.MaxFeePerGas),
		MaxPriorityFeePerGas: bigutil.Clone(outboxTx.MaxPriorityFeePerGas),
		Nonce:                &nonce,
		TxHash:               outboxTx.TxHash,
		SignerID:             outboxTx.SignerID,
		Status:               db.TxStatusQueued,
		Attempts:             outboxTx.Attempts,
		FailureKind:          outboxTx.FailureKind,
		NextRetryAt:          outboxTx.NextRetryAt,
		RetryOfID:            outboxTx.RetryOfID,
	}
}

func quoteFee(ctx context.Context, queued db.QueuedOutboxTx, policy FeePolicy, client ChainClient) (feeQuote, error) {
	if policy.ConfiguredMaxFeePerGas == nil || policy.ConfiguredMaxFeePerGas.Sign() <= 0 {
		return feeQuote{}, errors.New("max fee per gas cap is required")
	}
	header, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		return feeQuote{}, err
	}
	if header == nil {
		return feeQuote{}, errors.New("latest block header is required")
	}
	if header.BaseFee == nil {
		return quoteLegacyFee(ctx, queued, policy, client)
	}
	return quoteDynamicFee(ctx, queued, policy, client, header.BaseFee)
}

func quoteLegacyFee(ctx context.Context, queued db.QueuedOutboxTx, policy FeePolicy, client ChainClient) (feeQuote, error) {
	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return feeQuote{}, err
	}
	if gasPrice == nil || gasPrice.Sign() <= 0 {
		return feeQuote{}, fmt.Errorf("outbox tx %d legacy gas price is required", queued.ID)
	}
	price := bigutil.Clone(gasPrice)
	if queued.Nonce != nil && queued.MaxFeePerGas != nil {
		if queued.MaxFeePerGas.Sign() <= 0 {
			return feeQuote{}, fmt.Errorf("outbox tx %d previous max fee per gas must be positive for replacement", queued.ID)
		}
		price = bigutil.Max(price, bumpFee(queued.MaxFeePerGas))
	}
	if price.Cmp(policy.ConfiguredMaxFeePerGas) > 0 {
		return feeQuote{}, ErrTxDeferred
	}
	return feeQuote{MaxFeePerGas: price}, nil
}

func quoteDynamicFee(ctx context.Context, queued db.QueuedOutboxTx, policy FeePolicy, client ChainClient, baseFee *big.Int) (feeQuote, error) {
	if baseFee.Sign() < 0 {
		return feeQuote{}, fmt.Errorf("latest block base fee is negative: %s", baseFee)
	}
	if policy.ConfiguredMaxPriorityFeePerGas == nil || policy.ConfiguredMaxPriorityFeePerGas.Sign() <= 0 {
		return feeQuote{}, errors.New("max priority fee per gas cap is required for dynamic-fee chains")
	}
	suggestedTip, err := client.SuggestGasTipCap(ctx)
	if err != nil {
		return feeQuote{}, err
	}
	if suggestedTip == nil || suggestedTip.Sign() <= 0 {
		return feeQuote{}, fmt.Errorf("outbox tx %d priority fee per gas is required", queued.ID)
	}
	tip := bigutil.Min(suggestedTip, policy.ConfiguredMaxPriorityFeePerGas)
	hasPreviousFee := queued.Nonce != nil && (queued.MaxFeePerGas != nil || queued.MaxPriorityFeePerGas != nil)
	if hasPreviousFee {
		if queued.MaxFeePerGas == nil || queued.MaxFeePerGas.Sign() <= 0 {
			return feeQuote{}, fmt.Errorf("outbox tx %d previous max fee per gas must be positive for replacement", queued.ID)
		}
		if queued.MaxPriorityFeePerGas == nil || queued.MaxPriorityFeePerGas.Sign() <= 0 {
			return feeQuote{}, fmt.Errorf("outbox tx %d previous priority fee per gas must be positive for replacement", queued.ID)
		}
		tip = bigutil.Max(tip, bumpFee(queued.MaxPriorityFeePerGas))
		if tip.Cmp(policy.ConfiguredMaxPriorityFeePerGas) > 0 {
			return feeQuote{}, ErrTxDeferred
		}
	}
	feeCap := new(big.Int).Mul(baseFee, big.NewInt(2))
	feeCap.Add(feeCap, tip)
	if hasPreviousFee {
		feeCap = bigutil.Max(feeCap, bumpFee(queued.MaxFeePerGas))
	}
	if feeCap.Cmp(policy.ConfiguredMaxFeePerGas) > 0 {
		return feeQuote{}, ErrTxDeferred
	}
	return feeQuote{Dynamic: true, MaxFeePerGas: feeCap, MaxPriorityFeePerGas: tip}, nil
}

func signOutboxTx(ctx context.Context, outboxTx db.OutboxTx, chainID *big.Int, gasLimit uint64, quote feeQuote, signer signer.Signer) (*types.Transaction, error) {
	return signOutboxTxWithStatuses(ctx, outboxTx, chainID, gasLimit, quote, signer, db.TxStatusNonceAssigned)
}

func signReplacementOutboxTx(ctx context.Context, outboxTx db.OutboxTx, chainID *big.Int, gasLimit uint64, quote feeQuote, signer signer.Signer) (*types.Transaction, error) {
	return signOutboxTxWithStatuses(ctx, outboxTx, chainID, gasLimit, quote, signer, db.TxStatusSigned, db.TxStatusBroadcast)
}

func signOutboxTxWithStatuses(ctx context.Context, outboxTx db.OutboxTx, chainID *big.Int, gasLimit uint64, quote feeQuote, signer signer.Signer, signableStatuses ...string) (*types.Transaction, error) {
	signable := false
	for _, status := range signableStatuses {
		if outboxTx.Status == status {
			signable = true
			break
		}
	}
	if !signable {
		return nil, fmt.Errorf("outbox tx %d status %q is not signable", outboxTx.ID, outboxTx.Status)
	}
	if gasLimit == 0 {
		return nil, fmt.Errorf("outbox tx %d gas limit is required", outboxTx.ID)
	}
	var tx *types.Transaction
	if quote.Dynamic {
		if quote.MaxFeePerGas == nil || quote.MaxFeePerGas.Sign() <= 0 {
			return nil, fmt.Errorf("outbox tx %d max fee per gas is required", outboxTx.ID)
		}
		if quote.MaxPriorityFeePerGas == nil || quote.MaxPriorityFeePerGas.Sign() <= 0 {
			return nil, fmt.Errorf("outbox tx %d max priority fee per gas is required", outboxTx.ID)
		}
		tx = types.NewTx(&types.DynamicFeeTx{
			ChainID:   chainID,
			Nonce:     outboxTx.Nonce,
			GasTipCap: quote.MaxPriorityFeePerGas,
			GasFeeCap: quote.MaxFeePerGas,
			Gas:       gasLimit,
			To:        &outboxTx.To,
			Value:     outboxTx.Value,
			Data:      outboxTx.Calldata,
		})
	} else {
		if quote.MaxFeePerGas == nil || quote.MaxFeePerGas.Sign() <= 0 {
			return nil, fmt.Errorf("outbox tx %d legacy gas price is required", outboxTx.ID)
		}
		tx = types.NewTx(&types.LegacyTx{
			Nonce:    outboxTx.Nonce,
			GasPrice: quote.MaxFeePerGas,
			Gas:      gasLimit,
			To:       &outboxTx.To,
			Value:    outboxTx.Value,
			Data:     outboxTx.Calldata,
		})
	}
	return signer.SignTx(ctx, tx, chainID)
}

func isEstimateGasRevert(err error) bool {
	if err == nil {
		return false
	}
	if dataErr, ok := errors.AsType[rpc.DataError](err); ok {
		if isRevertErrorData(dataErr.ErrorData()) {
			return true
		}
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) && rpcErr.ErrorCode() == 3 {
		return true
	}
	return errorChainContainsRevertText(err)
}

func errorChainContainsRevertText(err error) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		if containsRevertText(current.Error()) {
			return true
		}
	}
	return false
}

func isRevertErrorData(data any) bool {
	switch value := data.(type) {
	case string:
		normalized := strings.ToLower(strings.TrimSpace(value))
		return strings.HasPrefix(normalized, "0x") || containsRevertText(normalized)
	case []byte:
		return len(value) > 0
	default:
		return false
	}
}

func containsRevertText(value string) bool {
	normalized := strings.ToLower(value)
	return strings.Contains(normalized, "execution reverted") || strings.Contains(normalized, "reverted")
}

func bumpFee(value *big.Int) *big.Int {
	bumped := new(big.Int).Mul(value, big.NewInt(replacementBumpNumerator))
	bumped.Add(bumped, big.NewInt(replacementBumpDenominator-1))
	bumped.Div(bumped, big.NewInt(replacementBumpDenominator))
	return bumped
}
