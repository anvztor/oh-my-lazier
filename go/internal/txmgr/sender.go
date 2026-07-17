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
	"github.com/google/uuid"
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

func validateTarget(target Target) error {
	if target.ChainID == nil || target.ChainID.Sign() <= 0 {
		return errors.New("chain id is required")
	}
	if target.Signer == nil {
		return errors.New("target signer is required")
	}
	if target.Client == nil {
		return errors.New("target client is required")
	}
	return nil
}

// ProcessNext signs one queued or recovering outbox transaction into a durable
// signed attempt. It never broadcasts: every send goes through ProcessBroadcast,
// so a signed raw is always persisted before it can reach a node.
func (m *Manager) ProcessNext(ctx context.Context, target Target) (int64, error) {
	if err := validateTarget(target); err != nil {
		return 0, err
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
	switch queued.Status {
	case db.TxStatusQueued:
		return m.signQueuedTx(ctx, target, signerID, queued, policy)
	case db.TxStatusNonceAssigned:
		return m.recoverNonceAssignedTx(ctx, target, signerID, queued, policy)
	default:
		return 0, fmt.Errorf("outbox tx %d has unsupported sendable status %s", queued.ID, queued.Status)
	}
}

// signQueuedTx runs the RPC preflight before the nonce is assigned, so a
// deterministic estimate revert never consumes a nonce, then claims the row,
// signs, and persists the attempt.
func (m *Manager) signQueuedTx(ctx context.Context, target Target, signerID string, queued db.QueuedOutboxTx, policy FeePolicy) (int64, error) {
	quote, gasLimit, err := m.preflight(ctx, target, queued, policy, false)
	if err != nil {
		if errors.Is(err, ErrTxDeferred) {
			m.logger.Debug("deferred tx outbox row", "reason", "fee_cap", "id", queued.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", queued.Purpose)
			return 0, err
		}
		if isEstimateGasRevert(err) {
			// The compare-and-set loses against another instance that meanwhile
			// claimed or signed this row; that instance owns it now.
			applied, markErr := m.store.MarkQueuedTxEstimateRevertFailed(ctx, queued.ID, fmt.Errorf("estimate gas reverted: %w", err))
			if markErr != nil {
				return 0, markErr
			}
			if !applied {
				m.logger.Debug("skipped estimate revert for a row another instance advanced", "id", queued.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", queued.Purpose)
				return 0, fmt.Errorf("%w: outbox tx %d advanced past the queued estimate", ErrTxDeferred, queued.ID)
			}
			m.logger.Warn("failed tx gas estimate", "reason", "estimate_gas_revert", "id", queued.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", queued.Purpose, "failure_kind", db.TxFailureEstimateGasRevert, "error", err.Error())
			return queued.ID, nil
		}
		m.logger.Debug("deferred tx outbox row", "reason", "preflight_error", "id", queued.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", queued.Purpose, "error", err.Error())
		return 0, fmt.Errorf("%w: preflight for outbox tx %d: %w", ErrTxDeferred, queued.ID, err)
	}
	outboxTx, leaseToken, err := m.claimForSigning(ctx, target, signerID, queued.ID)
	if err != nil {
		return 0, err
	}
	return m.signAndPersistAttempt(ctx, target, signerID, outboxTx, leaseToken, gasLimit, quote)
}

// recoverNonceAssignedTx claims the signing lease first: the row already holds a
// nonce, so every preflight or signing failure charges the pre-sign budget
// instead of requeueing (a requeue would release the nonce and wedge the lane).
func (m *Manager) recoverNonceAssignedTx(ctx context.Context, target Target, signerID string, queued db.QueuedOutboxTx, policy FeePolicy) (int64, error) {
	outboxTx, leaseToken, err := m.claimForSigning(ctx, target, signerID, queued.ID)
	if err != nil {
		return 0, err
	}
	quote, gasLimit, err := m.preflight(ctx, target, queued, policy, false)
	if err != nil {
		return m.chargePreSignFailure(ctx, target, signerID, outboxTx, leaseToken, "preflight", err)
	}
	return m.signAndPersistAttempt(ctx, target, signerID, outboxTx, leaseToken, gasLimit, quote)
}

// preflight quotes fees and determines the gas limit inside the pre-sign RPC
// deadline. reuseGasLimit keeps the active attempt's gas limit (replacements).
func (m *Manager) preflight(ctx context.Context, target Target, queued db.QueuedOutboxTx, policy FeePolicy, reuseGasLimit bool) (feeQuote, uint64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, m.options.PreSignRPCTimeout)
	defer cancel()
	quote, err := quoteFee(rpcCtx, queued, policy, target.Client)
	if err != nil {
		return feeQuote{}, 0, err
	}
	gasLimit := queued.GasLimit
	if !reuseGasLimit || gasLimit == 0 {
		gasLimit, err = estimateGas(rpcCtx, queued, target.Signer.Address(), target.Client)
		if err != nil {
			return feeQuote{}, 0, err
		}
	}
	return quote, gasLimit, nil
}

// claimForSigning reserves the signing lease, bootstrapping the local nonce
// cursor from the RPC pending nonce on first use of a signer.
func (m *Manager) claimForSigning(ctx context.Context, target Target, signerID string, id int64) (db.OutboxTx, uuid.UUID, error) {
	leaseToken := uuid.New()
	outboxTx, err := m.store.ClaimOutboxForSigning(ctx, id, target.ChainEID, signerID, leaseToken, m.options.SigningLeaseTTL)
	if errors.Is(err, db.ErrNonceCursorMissing) {
		rpcNonce, nonceErr := target.Client.PendingNonceAt(ctx, target.Signer.Address())
		if nonceErr != nil {
			return db.OutboxTx{}, uuid.UUID{}, nonceErr
		}
		if _, nonceErr := m.store.BootstrapTxNonceCursor(ctx, target.ChainEID, signerID, rpcNonce); nonceErr != nil {
			return db.OutboxTx{}, uuid.UUID{}, nonceErr
		}
		m.logger.Info("bootstrapped tx nonce cursor", "id", id, "chain_eid", target.ChainEID, "signer", signerID, "rpc_nonce", rpcNonce)
		outboxTx, err = m.store.ClaimOutboxForSigning(ctx, id, target.ChainEID, signerID, leaseToken, m.options.SigningLeaseTTL)
	}
	if err != nil {
		return db.OutboxTx{}, uuid.UUID{}, err
	}
	m.logger.Info("claimed tx outbox row for signing", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce)
	return outboxTx, leaseToken, nil
}

// signAndPersistAttempt signs under the sign deadline, verifies the signer
// backend returned exactly the requested transaction, and persists the attempt.
func (m *Manager) signAndPersistAttempt(ctx context.Context, target Target, signerID string, outboxTx db.OutboxTx, leaseToken uuid.UUID, gasLimit uint64, quote feeQuote) (int64, error) {
	signCtx, cancel := context.WithTimeout(ctx, m.options.SignTimeout)
	signed, err := signOutboxTx(signCtx, outboxTx, target.ChainID, gasLimit, quote, target.Signer)
	cancel()
	if err != nil {
		return m.chargePreSignFailure(ctx, target, signerID, outboxTx, leaseToken, "sign", err)
	}
	attempt, err := signedAttemptFromTx(signed, target, outboxTx, gasLimit, quote, db.TxAttemptOriginal)
	if err != nil {
		return m.chargePreSignFailure(ctx, target, signerID, outboxTx, leaseToken, "verify", err)
	}
	if _, err := m.store.InsertSignedAttempt(ctx, outboxTx.ID, leaseToken, attempt); err != nil {
		return 0, err
	}
	m.logger.Info("signed tx attempt", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", attempt.TxHash, "gas_limit", gasLimit, "dynamic_fee", quote.Dynamic)
	return outboxTx.ID, nil
}

// chargePreSignFailure books one pre-sign failure against the signing lease. A
// shutdown of this process is not the lane's fault: the budget is left alone and
// the lease expires on its own.
func (m *Manager) chargePreSignFailure(ctx context.Context, target Target, signerID string, outboxTx db.OutboxTx, leaseToken uuid.UUID, stage string, cause error) (int64, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	held, err := m.store.RecordPreSignFailure(ctx, outboxTx.ID, leaseToken)
	if err != nil {
		return 0, err
	}
	m.logger.Warn("failed tx pre-sign stage", "stage", stage, "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "held", held, "error", cause.Error())
	return outboxTx.ID, nil
}

// ProcessBroadcast claims one due persisted attempt and sends its raw bytes. The
// attempt was marked ambiguous at claim time, so no send outcome can lose a
// possibly accepted transaction; the recorded class only decides the next action.
func (m *Manager) ProcessBroadcast(ctx context.Context, target Target) (int64, error) {
	if err := validateTarget(target); err != nil {
		return 0, err
	}
	signerID := target.Signer.Address().Hex()
	broadcastToken := uuid.New()
	claim, err := m.store.ClaimAttemptForBroadcast(ctx, target.ChainEID, signerID, broadcastToken, m.options.BroadcastLeaseTTL)
	if err != nil {
		return 0, err
	}
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(claim.RawTx); err != nil {
		// A corrupt persisted raw is deterministic; park the lane for review.
		if markErr := m.store.MarkAttemptSendResult(ctx, claim.AttemptID, broadcastToken, db.SendErrorDefinitive, "raw transaction decode failed"); markErr != nil && !errors.Is(markErr, db.ErrOutboxLeaseLost) {
			return 0, markErr
		}
		m.logger.Warn("failed tx raw decode before broadcast", "id", claim.OutboxID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", claim.Purpose, "nonce", claim.Nonce, "tx_hash", claim.TxHash)
		return claim.OutboxID, nil
	}
	sendCtx, cancel := context.WithTimeout(ctx, m.options.SendTimeout)
	sendErr := target.Client.SendTransaction(sendCtx, tx)
	cancel()
	class, detail := classifyBroadcastError(sendErr)
	if err := m.store.MarkAttemptSendResult(ctx, claim.AttemptID, broadcastToken, class, detail); err != nil {
		if errors.Is(err, db.ErrOutboxLeaseLost) {
			// The broadcast lease expired mid-send; the replaying owner records the outcome.
			m.logger.Warn("lost broadcast lease before recording send result", "id", claim.OutboxID, "chain_eid", target.ChainEID, "signer", signerID, "nonce", claim.Nonce, "tx_hash", claim.TxHash, "send_class", class)
			return claim.OutboxID, nil
		}
		return 0, err
	}
	// Only the canonical class and detail are logged; raw node errors can embed
	// RPC URLs or API keys.
	if class == db.SendErrorAccepted {
		m.logger.Info("broadcast tx attempt", "id", claim.OutboxID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", claim.Purpose, "nonce", claim.Nonce, "tx_hash", claim.TxHash, "kind", claim.Kind)
	} else {
		m.logger.Warn("tx broadcast not accepted", "id", claim.OutboxID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", claim.Purpose, "nonce", claim.Nonce, "tx_hash", claim.TxHash, "kind", claim.Kind, "send_class", class, "send_detail", detail)
	}
	return claim.OutboxID, nil
}

// ProcessReceipts polls every persisted attempt of the least recently polled
// non-terminal outbox rows and terminalizes the first receipt that has reached
// the chain's confirmation depth.
func (m *Manager) ProcessReceipts(ctx context.Context, target Target, limit int) (int64, error) {
	if err := validateTarget(target); err != nil {
		return 0, err
	}
	signerID := target.Signer.Address().Hex()
	tasks, err := m.store.ListReceiptPollTasks(ctx, target.ChainEID, signerID, limit)
	if err != nil {
		return 0, err
	}
	var head *types.Header
	for _, task := range tasks {
		var receipt *types.Receipt
		var winning db.TxAttempt
		for _, attempt := range task.Attempts {
			candidate, err := target.Client.TransactionReceipt(ctx, attempt.TxHash)
			if errors.Is(err, ethereum.NotFound) {
				continue
			}
			if err != nil {
				return 0, err
			}
			if candidate.TxHash != attempt.TxHash {
				return 0, fmt.Errorf("receipt tx hash %s does not match attempt tx hash %s", candidate.TxHash, attempt.TxHash)
			}
			receipt = candidate
			winning = attempt
			break
		}
		if err := m.store.TouchReceiptPoll(ctx, task.Outbox.ID); err != nil {
			return 0, err
		}
		if receipt == nil {
			continue
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
				if err := m.store.RefreshBroadcastReceiptObservedAt(ctx, task.Outbox.ID); err != nil {
					return 0, err
				}
				m.logger.Debug("deferred tx receipt below confirmation depth", "id", task.Outbox.ID, "chain_eid", target.ChainEID, "purpose", task.Outbox.Purpose, "tx_hash", winning.TxHash, "confirmations", target.Confirmations)
				continue
			}
		}
		facts, err := txReceiptFacts(receipt)
		if err != nil {
			return 0, err
		}
		// The idempotent workflow effects run first: until the atomic receipt
		// finalizer commits, the attempt stays a poll candidate, so a crash or a
		// transient error here is retried on the next pass instead of leaving the
		// outbox stuck non-terminal with an unpollable mined attempt.
		success := receipt.Status == types.ReceiptStatusSuccessful
		if err := m.applyWorkflowReceipt(ctx, task.Outbox, winning.TxHash, success); err != nil {
			return 0, err
		}
		var receiptFailure error
		if !success {
			receiptFailure = fmt.Errorf("transaction receipt status %d", receipt.Status)
		}
		if err := m.store.FinalizeAttemptReceipt(ctx, winning.ID, facts, receiptFailure); err != nil {
			return 0, err
		}
		if success {
			m.logger.Info("confirmed tx receipt", "id", task.Outbox.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", task.Outbox.Purpose, "tx_hash", winning.TxHash, "receipt_status", receipt.Status, "gas_used", facts.GasUsed, "effective_gas_price", facts.EffectiveGasPrice, "gas_cost_dst_wei", facts.GasCostDstWei)
		} else {
			m.logger.Warn("failed tx receipt", "id", task.Outbox.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", task.Outbox.Purpose, "tx_hash", winning.TxHash, "receipt_status", receipt.Status, "gas_used", facts.GasUsed, "effective_gas_price", facts.EffectiveGasPrice, "gas_cost_dst_wei", facts.GasCostDstWei, "failure_kind", db.TxFailureReceiptFailed)
		}
		return task.Outbox.ID, nil
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

// ProcessStaleBroadcastReplacement signs one same-nonce replacement attempt for a
// long-receiptless broadcast (or an operator replacement request). The signed
// replacement is persisted and switched active; ProcessBroadcast sends it.
func (m *Manager) ProcessStaleBroadcastReplacement(ctx context.Context, target Target) (int64, error) {
	if err := validateTarget(target); err != nil {
		return 0, err
	}
	signerID := target.Signer.Address().Hex()
	candidate, err := m.store.NextReplacementCandidate(ctx, target.ChainEID, signerID, m.options.StaleBroadcastReplacementAfter)
	if err != nil {
		return 0, err
	}
	outboxTx := candidate.Outbox
	// Any attempt of this row may be mined and only waiting to reach confirmation
	// depth (the receipt gate keeps the row non-terminal). Replacing it would
	// broadcast a doomed same-nonce tx, so check every persisted hash, not only
	// the active attempt.
	for _, hash := range candidate.AttemptHashes {
		receipt, receiptErr := target.Client.TransactionReceipt(ctx, hash)
		if errors.Is(receiptErr, ethereum.NotFound) {
			continue
		}
		if receiptErr != nil {
			return 0, receiptErr
		}
		if receipt != nil {
			if err := m.store.DeferReplacement(ctx, outboxTx.ID); err != nil {
				return 0, err
			}
			m.logger.Debug("skipped stale replacement for a mined tx awaiting confirmations", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", hash)
			return outboxTx.ID, nil
		}
	}
	policy, ok := target.FeePolicies[outboxTx.Purpose]
	if !ok {
		return 0, fmt.Errorf("missing fee policy for purpose %q", outboxTx.Purpose)
	}
	quote, gasLimit, err := m.preflight(ctx, target, queuedFromOutbox(outboxTx), policy, true)
	if err != nil {
		if deferErr := m.store.DeferReplacement(ctx, outboxTx.ID); deferErr != nil {
			return 0, deferErr
		}
		if errors.Is(err, ErrTxDeferred) {
			return 0, ErrTxDeferred
		}
		m.logger.Warn("failed stale broadcast replacement preflight", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "error", err.Error())
		return outboxTx.ID, nil
	}
	leaseToken := uuid.New()
	claimed, err := m.store.ClaimOutboxForReplacementSigning(ctx, outboxTx.ID, candidate.ActiveAttemptID, leaseToken, m.options.SigningLeaseTTL)
	if err != nil {
		return 0, err
	}
	signCtx, cancel := context.WithTimeout(ctx, m.options.SignTimeout)
	signed, err := signReplacementOutboxTx(signCtx, claimed, target.ChainID, gasLimit, quote, target.Signer)
	cancel()
	if err != nil {
		return m.chargePreSignFailure(ctx, target, signerID, claimed, leaseToken, "replacement_sign", err)
	}
	attempt, err := signedAttemptFromTx(signed, target, claimed, gasLimit, quote, db.TxAttemptReplacement)
	if err != nil {
		return m.chargePreSignFailure(ctx, target, signerID, claimed, leaseToken, "replacement_verify", err)
	}
	if _, err := m.store.InsertReplacementAttempt(ctx, outboxTx.ID, candidate.ActiveAttemptID, leaseToken, attempt); err != nil {
		return 0, err
	}
	m.logger.Info("signed stale tx replacement attempt", "id", outboxTx.ID, "chain_eid", target.ChainEID, "signer", signerID, "purpose", outboxTx.Purpose, "nonce", outboxTx.Nonce, "tx_hash", attempt.TxHash, "previous_tx_hash", outboxTx.TxHash)
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

// applyWorkflowReceipt applies the workflow effect of a mined receipt using the
// winning attempt's hash (a non-active attempt can win the receipt race, so the
// projected active-attempt hash must not be trusted here). Every alreadyApplied set includes
// MANUAL_REVIEW: a crash between the workflow write and the receipt finalizer
// replays this on the next pass, and by then the worker may have legally parked
// the job for operator review; the replay must not wedge on that terminal state.
func (m *Manager) applyWorkflowReceipt(ctx context.Context, outboxTx db.OutboxTx, txHash common.Hash, success bool) error {
	if len(outboxTx.GUID) != common.HashLength {
		return nil
	}
	guid := common.BytesToHash(outboxTx.GUID)
	switch outboxTx.Purpose {
	case executorCommitVerificationPurpose:
		if success {
			return m.markExecutorReceipt(ctx, guid, func() error {
				return m.store.MarkExecutorCommitted(ctx, guid, txHash)
			}, packets.ExecutorCommitted, packets.ExecutorExecutable, packets.ExecutorLzReceiveTxEnqueued, packets.ExecutorLzReceiveFailed, packets.ExecutorDelivered, packets.ExecutorManualReview)
		}
	case executorLzReceivePurpose:
		if success {
			return m.markExecutorReceipt(ctx, guid, func() error {
				return m.store.MarkExecutorDelivered(ctx, guid, txHash)
			}, packets.ExecutorDelivered, packets.ExecutorManualReview)
		}
		return m.markExecutorReceipt(ctx, guid, func() error {
			return m.store.MarkExecutorReceiveFailed(ctx, guid, txHash, "lzReceive transaction reverted")
		}, packets.ExecutorLzReceiveFailed, packets.ExecutorDelivered, packets.ExecutorManualReview)
	case dvnVerifyPurpose:
		if success {
			return m.markDVNReceipt(ctx, guid, func() error {
				return m.store.MarkDVNVerified(ctx, guid, txHash)
			}, packets.DVNVerified, packets.DVNManualReview)
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

// signedAttemptFromTx verifies the signer backend returned exactly the requested
// transaction and packages it as a durable attempt.
func signedAttemptFromTx(signed *types.Transaction, target Target, outboxTx db.OutboxTx, gasLimit uint64, quote feeQuote, kind string) (db.SignedAttempt, error) {
	if err := verifySignedOutboxTx(signed, target, outboxTx, gasLimit, quote); err != nil {
		return db.SignedAttempt{}, err
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		return db.SignedAttempt{}, fmt.Errorf("marshal signed tx: %w", err)
	}
	var priority *big.Int
	if quote.Dynamic {
		priority = quote.MaxPriorityFeePerGas
	}
	return db.SignedAttempt{
		Kind:                 kind,
		Nonce:                outboxTx.Nonce,
		TxType:               signed.Type(),
		TxHash:               signed.Hash(),
		RawTx:                raw,
		GasLimit:             gasLimit,
		MaxFeePerGas:         quote.MaxFeePerGas,
		MaxPriorityFeePerGas: priority,
		SigningToken:         uuid.New(),
	}, nil
}

// verifySignedOutboxTx guards against a signer backend (KMS, keystore) returning
// a transaction other than the one requested; the raw bytes become the durable
// broadcast source of truth, so they are checked field by field before persisting.
func verifySignedOutboxTx(signed *types.Transaction, target Target, outboxTx db.OutboxTx, gasLimit uint64, quote feeQuote) error {
	if signed == nil {
		return errors.New("signed tx is required")
	}
	sender, err := types.Sender(types.LatestSignerForChainID(target.ChainID), signed)
	if err != nil {
		return fmt.Errorf("recover signed tx sender: %w", err)
	}
	if sender != target.Signer.Address() {
		return fmt.Errorf("signed tx sender %s does not match signer %s", sender, target.Signer.Address())
	}
	if signed.Nonce() != outboxTx.Nonce {
		return fmt.Errorf("signed tx nonce %d does not match outbox nonce %d", signed.Nonce(), outboxTx.Nonce)
	}
	if signed.To() == nil || *signed.To() != outboxTx.To {
		return fmt.Errorf("signed tx destination does not match outbox tx %d", outboxTx.ID)
	}
	value := outboxTx.Value
	if value == nil {
		value = new(big.Int)
	}
	if signed.Value() == nil || signed.Value().Cmp(value) != 0 {
		return fmt.Errorf("signed tx value does not match outbox tx %d", outboxTx.ID)
	}
	if !bytes.Equal(signed.Data(), outboxTx.Calldata) {
		return fmt.Errorf("signed tx calldata does not match outbox tx %d", outboxTx.ID)
	}
	if signed.Gas() != gasLimit {
		return fmt.Errorf("signed tx gas limit %d does not match %d", signed.Gas(), gasLimit)
	}
	if signed.ChainId() == nil || signed.ChainId().Cmp(target.ChainID) != 0 {
		return fmt.Errorf("signed tx chain id %s does not match %s", signed.ChainId(), target.ChainID)
	}
	if quote.Dynamic {
		if signed.Type() != types.DynamicFeeTxType {
			return fmt.Errorf("signed tx type %d is not a dynamic-fee tx", signed.Type())
		}
		if signed.GasFeeCap().Cmp(quote.MaxFeePerGas) != 0 || signed.GasTipCap().Cmp(quote.MaxPriorityFeePerGas) != 0 {
			return fmt.Errorf("signed tx fees do not match the quote for outbox tx %d", outboxTx.ID)
		}
	} else {
		if signed.Type() != types.LegacyTxType {
			return fmt.Errorf("signed tx type %d is not a legacy tx", signed.Type())
		}
		if signed.GasPrice().Cmp(quote.MaxFeePerGas) != 0 {
			return fmt.Errorf("signed tx gas price does not match the quote for outbox tx %d", outboxTx.ID)
		}
	}
	return nil
}

func signOutboxTx(ctx context.Context, outboxTx db.OutboxTx, chainID *big.Int, gasLimit uint64, quote feeQuote, signer signer.Signer) (*types.Transaction, error) {
	return signOutboxTxWithStatuses(ctx, outboxTx, chainID, gasLimit, quote, signer, db.TxStatusNonceAssigned)
}

func signReplacementOutboxTx(ctx context.Context, outboxTx db.OutboxTx, chainID *big.Int, gasLimit uint64, quote feeQuote, signer signer.Signer) (*types.Transaction, error) {
	return signOutboxTxWithStatuses(ctx, outboxTx, chainID, gasLimit, quote, signer, db.TxStatusBroadcast, db.TxStatusHeld)
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
