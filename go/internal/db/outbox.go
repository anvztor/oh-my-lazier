package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/bigutil"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/jackc/pgx/v5"
)

const (
	// TxStatusQueued means a transaction request is waiting for nonce assignment or re-signing.
	TxStatusQueued = "queued"
	// TxStatusNonceAssigned means the tx manager has reserved a nonce for signing.
	TxStatusNonceAssigned = "nonce_assigned"
	// TxStatusSigned means the transaction was signed but not yet broadcast.
	TxStatusSigned = "signed"
	// TxStatusBroadcast means the transaction was submitted and is awaiting a receipt.
	TxStatusBroadcast = "broadcast"
	// TxStatusConfirmed means the transaction receipt succeeded.
	TxStatusConfirmed = "confirmed"
	// TxStatusFailed means the transaction needs retry or manual review.
	TxStatusFailed = "failed"
)

const maxDBNonce = uint64(1<<63 - 1)

const (
	// TxFailureEstimateGasRevert records a deterministic estimate-gas revert before nonce assignment.
	TxFailureEstimateGasRevert = "estimate_gas_revert"
	// TxFailureReceiptFailed records a mined receipt with failed status.
	TxFailureReceiptFailed = "receipt_failed"

	// TxOutboxRetryStateRetrying means a failed row is still eligible for automatic retry.
	TxOutboxRetryStateRetrying = "retrying"
	// TxOutboxRetryStateSuperseded means a failed row already has a fresh retry child or its workflow advanced past the failure.
	TxOutboxRetryStateSuperseded = "superseded"
	// TxOutboxRetryStateExhausted means a failed row requires manual intervention.
	TxOutboxRetryStateExhausted = "exhausted"

	// TxAutoRetryMaxAttempts is the maximum automatic retry count recorded in tx_outbox.attempts.
	TxAutoRetryMaxAttempts = uint32(5)
)

const (
	txAutoRetryBaseDelay = time.Minute
	txAutoRetryMaxDelay  = 30 * time.Minute
)

const txPurposeExecutorLzReceive = "executor_lz_receive"

// ErrNonceCursorMissing indicates a signer has no durable local nonce cursor yet.
var ErrNonceCursorMissing = errors.New("tx nonce cursor missing")

// ErrNoFailedTxRetry indicates no failed outbox row is due for automatic retry.
var ErrNoFailedTxRetry = errors.New("no failed tx retry")

// ErrNoStaleBroadcastReplacement indicates no pending broadcast row is due for automatic replacement.
var ErrNoStaleBroadcastReplacement = errors.New("no stale broadcast replacement")

// TxRequest describes a durable transaction request before nonce assignment.
type TxRequest struct {
	ChainEID uint32
	Purpose  string
	GUID     []byte
	To       common.Address
	Calldata []byte
	Value    *big.Int
	SignerID string
}

// OutboxTx is a transaction request after it has been persisted.
type OutboxTx struct {
	ID                       int64
	ChainEID                 uint32
	Purpose                  string
	GUID                     []byte
	To                       common.Address
	Calldata                 []byte
	Value                    *big.Int
	GasLimit                 uint64
	MaxFeePerGas             *big.Int
	MaxPriorityFeePerGas     *big.Int
	Nonce                    uint64
	TxHash                   common.Hash
	ReceiptTxHash            common.Hash
	ReceiptStatus            *uint64
	ReceiptBlockNumber       *uint64
	ReceiptGasUsed           *uint64
	ReceiptEffectiveGasPrice *big.Int
	ReceiptGasCostDstWei     *big.Int
	ReceiptGasCostSrcWei     *big.Int
	ReceiptObservedAt        *time.Time
	ReceiptCostPricedAt      *time.Time
	SignerID                 string
	Status                   string
	Attempts                 uint32
	FailureKind              string
	NextRetryAt              *time.Time
	RetryOfID                *int64
}

// QueuedOutboxTx is a queued transaction request before the tx manager decides whether to sign it.
type QueuedOutboxTx struct {
	ID                   int64
	ChainEID             uint32
	Purpose              string
	GUID                 []byte
	To                   common.Address
	Calldata             []byte
	Value                *big.Int
	GasLimit             uint64
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	Nonce                *uint64
	TxHash               common.Hash
	SignerID             string
	Status               string
	Attempts             uint32
	FailureKind          string
	NextRetryAt          *time.Time
	RetryOfID            *int64
}

// EnqueueTx inserts a transaction request into tx_outbox with queued status.
func (s *Store) EnqueueTx(ctx context.Context, request TxRequest) (int64, error) {
	if request.ChainEID == 0 {
		return 0, errors.New("chain eid is required")
	}
	if request.Purpose == "" {
		return 0, errors.New("purpose is required")
	}
	if request.To == (common.Address{}) {
		return 0, errors.New("to address is required")
	}
	if request.SignerID == "" {
		return 0, errors.New("signer id is required")
	}
	value := request.Value
	if value == nil {
		value = new(big.Int)
	}

	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO tx_outbox (
			chain_eid, purpose, guid, to_address, calldata, value,
			signer_id, status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`, request.ChainEID, request.Purpose, optionalBytes(request.GUID), addressBytes(request.To), request.Calldata, value.String(), request.SignerID, TxStatusQueued).Scan(&id)
	return id, err
}

// PeekQueuedTx returns the next queued outbox row for one chain signer without reserving a nonce.
func (s *Store) PeekQueuedTx(ctx context.Context, chainEID uint32, signerID string) (QueuedOutboxTx, error) {
	return s.peekSendableTx(ctx, chainEID, signerID, []string{TxStatusQueued})
}

// PeekSendableTx returns the next queued or nonce-assigned outbox row that can be signed or re-signed.
func (s *Store) PeekSendableTx(ctx context.Context, chainEID uint32, signerID string) (QueuedOutboxTx, error) {
	return s.peekSendableTx(ctx, chainEID, signerID, []string{TxStatusQueued, TxStatusNonceAssigned})
}

func (s *Store) peekSendableTx(ctx context.Context, chainEID uint32, signerID string, statuses []string) (QueuedOutboxTx, error) {
	if chainEID == 0 {
		return QueuedOutboxTx{}, errors.New("chain eid is required")
	}
	if signerID == "" {
		return QueuedOutboxTx{}, errors.New("signer id is required")
	}
	if len(statuses) == 0 {
		return QueuedOutboxTx{}, errors.New("tx statuses are required")
	}
	var row outboxTxRow
	err := s.pool.QueryRow(ctx, `
		SELECT
			id, chain_eid, purpose, guid, to_address, calldata, value::text,
			gas_limit::text, max_fee_per_gas::text, max_priority_fee_per_gas::text,
			nonce, tx_hash, signer_id, status, attempts,
			failure_kind, next_retry_at, retry_of_id,
			receipt_tx_hash, receipt_status::text, receipt_block_number::text,
			receipt_gas_used::text, receipt_effective_gas_price::text,
			receipt_gas_cost_dst_wei::text, receipt_gas_cost_src_wei::text,
			receipt_observed_at, receipt_cost_priced_at
		FROM tx_outbox
		WHERE chain_eid = $1 AND signer_id = $2 AND status = ANY($3)
			AND (next_sign_at IS NULL OR next_sign_at <= now())
			AND (lease_until IS NULL OR lease_until <= now())
		ORDER BY CASE WHEN status = 'nonce_assigned' THEN 0 ELSE 1 END, nonce ASC NULLS LAST, id
		LIMIT 1
	`, chainEID, signerID, statuses).Scan(
		&row.ID,
		&row.ChainEID,
		&row.Purpose,
		&row.GUID,
		&row.ToAddress,
		&row.Calldata,
		&row.Value,
		&row.GasLimit,
		&row.MaxFeePerGas,
		&row.MaxPriorityFeePerGas,
		&row.Nonce,
		&row.TxHash,
		&row.SignerID,
		&row.Status,
		&row.Attempts,
		&row.FailureKind,
		&row.NextRetryAt,
		&row.RetryOfID,
		&row.ReceiptTxHash,
		&row.ReceiptStatus,
		&row.ReceiptBlockNumber,
		&row.ReceiptGasUsed,
		&row.ReceiptEffectiveGasPrice,
		&row.ReceiptGasCostDstWei,
		&row.ReceiptGasCostSrcWei,
		&row.ReceiptObservedAt,
		&row.ReceiptCostPricedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return QueuedOutboxTx{}, pgx.ErrNoRows
	}
	if err != nil {
		return QueuedOutboxTx{}, err
	}
	return row.toQueuedOutboxTx()
}

// BootstrapTxNonceCursor inserts a local signer nonce cursor when one does not exist.
//
// This is the only tx manager boundary that accepts an RPC nonce. Existing
// cursors are never updated from RPC; first-use bootstrap chooses the greater
// of the RPC pending nonce and all locally recorded outbox nonces plus one.
func (s *Store) BootstrapTxNonceCursor(ctx context.Context, chainEID uint32, signerID string, rpcPendingNonce uint64) (bool, error) {
	if chainEID == 0 {
		return false, errors.New("chain eid is required")
	}
	if signerID == "" {
		return false, errors.New("signer id is required")
	}
	if rpcPendingNonce > maxDBNonce {
		return false, fmt.Errorf("rpc pending nonce %d exceeds database nonce limit", rpcPendingNonce)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockSignerNonce(ctx, tx, chainEID, signerID); err != nil {
		return false, err
	}
	localNext, err := s.localNextNonce(ctx, tx, chainEID, signerID)
	if err != nil {
		return false, err
	}
	nextNonce := max(localNext, rpcPendingNonce)
	tag, err := tx.Exec(ctx, `
		INSERT INTO tx_nonce_cursors (chain_eid, signer_id, next_nonce)
		VALUES ($1, $2, $3)
		ON CONFLICT (chain_eid, signer_id) DO NOTHING
	`, chainEID, signerID, int64(nextNonce))
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// GetOutboxTx returns one persisted transaction request.
func (s *Store) GetOutboxTx(ctx context.Context, id int64) (OutboxTx, error) {
	var row outboxTxRow
	err := s.pool.QueryRow(ctx, `
		SELECT
			id, chain_eid, purpose, guid, to_address, calldata, value::text,
			gas_limit::text, max_fee_per_gas::text, max_priority_fee_per_gas::text,
			nonce, tx_hash, signer_id, status, attempts,
			failure_kind, next_retry_at, retry_of_id,
			receipt_tx_hash, receipt_status::text, receipt_block_number::text,
			receipt_gas_used::text, receipt_effective_gas_price::text,
			receipt_gas_cost_dst_wei::text, receipt_gas_cost_src_wei::text,
			receipt_observed_at, receipt_cost_priced_at
		FROM tx_outbox
		WHERE id = $1
	`, id).Scan(
		&row.ID,
		&row.ChainEID,
		&row.Purpose,
		&row.GUID,
		&row.ToAddress,
		&row.Calldata,
		&row.Value,
		&row.GasLimit,
		&row.MaxFeePerGas,
		&row.MaxPriorityFeePerGas,
		&row.Nonce,
		&row.TxHash,
		&row.SignerID,
		&row.Status,
		&row.Attempts,
		&row.FailureKind,
		&row.NextRetryAt,
		&row.RetryOfID,
		&row.ReceiptTxHash,
		&row.ReceiptStatus,
		&row.ReceiptBlockNumber,
		&row.ReceiptGasUsed,
		&row.ReceiptEffectiveGasPrice,
		&row.ReceiptGasCostDstWei,
		&row.ReceiptGasCostSrcWei,
		&row.ReceiptObservedAt,
		&row.ReceiptCostPricedAt,
	)
	if err != nil {
		return OutboxTx{}, err
	}
	return row.toOutboxTx()
}

// RefreshBroadcastReceiptObservedAt bumps updated_at for a broadcast row whose
// receipt has been observed but is not yet buried under the required
// confirmation depth, so the stale-broadcast replacement mechanism does not
// mistake a mined-but-shallow transaction for one stuck in the mempool and
// replace it.
func (s *Store) RefreshBroadcastReceiptObservedAt(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET updated_at = now()
		WHERE id = $1 AND status = ANY($2)
	`, id, []string{TxStatusSigned, TxStatusBroadcast})
	return err
}

// MarkQueuedTxEstimateRevertFailed records a deterministic estimate-gas revert
// for a pristine queued row. The compare-and-set only touches rows no other
// instance has advanced (no nonce, no attempt, no live signing lease), so a
// concurrent claim-sign-broadcast can never be overwritten into failed. Losing
// the race reports applied=false and is not an error.
func (s *Store) MarkQueuedTxEstimateRevertFailed(ctx context.Context, id int64, failure error) (bool, error) {
	if id <= 0 {
		return false, errors.New("outbox tx id is required")
	}
	message := ""
	if failure != nil {
		message = failure.Error()
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var attempts uint32
	if err := tx.QueryRow(ctx, `
		SELECT attempts
		FROM tx_outbox
		WHERE id = $1 AND status = $2
			AND nonce IS NULL AND active_attempt_id IS NULL
			AND (lease_until IS NULL OR lease_until <= now())
		FOR UPDATE
	`, id, TxStatusQueued).Scan(&attempts); errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var retryAt any
	if attempts < TxAutoRetryMaxAttempts {
		retryAt = time.Now().UTC().Add(autoRetryDelay(attempts))
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET
			status = $1,
			failure_kind = $2,
			next_retry_at = $3,
			gas_limit = NULL,
			max_fee_per_gas = NULL,
			max_priority_fee_per_gas = NULL,
			last_error = $4,
			updated_at = now()
		WHERE id = $5
	`, TxStatusFailed, TxFailureEstimateGasRevert, retryAt, optionalString(message), id); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// RetryFailedTx returns a failed transaction request to the queue.
//
// Receipt-failed rows are cloned to preserve the mined failed nonce evidence.
// Rows that never reached a receipt are requeued in place, preserving any
// assigned nonce so the signer cannot create a local nonce gap.
func (s *Store) RetryFailedTx(ctx context.Context, id int64) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var row struct {
		Purpose     string
		GUID        *[]byte
		Nonce       *int64
		FailureKind string
	}
	if err := tx.QueryRow(ctx, `
		SELECT purpose, guid, nonce, COALESCE(failure_kind, '')
		FROM tx_outbox
		WHERE id = $1 AND status = $2
		FOR UPDATE
	`, id, TxStatusFailed).Scan(&row.Purpose, &row.GUID, &row.Nonce, &row.FailureKind); errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("outbox tx %d is not failed", id)
	} else if err != nil {
		return 0, err
	}

	if row.Nonce == nil || row.FailureKind != TxFailureReceiptFailed {
		if err := requeueFailedTx(ctx, tx, id, true); err != nil {
			return 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return id, nil
	}
	if *row.Nonce < 0 {
		return 0, fmt.Errorf("negative nonce for outbox tx %d", id)
	}

	if retryPrepared, err := prepareReceiptRetryWorkflow(ctx, tx, id, row.Purpose, row.GUID); err != nil {
		return 0, err
	} else if !retryPrepared {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, ErrNoFailedTxRetry
	}
	retryID, err := cloneFailedTxRetry(ctx, tx, id)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return retryID, nil
}

// PrepareNextFailedTxRetry promotes one due failed row for automatic retry.
func (s *Store) PrepareNextFailedTxRetry(ctx context.Context, chainEID uint32, signerID string) (int64, error) {
	if chainEID == 0 {
		return 0, errors.New("chain eid is required")
	}
	if signerID == "" {
		return 0, errors.New("signer id is required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var row struct {
		ID          int64
		Purpose     string
		GUID        *[]byte
		Nonce       *int64
		FailureKind string
	}
	err = tx.QueryRow(ctx, `
		SELECT id, purpose, guid, nonce, failure_kind
		FROM tx_outbox failed
		WHERE chain_eid = $1
			AND signer_id = $2
			AND status = $3
			AND attempts < $4
			AND next_retry_at IS NOT NULL
			AND next_retry_at <= now()
			AND failure_kind IN ($5, $6)
			AND NOT EXISTS (
				SELECT 1
				FROM tx_outbox child
				WHERE child.retry_of_id = failed.id
			)
		ORDER BY next_retry_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, chainEID, signerID, TxStatusFailed, TxAutoRetryMaxAttempts, TxFailureEstimateGasRevert, TxFailureReceiptFailed).Scan(
		&row.ID,
		&row.Purpose,
		&row.GUID,
		&row.Nonce,
		&row.FailureKind,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNoFailedTxRetry
	}
	if err != nil {
		return 0, err
	}

	var retryID int64
	switch {
	case row.Nonce == nil || row.FailureKind == TxFailureEstimateGasRevert:
		if err := requeueFailedTx(ctx, tx, row.ID, true); err != nil {
			return 0, err
		}
		retryID = row.ID
	case row.FailureKind == TxFailureReceiptFailed:
		if retryPrepared, err := prepareReceiptRetryWorkflow(ctx, tx, row.ID, row.Purpose, row.GUID); err != nil {
			return 0, err
		} else if !retryPrepared {
			if err := tx.Commit(ctx); err != nil {
				return 0, err
			}
			return 0, ErrNoFailedTxRetry
		}
		retryID, err = cloneFailedTxRetry(ctx, tx, row.ID)
		if err != nil {
			return 0, err
		}
	default:
		return 0, ErrNoFailedTxRetry
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return retryID, nil
}

func requeueFailedTx(ctx context.Context, tx pgx.Tx, id int64, clearGas bool) error {
	tag, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET
			status = $1,
			tx_hash = NULL,
			gas_limit = CASE WHEN $2 THEN NULL ELSE gas_limit END,
			max_fee_per_gas = CASE WHEN $2 THEN NULL ELSE max_fee_per_gas END,
			max_priority_fee_per_gas = CASE WHEN $2 THEN NULL ELSE max_priority_fee_per_gas END,
			attempts = attempts + 1,
			failure_kind = NULL,
			next_retry_at = NULL,
			last_error = NULL,
			updated_at = now()
		WHERE id = $3 AND status = $4
	`, TxStatusQueued, clearGas, id, TxStatusFailed)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox tx %d is not failed", id)
	}
	return nil
}

func cloneFailedTxRetry(ctx context.Context, tx pgx.Tx, id int64) (int64, error) {
	var retryID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO tx_outbox (
			chain_eid, purpose, guid, to_address, calldata, value,
			signer_id, status, attempts, retry_of_id
		)
		SELECT
			chain_eid, purpose, guid, to_address, calldata, value,
			signer_id, $1, attempts + 1, id
		FROM tx_outbox
		WHERE id = $2
			AND status = $3
			AND nonce IS NOT NULL
			AND NOT EXISTS (
				SELECT 1
				FROM tx_outbox child
				WHERE child.retry_of_id = tx_outbox.id
			)
		RETURNING id
	`, TxStatusQueued, id, TxStatusFailed).Scan(&retryID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("outbox tx %d is not a cloneable failed row", id)
		}
		return 0, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET next_retry_at = NULL, updated_at = now()
		WHERE id = $1
	`, id); err != nil {
		return 0, err
	}
	return retryID, nil
}

// lzReceiveJobPathwayActive reports whether the packet's pathway is enabled and
// not paused and both endpoint chains are enabled and not paused. It mirrors the
// work-selection gate so the receipt-retry re-activation path honors the same
// pause/disable semantics.
func lzReceiveJobPathwayActive(ctx context.Context, tx pgx.Tx, guidBytes []byte) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM packets p
			WHERE p.guid = $1
				AND EXISTS (
					SELECT 1 FROM pathways pw
					WHERE pw.src_eid = p.src_eid AND pw.dst_eid = p.dst_eid
						AND pw.src_oapp = p.sender AND pw.dst_oapp = p.receiver
						AND pw.enabled AND NOT pw.paused
				)
				AND NOT EXISTS (
					SELECT 1 FROM chains c
					WHERE c.eid IN (p.src_eid, p.dst_eid) AND (NOT c.enabled OR c.paused)
				)
		)
	`, guidBytes).Scan(&active)
	return active, err
}

func prepareReceiptRetryWorkflow(ctx context.Context, tx pgx.Tx, failedTxID int64, purpose string, guidBytes *[]byte) (bool, error) {
	if purpose != txPurposeExecutorLzReceive || guidBytes == nil {
		return true, nil
	}
	if len(*guidBytes) != common.HashLength {
		return false, fmt.Errorf("executor lzReceive retry guid has length %d", len(*guidBytes))
	}
	var status string
	var retryCount int64
	err := tx.QueryRow(ctx, `
		SELECT status, retry_count
		FROM executor_jobs
		WHERE guid = $1
		FOR UPDATE
	`, *guidBytes).Scan(&status, &retryCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("executor job %s not found for lzReceive retry", common.BytesToHash(*guidBytes))
	}
	if err != nil {
		return false, err
	}
	switch status {
	case string(packets.ExecutorLzReceiveFailed):
	case string(packets.ExecutorLzReceiveTxEnqueued), string(packets.ExecutorDelivered), string(packets.ExecutorManualReview):
		// The job was already advanced or parked (e.g. the deliverer re-enqueued
		// or the retry budget was exhausted). Finalize this failed row so the
		// txmgr auto-retry loop stops re-selecting it and wedging the signer.
		if _, err := tx.Exec(ctx, `
			UPDATE tx_outbox
			SET failure_kind = NULL, next_retry_at = NULL, updated_at = now()
			WHERE id = $1 AND status = $2
		`, failedTxID, TxStatusFailed); err != nil {
			return false, err
		}
		return false, nil
	default:
		return false, fmt.Errorf("executor job %s is in status %s, want %s", common.BytesToHash(*guidBytes), status, packets.ExecutorLzReceiveFailed)
	}
	// Enforce the retry budget atomically with the restore decision so that
	// whichever driver wins the FOR UPDATE lock cannot exceed the cap.
	if retryCount >= MaxLzReceiveDeliveryAttempts {
		reason := fmt.Sprintf("lzReceive reverted %d times, exceeding the %d-attempt retry budget", retryCount, MaxLzReceiveDeliveryAttempts)
		if _, err := tx.Exec(ctx, `
			UPDATE executor_jobs
			SET status = $1, last_error = $2, updated_at = now()
			WHERE guid = $3 AND status = $4
		`, string(packets.ExecutorManualReview), reason, *guidBytes, string(packets.ExecutorLzReceiveFailed)); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE packets
			SET status = $1, updated_at = now()
			WHERE guid = $2 AND status = $3
		`, string(packets.ExecutorManualReview), *guidBytes, string(packets.ExecutorLzReceiveFailed)); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tx_outbox
			SET failure_kind = NULL, next_retry_at = NULL, updated_at = now()
			WHERE id = $1 AND status = $2
		`, failedTxID, TxStatusFailed); err != nil {
			return false, err
		}
		return false, nil
	}
	// Do not re-activate a job whose pathway or chain was paused or removed from
	// config after the failure; that would broadcast a fresh paid tx on a halted
	// pathway. Finalize the failed row and leave the job LZ_RECEIVE_FAILED so the
	// deliverer resumes it once the pathway is active again (the retry_count cap
	// carries across the pause).
	active, err := lzReceiveJobPathwayActive(ctx, tx, *guidBytes)
	if err != nil {
		return false, err
	}
	if !active {
		if _, err := tx.Exec(ctx, `
			UPDATE tx_outbox
			SET failure_kind = NULL, next_retry_at = NULL, updated_at = now()
			WHERE id = $1 AND status = $2
		`, failedTxID, TxStatusFailed); err != nil {
			return false, err
		}
		return false, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE executor_jobs
		SET status = $1, last_error = NULL, updated_at = now()
		WHERE guid = $2 AND status = $3
	`, string(packets.ExecutorLzReceiveTxEnqueued), *guidBytes, string(packets.ExecutorLzReceiveFailed))
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("executor job %s is not in status %s", common.BytesToHash(*guidBytes), packets.ExecutorLzReceiveFailed)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE packets
		SET status = $1, updated_at = now()
		WHERE guid = $2 AND status = $3
	`, string(packets.ExecutorLzReceiveTxEnqueued), *guidBytes, string(packets.ExecutorLzReceiveFailed)); err != nil {
		return false, err
	}
	return true, nil
}

func lockSignerNonce(ctx context.Context, tx pgx.Tx, chainEID uint32, signerID string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::integer, hashtext($2)::integer)", int32(chainEID), signerID)
	return err
}

func (s *Store) localNextNonce(ctx context.Context, tx pgx.Tx, chainEID uint32, signerID string) (uint64, error) {
	var dbMax *int64
	if err := tx.QueryRow(ctx, `
		SELECT max(nonce)::bigint
		FROM tx_outbox
		WHERE chain_eid = $1 AND signer_id = $2 AND nonce IS NOT NULL
	`, chainEID, signerID).Scan(&dbMax); err != nil {
		return 0, err
	}
	if dbMax == nil {
		return 0, nil
	}
	if *dbMax < 0 {
		return 0, fmt.Errorf("negative nonce for chain %d signer %s", chainEID, signerID)
	}
	if uint64(*dbMax) >= maxDBNonce {
		return 0, fmt.Errorf("nonce overflow for chain %d signer %s", chainEID, signerID)
	}
	return uint64(*dbMax) + 1, nil
}

func (s *Store) claimCursorNonce(ctx context.Context, tx pgx.Tx, chainEID uint32, signerID string) (uint64, error) {
	var dbNext int64
	err := tx.QueryRow(ctx, `
		SELECT next_nonce
		FROM tx_nonce_cursors
		WHERE chain_eid = $1 AND signer_id = $2
		FOR UPDATE
	`, chainEID, signerID).Scan(&dbNext)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNonceCursorMissing
	}
	if err != nil {
		return 0, err
	}
	if dbNext < 0 {
		return 0, fmt.Errorf("negative nonce cursor for chain %d signer %s", chainEID, signerID)
	}
	if uint64(dbNext) >= maxDBNonce {
		return 0, fmt.Errorf("nonce cursor overflow for chain %d signer %s", chainEID, signerID)
	}
	nextNonce := uint64(dbNext)
	if _, err := tx.Exec(ctx, `
		UPDATE tx_nonce_cursors
		SET next_nonce = $1, updated_at = now()
		WHERE chain_eid = $2 AND signer_id = $3
	`, int64(nextNonce+1), chainEID, signerID); err != nil {
		return 0, err
	}
	return nextNonce, nil
}

func optionalString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func optionalBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	copied := make([]byte, len(value))
	copy(copied, value)
	return copied
}

type outboxTxRow struct {
	ID                       int64
	ChainEID                 uint32
	Purpose                  string
	GUID                     *[]byte
	ToAddress                []byte
	Calldata                 []byte
	Value                    string
	GasLimit                 *string
	MaxFeePerGas             *string
	MaxPriorityFeePerGas     *string
	Nonce                    *int64
	TxHash                   *[]byte
	SignerID                 string
	Status                   string
	Attempts                 uint32
	FailureKind              *string
	NextRetryAt              *time.Time
	RetryOfID                *int64
	ReceiptTxHash            *[]byte
	ReceiptStatus            *string
	ReceiptBlockNumber       *string
	ReceiptGasUsed           *string
	ReceiptEffectiveGasPrice *string
	ReceiptGasCostDstWei     *string
	ReceiptGasCostSrcWei     *string
	ReceiptObservedAt        *time.Time
	ReceiptCostPricedAt      *time.Time
}

func (r outboxTxRow) toOutboxTx() (OutboxTx, error) {
	queued, err := r.toQueuedOutboxTx()
	if err != nil {
		return OutboxTx{}, err
	}
	nonce := uint64(0)
	if queued.Nonce != nil {
		nonce = *queued.Nonce
	}
	receiptTxHash, err := parseOptionalHash("receipt_tx_hash", r.ReceiptTxHash)
	if err != nil {
		return OutboxTx{}, err
	}
	receiptStatus, err := parseOptionalUint64("receipt_status", r.ReceiptStatus)
	if err != nil {
		return OutboxTx{}, err
	}
	receiptBlockNumber, err := parseOptionalUint64("receipt_block_number", r.ReceiptBlockNumber)
	if err != nil {
		return OutboxTx{}, err
	}
	receiptGasUsed, err := parseOptionalUint64("receipt_gas_used", r.ReceiptGasUsed)
	if err != nil {
		return OutboxTx{}, err
	}
	receiptEffectiveGasPrice, err := bigutil.ParseOptionalDecimal("receipt_effective_gas_price", r.ReceiptEffectiveGasPrice)
	if err != nil {
		return OutboxTx{}, err
	}
	receiptGasCostDstWei, err := bigutil.ParseOptionalDecimal("receipt_gas_cost_dst_wei", r.ReceiptGasCostDstWei)
	if err != nil {
		return OutboxTx{}, err
	}
	receiptGasCostSrcWei, err := bigutil.ParseOptionalDecimal("receipt_gas_cost_src_wei", r.ReceiptGasCostSrcWei)
	if err != nil {
		return OutboxTx{}, err
	}
	return OutboxTx{
		ID:                       queued.ID,
		ChainEID:                 queued.ChainEID,
		Purpose:                  queued.Purpose,
		GUID:                     queued.GUID,
		To:                       queued.To,
		Calldata:                 queued.Calldata,
		Value:                    queued.Value,
		GasLimit:                 queued.GasLimit,
		MaxFeePerGas:             queued.MaxFeePerGas,
		MaxPriorityFeePerGas:     queued.MaxPriorityFeePerGas,
		Nonce:                    nonce,
		TxHash:                   queued.TxHash,
		ReceiptTxHash:            receiptTxHash,
		ReceiptStatus:            receiptStatus,
		ReceiptBlockNumber:       receiptBlockNumber,
		ReceiptGasUsed:           receiptGasUsed,
		ReceiptEffectiveGasPrice: receiptEffectiveGasPrice,
		ReceiptGasCostDstWei:     receiptGasCostDstWei,
		ReceiptGasCostSrcWei:     receiptGasCostSrcWei,
		ReceiptObservedAt:        cloneOptionalTime(r.ReceiptObservedAt),
		ReceiptCostPricedAt:      cloneOptionalTime(r.ReceiptCostPricedAt),
		SignerID:                 queued.SignerID,
		Status:                   queued.Status,
		Attempts:                 queued.Attempts,
		FailureKind:              queued.FailureKind,
		NextRetryAt:              cloneOptionalTime(queued.NextRetryAt),
		RetryOfID:                cloneOptionalInt64(queued.RetryOfID),
	}, nil
}

func (r outboxTxRow) toQueuedOutboxTx() (QueuedOutboxTx, error) {
	value, err := bigutil.ParseRequiredDecimal("value", &r.Value)
	if err != nil {
		return QueuedOutboxTx{}, err
	}
	maxFeePerGas, err := bigutil.ParseOptionalDecimal("max_fee_per_gas", r.MaxFeePerGas)
	if err != nil {
		return QueuedOutboxTx{}, err
	}
	maxPriorityFeePerGas, err := bigutil.ParseOptionalDecimal("max_priority_fee_per_gas", r.MaxPriorityFeePerGas)
	if err != nil {
		return QueuedOutboxTx{}, err
	}
	gasLimit, err := parseUint64("gas_limit", r.GasLimit)
	if err != nil {
		return QueuedOutboxTx{}, err
	}
	var nonce *uint64
	if r.Nonce != nil {
		if *r.Nonce < 0 {
			return QueuedOutboxTx{}, fmt.Errorf("outbox tx nonce is negative: %d", *r.Nonce)
		}
		parsedNonce := uint64(*r.Nonce)
		nonce = &parsedNonce
	}
	if len(r.ToAddress) != common.AddressLength {
		return QueuedOutboxTx{}, fmt.Errorf("outbox tx to_address has length %d", len(r.ToAddress))
	}
	var txHash common.Hash
	if r.TxHash != nil {
		if len(*r.TxHash) != common.HashLength {
			return QueuedOutboxTx{}, fmt.Errorf("outbox tx tx_hash has length %d", len(*r.TxHash))
		}
		txHash = common.BytesToHash(*r.TxHash)
	}
	return QueuedOutboxTx{
		ID:                   r.ID,
		ChainEID:             r.ChainEID,
		Purpose:              r.Purpose,
		GUID:                 cloneOptionalBytes(r.GUID),
		To:                   common.BytesToAddress(r.ToAddress),
		Calldata:             bytes.Clone(r.Calldata),
		Value:                value,
		GasLimit:             gasLimit,
		MaxFeePerGas:         maxFeePerGas,
		MaxPriorityFeePerGas: maxPriorityFeePerGas,
		Nonce:                nonce,
		TxHash:               txHash,
		SignerID:             r.SignerID,
		Status:               r.Status,
		Attempts:             r.Attempts,
		FailureKind:          optionalStringValue(r.FailureKind),
		NextRetryAt:          cloneOptionalTime(r.NextRetryAt),
		RetryOfID:            cloneOptionalInt64(r.RetryOfID),
	}, nil
}

func parseUint64(field string, value *string) (uint64, error) {
	if value == nil {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(*value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is not a valid uint64: %w", field, err)
	}
	return parsed, nil
}

func parseOptionalUint64(field string, value *string) (*uint64, error) {
	if value == nil {
		return nil, nil
	}
	parsed, err := strconv.ParseUint(*value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid uint64: %w", field, err)
	}
	return &parsed, nil
}

func parseOptionalHash(field string, value *[]byte) (common.Hash, error) {
	if value == nil {
		return common.Hash{}, nil
	}
	if len(*value) != common.HashLength {
		return common.Hash{}, fmt.Errorf("%s has length %d", field, len(*value))
	}
	return common.BytesToHash(*value), nil
}

func cloneOptionalBytes(value *[]byte) []byte {
	if value == nil {
		return nil
	}
	return bytes.Clone(*value)
}

func optionalStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func cloneOptionalTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneOptionalInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func autoRetryDelay(attempts uint32) time.Duration {
	delay := txAutoRetryBaseDelay
	for range attempts {
		if delay >= txAutoRetryMaxDelay/2 {
			return txAutoRetryMaxDelay
		}
		delay *= 2
	}
	if delay > txAutoRetryMaxDelay {
		return txAutoRetryMaxDelay
	}
	return delay
}

func pgInterval(duration time.Duration) string {
	return strconv.FormatInt(int64(duration/time.Microsecond), 10) + " microseconds"
}
