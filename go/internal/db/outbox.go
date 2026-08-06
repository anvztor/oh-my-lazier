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
	CancelRequestedAt        *time.Time
	ReceiptOutcome           string
	ReceiptAttemptID         int64
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

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Every purpose is gated at enqueue so a pause/disable committed before this
	// insert never accepts new work, and a purpose outside the closed scope
	// mapping is rejected here instead of lingering as a queued row the selector
	// permanently filters out. The signing gate remains the final safety
	// boundary for rows that slip in before a pause commits.
	if err := lockTxSendScope(ctx, tx, request.ChainEID, request.Purpose, request.GUID); err != nil {
		return 0, err
	}
	var id int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO tx_outbox (
			chain_eid, purpose, guid, to_address, calldata, value,
			signer_id, status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`, request.ChainEID, request.Purpose, optionalBytes(request.GUID), addressBytes(request.To), request.Calldata, value.String(), request.SignerID, TxStatusQueued).Scan(&id); err != nil {
		return 0, err
	}
	return id, tx.Commit(ctx)
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
			o.id, o.chain_eid, o.purpose, o.guid, o.to_address, o.calldata, o.value::text,
			a.gas_limit::text, a.max_fee_per_gas::text, a.max_priority_fee_per_gas::text,
			o.nonce, a.tx_hash, o.signer_id, o.status, o.attempts,
			o.failure_kind, o.next_retry_at, o.retry_of_id,
			o.receipt_tx_hash, o.receipt_status::text, o.receipt_block_number::text,
			o.receipt_gas_used::text, o.receipt_effective_gas_price::text,
			o.receipt_gas_cost_dst_wei::text, o.receipt_gas_cost_src_wei::text,
			o.receipt_observed_at, o.receipt_cost_priced_at, o.cancel_requested_at,
			o.receipt_outcome, o.receipt_attempt_id
		FROM tx_outbox o
		LEFT JOIN tx_attempts a ON a.outbox_id = o.id AND a.id = o.active_attempt_id
		WHERE o.chain_eid = $1 AND o.signer_id = $2 AND o.status = ANY($3)
			AND (o.next_sign_at IS NULL OR o.next_sign_at <= now())
			AND (o.lease_until IS NULL OR o.lease_until <= now())
			AND o.cancel_requested_at IS NULL
			-- A pinned receipt resolution means the nonce is consumed; such a
			-- row must never be selected for signing again.
			AND o.receipt_outcome IS NULL
			-- Rows without a nonce whose send scope is paused/disabled are held
			-- back here so they cannot starve active work behind them; the
			-- signing gate re-checks the scope under share locks.
			AND `+txSendScopeActiveSQL+`
		ORDER BY CASE WHEN o.status = 'nonce_assigned' THEN 0 ELSE 1 END, o.nonce ASC NULLS LAST, o.id
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
		&row.CancelRequestedAt,
		&row.ReceiptOutcome,
		&row.ReceiptAttemptID,
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

// GetOutboxTx returns one persisted transaction request. The current hash, gas,
// and fees are projected from the active attempt (the single source of truth).
func (s *Store) GetOutboxTx(ctx context.Context, id int64) (OutboxTx, error) {
	var row outboxTxRow
	err := s.pool.QueryRow(ctx, `
		SELECT
			o.id, o.chain_eid, o.purpose, o.guid, o.to_address, o.calldata, o.value::text,
			a.gas_limit::text, a.max_fee_per_gas::text, a.max_priority_fee_per_gas::text,
			o.nonce, a.tx_hash, o.signer_id, o.status, o.attempts,
			o.failure_kind, o.next_retry_at, o.retry_of_id,
			o.receipt_tx_hash, o.receipt_status::text, o.receipt_block_number::text,
			o.receipt_gas_used::text, o.receipt_effective_gas_price::text,
			o.receipt_gas_cost_dst_wei::text, o.receipt_gas_cost_src_wei::text,
			o.receipt_observed_at, o.receipt_cost_priced_at, o.cancel_requested_at,
			o.receipt_outcome, o.receipt_attempt_id
		FROM tx_outbox o
		LEFT JOIN tx_attempts a ON a.outbox_id = o.id AND a.id = o.active_attempt_id
		WHERE o.id = $1
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
		&row.CancelRequestedAt,
		&row.ReceiptOutcome,
		&row.ReceiptAttemptID,
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
	var purpose string
	if err := tx.QueryRow(ctx, `
		SELECT attempts, purpose
		FROM tx_outbox
		WHERE id = $1 AND status = $2
			AND nonce IS NULL AND active_attempt_id IS NULL
			AND (lease_until IS NULL OR lease_until <= now())
		FOR UPDATE
	`, id, TxStatusQueued).Scan(&attempts, &purpose); errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	var retryAt any
	// A failed pricing row is terminal (its calldata carries a time-bound
	// observation the retry paths refuse), so it never gets a retry window.
	if attempts < TxAutoRetryMaxAttempts && purpose != TxPurposePricingSetPriceSnapshot {
		retryAt = time.Now().UTC().Add(autoRetryDelay(attempts))
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET
			status = $1,
			failure_kind = $2,
			next_retry_at = $3,
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
		ChainEID    uint32
		Purpose     string
		GUID        *[]byte
		Nonce       *int64
		FailureKind string
	}
	var receiptOutcome *string
	var toAddress []byte
	var rowSignerID string
	if err := tx.QueryRow(ctx, `
		SELECT chain_eid, purpose, guid, nonce, COALESCE(failure_kind, ''), receipt_outcome, to_address, signer_id
		FROM tx_outbox
		WHERE id = $1 AND status = $2
		FOR UPDATE
	`, id, TxStatusFailed).Scan(&row.ChainEID, &row.Purpose, &row.GUID, &row.Nonce, &row.FailureKind, &receiptOutcome, &toAddress, &rowSignerID); errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("outbox tx %d is not failed", id)
	} else if err != nil {
		return 0, err
	}

	// A canceled row was abandoned by the operator, and an externally consumed
	// nonce can only be re-executed through resolve-external-nonce; requeueing
	// either in place would try to reuse a consumed nonce.
	if row.FailureKind == TxFailureCanceled || row.FailureKind == TxFailureNonceConsumedExternally {
		return 0, fmt.Errorf("outbox tx %d failure kind %s is not retryable", id, row.FailureKind)
	}

	// A pinned receipt resolution means an attempt of this row mined and its
	// nonce is consumed. Rows whose failure kind was finalized to NULL (for
	// example an lzReceive failure whose executor job already advanced or
	// parked) would otherwise take the requeue-in-place branch below, re-sign
	// on the consumed nonce, and wedge the signer lane with an attempt the
	// broadcast claim always refuses. Only the receipt-failed clone branch may
	// act on such evidence, and it leaves the original row terminal.
	if receiptOutcome != nil && row.FailureKind != TxFailureReceiptFailed {
		return 0, fmt.Errorf("outbox tx %d has a pinned receipt resolution and its nonce is consumed; the row cannot be requeued in place", id)
	}

	// Pricing calldata carries a time-bound market observation: re-signing it
	// later would write a price whose updatedAt may already be past its own
	// staleAfter, so nonce-less pricing failures are refused and the bot
	// rebuilds from a fresh observation. The exception is a failed pricing row
	// still HOLDING an unconsumed nonce (an upgraded database can carry
	// historical sign/broadcast failures from before the attempts cutover):
	// nothing else can fill that nonce — failed rows do not block the
	// lower-nonce selector, so newer transactions would sign above the gap and
	// never mine — and the strictly-increasing on-chain timestamp makes a
	// superseded re-send a harmless skip. The in-place requeue below keeps the
	// nonce; pinned rows (consumed nonce) were already refused above.
	// A receipt-failed pricing row is refused too: its nonce was consumed by
	// the mined attempt, and the clone branch would re-sign the stale
	// observation on a fresh nonce.
	if row.Purpose == TxPurposePricingSetPriceSnapshot && (row.Nonce == nil || row.FailureKind == TxFailureReceiptFailed) {
		return 0, fmt.Errorf("outbox tx %d carries a pricing observation and is not retryable; the price bot rebuilds automatically", id)
	}
	// The legacy in-place requeue participates in the same per-feed invariant
	// as the bot's enqueue: under the feed advisory lock, an in-flight snapshot
	// for the feed refuses the requeue, so two snapshots can never race and the
	// stale calldata only re-signs to fill the nonce once the feed is idle.
	if row.Purpose == TxPurposePricingSetPriceSnapshot {
		if _, err := tx.Exec(ctx,
			"SELECT pg_advisory_xact_lock($1::integer, hashtext($2)::integer)",
			int32(row.ChainEID), "pricing_feed:"+common.BytesToAddress(toAddress).Hex(),
		); err != nil {
			return 0, err
		}
		// A same-signer in-flight row at a HIGHER nonce does not block: it can
		// never mine before this row's nonce gap is filled, so refusing here
		// would deadlock both (the pre-round-9 bot could enqueue such a row
		// before gap rows became feed gates). Everything else — un-nonced
		// queued rows, lower/equal nonces, other signers' lanes — still
		// refuses, keeping the one-snapshot-per-feed race closed.
		var blockingExists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM tx_outbox
				WHERE chain_eid = $1 AND purpose = $2 AND to_address = $3 AND status = ANY($4)
					AND NOT (signer_id = $5 AND nonce IS NOT NULL AND nonce > $6)
			)
		`, row.ChainEID, row.Purpose, toAddress, txPricingPendingStatuses, rowSignerID, *row.Nonce).Scan(&blockingExists); err != nil {
			return 0, err
		}
		if blockingExists {
			return 0, fmt.Errorf("outbox tx %d cannot be requeued while another snapshot for its feed is in flight; retry after it resolves", id)
		}
	}

	if row.Nonce == nil || row.FailureKind != TxFailureReceiptFailed {
		// Same scope gate as the clone path below: requeueing charges the
		// retry budget and clears failure metadata while a paused scope cannot
		// sign the queued row — the operator retry is refused instead of
		// silently burning an attempt.
		guid := []byte(nil)
		if row.GUID != nil {
			guid = *row.GUID
		}
		if err := lockTxSendScope(ctx, tx, row.ChainEID, row.Purpose, guid); err != nil {
			if !errors.Is(err, ErrTxSendScopeInactive) {
				return 0, err
			}
			if deferErr := deferFailedTxRetry(ctx, tx, id); deferErr != nil {
				return 0, deferErr
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return 0, commitErr
			}
			return 0, ErrTxSendScopeInactive
		}
		if err := requeueFailedTx(ctx, tx, id); err != nil {
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

	// Cloning a receipt-failed row is new spend for its scope: while the pathway
	// or chain is paused/disabled, defer the retry instead (nothing is cloned,
	// the failure metadata and attempts stay untouched) so it resumes on its own
	// once the scope is active. The lzReceive path applies the same gate inside
	// its workflow preparation, where it can finalize the row because the
	// deliverer owns resuming the job.
	if row.Purpose != txPurposeExecutorLzReceive {
		guid := []byte(nil)
		if row.GUID != nil {
			guid = *row.GUID
		}
		if err := lockTxSendScope(ctx, tx, row.ChainEID, row.Purpose, guid); err != nil {
			if !errors.Is(err, ErrTxSendScopeInactive) {
				return 0, err
			}
			if deferErr := deferFailedTxRetry(ctx, tx, id); deferErr != nil {
				return 0, deferErr
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return 0, commitErr
			}
			return 0, ErrTxSendScopeInactive
		}
	}
	if retryPrepared, err := prepareReceiptRetryWorkflow(ctx, tx, id, row.ChainEID, row.Purpose, row.GUID); err != nil {
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
		ChainEID    uint32
		Purpose     string
		GUID        *[]byte
		Nonce       *int64
		FailureKind string
	}
	err = tx.QueryRow(ctx, `
		SELECT id, chain_eid, purpose, guid, nonce, failure_kind
		FROM tx_outbox failed
		WHERE chain_eid = $1
			AND signer_id = $2
			AND status = $3
			AND attempts < $4
			AND next_retry_at IS NOT NULL
			AND next_retry_at <= now()
			AND failure_kind IN ($5, $6)
			-- Pricing calldata carries a time-bound market observation and must
			-- never be re-signed; the bot rebuilds from a fresh observation.
			AND purpose <> $7
			AND NOT EXISTS (
				SELECT 1
				FROM tx_outbox child
				WHERE child.retry_of_id = failed.id
			)
		ORDER BY next_retry_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, chainEID, signerID, TxStatusFailed, TxAutoRetryMaxAttempts, TxFailureEstimateGasRevert, TxFailureReceiptFailed, TxPurposePricingSetPriceSnapshot).Scan(
		&row.ID,
		&row.ChainEID,
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
		// Requeueing charges the retry budget (attempts + 1) and clears the
		// failure metadata, and the signing gate cannot act on the queued row
		// while its scope is paused — so a pause must defer the retry without
		// mutating anything, exactly like the receipt-failed branch, or every
		// pause cycle would burn an automatic retry for free.
		{
			guid := []byte(nil)
			if row.GUID != nil {
				guid = *row.GUID
			}
			if err := lockTxSendScope(ctx, tx, row.ChainEID, row.Purpose, guid); err != nil {
				if !errors.Is(err, ErrTxSendScopeInactive) {
					return 0, err
				}
				if deferErr := deferFailedTxRetry(ctx, tx, row.ID); deferErr != nil {
					return 0, deferErr
				}
				if commitErr := tx.Commit(ctx); commitErr != nil {
					return 0, commitErr
				}
				return 0, ErrNoFailedTxRetry
			}
		}
		if err := requeueFailedTx(ctx, tx, row.ID); err != nil {
			return 0, err
		}
		retryID = row.ID
	case row.FailureKind == TxFailureReceiptFailed:
		// Same scope gate as RetryFailedTx: no clone while the scope is paused,
		// only a deferred next_retry_at so the retry resumes after unpause.
		if row.Purpose != txPurposeExecutorLzReceive {
			guid := []byte(nil)
			if row.GUID != nil {
				guid = *row.GUID
			}
			if err := lockTxSendScope(ctx, tx, row.ChainEID, row.Purpose, guid); err != nil {
				if !errors.Is(err, ErrTxSendScopeInactive) {
					return 0, err
				}
				if deferErr := deferFailedTxRetry(ctx, tx, row.ID); deferErr != nil {
					return 0, deferErr
				}
				if commitErr := tx.Commit(ctx); commitErr != nil {
					return 0, commitErr
				}
				return 0, ErrNoFailedTxRetry
			}
		}
		if retryPrepared, err := prepareReceiptRetryWorkflow(ctx, tx, row.ID, row.ChainEID, row.Purpose, row.GUID); err != nil {
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

// deferFailedTxRetry pushes a failed row's next retry without touching its
// failure metadata or attempt count; used while the row's send scope is paused.
func deferFailedTxRetry(ctx context.Context, tx pgx.Tx, id int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET next_retry_at = now() + $1::interval, updated_at = now()
		WHERE id = $2 AND status = $3
	`, txRetryScopeDeferDelay.String(), id, TxStatusFailed)
	return err
}

func requeueFailedTx(ctx context.Context, tx pgx.Tx, id int64) error {
	// receipt_outcome IS NULL is a structural invariant, not just an entry
	// guard: a pinned resolution means the nonce is consumed, and a requeued
	// row would re-sign an attempt the broadcast claim always refuses,
	// wedging the signer lane behind it.
	tag, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET
			status = $1,
			attempts = attempts + 1,
			failure_kind = NULL,
			next_retry_at = NULL,
			last_error = NULL,
			updated_at = now()
		WHERE id = $2 AND status = $3 AND receipt_outcome IS NULL
	`, TxStatusQueued, id, TxStatusFailed)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox tx %d is not failed without a pinned receipt resolution", id)
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

func prepareReceiptRetryWorkflow(ctx context.Context, tx pgx.Tx, failedTxID int64, chainEID uint32, purpose string, guidBytes *[]byte) (bool, error) {
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
	// carries across the pause). The scope share locks serialize this decision
	// against a concurrent pause.
	err = lockTxSendScope(ctx, tx, chainEID, txPurposeExecutorLzReceive, *guidBytes)
	if err != nil && !errors.Is(err, ErrTxSendScopeInactive) {
		return false, err
	}
	if errors.Is(err, ErrTxSendScopeInactive) {
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
	CancelRequestedAt        *time.Time
	ReceiptOutcome           *string
	ReceiptAttemptID         *int64
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
		CancelRequestedAt:        cloneOptionalTime(r.CancelRequestedAt),
		ReceiptOutcome:           optionalStringValue(r.ReceiptOutcome),
		ReceiptAttemptID:         optionalInt64Value(r.ReceiptAttemptID),
	}, nil
}

func optionalInt64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
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
