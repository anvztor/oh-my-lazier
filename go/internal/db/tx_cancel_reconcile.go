package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Operator cancel and confirmed-block nonce reconciliation (P2-A #3). A held
// signer lane always has an exit: nonce_reconcile_required rows are reconciled
// against the chain's confirmed nonce, an operator can cancel any nonce-holding
// row with a same-nonce noop, and an externally consumed nonce is parked as
// evidence until the operator resolves it with retry or abandon.

const (
	// HeldNonceConsumedExternally means the confirmed chain nonce moved past this
	// row's nonce while none of its attempts has a receipt: something outside this
	// worker consumed the nonce, which violates the single-broadcaster assumption.
	// The row keeps its evidence and receipt polling until an operator resolves it.
	HeldNonceConsumedExternally = "nonce_consumed_externally"

	// TxFailureCanceled records a row terminated by an operator cancel; the nonce
	// was consumed by the cancel noop, so the row is never requeued in place.
	TxFailureCanceled = "canceled"
	// TxFailureNonceConsumedExternally records a row terminated after its nonce
	// was consumed outside this worker; re-execution goes through a fresh clone.
	TxFailureNonceConsumedExternally = "nonce_consumed_externally"
)

// ErrNoCancelWork indicates no row has a due cancel request needing a cancel attempt.
var ErrNoCancelWork = errors.New("no cancel work")

// ErrNoNonceReconcileWork indicates no held lane is due for nonce reconciliation.
var ErrNoNonceReconcileWork = errors.New("no nonce reconcile work")

// txCancelableStatuses are the non-terminal statuses an operator may cancel once
// the row holds a nonce.
var txCancelableStatuses = []string{TxStatusQueued, TxStatusNonceAssigned, TxStatusSigned, TxStatusBroadcast, TxStatusHeld}

// RequestTxCancel records operator cancel intent on a non-terminal row that
// holds a nonce and revokes any signing lease, so an in-flight original or
// replacement signature can no longer land its attempt. The intent persists
// until the final receipt terminalization, and each explicit request resets the
// signing-failure budget for one more cycle; re-requesting a row whose active
// attempt is already a cancel additionally authorizes one cancel fee bump
// through the replacement path. On the first cancel of a task attempt any
// pending operator replacement request is cleared instead: the operator just
// abandoned that intent, and leaving it would let the replacement path sign an
// unrequested bumped cancel as soon as the first cancel attempt is active. A
// row with a pinned receipt resolution is about to terminalize and can no
// longer be canceled, and an externally consumed nonce needs
// resolve-external-nonce instead.
func (s *Store) RequestTxCancel(ctx context.Context, id int64) error {
	if id <= 0 {
		return errors.New("outbox tx id is required")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE tx_outbox o
		SET cancel_requested_at = now(),
			cancel_defer_until = NULL,
			pre_sign_failure_count = 0,
			next_sign_at = NULL,
			replace_requested_at = CASE WHEN EXISTS (
				SELECT 1 FROM tx_attempts a
				WHERE a.outbox_id = o.id AND a.id = o.active_attempt_id AND a.kind = $4
			) THEN now() ELSE NULL END,
			lease_token = NULL, lease_until = NULL, updated_at = now()
		WHERE o.id = $1
			AND o.nonce IS NOT NULL
			AND o.status = ANY($2)
			AND (o.held_reason IS NULL OR o.held_reason <> $3)
			AND o.receipt_outcome IS NULL
	`, id, txCancelableStatuses, HeldNonceConsumedExternally, TxAttemptCancel)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox tx %d is not cancelable", id)
	}
	return nil
}

// CancelCandidate is one row whose due cancel request still needs its first
// cancel attempt (fee bumps of an existing cancel attempt flow through the
// kind-aware replacement path instead).
type CancelCandidate struct {
	Outbox OutboxTx
	// ActiveAttemptID is zero when the row has no attempt yet (a bare
	// nonce-holding row can still be canceled).
	ActiveAttemptID int64
	AttemptHashes   []common.Hash
}

// NextCancelCandidate peeks (without reserving) the next row with a due cancel
// request whose active attempt is not yet a cancel. Rows scheduled for a
// pre-sign failure backoff (next_sign_at in the future) are skipped so a bare
// nonce_assigned cancel cannot burn its signing budget in a hot loop.
func (s *Store) NextCancelCandidate(ctx context.Context, chainEID uint32, signerID string) (CancelCandidate, error) {
	if chainEID == 0 || signerID == "" {
		return CancelCandidate{}, errors.New("chain and signer are required")
	}
	var row outboxTxRow
	var activeAttemptID *int64
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
			o.receipt_outcome, o.receipt_attempt_id,
			a.id
		FROM tx_outbox o
		LEFT JOIN tx_attempts a ON a.outbox_id = o.id AND a.id = o.active_attempt_id
		WHERE o.chain_eid = $1 AND o.signer_id = $2
			AND o.cancel_requested_at IS NOT NULL AND o.cancel_requested_at <= now()
			AND (o.cancel_defer_until IS NULL OR o.cancel_defer_until <= now())
			AND o.nonce IS NOT NULL
			AND o.status = ANY($3)
			AND (o.held_reason IS NULL OR o.held_reason <> $4)
			AND (o.lease_until IS NULL OR o.lease_until <= now())
			AND (a.id IS NULL OR a.kind <> $5)
			AND o.pre_sign_failure_count < $6
			AND (o.next_sign_at IS NULL OR o.next_sign_at <= now())
			AND o.receipt_outcome IS NULL
		ORDER BY o.cancel_requested_at, o.id
		LIMIT 1
	`, chainEID, signerID, txCancelableStatuses, HeldNonceConsumedExternally, TxAttemptCancel, TxMaxPreSignFailures).Scan(
		&row.ID, &row.ChainEID, &row.Purpose, &row.GUID, &row.ToAddress,
		&row.Calldata, &row.Value, &row.GasLimit, &row.MaxFeePerGas,
		&row.MaxPriorityFeePerGas, &row.Nonce, &row.TxHash, &row.SignerID,
		&row.Status, &row.Attempts, &row.FailureKind, &row.NextRetryAt,
		&row.RetryOfID, &row.ReceiptTxHash, &row.ReceiptStatus,
		&row.ReceiptBlockNumber, &row.ReceiptGasUsed, &row.ReceiptEffectiveGasPrice,
		&row.ReceiptGasCostDstWei, &row.ReceiptGasCostSrcWei,
		&row.ReceiptObservedAt, &row.ReceiptCostPricedAt, &row.CancelRequestedAt,
		&row.ReceiptOutcome, &row.ReceiptAttemptID,
		&activeAttemptID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return CancelCandidate{}, ErrNoCancelWork
	}
	if err != nil {
		return CancelCandidate{}, err
	}
	outboxTx, err := row.toOutboxTx()
	if err != nil {
		return CancelCandidate{}, err
	}
	candidate := CancelCandidate{Outbox: outboxTx}
	if activeAttemptID != nil {
		candidate.ActiveAttemptID = *activeAttemptID
	}
	hashes, err := s.attemptPollHashes(ctx, outboxTx.ID)
	if err != nil {
		return CancelCandidate{}, err
	}
	candidate.AttemptHashes = hashes
	return candidate, nil
}

// attemptPollHashes returns the hashes worth checking on chain: only sent
// attempts qualify. A signed-state attempt was never handed to any node (the
// broadcast claim flips it to ambiguous first), so its hash cannot be mined.
func (s *Store) attemptPollHashes(ctx context.Context, outboxID int64) ([]common.Hash, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tx_hash FROM tx_attempts
		WHERE outbox_id = $1 AND state IN ('submitted', 'ambiguous')
		ORDER BY id ASC
	`, outboxID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	hashes := make([]common.Hash, 0)
	for rows.Next() {
		var hashBytes []byte
		if err := rows.Scan(&hashBytes); err != nil {
			return nil, err
		}
		hashes = append(hashes, common.BytesToHash(hashBytes))
	}
	return hashes, rows.Err()
}

// DeferCancel pushes a due cancel request back when its preflight defers (for
// example one of the attempts already has a receipt awaiting confirmation depth,
// or a fee cap blocks the cancel), so it is not retried on every manager pass.
// Only the pacing column moves: cancel_requested_at stays immutable so the
// cancel age surfaced by stats and readiness keeps measuring the operator's
// real wait, which is what escalates a fee-cap-blocked cancel.
func (s *Store) DeferCancel(ctx context.Context, id int64) error {
	if id <= 0 {
		return errors.New("outbox tx id is required")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET cancel_defer_until = now() + $1::interval, updated_at = now()
		WHERE id = $2 AND cancel_requested_at IS NOT NULL
			AND status NOT IN ('confirmed', 'failed')
	`, pgInterval(txReplacementDeferDelay), id)
	return err
}

// ClaimOutboxForCancelSigning writes a signing lease for the first cancel
// attempt of a row with persistent cancel intent. expectedActiveAttemptID is
// zero for a bare nonce-holding row without any attempt.
func (s *Store) ClaimOutboxForCancelSigning(ctx context.Context, id, expectedActiveAttemptID int64, leaseToken uuid.UUID, leaseTTL time.Duration) (OutboxTx, error) {
	if id <= 0 || leaseTTL <= 0 {
		return OutboxTx{}, errors.New("cancel signing claim requires an id and a positive ttl")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OutboxTx{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string
	var heldReason *string
	var activeAttemptID *int64
	var cancelRequestedAt *time.Time
	var receiptOutcome *string
	if err := tx.QueryRow(ctx, `
		SELECT status, held_reason, active_attempt_id, cancel_requested_at, receipt_outcome
		FROM tx_outbox
		WHERE id = $1 AND nonce IS NOT NULL AND (lease_until IS NULL OR lease_until <= now())
		FOR UPDATE
	`, id).Scan(&status, &heldReason, &activeAttemptID, &cancelRequestedAt, &receiptOutcome); errors.Is(err, pgx.ErrNoRows) {
		return OutboxTx{}, ErrOutboxLeaseLost
	} else if err != nil {
		return OutboxTx{}, err
	}
	if cancelRequestedAt == nil || receiptOutcome != nil {
		return OutboxTx{}, ErrOutboxLeaseLost
	}
	current := int64(0)
	if activeAttemptID != nil {
		current = *activeAttemptID
	}
	if current != expectedActiveAttemptID {
		return OutboxTx{}, ErrActiveAttemptChanged
	}
	cancelable := false
	for _, cancelableStatus := range txCancelableStatuses {
		if status == cancelableStatus {
			cancelable = true
			break
		}
	}
	if !cancelable || (heldReason != nil && *heldReason == HeldNonceConsumedExternally) {
		return OutboxTx{}, fmt.Errorf("outbox tx %d is not cancelable in status %s", id, status)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tx_outbox SET lease_token = $1::uuid, lease_until = now() + $2::interval, updated_at = now() WHERE id = $3
	`, leaseToken.String(), pgInterval(leaseTTL), id); err != nil {
		return OutboxTx{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OutboxTx{}, err
	}
	return s.GetOutboxTx(ctx, id)
}

// InsertCancelAttempt persists the first signed cancel attempt (state=signed),
// switches the outbox active pointer to it, and releases the signing lease. The
// cancel intent is deliberately NOT cleared here: it persists until the final
// receipt terminalization so no entry point can revive the original task.
func (s *Store) InsertCancelAttempt(ctx context.Context, outboxID, expectedActiveAttemptID int64, leaseToken uuid.UUID, a SignedAttempt) (TxAttempt, error) {
	if outboxID <= 0 {
		return TxAttempt{}, errors.New("outbox id is required")
	}
	if a.Kind != TxAttemptCancel {
		return TxAttempt{}, fmt.Errorf("cancel attempt kind %q must be %q", a.Kind, TxAttemptCancel)
	}
	if len(a.TxHash) != common.HashLength || len(a.RawTx) == 0 {
		return TxAttempt{}, errors.New("attempt requires a 32-byte hash and non-empty raw tx")
	}
	if a.MaxFeePerGas == nil || a.MaxFeePerGas.Sign() <= 0 {
		return TxAttempt{}, errors.New("attempt requires a positive max fee per gas")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TxAttempt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotent recovery: a committed insert whose result the client never saw.
	if existing, ok, err := scanAttemptByHash(ctx, tx, a.TxHash); err != nil {
		return TxAttempt{}, err
	} else if ok {
		if existing.OutboxID != outboxID || existing.Nonce != a.Nonce || existing.Kind != a.Kind {
			return TxAttempt{}, fmt.Errorf("attempt hash %s already exists with different immutable fields", a.TxHash)
		}
		return existing, nil
	}

	var curLease *string
	var outboxNonce *int64
	var activeAttemptID *int64
	var outboxStatus string
	var heldReason *string
	var cancelRequestedAt *time.Time
	var receiptOutcome *string
	if err := tx.QueryRow(ctx, `
		SELECT lease_token::text, nonce, active_attempt_id, status, held_reason, cancel_requested_at, receipt_outcome
		FROM tx_outbox WHERE id = $1 AND lease_until > now()
		FOR UPDATE
	`, outboxID).Scan(&curLease, &outboxNonce, &activeAttemptID, &outboxStatus, &heldReason, &cancelRequestedAt, &receiptOutcome); errors.Is(err, pgx.ErrNoRows) {
		return TxAttempt{}, ErrOutboxLeaseLost
	} else if err != nil {
		return TxAttempt{}, err
	}
	if curLease == nil || *curLease != leaseToken.String() {
		return TxAttempt{}, ErrOutboxLeaseLost
	}
	if cancelRequestedAt == nil || receiptOutcome != nil {
		// The row terminalized or pinned its receipt resolution while signing.
		return TxAttempt{}, ErrOutboxLeaseLost
	}
	current := int64(0)
	if activeAttemptID != nil {
		current = *activeAttemptID
	}
	if current != expectedActiveAttemptID {
		return TxAttempt{}, ErrActiveAttemptChanged
	}
	cancelable := false
	for _, cancelableStatus := range txCancelableStatuses {
		if outboxStatus == cancelableStatus {
			cancelable = true
			break
		}
	}
	if !cancelable || (heldReason != nil && *heldReason == HeldNonceConsumedExternally) {
		return TxAttempt{}, ErrActiveAttemptChanged
	}
	if outboxNonce == nil || *outboxNonce < 0 || uint64(*outboxNonce) != a.Nonce {
		return TxAttempt{}, fmt.Errorf("attempt nonce %d does not match outbox tx %d nonce", a.Nonce, outboxID)
	}

	var priorityArg any
	if a.MaxPriorityFeePerGas != nil {
		priorityArg = a.MaxPriorityFeePerGas.String()
	}
	var attemptID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO tx_attempts (
			outbox_id, kind, nonce, tx_type, tx_hash, raw_tx, gas_limit,
			max_fee_per_gas, max_priority_fee_per_gas, state, signing_token
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::numeric, $9::numeric, $10, $11::uuid)
		RETURNING id
	`, outboxID, a.Kind, int64(a.Nonce), int16(a.TxType), a.TxHash.Bytes(), a.RawTx,
		int64(a.GasLimit), a.MaxFeePerGas.String(), priorityArg, TxAttemptSigned, a.SigningToken.String()).Scan(&attemptID); err != nil {
		return TxAttempt{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET active_attempt_id = $1,
			status = CASE WHEN status IN ('queued', 'nonce_assigned', 'held') THEN 'signed' ELSE status END,
			held_reason = NULL,
			lease_token = NULL, lease_until = NULL,
			pre_sign_failure_count = 0, next_sign_at = NULL, updated_at = now()
		WHERE id = $2
	`, attemptID, outboxID); err != nil {
		return TxAttempt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TxAttempt{}, err
	}
	return TxAttempt{ID: attemptID, OutboxID: outboxID, Kind: a.Kind, Nonce: a.Nonce, TxType: a.TxType, TxHash: a.TxHash, RawTx: a.RawTx, GasLimit: a.GasLimit, MaxFeePerGas: a.MaxFeePerGas, MaxPriorityFeePerGas: a.MaxPriorityFeePerGas, State: TxAttemptSigned, SigningToken: a.SigningToken}, nil
}

// NonceReconcileHold is one held(nonce_reconcile_required) row selected for a
// confirmed-nonce reconciliation pass. ActiveKind distinguishes a lane whose
// active attempt is already a cancel (released holds resume the cancel raw)
// from one the first-cancel flow still owns.
type NonceReconcileHold struct {
	ID              int64
	Nonce           uint64
	CancelRequested bool
	ActiveKind      string
	AttemptHashes   []common.Hash
}

// ExtendNonceReconciliation renews a still-held reconciliation lease in place
// (token CAS). The receipt-probe phase is bounded only by real work, so a
// large backlog could otherwise outlive the initial lease and the final
// publish — which CASes on the same lease — would discard the whole pass and
// restart it from scratch every round. A lost or expired lease returns
// ErrOutboxLeaseLost so the pass aborts instead of probing for a publish that
// can never land.
func (s *Store) ExtendNonceReconciliation(ctx context.Context, chainEID uint32, signerID string, token uuid.UUID, leaseTTL time.Duration) error {
	if chainEID == 0 || signerID == "" || leaseTTL <= 0 {
		return errors.New("nonce reconciliation extension requires chain, signer, and a positive ttl")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE tx_nonce_cursors
		SET reconcile_lease_until = now() + $1::interval, updated_at = now()
		WHERE chain_eid = $2 AND signer_id = $3
			AND reconcile_lease_token = $4::uuid AND reconcile_lease_until > now()
	`, pgInterval(leaseTTL), chainEID, signerID, token.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrOutboxLeaseLost
	}
	return nil
}

// ClaimNonceReconciliation claims the signer lane's reconciliation lease when at
// least one held(nonce_reconcile_required) row exists and the backoff has
// expired, and returns the held rows with their poll-worthy attempt hashes. The
// caller performs all RPC reads outside any transaction and applies each row's
// outcome through its own compare-and-set.
func (s *Store) ClaimNonceReconciliation(ctx context.Context, chainEID uint32, signerID string, token uuid.UUID, leaseTTL time.Duration) ([]NonceReconcileHold, error) {
	if chainEID == 0 || signerID == "" || leaseTTL <= 0 {
		return nil, errors.New("nonce reconciliation claim requires chain, signer, and positive ttl")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT o.id, o.nonce, o.cancel_requested_at IS NOT NULL, COALESCE(a.kind, '')
		FROM tx_outbox o
		LEFT JOIN tx_attempts a ON a.outbox_id = o.id AND a.id = o.active_attempt_id
		WHERE o.chain_eid = $1 AND o.signer_id = $2
			AND o.status = $3 AND o.held_reason = $4
			AND o.receipt_outcome IS NULL
		ORDER BY o.nonce ASC, o.id ASC
	`, chainEID, signerID, TxStatusHeld, HeldNonceReconcileRequired)
	if err != nil {
		return nil, err
	}
	holds := make([]NonceReconcileHold, 0)
	for rows.Next() {
		var hold NonceReconcileHold
		var nonce int64
		if err := rows.Scan(&hold.ID, &nonce, &hold.CancelRequested, &hold.ActiveKind); err != nil {
			rows.Close()
			return nil, err
		}
		if nonce < 0 {
			rows.Close()
			return nil, fmt.Errorf("outbox tx %d has a negative held nonce", hold.ID)
		}
		hold.Nonce = uint64(nonce)
		holds = append(holds, hold)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(holds) == 0 {
		return nil, ErrNoNonceReconcileWork
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE tx_nonce_cursors
		SET reconcile_lease_token = $1::uuid, reconcile_lease_until = now() + $2::interval, updated_at = now()
		WHERE chain_eid = $3 AND signer_id = $4
			AND (reconcile_lease_until IS NULL OR reconcile_lease_until <= now())
			AND (next_reconcile_at IS NULL OR next_reconcile_at <= now())
	`, token.String(), pgInterval(leaseTTL), chainEID, signerID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrNoNonceReconcileWork
	}
	// AttemptHashes are deliberately NOT loaded here: the lease clock started
	// above, and the caller must be able to start its heartbeat before any
	// backlog-proportional work — load them afterwards through
	// LoadNonceReconcileAttemptHashes.
	return holds, nil
}

// LoadNonceReconcileAttemptHashes returns each held row's poll-worthy attempt
// hashes (sent attempts only) in one set-based query, keyed by outbox id. The
// reconciler calls this after its lease heartbeat is running, so a
// backlog-proportional result set can never expire the lease unattended.
func (s *Store) LoadNonceReconcileAttemptHashes(ctx context.Context, holdIDs []int64) (map[int64][]common.Hash, error) {
	hashes := make(map[int64][]common.Hash, len(holdIDs))
	if len(holdIDs) == 0 {
		return hashes, nil
	}
	hashRows, err := s.pool.Query(ctx, `
		SELECT outbox_id, tx_hash FROM tx_attempts
		WHERE outbox_id = ANY($1) AND state IN ('submitted', 'ambiguous')
		ORDER BY outbox_id ASC, id ASC
	`, holdIDs)
	if err != nil {
		return nil, err
	}
	defer hashRows.Close()
	for hashRows.Next() {
		var outboxID int64
		var hashBytes []byte
		if err := hashRows.Scan(&outboxID, &hashBytes); err != nil {
			return nil, err
		}
		hashes[outboxID] = append(hashes[outboxID], common.BytesToHash(hashBytes))
	}
	return hashes, hashRows.Err()
}

// FinishNonceReconciliation releases the reconciliation lease and schedules the
// next pass.
func (s *Store) FinishNonceReconciliation(ctx context.Context, chainEID uint32, signerID string, token uuid.UUID, backoff time.Duration) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tx_nonce_cursors
		SET reconcile_lease_token = NULL, reconcile_lease_until = NULL,
			next_reconcile_at = now() + $1::interval, updated_at = now()
		WHERE chain_eid = $2 AND signer_id = $3 AND reconcile_lease_token = $4::uuid
	`, pgInterval(backoff), chainEID, signerID, token.String())
	return err
}

// Nonce reconciliation decisions applied atomically by ApplyNonceReconciliation.
const (
	// NonceReconcileRelease returns a hold to broadcast: the nonce is still
	// unspent at the confirmed block, so the nonce-too-low answer was transient.
	// Budgets and attempts are preserved.
	NonceReconcileRelease = "release"
	// NonceReconcileMarkExternal parks a hold as externally consumed: the
	// confirmed nonce moved past it with no receipt on any of its attempts.
	NonceReconcileMarkExternal = "mark_external"
)

// NonceReconcileDecision is the reconciliation outcome for one held row.
type NonceReconcileDecision struct {
	ID     int64
	Action string
}

// NonceReconcileResult reports what one atomic reconciliation application did.
type NonceReconcileResult struct {
	Changed         int
	PreviousCursor  uint64
	CursorForwarded bool
}

// ApplyNonceReconciliation publishes a reconciliation pass in one transaction
// bound to the reconciliation lease: it validates the lease token (a stale
// owner's decisions from an old chain snapshot are rejected wholesale),
// fast-forwards the nonce cursor to the confirmed nonce under the signer
// advisory lock BEFORE any row state becomes visible (so a clone created from a
// freshly parked row can never claim an already-consumed nonce), applies every
// row decision with its own compare-and-set, and releases the lease with the
// next backoff. The caller performs all RPC reads first and calls this exactly
// once per claimed pass; on any RPC error it calls FinishNonceReconciliation
// instead, leaving every row untouched.
func (s *Store) ApplyNonceReconciliation(ctx context.Context, chainEID uint32, signerID string, token uuid.UUID, confirmedNonce, confirmedBlock uint64, backoff time.Duration, decisions []NonceReconcileDecision) (NonceReconcileResult, error) {
	if chainEID == 0 || signerID == "" || backoff <= 0 {
		return NonceReconcileResult{}, errors.New("apply nonce reconciliation requires chain, signer, and positive backoff")
	}
	if confirmedNonce > maxDBNonce {
		return NonceReconcileResult{}, fmt.Errorf("confirmed nonce %d exceeds database limit", confirmedNonce)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NonceReconcileResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The signer advisory lock comes first, matching the signing path's
	// advisory-then-cursor order; taking the cursor row lock first could
	// deadlock against a concurrent nonce assignment.
	if err := lockSignerNonce(ctx, tx, chainEID, signerID); err != nil {
		return NonceReconcileResult{}, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE tx_nonce_cursors
		SET reconcile_lease_token = NULL, reconcile_lease_until = NULL,
			next_reconcile_at = now() + $1::interval, updated_at = now()
		WHERE chain_eid = $2 AND signer_id = $3
			AND reconcile_lease_token = $4::uuid AND reconcile_lease_until > now()
	`, pgInterval(backoff), chainEID, signerID, token.String())
	if err != nil {
		return NonceReconcileResult{}, err
	}
	if tag.RowsAffected() != 1 {
		return NonceReconcileResult{}, ErrOutboxLeaseLost
	}

	var result NonceReconcileResult
	var current int64
	if err := tx.QueryRow(ctx, `
		SELECT next_nonce FROM tx_nonce_cursors WHERE chain_eid = $1 AND signer_id = $2 FOR UPDATE
	`, chainEID, signerID).Scan(&current); errors.Is(err, pgx.ErrNoRows) {
		return NonceReconcileResult{}, ErrNonceCursorMissing
	} else if err != nil {
		return NonceReconcileResult{}, err
	}
	if current < 0 {
		return NonceReconcileResult{}, fmt.Errorf("negative nonce cursor for chain %d signer %s", chainEID, signerID)
	}
	result.PreviousCursor = uint64(current)
	if confirmedNonce > result.PreviousCursor {
		if _, err := tx.Exec(ctx, `
			UPDATE tx_nonce_cursors SET next_nonce = $1, updated_at = now() WHERE chain_eid = $2 AND signer_id = $3
		`, int64(confirmedNonce), chainEID, signerID); err != nil {
			return NonceReconcileResult{}, err
		}
		result.CursorForwarded = true
	}

	for _, decision := range decisions {
		switch decision.Action {
		case NonceReconcileRelease:
			// A released row goes back to broadcast only while its active
			// attempt can still be replayed. An attempt whose broadcast
			// budget is exhausted would sit in broadcast unclaimable and
			// unparkable — outside the lower-nonce barrier — letting higher
			// nonces sign past a nonce that never reached the chain, so it
			// parks as held(broadcast_exhausted) for an operator replace
			// (fresh attempt, fresh budget) or cancel instead.
			tag, err := tx.Exec(ctx, `
				UPDATE tx_outbox o
				SET status = CASE WHEN a.state IN ($5, $6) AND a.broadcast_count >= $7 THEN o.status ELSE $1 END,
					held_reason = CASE WHEN a.state IN ($5, $6) AND a.broadcast_count >= $7 THEN $8 ELSE NULL END,
					updated_at = now()
				FROM tx_attempts a
				WHERE o.id = $2 AND a.id = o.active_attempt_id
					AND o.status = $3 AND o.held_reason = $4
					AND o.receipt_outcome IS NULL
			`, TxStatusBroadcast, decision.ID, TxStatusHeld, HeldNonceReconcileRequired, TxAttemptSigned, TxAttemptAmbiguous, TxMaxBroadcasts, HeldBroadcastExhausted)
			if err != nil {
				return NonceReconcileResult{}, err
			}
			result.Changed += int(tag.RowsAffected())
		case NonceReconcileMarkExternal:
			evidence := fmt.Sprintf("nonce consumed externally: confirmed nonce %d at block %d observed %s",
				confirmedNonce, confirmedBlock, time.Now().UTC().Format(time.RFC3339))
			tag, err := tx.Exec(ctx, `
				UPDATE tx_outbox
				SET held_reason = $1, last_error = $2, updated_at = now()
				WHERE id = $3 AND status = $4 AND held_reason = $5
					AND receipt_outcome IS NULL
			`, HeldNonceConsumedExternally, evidence, decision.ID, TxStatusHeld, HeldNonceReconcileRequired)
			if err != nil {
				return NonceReconcileResult{}, err
			}
			result.Changed += int(tag.RowsAffected())
		default:
			return NonceReconcileResult{}, fmt.Errorf("unknown nonce reconcile action %q", decision.Action)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return NonceReconcileResult{}, err
	}
	return result, nil
}

// ResolveExternalNonceRetry terminates an externally consumed row and atomically
// creates a fresh queued clone that re-executes the task with a new nonce. The
// worker workflow stays in its *_TX_ENQUEUED state so the clone's receipt (or
// chain reconciliation) completes it.
func (s *Store) ResolveExternalNonceRetry(ctx context.Context, id int64) (int64, error) {
	if id <= 0 {
		return 0, errors.New("outbox tx id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var chainEID uint32
	var purpose string
	var guid *[]byte
	if err := tx.QueryRow(ctx, `
		SELECT chain_eid, purpose, guid FROM tx_outbox WHERE id = $1 FOR UPDATE
	`, id).Scan(&chainEID, &purpose, &guid); errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("outbox tx %d not found", id)
	} else if err != nil {
		return 0, err
	}
	// A pricing clone would re-sign a time-bound market observation whose
	// updatedAt may already be past its own staleAfter; abandon the row instead
	// and the bot rebuilds from a fresh observation on its next cycle.
	if purpose == TxPurposePricingSetPriceSnapshot {
		return 0, fmt.Errorf("outbox tx %d carries a pricing observation; use -resolution abandon and let the price bot rebuild", id)
	}
	// Cloning is new spend for the scope: while its pathway or chain is paused
	// or disabled the operator command is refused without mutating anything —
	// terminalizing the evidence row and queueing a clone here would schedule
	// automatic re-execution the moment the scope unpauses, which every other
	// retry path refuses to do.
	scopeGUID := []byte(nil)
	if guid != nil {
		scopeGUID = *guid
	}
	if err := lockTxSendScope(ctx, tx, chainEID, purpose, scopeGUID); err != nil {
		return 0, err
	}
	// A clone only makes sense while the owning workflow still waits on this
	// enqueued transaction; a concurrently advanced or parked job means the task
	// already completed elsewhere or needs the abandon path.
	if guid != nil && len(*guid) == common.HashLength {
		if err := requireWorkflowEnqueuedInTx(ctx, tx, purpose, common.BytesToHash(*guid)); err != nil {
			return 0, err
		}
	}
	if err := terminalizeExternallyConsumed(ctx, tx, id); err != nil {
		return 0, err
	}
	var cloneID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO tx_outbox (
			chain_eid, purpose, guid, to_address, calldata, value,
			signer_id, status, attempts, retry_of_id
		)
		SELECT chain_eid, purpose, guid, to_address, calldata, value,
			signer_id, $1, attempts + 1, id
		FROM tx_outbox WHERE id = $2
		RETURNING id
	`, TxStatusQueued, id).Scan(&cloneID); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return cloneID, nil
}

// ResolveExternalNonceAbandon terminates an externally consumed row without a
// clone and parks the owning worker job for manual review in the same
// transaction: the task will not be re-executed automatically.
func (s *Store) ResolveExternalNonceAbandon(ctx context.Context, id int64) error {
	if id <= 0 {
		return errors.New("outbox tx id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var purpose string
	var guid *[]byte
	if err := tx.QueryRow(ctx, `
		SELECT purpose, guid FROM tx_outbox WHERE id = $1 FOR UPDATE
	`, id).Scan(&purpose, &guid); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("outbox tx %d not found", id)
	} else if err != nil {
		return err
	}
	if err := terminalizeExternallyConsumed(ctx, tx, id); err != nil {
		return err
	}
	if guid != nil && len(*guid) == common.HashLength {
		if err := parkWorkflowManualReviewInTx(ctx, tx, purpose, common.BytesToHash(*guid), "transaction nonce consumed externally; task abandoned by operator"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func terminalizeExternallyConsumed(ctx context.Context, tx pgx.Tx, id int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET status = $1, held_reason = NULL, failure_kind = $2,
			next_retry_at = NULL, cancel_requested_at = NULL, cancel_defer_until = NULL,
			lease_token = NULL, lease_until = NULL, updated_at = now()
		WHERE id = $3 AND status = $4 AND held_reason = $5
			AND receipt_outcome IS NULL
	`, TxStatusFailed, TxFailureNonceConsumedExternally, id, TxStatusHeld, HeldNonceConsumedExternally)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox tx %d is not held as externally consumed", id)
	}
	return nil
}

// parkWorkflowManualReviewInTx moves the owning worker job to MANUAL_REVIEW for
// an abandoned task. Rows whose workflow already advanced past the enqueued
// state are left untouched (the chain outcome wins over the abandon).
func parkWorkflowManualReviewInTx(ctx context.Context, tx pgx.Tx, purpose string, guid common.Hash, reason string) error {
	switch purpose {
	case "executor_commit_verification":
		return parkExecutorManualReviewInTx(ctx, tx, guid, "COMMIT_TX_ENQUEUED", reason)
	case txPurposeExecutorLzReceive:
		return parkExecutorManualReviewInTx(ctx, tx, guid, "LZ_RECEIVE_TX_ENQUEUED", reason)
	case "dvn_verify":
		_, err := tx.Exec(ctx, `
			UPDATE dvn_jobs SET status = 'MANUAL_REVIEW', last_error = $1, updated_at = now()
			WHERE guid = $2 AND status = 'VERIFY_TX_ENQUEUED'
		`, reason, guid.Bytes())
		return err
	default:
		return nil
	}
}

func parkExecutorManualReviewInTx(ctx context.Context, tx pgx.Tx, guid common.Hash, expectedStatus, reason string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE executor_jobs SET status = 'MANUAL_REVIEW', last_error = $1, updated_at = now()
		WHERE guid = $2 AND status = $3
	`, reason, guid.Bytes(), expectedStatus)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		packetTag, err := tx.Exec(ctx, `
			UPDATE packets SET status = 'MANUAL_REVIEW', updated_at = now()
			WHERE guid = $1 AND status = $2
		`, guid.Bytes(), expectedStatus)
		if err != nil {
			return err
		}
		// Fail closed on a job/packet status mismatch rather than committing a
		// half-parked pair.
		if packetTag.RowsAffected() != 1 {
			return fmt.Errorf("packet %s is not in status %s while its executor job was", guid, expectedStatus)
		}
	}
	return nil
}

// requireWorkflowEnqueuedInTx locks the owning worker job and verifies it still
// waits on the enqueued transaction for this purpose.
func requireWorkflowEnqueuedInTx(ctx context.Context, tx pgx.Tx, purpose string, guid common.Hash) error {
	var table, expected string
	switch purpose {
	case "executor_commit_verification":
		table, expected = "executor_jobs", "COMMIT_TX_ENQUEUED"
	case txPurposeExecutorLzReceive:
		table, expected = "executor_jobs", "LZ_RECEIVE_TX_ENQUEUED"
	case "dvn_verify":
		table, expected = "dvn_jobs", "VERIFY_TX_ENQUEUED"
	default:
		return nil
	}
	var status string
	if err := tx.QueryRow(ctx, `
		SELECT status FROM `+table+` WHERE guid = $1 FOR UPDATE
	`, guid.Bytes()).Scan(&status); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("workflow job %s not found for purpose %s", guid, purpose)
	} else if err != nil {
		return err
	}
	if status != expected {
		return fmt.Errorf("workflow job %s advanced to %s; retry would duplicate a task no longer waiting on this transaction (use -resolution abandon)", guid, status)
	}
	return nil
}
