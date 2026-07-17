package db

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Durable-attempt model (P2-A #1). tx_outbox owns the logical task and nonce;
// each physical signed transaction is an immutable tx_attempts row. The outbox
// mirrors the active attempt's hash/gas/fees for existing readers.

const (
	// TxStatusHeld means the signer lane is blocked and needs reconciliation,
	// repricing, or manual review (see held_reason). No higher nonce is signed
	// or broadcast for the signer while a held row holds a nonce.
	TxStatusHeld = "held"
)

// Attempt kinds.
const (
	TxAttemptOriginal    = "original"
	TxAttemptReplacement = "replacement"
	TxAttemptCancel      = "cancel"
)

// Attempt aggregate states. Monotonic: once submitted/ambiguous an attempt is
// never downgraded to rejected, because the first send may already have landed.
const (
	TxAttemptSigned    = "signed"    // signed, no SendTransaction issued yet
	TxAttemptSubmitted = "submitted" // node accepted this exact hash
	TxAttemptAmbiguous = "ambiguous" // sent, acceptance undetermined (or later error)
	TxAttemptRejected  = "rejected"  // deterministic local failure before any RPC send
	TxAttemptMined     = "mined"     // receipt reached confirmation depth
)

// Broadcast error classes (see classifyBroadcastError in the txmgr).
const (
	SendErrorAccepted     = "accepted"
	SendErrorAmbiguous    = "ambiguous"
	SendErrorNonceTooLow  = "nonce_too_low"
	SendErrorNonceTooHigh = "nonce_too_high"
	SendErrorUnderpriced  = "underpriced"
	SendErrorRetryableEnv = "retryable_env"
	SendErrorDefinitive   = "definitive"
)

// Held reasons.
const (
	HeldNonceReconcileRequired = "nonce_reconcile_required"
	HeldRepriceRequired        = "reprice_required"
	HeldManual                 = "manual"
)

// ErrSignerLaneBlocked indicates a lower nonce for the same signer has not yet
// reached broadcast (or is held), so a higher nonce must not be signed or sent.
var ErrSignerLaneBlocked = errors.New("signer lane blocked by an earlier nonce")

// ErrOutboxLeaseLost indicates the signing lease expired or was taken by another
// worker before the attempt could be persisted.
var ErrOutboxLeaseLost = errors.New("outbox signing lease lost")

// ErrNoBroadcastCandidate indicates no attempt is due for broadcast.
var ErrNoBroadcastCandidate = errors.New("no broadcast candidate attempt")

// ErrBroadcastLaneHeld indicates the claim found no candidate but parked at least
// one never-accepted lane whose replay budget was exhausted (state changed).
var ErrBroadcastLaneHeld = errors.New("broadcast lane held after replay budget exhausted")

// ErrActiveAttemptChanged indicates the outbox active attempt moved between a
// replacement claim and its persistence, so the signed replacement is stale.
var ErrActiveAttemptChanged = errors.New("outbox active attempt changed")

// TxAttempt is one immutable physical signed transaction for an outbox row.
type TxAttempt struct {
	ID                   int64
	OutboxID             int64
	Kind                 string
	Nonce                uint64
	TxType               uint8
	TxHash               common.Hash
	RawTx                []byte
	GasLimit             uint64
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	State                string
	SendErrorClass       string
	SigningToken         uuid.UUID
	BroadcastCount       int64
	BroadcastLeaseToken  *uuid.UUID
	NextBroadcastAt      *time.Time
	LastBroadcastAt      *time.Time
}

// SignedAttempt carries the already-signed, already-verified transaction facts
// the txmgr persists. Raw tx hash/signer/params are verified in the txmgr (which
// holds chain id and signer address) before this is called.
type SignedAttempt struct {
	Kind                 string
	Nonce                uint64
	TxType               uint8
	TxHash               common.Hash
	RawTx                []byte
	GasLimit             uint64
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int // nil for legacy (tx_type 0)
	SigningToken         uuid.UUID
}

// signerLaneBlockedByLowerNonce reports whether a lower nonce for the signer has
// not yet reached broadcast (status in nonce_assigned/signed/held). Callers must
// already hold the signer advisory lock in tx.
func signerLaneBlockedByLowerNonce(ctx context.Context, tx pgx.Tx, chainEID uint32, signerID string, nonce uint64) (bool, error) {
	if nonce > maxDBNonce {
		return false, fmt.Errorf("nonce %d exceeds database limit", nonce)
	}
	var blocked bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM tx_outbox
			WHERE chain_eid = $1 AND signer_id = $2
				AND nonce IS NOT NULL AND nonce < $3
				AND status IN ('nonce_assigned', 'signed', 'held')
		)
	`, chainEID, signerID, int64(nonce)).Scan(&blocked)
	return blocked, err
}

// ClaimOutboxForSigning reserves a queued or nonce_assigned outbox row for
// signing a new attempt, under the signer advisory lock. It assigns a cursor
// nonce for queued rows (only when no earlier nonce is still in flight), enforces
// the low-nonce barrier, and writes a signing lease. The caller signs outside any
// transaction and then calls InsertSignedAttempt with the same lease token.
func (s *Store) ClaimOutboxForSigning(ctx context.Context, id int64, chainEID uint32, signerID string, leaseToken uuid.UUID, leaseTTL time.Duration) (OutboxTx, error) {
	if id <= 0 || chainEID == 0 || signerID == "" || leaseTTL <= 0 {
		return OutboxTx{}, errors.New("claim outbox for signing requires id, chain, signer, and positive ttl")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OutboxTx{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockSignerNonce(ctx, tx, chainEID, signerID); err != nil {
		return OutboxTx{}, err
	}

	var status string
	var nonce *int64
	err = tx.QueryRow(ctx, `
		SELECT status, nonce
		FROM tx_outbox
		WHERE id = $1 AND chain_eid = $2 AND signer_id = $3
			AND (lease_until IS NULL OR lease_until <= now())
		FOR UPDATE
	`, id, chainEID, signerID).Scan(&status, &nonce)
	if errors.Is(err, pgx.ErrNoRows) {
		return OutboxTx{}, ErrOutboxLeaseLost
	}
	if err != nil {
		return OutboxTx{}, err
	}
	if status != TxStatusQueued && status != TxStatusNonceAssigned {
		return OutboxTx{}, fmt.Errorf("outbox tx %d is not signable in status %s", id, status)
	}

	if status == TxStatusQueued && nonce == nil {
		// A fresh nonce is one past every assigned nonce, so any signer row still
		// short of broadcast blocks assigning it.
		var blocked bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM tx_outbox
				WHERE chain_eid = $1 AND signer_id = $2
					AND status IN ('nonce_assigned', 'signed', 'held')
			)
		`, chainEID, signerID).Scan(&blocked); err != nil {
			return OutboxTx{}, err
		}
		if blocked {
			return OutboxTx{}, ErrSignerLaneBlocked
		}
		next, err := s.claimCursorNonce(ctx, tx, chainEID, signerID)
		if err != nil {
			return OutboxTx{}, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tx_outbox SET nonce = $1, status = $2, updated_at = now() WHERE id = $3
		`, int64(next), TxStatusNonceAssigned, id); err != nil {
			return OutboxTx{}, err
		}
	} else if status == TxStatusQueued {
		// A requeued row keeps its previously assigned nonce; only the standard
		// low-nonce barrier applies.
		if *nonce < 0 {
			return OutboxTx{}, fmt.Errorf("outbox tx %d has a negative nonce", id)
		}
		blocked, err := signerLaneBlockedByLowerNonce(ctx, tx, chainEID, signerID, uint64(*nonce))
		if err != nil {
			return OutboxTx{}, err
		}
		if blocked {
			return OutboxTx{}, ErrSignerLaneBlocked
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tx_outbox SET status = $1, updated_at = now() WHERE id = $2
		`, TxStatusNonceAssigned, id); err != nil {
			return OutboxTx{}, err
		}
	} else {
		if nonce == nil || *nonce < 0 {
			return OutboxTx{}, fmt.Errorf("outbox tx %d nonce_assigned without a nonce", id)
		}
		blocked, err := signerLaneBlockedByLowerNonce(ctx, tx, chainEID, signerID, uint64(*nonce))
		if err != nil {
			return OutboxTx{}, err
		}
		if blocked {
			return OutboxTx{}, ErrSignerLaneBlocked
		}
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

// InsertSignedAttempt persists an already-signed attempt (state=signed), switches
// the outbox active pointer and mirror to it, and clears the signing lease, all
// in one transaction. It is idempotent on tx_hash for crash recovery: a matching
// existing attempt is returned. The signing lease must still be held.
func (s *Store) InsertSignedAttempt(ctx context.Context, outboxID int64, leaseToken uuid.UUID, a SignedAttempt) (TxAttempt, error) {
	if outboxID <= 0 {
		return TxAttempt{}, errors.New("outbox id is required")
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
	var outboxStatus string
	if err := tx.QueryRow(ctx, `
		SELECT lease_token::text, nonce, status FROM tx_outbox WHERE id = $1 AND lease_until > now() FOR UPDATE
	`, outboxID).Scan(&curLease, &outboxNonce, &outboxStatus); errors.Is(err, pgx.ErrNoRows) {
		return TxAttempt{}, ErrOutboxLeaseLost
	} else if err != nil {
		return TxAttempt{}, err
	}
	if curLease == nil || *curLease != leaseToken.String() {
		return TxAttempt{}, ErrOutboxLeaseLost
	}
	// The signing claim left the row nonce_assigned; anything else means the row
	// moved on while this signer held the lease and the attempt must not land.
	if outboxStatus != TxStatusNonceAssigned {
		return TxAttempt{}, ErrOutboxLeaseLost
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
		SET active_attempt_id = $1, tx_hash = $2, gas_limit = $3::numeric,
			max_fee_per_gas = $4::numeric, max_priority_fee_per_gas = $5::numeric,
			status = $6, lease_token = NULL, lease_until = NULL,
			pre_sign_failure_count = 0, next_sign_at = NULL, updated_at = now()
		WHERE id = $7
	`, attemptID, a.TxHash.Bytes(), int64(a.GasLimit), a.MaxFeePerGas.String(), priorityArg, TxStatusSigned, outboxID); err != nil {
		return TxAttempt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TxAttempt{}, err
	}
	return TxAttempt{ID: attemptID, OutboxID: outboxID, Kind: a.Kind, Nonce: a.Nonce, TxType: a.TxType, TxHash: a.TxHash, RawTx: a.RawTx, GasLimit: a.GasLimit, MaxFeePerGas: a.MaxFeePerGas, MaxPriorityFeePerGas: a.MaxPriorityFeePerGas, State: TxAttemptSigned, SigningToken: a.SigningToken}, nil
}

func scanAttemptByHash(ctx context.Context, tx pgx.Tx, hash common.Hash) (TxAttempt, bool, error) {
	var a TxAttempt
	var kind, state string
	var nonce, gasLimit int64
	var txType int16
	var hashBytes, rawTx []byte
	err := tx.QueryRow(ctx, `
		SELECT id, outbox_id, kind, nonce, tx_type, tx_hash, raw_tx, gas_limit, state
		FROM tx_attempts WHERE tx_hash = $1
	`, hash.Bytes()).Scan(&a.ID, &a.OutboxID, &kind, &nonce, &txType, &hashBytes, &rawTx, &gasLimit, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return TxAttempt{}, false, nil
	}
	if err != nil {
		return TxAttempt{}, false, err
	}
	a.Kind = kind
	a.Nonce = uint64(nonce)
	a.TxType = uint8(txType)
	a.TxHash = common.BytesToHash(hashBytes)
	a.RawTx = rawTx
	a.GasLimit = uint64(gasLimit)
	a.State = state
	return a, true, nil
}

const (
	txBroadcastReplayBaseDelay = 3 * time.Second
	txBroadcastReplayMaxDelay  = time.Minute
	// TxMaxBroadcasts bounds how many times the same raw is (re)broadcast before
	// a still-unaccepted signer lane is held or an accepted row is left to the
	// stale-broadcast replacement.
	TxMaxBroadcasts = int64(5)
)

func broadcastReplayDelay(count int64) time.Duration {
	delay := txBroadcastReplayBaseDelay
	for i := int64(1); i < count; i++ {
		delay *= 2
		if delay >= txBroadcastReplayMaxDelay {
			return txBroadcastReplayMaxDelay
		}
	}
	return delay
}

// mapSendClassToOutbox returns the outbox status and held_reason for a broadcast
// error class. The pair always satisfies the (status='held')=(held_reason NOT NULL)
// constraint.
func mapSendClassToOutbox(class string) (status string, heldReason any) {
	switch class {
	case SendErrorAccepted, SendErrorAmbiguous:
		return TxStatusBroadcast, nil
	case SendErrorNonceTooHigh, SendErrorRetryableEnv:
		return TxStatusSigned, nil
	case SendErrorUnderpriced:
		return TxStatusHeld, HeldRepriceRequired
	case SendErrorNonceTooLow:
		return TxStatusHeld, HeldNonceReconcileRequired
	default: // definitive
		return TxStatusHeld, HeldManual
	}
}

// BroadcastClaim is a reserved attempt ready to be (re)broadcast.
type BroadcastClaim struct {
	AttemptID int64
	OutboxID  int64
	Purpose   string
	Nonce     uint64
	TxHash    common.Hash
	RawTx     []byte
	Kind      string
}

// ClaimAttemptForBroadcast atomically reserves the lowest-nonce due active attempt
// for a signer, marks it ambiguous (from the moment of send there is a crash
// window), increments broadcast_count, and returns the raw tx to broadcast. Cap
// exhaustion and the reservation happen in the same advisory-lock transaction so a
// row is never broadcast a (cap+1)-th time. Exhausted lanes are handled in SQL so
// they never block a higher nonce that is still legal to broadcast: a
// never-accepted lane at the budget is parked held(manual) (ErrBroadcastLaneHeld
// when that was the only state change), and an exhausted accepted row simply stops
// matching (receipt polling and stale replacement keep covering it).
func (s *Store) ClaimAttemptForBroadcast(ctx context.Context, chainEID uint32, signerID string, broadcastToken uuid.UUID, leaseTTL time.Duration) (BroadcastClaim, error) {
	if chainEID == 0 || signerID == "" || leaseTTL <= 0 {
		return BroadcastClaim{}, errors.New("claim for broadcast requires chain, signer, and positive ttl")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return BroadcastClaim{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockSignerNonce(ctx, tx, chainEID, signerID); err != nil {
		return BroadcastClaim{}, err
	}

	parked, err := tx.Exec(ctx, `
		UPDATE tx_outbox o
		SET status = $3, held_reason = $4, updated_at = now()
		FROM tx_attempts a
		WHERE a.outbox_id = o.id AND o.active_attempt_id = a.id
			AND o.chain_eid = $1 AND o.signer_id = $2
			AND o.status = 'signed' AND o.held_reason IS NULL
			AND a.state = 'ambiguous' AND a.broadcast_count >= $5
			AND (a.broadcast_lease_until IS NULL OR a.broadcast_lease_until <= now())
	`, chainEID, signerID, TxStatusHeld, HeldManual, TxMaxBroadcasts)
	if err != nil {
		return BroadcastClaim{}, err
	}

	var attemptID, outboxID int64
	var state, outboxStatus, kind, purpose string
	var nonce, broadcastCount int64
	var rawTx, hashBytes []byte
	err = tx.QueryRow(ctx, `
		SELECT a.id, a.outbox_id, a.state, a.nonce, a.broadcast_count, a.raw_tx, a.tx_hash, a.kind, o.status, o.purpose
		FROM tx_attempts a
		JOIN tx_outbox o ON o.id = a.outbox_id AND o.active_attempt_id = a.id
		WHERE o.chain_eid = $1 AND o.signer_id = $2
			AND o.status IN ('signed', 'broadcast') AND o.held_reason IS NULL
			AND a.state IN ('signed', 'ambiguous')
			AND a.broadcast_count < $3
			AND (a.next_broadcast_at IS NULL OR a.next_broadcast_at <= now())
			AND (a.broadcast_lease_until IS NULL OR a.broadcast_lease_until <= now())
			AND NOT EXISTS (
				SELECT 1 FROM tx_outbox lower
				WHERE lower.chain_eid = o.chain_eid AND lower.signer_id = o.signer_id
					AND lower.nonce IS NOT NULL AND lower.nonce < o.nonce
					AND lower.status IN ('nonce_assigned', 'signed', 'held')
			)
		ORDER BY o.nonce ASC
		FOR UPDATE OF a SKIP LOCKED
		LIMIT 1
	`, chainEID, signerID, TxMaxBroadcasts).Scan(&attemptID, &outboxID, &state, &nonce, &broadcastCount, &rawTx, &hashBytes, &kind, &outboxStatus, &purpose)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return BroadcastClaim{}, err
		}
		if parked.RowsAffected() > 0 {
			return BroadcastClaim{}, ErrBroadcastLaneHeld
		}
		return BroadcastClaim{}, ErrNoBroadcastCandidate
	}
	if err != nil {
		return BroadcastClaim{}, err
	}

	newCount := broadcastCount + 1
	if _, err := tx.Exec(ctx, `
		UPDATE tx_attempts
		SET state = CASE WHEN state = 'signed' THEN 'ambiguous' ELSE state END,
			broadcast_count = $1,
			last_broadcast_at = now(),
			next_broadcast_at = now() + $2::interval,
			broadcast_lease_token = $3::uuid,
			broadcast_lease_until = now() + $4::interval,
			updated_at = now()
		WHERE id = $5
	`, newCount, pgInterval(broadcastReplayDelay(newCount)), broadcastToken.String(), pgInterval(leaseTTL), attemptID); err != nil {
		return BroadcastClaim{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return BroadcastClaim{}, err
	}
	return BroadcastClaim{AttemptID: attemptID, OutboxID: outboxID, Purpose: purpose, Nonce: uint64(nonce), TxHash: common.BytesToHash(hashBytes), RawTx: rawTx, Kind: kind}, nil
}

// MarkAttemptSendResult records a broadcast outcome. The attempt aggregate state
// is monotonic (accepted upgrades to submitted; nothing downgrades ambiguous),
// and the outbox action status is updated only while this attempt is active and
// the outbox is not already terminal. Requires the broadcast lease token.
func (s *Store) MarkAttemptSendResult(ctx context.Context, attemptID int64, broadcastToken uuid.UUID, class, sendErr string) error {
	if attemptID <= 0 || class == "" {
		return errors.New("mark attempt send result requires attempt id and class")
	}
	if len(sendErr) > 500 {
		sendErr = sendErr[:500]
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var outboxID int64
	var state string
	if err := tx.QueryRow(ctx, `
		SELECT outbox_id, state FROM tx_attempts
		WHERE id = $1 AND broadcast_lease_token = $2::uuid AND broadcast_lease_until > now()
		FOR UPDATE
	`, attemptID, broadcastToken.String()).Scan(&outboxID, &state); errors.Is(err, pgx.ErrNoRows) {
		return ErrOutboxLeaseLost
	} else if err != nil {
		return err
	}

	newState := state
	if class == SendErrorAccepted && state != TxAttemptMined {
		newState = TxAttemptSubmitted
	}
	var sendErrArg any
	if sendErr != "" {
		sendErrArg = sendErr
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tx_attempts
		SET state = $1, send_error_class = $2, send_error = $3,
			broadcast_lease_token = NULL, broadcast_lease_until = NULL, updated_at = now()
		WHERE id = $4
	`, newState, class, sendErrArg, attemptID); err != nil {
		return err
	}
	status, heldReason := mapSendClassToOutbox(class)
	if _, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET status = $1, held_reason = $2, updated_at = now()
		WHERE id = $3 AND active_attempt_id = $4 AND status NOT IN ('confirmed', 'failed')
	`, status, heldReason, outboxID, attemptID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReceiptPollTask is one non-terminal outbox row with every attempt whose
// transaction may be on chain, so the receipt poller can query all known hashes.
type ReceiptPollTask struct {
	Outbox   OutboxTx
	Attempts []TxAttempt
}

// ListReceiptPollTasks returns non-terminal outbox rows that own at least one
// persisted attempt, oldest receipt poll first so one receiptless attempt cannot
// starve the rest of the batch, each with all of its poll-worthy attempts. Held
// rows are included (a held lane awaiting nonce reconciliation is still polled),
// and signed attempts are included because a process can crash after a successful
// send but before the result is recorded.
func (s *Store) ListReceiptPollTasks(ctx context.Context, chainEID uint32, signerID string, limit int) ([]ReceiptPollTask, error) {
	if chainEID == 0 || signerID == "" {
		return nil, errors.New("chain and signer are required")
	}
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	rows, err := s.pool.Query(ctx, `
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
		WHERE chain_eid = $1 AND signer_id = $2
			AND status IN ('signed', 'broadcast', 'held')
			AND active_attempt_id IS NOT NULL
		ORDER BY last_receipt_poll_at ASC NULLS FIRST, id ASC
		LIMIT $3
	`, chainEID, signerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]ReceiptPollTask, 0)
	byOutbox := make(map[int64]int)
	outboxIDs := make([]int64, 0)
	for rows.Next() {
		var row outboxTxRow
		if err := rows.Scan(
			&row.ID, &row.ChainEID, &row.Purpose, &row.GUID, &row.ToAddress,
			&row.Calldata, &row.Value, &row.GasLimit, &row.MaxFeePerGas,
			&row.MaxPriorityFeePerGas, &row.Nonce, &row.TxHash, &row.SignerID,
			&row.Status, &row.Attempts, &row.FailureKind, &row.NextRetryAt,
			&row.RetryOfID, &row.ReceiptTxHash, &row.ReceiptStatus,
			&row.ReceiptBlockNumber, &row.ReceiptGasUsed, &row.ReceiptEffectiveGasPrice,
			&row.ReceiptGasCostDstWei, &row.ReceiptGasCostSrcWei,
			&row.ReceiptObservedAt, &row.ReceiptCostPricedAt,
		); err != nil {
			return nil, err
		}
		outboxTx, err := row.toOutboxTx()
		if err != nil {
			return nil, err
		}
		byOutbox[outboxTx.ID] = len(tasks)
		outboxIDs = append(outboxIDs, outboxTx.ID)
		tasks = append(tasks, ReceiptPollTask{Outbox: outboxTx})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return tasks, nil
	}

	attemptRows, err := s.pool.Query(ctx, `
		SELECT id, outbox_id, kind, nonce, tx_type, tx_hash, state
		FROM tx_attempts
		WHERE outbox_id = ANY($1)
			AND state IN ('signed', 'submitted', 'ambiguous')
		ORDER BY outbox_id ASC, id ASC
	`, outboxIDs)
	if err != nil {
		return nil, err
	}
	defer attemptRows.Close()
	for attemptRows.Next() {
		var a TxAttempt
		var kind, state string
		var nonce int64
		var txType int16
		var hashBytes []byte
		if err := attemptRows.Scan(&a.ID, &a.OutboxID, &kind, &nonce, &txType, &hashBytes, &state); err != nil {
			return nil, err
		}
		a.Kind = kind
		a.Nonce = uint64(nonce)
		a.TxType = uint8(txType)
		a.TxHash = common.BytesToHash(hashBytes)
		a.State = state
		index, ok := byOutbox[a.OutboxID]
		if !ok {
			return nil, fmt.Errorf("attempt %d references unselected outbox tx %d", a.ID, a.OutboxID)
		}
		tasks[index].Attempts = append(tasks[index].Attempts, a)
	}
	if err := attemptRows.Err(); err != nil {
		return nil, err
	}
	return tasks, nil
}

// TouchReceiptPoll advances the receipt polling fairness cursor for one outbox
// row after its attempts were queried, without touching updated_at (which feeds
// the stale-replacement window).
func (s *Store) TouchReceiptPoll(ctx context.Context, id int64) error {
	if id <= 0 {
		return errors.New("outbox tx id is required")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE tx_outbox SET last_receipt_poll_at = now() WHERE id = $1
	`, id)
	return err
}

// FinalizeAttemptReceipt applies a confirmation-depth receipt in one atomic
// transaction: the winning attempt becomes mined, the outbox mirror, active
// pointer, and receipt facts switch to it, the outbox reaches its terminal
// status (confirmed, or failed with receipt-retry metadata), and any signing
// lease or pending replacement request is cleared so an in-flight replacement
// signer cannot land on the terminal row. The caller must apply the idempotent
// workflow effects BEFORE calling this: until it commits, the attempt stays a
// receipt-poll candidate, so a crash in between simply replays the workflow.
// Locks are taken attempt first, then outbox, matching MarkAttemptSendResult.
func (s *Store) FinalizeAttemptReceipt(ctx context.Context, attemptID int64, facts TxReceiptFacts, failure error) error {
	if attemptID <= 0 {
		return errors.New("attempt id is required")
	}
	if err := validateTxReceiptFacts(facts); err != nil {
		return err
	}
	success := facts.Status == 1
	if success != (failure == nil) {
		return errors.New("receipt failure detail must accompany exactly the failed receipts")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var outboxID int64
	var hashBytes []byte
	var gasLimit, maxFee string
	var maxPriority *string
	if err := tx.QueryRow(ctx, `
		SELECT outbox_id, tx_hash, gas_limit::text, max_fee_per_gas::text, max_priority_fee_per_gas::text
		FROM tx_attempts
		WHERE id = $1
		FOR UPDATE
	`, attemptID).Scan(&outboxID, &hashBytes, &gasLimit, &maxFee, &maxPriority); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("tx attempt %d not found", attemptID)
	} else if err != nil {
		return err
	}
	if common.BytesToHash(hashBytes) != facts.TxHash {
		return fmt.Errorf("receipt tx hash %s does not match attempt %d hash", facts.TxHash, attemptID)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tx_attempts SET state = $1, updated_at = now() WHERE id = $2
	`, TxAttemptMined, attemptID); err != nil {
		return err
	}

	var attempts uint32
	if err := tx.QueryRow(ctx, `
		SELECT attempts FROM tx_outbox WHERE id = $1 FOR UPDATE
	`, outboxID).Scan(&attempts); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("outbox tx %d not found", outboxID)
	} else if err != nil {
		return err
	}
	status := TxStatusConfirmed
	var failureKindArg, retryAtArg, lastErrorArg any
	if !success {
		status = TxStatusFailed
		failureKindArg = TxFailureReceiptFailed
		if attempts < TxAutoRetryMaxAttempts {
			retryAtArg = time.Now().UTC().Add(autoRetryDelay(attempts))
		}
		lastErrorArg = optionalString(failure.Error())
	}
	var maxPriorityArg any
	if maxPriority != nil {
		maxPriorityArg = *maxPriority
	}
	tag, err := tx.Exec(ctx, `
		UPDATE tx_outbox
		SET
			active_attempt_id = $1,
			tx_hash = $2,
			gas_limit = $3::numeric,
			max_fee_per_gas = $4::numeric,
			max_priority_fee_per_gas = $5::numeric,
			receipt_tx_hash = $2,
			receipt_status = $6,
			receipt_block_number = $7,
			receipt_gas_used = $8::numeric,
			receipt_effective_gas_price = $9::numeric,
			receipt_gas_cost_dst_wei = $10::numeric,
			receipt_observed_at = now(),
			status = $12,
			held_reason = NULL,
			failure_kind = $13,
			next_retry_at = $14,
			last_error = $15,
			lease_token = NULL,
			lease_until = NULL,
			replace_requested_at = NULL,
			next_sign_at = NULL,
			updated_at = now()
		WHERE id = $11
	`, attemptID, facts.TxHash.Bytes(), gasLimit, maxFee, maxPriorityArg,
		int64(facts.Status), int64(facts.BlockNumber), strconv.FormatUint(facts.GasUsed, 10),
		facts.EffectiveGasPrice.String(), facts.GasCostDstWei.String(), outboxID,
		status, failureKindArg, retryAtArg, lastErrorArg)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox tx %d not found", outboxID)
	}
	return tx.Commit(ctx)
}

// TxMaxPreSignFailures bounds consecutive estimate, fee-quote, or signing
// failures for a row that already holds a nonce before its lane is parked for
// manual review; the destructive alternative (requeue and release the nonce)
// is what used to wedge the signer.
const TxMaxPreSignFailures = int32(5)

// TxMaxReplacements bounds how many replacement attempts one outbox row may
// accumulate before automatic replacement stops offering it.
const TxMaxReplacements = 5

// RecordPreSignFailure charges one pre-sign failure (estimate, fee quote, or
// signing) against the signing lease and releases the lease. A nonce_assigned
// lane at the budget is parked held(manual) so it never falls back to the
// destructive requeue path; a replacement signing failure (broadcast or held
// row) instead pushes the stale/request window so the original attempt keeps
// receipt polling. Returns whether the row was parked. Callers must not charge
// failures caused by their own shutdown (parent context cancellation).
func (s *Store) RecordPreSignFailure(ctx context.Context, id int64, leaseToken uuid.UUID) (bool, error) {
	if id <= 0 {
		return false, errors.New("outbox tx id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string
	var count int32
	if err := tx.QueryRow(ctx, `
		SELECT status, pre_sign_failure_count
		FROM tx_outbox
		WHERE id = $1 AND lease_token = $2::uuid AND lease_until > now()
		FOR UPDATE
	`, id, leaseToken.String()).Scan(&status, &count); errors.Is(err, pgx.ErrNoRows) {
		return false, ErrOutboxLeaseLost
	} else if err != nil {
		return false, err
	}

	newCount := count + 1
	switch status {
	case TxStatusNonceAssigned:
		if newCount >= TxMaxPreSignFailures {
			if _, err := tx.Exec(ctx, `
				UPDATE tx_outbox
				SET status = $1, held_reason = $2, pre_sign_failure_count = $3,
					next_sign_at = NULL, lease_token = NULL, lease_until = NULL, updated_at = now()
				WHERE id = $4
			`, TxStatusHeld, HeldManual, newCount, id); err != nil {
				return false, err
			}
			if err := tx.Commit(ctx); err != nil {
				return false, err
			}
			return true, nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tx_outbox
			SET pre_sign_failure_count = $1, next_sign_at = now() + $2::interval,
				lease_token = NULL, lease_until = NULL, updated_at = now()
			WHERE id = $3
		`, newCount, pgInterval(autoRetryDelay(uint32(count))), id); err != nil {
			return false, err
		}
		return false, tx.Commit(ctx)
	case TxStatusBroadcast, TxStatusHeld:
		if newCount >= TxMaxPreSignFailures {
			// The replacement signing budget is exhausted: park the lane visibly for
			// manual review instead of silently dropping out of the replacement
			// selector. Receipt polling still covers every persisted attempt.
			if _, err := tx.Exec(ctx, `
				UPDATE tx_outbox
				SET status = $1, held_reason = $2, pre_sign_failure_count = $3,
					replace_requested_at = NULL,
					lease_token = NULL, lease_until = NULL, updated_at = now()
				WHERE id = $4
			`, TxStatusHeld, HeldManual, newCount, id); err != nil {
				return false, err
			}
			if err := tx.Commit(ctx); err != nil {
				return false, err
			}
			return true, nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tx_outbox
			SET pre_sign_failure_count = $1,
				replace_requested_at = CASE
					WHEN replace_requested_at IS NOT NULL THEN now() + $2::interval
					ELSE NULL
				END,
				lease_token = NULL, lease_until = NULL, updated_at = now()
			WHERE id = $3
		`, newCount, pgInterval(autoRetryDelay(uint32(count))), id); err != nil {
			return false, err
		}
		return false, tx.Commit(ctx)
	default:
		return false, fmt.Errorf("outbox tx %d cannot record a pre-sign failure in status %s", id, status)
	}
}

// RequestTxReplacement records an operator request for a same-nonce replacement
// of the active attempt. It resets the pre-sign failure budget so an explicit
// request always gets fresh signing attempts. Only mempool-possible rows
// (broadcast) and reprice-held lanes are replaceable; held(manual) and
// held(nonce_reconcile_required) stay parked for reconciliation or cancel.
func (s *Store) RequestTxReplacement(ctx context.Context, id int64) error {
	if id <= 0 {
		return errors.New("outbox tx id is required")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET replace_requested_at = now(), pre_sign_failure_count = 0, updated_at = now()
		WHERE id = $1
			AND active_attempt_id IS NOT NULL
			AND (status = $2 OR (status = $3 AND held_reason = $4))
	`, id, TxStatusBroadcast, TxStatusHeld, HeldRepriceRequired)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox tx %d is not replaceable", id)
	}
	return nil
}

// txReplacementDeferDelay is how long an operator replacement request is pushed
// back when the replacement preflight defers (for example a fee cap).
const txReplacementDeferDelay = time.Minute

// DeferReplacement pushes a replacement candidate out of the stale window (and an
// operator request forward) after a preflight deferral such as a fee-cap hit, so
// the candidate is not retried on every manager pass. It charges no failure
// budget: a deferral is an environmental wait, not a signing failure.
func (s *Store) DeferReplacement(ctx context.Context, id int64) error {
	if id <= 0 {
		return errors.New("outbox tx id is required")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET updated_at = now(),
			replace_requested_at = CASE
				WHEN replace_requested_at IS NOT NULL THEN now() + $1::interval
				ELSE NULL
			END
		WHERE id = $2 AND status NOT IN ('confirmed', 'failed')
	`, pgInterval(txReplacementDeferDelay), id)
	return err
}

// ReplacementCandidate is one outbox row due for a same-nonce replacement, with
// every poll-worthy attempt hash so the caller can pre-check receipts across all
// of them (not only the active mirror) before signing a doomed replacement.
type ReplacementCandidate struct {
	Outbox          OutboxTx
	ActiveAttemptID int64
	AttemptHashes   []common.Hash
}

// NextReplacementCandidate peeks (without reserving) the next outbox row whose
// active attempt should be replaced: either a stale broadcast whose accepted or
// ambiguous attempt has gone staleAfter without a receipt, or an operator
// replacement request that has come due. Rows whose active attempt has not been
// sent yet are skipped (the pending signed attempt broadcasts first), as are rows
// at the replacement or pre-sign failure budget or under a signing lease.
func (s *Store) NextReplacementCandidate(ctx context.Context, chainEID uint32, signerID string, staleAfter time.Duration) (ReplacementCandidate, error) {
	if chainEID == 0 || signerID == "" {
		return ReplacementCandidate{}, errors.New("chain and signer are required")
	}
	if staleAfter <= 0 {
		return ReplacementCandidate{}, errors.New("stale broadcast duration must be positive")
	}
	var row outboxTxRow
	var activeAttemptID int64
	err := s.pool.QueryRow(ctx, `
		SELECT
			o.id, o.chain_eid, o.purpose, o.guid, o.to_address, o.calldata, o.value::text,
			o.gas_limit::text, o.max_fee_per_gas::text, o.max_priority_fee_per_gas::text,
			o.nonce, o.tx_hash, o.signer_id, o.status, o.attempts,
			o.failure_kind, o.next_retry_at, o.retry_of_id,
			o.receipt_tx_hash, o.receipt_status::text, o.receipt_block_number::text,
			o.receipt_gas_used::text, o.receipt_effective_gas_price::text,
			o.receipt_gas_cost_dst_wei::text, o.receipt_gas_cost_src_wei::text,
			o.receipt_observed_at, o.receipt_cost_priced_at,
			a.id
		FROM tx_outbox o
		JOIN tx_attempts a ON a.id = o.active_attempt_id
		WHERE o.chain_eid = $1 AND o.signer_id = $2
			AND (o.lease_until IS NULL OR o.lease_until <= now())
			AND a.state IN ('submitted', 'ambiguous')
			AND o.pre_sign_failure_count < $3
			AND (
				-- An operator request authorizes one replacement past the automatic
				-- cap (the request is cleared when the replacement is persisted).
				(
					o.replace_requested_at IS NOT NULL AND o.replace_requested_at <= now()
					AND (o.status = $4 OR (o.status = $6 AND o.held_reason = $7))
				)
				OR (
					o.status = $4 AND o.updated_at <= now() - $5::interval
					AND (
						SELECT count(*) FROM tx_attempts r
						WHERE r.outbox_id = o.id AND r.kind = $8
					) < $9
				)
			)
		ORDER BY o.updated_at, o.id
		LIMIT 1
	`, chainEID, signerID, TxMaxPreSignFailures, TxStatusBroadcast, pgInterval(staleAfter),
		TxStatusHeld, HeldRepriceRequired, TxAttemptReplacement, TxMaxReplacements).Scan(
		&row.ID, &row.ChainEID, &row.Purpose, &row.GUID, &row.ToAddress,
		&row.Calldata, &row.Value, &row.GasLimit, &row.MaxFeePerGas,
		&row.MaxPriorityFeePerGas, &row.Nonce, &row.TxHash, &row.SignerID,
		&row.Status, &row.Attempts, &row.FailureKind, &row.NextRetryAt,
		&row.RetryOfID, &row.ReceiptTxHash, &row.ReceiptStatus,
		&row.ReceiptBlockNumber, &row.ReceiptGasUsed, &row.ReceiptEffectiveGasPrice,
		&row.ReceiptGasCostDstWei, &row.ReceiptGasCostSrcWei,
		&row.ReceiptObservedAt, &row.ReceiptCostPricedAt,
		&activeAttemptID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReplacementCandidate{}, ErrNoStaleBroadcastReplacement
	}
	if err != nil {
		return ReplacementCandidate{}, err
	}
	outboxTx, err := row.toOutboxTx()
	if err != nil {
		return ReplacementCandidate{}, err
	}

	hashRows, err := s.pool.Query(ctx, `
		SELECT tx_hash FROM tx_attempts
		WHERE outbox_id = $1 AND state IN ('signed', 'submitted', 'ambiguous')
		ORDER BY id ASC
	`, outboxTx.ID)
	if err != nil {
		return ReplacementCandidate{}, err
	}
	defer hashRows.Close()
	hashes := make([]common.Hash, 0)
	for hashRows.Next() {
		var hashBytes []byte
		if err := hashRows.Scan(&hashBytes); err != nil {
			return ReplacementCandidate{}, err
		}
		hashes = append(hashes, common.BytesToHash(hashBytes))
	}
	if err := hashRows.Err(); err != nil {
		return ReplacementCandidate{}, err
	}
	return ReplacementCandidate{Outbox: outboxTx, ActiveAttemptID: activeAttemptID, AttemptHashes: hashes}, nil
}

// ClaimOutboxForReplacementSigning writes a signing lease for a same-nonce
// replacement of the expected active attempt. The nonce is already owned by the
// row, so no signer advisory lock is needed; the lease plus the active-attempt
// CAS keep concurrent replacers out.
func (s *Store) ClaimOutboxForReplacementSigning(ctx context.Context, id, expectedActiveAttemptID int64, leaseToken uuid.UUID, leaseTTL time.Duration) (OutboxTx, error) {
	if id <= 0 || expectedActiveAttemptID <= 0 || leaseTTL <= 0 {
		return OutboxTx{}, errors.New("replacement signing claim requires ids and a positive ttl")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OutboxTx{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string
	var heldReason *string
	var activeAttemptID *int64
	if err := tx.QueryRow(ctx, `
		SELECT status, held_reason, active_attempt_id
		FROM tx_outbox
		WHERE id = $1 AND (lease_until IS NULL OR lease_until <= now())
		FOR UPDATE
	`, id).Scan(&status, &heldReason, &activeAttemptID); errors.Is(err, pgx.ErrNoRows) {
		return OutboxTx{}, ErrOutboxLeaseLost
	} else if err != nil {
		return OutboxTx{}, err
	}
	if activeAttemptID == nil || *activeAttemptID != expectedActiveAttemptID {
		return OutboxTx{}, ErrActiveAttemptChanged
	}
	replaceable := status == TxStatusBroadcast ||
		(status == TxStatusHeld && heldReason != nil && *heldReason == HeldRepriceRequired)
	if !replaceable {
		return OutboxTx{}, fmt.Errorf("outbox tx %d is not replaceable in status %s", id, status)
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

// InsertReplacementAttempt persists an already-signed replacement attempt
// (state=signed), switches the outbox active pointer and mirror to it, clears any
// reprice hold and the replacement request, and releases the signing lease, all in
// one transaction. The active-attempt CAS rejects a replacement raced by another
// switch; tx_hash reinsertion is idempotent for crash recovery.
func (s *Store) InsertReplacementAttempt(ctx context.Context, outboxID, expectedActiveAttemptID int64, leaseToken uuid.UUID, a SignedAttempt) (TxAttempt, error) {
	if outboxID <= 0 || expectedActiveAttemptID <= 0 {
		return TxAttempt{}, errors.New("outbox and expected active attempt ids are required")
	}
	if a.Kind != TxAttemptReplacement {
		return TxAttempt{}, fmt.Errorf("replacement attempt kind %q must be %q", a.Kind, TxAttemptReplacement)
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
	if err := tx.QueryRow(ctx, `
		SELECT lease_token::text, nonce, active_attempt_id, status, held_reason
		FROM tx_outbox WHERE id = $1 AND lease_until > now()
		FOR UPDATE
	`, outboxID).Scan(&curLease, &outboxNonce, &activeAttemptID, &outboxStatus, &heldReason); errors.Is(err, pgx.ErrNoRows) {
		return TxAttempt{}, ErrOutboxLeaseLost
	} else if err != nil {
		return TxAttempt{}, err
	}
	if curLease == nil || *curLease != leaseToken.String() {
		return TxAttempt{}, ErrOutboxLeaseLost
	}
	if activeAttemptID == nil || *activeAttemptID != expectedActiveAttemptID {
		return TxAttempt{}, ErrActiveAttemptChanged
	}
	// The row must still be replaceable: a receipt terminalization or a hold that
	// raced this signature must win, or a terminal row's mirror would be switched
	// to a never-sent replacement.
	replaceable := outboxStatus == TxStatusBroadcast ||
		(outboxStatus == TxStatusHeld && heldReason != nil && *heldReason == HeldRepriceRequired)
	if !replaceable {
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
		SET active_attempt_id = $1, tx_hash = $2, gas_limit = $3::numeric,
			max_fee_per_gas = $4::numeric, max_priority_fee_per_gas = $5::numeric,
			status = CASE WHEN status = $6 THEN $7 ELSE status END,
			held_reason = NULL, replace_requested_at = NULL,
			lease_token = NULL, lease_until = NULL,
			pre_sign_failure_count = 0, next_sign_at = NULL, updated_at = now()
		WHERE id = $8
	`, attemptID, a.TxHash.Bytes(), int64(a.GasLimit), a.MaxFeePerGas.String(), priorityArg,
		TxStatusHeld, TxStatusSigned, outboxID); err != nil {
		return TxAttempt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TxAttempt{}, err
	}
	return TxAttempt{ID: attemptID, OutboxID: outboxID, Kind: a.Kind, Nonce: a.Nonce, TxType: a.TxType, TxHash: a.TxHash, RawTx: a.RawTx, GasLimit: a.GasLimit, MaxFeePerGas: a.MaxFeePerGas, MaxPriorityFeePerGas: a.MaxPriorityFeePerGas, State: TxAttemptSigned, SigningToken: a.SigningToken}, nil
}
