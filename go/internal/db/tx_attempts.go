package db

import (
	"context"
	"errors"
	"fmt"
	"math/big"
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

	if status == TxStatusQueued {
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
	if err := tx.QueryRow(ctx, `
		SELECT lease_token::text FROM tx_outbox WHERE id = $1 AND lease_until > now() FOR UPDATE
	`, outboxID).Scan(&curLease); errors.Is(err, pgx.ErrNoRows) {
		return TxAttempt{}, ErrOutboxLeaseLost
	} else if err != nil {
		return TxAttempt{}, err
	}
	if curLease == nil || *curLease != leaseToken.String() {
		return TxAttempt{}, ErrOutboxLeaseLost
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
			status = $6, lease_token = NULL, lease_until = NULL, updated_at = now()
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
// row is never broadcast a (cap+1)-th time. An exhausted still-unaccepted lane is
// held for manual review here.
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
	`, chainEID, signerID).Scan(&attemptID, &outboxID, &state, &nonce, &broadcastCount, &rawTx, &hashBytes, &kind, &outboxStatus, &purpose)
	if errors.Is(err, pgx.ErrNoRows) {
		return BroadcastClaim{}, ErrNoBroadcastCandidate
	}
	if err != nil {
		return BroadcastClaim{}, err
	}

	// A replay (already sent once) that reached the budget is not re-broadcast.
	if state == TxAttemptAmbiguous && broadcastCount >= TxMaxBroadcasts {
		if outboxStatus == TxStatusSigned {
			// Never accepted; park for manual review instead of retrying forever.
			if _, err := tx.Exec(ctx, `
				UPDATE tx_outbox SET status = $1, held_reason = $2, updated_at = now() WHERE id = $3
			`, TxStatusHeld, HeldManual, outboxID); err != nil {
				return BroadcastClaim{}, err
			}
		}
		// A broadcast row keeps its status for receipt polling and stale replacement.
		if err := tx.Commit(ctx); err != nil {
			return BroadcastClaim{}, err
		}
		return BroadcastClaim{}, ErrNoBroadcastCandidate
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

// ListReceiptCandidateAttempts returns attempts whose transaction may be on chain
// but is not yet terminal, so the receipt poller can query every known hash. It is
// keyed on attempt state (not outbox status) so a held row awaiting nonce
// reconciliation is still polled; signed attempts are included because a process
// can crash after a successful send but before the result is recorded.
func (s *Store) ListReceiptCandidateAttempts(ctx context.Context, chainEID uint32, signerID string, limit int) ([]TxAttempt, error) {
	if chainEID == 0 || signerID == "" {
		return nil, errors.New("chain and signer are required")
	}
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT a.id, a.outbox_id, a.kind, a.nonce, a.tx_type, a.tx_hash, a.state
		FROM tx_attempts a
		JOIN tx_outbox o ON o.id = a.outbox_id
		WHERE o.chain_eid = $1 AND o.signer_id = $2
			AND a.state IN ('signed', 'submitted', 'ambiguous')
			AND o.status NOT IN ('confirmed', 'failed')
		ORDER BY o.nonce ASC, a.id ASC
		LIMIT $3
	`, chainEID, signerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attempts := make([]TxAttempt, 0)
	for rows.Next() {
		var a TxAttempt
		var kind, state string
		var nonce int64
		var txType int16
		var hashBytes []byte
		if err := rows.Scan(&a.ID, &a.OutboxID, &kind, &nonce, &txType, &hashBytes, &state); err != nil {
			return nil, err
		}
		a.Kind = kind
		a.Nonce = uint64(nonce)
		a.TxType = uint8(txType)
		a.TxHash = common.BytesToHash(hashBytes)
		a.State = state
		attempts = append(attempts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return attempts, nil
}
