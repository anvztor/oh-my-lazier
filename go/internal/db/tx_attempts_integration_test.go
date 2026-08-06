package db

import (
	"context"
	"errors"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
)

func TestClaimOutboxForSigningInsertAttemptAndBarrier(t *testing.T) {
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer store.Close()
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
	const signerID = "0x5151515151515151515151515151515151515151"
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id=$1", signerID); err != nil {
		t.Fatalf("clean: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_nonce_cursors WHERE signer_id=$1", signerID); err != nil {
		t.Fatalf("clean cursor: %v", err)
	}
	if _, err := store.BootstrapTxNonceCursor(ctx, 40161, signerID, 9); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	newRow := func() int64 {
		id, err := store.EnqueueTx(ctx, TxRequest{ChainEID: 40161, Purpose: TxPurposePricingSetPriceSnapshot, To: common.HexToAddress("0x22"), Calldata: []byte{0x1}, Value: big.NewInt(0), SignerID: signerID})
		if err != nil {
			t.Fatalf("EnqueueTx: %v", err)
		}
		return id
	}
	id1 := newRow()
	id2 := newRow()

	token1 := uuid.New()
	row, err := store.ClaimOutboxForSigning(ctx, id1, 40161, signerID, token1, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutboxForSigning(id1) error = %v", err)
	}
	if row.Nonce != 9 || row.Status != TxStatusNonceAssigned {
		t.Fatalf("row nonce=%d status=%q, want 9/nonce_assigned", row.Nonce, row.Status)
	}

	// Barrier: id2 (queued) must be blocked while id1 is nonce_assigned (not broadcast).
	if _, err := store.ClaimOutboxForSigning(ctx, id2, 40161, signerID, uuid.New(), 30*time.Second); err == nil || err.Error() != ErrSignerLaneBlocked.Error() {
		t.Fatalf("ClaimOutboxForSigning(id2) error = %v, want ErrSignerLaneBlocked", err)
	}

	// Wrong lease token cannot insert.
	att := SignedAttempt{Kind: TxAttemptOriginal, Nonce: 9, TxType: 2, TxHash: common.HexToHash("0xaa"), RawTx: []byte{0xde, 0xad}, GasLimit: 21000, MaxFeePerGas: big.NewInt(2_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_000_000_000), SigningToken: uuid.New()}
	if _, err := store.InsertSignedAttempt(ctx, id1, uuid.New(), att); err == nil {
		t.Fatal("InsertSignedAttempt with wrong lease succeeded, want ErrOutboxLeaseLost")
	}

	// Correct lease inserts, sets active + mirror + status=signed + clears lease.
	got, err := store.InsertSignedAttempt(ctx, id1, token1, att)
	if err != nil {
		t.Fatalf("InsertSignedAttempt error = %v", err)
	}
	// Idempotent re-insert returns the same attempt.
	got2, err := store.InsertSignedAttempt(ctx, id1, token1, att)
	if err != nil || got2.ID != got.ID {
		t.Fatalf("idempotent re-insert = (%d, %v), want %d", got2.ID, err, got.ID)
	}
	after, err := store.GetOutboxTx(ctx, id1)
	if err != nil {
		t.Fatalf("GetOutboxTx: %v", err)
	}
	if after.Status != TxStatusSigned || after.TxHash != att.TxHash {
		t.Fatalf("after status=%q hash=%s, want signed + mirrored hash", after.Status, after.TxHash)
	}
	var activeID *int64
	var leaseToken *string
	if err := store.pool.QueryRow(ctx, "SELECT active_attempt_id, lease_token::text FROM tx_outbox WHERE id=$1", id1).Scan(&activeID, &leaseToken); err != nil {
		t.Fatalf("select active: %v", err)
	}
	if activeID == nil || *activeID != got.ID {
		t.Fatalf("active_attempt_id = %v, want %d", activeID, got.ID)
	}
	if leaseToken != nil {
		t.Fatalf("lease_token = %v, want cleared", *leaseToken)
	}
}

func TestClaimAttemptForBroadcastAndSendResult(t *testing.T) {
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer store.Close()
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
	const signerID = "0x5252525252525252525252525252525252525252"
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id=$1", signerID); err != nil {
		t.Fatalf("clean: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_nonce_cursors WHERE signer_id=$1", signerID); err != nil {
		t.Fatalf("clean cursor: %v", err)
	}
	if _, err := store.BootstrapTxNonceCursor(ctx, 40161, signerID, 3); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	id, err := store.EnqueueTx(ctx, TxRequest{ChainEID: 40161, Purpose: TxPurposePricingSetPriceSnapshot, To: common.HexToAddress("0x22"), Calldata: []byte{0x1}, Value: big.NewInt(0), SignerID: signerID})
	if err != nil {
		t.Fatalf("EnqueueTx: %v", err)
	}
	token := uuid.New()
	if _, err := store.ClaimOutboxForSigning(ctx, id, 40161, signerID, token, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForSigning: %v", err)
	}
	att := SignedAttempt{Kind: TxAttemptOriginal, Nonce: 3, TxType: 2, TxHash: common.HexToHash("0xbb"), RawTx: []byte{0xbe, 0xef}, GasLimit: 21000, MaxFeePerGas: big.NewInt(2_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_000_000_000), SigningToken: uuid.New()}
	if _, err := store.InsertSignedAttempt(ctx, id, token, att); err != nil {
		t.Fatalf("InsertSignedAttempt: %v", err)
	}

	// First broadcast claim: state -> ambiguous, count=1, raw returned.
	bt := uuid.New()
	claim, err := store.ClaimAttemptForBroadcast(ctx, 40161, signerID, bt, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast: %v", err)
	}
	if claim.TxHash != att.TxHash || string(claim.RawTx) != string(att.RawTx) {
		t.Fatalf("claim raw/hash mismatch")
	}
	var state string
	var count int64
	if err := store.pool.QueryRow(ctx, "SELECT state, broadcast_count FROM tx_attempts WHERE id=$1", claim.AttemptID).Scan(&state, &count); err != nil {
		t.Fatalf("select attempt: %v", err)
	}
	if state != TxAttemptAmbiguous || count != 1 {
		t.Fatalf("attempt state=%q count=%d, want ambiguous/1", state, count)
	}

	// A concurrent second claim while lease is held returns no candidate.
	if _, err := store.ClaimAttemptForBroadcast(ctx, 40161, signerID, uuid.New(), 30*time.Second); err == nil {
		t.Fatal("second claim while leased succeeded, want ErrNoBroadcastCandidate")
	}

	// Definitive send result -> attempt stays ambiguous (monotonic), outbox held(manual).
	if err := store.MarkAttemptSendResult(ctx, claim.AttemptID, bt, SendErrorDefinitive, "intrinsic gas too low"); err != nil {
		t.Fatalf("MarkAttemptSendResult: %v", err)
	}
	var aState, oStatus, heldReason string
	if err := store.pool.QueryRow(ctx, `
		SELECT a.state, o.status, COALESCE(o.held_reason,'') FROM tx_attempts a JOIN tx_outbox o ON o.id=a.outbox_id WHERE a.id=$1
	`, claim.AttemptID).Scan(&aState, &oStatus, &heldReason); err != nil {
		t.Fatalf("select after result: %v", err)
	}
	if aState != TxAttemptAmbiguous {
		t.Fatalf("attempt state = %q, want ambiguous (must not downgrade to rejected)", aState)
	}
	if oStatus != TxStatusHeld || heldReason != HeldManual {
		t.Fatalf("outbox status=%q held_reason=%q, want held/manual", oStatus, heldReason)
	}
	// Held outbox is not a broadcast candidate anymore.
	if _, err := store.ClaimAttemptForBroadcast(ctx, 40161, signerID, uuid.New(), 30*time.Second); err == nil {
		t.Fatal("held outbox was claimed for broadcast")
	}
}

// attemptHarness drives one signer lane through the durable-attempt store methods
// against a real Postgres.
type attemptHarness struct {
	t        *testing.T
	ctx      context.Context
	store    *Store
	signerID string
}

func newAttemptHarness(t *testing.T, signerID string, bootstrapNonce uint64) *attemptHarness {
	t.Helper()
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	store, err := Connect(ctx, databaseURL)
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
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id=$1", signerID); err != nil {
		t.Fatalf("clean outbox: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_nonce_cursors WHERE signer_id=$1", signerID); err != nil {
		t.Fatalf("clean cursor: %v", err)
	}
	if _, err := store.BootstrapTxNonceCursor(ctx, 40161, signerID, bootstrapNonce); err != nil {
		t.Fatalf("bootstrap cursor: %v", err)
	}
	return &attemptHarness{t: t, ctx: ctx, store: store, signerID: signerID}
}

func (h *attemptHarness) enqueue() int64 {
	h.t.Helper()
	id, err := h.store.EnqueueTx(h.ctx, TxRequest{ChainEID: 40161, Purpose: TxPurposePricingSetPriceSnapshot, To: common.HexToAddress("0x22"), Calldata: []byte{0x1}, Value: big.NewInt(0), SignerID: h.signerID})
	if err != nil {
		h.t.Fatalf("EnqueueTx: %v", err)
	}
	return id
}

// signAttempt claims the row for signing and persists an original attempt.
func (h *attemptHarness) signAttempt(id int64, nonce uint64, hash common.Hash) TxAttempt {
	h.t.Helper()
	return h.signAttemptOn(40161, id, nonce, hash)
}

// signAttemptOn is signAttempt for a row on an explicit chain.
func (h *attemptHarness) signAttemptOn(chainEID uint32, id int64, nonce uint64, hash common.Hash) TxAttempt {
	h.t.Helper()
	token := uuid.New()
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, chainEID, h.signerID, token, 30*time.Second); err != nil {
		h.t.Fatalf("ClaimOutboxForSigning(%d): %v", id, err)
	}
	attempt, err := h.store.InsertSignedAttempt(h.ctx, id, token, SignedAttempt{
		Kind: TxAttemptOriginal, Nonce: nonce, TxType: 2, TxHash: hash,
		RawTx: hash.Bytes(), GasLimit: 21000,
		MaxFeePerGas: big.NewInt(2_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_000_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		h.t.Fatalf("InsertSignedAttempt(%d): %v", id, err)
	}
	return attempt
}

// broadcastResult claims the next broadcast candidate, asserts it is the expected
// attempt, and records the send result.
func (h *attemptHarness) broadcastResult(attemptID int64, class string) {
	h.t.Helper()
	h.broadcastResultOn(40161, attemptID, class)
}

// broadcastResultOn is broadcastResult for a row on an explicit chain.
func (h *attemptHarness) broadcastResultOn(chainEID uint32, attemptID int64, class string) {
	h.t.Helper()
	token := uuid.New()
	claim, err := h.store.ClaimAttemptForBroadcast(h.ctx, chainEID, h.signerID, token, 30*time.Second)
	if err != nil {
		h.t.Fatalf("ClaimAttemptForBroadcast: %v", err)
	}
	if claim.AttemptID != attemptID {
		h.t.Fatalf("claimed attempt %d, want %d", claim.AttemptID, attemptID)
	}
	if err := h.store.MarkAttemptSendResult(h.ctx, claim.AttemptID, token, class, ""); err != nil {
		h.t.Fatalf("MarkAttemptSendResult: %v", err)
	}
}

func (h *attemptHarness) exhaustAttempt(attemptID int64) {
	h.t.Helper()
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_attempts
		SET broadcast_count = $1, next_broadcast_at = NULL,
			broadcast_lease_token = NULL, broadcast_lease_until = NULL
		WHERE id = $2
	`, TxMaxBroadcasts, attemptID); err != nil {
		h.t.Fatalf("exhaust attempt: %v", err)
	}
}

// heldReasonReported reports whether the held-lane stats contain the given
// reason (persistent or synthetic) for this harness signer with a non-zero count.
func (h *attemptHarness) heldReasonReported(reason string) bool {
	h.t.Helper()
	snapshot, err := h.store.Stats(h.ctx)
	if err != nil {
		h.t.Fatalf("Stats() error = %v", err)
	}
	for _, stat := range snapshot.TxOutboxHeld {
		if stat.ChainEID == 40161 && stat.SignerID == h.signerID && stat.HeldReason == reason && stat.Count > 0 {
			return true
		}
	}
	return false
}

func (h *attemptHarness) outboxState(id int64) (status, heldReason string, activeAttemptID int64) {
	h.t.Helper()
	if err := h.store.pool.QueryRow(h.ctx, `
		SELECT status, COALESCE(held_reason, ''), COALESCE(active_attempt_id, 0) FROM tx_outbox WHERE id = $1
	`, id).Scan(&status, &heldReason, &activeAttemptID); err != nil {
		h.t.Fatalf("outbox state: %v", err)
	}
	return status, heldReason, activeAttemptID
}

func TestClaimAttemptForBroadcastParksExhaustedSignedLane(t *testing.T) {
	h := newAttemptHarness(t, "0x5353535353535353535353535353535353535353", 7)
	id := h.enqueue()
	attempt := h.signAttempt(id, 7, common.HexToHash("0xc1"))
	// Send once, node keeps the acceptance undetermined; the outbox stays signed.
	h.broadcastResult(attempt.ID, SendErrorRetryableEnv)
	h.exhaustAttempt(attempt.ID)

	_, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, uuid.New(), 30*time.Second)
	if !errors.Is(err, ErrBroadcastLaneHeld) {
		t.Fatalf("ClaimAttemptForBroadcast error = %v, want ErrBroadcastLaneHeld", err)
	}
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldBroadcastExhausted {
		t.Fatalf("outbox status=%q held_reason=%q, want held/broadcast_exhausted", status, heldReason)
	}
	// The park is the only state change; the next claim reports no candidate.
	if _, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrNoBroadcastCandidate) {
		t.Fatalf("second claim error = %v, want ErrNoBroadcastCandidate", err)
	}
}

func TestClaimAttemptForBroadcastSkipsExhaustedBroadcastRow(t *testing.T) {
	h := newAttemptHarness(t, "0x5454545454545454545454545454545454545454", 11)
	id1 := h.enqueue()
	attempt1 := h.signAttempt(id1, 11, common.HexToHash("0xd1"))
	// Ambiguous acceptance moves the outbox to broadcast; then the replay budget runs out.
	h.broadcastResult(attempt1.ID, SendErrorAmbiguous)
	h.exhaustAttempt(attempt1.ID)

	id2 := h.enqueue()
	attempt2 := h.signAttempt(id2, 12, common.HexToHash("0xd2"))

	// The exhausted broadcast row must not shadow the legal higher-nonce candidate.
	token := uuid.New()
	claim, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, token, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast error = %v, want claim of the higher nonce", err)
	}
	if claim.AttemptID != attempt2.ID || claim.OutboxID != id2 {
		t.Fatalf("claimed attempt %d outbox %d, want %d/%d", claim.AttemptID, claim.OutboxID, attempt2.ID, id2)
	}
	// The exhausted accepted row keeps its broadcast status for receipt polling.
	status, heldReason, _ := h.outboxState(id1)
	if status != TxStatusBroadcast || heldReason != "" {
		t.Fatalf("exhausted broadcast row status=%q held_reason=%q, want broadcast/none", status, heldReason)
	}
}

func TestRecordPreSignFailureBudget(t *testing.T) {
	h := newAttemptHarness(t, "0x5555555555555555555555555555555555555555", 21)
	id := h.enqueue()

	// Wrong lease token cannot charge the budget.
	token := uuid.New()
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, token, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForSigning: %v", err)
	}
	if _, err := h.store.RecordPreSignFailure(h.ctx, id, uuid.New()); !errors.Is(err, ErrOutboxLeaseLost) {
		t.Fatalf("RecordPreSignFailure(wrong token) error = %v, want ErrOutboxLeaseLost", err)
	}

	for i := int32(1); i < TxMaxPreSignFailures; i++ {
		held, err := h.store.RecordPreSignFailure(h.ctx, id, token)
		if err != nil {
			t.Fatalf("RecordPreSignFailure #%d: %v", i, err)
		}
		if held {
			t.Fatalf("RecordPreSignFailure #%d parked the lane before the budget", i)
		}
		var count int32
		var nextSignAt *time.Time
		var leaseToken *string
		if err := h.store.pool.QueryRow(h.ctx, `
			SELECT pre_sign_failure_count, next_sign_at, lease_token::text FROM tx_outbox WHERE id = $1
		`, id).Scan(&count, &nextSignAt, &leaseToken); err != nil {
			t.Fatalf("select budget: %v", err)
		}
		if count != i || nextSignAt == nil || leaseToken != nil {
			t.Fatalf("after failure #%d: count=%d next_sign_at=%v lease=%v, want count=%d, backoff set, lease cleared", i, count, nextSignAt, leaseToken, i)
		}
		// The backoff keeps the row out of the sendable peek.
		if _, err := h.store.PeekSendableTx(h.ctx, 40161, h.signerID); err == nil {
			t.Fatalf("PeekSendableTx returned a row inside the pre-sign backoff window")
		}
		// Re-claim for the next signing attempt (claim by id ignores the backoff).
		token = uuid.New()
		if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, token, 30*time.Second); err != nil {
			t.Fatalf("re-claim #%d: %v", i, err)
		}
	}

	held, err := h.store.RecordPreSignFailure(h.ctx, id, token)
	if err != nil {
		t.Fatalf("RecordPreSignFailure at cap: %v", err)
	}
	if !held {
		t.Fatal("RecordPreSignFailure at cap did not park the lane")
	}
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldManual {
		t.Fatalf("outbox status=%q held_reason=%q, want held/manual", status, heldReason)
	}
}

func TestReplacementClaimInsertAndSwitch(t *testing.T) {
	h := newAttemptHarness(t, "0x5656565656565656565656565656565656565656", 31)
	id := h.enqueue()
	original := h.signAttempt(id, 31, common.HexToHash("0xe1"))
	h.broadcastResult(original.ID, SendErrorAccepted)

	// Not yet stale: no candidate.
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(fresh) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET updated_at = now() - interval '16 minutes' WHERE id = $1`, id); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	candidate, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(stale): %v", err)
	}
	if candidate.Outbox.ID != id || candidate.ActiveAttemptID != original.ID {
		t.Fatalf("candidate outbox=%d active=%d, want %d/%d", candidate.Outbox.ID, candidate.ActiveAttemptID, id, original.ID)
	}
	if len(candidate.AttemptHashes) != 1 || candidate.AttemptHashes[0] != original.TxHash {
		t.Fatalf("candidate hashes = %v, want the original attempt hash", candidate.AttemptHashes)
	}

	leaseToken := uuid.New()
	if _, err := h.store.ClaimOutboxForReplacementSigning(h.ctx, id, original.ID, leaseToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForReplacementSigning: %v", err)
	}
	// The signing lease excludes concurrent replacers.
	if _, err := h.store.ClaimOutboxForReplacementSigning(h.ctx, id, original.ID, uuid.New(), 30*time.Second); !errors.Is(err, ErrOutboxLeaseLost) {
		t.Fatalf("second replacement claim error = %v, want ErrOutboxLeaseLost", err)
	}

	replacement := SignedAttempt{
		Kind: TxAttemptReplacement, Nonce: 31, TxType: 2, TxHash: common.HexToHash("0xe2"),
		RawTx: []byte{0xe2, 0x02}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(3_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		SigningToken: uuid.New(),
	}
	if _, err := h.store.InsertReplacementAttempt(h.ctx, id, original.ID+9999, leaseToken, replacement); !errors.Is(err, ErrActiveAttemptChanged) {
		t.Fatalf("InsertReplacementAttempt(wrong active) error = %v, want ErrActiveAttemptChanged", err)
	}
	inserted, err := h.store.InsertReplacementAttempt(h.ctx, id, original.ID, leaseToken, replacement)
	if err != nil {
		t.Fatalf("InsertReplacementAttempt: %v", err)
	}
	status, _, activeID := h.outboxState(id)
	if status != TxStatusBroadcast || activeID != inserted.ID {
		t.Fatalf("after replacement: status=%q active=%d, want broadcast/%d", status, activeID, inserted.ID)
	}
	after, err := h.store.GetOutboxTx(h.ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx: %v", err)
	}
	if after.TxHash != replacement.TxHash || after.MaxFeePerGas.Cmp(replacement.MaxFeePerGas) != 0 {
		t.Fatalf("mirror hash=%s fee=%s, want replacement values", after.TxHash, after.MaxFeePerGas)
	}

	// The signed replacement is the next broadcast candidate.
	token := uuid.New()
	claim, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, token, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast(replacement): %v", err)
	}
	if claim.AttemptID != inserted.ID || claim.Kind != TxAttemptReplacement {
		t.Fatalf("claimed attempt %d kind %q, want the replacement %d", claim.AttemptID, claim.Kind, inserted.ID)
	}
}

func TestRequestTxReplacementFromRepriceHold(t *testing.T) {
	h := newAttemptHarness(t, "0x5757575757575757575757575757575757575757", 41)
	id := h.enqueue()
	original := h.signAttempt(id, 41, common.HexToHash("0xf1"))
	h.broadcastResult(original.ID, SendErrorUnderpriced)
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldRepriceRequired {
		t.Fatalf("outbox status=%q held_reason=%q, want held/reprice_required", status, heldReason)
	}

	// A fresh reprice hold is inside the automatic cooldown window.
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(inside cooldown) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	// An operator request bypasses the cooldown.
	if err := h.store.RequestTxReplacement(h.ctx, id); err != nil {
		t.Fatalf("RequestTxReplacement: %v", err)
	}
	candidate, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(requested): %v", err)
	}
	if candidate.Outbox.ID != id {
		t.Fatalf("candidate outbox = %d, want %d", candidate.Outbox.ID, id)
	}
	// Once the cooldown has passed, the reprice hold recovers automatically,
	// with no operator request at all.
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET replace_requested_at = NULL WHERE id = $1`, id); err != nil {
		t.Fatalf("clear request: %v", err)
	}
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(cooldown, request cleared) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET updated_at = now() - interval '2 minutes' WHERE id = $1`, id); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	candidate, err = h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(auto reprice): %v", err)
	}
	if candidate.Outbox.ID != id {
		t.Fatalf("auto reprice candidate = %d, want %d", candidate.Outbox.ID, id)
	}

	leaseToken := uuid.New()
	if _, err := h.store.ClaimOutboxForReplacementSigning(h.ctx, id, original.ID, leaseToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForReplacementSigning: %v", err)
	}
	inserted, err := h.store.InsertReplacementAttempt(h.ctx, id, original.ID, leaseToken, SignedAttempt{
		Kind: TxAttemptReplacement, Nonce: 41, TxType: 2, TxHash: common.HexToHash("0xf2"),
		RawTx: []byte{0xf2, 0x02}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(4_000_000_000), MaxPriorityFeePerGas: big.NewInt(2_000_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertReplacementAttempt: %v", err)
	}
	status, heldReason, activeID := h.outboxState(id)
	if status != TxStatusSigned || heldReason != "" || activeID != inserted.ID {
		t.Fatalf("after reprice: status=%q held_reason=%q active=%d, want signed/none/%d", status, heldReason, activeID, inserted.ID)
	}
	var requestedAt *time.Time
	if err := h.store.pool.QueryRow(h.ctx, `SELECT replace_requested_at FROM tx_outbox WHERE id = $1`, id).Scan(&requestedAt); err != nil {
		t.Fatalf("select request: %v", err)
	}
	if requestedAt != nil {
		t.Fatalf("replace_requested_at = %v, want cleared", requestedAt)
	}

	// held(manual) lanes are not replaceable.
	heldID := h.enqueue()
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET status = 'held', held_reason = 'manual' WHERE id = $1`, heldID); err != nil {
		t.Fatalf("force held: %v", err)
	}
	if err := h.store.RequestTxReplacement(h.ctx, heldID); err == nil {
		t.Fatal("RequestTxReplacement on held(manual) succeeded, want error")
	}
}

func TestFinalizeAttemptReceiptSwitchesActiveAndTerminalizes(t *testing.T) {
	h := newAttemptHarness(t, "0x5858585858585858585858585858585858585858", 51)
	id := h.enqueue()
	original := h.signAttempt(id, 51, common.HexToHash("0xa1a1"))
	h.broadcastResult(original.ID, SendErrorAccepted)
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET updated_at = now() - interval '16 minutes' WHERE id = $1`, id); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	leaseToken := uuid.New()
	if _, err := h.store.ClaimOutboxForReplacementSigning(h.ctx, id, original.ID, leaseToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForReplacementSigning: %v", err)
	}
	replacement, err := h.store.InsertReplacementAttempt(h.ctx, id, original.ID, leaseToken, SignedAttempt{
		Kind: TxAttemptReplacement, Nonce: 51, TxType: 2, TxHash: common.HexToHash("0xa2a2"),
		RawTx: []byte{0xa2, 0x02}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(3_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertReplacementAttempt: %v", err)
	}
	// A pending replacement signing lease must not survive terminalization.
	staleLease := uuid.New()
	if _, err := h.store.ClaimOutboxForReplacementSigning(h.ctx, id, replacement.ID, staleLease, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForReplacementSigning(stale): %v", err)
	}

	// The receipt hash must match the attempt being terminalized.
	badFacts := TxReceiptFacts{TxHash: replacement.TxHash, Status: 1, BlockNumber: 100, GasUsed: 21000, EffectiveGasPrice: big.NewInt(1_000_000_000), GasCostDstWei: new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000))}
	if _, err := h.store.FinalizeAttemptReceipt(h.ctx, original.ID, badFacts); err == nil {
		t.Fatal("FinalizeAttemptReceipt with a mismatched hash succeeded")
	}

	// The non-active original wins the receipt race: the whole terminalization is
	// one transaction (mined attempt, active/mirror switch, terminal status, lease
	// cleanup), so no crash window can strand a mined-but-non-terminal row.
	facts := badFacts
	facts.TxHash = original.TxHash
	if outcome, err := h.store.PrepareReceiptResolution(h.ctx, original.ID, facts); err != nil || outcome != ReceiptOutcomeConfirmed {
		t.Fatalf("PrepareReceiptResolution = (%q, %v), want confirmed", outcome, err)
	}
	if outcome, err := h.store.FinalizeAttemptReceipt(h.ctx, original.ID, facts); err != nil || outcome != ReceiptOutcomeConfirmed {
		t.Fatalf("FinalizeAttemptReceipt = (%q, %v), want confirmed", outcome, err)
	}
	var attemptState string
	if err := h.store.pool.QueryRow(h.ctx, `SELECT state FROM tx_attempts WHERE id = $1`, original.ID).Scan(&attemptState); err != nil {
		t.Fatalf("select attempt: %v", err)
	}
	if attemptState != TxAttemptMined {
		t.Fatalf("attempt state = %q, want mined", attemptState)
	}
	status, _, activeID := h.outboxState(id)
	if status != TxStatusConfirmed || activeID != original.ID {
		t.Fatalf("after finalize: status=%q active=%d, want confirmed/%d (winning attempt)", status, activeID, original.ID)
	}
	after, err := h.store.GetOutboxTx(h.ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx: %v", err)
	}
	if after.TxHash != original.TxHash || after.ReceiptTxHash != original.TxHash {
		t.Fatalf("mirror hash=%s receipt hash=%s, want the winning attempt hash", after.TxHash, after.ReceiptTxHash)
	}
	if after.ReceiptBlockNumber == nil || *after.ReceiptBlockNumber != 100 {
		t.Fatalf("receipt block = %v, want 100", after.ReceiptBlockNumber)
	}
	var leaseTokenValue *string
	var requestedAt *time.Time
	if err := h.store.pool.QueryRow(h.ctx, `SELECT lease_token::text, replace_requested_at FROM tx_outbox WHERE id = $1`, id).Scan(&leaseTokenValue, &requestedAt); err != nil {
		t.Fatalf("select lease: %v", err)
	}
	if leaseTokenValue != nil || requestedAt != nil {
		t.Fatalf("lease=%v request=%v, want both cleared by the finalizer", leaseTokenValue, requestedAt)
	}

	// The raced replacement signer cannot land its attempt on the terminal row.
	if _, err := h.store.InsertReplacementAttempt(h.ctx, id, replacement.ID, staleLease, SignedAttempt{
		Kind: TxAttemptReplacement, Nonce: 51, TxType: 2, TxHash: common.HexToHash("0xa3a3"),
		RawTx: []byte{0xa3, 0x03}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(4_000_000_000), MaxPriorityFeePerGas: big.NewInt(2_000_000_000),
		SigningToken: uuid.New(),
	}); !errors.Is(err, ErrOutboxLeaseLost) {
		t.Fatalf("InsertReplacementAttempt(after finalize) error = %v, want ErrOutboxLeaseLost", err)
	}
	status, _, activeID = h.outboxState(id)
	if status != TxStatusConfirmed || activeID != original.ID {
		t.Fatalf("terminal row changed by raced replacement: status=%q active=%d", status, activeID)
	}
}

func TestFinalizeAttemptReceiptFailedReceiptKeepsRetryMetadata(t *testing.T) {
	h := newAttemptHarness(t, "0x5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a", 71)

	// A packet-scoped task keeps its retry window on a failed receipt.
	_, id := seedScopedPacketRow(t, h, 71)
	attempt := h.signAttemptOn(40449, id, 71, common.HexToHash("0xc1c1"))
	h.broadcastResultOn(40449, attempt.ID, SendErrorAccepted)

	facts := TxReceiptFacts{TxHash: attempt.TxHash, Status: 0, BlockNumber: 200, GasUsed: 21000, EffectiveGasPrice: big.NewInt(1_000_000_000), GasCostDstWei: new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000))}
	if outcome, err := h.store.PrepareReceiptResolution(h.ctx, attempt.ID, facts); err != nil || outcome != ReceiptOutcomeFailed {
		t.Fatalf("PrepareReceiptResolution = (%q, %v), want receipt_failed", outcome, err)
	}
	if outcome, err := h.store.FinalizeAttemptReceipt(h.ctx, attempt.ID, facts); err != nil || outcome != ReceiptOutcomeFailed {
		t.Fatalf("FinalizeAttemptReceipt = (%q, %v), want receipt_failed", outcome, err)
	}
	after, err := h.store.GetOutboxTx(h.ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx: %v", err)
	}
	if after.Status != TxStatusFailed || after.FailureKind != TxFailureReceiptFailed || after.NextRetryAt == nil {
		t.Fatalf("failed receipt = %q/%q/%v, want failed/receipt_failed with a due retry", after.Status, after.FailureKind, after.NextRetryAt)
	}
	if after.ReceiptTxHash != attempt.TxHash {
		t.Fatalf("receipt hash = %s, want %s", after.ReceiptTxHash, attempt.TxHash)
	}

	// A pricing row is terminal on a failed receipt: no retry window, because
	// its calldata carries a time-bound observation the retry paths refuse.
	pricingID := h.enqueue()
	pricingAttempt := h.signAttempt(pricingID, 71, common.HexToHash("0xc1c2"))
	h.broadcastResult(pricingAttempt.ID, SendErrorAccepted)
	pricingFacts := TxReceiptFacts{TxHash: pricingAttempt.TxHash, Status: 0, BlockNumber: 201, GasUsed: 21000, EffectiveGasPrice: big.NewInt(1_000_000_000), GasCostDstWei: new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000))}
	if outcome, err := h.store.PrepareReceiptResolution(h.ctx, pricingAttempt.ID, pricingFacts); err != nil || outcome != ReceiptOutcomeFailed {
		t.Fatalf("PrepareReceiptResolution(pricing) = (%q, %v), want receipt_failed", outcome, err)
	}
	if outcome, err := h.store.FinalizeAttemptReceipt(h.ctx, pricingAttempt.ID, pricingFacts); err != nil || outcome != ReceiptOutcomeFailed {
		t.Fatalf("FinalizeAttemptReceipt(pricing) = (%q, %v), want receipt_failed", outcome, err)
	}
	pricingAfter, err := h.store.GetOutboxTx(h.ctx, pricingID)
	if err != nil {
		t.Fatalf("GetOutboxTx(pricing): %v", err)
	}
	if pricingAfter.Status != TxStatusFailed || pricingAfter.FailureKind != TxFailureReceiptFailed || pricingAfter.NextRetryAt != nil {
		t.Fatalf("pricing failed receipt = %q/%q/%v, want terminal failed/receipt_failed without a retry window", pricingAfter.Status, pricingAfter.FailureKind, pricingAfter.NextRetryAt)
	}
}

func TestMarkQueuedTxEstimateRevertFailedLosesRaces(t *testing.T) {
	h := newAttemptHarness(t, "0x5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b", 81)

	// A pristine queued pricing row takes the deterministic failure but gets no
	// retry window: its calldata carries a time-bound observation.
	pristine := h.enqueue()
	applied, err := h.store.MarkQueuedTxEstimateRevertFailed(h.ctx, pristine, errors.New("estimate gas reverted"))
	if err != nil || !applied {
		t.Fatalf("MarkQueuedTxEstimateRevertFailed(pristine) = (%t, %v), want applied", applied, err)
	}
	failed, err := h.store.GetOutboxTx(h.ctx, pristine)
	if err != nil {
		t.Fatalf("GetOutboxTx: %v", err)
	}
	if failed.Status != TxStatusFailed || failed.FailureKind != TxFailureEstimateGasRevert || failed.NextRetryAt != nil {
		t.Fatalf("pristine pricing row = %q/%q/%v, want terminal estimate failure without a retry window", failed.Status, failed.FailureKind, failed.NextRetryAt)
	}

	// A packet-scoped task keeps the automatic retry window.
	_, packetID := seedScopedPacketRow(t, h, 81)
	applied, err = h.store.MarkQueuedTxEstimateRevertFailed(h.ctx, packetID, errors.New("estimate gas reverted"))
	if err != nil || !applied {
		t.Fatalf("MarkQueuedTxEstimateRevertFailed(packet) = (%t, %v), want applied", applied, err)
	}
	packetFailed, err := h.store.GetOutboxTx(h.ctx, packetID)
	if err != nil {
		t.Fatalf("GetOutboxTx(packet): %v", err)
	}
	if packetFailed.Status != TxStatusFailed || packetFailed.FailureKind != TxFailureEstimateGasRevert || packetFailed.NextRetryAt == nil {
		t.Fatalf("packet row = %q/%q/%v, want retryable estimate failure", packetFailed.Status, packetFailed.FailureKind, packetFailed.NextRetryAt)
	}

	// A row another instance meanwhile claimed for signing must not be overwritten
	// into failed: the estimate result is stale.
	claimed := h.enqueue()
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, claimed, 40161, h.signerID, uuid.New(), 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForSigning: %v", err)
	}
	applied, err = h.store.MarkQueuedTxEstimateRevertFailed(h.ctx, claimed, errors.New("estimate gas reverted"))
	if err != nil {
		t.Fatalf("MarkQueuedTxEstimateRevertFailed(claimed): %v", err)
	}
	if applied {
		t.Fatal("stale estimate revert overwrote a claimed row")
	}
	status, _, _ := h.outboxState(claimed)
	if status != TxStatusNonceAssigned {
		t.Fatalf("claimed row status = %q, want nonce_assigned", status)
	}
}

// TestHeldStatsReportRepriceExhaustion pins the readiness linkage for reprice
// holds: below the automatic replacement cap they report reprice_required
// (self-healing), at the cap they report the synthetic reprice_exhausted
// reason (operator action), and cancel-intent rows stay under the cancel
// pipeline reporting.
func TestHeldStatsReportRepriceExhaustion(t *testing.T) {
	h := newAttemptHarness(t, "0x6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d", 141)
	id := h.enqueue()
	attempt := h.signAttempt(id, 141, common.HexToHash("0x6d41"))
	h.broadcastResult(attempt.ID, SendErrorUnderpriced)
	if status, heldReason, _ := h.outboxState(id); status != TxStatusHeld || heldReason != HeldRepriceRequired {
		t.Fatalf("outbox = %q/%q, want held/reprice_required", status, heldReason)
	}

	findHeld := h.heldReasonReported
	if !findHeld(HeldRepriceRequired) {
		t.Fatal("below the cap the hold must report reprice_required")
	}
	if findHeld(HeldRepriceExhausted) {
		t.Fatal("below the cap the hold must not report reprice_exhausted")
	}

	if _, err := h.store.pool.Exec(h.ctx, `
		INSERT INTO tx_attempts (outbox_id, kind, nonce, tx_type, tx_hash, raw_tx, gas_limit,
			max_fee_per_gas, max_priority_fee_per_gas, state, signing_token)
		SELECT $1, 'replacement', 141, 2, decode(lpad(to_hex(g), 64, '0'), 'hex'), '\x01',
			21000, 1, 1, 'rejected', gen_random_uuid()
		FROM generate_series(9200001, 9200005) AS g
	`, id); err != nil {
		t.Fatalf("seed replacement attempts: %v", err)
	}
	if !findHeld(HeldRepriceExhausted) {
		t.Fatal("at the cap the hold must report reprice_exhausted")
	}
	if findHeld(HeldRepriceRequired) {
		t.Fatal("at the cap the hold must not double-report reprice_required")
	}

	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}
	if findHeld(HeldRepriceExhausted) {
		t.Fatal("a cancel-intent row must not report reprice_exhausted")
	}
	if !findHeld(HeldRepriceRequired) {
		t.Fatal("a cancel-intent row keeps its persistent held reason")
	}
	if !findHeld("cancel_requested") {
		t.Fatal("a cancel-intent row must report cancel_requested")
	}
}

// TestHeldStatsReportCancelRepriceExhaustion mirrors the selector's cancel
// counting domain: an underpriced active cancel is bumped and capped by
// cancel-kind attempts, so at that cap the lane must surface as
// reprice_exhausted even though the row carries a cancel intent.
func TestHeldStatsReportCancelRepriceExhaustion(t *testing.T) {
	h := newAttemptHarness(t, "0x6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e", 151)
	id := h.enqueue()
	original := h.signAttempt(id, 151, common.HexToHash("0x6e51"))
	h.broadcastResult(original.ID, SendErrorAccepted)
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}
	cancelToken := uuid.New()
	if _, err := h.store.ClaimOutboxForCancelSigning(h.ctx, id, original.ID, cancelToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForCancelSigning: %v", err)
	}
	cancelAttempt, err := h.store.InsertCancelAttempt(h.ctx, id, original.ID, cancelToken, SignedAttempt{
		Kind: TxAttemptCancel, Nonce: 151, TxType: 2, TxHash: common.HexToHash("0x6e52"),
		RawTx: []byte{0x6e, 0x52}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(3_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertCancelAttempt: %v", err)
	}
	h.broadcastResult(cancelAttempt.ID, SendErrorUnderpriced)
	if status, heldReason, _ := h.outboxState(id); status != TxStatusHeld || heldReason != HeldRepriceRequired {
		t.Fatalf("outbox = %q/%q, want held/reprice_required", status, heldReason)
	}

	// One cancel attempt is below the cancel bump cap: still self-healing.
	if h.heldReasonReported(HeldRepriceExhausted) {
		t.Fatal("below the cancel cap the hold must not report reprice_exhausted")
	}
	if !h.heldReasonReported(HeldRepriceRequired) || !h.heldReasonReported("cancel_requested") {
		t.Fatal("below the cancel cap the hold must report reprice_required and cancel_requested")
	}

	if _, err := h.store.pool.Exec(h.ctx, `
		INSERT INTO tx_attempts (outbox_id, kind, nonce, tx_type, tx_hash, raw_tx, gas_limit,
			max_fee_per_gas, max_priority_fee_per_gas, state, signing_token)
		SELECT $1, 'cancel', 151, 2, decode(lpad(to_hex(g), 64, '0'), 'hex'), '\x01',
			21000, 1, 1, 'rejected', gen_random_uuid()
		FROM generate_series(9300001, 9300004) AS g
	`, id); err != nil {
		t.Fatalf("seed cancel attempts: %v", err)
	}
	if !h.heldReasonReported(HeldRepriceExhausted) {
		t.Fatal("at the cancel cap the hold must report reprice_exhausted")
	}
	if h.heldReasonReported(HeldRepriceRequired) {
		t.Fatal("at the cancel cap the hold must not double-report reprice_required")
	}
}

func TestRequestTxReplacementBypassesAutomaticCap(t *testing.T) {
	h := newAttemptHarness(t, "0x5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c", 91)
	id := h.enqueue()
	attempt := h.signAttempt(id, 91, common.HexToHash("0xd1d1"))
	h.broadcastResult(attempt.ID, SendErrorAccepted)
	// Exhaust the automatic replacement budget with synthetic replacement rows.
	if _, err := h.store.pool.Exec(h.ctx, `
		INSERT INTO tx_attempts (outbox_id, kind, nonce, tx_type, tx_hash, raw_tx, gas_limit,
			max_fee_per_gas, max_priority_fee_per_gas, state, signing_token)
		SELECT $1, 'replacement', 91, 2, decode(lpad(to_hex(g), 64, '0'), 'hex'), '\x01',
			21000, 1, 1, 'rejected', gen_random_uuid()
		FROM generate_series(9100001, 9100005) AS g
	`, id); err != nil {
		t.Fatalf("seed replacement attempts: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET updated_at = now() - interval '16 minutes' WHERE id = $1`, id); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	// The automatic path is capped out ...
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(capped) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	// ... but an explicit operator request still authorizes one more replacement.
	if err := h.store.RequestTxReplacement(h.ctx, id); err != nil {
		t.Fatalf("RequestTxReplacement: %v", err)
	}
	candidate, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(requested) error = %v, want the requested row", err)
	}
	if candidate.Outbox.ID != id {
		t.Fatalf("candidate outbox = %d, want %d", candidate.Outbox.ID, id)
	}
}

func TestRecordPreSignFailureReplacementBudgetHoldsLane(t *testing.T) {
	h := newAttemptHarness(t, "0x5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d", 95)
	id := h.enqueue()
	attempt := h.signAttempt(id, 95, common.HexToHash("0xe1e1"))
	h.broadcastResult(attempt.ID, SendErrorAccepted)

	for i := int32(1); i <= TxMaxPreSignFailures; i++ {
		leaseToken := uuid.New()
		if _, err := h.store.ClaimOutboxForReplacementSigning(h.ctx, id, attempt.ID, leaseToken, 30*time.Second); err != nil {
			t.Fatalf("ClaimOutboxForReplacementSigning #%d: %v", i, err)
		}
		held, err := h.store.RecordPreSignFailure(h.ctx, id, leaseToken)
		if err != nil {
			t.Fatalf("RecordPreSignFailure #%d: %v", i, err)
		}
		if held != (i == TxMaxPreSignFailures) {
			t.Fatalf("RecordPreSignFailure #%d held = %t", i, held)
		}
	}
	// The exhausted replacement signing budget parks the lane visibly instead of
	// silently dropping out of the replacement selector.
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldManual {
		t.Fatalf("outbox status=%q held_reason=%q, want held/manual", status, heldReason)
	}
	// Receipt polling still covers the held row's attempts.
	tasks, err := h.store.ListReceiptPollTasks(h.ctx, 40161, h.signerID, 10)
	if err != nil {
		t.Fatalf("ListReceiptPollTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Outbox.ID != id || len(tasks[0].Attempts) == 0 {
		t.Fatalf("poll tasks = %+v, want the held row with its attempts", tasks)
	}
}

func TestListReceiptPollTasksFairness(t *testing.T) {
	h := newAttemptHarness(t, "0x5959595959595959595959595959595959595959", 61)
	id1 := h.enqueue()
	attempt1 := h.signAttempt(id1, 61, common.HexToHash("0xb1b1"))
	h.broadcastResult(attempt1.ID, SendErrorAccepted)
	id2 := h.enqueue()
	attempt2 := h.signAttempt(id2, 62, common.HexToHash("0xb2b2"))

	// A signed-but-never-sent attempt is not poll-worthy: its hash cannot be
	// on chain, and polling it before the first broadcast could starve the
	// send when the receipt endpoint is failing.
	tasks, err := h.store.ListReceiptPollTasks(h.ctx, 40161, h.signerID, 10)
	if err != nil {
		t.Fatalf("ListReceiptPollTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Outbox.ID != id1 {
		t.Fatalf("tasks = %+v, want only the sent row %d", tasks, id1)
	}
	h.broadcastResult(attempt2.ID, SendErrorAmbiguous)

	tasks, err = h.store.ListReceiptPollTasks(h.ctx, 40161, h.signerID, 10)
	if err != nil {
		t.Fatalf("ListReceiptPollTasks: %v", err)
	}
	if len(tasks) != 2 || tasks[0].Outbox.ID != id1 || tasks[1].Outbox.ID != id2 {
		t.Fatalf("tasks = %+v, want id order %d,%d for never-polled rows", tasks, id1, id2)
	}
	if len(tasks[0].Attempts) != 1 || tasks[0].Attempts[0].TxHash != attempt1.TxHash {
		t.Fatalf("task 1 attempts = %+v, want the accepted attempt hash", tasks[0].Attempts)
	}
	if len(tasks[1].Attempts) != 1 || tasks[1].Attempts[0].TxHash != attempt2.TxHash {
		t.Fatalf("task 2 attempts = %+v, want the sent attempt hash", tasks[1].Attempts)
	}

	// Touching the first row rotates it behind the never-polled one.
	if err := h.store.TouchReceiptPoll(h.ctx, id1); err != nil {
		t.Fatalf("TouchReceiptPoll: %v", err)
	}
	tasks, err = h.store.ListReceiptPollTasks(h.ctx, 40161, h.signerID, 1)
	if err != nil {
		t.Fatalf("ListReceiptPollTasks(limit 1): %v", err)
	}
	if len(tasks) != 1 || tasks[0].Outbox.ID != id2 {
		t.Fatalf("tasks after touch = %+v, want only %d first", tasks, id2)
	}
}

// TestHeldStatsRepriceAgeTracksActiveAttempt pins the stall-age basis: the
// row's updated_at is refreshed by every underpriced result and deferral, so
// a fee-cap-stuck reprice lane must age from its active attempt's creation
// (the last real signing progress) or readiness stall detection never fires.
func TestHeldStatsRepriceAgeTracksActiveAttempt(t *testing.T) {
	h := newAttemptHarness(t, "0x7474747474747474747474747474747474747474", 241)
	id := h.enqueue()
	attempt := h.signAttempt(id, 241, common.HexToHash("0xa741"))
	h.broadcastResult(attempt.ID, SendErrorUnderpriced)
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldRepriceRequired {
		t.Fatalf("outbox = %q/%q, want held/reprice_required", status, heldReason)
	}

	// The stuck loop keeps refreshing the row while the attempt stays put.
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_attempts SET created_at = now() - interval '16 minutes' WHERE id = $1
	`, attempt.ID); err != nil {
		t.Fatalf("backdate attempt: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox SET updated_at = now() WHERE id = $1
	`, id); err != nil {
		t.Fatalf("refresh row: %v", err)
	}

	snapshot, err := h.store.Stats(h.ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	var found bool
	for _, stat := range snapshot.TxOutboxHeld {
		if stat.ChainEID == 40161 && stat.SignerID == h.signerID && stat.HeldReason == HeldRepriceRequired {
			found = true
			if stat.OldestAgeSeconds < 900 {
				t.Fatalf("reprice hold age = %ds, want >= 900 (attempt-based, not the refreshed updated_at)", stat.OldestAgeSeconds)
			}
		}
	}
	if !found {
		t.Fatal("reprice hold not reported")
	}
}

func TestDeferReplacementAndReceiptRefreshKeepRowOutOfStaleWindow(t *testing.T) {
	h := newAttemptHarness(t, "0x7676767676767676767676767676767676767676", 130)
	id := h.enqueue()
	attempt := h.signAttempt(id, 130, common.HexToHash("0x7201"))
	h.broadcastResult(attempt.ID, SendErrorAccepted)
	backdate := func() {
		t.Helper()
		if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET updated_at = now() - interval '16 minutes' WHERE id = $1`, id); err != nil {
			t.Fatalf("backdate: %v", err)
		}
	}

	backdate()
	candidate, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(stale) error = %v", err)
	}
	if candidate.Outbox.ID != id || candidate.ActiveAttemptID != attempt.ID {
		t.Fatalf("candidate = %d/%d, want %d/%d", candidate.Outbox.ID, candidate.ActiveAttemptID, id, attempt.ID)
	}

	// An observed-but-shallow receipt restarts the stale clock so the mined
	// transaction is not replaced out from under its confirmation depth.
	if err := h.store.RefreshBroadcastReceiptObservedAt(h.ctx, id); err != nil {
		t.Fatalf("RefreshBroadcastReceiptObservedAt() error = %v", err)
	}
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(refreshed) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}

	// A preflight deferral pushes a stale candidate out of the window without
	// inventing an operator request.
	backdate()
	if err := h.store.DeferReplacement(h.ctx, id); err != nil {
		t.Fatalf("DeferReplacement() error = %v", err)
	}
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(deferred) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	var replaceRequestedAt *time.Time
	if err := h.store.pool.QueryRow(h.ctx, "SELECT replace_requested_at FROM tx_outbox WHERE id = $1", id).Scan(&replaceRequestedAt); err != nil {
		t.Fatalf("select replace_requested_at: %v", err)
	}
	if replaceRequestedAt != nil {
		t.Fatalf("deferral of a stale candidate set replace_requested_at = %v, want NULL", replaceRequestedAt)
	}

	// A due operator request selects immediately; a deferral pushes the request
	// itself into the future instead of dropping it.
	if err := h.store.RequestTxReplacement(h.ctx, id); err != nil {
		t.Fatalf("RequestTxReplacement() error = %v", err)
	}
	candidate, err = h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(requested) error = %v", err)
	}
	if candidate.Outbox.ID != id {
		t.Fatalf("requested candidate = %d, want %d", candidate.Outbox.ID, id)
	}
	if err := h.store.DeferReplacement(h.ctx, id); err != nil {
		t.Fatalf("DeferReplacement(requested) error = %v", err)
	}
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(deferred request) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}
	var requestInFuture bool
	if err := h.store.pool.QueryRow(h.ctx, "SELECT replace_requested_at > now() FROM tx_outbox WHERE id = $1", id).Scan(&requestInFuture); err != nil {
		t.Fatalf("select deferred replace_requested_at: %v", err)
	}
	if !requestInFuture {
		t.Fatal("DeferReplacement() did not push the operator request into the future")
	}

	// The receipt refresh only touches signed/broadcast rows: a terminal row's
	// updated_at stays put.
	seedConfirmed(h.ctx, t, h.store, id, common.HexToHash("0x7201"))
	backdate()
	var before time.Time
	if err := h.store.pool.QueryRow(h.ctx, "SELECT updated_at FROM tx_outbox WHERE id = $1", id).Scan(&before); err != nil {
		t.Fatalf("select updated_at: %v", err)
	}
	if err := h.store.RefreshBroadcastReceiptObservedAt(h.ctx, id); err != nil {
		t.Fatalf("RefreshBroadcastReceiptObservedAt(confirmed) error = %v", err)
	}
	var after time.Time
	if err := h.store.pool.QueryRow(h.ctx, "SELECT updated_at FROM tx_outbox WHERE id = $1", id).Scan(&after); err != nil {
		t.Fatalf("select updated_at after refresh: %v", err)
	}
	if !after.Equal(before) {
		t.Fatalf("refresh touched a confirmed row: updated_at %v -> %v", before, after)
	}
}

// TestOrphanedOutboxStatsDetectSendStateWithoutAttempt pins the invariant
// detector for rows the 002 schema upgrade can strand: a send-state row whose
// active attempt is gone is invisible to receipt polling and (when signed)
// blocks the nonce lane, so the stats surface must report it.
func TestOrphanedOutboxStatsDetectSendStateWithoutAttempt(t *testing.T) {
	h := newAttemptHarness(t, "0x7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e", 311)

	orphaned := func(status string) (int64, uint64) {
		h.t.Helper()
		snapshot, err := h.store.Stats(h.ctx)
		if err != nil {
			h.t.Fatalf("Stats() error = %v", err)
		}
		for _, stat := range snapshot.TxOutboxOrphaned {
			if stat.ChainEID == 40161 && stat.SignerID == h.signerID && stat.Status == status {
				return stat.Count, stat.OldestAgeSeconds
			}
		}
		return 0, 0
	}

	id := h.enqueue()
	attempt := h.signAttempt(id, 311, common.HexToHash("0x7e31"))
	if count, _ := orphaned(TxStatusSigned); count != 0 {
		t.Fatalf("healthy signed row reported as orphaned (count = %d)", count)
	}

	// Reproduce what 002 leaves behind: the row keeps its send state and nonce
	// while its only attempt is gone.
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox SET active_attempt_id = NULL, updated_at = now() - interval '30 minutes' WHERE id = $1
	`, id); err != nil {
		t.Fatalf("detach active attempt: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, "DELETE FROM tx_attempts WHERE id = $1", attempt.ID); err != nil {
		t.Fatalf("delete attempt: %v", err)
	}
	count, age := orphaned(TxStatusSigned)
	if count != 1 {
		t.Fatalf("orphaned signed rows = %d, want 1", count)
	}
	if age < 1500 {
		t.Fatalf("orphaned age = %ds, want the stuck age from updated_at", age)
	}
	// Receipt polling can no longer see it, which is exactly why the stats
	// surface has to.
	tasks, err := h.store.ListReceiptPollTasks(h.ctx, 40161, h.signerID, 10)
	if err != nil {
		t.Fatalf("ListReceiptPollTasks() error = %v", err)
	}
	for _, task := range tasks {
		if task.Outbox.ID == id {
			t.Fatal("an orphaned row must not appear in receipt polling")
		}
	}

	// A held row without an attempt is legitimate (exhausted pre-sign budget)
	// and must not be reported.
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox SET status = $1, held_reason = $2 WHERE id = $3
	`, TxStatusHeld, HeldManual, id); err != nil {
		t.Fatalf("park row as held: %v", err)
	}
	if count, _ := orphaned(TxStatusSigned); count != 0 {
		t.Fatalf("held row without an attempt reported as orphaned (count = %d)", count)
	}
	if count, _ := orphaned(TxStatusHeld); count != 0 {
		t.Fatalf("held status must never be counted (count = %d)", count)
	}

	// A retired chain's retained rows cannot be acted on: they are excluded so
	// the metric matches the readiness escalation and cannot page forever.
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox SET status = $1, held_reason = NULL WHERE id = $2
	`, TxStatusSigned, id); err != nil {
		t.Fatalf("restore signed status: %v", err)
	}
	if count, _ := orphaned(TxStatusSigned); count != 1 {
		t.Fatalf("orphaned signed rows = %d, want 1 before disabling the chain", count)
	}
	if _, err := h.store.pool.Exec(h.ctx, "UPDATE chains SET enabled = false WHERE eid = 40161"); err != nil {
		t.Fatalf("disable chain: %v", err)
	}
	h.t.Cleanup(func() {
		if _, err := h.store.pool.Exec(context.Background(), "UPDATE chains SET enabled = true WHERE eid = 40161"); err != nil {
			h.t.Errorf("restore chain: %v", err)
		}
	})
	if count, _ := orphaned(TxStatusSigned); count != 0 {
		t.Fatalf("disabled chain rows reported as orphaned (count = %d)", count)
	}
}
