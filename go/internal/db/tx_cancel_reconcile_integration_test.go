package db

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
)

func TestCancelRequestGatesOriginalTaskAndFinalizesCanceled(t *testing.T) {
	h := newAttemptHarness(t, "0x6161616161616161616161616161616161616161", 101)
	id := h.enqueue()
	original := h.signAttempt(id, 101, common.HexToHash("0xaa01"))
	h.broadcastResult(original.ID, SendErrorAmbiguous)

	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}
	// The original raw must not be rebroadcast and must not be replaced while
	// the cancel intent is set.
	if _, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrNoBroadcastCandidate) {
		t.Fatalf("ClaimAttemptForBroadcast(original under cancel) error = %v, want ErrNoBroadcastCandidate", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET updated_at = now() - interval '16 minutes' WHERE id = $1`, id); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(under cancel) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}

	candidate, err := h.store.NextCancelCandidate(h.ctx, 40161, h.signerID)
	if err != nil {
		t.Fatalf("NextCancelCandidate: %v", err)
	}
	if candidate.Outbox.ID != id || candidate.ActiveAttemptID != original.ID {
		t.Fatalf("candidate = %d/%d, want %d/%d", candidate.Outbox.ID, candidate.ActiveAttemptID, id, original.ID)
	}
	leaseToken := uuid.New()
	if _, err := h.store.ClaimOutboxForCancelSigning(h.ctx, id, original.ID, leaseToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForCancelSigning: %v", err)
	}
	cancelAttempt, err := h.store.InsertCancelAttempt(h.ctx, id, original.ID, leaseToken, SignedAttempt{
		Kind: TxAttemptCancel, Nonce: 101, TxType: 2, TxHash: common.HexToHash("0xaa02"),
		RawTx: []byte{0xaa, 0x02}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(3_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertCancelAttempt: %v", err)
	}
	// The cancel intent persists past the insert, and only the cancel attempt flies.
	var requestedAt *time.Time
	if err := h.store.pool.QueryRow(h.ctx, `SELECT cancel_requested_at FROM tx_outbox WHERE id = $1`, id).Scan(&requestedAt); err != nil {
		t.Fatalf("select intent: %v", err)
	}
	if requestedAt == nil {
		t.Fatal("cancel intent cleared by the attempt insert; it must persist to terminalization")
	}
	broadcastToken := uuid.New()
	claim, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, broadcastToken, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast(cancel): %v", err)
	}
	if claim.AttemptID != cancelAttempt.ID || claim.Kind != TxAttemptCancel {
		t.Fatalf("claimed %d kind %q, want the cancel attempt %d", claim.AttemptID, claim.Kind, cancelAttempt.ID)
	}
	if err := h.store.MarkAttemptSendResult(h.ctx, claim.AttemptID, broadcastToken, SendErrorAccepted, ""); err != nil {
		t.Fatalf("MarkAttemptSendResult: %v", err)
	}

	// A mined cancel (receipt status 1) terminates as canceled with no retry.
	facts := TxReceiptFacts{TxHash: cancelAttempt.TxHash, Status: 1, BlockNumber: 300, GasUsed: 21000, EffectiveGasPrice: big.NewInt(1_000_000_000), GasCostDstWei: new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000))}
	outcome, err := h.store.PrepareReceiptResolution(h.ctx, cancelAttempt.ID, facts)
	if err != nil || outcome != ReceiptOutcomeCanceled {
		t.Fatalf("PrepareReceiptResolution = (%q, %v), want canceled", outcome, err)
	}
	outcome, err = h.store.FinalizeAttemptReceipt(h.ctx, cancelAttempt.ID, facts)
	if err != nil || outcome != ReceiptOutcomeCanceled {
		t.Fatalf("FinalizeAttemptReceipt = (%q, %v), want canceled", outcome, err)
	}
	after, err := h.store.GetOutboxTx(h.ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx: %v", err)
	}
	if after.Status != TxStatusFailed || after.FailureKind != TxFailureCanceled || after.NextRetryAt != nil {
		t.Fatalf("after = %q/%q/%v, want failed/canceled without retry", after.Status, after.FailureKind, after.NextRetryAt)
	}
	if after.CancelRequestedAt != nil {
		t.Fatal("cancel intent survived terminalization")
	}
	// The canceled row is rejected by the generic retry and no longer blocks the lane.
	if _, err := h.store.RetryFailedTx(h.ctx, id); err == nil {
		t.Fatal("RetryFailedTx(canceled) succeeded, want rejection")
	}
	nextID := h.enqueue()
	next, err := h.store.ClaimOutboxForSigning(h.ctx, nextID, 40161, h.signerID, uuid.New(), 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutboxForSigning(next) error = %v, want unlocked lane", err)
	}
	if next.Nonce != 102 {
		t.Fatalf("next nonce = %d, want 102", next.Nonce)
	}
}

func TestCancelRequestRevokesSigningLease(t *testing.T) {
	h := newAttemptHarness(t, "0x6262626262626262626262626262626262626262", 111)
	id := h.enqueue()
	token := uuid.New()
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, token, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForSigning: %v", err)
	}
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}
	// The in-flight original signature must not land after the cancel request.
	if _, err := h.store.InsertSignedAttempt(h.ctx, id, token, SignedAttempt{
		Kind: TxAttemptOriginal, Nonce: 111, TxType: 2, TxHash: common.HexToHash("0xbb01"),
		RawTx: []byte{0xbb, 0x01}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(2_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_000_000_000),
		SigningToken: uuid.New(),
	}); !errors.Is(err, ErrOutboxLeaseLost) {
		t.Fatalf("InsertSignedAttempt(after cancel request) error = %v, want ErrOutboxLeaseLost", err)
	}
	// A bare nonce-holding row (no attempt) is still a cancel candidate.
	candidate, err := h.store.NextCancelCandidate(h.ctx, 40161, h.signerID)
	if err != nil {
		t.Fatalf("NextCancelCandidate: %v", err)
	}
	if candidate.Outbox.ID != id || candidate.ActiveAttemptID != 0 {
		t.Fatalf("candidate = %d/%d, want %d with no active attempt", candidate.Outbox.ID, candidate.ActiveAttemptID, id)
	}
}

// TestBareNonceCancelHonorsPreSignBackoff pins the regression where a cancel
// signing failure on a bare nonce_assigned row scheduled next_sign_at but the
// cancel selector ignored it, burning the whole signing budget in a hot loop.
func TestBareNonceCancelHonorsPreSignBackoff(t *testing.T) {
	h := newAttemptHarness(t, "0x6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c", 131)
	id := h.enqueue()
	token := uuid.New()
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, token, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForSigning: %v", err)
	}
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}

	candidate, err := h.store.NextCancelCandidate(h.ctx, 40161, h.signerID)
	if err != nil {
		t.Fatalf("NextCancelCandidate: %v", err)
	}
	if candidate.Outbox.ID != id || candidate.ActiveAttemptID != 0 {
		t.Fatalf("candidate = %d/%d, want bare row %d", candidate.Outbox.ID, candidate.ActiveAttemptID, id)
	}
	cancelToken := uuid.New()
	if _, err := h.store.ClaimOutboxForCancelSigning(h.ctx, id, 0, cancelToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForCancelSigning: %v", err)
	}
	parked, err := h.store.RecordPreSignFailure(h.ctx, id, cancelToken)
	if err != nil {
		t.Fatalf("RecordPreSignFailure: %v", err)
	}
	if parked {
		t.Fatal("parked after the first signing failure, want a scheduled retry")
	}
	// The scheduled backoff must gate re-selection instead of a hot-loop retry.
	if _, err := h.store.NextCancelCandidate(h.ctx, 40161, h.signerID); !errors.Is(err, ErrNoCancelWork) {
		t.Fatalf("NextCancelCandidate(during backoff) error = %v, want ErrNoCancelWork", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET next_sign_at = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatalf("elapse backoff: %v", err)
	}
	candidate, err = h.store.NextCancelCandidate(h.ctx, 40161, h.signerID)
	if err != nil {
		t.Fatalf("NextCancelCandidate(after backoff): %v", err)
	}
	if candidate.Outbox.ID != id {
		t.Fatalf("candidate after backoff = %d, want %d", candidate.Outbox.ID, id)
	}
}

func TestTaskReceiptFailureUnderCancelIntentIsCanceled(t *testing.T) {
	h := newAttemptHarness(t, "0x6363636363636363636363636363636363636363", 121)
	id := h.enqueue()
	original := h.signAttempt(id, 121, common.HexToHash("0xcc01"))
	h.broadcastResult(original.ID, SendErrorAccepted)
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}

	// The original task mined with a failed status while cancel intent was set:
	// the nonce is consumed and the operator abandoned the task, so no automatic
	// receipt retry.
	facts := TxReceiptFacts{TxHash: original.TxHash, Status: 0, BlockNumber: 310, GasUsed: 21000, EffectiveGasPrice: big.NewInt(1_000_000_000), GasCostDstWei: new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000))}
	outcome, err := h.store.PrepareReceiptResolution(h.ctx, original.ID, facts)
	if err != nil || outcome != ReceiptOutcomeCanceled {
		t.Fatalf("PrepareReceiptResolution = (%q, %v), want canceled", outcome, err)
	}
	outcome, err = h.store.FinalizeAttemptReceipt(h.ctx, original.ID, facts)
	if err != nil || outcome != ReceiptOutcomeCanceled {
		t.Fatalf("FinalizeAttemptReceipt = (%q, %v), want canceled", outcome, err)
	}

	// A successful task receipt wins over the cancel intent: the task executed.
	successID := h.enqueue()
	successAttempt := h.signAttempt(successID, 122, common.HexToHash("0xcc02"))
	h.broadcastResult(successAttempt.ID, SendErrorAccepted)
	if err := h.store.RequestTxCancel(h.ctx, successID); err != nil {
		t.Fatalf("RequestTxCancel(success row): %v", err)
	}
	successFacts := TxReceiptFacts{TxHash: successAttempt.TxHash, Status: 1, BlockNumber: 311, GasUsed: 21000, EffectiveGasPrice: big.NewInt(1_000_000_000), GasCostDstWei: new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000))}
	outcome, err = h.store.PrepareReceiptResolution(h.ctx, successAttempt.ID, successFacts)
	if err != nil || outcome != ReceiptOutcomeConfirmed {
		t.Fatalf("PrepareReceiptResolution(success under cancel) = (%q, %v), want confirmed", outcome, err)
	}
	outcome, err = h.store.FinalizeAttemptReceipt(h.ctx, successAttempt.ID, successFacts)
	if err != nil || outcome != ReceiptOutcomeConfirmed {
		t.Fatalf("FinalizeAttemptReceipt(success under cancel) = (%q, %v), want confirmed", outcome, err)
	}
}

func TestNonceReconciliationClaimReleaseAndExternalConsumption(t *testing.T) {
	h := newAttemptHarness(t, "0x6464646464646464646464646464646464646464", 131)
	transientID := h.enqueue()
	transient := h.signAttempt(transientID, 131, common.HexToHash("0xdd01"))
	h.broadcastResult(transient.ID, SendErrorNonceTooLow)
	status, heldReason, _ := h.outboxState(transientID)
	if status != TxStatusHeld || heldReason != HeldNonceReconcileRequired {
		t.Fatalf("outbox = %q/%q, want held/nonce_reconcile_required", status, heldReason)
	}

	token := uuid.New()
	holds, err := h.store.ClaimNonceReconciliation(h.ctx, 40161, h.signerID, token, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimNonceReconciliation: %v", err)
	}
	if len(holds) != 1 || holds[0].ID != transientID || holds[0].Nonce != 131 {
		t.Fatalf("holds = %+v, want the held row", holds)
	}
	// Hashes load separately AFTER the caller's heartbeat is running.
	hashesByHold, err := h.store.LoadNonceReconcileAttemptHashes(h.ctx, []int64{transientID})
	if err != nil {
		t.Fatalf("LoadNonceReconcileAttemptHashes: %v", err)
	}
	if len(hashesByHold[transientID]) != 1 {
		t.Fatalf("hashes = %+v, want the held row's sent attempt hash", hashesByHold)
	}
	// The lease excludes a concurrent reconciler.
	if _, err := h.store.ClaimNonceReconciliation(h.ctx, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrNoNonceReconcileWork) {
		t.Fatalf("concurrent claim error = %v, want ErrNoNonceReconcileWork", err)
	}

	// Transient case: the confirmed nonce has not passed the held nonce; the
	// atomic apply releases the row, keeps the cursor (no forward), and frees
	// the lease with a backoff.
	result, err := h.store.ApplyNonceReconciliation(h.ctx, 40161, h.signerID, token, 131, 9_000, time.Minute,
		[]NonceReconcileDecision{{ID: transientID, Action: NonceReconcileRelease}})
	if err != nil || result.Changed != 1 || result.CursorForwarded {
		t.Fatalf("ApplyNonceReconciliation(release) = (%+v, %v), want 1 change without cursor forward", result, err)
	}
	status, heldReason, _ = h.outboxState(transientID)
	if status != TxStatusBroadcast || heldReason != "" {
		t.Fatalf("released row = %q/%q, want broadcast", status, heldReason)
	}
	// A stale token (the lease was already consumed) cannot publish decisions.
	if _, err := h.store.ApplyNonceReconciliation(h.ctx, 40161, h.signerID, token, 200, 9_001, time.Minute,
		[]NonceReconcileDecision{{ID: transientID, Action: NonceReconcileMarkExternal}}); !errors.Is(err, ErrOutboxLeaseLost) {
		t.Fatalf("ApplyNonceReconciliation(stale token) error = %v, want ErrOutboxLeaseLost", err)
	}
	// The backoff keeps the lane quiet even though another row is held later.
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET status = 'held', held_reason = 'nonce_reconcile_required' WHERE id = $1`, transientID); err != nil {
		t.Fatalf("re-hold: %v", err)
	}
	if _, err := h.store.ClaimNonceReconciliation(h.ctx, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrNoNonceReconcileWork) {
		t.Fatalf("claim inside backoff error = %v, want ErrNoNonceReconcileWork", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_nonce_cursors SET next_reconcile_at = NULL WHERE signer_id = $1`, h.signerID); err != nil {
		t.Fatalf("clear backoff: %v", err)
	}

	// External consumption: evidence recorded, row stays held for the operator,
	// and the cursor fast-forwards past the consumed nonce in the same atomic apply.
	token2 := uuid.New()
	if _, err := h.store.ClaimNonceReconciliation(h.ctx, 40161, h.signerID, token2, 30*time.Second); err != nil {
		t.Fatalf("ClaimNonceReconciliation(second): %v", err)
	}
	result, err = h.store.ApplyNonceReconciliation(h.ctx, 40161, h.signerID, token2, 140, 9_999, time.Minute,
		[]NonceReconcileDecision{{ID: transientID, Action: NonceReconcileMarkExternal}})
	if err != nil || result.Changed != 1 || !result.CursorForwarded || result.PreviousCursor != 132 {
		t.Fatalf("ApplyNonceReconciliation(external) = (%+v, %v), want 1 change with cursor forwarded from 132", result, err)
	}
	status, heldReason, _ = h.outboxState(transientID)
	if status != TxStatusHeld || heldReason != HeldNonceConsumedExternally {
		t.Fatalf("consumed row = %q/%q, want held/nonce_consumed_externally", status, heldReason)
	}
	// Consumed rows are not cancelable.
	if err := h.store.RequestTxCancel(h.ctx, transientID); err == nil {
		t.Fatal("RequestTxCancel(consumed) succeeded, want rejection")
	}

	// Operator resolution: retry terminates the row and clones a fresh task.
	cloneID, err := h.store.ResolveExternalNonceRetry(h.ctx, transientID)
	if err != nil {
		t.Fatalf("ResolveExternalNonceRetry: %v", err)
	}
	terminal, err := h.store.GetOutboxTx(h.ctx, transientID)
	if err != nil {
		t.Fatalf("GetOutboxTx(terminal): %v", err)
	}
	if terminal.Status != TxStatusFailed || terminal.FailureKind != TxFailureNonceConsumedExternally {
		t.Fatalf("terminal = %q/%q, want failed/nonce_consumed_externally", terminal.Status, terminal.FailureKind)
	}
	if _, err := h.store.RetryFailedTx(h.ctx, transientID); err == nil {
		t.Fatal("RetryFailedTx(consumed) succeeded, want rejection")
	}
	clone, err := h.store.GetOutboxTx(h.ctx, cloneID)
	if err != nil {
		t.Fatalf("GetOutboxTx(clone): %v", err)
	}
	if clone.Status != TxStatusQueued || clone.RetryOfID == nil || *clone.RetryOfID != transientID {
		t.Fatalf("clone = %q retry_of=%v, want fresh queued clone of %d", clone.Status, clone.RetryOfID, transientID)
	}
	// The clone signs at the fast-forwarded cursor, past the consumed nonce.
	claimed, err := h.store.ClaimOutboxForSigning(h.ctx, cloneID, 40161, h.signerID, uuid.New(), 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutboxForSigning(clone): %v", err)
	}
	if claimed.Nonce != 140 {
		t.Fatalf("clone nonce = %d, want the fast-forwarded 140", claimed.Nonce)
	}
}

func TestPreparedResolutionBlocksCancelRequest(t *testing.T) {
	h := newAttemptHarness(t, "0x6666666666666666666666666666666666666667", 151)
	id := h.enqueue()
	attempt := h.signAttempt(id, 151, common.HexToHash("0xee01"))
	h.broadcastResult(attempt.ID, SendErrorAccepted)

	facts := TxReceiptFacts{TxHash: attempt.TxHash, Status: 0, BlockNumber: 400, GasUsed: 21000, EffectiveGasPrice: big.NewInt(1_000_000_000), GasCostDstWei: new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000))}
	outcome, err := h.store.PrepareReceiptResolution(h.ctx, attempt.ID, facts)
	if err != nil || outcome != ReceiptOutcomeFailed {
		t.Fatalf("PrepareReceiptResolution = (%q, %v), want receipt_failed", outcome, err)
	}
	// Re-preparing the same attempt is idempotent.
	outcome, err = h.store.PrepareReceiptResolution(h.ctx, attempt.ID, facts)
	if err != nil || outcome != ReceiptOutcomeFailed {
		t.Fatalf("PrepareReceiptResolution(idempotent) = (%q, %v), want receipt_failed", outcome, err)
	}
	// A cancel racing the receipt pipeline after the resolution is pinned loses:
	// the workflow and the finalizer must consume the same fact.
	if err := h.store.RequestTxCancel(h.ctx, id); err == nil {
		t.Fatal("RequestTxCancel succeeded after the receipt resolution was pinned")
	}
	// The finalizer consumes exactly the pinned outcome.
	final, err := h.store.FinalizeAttemptReceipt(h.ctx, attempt.ID, facts)
	if err != nil || final != ReceiptOutcomeFailed {
		t.Fatalf("FinalizeAttemptReceipt = (%q, %v), want the pinned receipt_failed", final, err)
	}
}

func TestReconciliationReleasesActiveCancelHold(t *testing.T) {
	h := newAttemptHarness(t, "0x6767676767676767676767676767676767676767", 161)
	id := h.enqueue()
	original := h.signAttempt(id, 161, common.HexToHash("0xee11"))
	h.broadcastResult(original.ID, SendErrorAccepted)
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}
	leaseToken := uuid.New()
	if _, err := h.store.ClaimOutboxForCancelSigning(h.ctx, id, original.ID, leaseToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForCancelSigning: %v", err)
	}
	cancelAttempt, err := h.store.InsertCancelAttempt(h.ctx, id, original.ID, leaseToken, SignedAttempt{
		Kind: TxAttemptCancel, Nonce: 161, TxType: 2, TxHash: common.HexToHash("0xee12"),
		RawTx: []byte{0xee, 0x12}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(3_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertCancelAttempt: %v", err)
	}
	// The cancel raw hits a node with a stale view: nonce too low.
	h.broadcastResult(cancelAttempt.ID, SendErrorNonceTooLow)
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldNonceReconcileRequired {
		t.Fatalf("outbox = %q/%q, want held/nonce_reconcile_required", status, heldReason)
	}

	token := uuid.New()
	holds, err := h.store.ClaimNonceReconciliation(h.ctx, 40161, h.signerID, token, 30*time.Second)
	if err != nil || len(holds) != 1 {
		t.Fatalf("ClaimNonceReconciliation = (%+v, %v), want the cancel hold", holds, err)
	}
	if holds[0].ActiveKind != TxAttemptCancel || !holds[0].CancelRequested {
		t.Fatalf("hold = %+v, want active cancel kind with cancel intent", holds[0])
	}
	// An active cancel with an unspent nonce is released back to broadcast so the
	// same cancel raw resumes replaying: no other flow owns it.
	result, err := h.store.ApplyNonceReconciliation(h.ctx, 40161, h.signerID, token, 161, 9_100, time.Minute,
		[]NonceReconcileDecision{{ID: id, Action: NonceReconcileRelease}})
	if err != nil || result.Changed != 1 {
		t.Fatalf("ApplyNonceReconciliation = (%+v, %v), want the release", result, err)
	}
	broadcastToken := uuid.New()
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_attempts SET next_broadcast_at = now() - interval '1 second' WHERE id = $1`, cancelAttempt.ID); err != nil {
		t.Fatalf("force due: %v", err)
	}
	claim, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, broadcastToken, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast(after release): %v", err)
	}
	if claim.AttemptID != cancelAttempt.ID || claim.Kind != TxAttemptCancel {
		t.Fatalf("claimed %d kind %q, want the released cancel raw %d", claim.AttemptID, claim.Kind, cancelAttempt.ID)
	}
}

func TestCancelSignBudgetFailsClosedUntilReRequest(t *testing.T) {
	h := newAttemptHarness(t, "0x6868686868686868686868686868686868686868", 171)
	id := h.enqueue()
	original := h.signAttempt(id, 171, common.HexToHash("0xee21"))
	h.broadcastResult(original.ID, SendErrorAccepted)
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}

	// Five straight cancel signing failures exhaust the budget.
	for i := int32(1); i <= TxMaxPreSignFailures; i++ {
		leaseToken := uuid.New()
		if _, err := h.store.ClaimOutboxForCancelSigning(h.ctx, id, original.ID, leaseToken, 30*time.Second); err != nil {
			t.Fatalf("ClaimOutboxForCancelSigning #%d: %v", i, err)
		}
		if _, err := h.store.RecordPreSignFailure(h.ctx, id, leaseToken); err != nil {
			t.Fatalf("RecordPreSignFailure #%d: %v", i, err)
		}
		// Each failure pushes the request window; pull it due again.
		if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET cancel_requested_at = now() - interval '1 second' WHERE id = $1`, id); err != nil {
			t.Fatalf("force due #%d: %v", i, err)
		}
	}
	// The sixth attempt is not signed automatically.
	if _, err := h.store.NextCancelCandidate(h.ctx, 40161, h.signerID); !errors.Is(err, ErrNoCancelWork) {
		t.Fatalf("NextCancelCandidate(at budget) error = %v, want ErrNoCancelWork", err)
	}
	// An explicit re-request authorizes exactly one more budget cycle.
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel(re-request): %v", err)
	}
	candidate, err := h.store.NextCancelCandidate(h.ctx, 40161, h.signerID)
	if err != nil {
		t.Fatalf("NextCancelCandidate(after re-request): %v", err)
	}
	if candidate.Outbox.ID != id {
		t.Fatalf("candidate = %d, want %d", candidate.Outbox.ID, id)
	}
}

func TestHeldManualCancelRecoversViaReRequest(t *testing.T) {
	h := newAttemptHarness(t, "0x6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a", 191)
	id := h.enqueue()
	original := h.signAttempt(id, 191, common.HexToHash("0xef01"))
	h.broadcastResult(original.ID, SendErrorAccepted)
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}
	leaseToken := uuid.New()
	if _, err := h.store.ClaimOutboxForCancelSigning(h.ctx, id, original.ID, leaseToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForCancelSigning: %v", err)
	}
	cancelAttempt, err := h.store.InsertCancelAttempt(h.ctx, id, original.ID, leaseToken, SignedAttempt{
		Kind: TxAttemptCancel, Nonce: 191, TxType: 2, TxHash: common.HexToHash("0xef02"),
		RawTx: []byte{0xef, 0x02}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(3_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertCancelAttempt: %v", err)
	}
	// The cancel raw hits a deterministic node rejection: the lane parks manual.
	h.broadcastResult(cancelAttempt.ID, SendErrorDefinitive)
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldManual {
		t.Fatalf("outbox = %q/%q, want held/manual", status, heldReason)
	}

	// An explicit cancel re-request must recover the lane end to end: selector,
	// signing claim, and insert CAS all accept the held(manual) cancel bump.
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel(re-request): %v", err)
	}
	candidate, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(held manual cancel): %v", err)
	}
	if candidate.Outbox.ID != id || candidate.ActiveKind != TxAttemptCancel {
		t.Fatalf("candidate = %d kind %q, want the held cancel lane", candidate.Outbox.ID, candidate.ActiveKind)
	}
	bumpLease := uuid.New()
	if _, err := h.store.ClaimOutboxForReplacementSigning(h.ctx, id, cancelAttempt.ID, bumpLease, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForReplacementSigning(held manual cancel): %v", err)
	}
	bump, err := h.store.InsertReplacementAttempt(h.ctx, id, cancelAttempt.ID, bumpLease, SignedAttempt{
		Kind: TxAttemptCancel, Nonce: 191, TxType: 2, TxHash: common.HexToHash("0xef03"),
		RawTx: []byte{0xef, 0x03}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(4_000_000_000), MaxPriorityFeePerGas: big.NewInt(2_000_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertReplacementAttempt(cancel bump): %v", err)
	}
	status, heldReason, activeID := h.outboxState(id)
	if status != TxStatusSigned || heldReason != "" || activeID != bump.ID {
		t.Fatalf("after recovery = %q/%q/%d, want signed lane with the cancel bump active", status, heldReason, activeID)
	}
}

func TestPinnedResolutionExcludesReconciliationAndResolve(t *testing.T) {
	h := newAttemptHarness(t, "0x6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b", 201)
	id := h.enqueue()
	attempt := h.signAttempt(id, 201, common.HexToHash("0xef11"))
	h.broadcastResult(attempt.ID, SendErrorNonceTooLow)
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldNonceReconcileRequired {
		t.Fatalf("outbox = %q/%q, want held/nonce_reconcile_required", status, heldReason)
	}
	// The receipt pipeline pins a resolution (our attempt actually mined).
	facts := TxReceiptFacts{TxHash: attempt.TxHash, Status: 1, BlockNumber: 500, GasUsed: 21000, EffectiveGasPrice: big.NewInt(1_000_000_000), GasCostDstWei: new(big.Int).Mul(big.NewInt(21000), big.NewInt(1_000_000_000))}
	if _, err := h.store.PrepareReceiptResolution(h.ctx, attempt.ID, facts); err != nil {
		t.Fatalf("PrepareReceiptResolution: %v", err)
	}
	// A pinned resolution makes the receipt pipeline the only owner: the row is
	// no longer reconciliation work at all.
	if _, err := h.store.ClaimNonceReconciliation(h.ctx, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrNoNonceReconcileWork) {
		t.Fatalf("ClaimNonceReconciliation(pinned) error = %v, want ErrNoNonceReconcileWork", err)
	}
	// Even a stale external mark or an operator resolve cannot steal it.
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_outbox SET held_reason = 'nonce_consumed_externally' WHERE id = $1`, id); err != nil {
		t.Fatalf("force consumed: %v", err)
	}
	if _, err := h.store.ResolveExternalNonceRetry(h.ctx, id); err == nil {
		t.Fatal("ResolveExternalNonceRetry succeeded on a pinned resolution, want rejection")
	}
	if err := h.store.ResolveExternalNonceAbandon(h.ctx, id); err == nil {
		t.Fatal("ResolveExternalNonceAbandon succeeded on a pinned resolution, want rejection")
	}
}

func TestResolveExternalNonceRetryRejectsAdvancedWorkflow(t *testing.T) {
	h := newAttemptHarness(t, "0x6969696969696969696969696969696969696969", 181)
	guid := common.HexToHash("0xadad0000adad0000adad0000adad0000adad0000adad0000adad0000adad0000")
	if _, err := h.store.pool.Exec(h.ctx, `DELETE FROM executor_jobs WHERE guid = $1`, guid.Bytes()); err != nil {
		t.Fatalf("clean jobs: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `DELETE FROM packets WHERE guid = $1`, guid.Bytes()); err != nil {
		t.Fatalf("clean packets: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `
		INSERT INTO packets (guid, src_eid, dst_eid, nonce, sender, receiver, send_lib,
			src_tx_hash, src_block_number, src_log_index, encoded_packet, packet_header,
			message, payload_hash, options, status)
		VALUES ($1, 40161, 40449, 2, '\x71', '\x72', '\x73', $2, 1, 0, '\x01', '\x01', '\x02',
			$3, '\x04', 'DELIVERED')
	`, guid.Bytes(), common.HexToHash("0xf00e").Bytes(), common.HexToHash("0xbeee").Bytes()); err != nil {
		t.Fatalf("seed packet: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `
		INSERT INTO executor_jobs (guid, assigned_fee, status) VALUES ($1, 1, 'DELIVERED')
	`, guid.Bytes()); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	// Seed the outbox row directly: these tests exercise the operator resolve
	// path, not the enqueue-time send-scope gate (the harness has no pathway
	// covering this packet).
	var id int64
	if err := h.store.pool.QueryRow(h.ctx, `
		INSERT INTO tx_outbox (chain_eid, purpose, guid, to_address, calldata, value, signer_id, status)
		VALUES (40161, $1, $2, $3, '\x01', 0, $4, 'queued')
		RETURNING id
	`, txPurposeExecutorLzReceive, guid.Bytes(), addressBytes(common.HexToAddress("0x22")), h.signerID).Scan(&id); err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox SET nonce = 181, status = 'held', held_reason = 'nonce_consumed_externally' WHERE id = $1
	`, id); err != nil {
		t.Fatalf("seed held: %v", err)
	}

	// The workflow already advanced (a third party delivered): cloning would
	// re-execute a task nobody is waiting on.
	if _, err := h.store.ResolveExternalNonceRetry(h.ctx, id); err == nil {
		t.Fatal("ResolveExternalNonceRetry succeeded for an advanced workflow, want rejection")
	}
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldNonceConsumedExternally {
		t.Fatalf("row = %q/%q, want untouched held/nonce_consumed_externally", status, heldReason)
	}
}

func TestResolveExternalNonceAbandonParksWorkflow(t *testing.T) {
	h := newAttemptHarness(t, "0x6565656565656565656565656565656565656565", 141)
	guid := common.HexToHash("0xabad0000abad0000abad0000abad0000abad0000abad0000abad0000abad0000")
	if _, err := h.store.pool.Exec(h.ctx, `DELETE FROM executor_jobs WHERE guid = $1`, guid.Bytes()); err != nil {
		t.Fatalf("clean jobs: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `DELETE FROM packets WHERE guid = $1`, guid.Bytes()); err != nil {
		t.Fatalf("clean packets: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `
		INSERT INTO packets (guid, src_eid, dst_eid, nonce, sender, receiver, send_lib,
			src_tx_hash, src_block_number, src_log_index, encoded_packet, packet_header,
			message, payload_hash, options, status)
		VALUES ($1, 40161, 40449, 1, '\x71', '\x72', '\x73', $2, 1, 0, '\x01', '\x01', '\x02',
			$3, '\x04', 'LZ_RECEIVE_TX_ENQUEUED')
	`, guid.Bytes(), common.HexToHash("0xf00d").Bytes(), common.HexToHash("0xbeef").Bytes()); err != nil {
		t.Fatalf("seed packet: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `
		INSERT INTO executor_jobs (guid, assigned_fee, status) VALUES ($1, 1, 'LZ_RECEIVE_TX_ENQUEUED')
	`, guid.Bytes()); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	// Seed the outbox row directly: these tests exercise the operator resolve
	// path, not the enqueue-time send-scope gate (the harness has no pathway
	// covering this packet).
	var id int64
	if err := h.store.pool.QueryRow(h.ctx, `
		INSERT INTO tx_outbox (chain_eid, purpose, guid, to_address, calldata, value, signer_id, status)
		VALUES (40161, $1, $2, $3, '\x01', 0, $4, 'queued')
		RETURNING id
	`, txPurposeExecutorLzReceive, guid.Bytes(), addressBytes(common.HexToAddress("0x22")), h.signerID).Scan(&id); err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox SET nonce = 141, status = 'held', held_reason = 'nonce_consumed_externally' WHERE id = $1
	`, id); err != nil {
		t.Fatalf("seed held: %v", err)
	}

	if err := h.store.ResolveExternalNonceAbandon(h.ctx, id); err != nil {
		t.Fatalf("ResolveExternalNonceAbandon: %v", err)
	}
	terminal, err := h.store.GetOutboxTx(h.ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx: %v", err)
	}
	if terminal.Status != TxStatusFailed || terminal.FailureKind != TxFailureNonceConsumedExternally {
		t.Fatalf("terminal = %q/%q, want failed/nonce_consumed_externally", terminal.Status, terminal.FailureKind)
	}
	var jobStatus, packetStatus string
	if err := h.store.pool.QueryRow(h.ctx, `
		SELECT ej.status, p.status FROM executor_jobs ej JOIN packets p ON p.guid = ej.guid WHERE ej.guid = $1
	`, guid.Bytes()).Scan(&jobStatus, &packetStatus); err != nil {
		t.Fatalf("select workflow: %v", err)
	}
	if jobStatus != "MANUAL_REVIEW" || packetStatus != "MANUAL_REVIEW" {
		t.Fatalf("workflow = %q/%q, want MANUAL_REVIEW for the abandoned task", jobStatus, packetStatus)
	}
}

func TestCancelClearsStaleReplacementRequest(t *testing.T) {
	h := newAttemptHarness(t, "0x7070707070707070707070707070707070707070", 181)
	id := h.enqueue()
	original := h.signAttempt(id, 181, common.HexToHash("0xa701"))
	h.broadcastResult(original.ID, SendErrorAccepted)

	// The operator asks for a replacement, then abandons the task before it is
	// processed: the stale intent must not survive into the cancel pipeline.
	if err := h.store.RequestTxReplacement(h.ctx, id); err != nil {
		t.Fatalf("RequestTxReplacement: %v", err)
	}
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel: %v", err)
	}
	var replaceRequestedAt *time.Time
	if err := h.store.pool.QueryRow(h.ctx, `SELECT replace_requested_at FROM tx_outbox WHERE id = $1`, id).Scan(&replaceRequestedAt); err != nil {
		t.Fatalf("select replace intent: %v", err)
	}
	if replaceRequestedAt != nil {
		t.Fatal("replace intent survived the cancel request; the first cancel attempt would be bumped without a re-request")
	}
	// And a NEW replace request on the canceling row is rejected outright: it
	// would sit dormant and authorize an unintended cancel bump later.
	if err := h.store.RequestTxReplacement(h.ctx, id); err == nil {
		t.Fatal("RequestTxReplacement(cancel-pending) succeeded, want rejection")
	}

	candidate, err := h.store.NextCancelCandidate(h.ctx, 40161, h.signerID)
	if err != nil {
		t.Fatalf("NextCancelCandidate: %v", err)
	}
	leaseToken := uuid.New()
	if _, err := h.store.ClaimOutboxForCancelSigning(h.ctx, id, candidate.ActiveAttemptID, leaseToken, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForCancelSigning: %v", err)
	}
	cancelAttempt, err := h.store.InsertCancelAttempt(h.ctx, id, original.ID, leaseToken, SignedAttempt{
		Kind: TxAttemptCancel, Nonce: 181, TxType: 2, TxHash: common.HexToHash("0xa702"),
		RawTx: []byte{0xa7, 0x02}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(3_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertCancelAttempt: %v", err)
	}
	broadcastToken := uuid.New()
	claim, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, broadcastToken, 30*time.Second)
	if err != nil || claim.AttemptID != cancelAttempt.ID {
		t.Fatalf("ClaimAttemptForBroadcast(cancel) = (%d, %v), want %d", claim.AttemptID, err, cancelAttempt.ID)
	}
	if err := h.store.MarkAttemptSendResult(h.ctx, claim.AttemptID, broadcastToken, SendErrorAccepted, ""); err != nil {
		t.Fatalf("MarkAttemptSendResult: %v", err)
	}

	// With the stale intent cleared, the active cancel gets no unrequested bump.
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute); !errors.Is(err, ErrNoStaleBroadcastReplacement) {
		t.Fatalf("NextReplacementCandidate(fresh cancel) error = %v, want ErrNoStaleBroadcastReplacement", err)
	}

	// An explicit cancel re-request still authorizes exactly one bump.
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel(re-request): %v", err)
	}
	bumpCandidate, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(re-requested cancel): %v", err)
	}
	if bumpCandidate.Outbox.ID != id || bumpCandidate.ActiveKind != TxAttemptCancel {
		t.Fatalf("candidate = %d kind %q, want the re-requested cancel lane", bumpCandidate.Outbox.ID, bumpCandidate.ActiveKind)
	}
}

func TestReconciliationParksExhaustedReleaseAsBroadcastExhausted(t *testing.T) {
	h := newAttemptHarness(t, "0x7171717171717171717171717171717171717171", 211)
	id := h.enqueue()
	original := h.signAttempt(id, 211, common.HexToHash("0xa711"))
	// The raw hits a stale node: nonce too low parks the lane for
	// reconciliation. By then the attempt's broadcast budget is exhausted.
	h.broadcastResult(original.ID, SendErrorNonceTooLow)
	if _, err := h.store.pool.Exec(h.ctx, `UPDATE tx_attempts SET broadcast_count = $1 WHERE id = $2`, TxMaxBroadcasts, original.ID); err != nil {
		t.Fatalf("exhaust broadcast budget: %v", err)
	}

	token := uuid.New()
	holds, err := h.store.ClaimNonceReconciliation(h.ctx, 40161, h.signerID, token, 30*time.Second)
	if err != nil || len(holds) != 1 {
		t.Fatalf("ClaimNonceReconciliation = (%+v, %v), want the hold", holds, err)
	}
	// Releasing to broadcast would strand the row: the claim ignores attempts
	// at the cap, the parking sweep only covers signed rows, and broadcast is
	// outside the lower-nonce barrier — higher nonces would sign past a nonce
	// that never reached the chain. The release must park it for the operator.
	result, err := h.store.ApplyNonceReconciliation(h.ctx, 40161, h.signerID, token, 211, 9_100, time.Minute,
		[]NonceReconcileDecision{{ID: id, Action: NonceReconcileRelease}})
	if err != nil || result.Changed != 1 {
		t.Fatalf("ApplyNonceReconciliation = (%+v, %v), want one change", result, err)
	}
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldBroadcastExhausted {
		t.Fatalf("outbox = %q/%q, want held/broadcast_exhausted for an exhausted release", status, heldReason)
	}
	if _, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrNoBroadcastCandidate) {
		t.Fatalf("ClaimAttemptForBroadcast error = %v, want ErrNoBroadcastCandidate", err)
	}
	// The parked row keeps the lane barrier: fresh work must not sign past it.
	nextID := h.enqueue()
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, nextID, 40161, h.signerID, uuid.New(), 30*time.Second); err == nil || err.Error() != ErrSignerLaneBlocked.Error() {
		t.Fatalf("ClaimOutboxForSigning(next) error = %v, want ErrSignerLaneBlocked", err)
	}

	// The operator recovery is a replace: it authorizes a fresh attempt with a
	// fresh broadcast budget for the same intent and unblocks the lane.
	if err := h.store.RequestTxReplacement(h.ctx, id); err != nil {
		t.Fatalf("RequestTxReplacement(parked) error = %v", err)
	}
	candidate, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, 15*time.Minute)
	if err != nil {
		t.Fatalf("NextReplacementCandidate(broadcast_exhausted): %v", err)
	}
	if candidate.Outbox.ID != id || candidate.ActiveKind == TxAttemptCancel {
		t.Fatalf("candidate = %d kind %q, want the parked task lane", candidate.Outbox.ID, candidate.ActiveKind)
	}
	bumpLease := uuid.New()
	if _, err := h.store.ClaimOutboxForReplacementSigning(h.ctx, id, original.ID, bumpLease, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForReplacementSigning(broadcast_exhausted): %v", err)
	}
	replacement, err := h.store.InsertReplacementAttempt(h.ctx, id, original.ID, bumpLease, SignedAttempt{
		Kind: TxAttemptReplacement, Nonce: 211, TxType: 2, TxHash: common.HexToHash("0xa712"),
		RawTx: []byte{0xa7, 0x12}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(4_000_000_000), MaxPriorityFeePerGas: big.NewInt(2_000_000_000),
		SigningToken: uuid.New(),
	})
	if err != nil {
		t.Fatalf("InsertReplacementAttempt(broadcast_exhausted): %v", err)
	}
	status, heldReason, _ = h.outboxState(id)
	if status != TxStatusSigned || heldReason != "" {
		t.Fatalf("outbox after replacement = %q/%q, want signed with no hold", status, heldReason)
	}
	broadcastToken := uuid.New()
	claim, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, broadcastToken, 30*time.Second)
	if err != nil || claim.AttemptID != replacement.ID {
		t.Fatalf("ClaimAttemptForBroadcast = (%d, %v), want the fresh replacement %d", claim.AttemptID, err, replacement.ID)
	}
}

func TestResolveExternalNonceRetryRefusesInactiveScope(t *testing.T) {
	h := newAttemptHarness(t, "0x7373737373737373737373737373737373737373", 231)
	restoreScopeFlags(t, h.store)
	id := h.enqueue()
	original := h.signAttempt(id, 231, common.HexToHash("0xa731"))
	h.broadcastResult(original.ID, SendErrorNonceTooLow)
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox SET held_reason = $1, last_error = 'evidence' WHERE id = $2
	`, HeldNonceConsumedExternally, id); err != nil {
		t.Fatalf("park externally consumed: %v", err)
	}

	// Cloning is new spend: a paused scope refuses the operator retry without
	// terminalizing the evidence row or queueing anything.
	setChainFlags(t, h, 40161, true, true)
	if _, err := h.store.ResolveExternalNonceRetry(h.ctx, id); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("ResolveExternalNonceRetry(paused chain) error = %v, want ErrTxSendScopeInactive", err)
	}
	status, heldReason, _ := h.outboxState(id)
	if status != TxStatusHeld || heldReason != HeldNonceConsumedExternally {
		t.Fatalf("row = %q/%q, want untouched held/nonce_consumed_externally", status, heldReason)
	}
	var clones int
	if err := h.store.pool.QueryRow(h.ctx, "SELECT count(*) FROM tx_outbox WHERE retry_of_id = $1", id).Scan(&clones); err != nil {
		t.Fatalf("count clones: %v", err)
	}
	if clones != 0 {
		t.Fatalf("clones = %d, want none while the scope is paused", clones)
	}

	// Unpause: the retry clones normally.
	setChainFlags(t, h, 40161, true, false)
	cloneID, err := h.store.ResolveExternalNonceRetry(h.ctx, id)
	if err != nil {
		t.Fatalf("ResolveExternalNonceRetry(active) error = %v", err)
	}
	if cloneID == 0 || cloneID == id {
		t.Fatalf("clone id = %d, want a fresh clone", cloneID)
	}
}

func TestExtendNonceReconciliationRenewsHeldLeaseOnly(t *testing.T) {
	h := newAttemptHarness(t, "0x7575757575757575757575757575757575757575", 251)
	id := h.enqueue()
	original := h.signAttempt(id, 251, common.HexToHash("0xa751"))
	h.broadcastResult(original.ID, SendErrorNonceTooLow)

	token := uuid.New()
	if _, err := h.store.ClaimNonceReconciliation(h.ctx, 40161, h.signerID, token, 30*time.Second); err != nil {
		t.Fatalf("ClaimNonceReconciliation: %v", err)
	}
	// The held token renews in place; a foreign token cannot.
	if err := h.store.ExtendNonceReconciliation(h.ctx, 40161, h.signerID, token, 30*time.Second); err != nil {
		t.Fatalf("ExtendNonceReconciliation: %v", err)
	}
	if err := h.store.ExtendNonceReconciliation(h.ctx, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrOutboxLeaseLost) {
		t.Fatalf("ExtendNonceReconciliation(foreign token) error = %v, want ErrOutboxLeaseLost", err)
	}
	// A finished (released) lease cannot be renewed either.
	if err := h.store.FinishNonceReconciliation(h.ctx, 40161, h.signerID, token, time.Minute); err != nil {
		t.Fatalf("FinishNonceReconciliation: %v", err)
	}
	if err := h.store.ExtendNonceReconciliation(h.ctx, 40161, h.signerID, token, 30*time.Second); !errors.Is(err, ErrOutboxLeaseLost) {
		t.Fatalf("ExtendNonceReconciliation(released) error = %v, want ErrOutboxLeaseLost", err)
	}
}
