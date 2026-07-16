package db

import (
	"context"
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
		id, err := store.EnqueueTx(ctx, TxRequest{ChainEID: 40161, Purpose: "p", To: common.HexToAddress("0x22"), Calldata: []byte{0x1}, Value: big.NewInt(0), SignerID: signerID})
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
	id, err := store.EnqueueTx(ctx, TxRequest{ChainEID: 40161, Purpose: "p", To: common.HexToAddress("0x22"), Calldata: []byte{0x1}, Value: big.NewInt(0), SignerID: signerID})
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
