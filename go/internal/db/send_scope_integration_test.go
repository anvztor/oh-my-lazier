package db

import (
	"context"
	"errors"
	"math/big"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/jackc/pgx/v5"
)

// restoreScopeFlags re-activates the test chains and pathway after a test
// paused or disabled them, on a fresh context so it also runs when the test's
// own context already expired.
func restoreScopeFlags(t *testing.T, store *Store) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := store.pool.Exec(ctx, "UPDATE chains SET paused = false, enabled = true WHERE eid IN (40161, 40449)"); err != nil {
			t.Fatalf("restore chains: %v", err)
		}
		if _, err := store.pool.Exec(ctx, "UPDATE pathways SET paused = false, enabled = true WHERE src_eid = 40161 AND dst_eid = 40449"); err != nil {
			t.Fatalf("restore pathway: %v", err)
		}
	})
}

func setChainFlags(t *testing.T, h *attemptHarness, eid uint32, enabled, paused bool) {
	t.Helper()
	if _, err := h.store.pool.Exec(h.ctx, "UPDATE chains SET enabled = $2, paused = $3 WHERE eid = $1", eid, enabled, paused); err != nil {
		t.Fatalf("set chain flags: %v", err)
	}
}

// seedScopedPacketRow seeds the standard test packet and enqueues a
// commit-verification row on its destination chain for the harness signer.
func seedScopedPacketRow(t *testing.T, h *attemptHarness, bootstrapNonce uint64) (PacketRecord, int64) {
	t.Helper()
	packet := testPacketRecord()
	cleanPacketRows(h.ctx, t, h.store, packet.GUID)
	if err := h.store.UpsertPacket(h.ctx, packet); err != nil {
		t.Fatalf("UpsertPacket: %v", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, "DELETE FROM tx_nonce_cursors WHERE chain_eid = 40449 AND signer_id = $1", h.signerID); err != nil {
		t.Fatalf("clean dst cursor: %v", err)
	}
	if _, err := h.store.BootstrapTxNonceCursor(h.ctx, 40449, h.signerID, bootstrapNonce); err != nil {
		t.Fatalf("bootstrap dst cursor: %v", err)
	}
	id, err := h.store.EnqueueTx(h.ctx, TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  txPurposeExecutorCommitVerification,
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: h.signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(packet scope): %v", err)
	}
	return packet, id
}

func TestClaimOutboxForSigningRefusesInactiveChainScope(t *testing.T) {
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a1", 700)
	restoreScopeFlags(t, h.store)
	id := h.enqueue()

	setChainFlags(t, h, 40161, true, true)
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("ClaimOutboxForSigning(paused chain) error = %v, want ErrTxSendScopeInactive", err)
	}
	if _, err := h.store.PeekSendableTx(h.ctx, 40161, h.signerID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("PeekSendableTx(paused chain) error = %v, want no rows", err)
	}

	setChainFlags(t, h, 40161, false, false)
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("ClaimOutboxForSigning(disabled chain) error = %v, want ErrTxSendScopeInactive", err)
	}

	setChainFlags(t, h, 40161, true, false)
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, uuid.New(), 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForSigning(active again) error = %v", err)
	}
}

func TestClaimOutboxForSigningRefusesInactivePacketScope(t *testing.T) {
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a2", 710)
	restoreScopeFlags(t, h.store)
	packet, id := seedScopedPacketRow(t, h, 710)

	if err := h.store.PausePathwayForPacket(h.ctx, packet.GUID); err != nil {
		t.Fatalf("PausePathwayForPacket: %v", err)
	}
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40449, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("ClaimOutboxForSigning(paused pathway) error = %v, want ErrTxSendScopeInactive", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, "UPDATE pathways SET paused = false WHERE src_eid = 40161 AND dst_eid = 40449"); err != nil {
		t.Fatalf("unpause pathway: %v", err)
	}

	// The source chain participates in the scope even though the tx spends on
	// the destination chain.
	setChainFlags(t, h, 40161, true, true)
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40449, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("ClaimOutboxForSigning(paused src chain) error = %v, want ErrTxSendScopeInactive", err)
	}
	setChainFlags(t, h, 40161, true, false)

	setChainFlags(t, h, 40449, true, true)
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40449, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("ClaimOutboxForSigning(paused dst chain) error = %v, want ErrTxSendScopeInactive", err)
	}
	setChainFlags(t, h, 40449, true, false)

	// A pathway removed from config fails closed as well.
	if _, err := h.store.pool.Exec(h.ctx, "UPDATE pathways SET enabled = false WHERE src_eid = 40161 AND dst_eid = 40449"); err != nil {
		t.Fatalf("disable pathway: %v", err)
	}
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40449, h.signerID, uuid.New(), 30*time.Second); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("ClaimOutboxForSigning(disabled pathway) error = %v, want ErrTxSendScopeInactive", err)
	}
	if _, err := h.store.pool.Exec(h.ctx, "UPDATE pathways SET enabled = true WHERE src_eid = 40161 AND dst_eid = 40449"); err != nil {
		t.Fatalf("re-enable pathway: %v", err)
	}

	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40449, h.signerID, uuid.New(), 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForSigning(active again) error = %v", err)
	}
}

func TestClaimOutboxForSigningFailsClosedOnMalformedScope(t *testing.T) {
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a3", 720)
	restoreScopeFlags(t, h.store)
	packet := testPacketRecord()
	cleanPacketRows(h.ctx, t, h.store, packet.GUID)
	if err := h.store.UpsertPacket(h.ctx, packet); err != nil {
		t.Fatalf("UpsertPacket: %v", err)
	}

	seedRaw := func(chainEID uint32, purpose string, guid []byte) int64 {
		var id int64
		if err := h.store.pool.QueryRow(h.ctx, `
			INSERT INTO tx_outbox (chain_eid, purpose, guid, to_address, calldata, value, signer_id, status)
			VALUES ($1, $2, $3, $4, '\x01', 0, $5, 'queued')
			RETURNING id
		`, chainEID, purpose, optionalBytes(guid), addressBytes(common.HexToAddress("0x22")), h.signerID).Scan(&id); err != nil {
			t.Fatalf("seed raw row: %v", err)
		}
		return id
	}

	unknown := seedRaw(40161, "unmapped_purpose", nil)
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, unknown, 40161, h.signerID, uuid.New(), 30*time.Second); err == nil || errors.Is(err, ErrTxSendScopeInactive) || !strings.Contains(err.Error(), "no send scope") {
		t.Fatalf("ClaimOutboxForSigning(unknown purpose) error = %v, want fail-closed scope error", err)
	}

	// The packet's destination is 40449; a row claiming to spend for it on the
	// source chain must fail closed.
	mismatch := seedRaw(40161, txPurposeExecutorCommitVerification, packet.GUID.Bytes())
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, mismatch, 40161, h.signerID, uuid.New(), 30*time.Second); err == nil || errors.Is(err, ErrTxSendScopeInactive) || !strings.Contains(err.Error(), "does not match packet") {
		t.Fatalf("ClaimOutboxForSigning(chain mismatch) error = %v, want fail-closed mismatch error", err)
	}

	missingGUID := seedRaw(40161, txPurposeDVNVerify, nil)
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, missingGUID, 40161, h.signerID, uuid.New(), 30*time.Second); err == nil || errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("ClaimOutboxForSigning(packet purpose without guid) error = %v, want fail-closed error", err)
	}

	// Selection also fails closed: none of the malformed rows are offered.
	if _, err := h.store.PeekSendableTx(h.ctx, 40161, h.signerID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("PeekSendableTx(malformed rows only) error = %v, want no rows", err)
	}

	// The enqueue front door rejects unmapped purposes outright instead of
	// accepting a row the selector would silently filter forever.
	if _, err := h.store.EnqueueTx(h.ctx, TxRequest{
		ChainEID: 40161,
		Purpose:  "unmapped_purpose",
		To:       common.HexToAddress("0x22"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: h.signerID,
	}); err == nil || errors.Is(err, ErrTxSendScopeInactive) || !strings.Contains(err.Error(), "no send scope") {
		t.Fatalf("EnqueueTx(unknown purpose) error = %v, want fail-closed scope error", err)
	}
}

func TestPeekSendableTxSkipsPausedScopeWithoutStarvingActiveWork(t *testing.T) {
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a4", 730)
	restoreScopeFlags(t, h.store)
	packet, packetRowID := seedScopedPacketRow(t, h, 730)
	pricingID, err := h.store.EnqueueTx(h.ctx, TxRequest{
		ChainEID: 40449,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x22"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: h.signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(pricing on 40449): %v", err)
	}
	if packetRowID >= pricingID {
		t.Fatalf("test setup: packet row %d must precede pricing row %d", packetRowID, pricingID)
	}

	if err := h.store.PausePathwayForPacket(h.ctx, packet.GUID); err != nil {
		t.Fatalf("PausePathwayForPacket: %v", err)
	}
	peeked, err := h.store.PeekSendableTx(h.ctx, 40449, h.signerID)
	if err != nil {
		t.Fatalf("PeekSendableTx: %v", err)
	}
	if peeked.ID != pricingID {
		t.Fatalf("peeked row = %d, want the active pricing row %d (paused pathway row must not starve the lane)", peeked.ID, pricingID)
	}

	// A row that already holds a nonce is in flight and stays selectable even
	// though its pathway is paused.
	if _, err := h.store.pool.Exec(h.ctx, "UPDATE tx_outbox SET nonce = 730, status = 'nonce_assigned' WHERE id = $1", packetRowID); err != nil {
		t.Fatalf("seed in-flight nonce: %v", err)
	}
	peeked, err = h.store.PeekSendableTx(h.ctx, 40449, h.signerID)
	if err != nil {
		t.Fatalf("PeekSendableTx(in-flight): %v", err)
	}
	if peeked.ID != packetRowID {
		t.Fatalf("peeked row = %d, want the in-flight packet row %d", peeked.ID, packetRowID)
	}
}

func TestEnqueueGatesRefuseInactiveScope(t *testing.T) {
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a5", 740)
	restoreScopeFlags(t, h.store)
	packet := testPacketRecord()
	cleanPacketRows(h.ctx, t, h.store, packet.GUID)
	if err := h.store.UpsertPacket(h.ctx, packet); err != nil {
		t.Fatalf("UpsertPacket: %v", err)
	}
	if err := h.store.UpsertExecutorJob(h.ctx, ExecutorJobRecord{GUID: packet.GUID, AssignedFee: big.NewInt(1), Status: string(packets.ExecutorVerifiable)}); err != nil {
		t.Fatalf("UpsertExecutorJob: %v", err)
	}
	if err := h.store.UpsertDVNJob(h.ctx, DVNJobRecord{GUID: packet.GUID, ConfirmationsRequired: 1, Status: string(packets.DVNReadyToVerify)}); err != nil {
		t.Fatalf("UpsertDVNJob: %v", err)
	}
	if err := h.store.PausePathwayForPacket(h.ctx, packet.GUID); err != nil {
		t.Fatalf("PausePathwayForPacket: %v", err)
	}

	request := TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  txPurposeExecutorCommitVerification,
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0x22"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: h.signerID,
	}
	if _, err := h.store.EnqueueExecutorTx(h.ctx, packet.GUID, string(packets.ExecutorVerifiable), string(packets.ExecutorCommitTxEnqueued), request); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("EnqueueExecutorTx(paused pathway) error = %v, want ErrTxSendScopeInactive", err)
	}
	job, err := h.store.GetExecutorJob(h.ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob: %v", err)
	}
	if job.Status != string(packets.ExecutorVerifiable) {
		t.Fatalf("executor job status = %q, want untouched %q", job.Status, packets.ExecutorVerifiable)
	}

	dvnRequest := request
	dvnRequest.Purpose = txPurposeDVNVerify
	if _, err := h.store.EnqueueDVNVerifyTx(h.ctx, packet.GUID, string(packets.DVNReadyToVerify), string(packets.DVNVerifyTxEnqueued), dvnRequest, []byte(`{}`)); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("EnqueueDVNVerifyTx(paused pathway) error = %v, want ErrTxSendScopeInactive", err)
	}

	setChainFlags(t, h, 40161, true, true)
	if _, err := h.store.EnqueueTx(h.ctx, TxRequest{
		ChainEID: 40161,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x22"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: h.signerID,
	}); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("EnqueueTx(pricing, paused chain) error = %v, want ErrTxSendScopeInactive", err)
	}
}

func TestFailedRetryDefersWhileScopePausedAndResumes(t *testing.T) {
	// Retry semantics are exercised with a packet-scoped purpose: pricing rows
	// carry time-bound observations and are excluded from every retry path.
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a6", 750)
	restoreScopeFlags(t, h.store)
	_, id := seedScopedPacketRow(t, h, 750)
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox
		SET status = 'failed', failure_kind = 'receipt_failed', nonce = 750,
			next_retry_at = now() - interval '1 second',
			lease_token = NULL, lease_until = NULL, updated_at = now()
		WHERE id = $1
	`, id); err != nil {
		t.Fatalf("seed receipt-failed row: %v", err)
	}
	setChainFlags(t, h, 40449, true, true)

	if _, err := h.store.RetryFailedTx(h.ctx, id); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("RetryFailedTx(paused chain) error = %v, want ErrTxSendScopeInactive", err)
	}
	var status, failureKind string
	var nextRetryAt time.Time
	var children int
	if err := h.store.pool.QueryRow(h.ctx, `
		SELECT status, failure_kind, next_retry_at,
			(SELECT count(*) FROM tx_outbox child WHERE child.retry_of_id = tx_outbox.id)
		FROM tx_outbox WHERE id = $1
	`, id).Scan(&status, &failureKind, &nextRetryAt, &children); err != nil {
		t.Fatalf("read deferred row: %v", err)
	}
	if status != TxStatusFailed || failureKind != TxFailureReceiptFailed || children != 0 {
		t.Fatalf("deferred row = %s/%s with %d children, want untouched failed/receipt_failed with no clone", status, failureKind, children)
	}
	if !nextRetryAt.After(time.Now()) {
		t.Fatalf("next_retry_at = %s, want pushed into the future while paused", nextRetryAt)
	}

	// The automatic driver defers the same way instead of cloning.
	if _, err := h.store.pool.Exec(h.ctx, "UPDATE tx_outbox SET next_retry_at = now() - interval '1 second' WHERE id = $1", id); err != nil {
		t.Fatalf("make row due: %v", err)
	}
	if _, err := h.store.PrepareNextFailedTxRetry(h.ctx, 40449, h.signerID); !errors.Is(err, ErrNoFailedTxRetry) {
		t.Fatalf("PrepareNextFailedTxRetry(paused chain) error = %v, want ErrNoFailedTxRetry", err)
	}
	if err := h.store.pool.QueryRow(h.ctx, "SELECT next_retry_at FROM tx_outbox WHERE id = $1", id).Scan(&nextRetryAt); err != nil {
		t.Fatalf("read auto-deferred row: %v", err)
	}
	if !nextRetryAt.After(time.Now()) {
		t.Fatalf("auto next_retry_at = %s, want pushed into the future while paused", nextRetryAt)
	}

	// Unpause: the retry clones normally with the failure budget intact.
	setChainFlags(t, h, 40449, true, false)
	retryID, err := h.store.RetryFailedTx(h.ctx, id)
	if err != nil {
		t.Fatalf("RetryFailedTx(active again) error = %v", err)
	}
	if retryID == id || retryID == 0 {
		t.Fatalf("retry id = %d, want a fresh clone of %d", retryID, id)
	}
}

func TestClaimAndPauseLinearizeOnScopeLocks(t *testing.T) {
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a7", 760)
	restoreScopeFlags(t, h.store)
	id := h.enqueue()

	// Pause commits first: a claim that raced it must observe the pause.
	pauseTx, err := h.store.pool.Begin(h.ctx)
	if err != nil {
		t.Fatalf("begin pause tx: %v", err)
	}
	if _, err := pauseTx.Exec(h.ctx, "UPDATE chains SET paused = true WHERE eid = 40161"); err != nil {
		t.Fatalf("pause inside tx: %v", err)
	}
	claimErr := make(chan error, 1)
	go func() {
		_, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, uuid.New(), 30*time.Second)
		claimErr <- err
	}()
	// Give the claim time to reach the scope share lock and block on it.
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-claimErr:
		t.Fatalf("claim finished before the pause committed: %v (must block on the scope lock)", err)
	default:
	}
	if err := pauseTx.Commit(h.ctx); err != nil {
		t.Fatalf("commit pause: %v", err)
	}
	if err := <-claimErr; !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("claim after pause commit error = %v, want ErrTxSendScopeInactive", err)
	}
	var nonce *int64
	if err := h.store.pool.QueryRow(h.ctx, "SELECT nonce FROM tx_outbox WHERE id = $1", id).Scan(&nonce); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if nonce != nil {
		t.Fatalf("nonce = %d, want none reserved after the pause won", *nonce)
	}

	// Claim commits first: the row is in flight and converges under the pause.
	setChainFlags(t, h, 40161, true, false)
	token := uuid.New()
	if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, token, 30*time.Second); err != nil {
		t.Fatalf("ClaimOutboxForSigning(before pause) error = %v", err)
	}
	setChainFlags(t, h, 40161, true, true)
	attempt, err := h.store.InsertSignedAttempt(h.ctx, id, token, SignedAttempt{
		Kind: TxAttemptOriginal, Nonce: 760, TxType: 2, TxHash: common.HexToHash("0x760a"),
		RawTx: []byte{0x76, 0x0a}, GasLimit: 21000,
		MaxFeePerGas: big.NewInt(2_000_000_000), MaxPriorityFeePerGas: big.NewInt(1_000_000_000),
	})
	if err != nil {
		t.Fatalf("InsertSignedAttempt(under pause) error = %v (in-flight rows must converge)", err)
	}
	broadcastToken := uuid.New()
	claim, err := h.store.ClaimAttemptForBroadcast(h.ctx, 40161, h.signerID, broadcastToken, 45*time.Second)
	if err != nil {
		t.Fatalf("ClaimAttemptForBroadcast(under pause) error = %v", err)
	}
	if claim.AttemptID != attempt.ID {
		t.Fatalf("broadcast claim attempt = %d, want %d", claim.AttemptID, attempt.ID)
	}
	if err := h.store.MarkAttemptSendResult(h.ctx, claim.AttemptID, broadcastToken, SendErrorAccepted, ""); err != nil {
		t.Fatalf("MarkAttemptSendResult(under pause) error = %v", err)
	}
	var status string
	if err := h.store.pool.QueryRow(h.ctx, "SELECT status FROM tx_outbox WHERE id = $1", id).Scan(&status); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != TxStatusBroadcast {
		t.Fatalf("status = %q, want broadcast (pre-pause nonce holders complete)", status)
	}

	// Replacement and cancel stay available while paused: they repair the lane
	// instead of adding new spend.
	if _, err := h.store.NextReplacementCandidate(h.ctx, 40161, h.signerID, time.Nanosecond); err != nil {
		t.Fatalf("NextReplacementCandidate(under pause) error = %v", err)
	}
	if err := h.store.RequestTxCancel(h.ctx, id); err != nil {
		t.Fatalf("RequestTxCancel(under pause) error = %v", err)
	}
}

func TestSyncConfigConcurrentWithClaimsDoesNotDeadlock(t *testing.T) {
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a8", 770)
	restoreScopeFlags(t, h.store)
	registry, err := chain.NewRegistry(testChains(), testPathways())
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 15; i++ {
			if err := h.store.SyncConfig(h.ctx, registry); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 15; i++ {
			id, err := h.store.EnqueueTx(h.ctx, TxRequest{
				ChainEID: 40161,
				Purpose:  TxPurposePricingSetPriceSnapshot,
				To:       common.HexToAddress("0x22"),
				Calldata: []byte{0x01},
				Value:    big.NewInt(0),
				SignerID: h.signerID,
			})
			if err != nil {
				errs <- err
				return
			}
			if _, err := h.store.ClaimOutboxForSigning(h.ctx, id, 40161, h.signerID, uuid.New(), 30*time.Second); err != nil {
				errs <- err
				return
			}
			if _, err := h.store.pool.Exec(h.ctx, "DELETE FROM tx_outbox WHERE id = $1", id); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent SyncConfig/claim error = %v", err)
	}
}

func TestSyncConfigConcurrentFirstStartupDoesNotDeadlock(t *testing.T) {
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	// A truly empty schema: with no chains or pathways rows at all, the ordered
	// pre-lock scans have nothing to serialize on and only the global
	// config_sync advisory lock stands between two first-startup instances
	// inserting the same brand-new rows.
	admin, err := Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("Connect(admin): %v", err)
	}
	t.Cleanup(admin.Close)
	const schema = "sync_fresh_startup_test"
	if _, err := admin.pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := admin.pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.pool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Fatalf("drop schema: %v", err)
		}
	})
	separator := "?"
	if strings.Contains(databaseURL, "?") {
		separator = "&"
	}
	store, err := Connect(ctx, databaseURL+separator+"search_path="+schema+",public")
	if err != nil {
		t.Fatalf("Connect(schema): %v", err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate(schema): %v", err)
	}

	freshChains := []config.ChainConfig{
		{
			EID:             52161,
			Name:            "sync-fresh-src",
			Family:          config.ChainFamilyEVM,
			ChainID:         52161,
			EndpointAddress: config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
			Confirmations:   1,
			RPCURLs:         []string{"http://localhost:8545"},
			TxRoles:         config.ChainTxRolesConfig{Executor: testExecutorRole()},
		},
		{
			EID:             52449,
			Name:            "sync-fresh-dst",
			Family:          config.ChainFamilyEVM,
			ChainID:         52449,
			EndpointAddress: config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
			Confirmations:   1,
			RPCURLs:         []string{"http://localhost:8546"},
			TxRoles:         config.ChainTxRolesConfig{Executor: testExecutorRole()},
		},
	}
	freshPathways := []config.PathwayConfig{
		{
			SrcEID:     52161,
			DstEID:     52449,
			SrcOApp:    config.MustEVMAddress("0x7777777777777777777777777777777777777777"),
			DstOApp:    config.MustEVMAddress("0x8888888888888888888888888888888888888888"),
			SendLib:    config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
			ReceiveLib: config.MustEVMAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			SourceWorkers: config.WorkerContractsConfig{
				OpenExecutor: config.MustEVMAddress("0x2222222222222222222222222222222222222222"),
				OpenDVN:      config.MustEVMAddress("0x3333333333333333333333333333333333333333"),
				PriceFeed:    config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
			},
			DestinationWorkers: config.DestinationWorkerContractsConfig{
				OpenDVN: config.MustEVMAddress("0x6666666666666666666666666666666666666666"),
			},
			DVN:            config.PathwayDVNConfig{Mode: config.DVNModeShadow},
			Enabled:        true,
			MaxMessageSize: 10000,
		},
	}
	freshRegistry, err := chain.NewRegistry(freshChains, freshPathways)
	if err != nil {
		t.Fatalf("NewRegistry(fresh): %v", err)
	}

	// Rolling first startup: several instances race to insert the same
	// brand-new chain and pathway rows into an empty schema.
	for round := 0; round < 5; round++ {
		if _, err := store.pool.Exec(ctx, "TRUNCATE pathways, chains CASCADE"); err != nil {
			t.Fatalf("truncate fresh tables: %v", err)
		}
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for instance := 0; instance < 2; instance++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := store.SyncConfig(ctx, freshRegistry); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent first-startup SyncConfig error = %v", err)
		}
		var chainCount, pathwayCount int
		if err := store.pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM chains), (SELECT count(*) FROM pathways)").Scan(&chainCount, &pathwayCount); err != nil {
			t.Fatalf("count synced rows: %v", err)
		}
		if chainCount != 2 || pathwayCount != 1 {
			t.Fatalf("synced rows = %d chains / %d pathways, want 2/1", chainCount, pathwayCount)
		}
	}

	// The serialization mechanism itself, independent of map-iteration luck: a
	// held config_sync advisory lock must block a concurrent SyncConfig until
	// it is released.
	lockTx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, "SELECT pg_advisory_xact_lock(0, hashtext('config_sync'))"); err != nil {
		t.Fatalf("hold config sync lock: %v", err)
	}
	syncDone := make(chan error, 1)
	go func() { syncDone <- store.SyncConfig(ctx, freshRegistry) }()
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-syncDone:
		t.Fatalf("SyncConfig finished while the config_sync lock was held: %v", err)
	default:
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release config sync lock: %v", err)
	}
	if err := <-syncDone; err != nil {
		t.Fatalf("SyncConfig after lock release error = %v", err)
	}
}

func TestEstimateRevertRetryDefersWhileScopePausedAndResumes(t *testing.T) {
	// Retry semantics are exercised with a packet-scoped purpose: pricing rows
	// carry time-bound observations and are excluded from every retry path.
	h := newAttemptHarness(t, "0x5c0be000000000000000000000000000000000a7", 760)
	restoreScopeFlags(t, h.store)
	_, id := seedScopedPacketRow(t, h, 760)
	if _, err := h.store.pool.Exec(h.ctx, `
		UPDATE tx_outbox
		SET status = 'failed', failure_kind = 'estimate_gas_revert', attempts = 4,
			next_retry_at = now() - interval '1 second', updated_at = now()
		WHERE id = $1
	`, id); err != nil {
		t.Fatalf("seed estimate-revert row: %v", err)
	}
	setChainFlags(t, h, 40449, true, true)

	// Requeueing charges attempts + 1 and clears failure metadata, so a paused
	// scope must defer the retry without mutating anything: otherwise every
	// pause cycle burns an automatic retry for free.
	if _, err := h.store.RetryFailedTx(h.ctx, id); !errors.Is(err, ErrTxSendScopeInactive) {
		t.Fatalf("RetryFailedTx(paused chain) error = %v, want ErrTxSendScopeInactive", err)
	}
	if _, err := h.store.PrepareNextFailedTxRetry(h.ctx, 40449, h.signerID); !errors.Is(err, ErrNoFailedTxRetry) {
		t.Fatalf("PrepareNextFailedTxRetry(paused chain) error = %v, want ErrNoFailedTxRetry", err)
	}
	var status, failureKind string
	var attempts int
	var nextRetryAt time.Time
	if err := h.store.pool.QueryRow(h.ctx, `
		SELECT status, failure_kind, attempts, next_retry_at FROM tx_outbox WHERE id = $1
	`, id).Scan(&status, &failureKind, &attempts, &nextRetryAt); err != nil {
		t.Fatalf("read deferred row: %v", err)
	}
	if status != TxStatusFailed || failureKind != TxFailureEstimateGasRevert || attempts != 4 {
		t.Fatalf("deferred row = %s/%s attempts=%d, want untouched failed/estimate_gas_revert attempts=4", status, failureKind, attempts)
	}
	if !nextRetryAt.After(time.Now()) {
		t.Fatalf("next_retry_at = %s, want pushed into the future while paused", nextRetryAt)
	}

	// Unpause: the requeue proceeds and charges the budget exactly once.
	if _, err := h.store.pool.Exec(h.ctx, "UPDATE tx_outbox SET next_retry_at = now() - interval '1 second' WHERE id = $1", id); err != nil {
		t.Fatalf("make row due: %v", err)
	}
	setChainFlags(t, h, 40449, true, false)
	retryID, err := h.store.RetryFailedTx(h.ctx, id)
	if err != nil {
		t.Fatalf("RetryFailedTx(active again) error = %v", err)
	}
	if retryID != id {
		t.Fatalf("retry id = %d, want in-place requeue of %d", retryID, id)
	}
	if err := h.store.pool.QueryRow(h.ctx, "SELECT status, attempts FROM tx_outbox WHERE id = $1", id).Scan(&status, &attempts); err != nil {
		t.Fatalf("read requeued row: %v", err)
	}
	if status != TxStatusQueued || attempts != 5 {
		t.Fatalf("requeued row = %s attempts=%d, want queued attempts=5", status, attempts)
	}
}
