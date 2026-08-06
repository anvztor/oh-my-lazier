package db

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
)

func TestMigrateAndSyncConfig(t *testing.T) {
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
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

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

	var chains, pathways int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM chains WHERE eid IN (40161, 40449)").Scan(&chains); err != nil {
		t.Fatalf("count chains: %v", err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM pathways WHERE src_eid = 40161 AND dst_eid = 40449").Scan(&pathways); err != nil {
		t.Fatalf("count pathways: %v", err)
	}
	if chains != 2 {
		t.Fatalf("chains = %d, want 2", chains)
	}
	if pathways != 1 {
		t.Fatalf("pathways = %d, want 1", pathways)
	}
	var openExecutor, openDVN, priceFeed, destinationOpenDVN []byte
	if err := store.pool.QueryRow(ctx, `
			SELECT open_executor, open_dvn, price_feed, destination_open_dvn
			FROM pathways
			WHERE src_eid = 40161 AND dst_eid = 40449
		`).Scan(&openExecutor, &openDVN, &priceFeed, &destinationOpenDVN); err != nil {
		t.Fatalf("select pathway workers: %v", err)
	}
	if got := common.BytesToAddress(openExecutor); got != common.HexToAddress("0x2222222222222222222222222222222222222222") {
		t.Fatalf("open_executor = %s", got)
	}
	if got := common.BytesToAddress(openDVN); got != common.HexToAddress("0x3333333333333333333333333333333333333333") {
		t.Fatalf("open_dvn = %s", got)
	}
	if got := common.BytesToAddress(priceFeed); got != common.HexToAddress("0x4444444444444444444444444444444444444444") {
		t.Fatalf("price_feed = %s", got)
	}
	if got := common.BytesToAddress(destinationOpenDVN); got != common.HexToAddress("0x6666666666666666666666666666666666666666") {
		t.Fatalf("destination_open_dvn = %s", got)
	}
}

func TestSourcePacketSkipRoundTrip(t *testing.T) {
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
	skip := SourcePacketSkip{
		Role:           "executor",
		SrcEID:         40161,
		DstEID:         40449,
		Nonce:          77,
		Sender:         common.HexToAddress("0x7777777777777777777777777777777777777777"),
		Receiver:       common.HexToAddress("0x8888888888888888888888888888888888888888"),
		GUID:           common.HexToHash("0x7777777777777777777777777777777777777777777777777777777777777777"),
		SrcTxHash:      common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		SrcBlockNumber: 123,
		SrcLogIndex:    4,
		Reason:         "unexpected_worker",
		Worker:         common.HexToAddress("0x2323232323232323232323232323232323232323"),
	}
	if err := store.RecordSourcePacketSkip(ctx, skip); err != nil {
		t.Fatalf("RecordSourcePacketSkip() error = %v", err)
	}
	got, err := store.GetSourcePacketSkip(ctx, skip.Role, skip.SrcEID, skip.DstEID, skip.Sender, skip.Receiver, skip.Nonce)
	if err != nil {
		t.Fatalf("GetSourcePacketSkip() error = %v", err)
	}
	if got != skip {
		t.Fatalf("source packet skip = %+v, want %+v", got, skip)
	}
}

func TestPausePathwayForPacketAndChain(t *testing.T) {
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

	packet := testPacketRecord()
	cleanPacketRows(ctx, t, store, packet.GUID)
	resetPause := func(ctx context.Context, store *Store) error {
		if _, err := store.pool.Exec(ctx, "UPDATE chains SET paused = false WHERE eid = $1", packet.SrcEID); err != nil {
			return err
		}
		_, err := store.pool.Exec(ctx, `
			UPDATE pathways
			SET paused = false
			WHERE src_eid = $1 AND dst_eid = $2 AND src_oapp = $3 AND dst_oapp = $4
		`, packet.SrcEID, packet.DstEID, addressBytes(packet.Sender), addressBytes(packet.Receiver))
		return err
	}
	if err := resetPause(ctx, store); err != nil {
		t.Fatalf("reset pause: %v", err)
	}
	// The pause must not leak into later tests that select work on this shared
	// chain/pathway (ListDVNWork/ListExecutorWork exclude paused rows). The test
	// context and store are already torn down when cleanups run, so dial fresh.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupStore, err := Connect(cleanupCtx, databaseURL)
		if err != nil {
			t.Errorf("cleanup connect: %v", err)
			return
		}
		defer cleanupStore.Close()
		if err := resetPause(cleanupCtx, cleanupStore); err != nil {
			t.Errorf("cleanup reset pause: %v", err)
		}
	})
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}

	if err := store.PausePathwayForPacket(ctx, packet.GUID); err != nil {
		t.Fatalf("PausePathwayForPacket() error = %v", err)
	}
	if err := store.PauseChain(ctx, packet.SrcEID); err != nil {
		t.Fatalf("PauseChain() error = %v", err)
	}

	var pathwayPaused, chainPaused bool
	if err := store.pool.QueryRow(ctx, `
		SELECT paused
		FROM pathways
		WHERE src_eid = $1 AND dst_eid = $2 AND src_oapp = $3 AND dst_oapp = $4
	`, packet.SrcEID, packet.DstEID, addressBytes(packet.Sender), addressBytes(packet.Receiver)).Scan(&pathwayPaused); err != nil {
		t.Fatalf("select pathway paused: %v", err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT paused FROM chains WHERE eid = $1", packet.SrcEID).Scan(&chainPaused); err != nil {
		t.Fatalf("select chain paused: %v", err)
	}
	if !pathwayPaused {
		t.Fatal("pathway paused = false, want true")
	}
	if !chainPaused {
		t.Fatal("chain paused = false, want true")
	}
}

func TestMarkDVNManualReviewAndPausePathwayIsAtomic(t *testing.T) {
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

	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xfeedcccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	packet.SrcEID = 50211
	packet.DstEID = 50212
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	report := []byte(`{"status":"ready"}`)
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		AssignedFee:           big.NewInt(43),
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNReadyToVerify),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	const reason = "destination receive library drift"
	if err := store.MarkDVNManualReviewAndPausePathway(ctx, packet.GUID, string(packets.DVNReadyToVerify), reason, report); err != nil {
		t.Fatalf("MarkDVNManualReviewAndPausePathway() error = %v", err)
	}

	var status, lastError, quorumResult string
	var paused bool
	if err := store.pool.QueryRow(ctx, `
		SELECT dj.status, dj.last_error, dj.quorum_result::text, pathway.paused
		FROM dvn_jobs AS dj
		JOIN packets AS packet ON packet.guid = dj.guid
		JOIN pathways AS pathway ON
			pathway.src_eid = packet.src_eid
			AND pathway.dst_eid = packet.dst_eid
			AND pathway.src_oapp = packet.sender
			AND pathway.dst_oapp = packet.receiver
		WHERE dj.guid = $1
	`, packet.GUID.Bytes()).Scan(&status, &lastError, &quorumResult, &paused); err != nil {
		t.Fatalf("select manual review state: %v", err)
	}
	if status != string(packets.DVNManualReview) || lastError != reason || quorumResult == "" || !paused {
		t.Fatalf("manual review state = status %q, error %q, report %q, paused %t", status, lastError, quorumResult, paused)
	}
}

func TestClaimOutboxForSigningSerializesLane(t *testing.T) {
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

	const signerID = "0x9999999999999999999999999999999999999999"
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test rows: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_nonce_cursors WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test cursor: %v", err)
	}
	ids := make([]int64, 0, 5)
	for range 5 {
		id, err := store.EnqueueTx(ctx, TxRequest{
			ChainEID: 40161,
			Purpose:  TxPurposePricingSetPriceSnapshot,
			To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Calldata: []byte{0x01, 0x02},
			Value:    big.NewInt(0),
			SignerID: signerID,
		})
		if err != nil {
			t.Fatalf("EnqueueTx() error = %v", err)
		}
		ids = append(ids, id)
	}
	inserted, err := store.BootstrapTxNonceCursor(ctx, 40161, signerID, 42)
	if err != nil {
		t.Fatalf("BootstrapTxNonceCursor() error = %v", err)
	}
	if !inserted {
		t.Fatal("BootstrapTxNonceCursor() inserted = false, want true")
	}

	// Concurrent signing claims across 5 queued rows: the signer lane admits
	// exactly one in-flight nonce, so exactly one claim wins and the rest are
	// blocked (never a duplicate or skipped nonce).
	type result struct {
		nonce uint64
		err   error
	}
	results := make(chan result, len(ids))
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() {
			row, err := store.ClaimOutboxForSigning(ctx, id, 40161, signerID, uuid.New(), 30*time.Second)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{nonce: row.Nonce}
		})
	}
	wg.Wait()
	close(results)

	winners := 0
	for res := range results {
		switch {
		case res.err == nil:
			winners++
			if res.nonce != 42 {
				t.Fatalf("claimed nonce = %d, want 42", res.nonce)
			}
		case errors.Is(res.err, ErrSignerLaneBlocked) || errors.Is(res.err, ErrOutboxLeaseLost):
			// Normal lane contention.
		default:
			t.Fatalf("ClaimOutboxForSigning() error = %v", res.err)
		}
	}
	if winners != 1 {
		t.Fatalf("winning claims = %d, want exactly 1", winners)
	}

	// Once the winner reaches broadcast, the next claim takes the next nonce.
	var winnerID int64
	if err := store.pool.QueryRow(ctx, `
		SELECT id FROM tx_outbox WHERE signer_id = $1 AND status = 'nonce_assigned'
	`, signerID).Scan(&winnerID); err != nil {
		t.Fatalf("select winner: %v", err)
	}
	seedBroadcastMirror(ctx, t, store, winnerID, 42, common.HexToHash("0x4242"))
	var nextID int64
	for _, id := range ids {
		if id != winnerID {
			nextID = id
			break
		}
	}
	next, err := store.ClaimOutboxForSigning(ctx, nextID, 40161, signerID, uuid.New(), 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutboxForSigning(next) error = %v", err)
	}
	if next.Nonce != 43 {
		t.Fatalf("next nonce = %d, want 43", next.Nonce)
	}
}

func TestBootstrapTxNonceCursorIsInsertOnlyAndUsesLocalMax(t *testing.T) {
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

	const signerID = "0x7777777777777777777777777777777777777777"
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test rows: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_nonce_cursors WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test cursor: %v", err)
	}
	usedID, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: 40161,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(used) error = %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET nonce = 10, status = $1
		WHERE id = $2
	`, TxStatusConfirmed, usedID); err != nil {
		t.Fatalf("mark used nonce: %v", err)
	}
	firstQueuedID, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: 40161,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x02},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(first queued) error = %v", err)
	}

	inserted, err := store.BootstrapTxNonceCursor(ctx, 40161, signerID, 5)
	if err != nil {
		t.Fatalf("BootstrapTxNonceCursor() error = %v", err)
	}
	if !inserted {
		t.Fatal("BootstrapTxNonceCursor() inserted = false, want true")
	}
	claimed, err := store.ClaimOutboxForSigning(ctx, firstQueuedID, 40161, signerID, uuid.New(), 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutboxForSigning() error = %v", err)
	}
	if claimed.ID != firstQueuedID {
		t.Fatalf("claimed id = %d, want %d", claimed.ID, firstQueuedID)
	}
	if claimed.Nonce != 11 {
		t.Fatalf("claimed nonce = %d, want 11", claimed.Nonce)
	}

	inserted, err = store.BootstrapTxNonceCursor(ctx, 40161, signerID, 99)
	if err != nil {
		t.Fatalf("BootstrapTxNonceCursor(existing) error = %v", err)
	}
	if inserted {
		t.Fatal("BootstrapTxNonceCursor(existing) inserted = true, want false")
	}
	// Move the first claim to broadcast so the signer lane admits the next nonce.
	seedBroadcastMirror(ctx, t, store, firstQueuedID, 11, common.HexToHash("0x1101"))
	secondQueuedID, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: 40161,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x03},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(second queued) error = %v", err)
	}
	claimed, err = store.ClaimOutboxForSigning(ctx, secondQueuedID, 40161, signerID, uuid.New(), 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutboxForSigning() after existing bootstrap error = %v", err)
	}
	if claimed.ID != secondQueuedID {
		t.Fatalf("claimed id = %d, want %d", claimed.ID, secondQueuedID)
	}
	if claimed.Nonce != 12 {
		t.Fatalf("claimed nonce = %d, want 12", claimed.Nonce)
	}
}

func TestRetryFailedTxClonesAssignedNonceAndFreshRetryUsesCursor(t *testing.T) {
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

	// Retry semantics are exercised with a packet-scoped purpose: pricing rows
	// carry time-bound observations and are excluded from every retry path.
	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1")
	packet.Nonce = big.NewInt(9101)
	packet.PayloadHash = common.HexToHash("0xd1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1")
	cleanPacketRows(ctx, t, store, packet.GUID)
	t.Cleanup(func() {
		cleanCtx, cancelClean := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelClean()
		cleanStore, err := Connect(cleanCtx, databaseURL)
		if err != nil {
			t.Fatalf("connect for packet cleanup: %v", err)
		}
		defer cleanStore.Close()
		cleanPacketRows(cleanCtx, t, cleanStore, packet.GUID)
	})
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	const signerID = "0x8888888888888888888888888888888888888888"
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test rows: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_nonce_cursors WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test cursor: %v", err)
	}
	if inserted, err := store.BootstrapTxNonceCursor(ctx, 40449, signerID, 42); err != nil {
		t.Fatalf("BootstrapTxNonceCursor() error = %v", err)
	} else if !inserted {
		t.Fatal("BootstrapTxNonceCursor() inserted = false, want true")
	}
	id, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  txPurposeExecutorCommitVerification,
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	claimed, err := store.ClaimOutboxForSigning(ctx, id, 40449, signerID, uuid.New(), 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutboxForSigning() error = %v", err)
	}
	if claimed.Nonce != 42 {
		t.Fatalf("initial nonce = %d, want 42", claimed.Nonce)
	}
	txHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	seedBroadcastMirror(ctx, t, store, id, 42, txHash)
	seedReceiptFailed(ctx, t, store, id)

	retryID, err := store.RetryFailedTx(ctx, id)
	if err != nil {
		t.Fatalf("RetryFailedTx() error = %v", err)
	}
	if retryID == id {
		t.Fatalf("retry id = %d, want cloned row", retryID)
	}
	originalTx, err := store.GetOutboxTx(ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx(original) error = %v", err)
	}
	if originalTx.Status != TxStatusFailed {
		t.Fatalf("original status = %q, want %q", originalTx.Status, TxStatusFailed)
	}
	if originalTx.Nonce != 42 {
		t.Fatalf("original nonce = %d, want 42", originalTx.Nonce)
	}
	if originalTx.TxHash != txHash {
		t.Fatalf("original tx hash = %s, want %s", originalTx.TxHash, txHash)
	}
	retryTx, err := store.GetOutboxTx(ctx, retryID)
	if err != nil {
		t.Fatalf("GetOutboxTx(retry) error = %v", err)
	}
	if retryTx.Status != TxStatusQueued {
		t.Fatalf("retry status = %q, want %q", retryTx.Status, TxStatusQueued)
	}
	if retryTx.Nonce != 0 {
		t.Fatalf("retry nonce = %d, want unassigned zero value", retryTx.Nonce)
	}
	if retryTx.TxHash != (common.Hash{}) {
		t.Fatalf("retry tx hash = %s, want zero hash", retryTx.TxHash)
	}
	if retryTx.MaxFeePerGas != nil || retryTx.MaxPriorityFeePerGas != nil {
		t.Fatalf("retry fees = %v/%v, want nil", retryTx.MaxFeePerGas, retryTx.MaxPriorityFeePerGas)
	}
	if retryTx.Attempts != 1 {
		t.Fatalf("retry attempts = %d, want 1", retryTx.Attempts)
	}
	if duplicateID, err := store.RetryFailedTx(ctx, id); err == nil {
		t.Fatalf("duplicate RetryFailedTx() id = %d, want error", duplicateID)
	}

	reclaimed, err := store.ClaimOutboxForSigning(ctx, retryID, 40449, signerID, uuid.New(), 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutboxForSigning() after retry error = %v", err)
	}
	if reclaimed.ID != retryID {
		t.Fatalf("reclaimed id = %d, want %d", reclaimed.ID, retryID)
	}
	if reclaimed.Nonce != 43 {
		t.Fatalf("retry nonce = %d, want 43", reclaimed.Nonce)
	}
	retryTx, err = store.GetOutboxTx(ctx, retryID)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if retryTx.Status != TxStatusNonceAssigned {
		t.Fatalf("status = %q, want %q", retryTx.Status, TxStatusNonceAssigned)
	}
	if retryTx.TxHash != (common.Hash{}) {
		t.Fatalf("tx hash = %s, want zero hash", retryTx.TxHash)
	}
	if retryTx.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", retryTx.Attempts)
	}
}

func TestRetryFailedTxRequeuesNoNonceRowInPlace(t *testing.T) {
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

	// Retry semantics are exercised with a packet-scoped purpose: pricing rows
	// carry time-bound observations and are excluded from every retry path.
	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xa2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2")
	packet.Nonce = big.NewInt(9102)
	packet.PayloadHash = common.HexToHash("0xd2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2")
	cleanPacketRows(ctx, t, store, packet.GUID)
	t.Cleanup(func() {
		cleanCtx, cancelClean := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelClean()
		cleanStore, err := Connect(cleanCtx, databaseURL)
		if err != nil {
			t.Fatalf("connect for packet cleanup: %v", err)
		}
		defer cleanStore.Close()
		cleanPacketRows(cleanCtx, t, cleanStore, packet.GUID)
	})
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	const signerID = "0x6666666666666666666666666666666666666666"
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test rows: %v", err)
	}
	id, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  txPurposeExecutorCommitVerification,
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01, 0x02},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	if applied, err := store.MarkQueuedTxEstimateRevertFailed(ctx, id, errors.New("estimate gas reverted")); err != nil || !applied {
		t.Fatalf("MarkQueuedTxEstimateRevertFailed() = (%t, %v), want applied", applied, err)
	}

	retryID, err := store.RetryFailedTx(ctx, id)
	if err != nil {
		t.Fatalf("RetryFailedTx() error = %v", err)
	}
	if retryID != id {
		t.Fatalf("retry id = %d, want original id %d", retryID, id)
	}
	retryTx, err := store.GetOutboxTx(ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if retryTx.Status != TxStatusQueued {
		t.Fatalf("status = %q, want %q", retryTx.Status, TxStatusQueued)
	}
	if retryTx.Nonce != 0 {
		t.Fatalf("nonce = %d, want unassigned zero value", retryTx.Nonce)
	}
	if retryTx.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", retryTx.Attempts)
	}
}

func TestPrepareNextFailedTxRetryStopsAtAttemptCapAndStatsExposeRetryState(t *testing.T) {
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

	const (
		retryStatsChainEID = 49991
		signerID           = "0x5555555555555555555555555555555555555555"
	)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO chains (eid, name, chain_id, endpoint_address, enabled, paused)
		VALUES ($1, $2, $3, $4, true, false)
		ON CONFLICT (eid) DO UPDATE SET enabled = true, paused = false
	`, retryStatsChainEID, "retry-stats-test", int64(49991), common.HexToAddress("0x9999999999999999999999999999999999999999").Bytes()); err != nil {
		t.Fatalf("insert retry stats chain: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE chain_eid = $1", retryStatsChainEID); err != nil {
		t.Fatalf("delete retry stats chain rows: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test rows: %v", err)
	}
	exhaustedID, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: retryStatsChainEID,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(exhausted) error = %v", err)
	}
	retryingID, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: retryStatsChainEID,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x02},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(retrying) error = %v", err)
	}
	parentID, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: retryStatsChainEID,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x03},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(parent) error = %v", err)
	}
	neutralizedID, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: retryStatsChainEID,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x05},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(neutralized) error = %v", err)
	}
	childID, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: retryStatsChainEID,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x04},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx(child) error = %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET status = $1, failure_kind = $2, next_retry_at = now() - interval '1 second', attempts = $3
		WHERE id = $4
	`, TxStatusFailed, TxFailureEstimateGasRevert, TxAutoRetryMaxAttempts, exhaustedID); err != nil {
		t.Fatalf("mark exhausted: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET status = $1, failure_kind = $2, next_retry_at = now() + interval '1 minute', attempts = 1
		WHERE id = $3
	`, TxStatusFailed, TxFailureEstimateGasRevert, retryingID); err != nil {
		t.Fatalf("mark retrying: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET status = $1, failure_kind = $2, next_retry_at = NULL, attempts = 1
		WHERE id = $3
		`, TxStatusFailed, TxFailureReceiptFailed, parentID); err != nil {
		t.Fatalf("mark superseded parent: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
			UPDATE tx_outbox
			SET status = $1, failure_kind = NULL, next_retry_at = NULL, attempts = 1
			WHERE id = $2
		`, TxStatusFailed, neutralizedID); err != nil {
		t.Fatalf("mark neutralized failed row: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE tx_outbox SET retry_of_id = $1 WHERE id = $2", parentID, childID); err != nil {
		t.Fatalf("mark superseded child: %v", err)
	}

	// A legacy pricing failure still holding an unconsumed nonce (upgraded
	// data) must NOT hide as superseded: RetryFailedTx keeps it retryable
	// because the nonce gap can wedge the signer lane, so it falls through to
	// the generic classification and pages as exhausted.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO tx_outbox (chain_eid, purpose, to_address, calldata, value, signer_id, status, failure_kind, nonce, attempts)
		VALUES ($1, $2, $3, $4, 0, $5, $6, 'broadcast_failed', 9001, 1)
	`, retryStatsChainEID, TxPurposePricingSetPriceSnapshot, common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes(), []byte{0x08}, signerID, TxStatusFailed); err != nil {
		t.Fatalf("seed legacy nonce-bearing pricing failure: %v", err)
	}

	// Generic (non-pricing) rows keep the exhausted/retrying buckets; they are
	// seeded raw because stats classification does not consult scope validity.
	for _, seed := range []struct {
		calldata byte
		attempts uint32
		due      string
	}{
		{calldata: 0x06, attempts: TxAutoRetryMaxAttempts, due: "now() - interval '1 second'"},
		{calldata: 0x07, attempts: 1, due: "now() + interval '1 minute'"},
	} {
		if _, err := store.pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO tx_outbox (chain_eid, purpose, to_address, calldata, value, signer_id, status, failure_kind, next_retry_at, attempts)
			VALUES ($1, $2, $3, $4, 0, $5, $6, $7, %s, $8)
		`, seed.due), retryStatsChainEID, txPurposeExecutorCommitVerification, common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes(), []byte{seed.calldata}, signerID, TxStatusFailed, TxFailureEstimateGasRevert, seed.attempts); err != nil {
			t.Fatalf("seed generic failed row: %v", err)
		}
	}

	// Pricing rows never auto-retry, and the generic retrying row is not due.
	if _, err := store.PrepareNextFailedTxRetry(ctx, retryStatsChainEID, signerID); !errors.Is(err, ErrNoFailedTxRetry) {
		t.Fatalf("PrepareNextFailedTxRetry() error = %v, want ErrNoFailedTxRetry", err)
	}
	snapshot, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	counts := make(map[string]uint64)
	for _, stat := range snapshot.TxOutbox {
		if stat.ChainEID == retryStatsChainEID && stat.Status == TxStatusFailed {
			counts[stat.RetryState] += stat.Count
		}
	}
	// The exhausted generic row and the legacy nonce-bearing pricing row page;
	// nonce-less pricing rows are terminal by design and report superseded
	// (the bot rebuilds from fresh observations).
	if counts[TxOutboxRetryStateExhausted] != 2 {
		t.Fatalf("exhausted count = %d, want 2; counts=%v", counts[TxOutboxRetryStateExhausted], counts)
	}
	if counts[TxOutboxRetryStateRetrying] != 1 {
		t.Fatalf("retrying count = %d, want 1; counts=%v", counts[TxOutboxRetryStateRetrying], counts)
	}
	if counts[TxOutboxRetryStateSuperseded] != 4 {
		t.Fatalf("superseded count = %d, want 4; counts=%v", counts[TxOutboxRetryStateSuperseded], counts)
	}
}

func TestUpsertPacketPersistsIndexedPacket(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorAssigned)
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() insert error = %v", err)
	}
	packet.Status = string(packets.ExecutorNew)
	packet.SrcBlockNumber = 124
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() update error = %v", err)
	}

	var status string
	var blockNumber uint64
	if err := store.pool.QueryRow(ctx, "SELECT status, src_block_number FROM packets WHERE guid = $1", packet.GUID.Bytes()).Scan(&status, &blockNumber); err != nil {
		t.Fatalf("select packet: %v", err)
	}
	if status != string(packets.ExecutorAssigned) {
		t.Fatalf("status = %q, want %q", status, packets.ExecutorAssigned)
	}
	if blockNumber != 124 {
		t.Fatalf("src_block_number = %d, want 124", blockNumber)
	}
	byGUID, err := store.GetPacket(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if byGUID.GUID != packet.GUID {
		t.Fatalf("GetPacket() guid = %s, want %s", byGUID.GUID, packet.GUID)
	}
	byDestination, err := store.GetPacketByDestination(ctx, packet.DstEID, packet.SrcEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64())
	if err != nil {
		t.Fatalf("GetPacketByDestination() error = %v", err)
	}
	if byDestination.GUID != packet.GUID {
		t.Fatalf("GetPacketByDestination() guid = %s, want %s", byDestination.GUID, packet.GUID)
	}
}

func TestUpsertExecutorJobPersistsAssignment(t *testing.T) {
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
	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xfeedaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	packet.SrcEID = 50101
	packet.DstEID = 50102
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorAssigned),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}

	var assigned bool
	var fee string
	var status string
	if err := store.pool.QueryRow(ctx, "SELECT assigned, assigned_fee::text, status FROM executor_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&assigned, &fee, &status); err != nil {
		t.Fatalf("select executor job: %v", err)
	}
	if !assigned {
		t.Fatal("assigned = false, want true")
	}
	if fee != "42" {
		t.Fatalf("assigned_fee = %q, want 42", fee)
	}
	if status != string(packets.ExecutorAssigned) {
		t.Fatalf("status = %q, want %q", status, packets.ExecutorAssigned)
	}
	job, err := store.GetExecutorJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob() error = %v", err)
	}
	if job.GUID != packet.GUID {
		t.Fatalf("GetExecutorJob() guid = %s, want %s", job.GUID, packet.GUID)
	}
	if job.AssignedFee == nil || job.AssignedFee.Cmp(big.NewInt(42)) != 0 {
		t.Fatalf("GetExecutorJob() assigned fee = %v, want 42", job.AssignedFee)
	}
	if job.Status != string(packets.ExecutorAssigned) {
		t.Fatalf("GetExecutorJob() status = %q, want %q", job.Status, packets.ExecutorAssigned)
	}
	if err := store.MarkExecutorWaitingDVNVerification(ctx, packet.GUID, string(packets.ExecutorAssigned)); err != nil {
		t.Fatalf("MarkExecutorWaitingDVNVerification() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(99),
		Status:      string(packets.ExecutorAssigned),
		LastError:   "stale index replay",
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() conflict update error = %v", err)
	}
	job, err = store.GetExecutorJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob() after conflict update error = %v", err)
	}
	if job.Status != string(packets.ExecutorWaitingDVNVerification) {
		t.Fatalf("GetExecutorJob() status after conflict update = %q, want %q", job.Status, packets.ExecutorWaitingDVNVerification)
	}
	if job.AssignedFee == nil || job.AssignedFee.Cmp(big.NewInt(99)) != 0 {
		t.Fatalf("GetExecutorJob() assigned fee after conflict update = %v, want 99", job.AssignedFee)
	}
	if job.LastError != "" {
		t.Fatalf("GetExecutorJob() last error after conflict update = %q, want empty", job.LastError)
	}
}

func TestIndexerCursorPersistsMonotonicProgress(t *testing.T) {
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

	const stream = "executor_source"
	if _, err := store.pool.Exec(ctx, "DELETE FROM indexer_cursors WHERE chain_eid = $1 AND stream = $2", 40161, stream); err != nil {
		t.Fatalf("delete cursor: %v", err)
	}
	if err := store.UpdateIndexerCursor(ctx, 40161, stream, 100); err != nil {
		t.Fatalf("UpdateIndexerCursor() insert error = %v", err)
	}
	if err := store.UpdateIndexerCursor(ctx, 40161, stream, 90); err != nil {
		t.Fatalf("UpdateIndexerCursor() lower update error = %v", err)
	}
	cursor, err := store.GetIndexerCursor(ctx, 40161, stream)
	if err != nil {
		t.Fatalf("GetIndexerCursor() error = %v", err)
	}
	if cursor != 100 {
		t.Fatalf("cursor = %d, want 100", cursor)
	}
	if err := store.UpdateIndexerCursor(ctx, 40161, stream, 101); err != nil {
		t.Fatalf("UpdateIndexerCursor() advance error = %v", err)
	}
	cursor, err = store.GetIndexerCursor(ctx, 40161, stream)
	if err != nil {
		t.Fatalf("GetIndexerCursor() after advance error = %v", err)
	}
	if cursor != 101 {
		t.Fatalf("cursor = %d, want 101", cursor)
	}
}

func TestUpsertDVNJobPersistsAssignment(t *testing.T) {
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

	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xfeedbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	packet.SrcEID = 50111
	packet.DstEID = 50112
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		AssignedFee:           big.NewInt(43),
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNAssigned),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	var assignedFee string
	if err := store.pool.QueryRow(ctx, "SELECT assigned_fee::text FROM dvn_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&assignedFee); err != nil {
		t.Fatalf("select dvn assigned fee: %v", err)
	}
	if assignedFee != "43" {
		t.Fatalf("assigned_fee = %q, want 43", assignedFee)
	}
	work, err := store.ListDVNWork(ctx, string(packets.DVNAssigned), 10)
	if err != nil {
		t.Fatalf("ListDVNWork() error = %v", err)
	}
	if len(work) != 1 {
		t.Fatalf("work length = %d, want 1", len(work))
	}
	if work[0].Packet.GUID != packet.GUID {
		t.Fatalf("work packet guid = %s, want %s", work[0].Packet.GUID, packet.GUID)
	}
	if work[0].Job.ConfirmationsRequired != 12 {
		t.Fatalf("confirmations = %d, want 12", work[0].Job.ConfirmationsRequired)
	}
	if work[0].Job.AssignedFee == nil || work[0].Job.AssignedFee.Cmp(big.NewInt(43)) != 0 {
		t.Fatalf("assigned fee = %v, want 43", work[0].Job.AssignedFee)
	}
	if err := store.MarkDVNWaitingConfirmations(ctx, packet.GUID, string(packets.DVNAssigned)); err != nil {
		t.Fatalf("MarkDVNWaitingConfirmations() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		AssignedFee:           big.NewInt(99),
		ConfirmationsRequired: 15,
		Status:                string(packets.DVNAssigned),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() conflict update error = %v", err)
	}
	job, err := store.GetDVNJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetDVNJob() after conflict update error = %v", err)
	}
	if job.Status != string(packets.DVNWaitingConfirmations) {
		t.Fatalf("GetDVNJob() status after conflict update = %q, want %q", job.Status, packets.DVNWaitingConfirmations)
	}
	if job.AssignedFee == nil || job.AssignedFee.Cmp(big.NewInt(99)) != 0 {
		t.Fatalf("GetDVNJob() assigned fee after conflict update = %v, want 99", job.AssignedFee)
	}
	if job.ConfirmationsRequired != 15 {
		t.Fatalf("GetDVNJob() confirmations after conflict update = %d, want 15", job.ConfirmationsRequired)
	}
	if err := store.MarkDVNQuorumChecking(ctx, packet.GUID, string(packets.DVNWaitingConfirmations)); err != nil {
		t.Fatalf("MarkDVNQuorumChecking() error = %v", err)
	}
	work, err = store.ListDVNWork(ctx, string(packets.DVNQuorumChecking), 10)
	if err != nil {
		t.Fatalf("ListDVNWork() quorum error = %v", err)
	}
	if len(work) != 1 {
		t.Fatalf("quorum work length = %d, want 1", len(work))
	}
	report := []byte(`{"status": "would_verify"}`)
	if err := store.MarkDVNReadyToVerify(ctx, packet.GUID, string(packets.DVNQuorumChecking), report); err != nil {
		t.Fatalf("MarkDVNReadyToVerify() error = %v", err)
	}
	work, err = store.ListDVNWork(ctx, string(packets.DVNReadyToVerify), 10)
	if err != nil {
		t.Fatalf("ListDVNWork() ready error = %v", err)
	}
	if len(work) != 1 {
		t.Fatalf("ready work length = %d, want 1", len(work))
	}
	if string(work[0].Job.QuorumResult) != string(report) {
		t.Fatalf("ready quorum result = %s, want %s", work[0].Job.QuorumResult, report)
	}
	if err := store.MarkDVNWouldVerify(ctx, packet.GUID, string(packets.DVNReadyToVerify), report); err != nil {
		t.Fatalf("MarkDVNWouldVerify() error = %v", err)
	}
	var status string
	var quorumResult string
	if err := store.pool.QueryRow(ctx, "SELECT status, quorum_result::text FROM dvn_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&status, &quorumResult); err != nil {
		t.Fatalf("select dvn report: %v", err)
	}
	if status != string(packets.DVNWouldVerify) {
		t.Fatalf("status = %q, want %q", status, packets.DVNWouldVerify)
	}
	if quorumResult == "" {
		t.Fatal("quorum_result is empty")
	}
}

func TestDVNFailureTransitionsAndDefer(t *testing.T) {
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

	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xfeedcccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	packet.SrcEID = 50121
	packet.DstEID = 50122
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		AssignedFee:           big.NewInt(7),
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNQuorumChecking),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	listedDVNWork := func(status string) bool {
		t.Helper()
		work, err := store.ListDVNWork(ctx, status, 50)
		if err != nil {
			t.Fatalf("ListDVNWork(%s) error = %v", status, err)
		}
		for _, item := range work {
			if item.Packet.GUID == packet.GUID {
				return true
			}
		}
		return false
	}
	if !listedDVNWork(string(packets.DVNQuorumChecking)) {
		t.Fatal("fresh quorum-checking job is not offered as work")
	}

	if err := store.DeferDVNJob(ctx, packet.GUID, string(packets.DVNQuorumChecking), 30*time.Minute); err != nil {
		t.Fatalf("DeferDVNJob() error = %v", err)
	}
	if listedDVNWork(string(packets.DVNQuorumChecking)) {
		t.Fatal("deferred job is still offered as work")
	}
	var retryInFuture bool
	if err := store.pool.QueryRow(ctx, "SELECT next_retry_at > now() FROM dvn_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&retryInFuture); err != nil {
		t.Fatalf("select next_retry_at: %v", err)
	}
	if !retryInFuture {
		t.Fatal("DeferDVNJob() did not push next_retry_at into the future")
	}
	if err := store.DeferDVNJob(ctx, packet.GUID, string(packets.DVNAssigned), 30*time.Minute); err == nil {
		t.Fatal("DeferDVNJob() with wrong expected status error = nil, want status mismatch")
	}

	dvnJobState := func() (status, lastError, quorumResult string) {
		t.Helper()
		var lastErrorCol, quorumCol *string
		if err := store.pool.QueryRow(ctx, "SELECT status, last_error, quorum_result::text FROM dvn_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&status, &lastErrorCol, &quorumCol); err != nil {
			t.Fatalf("select dvn job state: %v", err)
		}
		if lastErrorCol != nil {
			lastError = *lastErrorCol
		}
		if quorumCol != nil {
			quorumResult = *quorumCol
		}
		return status, lastError, quorumResult
	}
	resetDVNStatus := func(status string) {
		t.Helper()
		if _, err := store.pool.Exec(ctx, "UPDATE dvn_jobs SET status = $1 WHERE guid = $2", status, packet.GUID.Bytes()); err != nil {
			t.Fatalf("reset dvn status: %v", err)
		}
	}

	conflictReport := []byte(`{"status": "conflict"}`)
	if err := store.MarkDVNQuorumConflict(ctx, packet.GUID, string(packets.DVNQuorumChecking), "providers disagree on receipt", conflictReport); err != nil {
		t.Fatalf("MarkDVNQuorumConflict() error = %v", err)
	}
	status, lastError, quorumResult := dvnJobState()
	if status != string(packets.DVNQuorumConflict) || lastError != "providers disagree on receipt" || quorumResult != string(conflictReport) {
		t.Fatalf("after quorum conflict: status=%q lastError=%q quorum=%q", status, lastError, quorumResult)
	}

	resetDVNStatus(string(packets.DVNQuorumChecking))
	if err := store.MarkDVNReorgDetected(ctx, packet.GUID, string(packets.DVNQuorumChecking), "source receipt disappeared", nil); err != nil {
		t.Fatalf("MarkDVNReorgDetected() error = %v", err)
	}
	status, lastError, quorumResult = dvnJobState()
	if status != string(packets.DVNReorgDetected) || lastError != "source receipt disappeared" {
		t.Fatalf("after reorg: status=%q lastError=%q", status, lastError)
	}
	if quorumResult != string(conflictReport) {
		t.Fatalf("reorg with nil report clobbered quorum_result: %q", quorumResult)
	}

	resetDVNStatus(string(packets.DVNQuorumChecking))
	if err := store.MarkDVNManualReview(ctx, packet.GUID, string(packets.DVNQuorumChecking), "operator canceled verify tx"); err != nil {
		t.Fatalf("MarkDVNManualReview() error = %v", err)
	}
	status, lastError, _ = dvnJobState()
	if status != string(packets.DVNManualReview) || lastError != "operator canceled verify tx" {
		t.Fatalf("after manual review: status=%q lastError=%q", status, lastError)
	}
	var pathwayPaused bool
	if err := store.pool.QueryRow(ctx, "SELECT paused FROM pathways WHERE src_eid = $1 AND dst_eid = $2", packet.SrcEID, packet.DstEID).Scan(&pathwayPaused); err != nil {
		t.Fatalf("select pathway paused: %v", err)
	}
	if pathwayPaused {
		t.Fatal("MarkDVNManualReview() paused the pathway; only MarkDVNManualReviewAndPausePathway may")
	}

	resetDVNStatus(string(packets.DVNReadyToVerify))
	chainReport := []byte(`{"status": "verified_on_chain"}`)
	if err := store.MarkDVNVerifiedFromChain(ctx, packet.GUID, string(packets.DVNReadyToVerify), chainReport); err != nil {
		t.Fatalf("MarkDVNVerifiedFromChain() error = %v", err)
	}
	status, _, quorumResult = dvnJobState()
	if status != string(packets.DVNVerified) || quorumResult != string(chainReport) {
		t.Fatalf("after verified-from-chain: status=%q quorum=%q", status, quorumResult)
	}
	var verifyTxHash []byte
	if err := store.pool.QueryRow(ctx, "SELECT verify_tx_hash FROM dvn_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&verifyTxHash); err != nil {
		t.Fatalf("select verify_tx_hash: %v", err)
	}
	if len(verifyTxHash) != 0 {
		t.Fatalf("verified-from-chain recorded a local verify tx hash: %x", verifyTxHash)
	}
	if err := store.MarkDVNVerifiedFromChain(ctx, packet.GUID, string(packets.DVNReadyToVerify), chainReport); err == nil {
		t.Fatal("MarkDVNVerifiedFromChain() replay error = nil, want status mismatch")
	}
}

func TestReceiptFeeAccountingStats(t *testing.T) {
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

	packet := testPacketRecord()
	// GUID must be unique per test: a shared GUID moves the packet between
	// pathways across runs, breaking the other test's pathway-scoped cleanup.
	packet.GUID = common.HexToHash("0xfeedaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	packet.SrcEID = 50121
	packet.DstEID = 50122
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(100),
		Status:      string(packets.ExecutorAssigned),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	id, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  "executor_lz_receive",
		GUID:     packet.GUID.Bytes(),
		To:       packet.Receiver,
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: "0x9999999999999999999999999999999999999999",
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	// Neutralize any unpriced receipt-cost backlog other integration tests left in
	// the shared database, so the bounded unpriced listing below surfaces this
	// test's row.
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET receipt_gas_cost_src_wei = receipt_gas_cost_dst_wei, receipt_cost_priced_at = now()
		WHERE receipt_gas_cost_dst_wei IS NOT NULL AND receipt_gas_cost_src_wei IS NULL
	`); err != nil {
		t.Fatalf("neutralize unpriced backlog: %v", err)
	}
	// Seed the receipt facts directly: production writes them via MarkAttemptMined,
	// which needs a full durable-attempt flow this stats test does not exercise.
	receiptHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET receipt_tx_hash = $1, receipt_status = 1, receipt_block_number = 1234,
			receipt_gas_used = 21, receipt_effective_gas_price = 5,
			receipt_gas_cost_dst_wei = 105, receipt_observed_at = now(), updated_at = now()
		WHERE id = $2
	`, receiptHash.Bytes(), id); err != nil {
		t.Fatalf("seed receipt facts: %v", err)
	}
	seedConfirmed(ctx, t, store, id, receiptHash)

	tx, err := store.GetOutboxTx(ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if tx.ReceiptGasCostDstWei == nil || tx.ReceiptGasCostDstWei.Cmp(big.NewInt(105)) != 0 {
		t.Fatalf("receipt dst gas cost = %v, want 105", tx.ReceiptGasCostDstWei)
	}
	unpriced, err := store.ListUnpricedWorkerReceiptCosts(ctx, 10)
	if err != nil {
		t.Fatalf("ListUnpricedWorkerReceiptCosts() error = %v", err)
	}
	var unpricedCost *UnpricedWorkerReceiptCost
	for i := range unpriced {
		if unpriced[i].ID == id {
			unpricedCost = &unpriced[i]
			break
		}
	}
	if unpricedCost == nil || unpricedCost.GasCostDstWei.Cmp(big.NewInt(105)) != 0 {
		t.Fatalf("unpriced = %+v, want tx %d cost 105", unpriced, id)
	}
	if err := store.MarkTxReceiptCostPriced(ctx, id, big.NewInt(120)); err != nil {
		t.Fatalf("MarkTxReceiptCostPriced() error = %v", err)
	}

	snapshot, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	var gasStat *TxReceiptGasCostStat
	for i := range snapshot.TxReceiptGasCosts {
		if snapshot.TxReceiptGasCosts[i].ChainEID == packet.DstEID && snapshot.TxReceiptGasCosts[i].Purpose == "executor_lz_receive" {
			gasStat = &snapshot.TxReceiptGasCosts[i]
			break
		}
	}
	if gasStat == nil || gasStat.GasCostDstWei != "105" {
		t.Fatalf("tx receipt gas stats = %+v, want 105", snapshot.TxReceiptGasCosts)
	}
	var feeStat *WorkerFeeStat
	for i := range snapshot.WorkerFees {
		if snapshot.WorkerFees[i].Role == "executor" && snapshot.WorkerFees[i].SrcEID == packet.SrcEID && snapshot.WorkerFees[i].DstEID == packet.DstEID {
			feeStat = &snapshot.WorkerFees[i]
			break
		}
	}
	if feeStat == nil {
		t.Fatalf("worker fee stats = %+v, want executor pathway", snapshot.WorkerFees)
	}
	if feeStat.RevenueSrcWei != "100" || feeStat.ActualGasCostSrcWei != "120" || feeStat.GrossMarginSrcWei != "-20" || feeStat.NegativeMarginJobs != 1 || feeStat.UnpricedReceipts != 0 {
		t.Fatalf("worker fee stat = %+v, want revenue 100 cost 120 margin -20", *feeStat)
	}
}

func TestEnqueueDVNVerifyTxAdvancesJobAtomically(t *testing.T) {
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

	packet := testPacketRecord()
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNQuorumChecking),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	report := []byte(`{"status":"ready"}`)
	if err := store.MarkDVNReadyToVerify(ctx, packet.GUID, string(packets.DVNQuorumChecking), report); err != nil {
		t.Fatalf("MarkDVNReadyToVerify() error = %v", err)
	}

	id, err := store.EnqueueDVNVerifyTx(ctx, packet.GUID, string(packets.DVNReadyToVerify), string(packets.DVNVerifyTxEnqueued), TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  "dvn_verify",
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Calldata: []byte{0x01, 0x02},
		Value:    big.NewInt(0),
		SignerID: "0x9999999999999999999999999999999999999999",
	}, report)
	if err != nil {
		t.Fatalf("EnqueueDVNVerifyTx() error = %v", err)
	}
	if id == 0 {
		t.Fatal("outbox id = 0, want nonzero")
	}
	job, err := store.GetDVNJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetDVNJob() error = %v", err)
	}
	if job.Status != string(packets.DVNVerifyTxEnqueued) {
		t.Fatalf("dvn job status = %q, want %q", job.Status, packets.DVNVerifyTxEnqueued)
	}
	verifyHash := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	if err := store.MarkDVNVerified(ctx, packet.GUID, verifyHash); err != nil {
		t.Fatalf("MarkDVNVerified() error = %v", err)
	}
	var status string
	var hashBytes []byte
	if err := store.pool.QueryRow(ctx, "SELECT status, verify_tx_hash FROM dvn_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&status, &hashBytes); err != nil {
		t.Fatalf("select dvn verify tx: %v", err)
	}
	if status != string(packets.DVNVerified) {
		t.Fatalf("status = %q, want %q", status, packets.DVNVerified)
	}
	if common.BytesToHash(hashBytes) != verifyHash {
		t.Fatalf("verify hash = %s, want %s", common.BytesToHash(hashBytes), verifyHash)
	}
}

func TestGetPacketByVerification(t *testing.T) {
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

	packet := testPacketRecord()
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	found, err := store.GetPacketByVerification(ctx, packet.DstEID, packet.PacketHeader, packet.PayloadHash)
	if err != nil {
		t.Fatalf("GetPacketByVerification() error = %v", err)
	}
	if found.GUID != packet.GUID {
		t.Fatalf("found guid = %s, want %s", found.GUID, packet.GUID)
	}
}

func TestExecutorWorkEnqueueAdvancesStatusAtomically(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorVerifiable)
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE guid = $1", packet.GUID.Bytes()); err != nil {
		t.Fatalf("delete tx_outbox: %v", err)
	}
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorVerifiable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}

	work, err := store.ListExecutorWork(ctx, string(packets.ExecutorVerifiable), 10)
	if err != nil {
		t.Fatalf("ListExecutorWork() error = %v", err)
	}
	if len(work) != 1 {
		t.Fatalf("work len = %d, want 1", len(work))
	}
	if work[0].Packet.GUID != packet.GUID {
		t.Fatalf("work guid = %s, want %s", work[0].Packet.GUID, packet.GUID)
	}

	id, err := store.EnqueueExecutorTx(ctx, packet.GUID, string(packets.ExecutorVerifiable), string(packets.ExecutorCommitTxEnqueued), TxRequest{
		ChainEID: 40449,
		Purpose:  "executor_commit_verification",
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Calldata: []byte{0x01, 0x02},
		Value:    big.NewInt(0),
		SignerID: "0x9999999999999999999999999999999999999999",
	})
	if err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}
	if id == 0 {
		t.Fatal("outbox id = 0, want persisted id")
	}

	var packetStatus, jobStatus, purpose string
	if err := store.pool.QueryRow(ctx, `
		SELECT p.status, ej.status, tx.purpose
		FROM packets p
		JOIN executor_jobs ej ON ej.guid = p.guid
		JOIN tx_outbox tx ON tx.guid = p.guid
		WHERE p.guid = $1
	`, packet.GUID.Bytes()).Scan(&packetStatus, &jobStatus, &purpose); err != nil {
		t.Fatalf("select transitioned rows: %v", err)
	}
	if packetStatus != string(packets.ExecutorCommitTxEnqueued) {
		t.Fatalf("packet status = %q, want %q", packetStatus, packets.ExecutorCommitTxEnqueued)
	}
	if jobStatus != string(packets.ExecutorCommitTxEnqueued) {
		t.Fatalf("job status = %q, want %q", jobStatus, packets.ExecutorCommitTxEnqueued)
	}
	if purpose != "executor_commit_verification" {
		t.Fatalf("purpose = %q, want executor_commit_verification", purpose)
	}
}

func TestExecutorReadinessTransitionsUpdatePacketAndJob(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorAssigned)
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE guid = $1", packet.GUID.Bytes()); err != nil {
		t.Fatalf("delete tx_outbox: %v", err)
	}
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorAssigned),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}

	if err := store.MarkExecutorWaitingDVNVerification(ctx, packet.GUID, string(packets.ExecutorAssigned)); err != nil {
		t.Fatalf("MarkExecutorWaitingDVNVerification() error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorWaitingDVNVerification))

	if err := store.MarkExecutorVerifiable(ctx, packet.GUID, string(packets.ExecutorWaitingDVNVerification)); err != nil {
		t.Fatalf("MarkExecutorVerifiable() error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorVerifiable))
	if err := store.MarkExecutorCommittedFromChain(ctx, packet.GUID, string(packets.ExecutorVerifiable)); err != nil {
		t.Fatalf("MarkExecutorCommittedFromChain() error = %v", err)
	}
	if err := store.MarkExecutorExecutable(ctx, packet.GUID); err != nil {
		t.Fatalf("MarkExecutorExecutable() error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorExecutable))

	const reason = "unsupported executor option type 2"
	if err := store.MarkExecutorManualReview(ctx, packet.GUID, string(packets.ExecutorExecutable), reason); err != nil {
		t.Fatalf("MarkExecutorManualReview() error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorManualReview))
	job, err := store.GetExecutorJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob() error = %v", err)
	}
	if job.LastError != reason {
		t.Fatalf("executor last error = %q, want %q", job.LastError, reason)
	}
}

func TestExecutorReceiptTransitionsPersistTxHashes(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorCommitTxEnqueued)
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE guid = $1", packet.GUID.Bytes()); err != nil {
		t.Fatalf("delete tx_outbox: %v", err)
	}
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorCommitTxEnqueued),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}

	commitHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	if err := store.MarkExecutorCommitted(ctx, packet.GUID, commitHash); err != nil {
		t.Fatalf("MarkExecutorCommitted() error = %v", err)
	}
	if err := store.MarkExecutorExecutable(ctx, packet.GUID); err != nil {
		t.Fatalf("MarkExecutorExecutable() error = %v", err)
	}
	receiveHash := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, receiveHash, "lzReceive reverted"); err == nil {
		t.Fatal("MarkExecutorReceiveFailed() error = nil, want wrong-state error")
	}
	if _, err := store.pool.Exec(ctx, "UPDATE executor_jobs SET status = $1 WHERE guid = $2", string(packets.ExecutorLzReceiveTxEnqueued), packet.GUID.Bytes()); err != nil {
		t.Fatalf("force receive status: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE packets SET status = $1 WHERE guid = $2", string(packets.ExecutorLzReceiveTxEnqueued), packet.GUID.Bytes()); err != nil {
		t.Fatalf("force packet receive status: %v", err)
	}
	if err := store.MarkExecutorDelivered(ctx, packet.GUID, receiveHash); err != nil {
		t.Fatalf("MarkExecutorDelivered() error = %v", err)
	}

	var packetStatus, jobStatus string
	var commitBytes, receiveBytes []byte
	if err := store.pool.QueryRow(ctx, `
		SELECT p.status, ej.status, ej.commit_tx_hash, ej.receive_tx_hash
		FROM packets p
		JOIN executor_jobs ej ON ej.guid = p.guid
		WHERE p.guid = $1
	`, packet.GUID.Bytes()).Scan(&packetStatus, &jobStatus, &commitBytes, &receiveBytes); err != nil {
		t.Fatalf("select receipt rows: %v", err)
	}
	if packetStatus != string(packets.ExecutorDelivered) {
		t.Fatalf("packet status = %q, want %q", packetStatus, packets.ExecutorDelivered)
	}
	if jobStatus != string(packets.ExecutorDelivered) {
		t.Fatalf("job status = %q, want %q", jobStatus, packets.ExecutorDelivered)
	}
	if common.BytesToHash(commitBytes) != commitHash {
		t.Fatalf("commit tx hash = %s, want %s", common.BytesToHash(commitBytes), commitHash)
	}
	if common.BytesToHash(receiveBytes) != receiveHash {
		t.Fatalf("receive tx hash = %s, want %s", common.BytesToHash(receiveBytes), receiveHash)
	}
}

func TestExecutorReceiveFailureCountsEachTxHashOnce(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorLzReceiveTxEnqueued)
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE guid = $1", packet.GUID.Bytes()); err != nil {
		t.Fatalf("delete tx_outbox: %v", err)
	}
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorLzReceiveTxEnqueued),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}

	firstFailedHash := common.HexToHash("0x3131313131313131313131313131313131313131313131313131313131313131")
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, firstFailedHash, "lzReceive reverted"); err != nil {
		t.Fatalf("MarkExecutorReceiveFailed() error = %v", err)
	}
	job, err := store.GetExecutorJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob() error = %v", err)
	}
	if job.RetryCount != 1 {
		t.Fatalf("retry count = %d, want 1", job.RetryCount)
	}
	if job.ReceiveTxHash != firstFailedHash {
		t.Fatalf("receive tx hash = %s, want %s", job.ReceiveTxHash, firstFailedHash)
	}

	// The deliverer re-enqueues the failed job for a fresh attempt; the counted
	// hash stays on the row.
	for _, table := range []string{"executor_jobs", "packets"} {
		if _, err := store.pool.Exec(ctx, "UPDATE "+table+" SET status = $1 WHERE guid = $2", string(packets.ExecutorLzReceiveTxEnqueued), packet.GUID.Bytes()); err != nil {
			t.Fatalf("force %s re-enqueue: %v", table, err)
		}
	}

	// A crash-replayed receipt or lagging LzReceiveAlert for the already
	// counted hash must not charge the retry budget again or fail the new
	// in-flight attempt.
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, firstFailedHash, "lzReceive reverted"); err != nil {
		t.Fatalf("replayed MarkExecutorReceiveFailed() error = %v", err)
	}
	job, err = store.GetExecutorJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob() error = %v", err)
	}
	if job.Status != string(packets.ExecutorLzReceiveTxEnqueued) {
		t.Fatalf("job status after replay = %q, want %q", job.Status, packets.ExecutorLzReceiveTxEnqueued)
	}
	if job.RetryCount != 1 {
		t.Fatalf("retry count after replay = %d, want 1", job.RetryCount)
	}
	var packetStatus string
	if err := store.pool.QueryRow(ctx, "SELECT status FROM packets WHERE guid = $1", packet.GUID.Bytes()).Scan(&packetStatus); err != nil {
		t.Fatalf("select packet status: %v", err)
	}
	if packetStatus != string(packets.ExecutorLzReceiveTxEnqueued) {
		t.Fatalf("packet status after replay = %q, want %q", packetStatus, packets.ExecutorLzReceiveTxEnqueued)
	}

	// The next attempt's own failure carries a new hash and still counts.
	secondFailedHash := common.HexToHash("0x3232323232323232323232323232323232323232323232323232323232323232")
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, secondFailedHash, "lzReceive reverted"); err != nil {
		t.Fatalf("second MarkExecutorReceiveFailed() error = %v", err)
	}
	job, err = store.GetExecutorJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob() error = %v", err)
	}
	if job.Status != string(packets.ExecutorLzReceiveFailed) {
		t.Fatalf("job status = %q, want %q", job.Status, packets.ExecutorLzReceiveFailed)
	}
	if job.RetryCount != 2 {
		t.Fatalf("retry count = %d, want 2", job.RetryCount)
	}
	if job.ReceiveTxHash != secondFailedHash {
		t.Fatalf("receive tx hash = %s, want %s", job.ReceiveTxHash, secondFailedHash)
	}

	// A counted hash is a benign no-op in any state, including already-failed.
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, secondFailedHash, "lzReceive reverted"); err != nil {
		t.Fatalf("failed-state replay MarkExecutorReceiveFailed() error = %v", err)
	}

	// An alert can lag several attempts behind: with a third attempt in flight
	// and the first hash long overwritten on the job row, the stale first-hash
	// alert must still not charge the budget or fail the in-flight attempt.
	for _, table := range []string{"executor_jobs", "packets"} {
		if _, err := store.pool.Exec(ctx, "UPDATE "+table+" SET status = $1 WHERE guid = $2", string(packets.ExecutorLzReceiveTxEnqueued), packet.GUID.Bytes()); err != nil {
			t.Fatalf("force %s re-enqueue: %v", table, err)
		}
	}
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, firstFailedHash, "lzReceive reverted"); err != nil {
		t.Fatalf("stale first-hash MarkExecutorReceiveFailed() error = %v", err)
	}
	job, err = store.GetExecutorJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob() error = %v", err)
	}
	if job.Status != string(packets.ExecutorLzReceiveTxEnqueued) {
		t.Fatalf("job status after stale alert = %q, want %q", job.Status, packets.ExecutorLzReceiveTxEnqueued)
	}
	if job.RetryCount != 2 {
		t.Fatalf("retry count after stale alert = %d, want 2", job.RetryCount)
	}

	// An uncounted hash in the wrong state still fails loudly.
	thirdFailedHash := common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
	if _, err := store.pool.Exec(ctx, "UPDATE executor_jobs SET status = $1 WHERE guid = $2", string(packets.ExecutorDelivered), packet.GUID.Bytes()); err != nil {
		t.Fatalf("force delivered status: %v", err)
	}
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, thirdFailedHash, "lzReceive reverted"); err == nil {
		t.Fatal("MarkExecutorReceiveFailed() error = nil, want wrong-state error")
	}
	job, err = store.GetExecutorJob(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetExecutorJob() error = %v", err)
	}
	if job.RetryCount != 2 {
		t.Fatalf("retry count after rejected mark = %d, want 2 (rolled back)", job.RetryCount)
	}
}

func TestObservedDestinationTransitionsPersistTxHashesFromReplayStatuses(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorAssigned)
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorAssigned),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNAssigned),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}

	commitHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	if err := store.MarkExecutorCommittedObserved(ctx, packet.GUID, commitHash, string(packets.ExecutorAssigned)); err != nil {
		t.Fatalf("MarkExecutorCommittedObserved() error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorCommitted))

	receiveHash := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	if err := store.MarkExecutorDeliveredObserved(ctx, packet.GUID, receiveHash, string(packets.ExecutorCommitted)); err != nil {
		t.Fatalf("MarkExecutorDeliveredObserved() error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorDelivered))

	verifyHash := common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
	if err := store.MarkDVNVerifiedObserved(ctx, packet.GUID, verifyHash, string(packets.DVNAssigned)); err != nil {
		t.Fatalf("MarkDVNVerifiedObserved() error = %v", err)
	}

	var packetStatus, executorStatus, dvnStatus string
	var commitBytes, receiveBytes, verifyBytes []byte
	if err := store.pool.QueryRow(ctx, `
		SELECT p.status, ej.status, dj.status, ej.commit_tx_hash, ej.receive_tx_hash, dj.verify_tx_hash
		FROM packets p
		JOIN executor_jobs ej ON ej.guid = p.guid
		JOIN dvn_jobs dj ON dj.guid = p.guid
		WHERE p.guid = $1
	`, packet.GUID.Bytes()).Scan(&packetStatus, &executorStatus, &dvnStatus, &commitBytes, &receiveBytes, &verifyBytes); err != nil {
		t.Fatalf("select observed rows: %v", err)
	}
	if packetStatus != string(packets.ExecutorDelivered) || executorStatus != string(packets.ExecutorDelivered) {
		t.Fatalf("executor statuses = %q/%q, want %q", packetStatus, executorStatus, packets.ExecutorDelivered)
	}
	if dvnStatus != string(packets.DVNVerified) {
		t.Fatalf("dvn status = %q, want %q", dvnStatus, packets.DVNVerified)
	}
	if common.BytesToHash(commitBytes) != commitHash {
		t.Fatalf("commit tx hash = %s, want %s", common.BytesToHash(commitBytes), commitHash)
	}
	if common.BytesToHash(receiveBytes) != receiveHash {
		t.Fatalf("receive tx hash = %s, want %s", common.BytesToHash(receiveBytes), receiveHash)
	}
	if common.BytesToHash(verifyBytes) != verifyHash {
		t.Fatalf("verify tx hash = %s, want %s", common.BytesToHash(verifyBytes), verifyHash)
	}
}

func TestExecutorDeferAndDeliveredFromChain(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorAssigned)
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorAssigned),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	listedExecutorWork := func(status string) bool {
		t.Helper()
		work, err := store.ListExecutorWork(ctx, status, 50)
		if err != nil {
			t.Fatalf("ListExecutorWork(%s) error = %v", status, err)
		}
		for _, item := range work {
			if item.Packet.GUID == packet.GUID {
				return true
			}
		}
		return false
	}
	if !listedExecutorWork(string(packets.ExecutorAssigned)) {
		t.Fatal("fresh assigned job is not offered as work")
	}

	if err := store.DeferExecutorJob(ctx, packet.GUID, string(packets.ExecutorAssigned), 30*time.Minute); err != nil {
		t.Fatalf("DeferExecutorJob() error = %v", err)
	}
	if listedExecutorWork(string(packets.ExecutorAssigned)) {
		t.Fatal("deferred job is still offered as work")
	}
	var retryInFuture bool
	if err := store.pool.QueryRow(ctx, "SELECT next_retry_at > now() FROM executor_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&retryInFuture); err != nil {
		t.Fatalf("select next_retry_at: %v", err)
	}
	if !retryInFuture {
		t.Fatal("DeferExecutorJob() did not push next_retry_at into the future")
	}
	if err := store.DeferExecutorJob(ctx, packet.GUID, string(packets.ExecutorExecutable), 30*time.Minute); err == nil {
		t.Fatal("DeferExecutorJob() with wrong expected status error = nil, want status mismatch")
	}

	if _, err := store.pool.Exec(ctx, "UPDATE executor_jobs SET status = $1 WHERE guid = $2", string(packets.ExecutorExecutable), packet.GUID.Bytes()); err != nil {
		t.Fatalf("force executable status: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE packets SET status = $1 WHERE guid = $2", string(packets.ExecutorExecutable), packet.GUID.Bytes()); err != nil {
		t.Fatalf("force packet executable status: %v", err)
	}
	if err := store.MarkExecutorDeliveredFromChain(ctx, packet.GUID, string(packets.ExecutorExecutable)); err != nil {
		t.Fatalf("MarkExecutorDeliveredFromChain() error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorDelivered))
	var receiveTxHash []byte
	if err := store.pool.QueryRow(ctx, "SELECT receive_tx_hash FROM executor_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&receiveTxHash); err != nil {
		t.Fatalf("select receive_tx_hash: %v", err)
	}
	if len(receiveTxHash) != 0 {
		t.Fatalf("delivered-from-chain recorded a local receive tx hash: %x", receiveTxHash)
	}
	if err := store.MarkExecutorDeliveredFromChain(ctx, packet.GUID, string(packets.ExecutorExecutable)); err == nil {
		t.Fatal("MarkExecutorDeliveredFromChain() replay error = nil, want status mismatch")
	}
}

func TestExecutorReceiveFailedObservedCountsOncePerHash(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorLzReceiveTxEnqueued)
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorLzReceiveTxEnqueued),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	retryCount := func() int64 {
		t.Helper()
		var count int64
		if err := store.pool.QueryRow(ctx, "SELECT retry_count FROM executor_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&count); err != nil {
			t.Fatalf("select retry_count: %v", err)
		}
		return count
	}

	failedHash := common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444")
	if err := store.MarkExecutorReceiveFailedObserved(ctx, packet.GUID, failedHash, string(packets.ExecutorLzReceiveTxEnqueued), "lzReceive reverted"); err != nil {
		t.Fatalf("MarkExecutorReceiveFailedObserved() error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorLzReceiveFailed))
	if got := retryCount(); got != 1 {
		t.Fatalf("retry_count after first observed failure = %d, want 1", got)
	}

	// A replayed alert for an already-counted hash is a no-op before the status
	// CAS runs: the stale expected status must not surface as an error and the
	// retry budget must not be charged again.
	if err := store.MarkExecutorReceiveFailedObserved(ctx, packet.GUID, failedHash, string(packets.ExecutorLzReceiveTxEnqueued), "lzReceive reverted"); err != nil {
		t.Fatalf("MarkExecutorReceiveFailedObserved() replay error = %v", err)
	}
	assertPacketAndExecutorStatus(ctx, t, store, packet.GUID, string(packets.ExecutorLzReceiveFailed))
	if got := retryCount(); got != 1 {
		t.Fatalf("retry_count after replayed hash = %d, want 1", got)
	}

	secondHash := common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555")
	if err := store.MarkExecutorReceiveFailedObserved(ctx, packet.GUID, secondHash, string(packets.ExecutorLzReceiveFailed), "lzReceive reverted again"); err != nil {
		t.Fatalf("MarkExecutorReceiveFailedObserved() second hash error = %v", err)
	}
	if got := retryCount(); got != 2 {
		t.Fatalf("retry_count after second hash = %d, want 2", got)
	}
	var receiveTxHash []byte
	var lastError string
	if err := store.pool.QueryRow(ctx, "SELECT receive_tx_hash, last_error FROM executor_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&receiveTxHash, &lastError); err != nil {
		t.Fatalf("select failure columns: %v", err)
	}
	if common.BytesToHash(receiveTxHash) != secondHash {
		t.Fatalf("receive_tx_hash = %s, want %s", common.BytesToHash(receiveTxHash), secondHash)
	}
	if lastError != "lzReceive reverted again" {
		t.Fatalf("last_error = %q, want %q", lastError, "lzReceive reverted again")
	}
}

func TestStrictReceiptTransitionsRejectReplayStatuses(t *testing.T) {
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

	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorAssigned)
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorAssigned),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNReadyToVerify),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}

	txHash := common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	if err := store.MarkExecutorCommitted(ctx, packet.GUID, txHash); err == nil {
		t.Fatal("MarkExecutorCommitted() error = nil, want wrong-state error")
	}
	if err := store.MarkExecutorDelivered(ctx, packet.GUID, txHash); err == nil {
		t.Fatal("MarkExecutorDelivered() error = nil, want wrong-state error")
	}
	if err := store.MarkDVNVerified(ctx, packet.GUID, txHash); err == nil {
		t.Fatal("MarkDVNVerified() error = nil, want wrong-state error")
	}
}

func TestCheckDrainStatusReportsPendingWork(t *testing.T) {
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
	packet := testPacketRecord()
	packet.SrcEID = 49161
	packet.DstEID = 49245
	packet.Status = string(packets.ExecutorExecutable)
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNWaitingConfirmations),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	if _, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  "executor_lz_receive",
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Calldata: []byte{0x01, 0x02},
		Value:    big.NewInt(0),
		SignerID: "0x9999999999999999999999999999999999999999",
	}); err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}

	status, err := store.CheckDrainStatus(ctx, packet.SrcEID, packet.DstEID)
	if err != nil {
		t.Fatalf("CheckDrainStatus() error = %v", err)
	}
	if status.Ready {
		t.Fatal("ready = true, want false")
	}
	if status.PacketsTotal != 1 {
		t.Fatalf("packets total = %d, want 1", status.PacketsTotal)
	}
	if got := statusCount(status.ExecutorPending, string(packets.ExecutorExecutable)); got != 1 {
		t.Fatalf("executor pending executable = %d, want 1", got)
	}
	if got := statusCount(status.DVNPending, string(packets.DVNWaitingConfirmations)); got != 1 {
		t.Fatalf("dvn pending waiting confirmations = %d, want 1", got)
	}
	if got := statusCount(status.OutboxPending, TxStatusQueued); got != 1 {
		t.Fatalf("outbox pending queued = %d, want 1", got)
	}
	if status.VerifiedButUndeliveredCount != 1 {
		t.Fatalf("verified but undelivered = %d, want 1", status.VerifiedButUndeliveredCount)
	}
}

func TestCheckDrainStatusAcceptsDeliveredShadowPathway(t *testing.T) {
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
	packet := testPacketRecord()
	packet.SrcEID = 49162
	packet.DstEID = 49246
	packet.Status = string(packets.ExecutorDelivered)
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	cleanPacketRows(ctx, t, store, packet.GUID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorDelivered),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNWouldVerify),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	id, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  "executor_lz_receive",
		GUID:     packet.GUID.Bytes(),
		To:       common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Calldata: []byte{0x01, 0x02},
		Value:    big.NewInt(0),
		SignerID: "0x9999999999999999999999999999999999999999",
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	seedConfirmed(ctx, t, store, id, common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"))

	status, err := store.CheckDrainStatus(ctx, packet.SrcEID, packet.DstEID)
	if err != nil {
		t.Fatalf("CheckDrainStatus() error = %v", err)
	}
	if !status.Ready {
		t.Fatalf("ready = false, status = %+v", status)
	}
	if len(status.ExecutorPending) != 0 || len(status.DVNPending) != 0 || len(status.OutboxPending) != 0 {
		t.Fatalf("pending counts are not empty: %+v", status)
	}
}

func statusCount(counts []StatusCount, status string) int64 {
	for _, count := range counts {
		if count.Status == status {
			return count.Count
		}
	}
	return 0
}

func TestRetryFailedTxParksLzReceiveAtRetryBudget(t *testing.T) {
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

	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xd00dd00dd00dd00dd00dd00dd00dd00dd00dd00dd00dd00dd00dd00dd00dd00d")
	packet.SrcEID = 50241
	packet.DstEID = 50242
	packet.Status = string(packets.ExecutorExecutable)
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}

	const signerID = "0x7777777777777777777777777777777777777777"
	if _, err := store.BootstrapTxNonceCursor(ctx, packet.DstEID, signerID, 3); err != nil {
		t.Fatalf("BootstrapTxNonceCursor() error = %v", err)
	}
	id, err := store.EnqueueExecutorTx(ctx, packet.GUID, string(packets.ExecutorExecutable), string(packets.ExecutorLzReceiveTxEnqueued), TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  txPurposeExecutorLzReceive,
		GUID:     packet.GUID.Bytes(),
		To:       packet.Receiver,
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}
	txHash := common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
	seedBroadcastMirror(ctx, t, store, id, 3, txHash)
	// The lzReceive reverted: mark the job failed (bumps retry_count) and the row receipt-failed.
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, txHash, "reverted"); err != nil {
		t.Fatalf("MarkExecutorReceiveFailed() error = %v", err)
	}
	seedReceiptFailed(ctx, t, store, id)
	// Simulate the budget already being exhausted and the auto-retry falling due.
	if _, err := store.pool.Exec(ctx, "UPDATE executor_jobs SET retry_count = $1 WHERE guid = $2", MaxLzReceiveDeliveryAttempts, packet.GUID.Bytes()); err != nil {
		t.Fatalf("bump retry_count: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE tx_outbox SET next_retry_at = now() - interval '1 minute' WHERE id = $1", id); err != nil {
		t.Fatalf("force retry due: %v", err)
	}

	if _, err := store.RetryFailedTx(ctx, id); !errors.Is(err, ErrNoFailedTxRetry) {
		t.Fatalf("RetryFailedTx() error = %v, want ErrNoFailedTxRetry (must not clone past the budget)", err)
	}
	job, err := store.GetPacket(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if job.Status != string(packets.ExecutorManualReview) {
		t.Fatalf("packet status = %q, want %q", job.Status, packets.ExecutorManualReview)
	}
	var failureKind *string
	var nextRetryAt *time.Time
	if err := store.pool.QueryRow(ctx, "SELECT failure_kind, next_retry_at FROM tx_outbox WHERE id = $1", id).Scan(&failureKind, &nextRetryAt); err != nil {
		t.Fatalf("select finalized row: %v", err)
	}
	if failureKind != nil || nextRetryAt != nil {
		t.Fatalf("failed row not finalized: failure_kind=%v next_retry_at=%v (would wedge the signer)", failureKind, nextRetryAt)
	}
}

func TestRetryFailedTxDoesNotReactivateLzReceiveOnPausedPathway(t *testing.T) {
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

	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xfeed0000feed0000feed0000feed0000feed0000feed0000feed0000feed0000")
	packet.SrcEID = 50251
	packet.DstEID = 50252
	packet.Status = string(packets.ExecutorExecutable)
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}

	const signerID = "0x6666666666666666666666666666666666666666"
	if _, err := store.BootstrapTxNonceCursor(ctx, packet.DstEID, signerID, 3); err != nil {
		t.Fatalf("BootstrapTxNonceCursor() error = %v", err)
	}
	id, err := store.EnqueueExecutorTx(ctx, packet.GUID, string(packets.ExecutorExecutable), string(packets.ExecutorLzReceiveTxEnqueued), TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  txPurposeExecutorLzReceive,
		GUID:     packet.GUID.Bytes(),
		To:       packet.Receiver,
		Calldata: []byte{0x04, 0x05},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueExecutorTx() error = %v", err)
	}
	txHash := common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555")
	seedBroadcastMirror(ctx, t, store, id, 3, txHash)
	if err := store.MarkExecutorReceiveFailed(ctx, packet.GUID, txHash, "reverted"); err != nil {
		t.Fatalf("MarkExecutorReceiveFailed() error = %v", err)
	}
	seedReceiptFailed(ctx, t, store, id)
	// The pathway is paused (retry_count is still well under the budget) and the
	// auto-retry has fallen due.
	if err := store.PausePathwayForPacket(ctx, packet.GUID); err != nil {
		t.Fatalf("PausePathwayForPacket() error = %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE tx_outbox SET next_retry_at = now() - interval '1 minute' WHERE id = $1", id); err != nil {
		t.Fatalf("force retry due: %v", err)
	}

	if _, err := store.RetryFailedTx(ctx, id); !errors.Is(err, ErrNoFailedTxRetry) {
		t.Fatalf("RetryFailedTx() error = %v, want ErrNoFailedTxRetry (must not re-activate a paused pathway)", err)
	}
	job, err := store.GetPacket(ctx, packet.GUID)
	if err != nil {
		t.Fatalf("GetPacket() error = %v", err)
	}
	if job.Status != string(packets.ExecutorLzReceiveFailed) {
		t.Fatalf("packet status = %q, want %q (must stay failed for the deliverer to resume when unpaused)", job.Status, packets.ExecutorLzReceiveFailed)
	}
	var failureKind *string
	var nextRetryAt *time.Time
	if err := store.pool.QueryRow(ctx, "SELECT failure_kind, next_retry_at FROM tx_outbox WHERE id = $1", id).Scan(&failureKind, &nextRetryAt); err != nil {
		t.Fatalf("select finalized row: %v", err)
	}
	if failureKind != nil || nextRetryAt != nil {
		t.Fatalf("failed row not finalized: failure_kind=%v next_retry_at=%v", failureKind, nextRetryAt)
	}
}

func TestListWorkExcludesPausedPathway(t *testing.T) {
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

	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xba5eba11ba5eba11ba5eba11ba5eba11ba5eba11ba5eba11ba5eba11ba5eba11")
	packet.SrcEID = 50231
	packet.DstEID = 50232
	syncDrainPathway(ctx, t, store, packet)
	cleanPathwayRows(ctx, t, store, packet.SrcEID, packet.DstEID)
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{
		GUID:                  packet.GUID,
		AssignedFee:           big.NewInt(43),
		ConfirmationsRequired: 12,
		Status:                string(packets.DVNQuorumChecking),
	}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{
		GUID:        packet.GUID,
		AssignedFee: big.NewInt(42),
		Status:      string(packets.ExecutorExecutable),
	}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}

	hasDVN := func() bool {
		work, err := store.ListDVNWork(ctx, string(packets.DVNQuorumChecking), 100)
		if err != nil {
			t.Fatalf("ListDVNWork() error = %v", err)
		}
		for _, item := range work {
			if item.Packet.GUID == packet.GUID {
				return true
			}
		}
		return false
	}
	hasExecutor := func() bool {
		work, err := store.ListExecutorWork(ctx, string(packets.ExecutorExecutable), 100)
		if err != nil {
			t.Fatalf("ListExecutorWork() error = %v", err)
		}
		for _, item := range work {
			if item.Packet.GUID == packet.GUID {
				return true
			}
		}
		return false
	}

	if !hasDVN() || !hasExecutor() {
		t.Fatal("work not surfaced before pause")
	}
	if err := store.PausePathwayForPacket(ctx, packet.GUID); err != nil {
		t.Fatalf("PausePathwayForPacket() error = %v", err)
	}
	if hasDVN() {
		t.Fatal("ListDVNWork surfaced a paused pathway's work")
	}
	if hasExecutor() {
		t.Fatal("ListExecutorWork surfaced a paused pathway's work")
	}

	// A pathway removed from config (disabled, not paused) must also be excluded.
	if _, err := store.pool.Exec(ctx, "UPDATE pathways SET paused = false, enabled = false WHERE src_eid = $1 AND dst_eid = $2", packet.SrcEID, packet.DstEID); err != nil {
		t.Fatalf("disable pathway: %v", err)
	}
	if hasDVN() || hasExecutor() {
		t.Fatal("work surfaced for a disabled pathway")
	}
}

func TestRequestTxReplacementRejectsConfirmedRow(t *testing.T) {
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

	const signerID = "0x8889888888888888888888888888888888888888"
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test rows: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_nonce_cursors WHERE signer_id = $1", signerID); err != nil {
		t.Fatalf("delete test cursor: %v", err)
	}
	if _, err := store.BootstrapTxNonceCursor(ctx, 40161, signerID, 7); err != nil {
		t.Fatalf("BootstrapTxNonceCursor() error = %v", err)
	}
	id, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: 40161,
		Purpose:  TxPurposePricingSetPriceSnapshot,
		To:       common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Calldata: []byte{0x01},
		Value:    big.NewInt(0),
		SignerID: signerID,
	})
	if err != nil {
		t.Fatalf("EnqueueTx() error = %v", err)
	}
	txHash := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	seedBroadcastMirror(ctx, t, store, id, 7, txHash)
	seedConfirmed(ctx, t, store, id, txHash)

	if err := store.RequestTxReplacement(ctx, id); err == nil {
		t.Fatal("RequestTxReplacement(confirmed) error = nil, want not replaceable")
	}
	confirmed, err := store.GetOutboxTx(ctx, id)
	if err != nil {
		t.Fatalf("GetOutboxTx() error = %v", err)
	}
	if confirmed.Status != TxStatusConfirmed {
		t.Fatalf("status = %q, want %q (row must stay confirmed)", confirmed.Status, TxStatusConfirmed)
	}
	if confirmed.TxHash != txHash {
		t.Fatalf("tx hash = %s, want %s (confirmed hash must not be cleared)", confirmed.TxHash, txHash)
	}
}

func TestSyncConfigDisablesRemovedChainsAndPathways(t *testing.T) {
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

	full, err := chain.NewRegistry(testChains(), testPathways())
	if err != nil {
		t.Fatalf("NewRegistry(full) error = %v", err)
	}
	if err := store.SyncConfig(ctx, full); err != nil {
		t.Fatalf("SyncConfig(full) error = %v", err)
	}

	// Re-sync with the second chain and its pathway removed from config.
	reduced, err := chain.NewRegistry(testChains()[:1], nil)
	if err != nil {
		t.Fatalf("NewRegistry(reduced) error = %v", err)
	}
	if err := store.SyncConfig(ctx, reduced); err != nil {
		t.Fatalf("SyncConfig(reduced) error = %v", err)
	}

	var keptChainEnabled, removedChainEnabled, pathwayEnabled bool
	if err := store.pool.QueryRow(ctx, "SELECT enabled FROM chains WHERE eid = 40161").Scan(&keptChainEnabled); err != nil {
		t.Fatalf("select kept chain: %v", err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT enabled FROM chains WHERE eid = 40449").Scan(&removedChainEnabled); err != nil {
		t.Fatalf("select removed chain: %v", err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT enabled FROM pathways WHERE src_eid = 40161 AND dst_eid = 40449").Scan(&pathwayEnabled); err != nil {
		t.Fatalf("select removed pathway: %v", err)
	}
	if !keptChainEnabled {
		t.Fatal("kept chain 40161 enabled = false, want true")
	}
	if removedChainEnabled {
		t.Fatal("removed chain 40449 enabled = true, want false")
	}
	if pathwayEnabled {
		t.Fatal("removed pathway enabled = true, want false")
	}
}

func testChains() []config.ChainConfig {
	return []config.ChainConfig{
		{
			EID:             40161,
			Name:            "ethereum-sepolia",
			Family:          config.ChainFamilyEVM,
			ChainID:         11155111,
			EndpointAddress: config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
			Confirmations:   12,
			RPCURLs:         []string{"http://localhost:8545"},
			TxRoles: config.ChainTxRolesConfig{
				Executor: testExecutorRole(),
			},
		},
		{
			EID:             40449,
			Name:            "hoodi",
			Family:          config.ChainFamilyEVM,
			ChainID:         560048,
			EndpointAddress: config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
			Confirmations:   12,
			RPCURLs:         []string{"http://localhost:8546"},
			TxRoles: config.ChainTxRolesConfig{
				Executor: testExecutorRole(),
			},
		},
	}
}

func testExecutorRole() config.ExecutorTxRoleConfig {
	return config.ExecutorTxRoleConfig{
		Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func testPacketRecord() PacketRecord {
	return PacketRecord{
		GUID:           common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		SrcEID:         40161,
		DstEID:         40449,
		Nonce:          big.NewInt(7),
		Sender:         common.HexToAddress("0x7777777777777777777777777777777777777777"),
		Receiver:       common.HexToAddress("0x8888888888888888888888888888888888888888"),
		SendLib:        common.HexToAddress("0x9999999999999999999999999999999999999999"),
		SrcTxHash:      common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		SrcBlockNumber: 123,
		SrcLogIndex:    4,
		EncodedPacket:  []byte{0x01, 0x02},
		PacketHeader:   []byte{0x03, 0x04},
		Message:        []byte{0x05, 0x06},
		PayloadHash:    common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
		Options:        []byte{0x07, 0x08},
		Status:         string(packets.ExecutorNew),
	}
}

// seedBroadcastMirror puts a nonce, a submitted attempt, and broadcast status
// onto an outbox row directly in SQL: production reaches this state through the
// durable-attempt flow, which these workflow tests do not exercise.
func seedBroadcastMirror(ctx context.Context, t *testing.T, store *Store, id, nonce int64, txHash common.Hash) {
	t.Helper()
	var attemptID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO tx_attempts (outbox_id, kind, nonce, tx_type, tx_hash, raw_tx, gas_limit,
			max_fee_per_gas, max_priority_fee_per_gas, state, signing_token, broadcast_count)
		VALUES ($1, 'original', $2, 2, $3, '\x01', 100000, 2000000000, 1000000000, 'submitted', gen_random_uuid(), 1)
		RETURNING id
	`, id, nonce, txHash.Bytes()).Scan(&attemptID); err != nil {
		t.Fatalf("seed broadcast attempt: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET nonce = $2, status = 'broadcast', active_attempt_id = $3,
			lease_token = NULL, lease_until = NULL, updated_at = now()
		WHERE id = $1
	`, id, nonce, attemptID); err != nil {
		t.Fatalf("seed broadcast state: %v", err)
	}
}

// seedReceiptFailed marks a row failed with receipt-retry metadata directly in
// SQL; production writes this state via FinalizeAttemptReceipt.
func seedReceiptFailed(ctx context.Context, t *testing.T, store *Store, id int64) {
	t.Helper()
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET status = 'failed', held_reason = NULL, failure_kind = 'receipt_failed',
			next_retry_at = now() + interval '1 minute', last_error = 'receipt reverted', updated_at = now()
		WHERE id = $1
	`, id); err != nil {
		t.Fatalf("seed receipt failed: %v", err)
	}
}

// seedConfirmed marks a row terminal-confirmed directly in SQL; production
// writes this state via FinalizeAttemptReceipt. The winning hash is recorded in
// the receipt facts column (the current hash projects from the active attempt).
func seedConfirmed(ctx context.Context, t *testing.T, store *Store, id int64, txHash common.Hash) {
	t.Helper()
	if _, err := store.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET status = 'confirmed', held_reason = NULL, receipt_tx_hash = $2,
			failure_kind = NULL, next_retry_at = NULL, updated_at = now()
		WHERE id = $1
	`, id, txHash.Bytes()); err != nil {
		t.Fatalf("seed confirmed: %v", err)
	}
}

func cleanPacketRows(ctx context.Context, t *testing.T, store *Store, guid common.Hash) {
	t.Helper()
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE guid = $1", guid.Bytes()); err != nil {
		t.Fatalf("delete tx_outbox: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM dvn_jobs WHERE guid = $1", guid.Bytes()); err != nil {
		t.Fatalf("delete dvn job: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM executor_jobs WHERE guid = $1", guid.Bytes()); err != nil {
		t.Fatalf("delete executor job: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM packets WHERE guid = $1", guid.Bytes()); err != nil {
		t.Fatalf("delete packet: %v", err)
	}
}

func assertPacketAndExecutorStatus(ctx context.Context, t *testing.T, store *Store, guid common.Hash, want string) {
	t.Helper()
	var packetStatus, jobStatus string
	if err := store.pool.QueryRow(ctx, `
		SELECT p.status, ej.status
		FROM packets p
		JOIN executor_jobs ej ON ej.guid = p.guid
		WHERE p.guid = $1
	`, guid.Bytes()).Scan(&packetStatus, &jobStatus); err != nil {
		t.Fatalf("select packet/executor status: %v", err)
	}
	if packetStatus != want {
		t.Fatalf("packet status = %q, want %q", packetStatus, want)
	}
	if jobStatus != want {
		t.Fatalf("executor job status = %q, want %q", jobStatus, want)
	}
}

func syncDrainPathway(ctx context.Context, t *testing.T, store *Store, packet PacketRecord) {
	t.Helper()
	registry, err := chain.NewRegistry(
		[]config.ChainConfig{
			{
				EID:             packet.SrcEID,
				Name:            "drain-source",
				Family:          config.ChainFamilyEVM,
				ChainID:         49161,
				EndpointAddress: config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
				Confirmations:   12,
				RPCURLs:         []string{"http://localhost:8545"},
				TxRoles: config.ChainTxRolesConfig{
					Executor: testExecutorRole(),
				},
			},
			{
				EID:             packet.DstEID,
				Name:            "drain-destination",
				Family:          config.ChainFamilyEVM,
				ChainID:         49245,
				EndpointAddress: config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
				Confirmations:   12,
				RPCURLs:         []string{"http://localhost:8546"},
				TxRoles: config.ChainTxRolesConfig{
					Executor: testExecutorRole(),
				},
			},
		},
		[]config.PathwayConfig{
			{
				SrcEID:     packet.SrcEID,
				DstEID:     packet.DstEID,
				SrcOApp:    config.EVMAddressFromCommon(packet.Sender),
				DstOApp:    config.EVMAddressFromCommon(packet.Receiver),
				SendLib:    config.EVMAddressFromCommon(packet.SendLib),
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
		},
	)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	if err := store.SyncConfig(ctx, registry); err != nil {
		t.Fatalf("SyncConfig() error = %v", err)
	}
	// SyncConfig only manages the enabled flag; clear any paused residue a
	// previous run's pause tests left on this pathway's scope so every test
	// starts from an active scope.
	if _, err := store.pool.Exec(ctx, "UPDATE chains SET paused = false WHERE eid IN ($1, $2)", packet.SrcEID, packet.DstEID); err != nil {
		t.Fatalf("clear chain pause residue: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE pathways SET paused = false WHERE src_eid = $1 AND dst_eid = $2", packet.SrcEID, packet.DstEID); err != nil {
		t.Fatalf("clear pathway pause residue: %v", err)
	}
}

func cleanPathwayRows(ctx context.Context, t *testing.T, store *Store, srcEID, dstEID uint32) {
	t.Helper()
	if _, err := store.pool.Exec(ctx, "DELETE FROM source_packet_skips WHERE src_eid = $1 AND dst_eid = $2", srcEID, dstEID); err != nil {
		t.Fatalf("delete pathway source_packet_skips: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		DELETE FROM tx_outbox
		WHERE guid IN (
			SELECT guid FROM packets WHERE src_eid = $1 AND dst_eid = $2
		)
	`, srcEID, dstEID); err != nil {
		t.Fatalf("delete pathway tx_outbox: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		DELETE FROM dvn_jobs
		WHERE guid IN (
			SELECT guid FROM packets WHERE src_eid = $1 AND dst_eid = $2
		)
	`, srcEID, dstEID); err != nil {
		t.Fatalf("delete pathway dvn_jobs: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		DELETE FROM executor_jobs
		WHERE guid IN (
			SELECT guid FROM packets WHERE src_eid = $1 AND dst_eid = $2
		)
	`, srcEID, dstEID); err != nil {
		t.Fatalf("delete pathway executor_jobs: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "DELETE FROM packets WHERE src_eid = $1 AND dst_eid = $2", srcEID, dstEID); err != nil {
		t.Fatalf("delete pathway packets: %v", err)
	}
}

func testPathways() []config.PathwayConfig {
	return []config.PathwayConfig{
		{
			SrcEID:     40161,
			DstEID:     40449,
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
}

func TestStatsExcludeDisabledScopeHistory(t *testing.T) {
	databaseURL := os.Getenv("TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	// A dedicated chain pair keeps every labeled assertion exclusive to this
	// test; global job-status buckets are asserted as exact deltas instead.
	const srcEID, dstEID = uint32(41000), uint32(41001)
	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0xd15ab1edd15ab1edd15ab1edd15ab1edd15ab1edd15ab1edd15ab1edd15ab1ed")
	packet.SrcEID = srcEID
	packet.DstEID = dstEID
	packet.Status = string(packets.ExecutorManualReview)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		// The test's store is already closed when cleanups run.
		cleanupStore, err := Connect(cleanupCtx, databaseURL)
		if err != nil {
			t.Fatalf("cleanup connect: %v", err)
		}
		defer cleanupStore.Close()
		if _, err := cleanupStore.pool.Exec(cleanupCtx, "DELETE FROM tx_outbox WHERE chain_eid IN (41000, 41001)"); err != nil {
			t.Fatalf("cleanup tx_outbox: %v", err)
		}
		for _, table := range []string{"executor_jobs", "dvn_jobs", "packets"} {
			if _, err := cleanupStore.pool.Exec(cleanupCtx, "DELETE FROM "+table+" WHERE guid = $1", packet.GUID.Bytes()); err != nil {
				t.Fatalf("cleanup %s: %v", table, err)
			}
		}
		if _, err := cleanupStore.pool.Exec(cleanupCtx, "DELETE FROM pathways WHERE src_eid = 41000"); err != nil {
			t.Fatalf("cleanup pathways: %v", err)
		}
		if _, err := cleanupStore.pool.Exec(cleanupCtx, "DELETE FROM chains WHERE eid IN (41000, 41001)"); err != nil {
			t.Fatalf("cleanup chains: %v", err)
		}
	})
	for _, eid := range []uint32{srcEID, dstEID} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO chains (eid, name, chain_id, endpoint_address, enabled)
			VALUES ($1, 'stats-scope-test', $2, '\x0000000000000000000000000000000000000001', true)
			ON CONFLICT (eid) DO UPDATE SET enabled = true, paused = false
		`, eid, int64(eid)); err != nil {
			t.Fatalf("seed chain %d: %v", eid, err)
		}
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO pathways (src_eid, dst_eid, src_oapp, dst_oapp, send_lib, receive_lib, open_executor, open_dvn, price_feed, destination_open_dvn, max_message_size, enabled)
		VALUES ($1, $2, $3, $4, $5, $5, $5, $5, $5, $5, 1024, true)
		ON CONFLICT (src_eid, dst_eid, src_oapp, dst_oapp) DO UPDATE SET enabled = true, paused = false
	`, srcEID, dstEID, packet.Sender.Bytes(), packet.Receiver.Bytes(), packet.SendLib.Bytes()); err != nil {
		t.Fatalf("seed pathway: %v", err)
	}
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertExecutorJob(ctx, ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorManualReview)}); err != nil {
		t.Fatalf("UpsertExecutorJob() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{GUID: packet.GUID, ConfirmationsRequired: 12, Status: string(packets.DVNManualReview)}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}
	if _, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: dstEID, Purpose: "executor_lz_receive", GUID: packet.GUID.Bytes(),
		To: common.HexToAddress("0x1234123412341234123412341234123412341234"), SignerID: "0xstats", Calldata: []byte{0x01},
	}); err != nil {
		t.Fatalf("EnqueueTx(packet) error = %v", err)
	}
	if _, err := store.EnqueueTx(ctx, TxRequest{
		ChainEID: srcEID, Purpose: "pricing_set_price_snapshot",
		To: common.HexToAddress("0x1234123412341234123412341234123412341234"), SignerID: "0xstats", Calldata: []byte{0x01},
	}); err != nil {
		t.Fatalf("EnqueueTx(pricing) error = %v", err)
	}

	statusCount := func(stats []StatusStat, status string) uint64 {
		for _, stat := range stats {
			if stat.Status == status {
				return stat.Count
			}
		}
		return 0
	}
	packetVisible := func(snapshot StatsSnapshot) bool {
		for _, stat := range snapshot.Packets {
			if stat.SrcEID == srcEID && stat.DstEID == dstEID {
				return true
			}
		}
		return false
	}
	outboxVisible := func(snapshot StatsSnapshot, chainEID uint32) bool {
		for _, stat := range snapshot.TxOutbox {
			if stat.ChainEID == chainEID {
				return true
			}
		}
		return false
	}

	baseline, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if !packetVisible(baseline) || !outboxVisible(baseline, dstEID) || !outboxVisible(baseline, srcEID) {
		t.Fatalf("baseline stats missing seeded rows: packets=%v outbox_dst=%v outbox_src=%v", packetVisible(baseline), outboxVisible(baseline, dstEID), outboxVisible(baseline, srcEID))
	}
	baseExecutor := statusCount(baseline.ExecutorJobs, string(packets.ExecutorManualReview))
	baseDVN := statusCount(baseline.DVNJobs, string(packets.DVNManualReview))
	if baseExecutor == 0 || baseDVN == 0 {
		t.Fatalf("baseline job stats missing seeded rows: executor=%d dvn=%d", baseExecutor, baseDVN)
	}

	// Removing the pathway hides its packet, job, and packet-linked outbox
	// history; the chain-scoped pricing row stays.
	if _, err := store.pool.Exec(ctx, "UPDATE pathways SET enabled = false WHERE src_eid = $1 AND dst_eid = $2", srcEID, dstEID); err != nil {
		t.Fatalf("disable pathway: %v", err)
	}
	disabledPathway, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if packetVisible(disabledPathway) {
		t.Fatal("packets of a disabled pathway still reported")
	}
	if outboxVisible(disabledPathway, dstEID) {
		t.Fatal("packet-linked outbox rows of a disabled pathway still reported")
	}
	if !outboxVisible(disabledPathway, srcEID) {
		t.Fatal("pricing outbox row disappeared with an unrelated pathway disable")
	}
	if got := statusCount(disabledPathway.ExecutorJobs, string(packets.ExecutorManualReview)); got != baseExecutor-1 {
		t.Fatalf("executor MANUAL_REVIEW count = %d, want %d after pathway disable", got, baseExecutor-1)
	}
	if got := statusCount(disabledPathway.DVNJobs, string(packets.DVNManualReview)); got != baseDVN-1 {
		t.Fatalf("dvn MANUAL_REVIEW count = %d, want %d after pathway disable", got, baseDVN-1)
	}

	// Removing a chain hides everything scoped to it, including the pricing row.
	if _, err := store.pool.Exec(ctx, "UPDATE pathways SET enabled = true WHERE src_eid = $1 AND dst_eid = $2", srcEID, dstEID); err != nil {
		t.Fatalf("re-enable pathway: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE chains SET enabled = false WHERE eid = $1", srcEID); err != nil {
		t.Fatalf("disable chain: %v", err)
	}
	disabledChain, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if packetVisible(disabledChain) || outboxVisible(disabledChain, dstEID) || outboxVisible(disabledChain, srcEID) {
		t.Fatal("rows scoped to a disabled chain still reported")
	}

	// Re-enabling restores visibility: the durable rows were never touched.
	if _, err := store.pool.Exec(ctx, "UPDATE chains SET enabled = true WHERE eid = $1", srcEID); err != nil {
		t.Fatalf("re-enable chain: %v", err)
	}
	restored, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if !packetVisible(restored) || !outboxVisible(restored, dstEID) || !outboxVisible(restored, srcEID) {
		t.Fatal("re-enabled scope did not restore stats visibility")
	}
	if got := statusCount(restored.ExecutorJobs, string(packets.ExecutorManualReview)); got != baseExecutor {
		t.Fatalf("executor MANUAL_REVIEW count = %d, want restored %d", got, baseExecutor)
	}
}

func TestMarkDVNReorgDetectedWithCoordinatesIsAtomic(t *testing.T) {
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

	packet := testPacketRecord()
	packet.GUID = common.HexToHash("0x4e094e094e094e094e094e094e094e094e094e094e094e094e094e094e094e09")
	packet.Nonce = big.NewInt(4909)
	packet.Status = string(packets.DVNQuorumChecking)
	if _, err := store.pool.Exec(ctx, "DELETE FROM tx_outbox WHERE guid = $1", packet.GUID.Bytes()); err != nil {
		t.Fatalf("delete tx_outbox: %v", err)
	}
	cleanPacketRows(ctx, t, store, packet.GUID)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupStore, err := Connect(cleanupCtx, databaseURL)
		if err != nil {
			t.Fatalf("cleanup connect: %v", err)
		}
		defer cleanupStore.Close()
		for _, table := range []string{"dvn_jobs", "packets"} {
			if _, err := cleanupStore.pool.Exec(cleanupCtx, "DELETE FROM "+table+" WHERE guid = $1", packet.GUID.Bytes()); err != nil {
				t.Fatalf("cleanup %s: %v", table, err)
			}
		}
	})
	if err := store.UpsertPacket(ctx, packet); err != nil {
		t.Fatalf("UpsertPacket() error = %v", err)
	}
	if err := store.UpsertDVNJob(ctx, DVNJobRecord{GUID: packet.GUID, ConfirmationsRequired: 12, Status: string(packets.DVNQuorumChecking)}); err != nil {
		t.Fatalf("UpsertDVNJob() error = %v", err)
	}

	// Wrong expected status: nothing changes.
	if err := store.MarkDVNReorgDetectedWithCoordinates(ctx, packet.GUID, string(packets.DVNWaitingConfirmations), "reorg", []byte(`{}`), packet.SrcBlockNumber+3, packet.SrcLogIndex+5, packet.SrcBlockNumber, packet.SrcLogIndex); err == nil {
		t.Fatal("MarkDVNReorgDetectedWithCoordinates(wrong status) error = nil, want CAS rejection")
	}
	var blockNumber uint64
	var logIndex uint
	if err := store.pool.QueryRow(ctx, "SELECT src_block_number, src_log_index FROM packets WHERE guid = $1", packet.GUID.Bytes()).Scan(&blockNumber, &logIndex); err != nil {
		t.Fatalf("select coords: %v", err)
	}
	if blockNumber != packet.SrcBlockNumber || logIndex != packet.SrcLogIndex {
		t.Fatalf("coords = %d/%d after rejected CAS, want untouched %d/%d", blockNumber, logIndex, packet.SrcBlockNumber, packet.SrcLogIndex)
	}

	// Matching CAS: transition and coordinates land together.
	if err := store.MarkDVNReorgDetectedWithCoordinates(ctx, packet.GUID, string(packets.DVNQuorumChecking), "reorg", []byte(`{}`), packet.SrcBlockNumber+3, packet.SrcLogIndex+5, packet.SrcBlockNumber, packet.SrcLogIndex); err != nil {
		t.Fatalf("MarkDVNReorgDetectedWithCoordinates() error = %v", err)
	}
	var jobStatus string
	if err := store.pool.QueryRow(ctx, "SELECT status FROM dvn_jobs WHERE guid = $1", packet.GUID.Bytes()).Scan(&jobStatus); err != nil {
		t.Fatalf("select job status: %v", err)
	}
	if jobStatus != string(packets.DVNReorgDetected) {
		t.Fatalf("job status = %q, want REORG_DETECTED", jobStatus)
	}
	if err := store.pool.QueryRow(ctx, "SELECT src_block_number, src_log_index FROM packets WHERE guid = $1", packet.GUID.Bytes()).Scan(&blockNumber, &logIndex); err != nil {
		t.Fatalf("select refreshed coords: %v", err)
	}
	if blockNumber != packet.SrcBlockNumber+3 || logIndex != packet.SrcLogIndex+5 {
		t.Fatalf("coords = %d/%d, want refreshed %d/%d", blockNumber, logIndex, packet.SrcBlockNumber+3, packet.SrcLogIndex+5)
	}

	// A stale coordinate expectation (someone else refreshed first) still
	// transitions but never clobbers the newer coordinates.
	if _, err := store.pool.Exec(ctx, "UPDATE dvn_jobs SET status = $1 WHERE guid = $2", string(packets.DVNQuorumChecking), packet.GUID.Bytes()); err != nil {
		t.Fatalf("reset job status: %v", err)
	}
	if err := store.MarkDVNReorgDetectedWithCoordinates(ctx, packet.GUID, string(packets.DVNQuorumChecking), "reorg", []byte(`{}`), packet.SrcBlockNumber+9, packet.SrcLogIndex+9, packet.SrcBlockNumber, packet.SrcLogIndex); err != nil {
		t.Fatalf("MarkDVNReorgDetectedWithCoordinates(stale expectation) error = %v", err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT src_block_number, src_log_index FROM packets WHERE guid = $1", packet.GUID.Bytes()).Scan(&blockNumber, &logIndex); err != nil {
		t.Fatalf("select final coords: %v", err)
	}
	if blockNumber != packet.SrcBlockNumber+3 || logIndex != packet.SrcLogIndex+5 {
		t.Fatalf("coords = %d/%d, want the earlier refresh preserved", blockNumber, logIndex)
	}
}
