package indexer

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/lzabi"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/jackc/pgx/v5"
)

func TestApplyExecutorDestinationLogMarksCommittedFromReplayStatuses(t *testing.T) {
	for _, status := range []packets.ExecutorState{
		packets.ExecutorAssigned,
		packets.ExecutorWaitingDVNVerification,
		packets.ExecutorVerifiable,
		packets.ExecutorCommitTxEnqueued,
	} {
		t.Run(string(status), func(t *testing.T) {
			packet := testDestinationPacketRecord()
			store := &fakeReceiptStore{}
			log := testPacketVerifiedLog(t, packet)
			job := db.ExecutorJobRecord{GUID: packet.GUID, Status: string(status)}

			applied, err := ApplyExecutorDestinationLog(context.Background(), store, packet, job, log)
			if err != nil {
				t.Fatalf("ApplyExecutorDestinationLog() error = %v", err)
			}
			if !applied {
				t.Fatal("applied = false, want true")
			}
			if store.committedGUID != packet.GUID {
				t.Fatalf("committed guid = %s, want %s", store.committedGUID, packet.GUID)
			}
			if store.committedTxHash != log.TxHash {
				t.Fatalf("committed tx = %s, want %s", store.committedTxHash, log.TxHash)
			}
			if store.expectedStatus != string(status) {
				t.Fatalf("expected status = %q, want %q", store.expectedStatus, status)
			}
		})
	}
}

func TestApplyExecutorDestinationLogMarksDelivered(t *testing.T) {
	packet := testDestinationPacketRecord()
	packet.Status = "LZ_RECEIVE_TX_ENQUEUED"
	store := &fakeReceiptStore{}
	log := testPacketDeliveredLog(t, packet)
	job := db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorAssigned)}

	applied, err := ApplyExecutorDestinationLog(context.Background(), store, packet, job, log)
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLog() error = %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	if store.deliveredGUID != packet.GUID {
		t.Fatalf("delivered guid = %s, want %s", store.deliveredGUID, packet.GUID)
	}
	if store.deliveredTxHash != log.TxHash {
		t.Fatalf("delivered tx = %s, want %s", store.deliveredTxHash, log.TxHash)
	}
}

func TestApplyExecutorDestinationLogMarksReceiveFailed(t *testing.T) {
	packet := testDestinationPacketRecord()
	packet.Status = "LZ_RECEIVE_TX_ENQUEUED"
	store := &fakeReceiptStore{}
	log := testLzReceiveAlertLog(t, packet, []byte{0xde, 0xad})
	job := db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorCommitted)}

	applied, err := ApplyExecutorDestinationLog(context.Background(), store, packet, job, log)
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLog() error = %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	if store.failedGUID != packet.GUID {
		t.Fatalf("failed guid = %s, want %s", store.failedGUID, packet.GUID)
	}
	if store.failedTxHash != log.TxHash {
		t.Fatalf("failed tx = %s, want %s", store.failedTxHash, log.TxHash)
	}
	if store.failedReason != "dead" {
		t.Fatalf("failed reason = %q, want dead", store.failedReason)
	}
}

func TestApplyExecutorDestinationLogRejectsMismatchedPacket(t *testing.T) {
	packet := testDestinationPacketRecord()
	log := testPacketVerifiedLog(t, packet)
	packet.PayloadHash = common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
	job := db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorCommitTxEnqueued)}

	applied, err := ApplyExecutorDestinationLog(context.Background(), &fakeReceiptStore{}, packet, job, log)
	if err == nil {
		t.Fatal("ApplyExecutorDestinationLog() error = nil, want mismatch")
	}
	if applied {
		t.Fatal("applied = true, want false on mismatch")
	}
}

func TestApplyExecutorDestinationLogsLooksUpAndAppliesKnownEvents(t *testing.T) {
	packet := testDestinationPacketRecord()
	verifiedLog := testPacketVerifiedLog(t, packet)
	alertLog := testLzReceiveAlertLog(t, packet, []byte{0xde, 0xad})
	store := &fakeDestinationStore{
		byGUID: map[common.Hash]db.PacketRecord{
			packet.GUID: packet,
		},
		byDestination: map[string]db.PacketRecord{
			destinationLookupKey(packet.DstEID, packet.SrcEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): packet,
		},
		executorJobs: map[common.Hash]db.ExecutorJobRecord{
			packet.GUID: {
				GUID:   packet.GUID,
				Status: string(packets.ExecutorCommitTxEnqueued),
			},
		},
	}

	applied, err := ApplyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []gethtypes.Log{
		{Topics: []common.Hash{common.HexToHash("0x01")}},
		verifiedLog,
		alertLog,
	})
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLogs() error = %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	if store.committedGUID != packet.GUID {
		t.Fatalf("committed guid = %s, want %s", store.committedGUID, packet.GUID)
	}
	if store.failedGUID != packet.GUID {
		t.Fatalf("failed guid = %s, want %s", store.failedGUID, packet.GUID)
	}
}

func TestApplyExecutorDestinationLogsSkipsPacketsWithoutExecutorJob(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		byDestination: map[string]db.PacketRecord{
			destinationLookupKey(packet.DstEID, packet.SrcEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): packet,
		},
	}

	applied, err := ApplyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	})
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLogs() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0", applied)
	}
	if store.committedGUID != (common.Hash{}) {
		t.Fatalf("committed guid = %s, want zero", store.committedGUID)
	}
}

func TestApplyExecutorDestinationLogsDefersConfiguredMissingSourceStateWithoutSourceSkip(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{}

	result, err := applyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, common.Address{}, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if !result.pending {
		t.Fatal("pending = false, want true")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyExecutorDestinationLogsSkipsConfiguredMissingSourceStateWithSourceSkip(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		sourceSkips: map[string]db.SourcePacketSkip{
			sourceSkipLookupKey(sourceRoleExecutor, packet.SrcEID, packet.DstEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): {
				Role:     sourceRoleExecutor,
				SrcEID:   packet.SrcEID,
				DstEID:   packet.DstEID,
				Nonce:    packet.Nonce.Uint64(),
				Sender:   packet.Sender,
				Receiver: packet.Receiver,
				GUID:     packet.GUID,
				Reason:   "unexpected_worker",
			},
		},
	}

	result, err := applyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, common.Address{}, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyExecutorDestinationLogsDefersConfiguredMissingExecutorJob(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		byDestination: map[string]db.PacketRecord{
			destinationLookupKey(packet.DstEID, packet.SrcEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): packet,
		},
	}

	result, err := applyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, common.Address{}, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if !result.pending {
		t.Fatal("pending = false, want true")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyExecutorDestinationLogsSkipsMissingExecutorJobWithSourceSkip(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		byDestination: map[string]db.PacketRecord{
			destinationLookupKey(packet.DstEID, packet.SrcEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): packet,
		},
		sourceSkips: map[string]db.SourcePacketSkip{
			sourceSkipLookupKey(sourceRoleExecutor, packet.SrcEID, packet.DstEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): {
				Role:     sourceRoleExecutor,
				SrcEID:   packet.SrcEID,
				DstEID:   packet.DstEID,
				Nonce:    packet.Nonce.Uint64(),
				Sender:   packet.Sender,
				Receiver: packet.Receiver,
				GUID:     packet.GUID,
				Reason:   "unexpected_worker",
			},
		},
	}

	result, err := applyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, common.Address{}, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyExecutorDestinationLogsSkipsExternalMissingPackets(t *testing.T) {
	packet := testDestinationPacketRecord()
	packet.Sender = common.HexToAddress("0x1212121212121212121212121212121212121212")
	store := &fakeDestinationStore{}

	result, err := applyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, common.Address{}, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyExecutorDestinationLogsSkipsDisabledMissingSourceState(t *testing.T) {
	packet := testDestinationPacketRecord()
	pathway := testIndexerPathway()
	pathway.Enabled = false
	store := &fakeDestinationStore{}

	result, err := applyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{pathway}, common.Address{}, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyExecutorDestinationLogsSkipsDisabledMissingExecutorJob(t *testing.T) {
	packet := testDestinationPacketRecord()
	pathway := testIndexerPathway()
	pathway.Enabled = false
	store := &fakeDestinationStore{
		byDestination: map[string]db.PacketRecord{
			destinationLookupKey(packet.DstEID, packet.SrcEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): packet,
		},
	}

	result, err := applyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{pathway}, common.Address{}, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyExecutorDestinationLogsSkipsAlreadyAppliedEvents(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		byDestination: map[string]db.PacketRecord{
			destinationLookupKey(packet.DstEID, packet.SrcEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): packet,
		},
		executorJobs: map[common.Hash]db.ExecutorJobRecord{
			packet.GUID: {
				GUID:   packet.GUID,
				Status: string(packets.ExecutorDelivered),
			},
		},
	}

	applied, err := ApplyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
		testPacketDeliveredLog(t, packet),
	})
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLogs() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0", applied)
	}
	if store.committedGUID != (common.Hash{}) {
		t.Fatalf("committed guid = %s, want zero", store.committedGUID)
	}
	if store.deliveredGUID != (common.Hash{}) {
		t.Fatalf("delivered guid = %s, want zero", store.deliveredGUID)
	}
}

func TestApplyExecutorDestinationLogsKeepsManualReviewParked(t *testing.T) {
	// A lagging indexer replaying an old PacketVerified event must not un-park
	// a MANUAL_REVIEW job: that would bypass the delivery retry cap and could
	// deliver a message the operator explicitly canceled. Parked keeps parked,
	// and the historical event must not error out and wedge the cursor.
	packet := testDestinationPacketRecord()
	log := testPacketVerifiedLog(t, packet)
	store := &fakeDestinationStore{
		byDestination: map[string]db.PacketRecord{
			destinationLookupKey(packet.DstEID, packet.SrcEID, packet.Sender, packet.Receiver, packet.Nonce.Uint64()): packet,
		},
		executorJobs: map[common.Hash]db.ExecutorJobRecord{
			packet.GUID: {GUID: packet.GUID, Status: string(packets.ExecutorManualReview)},
		},
	}

	applied, err := ApplyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []gethtypes.Log{log})
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLogs() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0 (parked keeps parked)", applied)
	}
	if store.committedGUID != (common.Hash{}) {
		t.Fatalf("committed guid = %s, want none", store.committedGUID)
	}
}

func TestApplyExecutorDestinationLogsDoesNotLetAlertOverrideDelivered(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		byGUID: map[common.Hash]db.PacketRecord{
			packet.GUID: packet,
		},
		executorJobs: map[common.Hash]db.ExecutorJobRecord{
			packet.GUID: {
				GUID:   packet.GUID,
				Status: string(packets.ExecutorDelivered),
			},
		},
	}

	applied, err := ApplyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []gethtypes.Log{
		testLzReceiveAlertLog(t, packet, []byte{0xde, 0xad}),
	})
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLogs() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0", applied)
	}
	if store.failedGUID != (common.Hash{}) {
		t.Fatalf("failed guid = %s, want zero", store.failedGUID)
	}
}

func TestApplyExecutorDestinationLogsSkipsAlertForManualReviewJob(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		byGUID: map[common.Hash]db.PacketRecord{
			packet.GUID: packet,
		},
		executorJobs: map[common.Hash]db.ExecutorJobRecord{
			packet.GUID: {
				GUID:   packet.GUID,
				Status: string(packets.ExecutorManualReview),
			},
		},
	}

	// A historical alert for a parked job must skip, not error: an error here
	// would wedge the destination cursor behind the un-appliable log forever.
	applied, err := ApplyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []gethtypes.Log{
		testLzReceiveAlertLog(t, packet, []byte{0xde, 0xad}),
	})
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLogs() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0", applied)
	}
	if store.failedGUID != (common.Hash{}) {
		t.Fatalf("failed guid = %s, want zero", store.failedGUID)
	}
}

func TestApplyExecutorDestinationLogsSkipsUnexpectedAlertExecutor(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		byGUID: map[common.Hash]db.PacketRecord{
			packet.GUID: packet,
		},
		executorJobs: map[common.Hash]db.ExecutorJobRecord{
			packet.GUID: {
				GUID:   packet.GUID,
				Status: string(packets.ExecutorLzReceiveTxEnqueued),
			},
		},
	}

	result, err := applyExecutorDestinationLogs(
		context.Background(),
		store,
		packet.DstEID,
		[]chain.Pathway{testIndexerPathway()},
		common.HexToAddress("0x8888888888888888888888888888888888888888"),
		[]gethtypes.Log{
			testLzReceiveAlertLogWithExecutor(t, packet, common.HexToAddress("0x9999999999999999999999999999999999999999"), []byte{0xde, 0xad}),
		},
		destinationLogObserver{},
	)
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
	if store.failedGUID != (common.Hash{}) {
		t.Fatalf("failed guid = %s, want zero", store.failedGUID)
	}
}

func TestApplyExecutorDestinationLogsSkipsUnexpectedAlertExecutorForMissingPacket(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{}

	result, err := applyExecutorDestinationLogs(
		context.Background(),
		store,
		packet.DstEID,
		[]chain.Pathway{testIndexerPathway()},
		common.HexToAddress("0x8888888888888888888888888888888888888888"),
		[]gethtypes.Log{
			testLzReceiveAlertLogWithExecutor(t, packet, common.HexToAddress("0x9999999999999999999999999999999999999999"), []byte{0xde, 0xad}),
		},
		destinationLogObserver{},
	)
	if err != nil {
		t.Fatalf("applyExecutorDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyExecutorDestinationLogsSkipsUnknownPackets(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{}

	applied, err := ApplyExecutorDestinationLogs(context.Background(), store, packet.DstEID, []gethtypes.Log{
		testPacketVerifiedLog(t, packet),
	})
	if err != nil {
		t.Fatalf("ApplyExecutorDestinationLogs() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0", applied)
	}
}

func TestApplyDVNDestinationLogsDefersConfiguredMissingSourcePacket(t *testing.T) {
	packet := testDestinationPacketRecord()
	packet.PacketHeader = testEncodedPacket()[:81]
	log := testPayloadVerifiedLog(t, packet, common.HexToAddress("0x6666666666666666666666666666666666666666"))
	store := &fakeDestinationStore{}

	result, err := applyDVNDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, []gethtypes.Log{log}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyDVNDestinationLogs() error = %v", err)
	}
	if !result.pending {
		t.Fatal("pending = false, want true")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyDVNDestinationLogsSkipsDisabledMissingSourcePacket(t *testing.T) {
	packet := testDestinationPacketRecord()
	packet.PacketHeader = testEncodedPacket()[:81]
	log := testPayloadVerifiedLog(t, packet, common.HexToAddress("0x6666666666666666666666666666666666666666"))
	pathway := testIndexerPathway()
	pathway.Enabled = false
	store := &fakeDestinationStore{}

	result, err := applyDVNDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{pathway}, []gethtypes.Log{log}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyDVNDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyDVNDestinationLogsSkipsDisabledMissingDVNJob(t *testing.T) {
	packet := testDestinationPacketRecord()
	pathway := testIndexerPathway()
	pathway.Enabled = false
	log := testPayloadVerifiedLog(t, packet, pathway.DestinationWorkers.OpenDVN)
	store := &fakeDestinationStore{
		byVerification: map[string]db.PacketRecord{
			verificationLookupKey(packet.DstEID, packet.PacketHeader, packet.PayloadHash): packet,
		},
	}

	result, err := applyDVNDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{pathway}, []gethtypes.Log{log}, destinationLogObserver{})
	if err != nil {
		t.Fatalf("applyDVNDestinationLogs() error = %v", err)
	}
	if result.pending {
		t.Fatal("pending = true, want false")
	}
	if result.applied != 0 {
		t.Fatalf("applied = %d, want 0", result.applied)
	}
}

func TestApplyDVNDestinationLogsMarksVerified(t *testing.T) {
	packet := testDestinationPacketRecord()
	log := testPayloadVerifiedLog(t, packet, common.HexToAddress("0x6666666666666666666666666666666666666666"))
	store := &fakeDestinationStore{
		byVerification: map[string]db.PacketRecord{
			verificationLookupKey(packet.DstEID, packet.PacketHeader, packet.PayloadHash): packet,
		},
		dvnJobs: map[common.Hash]db.DVNJobRecord{
			packet.GUID: {
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNVerifyTxEnqueued),
			},
		},
	}

	applied, err := ApplyDVNDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, []gethtypes.Log{log})
	if err != nil {
		t.Fatalf("ApplyDVNDestinationLogs() error = %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied = %d, want 1", applied)
	}
	if store.dvnVerifiedGUID != packet.GUID {
		t.Fatalf("dvn verified guid = %s, want %s", store.dvnVerifiedGUID, packet.GUID)
	}
	if store.dvnVerifiedTxHash != log.TxHash {
		t.Fatalf("dvn verified tx = %s, want %s", store.dvnVerifiedTxHash, log.TxHash)
	}
}

func TestApplyDVNDestinationLogsMarksManualReviewVerified(t *testing.T) {
	tests := []string{
		string(packets.DVNQuorumConflict),
		string(packets.DVNManualReview),
	}
	for _, status := range tests {
		t.Run(status, func(t *testing.T) {
			packet := testDestinationPacketRecord()
			log := testPayloadVerifiedLog(t, packet, common.HexToAddress("0x6666666666666666666666666666666666666666"))
			store := &fakeDestinationStore{
				byVerification: map[string]db.PacketRecord{
					verificationLookupKey(packet.DstEID, packet.PacketHeader, packet.PayloadHash): packet,
				},
				dvnJobs: map[common.Hash]db.DVNJobRecord{
					packet.GUID: {
						GUID:                  packet.GUID,
						ConfirmationsRequired: 12,
						Status:                status,
					},
				},
			}

			applied, err := ApplyDVNDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, []gethtypes.Log{log})
			if err != nil {
				t.Fatalf("ApplyDVNDestinationLogs() error = %v", err)
			}
			if applied != 1 {
				t.Fatalf("applied = %d, want 1", applied)
			}
			if store.dvnVerifiedGUID != packet.GUID {
				t.Fatalf("dvn verified guid = %s, want %s", store.dvnVerifiedGUID, packet.GUID)
			}
		})
	}
}

func TestApplyDVNDestinationLogsSkipsReorgDetected(t *testing.T) {
	packet := testDestinationPacketRecord()
	log := testPayloadVerifiedLog(t, packet, common.HexToAddress("0x6666666666666666666666666666666666666666"))
	store := &fakeDestinationStore{
		byVerification: map[string]db.PacketRecord{
			verificationLookupKey(packet.DstEID, packet.PacketHeader, packet.PayloadHash): packet,
		},
		dvnJobs: map[common.Hash]db.DVNJobRecord{
			packet.GUID: {
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNReorgDetected),
			},
		},
	}

	applied, err := ApplyDVNDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, []gethtypes.Log{log})
	if err != nil {
		t.Fatalf("ApplyDVNDestinationLogs() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0", applied)
	}
	if store.dvnVerifiedGUID != (common.Hash{}) {
		t.Fatalf("dvn verified guid = %s, want zero", store.dvnVerifiedGUID)
	}
}

func TestApplyDVNDestinationLogsSkipsOtherDVN(t *testing.T) {
	packet := testDestinationPacketRecord()
	store := &fakeDestinationStore{
		byVerification: map[string]db.PacketRecord{
			verificationLookupKey(packet.DstEID, packet.PacketHeader, packet.PayloadHash): packet,
		},
		dvnJobs: map[common.Hash]db.DVNJobRecord{
			packet.GUID: {
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNVerifyTxEnqueued),
			},
		},
	}

	applied, err := ApplyDVNDestinationLogs(context.Background(), store, packet.DstEID, []chain.Pathway{testIndexerPathway()}, []gethtypes.Log{
		testPayloadVerifiedLog(t, packet, common.HexToAddress("0x4444444444444444444444444444444444444444")),
	})
	if err != nil {
		t.Fatalf("ApplyDVNDestinationLogs() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0", applied)
	}
}

type fakeReceiptStore struct {
	committedGUID   common.Hash
	committedTxHash common.Hash
	deliveredGUID   common.Hash
	deliveredTxHash common.Hash
	failedGUID      common.Hash
	failedTxHash    common.Hash
	failedReason    string
	expectedStatus  string
}

func testDestinationPacketRecord() db.PacketRecord {
	return db.PacketRecord{
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
		Status:         "ASSIGNED",
	}
}

type fakeDestinationStore struct {
	fakeReceiptStore
	byGUID            map[common.Hash]db.PacketRecord
	byDestination     map[string]db.PacketRecord
	byVerification    map[string]db.PacketRecord
	sourceSkips       map[string]db.SourcePacketSkip
	executorJobs      map[common.Hash]db.ExecutorJobRecord
	dvnJobs           map[common.Hash]db.DVNJobRecord
	dvnVerifiedGUID   common.Hash
	dvnVerifiedTxHash common.Hash
}

func (s *fakeDestinationStore) GetPacket(_ context.Context, guid common.Hash) (db.PacketRecord, error) {
	packet, ok := s.byGUID[guid]
	if !ok {
		return db.PacketRecord{}, pgx.ErrNoRows
	}
	return packet, nil
}

func (s *fakeDestinationStore) GetExecutorJob(_ context.Context, guid common.Hash) (db.ExecutorJobRecord, error) {
	job, ok := s.executorJobs[guid]
	if !ok {
		return db.ExecutorJobRecord{}, pgx.ErrNoRows
	}
	return job, nil
}

func (s *fakeDestinationStore) GetPacketByDestination(_ context.Context, dstEID, srcEID uint32, sender, receiver common.Address, nonce uint64) (db.PacketRecord, error) {
	key := destinationLookupKey(dstEID, srcEID, sender, receiver, nonce)
	packet, ok := s.byDestination[key]
	if !ok {
		return db.PacketRecord{}, pgx.ErrNoRows
	}
	return packet, nil
}

func (s *fakeDestinationStore) GetPacketByVerification(_ context.Context, dstEID uint32, packetHeader []byte, payloadHash common.Hash) (db.PacketRecord, error) {
	packet, ok := s.byVerification[verificationLookupKey(dstEID, packetHeader, payloadHash)]
	if !ok {
		return db.PacketRecord{}, pgx.ErrNoRows
	}
	return packet, nil
}

func (s *fakeDestinationStore) GetDVNJob(_ context.Context, guid common.Hash) (db.DVNJobRecord, error) {
	job, ok := s.dvnJobs[guid]
	if !ok {
		return db.DVNJobRecord{}, pgx.ErrNoRows
	}
	return job, nil
}

func (s *fakeDestinationStore) GetSourcePacketSkip(_ context.Context, role string, srcEID, dstEID uint32, sender, receiver common.Address, nonce uint64) (db.SourcePacketSkip, error) {
	skip, ok := s.sourceSkips[sourceSkipLookupKey(role, srcEID, dstEID, sender, receiver, nonce)]
	if !ok {
		return db.SourcePacketSkip{}, pgx.ErrNoRows
	}
	return skip, nil
}

func (s *fakeDestinationStore) MarkDVNVerifiedObserved(_ context.Context, guid, txHash common.Hash, _ string) error {
	s.dvnVerifiedGUID = guid
	s.dvnVerifiedTxHash = txHash
	if job, ok := s.dvnJobs[guid]; ok {
		job.Status = string(packets.DVNVerified)
		s.dvnJobs[guid] = job
	}
	return nil
}

func destinationLookupKey(dstEID, srcEID uint32, sender, receiver common.Address, nonce uint64) string {
	return fmt.Sprintf("%d:%d:%s:%s:%d", dstEID, srcEID, sender, receiver, nonce)
}

func verificationLookupKey(dstEID uint32, packetHeader []byte, payloadHash common.Hash) string {
	return fmt.Sprintf("%d:%x:%s", dstEID, packetHeader, payloadHash)
}

func sourceSkipLookupKey(role string, srcEID, dstEID uint32, sender, receiver common.Address, nonce uint64) string {
	return fmt.Sprintf("%s:%d:%d:%s:%s:%d", role, srcEID, dstEID, sender, receiver, nonce)
}

func (s *fakeReceiptStore) MarkExecutorCommittedObserved(_ context.Context, guid, txHash common.Hash, expectedStatus string) error {
	s.committedGUID = guid
	s.committedTxHash = txHash
	s.expectedStatus = expectedStatus
	return nil
}

func (s *fakeReceiptStore) MarkExecutorDeliveredObserved(_ context.Context, guid, txHash common.Hash, expectedStatus string) error {
	s.deliveredGUID = guid
	s.deliveredTxHash = txHash
	s.expectedStatus = expectedStatus
	return nil
}

func (s *fakeReceiptStore) MarkExecutorReceiveFailedObserved(_ context.Context, guid, txHash common.Hash, expectedStatus, reason string) error {
	s.failedGUID = guid
	s.failedTxHash = txHash
	s.failedReason = reason
	s.expectedStatus = expectedStatus
	return nil
}

func (s *fakeDestinationStore) MarkExecutorCommittedObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus string) error {
	if err := s.fakeReceiptStore.MarkExecutorCommittedObserved(ctx, guid, txHash, expectedStatus); err != nil {
		return err
	}
	if job, ok := s.executorJobs[guid]; ok {
		job.Status = string(packets.ExecutorCommitted)
		s.executorJobs[guid] = job
	}
	return nil
}

func (s *fakeDestinationStore) MarkExecutorDeliveredObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus string) error {
	if err := s.fakeReceiptStore.MarkExecutorDeliveredObserved(ctx, guid, txHash, expectedStatus); err != nil {
		return err
	}
	if job, ok := s.executorJobs[guid]; ok {
		job.Status = string(packets.ExecutorDelivered)
		s.executorJobs[guid] = job
	}
	return nil
}

func (s *fakeDestinationStore) MarkExecutorReceiveFailedObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus, reason string) error {
	if err := s.fakeReceiptStore.MarkExecutorReceiveFailedObserved(ctx, guid, txHash, expectedStatus, reason); err != nil {
		return err
	}
	if job, ok := s.executorJobs[guid]; ok {
		job.Status = string(packets.ExecutorLzReceiveFailed)
		s.executorJobs[guid] = job
	}
	return nil
}

func testPacketVerifiedLog(t *testing.T, packetRecord db.PacketRecord) gethtypes.Log {
	t.Helper()
	data, err := endpointEventInputs(t, "PacketVerified").Pack(originFromPacketRecord(packetRecord), packetRecord.Receiver, packetRecord.PayloadHash)
	if err != nil {
		t.Fatalf("Pack PacketVerified error = %v", err)
	}
	return gethtypes.Log{
		Topics: []common.Hash{lzabi.PacketVerifiedTopic()},
		Data:   data,
		TxHash: common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
	}
}

func testPacketDeliveredLog(t *testing.T, packetRecord db.PacketRecord) gethtypes.Log {
	t.Helper()
	data, err := endpointEventInputs(t, "PacketDelivered").Pack(originFromPacketRecord(packetRecord), packetRecord.Receiver)
	if err != nil {
		t.Fatalf("Pack PacketDelivered error = %v", err)
	}
	return gethtypes.Log{
		Topics: []common.Hash{lzabi.PacketDeliveredTopic()},
		Data:   data,
		TxHash: common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
	}
}

func testLzReceiveAlertLog(t *testing.T, packetRecord db.PacketRecord, reason []byte) gethtypes.Log {
	return testLzReceiveAlertLogWithExecutor(t, packetRecord, common.HexToAddress("0x9999999999999999999999999999999999999999"), reason)
}

func testLzReceiveAlertLogWithExecutor(t *testing.T, packetRecord db.PacketRecord, executor common.Address, reason []byte) gethtypes.Log {
	t.Helper()
	data, err := endpointEventInputs(t, "LzReceiveAlert").NonIndexed().Pack(
		originFromPacketRecord(packetRecord),
		packetRecord.GUID,
		big.NewInt(100_000),
		big.NewInt(0),
		packetRecord.Message,
		[]byte{},
		reason,
	)
	if err != nil {
		t.Fatalf("Pack LzReceiveAlert error = %v", err)
	}
	return gethtypes.Log{
		Topics: []common.Hash{
			lzabi.LzReceiveAlertTopic(),
			common.BytesToHash(packetRecord.Receiver.Bytes()),
			common.BytesToHash(executor.Bytes()),
		},
		Data:   data,
		TxHash: common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
	}
}

func testPayloadVerifiedLog(t *testing.T, packetRecord db.PacketRecord, dvn common.Address) gethtypes.Log {
	t.Helper()
	receiveUlnABI := lzabi.ReceiveUln302ABI()
	data, err := receiveUlnABI.Events["PayloadVerified"].Inputs.Pack(dvn, packetRecord.PacketHeader, big.NewInt(12), packetRecord.PayloadHash)
	if err != nil {
		t.Fatalf("Pack PayloadVerified error = %v", err)
	}
	return gethtypes.Log{
		Address: common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Topics:  []common.Hash{lzabi.PayloadVerifiedTopic()},
		Data:    data,
		TxHash:  common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444"),
	}
}

func originFromPacketRecord(packet db.PacketRecord) lzabi.Origin {
	return lzabi.Origin{
		SrcEID: packet.SrcEID,
		Sender: common.BytesToHash(packet.Sender.Bytes()),
		Nonce:  packet.Nonce.Uint64(),
	}
}

func endpointEventInputs(t *testing.T, name string) abi.Arguments {
	t.Helper()
	return lzabi.EndpointV2ABI().Events[name].Inputs
}
