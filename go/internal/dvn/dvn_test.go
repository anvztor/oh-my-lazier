package dvn

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/lzabi"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

func TestProcessConfirmationsOnceWaitsForSourceConfirmations(t *testing.T) {
	packet := testDVNPacket()
	logger, logs := captureLogger(slog.LevelInfo)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNAssigned),
			},
		}},
	}
	worker := NewWithHeads(store, map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 10}}, logger)

	processed, err := worker.ProcessConfirmationsOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessConfirmationsOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.waitingGUID != packet.GUID {
		t.Fatalf("waiting guid = %s, want %s", store.waitingGUID, packet.GUID)
	}
	assertLogContains(t, logs.String(),
		`msg="dvn job waiting for source confirmations"`,
		`guid=0x`,
		`src_eid=40161`,
		`dst_eid=40449`,
		`from_status=ASSIGNED`,
		`to_status=WAITING_CONFIRMATIONS`,
		`src_block_number=123`,
		`observed_head_block=133`,
		`confirmations_required=12`,
	)
}

func TestProcessConfirmationsOnceLogsInsufficientConfirmationsWithoutStatusChange(t *testing.T) {
	packet := testDVNPacket()
	logger, logs := captureLogger(slog.LevelDebug)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNWaitingConfirmations),
			},
		}},
	}
	worker := NewWithHeads(store, map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 10}}, logger)

	processed, err := worker.ProcessConfirmationsOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessConfirmationsOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.waitingGUID != (common.Hash{}) {
		t.Fatalf("waiting guid = %s, want zero", store.waitingGUID)
	}
	if store.deferredGUID != packet.GUID {
		t.Fatalf("deferred guid = %s, want %s", store.deferredGUID, packet.GUID)
	}
	if store.deferredStatus != string(packets.DVNWaitingConfirmations) {
		t.Fatalf("deferred status = %q, want %q", store.deferredStatus, packets.DVNWaitingConfirmations)
	}
	assertLogContains(t, logs.String(),
		`level=DEBUG`,
		`msg="skipped dvn confirmations"`,
		`reason=insufficient_confirmations`,
		`status=WAITING_CONFIRMATIONS`,
		`observed_head_block=133`,
	)
}

func TestProcessConfirmationsOnceMarksQuorumChecking(t *testing.T) {
	packet := testDVNPacket()
	logger, logs := captureLogger(slog.LevelInfo)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNWaitingConfirmations),
			},
		}},
	}
	worker := NewWithHeads(store, map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 11}}, logger)

	processed, err := worker.ProcessConfirmationsOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessConfirmationsOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.quorumGUID != packet.GUID {
		t.Fatalf("quorum guid = %s, want %s", store.quorumGUID, packet.GUID)
	}
	assertLogContains(t, logs.String(),
		`msg="dvn job reached source confirmations"`,
		`from_status=WAITING_CONFIRMATIONS`,
		`to_status=QUORUM_CHECKING`,
		`observed_head_block=134`,
	)
}

func TestProcessConfirmationsOnceActivePausesPathwayOnInsufficientAssignedConfirmations(t *testing.T) {
	packet := testDVNPacket()
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 10,
				Status:                string(packets.DVNAssigned),
			},
		}},
	}
	ulnConfig := defaultReceiveUlnConfig()
	ulnConfig.Confirmations = 15
	worker := NewWithClientsSettingsAndCallers(
		store,
		testRegistry(t, packet, config.DVNModeActive),
		nil,
		nil,
		nil,
		map[uint32]ContractCaller{
			packet.DstEID: fakeDVNReconcileCaller{ulnConfig: ulnConfig},
		},
		discardLogger(),
	)

	processed, err := worker.ProcessConfirmationsOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessConfirmationsOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.manualReviewGUID != packet.GUID {
		t.Fatalf("manual review guid = %s, want %s", store.manualReviewGUID, packet.GUID)
	}
	if !strings.Contains(store.manualReviewReason, "assigned confirmations 10 are below receive uln required confirmations 15") {
		t.Fatalf("manual review reason = %q, want insufficient assigned confirmations", store.manualReviewReason)
	}
	if store.pausedPathwayGUID != packet.GUID {
		t.Fatalf("paused pathway guid = %s, want %s", store.pausedPathwayGUID, packet.GUID)
	}
}

func TestProcessConfirmationsOncePausesChainOnHeadConflict(t *testing.T) {
	packet := testDVNPacket()
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNWaitingConfirmations),
			},
		}},
	}
	worker := NewWithHeads(store, map[uint32]HeadReader{packet.SrcEID: fakeHeadConflict{eid: packet.SrcEID}}, discardLogger())

	processed, err := worker.ProcessConfirmationsOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessConfirmationsOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.pausedChainEID != packet.SrcEID {
		t.Fatalf("paused chain eid = %d, want %d", store.pausedChainEID, packet.SrcEID)
	}
}

func TestProcessConfirmationsOnceRollsReorgBackToWaiting(t *testing.T) {
	packet := testDVNPacket()
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNReorgDetected),
			},
		}},
	}
	worker := NewWithHeads(store, nil, discardLogger())

	processed, err := worker.ProcessConfirmationsOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessConfirmationsOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.waitingGUID != packet.GUID {
		t.Fatalf("waiting guid = %s, want %s", store.waitingGUID, packet.GUID)
	}
	if store.waitingExpectedStatus != string(packets.DVNReorgDetected) {
		t.Fatalf("waiting expected status = %q, want %q", store.waitingExpectedStatus, packets.DVNReorgDetected)
	}
}

func TestProcessQuorumOnceMarksReadyToVerify(t *testing.T) {
	packet := testDVNPacket()
	logger, logs := captureLogger(slog.LevelInfo)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNQuorumChecking),
			},
		}},
	}
	worker := NewWithClients(
		store,
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		map[uint32]ReceiptReader{packet.SrcEID: fakeReceiptReader{receipt: testSourceReceipt(t, packet)}},
		logger,
	)

	processed, err := worker.ProcessQuorumOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessQuorumOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.readyGUID != packet.GUID {
		t.Fatalf("ready guid = %s, want %s", store.readyGUID, packet.GUID)
	}
	if len(store.quorumResult) == 0 {
		t.Fatal("quorum result is empty")
	}
	var report QuorumReport
	if err := json.Unmarshal(store.quorumResult, &report); err != nil {
		t.Fatalf("Unmarshal quorum result error = %v", err)
	}
	if report.GUID != packet.GUID.Hex() {
		t.Fatalf("report guid = %s, want %s", report.GUID, packet.GUID.Hex())
	}
	if report.PayloadHash != packet.PayloadHash.Hex() {
		t.Fatalf("report payload hash = %s, want %s", report.PayloadHash, packet.PayloadHash.Hex())
	}
	if report.TxHash != packet.SrcTxHash.Hex() {
		t.Fatalf("report tx hash = %s, want %s", report.TxHash, packet.SrcTxHash.Hex())
	}
	assertLogContains(t, logs.String(),
		`msg="dvn job ready to verify"`,
		`guid=0x`,
		`src_eid=40161`,
		`dst_eid=40449`,
		`from_status=QUORUM_CHECKING`,
		`to_status=READY_TO_VERIFY`,
	)
}

func TestProcessReadyToVerifyOnceMarksWouldVerify(t *testing.T) {
	packet := testDVNPacket()
	report := []byte(`{"status":"ready"}`)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNReadyToVerify),
				QuorumResult:          report,
			},
		}},
	}
	worker := NewWithSettings(store, testRegistry(t, packet, config.DVNModeShadow), nil, discardLogger())

	processed, err := worker.ProcessReadyToVerifyOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessReadyToVerifyOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.wouldVerifyGUID != packet.GUID {
		t.Fatalf("would verify guid = %s, want %s", store.wouldVerifyGUID, packet.GUID)
	}
	if !bytes.Equal(store.quorumResult, report) {
		t.Fatalf("quorum result = %s, want %s", store.quorumResult, report)
	}
}

func TestProcessReadyToVerifyOnceActiveSkipsInactiveSendScope(t *testing.T) {
	packet := testDVNPacket()
	logger, logs := captureLogger(slog.LevelDebug)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNReadyToVerify),
				QuorumResult:          []byte(`{"status":"ready"}`),
			},
		}},
		enqueueErr: db.ErrTxSendScopeInactive,
	}
	registry := testRegistry(t, packet, config.DVNModeActive)
	worker := NewWithClientsSettingsAndCallers(
		store,
		registry,
		map[uint32]Settings{
			packet.DstEID: {
				SignerID: "0x8888888888888888888888888888888888888888",
			},
		},
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		nil,
		map[uint32]ContractCaller{packet.DstEID: fakeDVNReconcileCaller{}},
		logger,
	)

	// A pause committed between work selection and the enqueue is a normal
	// skip: no error, no job transition, resumed after unpause.
	processed, err := worker.ProcessReadyToVerifyOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessReadyToVerifyOnce() error = %v, want skipped without error", err)
	}
	if processed {
		t.Fatal("processed = true, want false for an inactive send scope")
	}
	if store.verifyGUID != (common.Hash{}) {
		t.Fatalf("verify enqueued for %s, want none", store.verifyGUID)
	}
	assertLogContains(t, logs.String(),
		`msg="skipped dvn verify tx enqueue"`,
		`reason=send_scope_inactive`,
	)
}

func TestProcessReadyToVerifyOnceActiveEnqueuesVerifyTxWithAsymmetricConfirmations(t *testing.T) {
	packet := testDVNPacket()
	report := []byte(`{"status":"ready"}`)
	logger, logs := captureLogger(slog.LevelInfo)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 20,
				Status:                string(packets.DVNReadyToVerify),
				QuorumResult:          report,
			},
		}},
	}
	registry := testRegistry(t, packet, config.DVNModeActive)
	worker := NewWithClientsSettingsAndCallers(
		store,
		registry,
		map[uint32]Settings{
			packet.DstEID: {
				SignerID: "0x8888888888888888888888888888888888888888",
			},
		},
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		nil,
		map[uint32]ContractCaller{
			packet.DstEID: fakeDVNReconcileCaller{ulnConfig: receiveUlnConfig{
				Confirmations:        15,
				RequiredDVNCount:     2,
				OptionalDVNCount:     nilDVNCount,
				OptionalDVNThreshold: 0,
				RequiredDVNs: []common.Address{
					common.HexToAddress("0x6666666666666666666666666666666666666666"),
					common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd"),
				},
			}},
		},
		logger,
	)

	processed, err := worker.ProcessReadyToVerifyOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessReadyToVerifyOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.verifyGUID != packet.GUID {
		t.Fatalf("verify guid = %s, want %s", store.verifyGUID, packet.GUID)
	}
	if store.verifyRequest.Purpose != TxPurposeVerify {
		t.Fatalf("verify purpose = %q, want %q", store.verifyRequest.Purpose, TxPurposeVerify)
	}
	if store.verifyRequest.ChainEID != packet.DstEID {
		t.Fatalf("verify chain = %d, want %d", store.verifyRequest.ChainEID, packet.DstEID)
	}
	if store.verifyRequest.To != common.HexToAddress("0x6666666666666666666666666666666666666666") {
		t.Fatalf("verify target = %s", store.verifyRequest.To)
	}
	if store.verifyRequest.SignerID != "0x8888888888888888888888888888888888888888" {
		t.Fatalf("verify signer = %q, want destination dvn signer", store.verifyRequest.SignerID)
	}
	openDVNABI := lzabi.OpenDVNABI()
	if len(store.verifyRequest.Calldata) < 4 || !bytes.Equal(store.verifyRequest.Calldata[:4], openDVNABI.Methods["submitVerification"].ID) {
		t.Fatalf("verify calldata selector = %x", store.verifyRequest.Calldata[:4])
	}
	args, err := openDVNABI.Methods["submitVerification"].Inputs.Unpack(store.verifyRequest.Calldata[4:])
	if err != nil {
		t.Fatalf("unpack submitVerification calldata: %v", err)
	}
	if args[0].(common.Address) != common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") {
		t.Fatalf("submitVerification receiveLib = %s", args[0].(common.Address))
	}
	if args[3].(uint64) != 20 {
		t.Fatalf("submitVerification confirmations = %d, want assigned confirmations 20", args[3].(uint64))
	}
	if store.manualReviewGUID != (common.Hash{}) {
		t.Fatalf("manual review guid = %s, want zero", store.manualReviewGUID)
	}
	if !bytes.Equal(store.quorumResult, report) {
		t.Fatalf("quorum result = %s, want %s", store.quorumResult, report)
	}
	assertLogContains(t, logs.String(),
		`msg="enqueued dvn verify tx"`,
		`guid=0x`,
		`from_status=READY_TO_VERIFY`,
		`to_status=VERIFY_TX_ENQUEUED`,
		`tx_outbox_id=42`,
	)
}

// fakeDVNDriftAtConfirmedCaller reports the configured receive library at latest
// but a different (older) library at any historical block, simulating a recent
// config change that has not yet reached confirmation depth.
type fakeDVNDriftAtConfirmedCaller struct{}

func (fakeDVNDriftAtConfirmedCaller) CallContract(_ context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	method, err := dvnMethodBySelector(call.Data)
	if err != nil {
		return nil, err
	}
	switch method.Name {
	case "inboundPayloadHash":
		return method.Outputs.Pack(common.Hash{})
	case "getReceiveLibrary":
		lib := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		if blockNumber != nil {
			lib = common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		}
		return method.Outputs.Pack(lib, false)
	case "getUlnConfig":
		return method.Outputs.Pack(defaultReceiveUlnConfig())
	case "hashLookup":
		return method.Outputs.Pack(true, uint64(12))
	default:
		return nil, fmt.Errorf("unexpected method %s", method.Name)
	}
}

func TestProcessReadyToVerifyOnceActiveDefersConfigMismatchAtConfirmedBlock(t *testing.T) {
	packet := testDVNPacket()
	report := []byte(`{"status":"ready"}`)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNReadyToVerify),
				QuorumResult:          report,
			},
		}},
	}
	worker := NewWithClientsSettingsAndCallers(
		store,
		testRegistry(t, packet, config.DVNModeActive),
		map[uint32]Settings{packet.DstEID: {SignerID: "0x8888888888888888888888888888888888888888"}},
		map[uint32]HeadReader{packet.DstEID: fakeHead{head: 1_000_000}},
		nil,
		map[uint32]ContractCaller{packet.DstEID: fakeDVNDriftAtConfirmedCaller{}},
		discardLogger(),
	)

	processed, err := worker.ProcessReadyToVerifyOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessReadyToVerifyOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.verifiedFromChainGUID != (common.Hash{}) {
		t.Fatal("marked verified from a below-confirmation-depth observation")
	}
	if store.manualReviewGUID != (common.Hash{}) || store.pausedPathwayGUID != (common.Hash{}) {
		t.Fatal("a historical (unconfirmed) config difference must not pause the pathway")
	}
	if store.deferredGUID != packet.GUID {
		t.Fatalf("deferred guid = %s, want %s", store.deferredGUID, packet.GUID)
	}
}

func TestProcessReadyToVerifyOnceActivePausesPathwayOnReceiveLibraryDrift(t *testing.T) {
	packet := testDVNPacket()
	report := []byte(`{"status":"ready"}`)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNReadyToVerify),
				QuorumResult:          report,
			},
		}},
	}
	worker := NewWithClientsSettingsAndCallers(
		store,
		testRegistry(t, packet, config.DVNModeActive),
		map[uint32]Settings{
			packet.DstEID: {
				SignerID: "0x8888888888888888888888888888888888888888",
			},
		},
		nil,
		nil,
		map[uint32]ContractCaller{
			packet.DstEID: fakeDVNReconcileCaller{receiveLib: common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")},
		},
		discardLogger(),
	)

	processed, err := worker.ProcessReadyToVerifyOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessReadyToVerifyOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.manualReviewGUID != packet.GUID {
		t.Fatalf("manual review guid = %s, want %s", store.manualReviewGUID, packet.GUID)
	}
	if !strings.Contains(store.manualReviewReason, "destination receive library") {
		t.Fatalf("manual review reason = %q, want receive library mismatch", store.manualReviewReason)
	}
	if store.pausedPathwayGUID != packet.GUID {
		t.Fatalf("paused pathway guid = %s, want %s", store.pausedPathwayGUID, packet.GUID)
	}
	if store.verifyRequest.Purpose != "" {
		t.Fatalf("unexpected verify enqueue purpose %q", store.verifyRequest.Purpose)
	}
}

func TestProcessReadyToVerifyOnceActivePausesPathwayOnInsufficientAssignedConfirmations(t *testing.T) {
	packet := testDVNPacket()
	report := []byte(`{"status":"ready"}`)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 10,
				Status:                string(packets.DVNReadyToVerify),
				QuorumResult:          report,
			},
		}},
	}
	ulnConfig := defaultReceiveUlnConfig()
	ulnConfig.Confirmations = 15
	worker := NewWithClientsSettingsAndCallers(
		store,
		testRegistry(t, packet, config.DVNModeActive),
		map[uint32]Settings{
			packet.DstEID: {
				SignerID: "0x8888888888888888888888888888888888888888",
			},
		},
		nil,
		nil,
		map[uint32]ContractCaller{
			packet.DstEID: fakeDVNReconcileCaller{ulnConfig: ulnConfig},
		},
		discardLogger(),
	)

	processed, err := worker.ProcessReadyToVerifyOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessReadyToVerifyOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.manualReviewGUID != packet.GUID {
		t.Fatalf("manual review guid = %s, want %s", store.manualReviewGUID, packet.GUID)
	}
	if !strings.Contains(store.manualReviewReason, "assigned confirmations 10 are below receive uln required confirmations 15") {
		t.Fatalf("manual review reason = %q, want insufficient assigned confirmations", store.manualReviewReason)
	}
	if store.pausedPathwayGUID != packet.GUID {
		t.Fatalf("paused pathway guid = %s, want %s", store.pausedPathwayGUID, packet.GUID)
	}
	if store.verifyRequest.Purpose != "" {
		t.Fatalf("unexpected verify enqueue purpose %q", store.verifyRequest.Purpose)
	}
}

func TestProcessReadyToVerifyOnceActiveDefersReconcileError(t *testing.T) {
	packet := testDVNPacket()
	report := []byte(`{"status":"ready"}`)
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNReadyToVerify),
				QuorumResult:          report,
			},
		}},
	}
	worker := NewWithClientsSettingsAndCallers(
		store,
		testRegistry(t, packet, config.DVNModeActive),
		map[uint32]Settings{
			packet.DstEID: {
				SignerID: "0x8888888888888888888888888888888888888888",
			},
		},
		nil,
		nil,
		map[uint32]ContractCaller{packet.DstEID: failingDVNCaller{}},
		discardLogger(),
	)

	processed, err := worker.ProcessReadyToVerifyOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessReadyToVerifyOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.deferredGUID != packet.GUID {
		t.Fatalf("deferred guid = %s, want %s", store.deferredGUID, packet.GUID)
	}
	if store.deferredStatus != string(packets.DVNReadyToVerify) {
		t.Fatalf("deferred status = %q, want %q", store.deferredStatus, packets.DVNReadyToVerify)
	}
	if store.verifyRequest.Purpose != "" {
		t.Fatalf("unexpected verify enqueue purpose %q", store.verifyRequest.Purpose)
	}
}

func TestProcessReadyToVerifyOnceActiveMarksVerifiedWhenAlreadyCompleteOnChain(t *testing.T) {
	packet := testDVNPacket()
	report := []byte(`{"status":"ready"}`)
	tests := []struct {
		name   string
		caller fakeDVNReconcileCaller
	}{
		{
			name:   "endpoint payload hash",
			caller: fakeDVNReconcileCaller{endpointPayloadHash: packet.PayloadHash},
		},
		{
			name:   "dvn hash lookup confirmations",
			caller: fakeDVNReconcileCaller{hashLookupSubmitted: true, hashLookupConfirmations: 12},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{
				work: []db.DVNWorkItem{{
					Packet: packet,
					Job: db.DVNJobRecord{
						GUID:                  packet.GUID,
						ConfirmationsRequired: 12,
						Status:                string(packets.DVNReadyToVerify),
						QuorumResult:          report,
					},
				}},
			}
			worker := NewWithClientsSettingsAndCallers(
				store,
				testRegistry(t, packet, config.DVNModeActive),
				map[uint32]Settings{
					packet.DstEID: {
						SignerID: "0x8888888888888888888888888888888888888888",
					},
				},
				map[uint32]HeadReader{packet.DstEID: fakeHead{head: 1_000_000}},
				nil,
				map[uint32]ContractCaller{packet.DstEID: test.caller},
				discardLogger(),
			)

			processed, err := worker.ProcessReadyToVerifyOnce(context.Background())
			if err != nil {
				t.Fatalf("ProcessReadyToVerifyOnce() error = %v", err)
			}
			if !processed {
				t.Fatal("processed = false, want true")
			}
			if store.verifiedFromChainGUID != packet.GUID {
				t.Fatalf("verified-from-chain guid = %s, want %s", store.verifiedFromChainGUID, packet.GUID)
			}
			if store.verifyRequest.Purpose != "" {
				t.Fatalf("unexpected verify enqueue purpose %q", store.verifyRequest.Purpose)
			}
			if !bytes.Equal(store.quorumResult, report) {
				t.Fatalf("quorum result = %s, want %s", store.quorumResult, report)
			}
		})
	}
}

func TestBuildVerifyTxRejectsMissingConfirmations(t *testing.T) {
	packet := testDVNPacket()
	_, err := BuildVerifyTx(
		packet,
		common.HexToAddress("0x6666666666666666666666666666666666666666"),
		common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		0,
		"0x9999999999999999999999999999999999999999",
	)
	if err == nil {
		t.Fatal("BuildVerifyTx() error = nil, want missing confirmations error")
	}
}

func TestProcessQuorumOnceMarksConflictOnMismatchedReceipt(t *testing.T) {
	packet := testDVNPacket()
	receipt := testSourceReceipt(t, packet)
	receipt.Status = gethtypes.ReceiptStatusFailed
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNQuorumChecking),
			},
		}},
	}
	worker := NewWithClients(
		store,
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		map[uint32]ReceiptReader{packet.SrcEID: fakeReceiptReader{receipt: receipt}},
		discardLogger(),
	)

	processed, err := worker.ProcessQuorumOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessQuorumOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.conflictGUID != packet.GUID {
		t.Fatalf("conflict guid = %s, want %s", store.conflictGUID, packet.GUID)
	}
	if store.conflictReason == "" {
		t.Fatal("conflict reason is empty")
	}
	if store.pausedPathwayGUID != packet.GUID {
		t.Fatalf("paused pathway guid = %s, want %s", store.pausedPathwayGUID, packet.GUID)
	}
}

func TestProcessQuorumOnceMarksConflictOnRPCDisagreement(t *testing.T) {
	packet := testDVNPacket()
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNQuorumChecking),
			},
		}},
	}
	worker := NewWithClients(
		store,
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		map[uint32]ReceiptReader{packet.SrcEID: fakeReceiptConflictReader{txHash: packet.SrcTxHash}},
		discardLogger(),
	)

	processed, err := worker.ProcessQuorumOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessQuorumOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.conflictGUID != packet.GUID {
		t.Fatalf("conflict guid = %s, want %s", store.conflictGUID, packet.GUID)
	}
	if !strings.Contains(store.conflictReason, "rpc receipt quorum conflict") {
		t.Fatalf("conflict reason = %q, want rpc receipt quorum conflict", store.conflictReason)
	}
	if !strings.Contains(store.conflictReason, "provider[0]") || !strings.Contains(store.conflictReason, "provider[1]") {
		t.Fatalf("conflict reason = %q, want redacted provider identities", store.conflictReason)
	}
	var persisted map[string]string
	if err := json.Unmarshal(store.quorumResult, &persisted); err != nil {
		t.Fatalf("Unmarshal(quorum result) error = %v", err)
	}
	if persisted["error"] != store.conflictReason {
		t.Fatalf("persisted error = %q, want %q", persisted["error"], store.conflictReason)
	}
	if strings.Contains(store.conflictReason, "://") || strings.Contains(string(store.quorumResult), "://") {
		t.Fatalf("persisted conflict contains raw provider URL: reason=%q result=%s", store.conflictReason, store.quorumResult)
	}
	if store.pausedPathwayGUID != packet.GUID {
		t.Fatalf("paused pathway guid = %s, want %s", store.pausedPathwayGUID, packet.GUID)
	}
}

func TestProcessQuorumOnceMarksReorgWhenReceiptDisappears(t *testing.T) {
	packet := testDVNPacket()
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNQuorumChecking),
			},
		}},
	}
	worker := NewWithClients(
		store,
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		map[uint32]ReceiptReader{packet.SrcEID: fakeReceiptNotFoundReader{}},
		discardLogger(),
	)

	processed, err := worker.ProcessQuorumOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessQuorumOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.reorgGUID != packet.GUID {
		t.Fatalf("reorg guid = %s, want %s", store.reorgGUID, packet.GUID)
	}
	if store.reorgReason == "" {
		t.Fatal("reorg reason is empty")
	}
	if store.pausedPathwayGUID != (common.Hash{}) {
		t.Fatalf("paused pathway guid = %s, want zero", store.pausedPathwayGUID)
	}
	if len(store.quorumResult) == 0 {
		t.Fatal("quorum result is empty")
	}
	if store.reorgCoords != nil {
		t.Fatal("packet coordinates must not change when the receipt disappeared entirely")
	}
}

func TestProcessQuorumOnceMarksReorgWhenReceiptBlockMoved(t *testing.T) {
	packet := testDVNPacket()
	receipt := testSourceReceipt(t, packet)
	// The source tx was re-included by a reorg at a later block. Re-inclusion at
	// a different position also shifts the block-global log index, so the receipt
	// block must be checked before the log-index lookup or the reorg is missed.
	receipt.BlockNumber = new(big.Int).SetUint64(packet.SrcBlockNumber + 3)
	receipt.Logs[0].BlockNumber = packet.SrcBlockNumber + 3
	receipt.Logs[0].Index = packet.SrcLogIndex + 5
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNQuorumChecking),
			},
		}},
	}
	worker := NewWithClients(
		store,
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		map[uint32]ReceiptReader{packet.SrcEID: fakeReceiptReader{receipt: receipt}},
		discardLogger(),
	)

	processed, err := worker.ProcessQuorumOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessQuorumOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.reorgGUID != packet.GUID {
		t.Fatalf("reorg guid = %s, want %s", store.reorgGUID, packet.GUID)
	}
	if store.readyGUID != (common.Hash{}) {
		t.Fatalf("ready guid = %s, want zero (must not verify a moved receipt)", store.readyGUID)
	}
	if store.pausedPathwayGUID != (common.Hash{}) {
		t.Fatalf("paused pathway guid = %s, want zero", store.pausedPathwayGUID)
	}
	// The stored coordinates must follow the re-included receipt — atomically
	// with the status transition: the forward-only source cursor never rescans
	// the replacement block, and a separate coordinate commit would leave a
	// window for another instance to re-enter quorum on the old coordinates.
	if store.reorgCoords == nil {
		t.Fatal("packet coordinates were not refreshed after re-inclusion")
	}
	if store.reorgCoords.srcBlockNumber != packet.SrcBlockNumber+3 {
		t.Fatalf("refreshed src block = %d, want %d", store.reorgCoords.srcBlockNumber, packet.SrcBlockNumber+3)
	}
	if store.reorgCoords.srcLogIndex != packet.SrcLogIndex+5 {
		t.Fatalf("refreshed src log index = %d, want %d", store.reorgCoords.srcLogIndex, packet.SrcLogIndex+5)
	}
	if store.reorgCoords.expectedSrcBlockNumber != packet.SrcBlockNumber || store.reorgCoords.expectedSrcLogIndex != packet.SrcLogIndex {
		t.Fatalf("coordinate CAS expects %d/%d, want the stale %d/%d", store.reorgCoords.expectedSrcBlockNumber, store.reorgCoords.expectedSrcLogIndex, packet.SrcBlockNumber, packet.SrcLogIndex)
	}
}

func TestProcessQuorumOnceDefersReceiptReaderError(t *testing.T) {
	packet := testDVNPacket()
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNQuorumChecking),
			},
		}},
	}
	worker := NewWithClients(
		store,
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		map[uint32]ReceiptReader{packet.SrcEID: fakeReceiptErrorReader{err: fmt.Errorf("receipt rpc unavailable")}},
		discardLogger(),
	)

	processed, err := worker.ProcessQuorumOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessQuorumOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.deferredGUID != packet.GUID {
		t.Fatalf("deferred guid = %s, want %s", store.deferredGUID, packet.GUID)
	}
	if store.deferredStatus != string(packets.DVNQuorumChecking) {
		t.Fatalf("deferred status = %q, want %q", store.deferredStatus, packets.DVNQuorumChecking)
	}
	if store.conflictGUID != (common.Hash{}) {
		t.Fatalf("conflict guid = %s, want zero", store.conflictGUID)
	}
	if store.reorgGUID != (common.Hash{}) {
		t.Fatalf("reorg guid = %s, want zero", store.reorgGUID)
	}
}

func TestVerifySourceReceiptForEndpointRejectsWrongPacketSentAddress(t *testing.T) {
	packet := testDVNPacket()
	receipt := testSourceReceipt(t, packet)
	receipt.Logs[0].Address = common.HexToAddress("0x1212121212121212121212121212121212121212")

	_, err := verifySourceReceiptForEndpoint(packet, receipt, common.HexToAddress("0x1111111111111111111111111111111111111111"))
	if err == nil {
		t.Fatal("verifySourceReceiptForEndpoint() error = nil, want endpoint mismatch")
	}
	if !strings.Contains(err.Error(), "PacketSent address") {
		t.Fatalf("verifySourceReceiptForEndpoint() error = %v, want PacketSent address mismatch", err)
	}
}

type fakeStore struct {
	work                  []db.DVNWorkItem
	waitingGUID           common.Hash
	waitingExpectedStatus string
	quorumGUID            common.Hash
	readyGUID             common.Hash
	wouldVerifyGUID       common.Hash
	verifyGUID            common.Hash
	verifiedFromChainGUID common.Hash
	verifyRequest         db.TxRequest
	conflictGUID          common.Hash
	conflictReason        string
	reorgGUID             common.Hash
	reorgReason           string
	manualReviewGUID      common.Hash
	manualReviewReason    string
	pausedChainEID        uint32
	pausedPathwayGUID     common.Hash
	quorumResult          []byte
	deferredGUID          common.Hash
	deferredStatus        string
	enqueueErr            error
	reorgCoords           *reorgCoordinates
	reorgCoordsErr        error
}

type reorgCoordinates struct {
	srcBlockNumber         uint64
	srcLogIndex            uint
	expectedSrcBlockNumber uint64
	expectedSrcLogIndex    uint
}

func (s *fakeStore) MarkDVNReorgDetectedWithCoordinates(_ context.Context, guid common.Hash, expectedStatus, reason string, quorumResult []byte, srcBlockNumber uint64, srcLogIndex uint, expectedSrcBlockNumber uint64, expectedSrcLogIndex uint) error {
	if s.reorgCoordsErr != nil {
		return s.reorgCoordsErr
	}
	if expectedStatus != string(packets.DVNQuorumChecking) {
		return fmt.Errorf("unexpected expected status %q", expectedStatus)
	}
	s.reorgGUID = guid
	s.reorgReason = reason
	s.quorumResult = quorumResult
	s.reorgCoords = &reorgCoordinates{
		srcBlockNumber:         srcBlockNumber,
		srcLogIndex:            srcLogIndex,
		expectedSrcBlockNumber: expectedSrcBlockNumber,
		expectedSrcLogIndex:    expectedSrcLogIndex,
	}
	return nil
}

func (s *fakeStore) ListDVNWork(_ context.Context, status string, _ int) ([]db.DVNWorkItem, error) {
	for _, item := range s.work {
		if item.Job.Status == status {
			return []db.DVNWorkItem{item}, nil
		}
	}
	return nil, nil
}

func (s *fakeStore) MarkDVNWaitingConfirmations(_ context.Context, guid common.Hash, expectedStatus string) error {
	s.waitingGUID = guid
	s.waitingExpectedStatus = expectedStatus
	return nil
}

func (s *fakeStore) MarkDVNQuorumChecking(_ context.Context, guid common.Hash, _ string) error {
	s.quorumGUID = guid
	return nil
}

func (s *fakeStore) MarkDVNReadyToVerify(_ context.Context, guid common.Hash, _ string, quorumResult []byte) error {
	s.readyGUID = guid
	s.quorumResult = bytes.Clone(quorumResult)
	return nil
}

func (s *fakeStore) MarkDVNWouldVerify(_ context.Context, guid common.Hash, _ string, quorumResult []byte) error {
	s.wouldVerifyGUID = guid
	s.quorumResult = bytes.Clone(quorumResult)
	return nil
}

func (s *fakeStore) EnqueueDVNVerifyTx(_ context.Context, guid common.Hash, _, _ string, request db.TxRequest, quorumResult []byte) (int64, error) {
	if s.enqueueErr != nil {
		return 0, s.enqueueErr
	}
	s.verifyGUID = guid
	s.verifyRequest = request
	s.quorumResult = bytes.Clone(quorumResult)
	return 42, nil
}

func (s *fakeStore) MarkDVNVerifiedFromChain(_ context.Context, guid common.Hash, _ string, quorumResult []byte) error {
	s.verifiedFromChainGUID = guid
	s.quorumResult = bytes.Clone(quorumResult)
	return nil
}

func (s *fakeStore) MarkDVNQuorumConflict(_ context.Context, guid common.Hash, _, reason string, quorumResult []byte) error {
	s.conflictGUID = guid
	s.conflictReason = reason
	s.quorumResult = bytes.Clone(quorumResult)
	return nil
}

func (s *fakeStore) MarkDVNReorgDetected(_ context.Context, guid common.Hash, _, reason string, quorumResult []byte) error {
	s.reorgGUID = guid
	s.reorgReason = reason
	s.quorumResult = bytes.Clone(quorumResult)
	return nil
}

func (s *fakeStore) MarkDVNManualReviewAndPausePathway(_ context.Context, guid common.Hash, _ string, reason string, quorumResult []byte) error {
	s.manualReviewGUID = guid
	s.manualReviewReason = reason
	s.quorumResult = bytes.Clone(quorumResult)
	s.pausedPathwayGUID = guid
	return nil
}

func (s *fakeStore) DeferDVNJob(_ context.Context, guid common.Hash, expectedStatus string, _ time.Duration) error {
	s.deferredGUID = guid
	s.deferredStatus = expectedStatus
	return nil
}

func testRegistry(t *testing.T, packet db.PacketRecord, mode config.DVNMode) *chain.Registry {
	t.Helper()
	registry, err := chain.NewRegistry(
		[]config.ChainConfig{
			{
				EID:             packet.SrcEID,
				Name:            "source",
				Family:          config.ChainFamilyEVM,
				ChainID:         11155111,
				EndpointAddress: config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
				Confirmations:   12,
				RPCURLs:         []string{"http://localhost:8545"},
				TxRoles: config.ChainTxRolesConfig{
					Executor: config.ExecutorTxRoleConfig{
						Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
						MaxFeePerGasWei:         "2000000000",
						MaxPriorityFeePerGasWei: "1000000000",
						MinNativeBalanceWei:     "100000000000000000",
					},
					DVN: config.DVNTxRoleConfig{
						Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
						MaxFeePerGasWei:         "2000000000",
						MaxPriorityFeePerGasWei: "1000000000",
						MinNativeBalanceWei:     "100000000000000000",
					},
				},
			},
			{
				EID:             packet.DstEID,
				Name:            "destination",
				Family:          config.ChainFamilyEVM,
				ChainID:         560048,
				EndpointAddress: config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
				Confirmations:   12,
				RPCURLs:         []string{"http://localhost:8546"},
				TxRoles: config.ChainTxRolesConfig{
					Executor: config.ExecutorTxRoleConfig{
						Signer:                  config.MustEVMAddress("0x8888888888888888888888888888888888888888"),
						MaxFeePerGasWei:         "2000000000",
						MaxPriorityFeePerGasWei: "1000000000",
						MinNativeBalanceWei:     "100000000000000000",
					},
					DVN: config.DVNTxRoleConfig{
						Signer:                  config.MustEVMAddress("0x8888888888888888888888888888888888888888"),
						MaxFeePerGasWei:         "2000000000",
						MaxPriorityFeePerGasWei: "1000000000",
						MinNativeBalanceWei:     "100000000000000000",
					},
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
				DVN:            config.PathwayDVNConfig{Mode: mode},
				Enabled:        true,
				MaxMessageSize: 10000,
			},
		},
	)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	return registry
}

func (s *fakeStore) PauseChain(_ context.Context, eid uint32) error {
	s.pausedChainEID = eid
	return nil
}

func (s *fakeStore) PausePathwayForPacket(_ context.Context, guid common.Hash) error {
	s.pausedPathwayGUID = guid
	return nil
}

type fakeHead struct {
	head uint64
}

func (h fakeHead) BlockNumber(context.Context) (uint64, error) {
	return h.head, nil
}

func (h fakeHead) CheckHead(context.Context) (rpcquorum.HeadResult, error) {
	return rpcquorum.HeadResult{Number: new(big.Int).SetUint64(h.head), Hash: common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").Hex()}, nil
}

type fakeHeadConflict struct {
	eid uint32
}

func (h fakeHeadConflict) CheckHead(context.Context) (rpcquorum.HeadResult, error) {
	return rpcquorum.HeadResult{}, &rpcquorum.HeadConflictError{
		ChainName: fmt.Sprintf("eid-%d", h.eid),
		Number:    big.NewInt(42),
		Details:   []string{"provider a disagrees with provider b"},
	}
}

type fakeReceiptReader struct {
	receipt *gethtypes.Receipt
}

func (r fakeReceiptReader) TransactionReceipt(context.Context, common.Hash) (*gethtypes.Receipt, error) {
	return r.receipt, nil
}

type fakeReceiptConflictReader struct {
	txHash common.Hash
}

func (r fakeReceiptConflictReader) TransactionReceipt(context.Context, common.Hash) (*gethtypes.Receipt, error) {
	return nil, &rpcquorum.ReceiptConflictError{
		TxHash:  r.txHash,
		Details: []string{"provider[0] returned receipt-a", "provider[1] returned receipt-b"},
	}
}

type fakeReceiptNotFoundReader struct{}

func (r fakeReceiptNotFoundReader) TransactionReceipt(context.Context, common.Hash) (*gethtypes.Receipt, error) {
	return nil, ethereum.NotFound
}

type fakeReceiptErrorReader struct {
	err error
}

func (r fakeReceiptErrorReader) TransactionReceipt(context.Context, common.Hash) (*gethtypes.Receipt, error) {
	return nil, r.err
}

type failingDVNCaller struct{}

func (failingDVNCaller) CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error) {
	return nil, fmt.Errorf("eth_call unavailable")
}

type fakeDVNReconcileCaller struct {
	endpointPayloadHash     common.Hash
	receiveLib              common.Address
	ulnConfig               receiveUlnConfig
	hashLookupSubmitted     bool
	hashLookupConfirmations uint64
}

func (c fakeDVNReconcileCaller) CallContract(_ context.Context, call ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	method, err := dvnMethodBySelector(call.Data)
	if err != nil {
		return nil, err
	}
	switch method.Name {
	case "inboundPayloadHash":
		return method.Outputs.Pack(c.endpointPayloadHash)
	case "getReceiveLibrary":
		receiveLib := c.receiveLib
		if receiveLib == (common.Address{}) {
			receiveLib = common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		}
		return method.Outputs.Pack(receiveLib, false)
	case "getUlnConfig":
		config := c.ulnConfig
		if config.Confirmations == 0 && len(config.RequiredDVNs) == 0 {
			config = defaultReceiveUlnConfig()
		}
		return method.Outputs.Pack(config)
	case "hashLookup":
		return method.Outputs.Pack(c.hashLookupSubmitted, c.hashLookupConfirmations)
	default:
		return nil, fmt.Errorf("unexpected method %s", method.Name)
	}
}

func defaultReceiveUlnConfig() receiveUlnConfig {
	return receiveUlnConfig{
		Confirmations:        12,
		RequiredDVNCount:     2,
		OptionalDVNCount:     nilDVNCount,
		OptionalDVNThreshold: 0,
		RequiredDVNs: []common.Address{
			common.HexToAddress("0x6666666666666666666666666666666666666666"),
			common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd"),
		},
	}
}

func dvnMethodBySelector(data []byte) (dvnMethodView, error) {
	if len(data) < 4 {
		return dvnMethodView{}, fmt.Errorf("call data length %d is shorter than selector", len(data))
	}
	for _, method := range endpointViewABI.Methods {
		if string(method.ID) == string(data[:4]) {
			return dvnMethodView{Name: method.Name, Outputs: method.Outputs}, nil
		}
	}
	for _, method := range receiveUlnViewABI.Methods {
		if string(method.ID) == string(data[:4]) {
			return dvnMethodView{Name: method.Name, Outputs: method.Outputs}, nil
		}
	}
	return dvnMethodView{}, fmt.Errorf("unknown selector %x", data[:4])
}

type dvnMethodView struct {
	Name    string
	Outputs interface {
		Pack(args ...any) ([]byte, error)
	}
}

func testDVNPacket() db.PacketRecord {
	encodedPacket := testEncodedPacket()
	return db.PacketRecord{
		GUID:           common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
		SrcEID:         40161,
		DstEID:         40449,
		Nonce:          big.NewInt(7),
		Sender:         common.HexToAddress("0x7777777777777777777777777777777777777777"),
		Receiver:       common.HexToAddress("0x8888888888888888888888888888888888888888"),
		SendLib:        common.HexToAddress("0x9999999999999999999999999999999999999999"),
		SrcTxHash:      common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		SrcBlockNumber: 123,
		SrcLogIndex:    4,
		EncodedPacket:  encodedPacket,
		PacketHeader:   encodedPacket[:81],
		Message:        encodedPacket[81:],
		PayloadHash:    crypto.Keccak256Hash(encodedPacket[81:]),
		Options:        []byte{0x07, 0x08},
		Status:         string(packets.ExecutorNew),
	}
}

func testSourceReceipt(t *testing.T, packet db.PacketRecord) *gethtypes.Receipt {
	t.Helper()
	eventABI := lzabi.EndpointV2ABI()
	data, err := eventABI.Events["PacketSent"].Inputs.Pack(packet.EncodedPacket, packet.Options, packet.SendLib)
	if err != nil {
		t.Fatalf("Pack PacketSent error = %v", err)
	}
	log := &gethtypes.Log{
		Address:     common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Topics:      []common.Hash{lzabi.PacketSentTopic()},
		Data:        data,
		TxHash:      packet.SrcTxHash,
		BlockNumber: packet.SrcBlockNumber,
		BlockHash:   common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
		Index:       packet.SrcLogIndex,
	}
	return &gethtypes.Receipt{
		TxHash:      packet.SrcTxHash,
		Status:      gethtypes.ReceiptStatusSuccessful,
		BlockNumber: new(big.Int).SetUint64(packet.SrcBlockNumber),
		Logs:        []*gethtypes.Log{log},
	}
}

func testEncodedPacket() []byte {
	encoded := make([]byte, 0, 118)
	encoded = append(encoded, 1)
	encoded = binary.BigEndian.AppendUint64(encoded, 7)
	encoded = binary.BigEndian.AppendUint32(encoded, 40161)
	encoded = append(encoded, addressToBytes32(common.HexToAddress("0x7777777777777777777777777777777777777777"))...)
	encoded = binary.BigEndian.AppendUint32(encoded, 40449)
	encoded = append(encoded, addressToBytes32(common.HexToAddress("0x8888888888888888888888888888888888888888"))...)
	encoded = append(encoded, common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc").Bytes()...)
	encoded = append(encoded, []byte("hello")...)
	return encoded
}

func addressToBytes32(address common.Address) []byte {
	out := make([]byte, 32)
	copy(out[12:], address.Bytes())
	return out
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func captureLogger(level slog.Leveler) (*slog.Logger, *bytes.Buffer) {
	var logs bytes.Buffer
	return slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: level})), &logs
}

func assertLogContains(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Fatalf("logs missing %q in:\n%s", want, output)
		}
	}
}

func TestProcessQuorumOnceReorgCoordinateWriteFailureIsRetried(t *testing.T) {
	packet := testDVNPacket()
	receipt := testSourceReceipt(t, packet)
	receipt.BlockNumber = new(big.Int).SetUint64(packet.SrcBlockNumber + 3)
	receipt.Logs[0].BlockNumber = packet.SrcBlockNumber + 3
	receipt.Logs[0].Index = packet.SrcLogIndex + 5
	store := &fakeStore{
		work: []db.DVNWorkItem{{
			Packet: packet,
			Job: db.DVNJobRecord{
				GUID:                  packet.GUID,
				ConfirmationsRequired: 12,
				Status:                string(packets.DVNQuorumChecking),
			},
		}},
		reorgCoordsErr: fmt.Errorf("coordinates write lost"),
	}
	worker := NewWithClients(
		store,
		map[uint32]HeadReader{packet.SrcEID: fakeHead{head: packet.SrcBlockNumber + 12}},
		map[uint32]ReceiptReader{packet.SrcEID: fakeReceiptReader{receipt: receipt}},
		discardLogger(),
	)

	// The transition and the coordinate refresh commit atomically: a failed
	// write changes nothing and the pass surfaces the error for a retry.
	if _, err := worker.ProcessQuorumOnce(context.Background()); err == nil {
		t.Fatal("ProcessQuorumOnce() error = nil, want the atomic write failure")
	}
	if store.reorgGUID != (common.Hash{}) || store.reorgCoords != nil {
		t.Fatal("nothing may persist when the atomic reorg write failed")
	}
}
