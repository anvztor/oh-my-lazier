package executor

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

func TestProcessCommitterOnceEnqueuesCommitTx(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorVerifiable)
	logger, logs := captureLogger(slog.LevelInfo)
	store := &fakeStore{
		work: []db.ExecutorWorkItem{{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorVerifiable)},
		}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeCommitReadyCaller{}},
		logger,
	)

	processed, err := worker.ProcessCommitterOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessCommitterOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorVerifiable) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorVerifiable)
	}
	if store.nextStatus != string(packets.ExecutorCommitTxEnqueued) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorCommitTxEnqueued)
	}
	if store.request.Purpose != TxPurposeCommitVerification {
		t.Fatalf("purpose = %q, want %q", store.request.Purpose, TxPurposeCommitVerification)
	}
	if store.request.To != common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") {
		t.Fatalf("to = %s, want receive lib", store.request.To)
	}
	if store.request.SignerID != "0x8888888888888888888888888888888888888888" {
		t.Fatalf("signer = %q, want destination executor signer", store.request.SignerID)
	}
	assertLogContains(t, logs.String(),
		`msg="enqueued executor commit tx"`,
		`guid=0x`,
		`src_eid=40161`,
		`dst_eid=40449`,
		`from_status=VERIFIABLE`,
		`to_status=COMMIT_TX_ENQUEUED`,
		`tx_outbox_id=123`,
	)
}

func TestProcessCommitterOnceSkipsInactiveSendScope(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorVerifiable)
	logger, logs := captureLogger(slog.LevelDebug)
	store := &fakeStore{
		work: []db.ExecutorWorkItem{{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorVerifiable)},
		}},
		enqueueErr: db.ErrTxSendScopeInactive,
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeCommitReadyCaller{}},
		logger,
	)

	// A pause committed between work selection and the enqueue is a normal
	// skip: no error, no job transition, resumed after unpause.
	processed, err := worker.ProcessCommitterOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessCommitterOnce() error = %v, want skipped without error", err)
	}
	if processed {
		t.Fatal("processed = true, want false for an inactive send scope")
	}
	if store.nextStatus != "" {
		t.Fatalf("job transitioned to %q, want untouched", store.nextStatus)
	}
	assertLogContains(t, logs.String(),
		`msg="skipped executor commit tx enqueue"`,
		`reason=send_scope_inactive`,
	)
}

func TestProcessCommitterOnceMarksAssignedWaitingWhenNotVerifiable(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorAssigned)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorAssigned): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorAssigned)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeCommitNotReadyCaller{}},
		slog.Default(),
	)

	processed, err := worker.ProcessCommitterOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessCommitterOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorAssigned) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorAssigned)
	}
	if store.nextStatus != string(packets.ExecutorWaitingDVNVerification) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorWaitingDVNVerification)
	}
	if store.request.Purpose != "" {
		t.Fatalf("unexpected enqueue purpose %q", store.request.Purpose)
	}
}

func TestProcessCommitterOnceMarksAssignedVerifiable(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorAssigned)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorAssigned): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorAssigned)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeCommitReadyCaller{}},
		slog.Default(),
	)

	processed, err := worker.ProcessCommitterOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessCommitterOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorAssigned) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorAssigned)
	}
	if store.nextStatus != string(packets.ExecutorVerifiable) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorVerifiable)
	}
}

func TestProcessCommitterOnceMarksWaitingVerifiable(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorWaitingDVNVerification)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorWaitingDVNVerification): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorWaitingDVNVerification)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeCommitReadyCaller{}},
		slog.Default(),
	)

	processed, err := worker.ProcessCommitterOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessCommitterOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorWaitingDVNVerification) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorWaitingDVNVerification)
	}
	if store.nextStatus != string(packets.ExecutorVerifiable) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorVerifiable)
	}
}

func TestProcessCommitterOnceMarksCommittedWhenEndpointAlreadyHasPayload(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorVerifiable)
	store := &fakeStore{
		work: []db.ExecutorWorkItem{{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorVerifiable)},
		}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeCommitAlreadyCommittedCaller{payloadHash: packet.PayloadHash}},
		slog.Default(),
	)

	processed, err := worker.ProcessCommitterOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessCommitterOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorVerifiable) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorVerifiable)
	}
	if store.nextStatus != string(packets.ExecutorCommitted) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorCommitted)
	}
	if store.request.Purpose != "" {
		t.Fatalf("unexpected enqueue purpose %q", store.request.Purpose)
	}
}

func TestProcessCommitterOnceDefersPathwayLookupError(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorVerifiable)
	packet.Sender = common.HexToAddress("0x1212121212121212121212121212121212121212")
	store := &fakeStore{
		work: []db.ExecutorWorkItem{{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorVerifiable)},
		}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeCommitReadyCaller{}},
		slog.Default(),
	)

	processed, err := worker.ProcessCommitterOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessCommitterOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.deferredGUID != packet.GUID {
		t.Fatalf("deferred guid = %s, want %s", store.deferredGUID, packet.GUID)
	}
	if store.deferredStatus != string(packets.ExecutorVerifiable) {
		t.Fatalf("deferred status = %q, want %q", store.deferredStatus, packets.ExecutorVerifiable)
	}
}

func TestIsCommitVerifiableRejectsEmptyPayloadHash(t *testing.T) {
	packet := testPacketRecord()
	packet.PayloadHash = common.Hash{}

	ready, err := IsCommitVerifiable(
		context.Background(),
		failingCaller{},
		common.HexToAddress("0x4444444444444444444444444444444444444444"),
		common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		packet,
	)
	if err == nil {
		t.Fatal("IsCommitVerifiable() error = nil, want payload hash validation error")
	}
	if ready {
		t.Fatal("ready = true, want false")
	}
}

func TestProcessDelivererOnceMarksCommittedExecutable(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorCommitted)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorCommitted): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorCommitted)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeExecutableCaller{payloadHash: packet.PayloadHash, inboundNonce: 7}},
		slog.Default(),
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorCommitted) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorCommitted)
	}
	if store.nextStatus != string(packets.ExecutorExecutable) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorExecutable)
	}
	if store.request.Purpose != "" {
		t.Fatalf("unexpected enqueue purpose %q", store.request.Purpose)
	}
}

func TestProcessDelivererOnceEnqueuesLzReceiveTx(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorExecutable)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorExecutable): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorExecutable)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeExecutableCaller{payloadHash: packet.PayloadHash, inboundNonce: 7}},
		slog.Default(),
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorExecutable) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorExecutable)
	}
	if store.nextStatus != string(packets.ExecutorLzReceiveTxEnqueued) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorLzReceiveTxEnqueued)
	}
	if store.request.Purpose != TxPurposeLzReceive {
		t.Fatalf("purpose = %q, want %q", store.request.Purpose, TxPurposeLzReceive)
	}
	if store.request.To != common.HexToAddress("0x4444444444444444444444444444444444444444") {
		t.Fatalf("to = %s, want destination endpoint", store.request.To)
	}
	if store.request.SignerID != "0x8888888888888888888888888888888888888888" {
		t.Fatalf("signer = %q, want destination executor signer", store.request.SignerID)
	}
}

func TestProcessDelivererOnceRetriesFailedLzReceive(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorLzReceiveFailed)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorLzReceiveFailed): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorLzReceiveFailed), LastError: "previous alert"},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeExecutableCaller{payloadHash: packet.PayloadHash, inboundNonce: 7}},
		slog.Default(),
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorLzReceiveFailed) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorLzReceiveFailed)
	}
	if store.nextStatus != string(packets.ExecutorLzReceiveTxEnqueued) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorLzReceiveTxEnqueued)
	}
	if store.request.Purpose != TxPurposeLzReceive {
		t.Fatalf("purpose = %q, want %q", store.request.Purpose, TxPurposeLzReceive)
	}
}

func TestProcessDelivererOnceParksLzReceiveAfterRetryBudget(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorLzReceiveFailed)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorLzReceiveFailed): {{
			Packet: packet,
			Job: db.ExecutorJobRecord{
				GUID:       packet.GUID,
				Status:     string(packets.ExecutorLzReceiveFailed),
				RetryCount: db.MaxLzReceiveDeliveryAttempts,
			},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeExecutableCaller{payloadHash: packet.PayloadHash, inboundNonce: 7}},
		slog.Default(),
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.nextStatus != string(packets.ExecutorManualReview) {
		t.Fatalf("next status = %q, want %q (retry budget exhausted must park, not re-enqueue)", store.nextStatus, packets.ExecutorManualReview)
	}
	if store.request.Purpose == TxPurposeLzReceive {
		t.Fatal("a new lzReceive tx was enqueued after the retry budget was exhausted")
	}
}

func TestProcessDelivererOnceMarksInvalidLzReceiveOptionsForManualReview(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorExecutable)
	packet.Options[5] = 2
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorExecutable): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorExecutable)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeExecutableCaller{payloadHash: packet.PayloadHash, inboundNonce: 7}},
		slog.Default(),
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.guid != packet.GUID || store.expectedStatus != string(packets.ExecutorExecutable) || store.nextStatus != string(packets.ExecutorManualReview) {
		t.Fatalf("manual review transition = %s/%q/%q, want %s/%q/%q", store.guid, store.expectedStatus, store.nextStatus, packet.GUID, packets.ExecutorExecutable, packets.ExecutorManualReview)
	}
	if !strings.Contains(store.lastError, "unsupported native drop executor option") {
		t.Fatalf("manual review error = %q, want unsupported native drop executor option", store.lastError)
	}
	if store.request.Purpose != "" {
		t.Fatalf("unexpected enqueue purpose %q", store.request.Purpose)
	}
}

func TestProcessDelivererOnceSkipsWhenEndpointNotExecutable(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorExecutable)
	logger, logs := captureLogger(slog.LevelDebug)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorExecutable): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorExecutable)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeExecutableCaller{payloadHash: packet.PayloadHash, inboundNonce: 6}},
		logger,
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.deferredGUID != packet.GUID {
		t.Fatalf("deferred guid = %s, want %s", store.deferredGUID, packet.GUID)
	}
	if store.deferredStatus != string(packets.ExecutorExecutable) {
		t.Fatalf("deferred status = %q, want %q", store.deferredStatus, packets.ExecutorExecutable)
	}
	if store.request.Purpose != "" {
		t.Fatalf("unexpected enqueue purpose %q", store.request.Purpose)
	}
	assertLogContains(t, logs.String(),
		`level=DEBUG`,
		`msg="skipped executor delivery workflow"`,
		`reason=delivery_not_executable`,
		`guid=0x`,
		`src_eid=40161`,
		`dst_eid=40449`,
		`status=EXECUTABLE`,
		`delivery_state=not_executable`,
	)
}

func TestProcessDelivererOnceMarksDeliveredWhenEndpointPayloadCleared(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorExecutable)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorExecutable): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorExecutable)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeExecutableCaller{payloadHash: common.Hash{}, inboundNonce: 7, lazyInboundNonce: 7}},
		slog.Default(),
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.expectedStatus != string(packets.ExecutorExecutable) {
		t.Fatalf("expected status = %q, want %q", store.expectedStatus, packets.ExecutorExecutable)
	}
	if store.nextStatus != string(packets.ExecutorDelivered) {
		t.Fatalf("next status = %q, want %q", store.nextStatus, packets.ExecutorDelivered)
	}
	if store.request.Purpose != "" {
		t.Fatalf("unexpected enqueue purpose %q", store.request.Purpose)
	}
}

func TestProcessDelivererOnceDefersDeliveryBelowConfirmationDepth(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorExecutable)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorExecutable): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorExecutable)},
		}}},
	}
	// The endpoint reports delivered at latest, but the head is below the chain's
	// confirmation depth (12), so the terminal DELIVERED state must not be written.
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: fakeExecutableCaller{payloadHash: common.Hash{}, inboundNonce: 7, lazyInboundNonce: 7, headBlock: 5}},
		slog.Default(),
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.nextStatus == string(packets.ExecutorDelivered) {
		t.Fatal("marked DELIVERED from a shallow (below-confirmation-depth) observation")
	}
	if store.deferredGUID != packet.GUID {
		t.Fatalf("deferred guid = %s, want %s", store.deferredGUID, packet.GUID)
	}
}

func TestProcessDelivererOnceDefersReadinessError(t *testing.T) {
	packet := testPacketRecord()
	packet.Status = string(packets.ExecutorExecutable)
	store := &fakeStore{
		workByStatus: map[string][]db.ExecutorWorkItem{string(packets.ExecutorExecutable): {{
			Packet: packet,
			Job:    db.ExecutorJobRecord{GUID: packet.GUID, Status: string(packets.ExecutorExecutable)},
		}}},
	}
	worker := NewWithCallers(
		store,
		testRegistry(t),
		map[uint32]ContractCaller{packet.DstEID: failingCaller{}},
		slog.Default(),
	)

	processed, err := worker.ProcessDelivererOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessDelivererOnce() error = %v", err)
	}
	if !processed {
		t.Fatal("processed = false, want true")
	}
	if store.deferredGUID != packet.GUID {
		t.Fatalf("deferred guid = %s, want %s", store.deferredGUID, packet.GUID)
	}
	if store.deferredStatus != string(packets.ExecutorExecutable) {
		t.Fatalf("deferred status = %q, want %q", store.deferredStatus, packets.ExecutorExecutable)
	}
	if store.request.Purpose != "" {
		t.Fatalf("unexpected enqueue purpose %q", store.request.Purpose)
	}
}

func TestCheckDeliveryStateKeepsPayloadExecutableWhenLazyNonceAdvanced(t *testing.T) {
	packet := testPacketRecord()
	state, err := CheckDeliveryState(
		context.Background(),
		fakeExecutableCaller{payloadHash: packet.PayloadHash, inboundNonce: packet.Nonce.Uint64(), lazyInboundNonce: packet.Nonce.Uint64()},
		common.HexToAddress("0x4444444444444444444444444444444444444444"),
		packet,
		nil,
	)
	if err != nil {
		t.Fatalf("CheckDeliveryState() error = %v", err)
	}
	if state != DeliveryExecutable {
		t.Fatalf("delivery state = %v, want %v", state, DeliveryExecutable)
	}
}

type fakeStore struct {
	work           []db.ExecutorWorkItem
	workByStatus   map[string][]db.ExecutorWorkItem
	guid           common.Hash
	expectedStatus string
	nextStatus     string
	request        db.TxRequest
	lastError      string
	deferredGUID   common.Hash
	deferredStatus string
	enqueueErr     error
}

func (s *fakeStore) ListExecutorWork(_ context.Context, status string, _ int) ([]db.ExecutorWorkItem, error) {
	if s.workByStatus != nil {
		return s.workByStatus[status], nil
	}
	var out []db.ExecutorWorkItem
	for _, item := range s.work {
		if item.Job.Status == status {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *fakeStore) MarkExecutorWaitingDVNVerification(_ context.Context, guid common.Hash, expectedStatus string) error {
	s.guid = guid
	s.expectedStatus = expectedStatus
	s.nextStatus = string(packets.ExecutorWaitingDVNVerification)
	return nil
}

func (s *fakeStore) MarkExecutorVerifiable(_ context.Context, guid common.Hash, expectedStatus string) error {
	s.guid = guid
	s.expectedStatus = expectedStatus
	s.nextStatus = string(packets.ExecutorVerifiable)
	return nil
}

func (s *fakeStore) MarkExecutorCommittedFromChain(_ context.Context, guid common.Hash, expectedStatus string) error {
	s.guid = guid
	s.expectedStatus = expectedStatus
	s.nextStatus = string(packets.ExecutorCommitted)
	return nil
}

func (s *fakeStore) MarkExecutorExecutable(_ context.Context, guid common.Hash) error {
	s.guid = guid
	s.expectedStatus = string(packets.ExecutorCommitted)
	s.nextStatus = string(packets.ExecutorExecutable)
	return nil
}

func (s *fakeStore) MarkExecutorManualReview(_ context.Context, guid common.Hash, expectedStatus, reason string) error {
	s.guid = guid
	s.expectedStatus = expectedStatus
	s.nextStatus = string(packets.ExecutorManualReview)
	s.lastError = reason
	return nil
}

func (s *fakeStore) MarkExecutorDeliveredFromChain(_ context.Context, guid common.Hash, expectedStatus string) error {
	s.guid = guid
	s.expectedStatus = expectedStatus
	s.nextStatus = string(packets.ExecutorDelivered)
	return nil
}

func (s *fakeStore) EnqueueExecutorTx(_ context.Context, guid common.Hash, expectedStatus, nextStatus string, request db.TxRequest) (int64, error) {
	if s.enqueueErr != nil {
		return 0, s.enqueueErr
	}
	s.guid = guid
	s.expectedStatus = expectedStatus
	s.nextStatus = nextStatus
	s.request = request
	return 123, nil
}

func (s *fakeStore) DeferExecutorJob(_ context.Context, guid common.Hash, expectedStatus string, _ time.Duration) error {
	s.deferredGUID = guid
	s.deferredStatus = expectedStatus
	return nil
}

// testAnchorHash is the fixed verified-head hash the fake callers report.
const testAnchorHash = "0x00000000000000000000000000000000000000000000000000000000000f4240"

type failingCaller struct{}

func (failingCaller) CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error) {
	return nil, fmt.Errorf("unexpected eth_call")
}

func (failingCaller) BlockNumber(context.Context) (uint64, error) {
	return 0, fmt.Errorf("unexpected block number")
}

func (failingCaller) CallContractAtHash(context.Context, ethereum.CallMsg, common.Hash) ([]byte, error) {
	return nil, fmt.Errorf("unexpected eth_call at hash")
}

func (failingCaller) CheckHead(context.Context) (rpcquorum.HeadResult, error) {
	return rpcquorum.HeadResult{}, fmt.Errorf("unexpected head check")
}

type fakeCommitReadyCaller struct{}

func (fakeCommitReadyCaller) BlockNumber(context.Context) (uint64, error) { return 1_000_000, nil }

func (fakeCommitReadyCaller) CheckHead(context.Context) (rpcquorum.HeadResult, error) {
	return rpcquorum.HeadResult{Number: big.NewInt(1_000_000), Hash: testAnchorHash}, nil
}

func (c fakeCommitReadyCaller) CallContractAtHash(ctx context.Context, call ethereum.CallMsg, _ common.Hash) ([]byte, error) {
	return c.CallContract(ctx, call, nil)
}

func (fakeCommitReadyCaller) CallContract(_ context.Context, call ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	method, err := methodBySelector(call.Data)
	if err != nil {
		return nil, err
	}
	switch method.Name {
	case "inboundPayloadHash":
		return method.Outputs.Pack(common.Hash{})
	case "isValidReceiveLibrary", "verifiable":
		return method.Outputs.Pack(true)
	case "getUlnConfig":
		return method.Outputs.Pack(ulnConfig{
			Confirmations:        12,
			RequiredDVNCount:     1,
			OptionalDVNCount:     0,
			OptionalDVNThreshold: 0,
			RequiredDVNs:         []common.Address{common.HexToAddress("0x3333333333333333333333333333333333333333")},
		})
	default:
		return nil, fmt.Errorf("unexpected method %s", method.Name)
	}
}

type fakeCommitAlreadyCommittedCaller struct {
	payloadHash common.Hash
}

func (fakeCommitAlreadyCommittedCaller) BlockNumber(context.Context) (uint64, error) {
	return 1_000_000, nil
}

func (fakeCommitAlreadyCommittedCaller) CheckHead(context.Context) (rpcquorum.HeadResult, error) {
	return rpcquorum.HeadResult{Number: big.NewInt(1_000_000), Hash: testAnchorHash}, nil
}

func (c fakeCommitAlreadyCommittedCaller) CallContractAtHash(ctx context.Context, call ethereum.CallMsg, _ common.Hash) ([]byte, error) {
	return c.CallContract(ctx, call, nil)
}

func (c fakeCommitAlreadyCommittedCaller) CallContract(_ context.Context, call ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	method, err := methodBySelector(call.Data)
	if err != nil {
		return nil, err
	}
	switch method.Name {
	case "inboundPayloadHash":
		return method.Outputs.Pack(c.payloadHash)
	default:
		return nil, fmt.Errorf("unexpected method %s", method.Name)
	}
}

type fakeCommitNotReadyCaller struct{}

func (fakeCommitNotReadyCaller) BlockNumber(context.Context) (uint64, error) { return 1_000_000, nil }

func (fakeCommitNotReadyCaller) CheckHead(context.Context) (rpcquorum.HeadResult, error) {
	return rpcquorum.HeadResult{Number: big.NewInt(1_000_000), Hash: testAnchorHash}, nil
}

func (c fakeCommitNotReadyCaller) CallContractAtHash(ctx context.Context, call ethereum.CallMsg, _ common.Hash) ([]byte, error) {
	return c.CallContract(ctx, call, nil)
}

func (fakeCommitNotReadyCaller) CallContract(_ context.Context, call ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	method, err := methodBySelector(call.Data)
	if err != nil {
		return nil, err
	}
	switch method.Name {
	case "inboundPayloadHash":
		return method.Outputs.Pack(common.Hash{})
	case "isValidReceiveLibrary":
		return method.Outputs.Pack(true)
	case "verifiable":
		return method.Outputs.Pack(false)
	default:
		return nil, fmt.Errorf("unexpected method %s", method.Name)
	}
}

type fakeExecutableCaller struct {
	payloadHash      common.Hash
	inboundNonce     uint64
	lazyInboundNonce uint64
	headBlock        uint64
}

func (c fakeExecutableCaller) BlockNumber(context.Context) (uint64, error) {
	if c.headBlock != 0 {
		return c.headBlock, nil
	}
	return 1_000_000, nil
}

func (c fakeExecutableCaller) CheckHead(ctx context.Context) (rpcquorum.HeadResult, error) {
	head, err := c.BlockNumber(ctx)
	if err != nil {
		return rpcquorum.HeadResult{}, err
	}
	return rpcquorum.HeadResult{Number: new(big.Int).SetUint64(head), Hash: testAnchorHash}, nil
}

func (c fakeExecutableCaller) CallContractAtHash(ctx context.Context, call ethereum.CallMsg, _ common.Hash) ([]byte, error) {
	return c.CallContract(ctx, call, nil)
}

func (c fakeExecutableCaller) CallContract(_ context.Context, call ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	method, err := methodBySelector(call.Data)
	if err != nil {
		return nil, err
	}
	switch method.Name {
	case "inboundPayloadHash":
		return method.Outputs.Pack(c.payloadHash)
	case "inboundNonce":
		return method.Outputs.Pack(c.inboundNonce)
	case "lazyInboundNonce":
		return method.Outputs.Pack(c.lazyInboundNonce)
	default:
		return nil, fmt.Errorf("unexpected method %s", method.Name)
	}
}

func methodBySelector(data []byte) (methodView, error) {
	if len(data) < 4 {
		return methodView{}, fmt.Errorf("call data length %d is shorter than selector", len(data))
	}
	for _, method := range endpointViewABI.Methods {
		if string(method.ID) == string(data[:4]) {
			return methodView{Name: method.Name, Outputs: method.Outputs}, nil
		}
	}
	for _, method := range receiveUlnViewABI.Methods {
		if string(method.ID) == string(data[:4]) {
			return methodView{Name: method.Name, Outputs: method.Outputs}, nil
		}
	}
	return methodView{}, fmt.Errorf("unknown selector %x", data[:4])
}

type methodView struct {
	Name    string
	Outputs interface {
		Pack(args ...any) ([]byte, error)
	}
}

func testRegistry(t *testing.T) *chain.Registry {
	t.Helper()
	registry, err := chain.NewRegistry([]config.ChainConfig{
		{
			EID:             40161,
			Name:            "ethereum-sepolia",
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
			EID:             40449,
			Name:            "hoodi",
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
	}, []config.PathwayConfig{
		{
			SrcEID:     40161,
			DstEID:     40449,
			SrcOApp:    config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
			DstOApp:    config.MustEVMAddress("0x2222222222222222222222222222222222222222"),
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
	})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	return registry
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
