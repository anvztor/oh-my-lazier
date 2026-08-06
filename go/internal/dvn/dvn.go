package dvn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"reflect"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/indexer"
	"github.com/islishude/oh-my-lazier/go/internal/lzabi"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
	"github.com/islishude/oh-my-lazier/go/internal/workerloop"
)

const loopInterval = 5 * time.Second

var errDestinationVerificationConfigMismatch = errors.New("destination verification config mismatch")

const (
	// TxPurposeVerify identifies OpenDVN.submitVerification outbox requests.
	TxPurposeVerify = "dvn_verify"

	nilDVNCount = uint8(255)
)

var (
	endpointViewABI   = lzabi.EndpointV2ABI()
	receiveUlnViewABI = lzabi.ReceiveUln302ABI()
)

// Store is the durable DVN state required by the worker.
type Store interface {
	ListDVNWork(ctx context.Context, status string, limit int) ([]db.DVNWorkItem, error)
	MarkDVNWaitingConfirmations(ctx context.Context, guid common.Hash, expectedStatus string) error
	MarkDVNQuorumChecking(ctx context.Context, guid common.Hash, expectedStatus string) error
	MarkDVNReadyToVerify(ctx context.Context, guid common.Hash, expectedStatus string, quorumResult []byte) error
	MarkDVNWouldVerify(ctx context.Context, guid common.Hash, expectedStatus string, quorumResult []byte) error
	EnqueueDVNVerifyTx(ctx context.Context, guid common.Hash, expectedStatus, nextStatus string, request db.TxRequest, quorumResult []byte) (int64, error)
	MarkDVNVerifiedFromChain(ctx context.Context, guid common.Hash, expectedStatus string, quorumResult []byte) error
	MarkDVNQuorumConflict(ctx context.Context, guid common.Hash, expectedStatus, reason string, quorumResult []byte) error
	MarkDVNReorgDetected(ctx context.Context, guid common.Hash, expectedStatus, reason string, quorumResult []byte) error
	MarkDVNManualReviewAndPausePathway(ctx context.Context, guid common.Hash, expectedStatus, reason string, quorumResult []byte) error
	DeferDVNJob(ctx context.Context, guid common.Hash, expectedStatus string, delay time.Duration) error
	PauseChain(ctx context.Context, eid uint32) error
	PausePathwayForPacket(ctx context.Context, guid common.Hash) error
	MarkDVNReorgDetectedWithCoordinates(ctx context.Context, guid common.Hash, expectedStatus, reason string, quorumResult []byte, srcBlockNumber uint64, srcLogIndex uint, expectedSrcBlockNumber uint64, expectedSrcLogIndex uint) error
}

// HeadReader reads a source chain head.
type HeadReader interface {
	CheckHead(ctx context.Context) (rpcquorum.HeadResult, error)
}

// ReceiptReader reads source-chain transaction receipts.
type ReceiptReader interface {
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*gethtypes.Receipt, error)
}

// ContractCaller is the anchor-routed eth_call surface used by
// destination-chain reconciliation.
type ContractCaller interface {
	CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	CallContractAtHash(ctx context.Context, call ethereum.CallMsg, blockHash common.Hash) ([]byte, error)
}

// Settings controls active DVN verification transaction generation.
type Settings struct {
	SignerID string
}

// Worker runs the DVN verification workflow.
type Worker struct {
	store    Store
	registry *chain.Registry
	settings map[uint32]Settings
	heads    map[uint32]HeadReader
	receipts map[uint32]ReceiptReader
	callers  map[uint32]ContractCaller
	logger   *slog.Logger
}

// New creates a DVN worker.
func New(store Store, registry *chain.Registry, logger *slog.Logger) *Worker {
	return NewWithSettings(store, registry, nil, logger)
}

// NewWithSettings creates a DVN worker with explicit active-mode transaction settings by destination EID.
func NewWithSettings(store Store, registry *chain.Registry, settings map[uint32]Settings, logger *slog.Logger) *Worker {
	heads := make(map[uint32]HeadReader)
	receipts := make(map[uint32]ReceiptReader)
	callers := make(map[uint32]ContractCaller)
	if registry != nil {
		for _, configuredChain := range registry.All() {
			if configuredChain.RPC != nil {
				heads[configuredChain.EID] = configuredChain.RPC
				receipts[configuredChain.EID] = configuredChain.RPC
				callers[configuredChain.EID] = configuredChain.RPC
			}
		}
	}
	return NewWithClientsSettingsAndCallers(store, registry, settings, heads, receipts, callers, logger)
}

// NewWithHeads creates a DVN worker with explicit head readers for tests.
func NewWithHeads(store Store, heads map[uint32]HeadReader, logger *slog.Logger) *Worker {
	return NewWithClients(store, heads, nil, logger)
}

// NewWithClients creates a DVN worker with explicit source-chain clients for tests.
func NewWithClients(store Store, heads map[uint32]HeadReader, receipts map[uint32]ReceiptReader, logger *slog.Logger) *Worker {
	return NewWithClientsAndSettings(store, nil, nil, heads, receipts, logger)
}

// NewWithClientsAndSettings creates a DVN worker with explicit clients and active-mode settings for tests.
func NewWithClientsAndSettings(store Store, registry *chain.Registry, settings map[uint32]Settings, heads map[uint32]HeadReader, receipts map[uint32]ReceiptReader, logger *slog.Logger) *Worker {
	return NewWithClientsSettingsAndCallers(store, registry, settings, heads, receipts, nil, logger)
}

// NewWithClientsSettingsAndCallers creates a DVN worker with explicit clients and destination callers for tests.
func NewWithClientsSettingsAndCallers(store Store, registry *chain.Registry, settings map[uint32]Settings, heads map[uint32]HeadReader, receipts map[uint32]ReceiptReader, callers map[uint32]ContractCaller, logger *slog.Logger) *Worker {
	copiedHeads := make(map[uint32]HeadReader, len(heads))
	maps.Copy(copiedHeads, heads)
	copiedReceipts := make(map[uint32]ReceiptReader, len(receipts))
	maps.Copy(copiedReceipts, receipts)
	copiedCallers := make(map[uint32]ContractCaller, len(callers))
	maps.Copy(copiedCallers, callers)
	copiedSettings := make(map[uint32]Settings, len(settings))
	maps.Copy(copiedSettings, settings)
	return &Worker{store: store, registry: registry, settings: copiedSettings, heads: copiedHeads, receipts: copiedReceipts, callers: copiedCallers, logger: logger}
}

// Run starts the DVN verifier loop until the context is canceled.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("dvn verifier loop started")
	for {
		processed, err := w.ProcessConfirmationsOnce(ctx)
		if err != nil {
			return err
		}
		if !processed {
			processed, err = w.ProcessQuorumOnce(ctx)
			if err != nil {
				return err
			}
		}
		if !processed {
			processed, err = w.ProcessReadyToVerifyOnce(ctx)
			if err != nil {
				return err
			}
		}
		if processed {
			continue
		}
		timer := time.NewTimer(loopInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// ProcessConfirmationsOnce advances one DVN job once source confirmations are available.
func (w *Worker) ProcessConfirmationsOnce(ctx context.Context) (bool, error) {
	for _, status := range []string{string(packets.DVNReorgDetected), string(packets.DVNAssigned), string(packets.DVNWaitingConfirmations)} {
		work, err := w.store.ListDVNWork(ctx, status, 1)
		if err != nil {
			return false, err
		}
		if len(work) == 0 {
			continue
		}
		item := work[0]
		if status == string(packets.DVNReorgDetected) {
			if err := w.store.MarkDVNWaitingConfirmations(ctx, item.Packet.GUID, status); err != nil {
				return false, err
			}
			w.logger.Warn("dvn job rolled back after source reorg", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.DVNWaitingConfirmations))
			return true, nil
		}
		if err := w.validateActiveDestinationConfig(ctx, item.Packet, item.Job.ConfirmationsRequired); err != nil {
			if isDestinationVerificationConfigMismatch(err) {
				return w.markDestinationConfigMismatch(ctx, item, status, err)
			}
			return w.deferDVNWorkError(ctx, item, status, "destination_config_check_error", err)
		}
		headReader := w.head(item.Packet.SrcEID)
		if headReader == nil {
			return w.deferDVNWorkError(ctx, item, status, "missing_source_head_reader", fmt.Errorf("missing source head reader for eid %d", item.Packet.SrcEID))
		}
		headResult, err := headReader.CheckHead(ctx)
		if err != nil {
			if rpcquorum.IsHeadConflict(err) {
				if pauseErr := w.store.PauseChain(ctx, item.Packet.SrcEID); pauseErr != nil {
					return false, pauseErr
				}
				w.logger.Warn("dvn head quorum conflict paused chain", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status, "error", err.Error())
				return true, nil
			}
			return w.deferDVNWorkError(ctx, item, status, "source_head_error", err)
		}
		if headResult.Number == nil {
			return w.deferDVNWorkError(ctx, item, status, "missing_source_head_number", fmt.Errorf("source head result for eid %d is missing number", item.Packet.SrcEID))
		}
		head := headResult.Number.Uint64()
		if !hasRequiredConfirmations(item.Packet.SrcBlockNumber, item.Job.ConfirmationsRequired, head) {
			if status == string(packets.DVNAssigned) {
				if err := w.store.MarkDVNWaitingConfirmations(ctx, item.Packet.GUID, status); err != nil {
					return false, err
				}
				w.logger.Info("dvn job waiting for source confirmations", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.DVNWaitingConfirmations), "src_block_number", item.Packet.SrcBlockNumber, "observed_head_block", head, "confirmations_required", item.Job.ConfirmationsRequired)
			} else {
				if err := w.store.DeferDVNJob(ctx, item.Packet.GUID, status, loopInterval); err != nil {
					return false, err
				}
				w.logger.Debug("skipped dvn confirmations", "reason", "insufficient_confirmations", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status, "src_block_number", item.Packet.SrcBlockNumber, "observed_head_block", head, "confirmations_required", item.Job.ConfirmationsRequired)
			}
			return true, nil
		}
		if err := w.store.MarkDVNQuorumChecking(ctx, item.Packet.GUID, status); err != nil {
			return false, err
		}
		w.logger.Info("dvn job reached source confirmations", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.DVNQuorumChecking), "src_block_number", item.Packet.SrcBlockNumber, "observed_head_block", head, "confirmations_required", item.Job.ConfirmationsRequired)
		return true, nil
	}
	return false, nil
}

// ProcessQuorumOnce verifies one confirmed DVN job against source-chain receipt evidence.
func (w *Worker) ProcessQuorumOnce(ctx context.Context) (bool, error) {
	work, err := w.store.ListDVNWork(ctx, string(packets.DVNQuorumChecking), 1)
	if err != nil {
		return false, err
	}
	if len(work) == 0 {
		return false, nil
	}
	item := work[0]
	receiptReader := w.receipt(item.Packet.SrcEID)
	if receiptReader == nil {
		return w.deferDVNWorkError(ctx, item, string(packets.DVNQuorumChecking), "missing_source_receipt_reader", fmt.Errorf("missing source receipt reader for eid %d", item.Packet.SrcEID))
	}
	// A NotFound answer routes this job into reorg handling, so the receipt
	// read passes the indexed source block when the reader supports it:
	// providers still below that block cannot authoritatively deny the tx and
	// must not form the negative quorum.
	var receipt *gethtypes.Receipt
	if reader, ok := receiptReader.(interface {
		TransactionReceiptAt(ctx context.Context, txHash common.Hash, minBlock *big.Int) (*gethtypes.Receipt, error)
	}); ok {
		receipt, err = reader.TransactionReceiptAt(ctx, item.Packet.SrcTxHash, new(big.Int).SetUint64(item.Packet.SrcBlockNumber))
	} else {
		receipt, err = receiptReader.TransactionReceipt(ctx, item.Packet.SrcTxHash)
	}
	if err != nil {
		if rpcquorum.IsReceiptConflict(err) {
			return true, w.markQuorumConflict(ctx, item.Packet, err)
		}
		if errors.Is(err, ethereum.NotFound) {
			return true, w.markReorgDetected(ctx, item.Packet, err)
		}
		return w.deferDVNWorkError(ctx, item, string(packets.DVNQuorumChecking), "source_receipt_error", err)
	}
	endpoint := common.Address{}
	if w.registry != nil {
		srcChain, err := w.registry.Get(item.Packet.SrcEID)
		if err != nil {
			return w.deferDVNWorkError(ctx, item, string(packets.DVNQuorumChecking), "source_chain_lookup_error", err)
		}
		endpoint = srcChain.EndpointAddress
	}
	report, err := verifySourceReceiptForEndpoint(item.Packet, receipt, endpoint)
	if err != nil {
		if errors.Is(err, errSourceReceiptReorg) {
			// The forward-only source cursor normally never rescans the block
			// the tx was re-included at, so the stored packet coordinates must
			// be refreshed here or the confirmation gate re-checks the stale
			// block forever. The transition and the coordinate refresh commit
			// in ONE transaction: with two commits another instance can run
			// the whole reorg round trip in between (the old block is deep,
			// its gate passes instantly) and the late coordinates would land
			// on a QUORUM_CHECKING job, skipping the new block's gate.
			if updated, ok := reorgedPacketRecord(item.Packet, receipt, endpoint); ok {
				payload, marshalErr := json.Marshal(map[string]any{
					"tx_hash": item.Packet.SrcTxHash.Hex(),
					"error":   err.Error(),
				})
				if marshalErr != nil {
					return false, marshalErr
				}
				if reorgErr := w.store.MarkDVNReorgDetectedWithCoordinates(ctx, item.Packet.GUID, string(packets.DVNQuorumChecking), err.Error(), payload, updated.SrcBlockNumber, updated.SrcLogIndex, item.Packet.SrcBlockNumber, item.Packet.SrcLogIndex); reorgErr != nil {
					return false, reorgErr
				}
				w.logger.Warn("dvn source reorg detected; refreshed source coordinates", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "tx_hash", item.Packet.SrcTxHash, "old_block_number", item.Packet.SrcBlockNumber, "new_block_number", updated.SrcBlockNumber, "old_log_index", item.Packet.SrcLogIndex, "new_log_index", updated.SrcLogIndex)
				return true, nil
			}
			return true, w.markReorgDetected(ctx, item.Packet, err)
		}
		return true, w.markQuorumConflict(ctx, item.Packet, err)
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return false, err
	}
	if err := w.store.MarkDVNReadyToVerify(ctx, item.Packet.GUID, string(packets.DVNQuorumChecking), payload); err != nil {
		return false, err
	}
	w.logger.Info("dvn job ready to verify", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", string(packets.DVNQuorumChecking), "to_status", string(packets.DVNReadyToVerify))
	return true, nil
}

// ProcessReadyToVerifyOnce advances one verified DVN job into shadow report or active tx enqueue.
func (w *Worker) ProcessReadyToVerifyOnce(ctx context.Context) (bool, error) {
	work, err := w.store.ListDVNWork(ctx, string(packets.DVNReadyToVerify), 1)
	if err != nil {
		return false, err
	}
	if len(work) == 0 {
		return false, nil
	}
	item := work[0]
	if len(item.Job.QuorumResult) == 0 {
		return false, fmt.Errorf("dvn job %s ready to verify without quorum result", item.Packet.GUID)
	}
	if w.registry == nil {
		return false, workerloop.Fatal(errors.New("dvn verifier requires chain registry"))
	}
	pathway, err := w.registry.Pathway(item.Packet.SrcEID, item.Packet.DstEID, item.Packet.Sender, item.Packet.Receiver)
	if err != nil {
		return w.deferDVNWorkError(ctx, item, string(packets.DVNReadyToVerify), "pathway_lookup_error", err)
	}
	switch pathway.DVNMode {
	case config.DVNModeShadow:
		if err := w.store.MarkDVNWouldVerify(ctx, item.Packet.GUID, string(packets.DVNReadyToVerify), item.Job.QuorumResult); err != nil {
			return false, err
		}
		w.logger.Info("dvn shadow job would verify", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", string(packets.DVNReadyToVerify), "to_status", string(packets.DVNWouldVerify))
	case config.DVNModeActive:
		// One verified anchor covers this check's serial reads: re-establishing
		// the head quorum per nil-block read would charge a probe deadline per
		// read when a minority provider black-holes.
		latestAnchor, err := w.destinationAnchor(ctx, item.Packet.DstEID)
		if err != nil {
			return w.deferDVNWorkError(ctx, item, string(packets.DVNReadyToVerify), "destination_anchor_error", err)
		}
		complete, err := w.verificationAlreadyComplete(ctx, item.Packet, pathway, item.Job.ConfirmationsRequired, latestAnchor)
		if err != nil {
			if isDestinationVerificationConfigMismatch(err) {
				return w.markDestinationConfigMismatch(ctx, item, string(packets.DVNReadyToVerify), err)
			}
			return w.deferDVNWorkError(ctx, item, string(packets.DVNReadyToVerify), "verification_reconcile_error", err)
		}
		if complete {
			// Only write the terminal verified state once the on-chain verification
			// is buried under the destination confirmation depth, so a shallow
			// third-party verification cannot be reorged out after we stop verifying.
			confirmed, err := w.verificationConfirmedOnChain(ctx, item.Packet, pathway, item.Job.ConfirmationsRequired)
			if err != nil {
				return w.deferDVNWorkError(ctx, item, string(packets.DVNReadyToVerify), "verification_reconcile_error", err)
			}
			if !confirmed {
				if err := w.store.DeferDVNJob(ctx, item.Packet.GUID, string(packets.DVNReadyToVerify), loopInterval); err != nil {
					return false, err
				}
				w.logger.Debug("deferred dvn verification below confirmation depth", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", string(packets.DVNReadyToVerify))
				return true, nil
			}
			if err := w.store.MarkDVNVerifiedFromChain(ctx, item.Packet.GUID, string(packets.DVNReadyToVerify), item.Job.QuorumResult); err != nil {
				return false, err
			}
			w.logger.Info("dvn verification already completed on chain", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", string(packets.DVNReadyToVerify), "to_status", string(packets.DVNVerified))
			return true, nil
		}
		request, err := w.buildVerifyTx(item.Packet, pathway, item.Job.ConfirmationsRequired)
		if err != nil {
			return false, err
		}
		id, err := w.store.EnqueueDVNVerifyTx(ctx, item.Packet.GUID, string(packets.DVNReadyToVerify), string(packets.DVNVerifyTxEnqueued), request, item.Job.QuorumResult)
		if errors.Is(err, db.ErrTxSendScopeInactive) {
			// The pathway or chain was paused/disabled between work selection and
			// this enqueue; the job keeps its status and resumes after unpause.
			w.logger.Debug("skipped dvn verify tx enqueue", "reason", "send_scope_inactive", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID)
			return false, nil
		}
		if err != nil {
			return false, err
		}
		w.logger.Info("enqueued dvn verify tx", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", string(packets.DVNReadyToVerify), "to_status", string(packets.DVNVerifyTxEnqueued), "tx_outbox_id", id)
	default:
		return false, workerloop.Fatal(fmt.Errorf("unsupported dvn mode %q", pathway.DVNMode))
	}
	return true, nil
}

func (w *Worker) verificationAlreadyComplete(ctx context.Context, packet db.PacketRecord, pathway chain.Pathway, confirmations uint64, anchor *rpcquorum.StateAnchor) (bool, error) {
	if err := packet.Validate(); err != nil {
		return false, err
	}
	if confirmations == 0 {
		return false, errors.New("dvn confirmations required is required")
	}
	if w.registry == nil {
		return false, workerloop.Fatal(errors.New("dvn active mode requires chain registry"))
	}
	dstChain, err := w.registry.Get(packet.DstEID)
	if err != nil {
		return false, err
	}
	caller := w.caller(packet.DstEID)
	if caller == nil {
		return false, fmt.Errorf("missing destination caller for eid %d", packet.DstEID)
	}
	payloadHash, err := callInboundPayloadHash(ctx, caller, dstChain.EndpointAddress, packet, anchor)
	if err != nil {
		return false, err
	}
	if payloadHash == packet.PayloadHash {
		return true, nil
	}
	if err := validateDestinationVerificationConfig(ctx, caller, dstChain.EndpointAddress, packet, pathway, confirmations, anchor); err != nil {
		return false, err
	}
	submitted, observedConfirmations, err := callHashLookup(ctx, caller, pathway.ReceiveLib, crypto.Keccak256Hash(packet.PacketHeader), packet.PayloadHash, pathway.DestinationWorkers.OpenDVN, anchor)
	if err != nil {
		return false, err
	}
	return submitted && observedConfirmations >= confirmations, nil
}

// verificationConfirmedOnChain re-checks the "already verified" reconciliation at
// the destination chain's confirmation-deep block, so a shallow (reorg-vulnerable)
// verification is not written as terminal state. A not-yet-deep chain reports false.
func (w *Worker) verificationConfirmedOnChain(ctx context.Context, packet db.PacketRecord, pathway chain.Pathway, confirmations uint64) (bool, error) {
	confBlock, err := w.confirmedDstBlock(ctx, packet.DstEID)
	if errors.Is(err, errAwaitingConfirmations) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	complete, err := w.verificationAlreadyComplete(ctx, packet, pathway, confirmations, confirmedAnchor(confBlock))
	if err != nil {
		// The current config was already validated against latest before this
		// re-check. A config mismatch at the historical confirmed block just means
		// a recent (still-unconfirmed) config change has not yet reached depth, so
		// treat it as not-yet-confirmed and defer rather than flagging drift.
		if isDestinationVerificationConfigMismatch(err) {
			return false, nil
		}
		return false, err
	}
	return complete, nil
}

var errAwaitingConfirmations = errors.New("awaiting confirmation depth")

// confirmedAnchor wraps a confirmation-deep block in a number-only anchor; a
// nil block (no confirmation gate) keeps the caller's own latest anchoring.
func confirmedAnchor(confBlock *big.Int) *rpcquorum.StateAnchor {
	if confBlock == nil {
		return nil
	}
	return &rpcquorum.StateAnchor{Number: confBlock}
}

// confirmedDstBlock returns the destination block that is chain.Confirmations
// deep. A nil block with no error means no confirmation gate (read latest);
// errAwaitingConfirmations means the chain has not produced enough blocks yet.
func (w *Worker) confirmedDstBlock(ctx context.Context, dstEID uint32) (*big.Int, error) {
	dstChain, err := w.registry.Get(dstEID)
	if err != nil {
		return nil, err
	}
	if dstChain.Confirmations == 0 {
		return nil, nil
	}
	reader := w.head(dstEID)
	if reader == nil {
		return nil, fmt.Errorf("missing destination head reader for eid %d", dstEID)
	}
	headResult, err := reader.CheckHead(ctx)
	if err != nil {
		return nil, err
	}
	if headResult.Number == nil || !headResult.Number.IsUint64() {
		return nil, errors.New("destination head block number is unavailable")
	}
	head := headResult.Number.Uint64()
	if head < dstChain.Confirmations {
		return nil, errAwaitingConfirmations
	}
	return new(big.Int).SetUint64(head - dstChain.Confirmations), nil
}

func (w *Worker) validateActiveDestinationConfig(ctx context.Context, packet db.PacketRecord, confirmations uint64) error {
	if w.registry == nil {
		return nil
	}
	pathway, err := w.registry.Pathway(packet.SrcEID, packet.DstEID, packet.Sender, packet.Receiver)
	if err != nil {
		return err
	}
	if pathway.DVNMode != config.DVNModeActive {
		return nil
	}
	dstChain, err := w.registry.Get(packet.DstEID)
	if err != nil {
		return err
	}
	caller := w.caller(packet.DstEID)
	if caller == nil {
		return fmt.Errorf("missing destination caller for eid %d", packet.DstEID)
	}
	anchor, err := w.destinationAnchor(ctx, packet.DstEID)
	if err != nil {
		return err
	}
	return validateDestinationVerificationConfig(ctx, caller, dstChain.EndpointAddress, packet, pathway, confirmations, anchor)
}

// destinationAnchor resolves one verified canonical tip anchor on the
// destination chain for a logical multi-read check, so serial reads share a
// single hash-pinned state view instead of re-establishing the head quorum
// per call and drifting across branches on a mid-check reorg.
func (w *Worker) destinationAnchor(ctx context.Context, dstEID uint32) (*rpcquorum.StateAnchor, error) {
	head := w.head(dstEID)
	if head == nil {
		return nil, fmt.Errorf("missing destination head reader for eid %d", dstEID)
	}
	result, err := head.CheckHead(ctx)
	if err != nil {
		return nil, err
	}
	return rpcquorum.AnchorFromHead(result)
}

func validateDestinationVerificationConfig(ctx context.Context, caller ContractCaller, endpoint common.Address, packet db.PacketRecord, pathway chain.Pathway, confirmations uint64, anchor *rpcquorum.StateAnchor) error {
	receiveLib, err := callReceiveLibrary(ctx, caller, endpoint, packet.Receiver, packet.SrcEID, anchor)
	if err != nil {
		return err
	}
	if receiveLib != pathway.ReceiveLib {
		return fmt.Errorf("%w: destination receive library %s does not match configured %s", errDestinationVerificationConfigMismatch, receiveLib, pathway.ReceiveLib)
	}
	config, err := callReceiveUlnConfig(ctx, caller, pathway.ReceiveLib, packet.Receiver, packet.SrcEID, anchor)
	if err != nil {
		return err
	}
	if err := validateReceiveUlnConfig(config, confirmations, pathway.ReceiveRequiredDVNs); err != nil {
		return fmt.Errorf("%w: %w", errDestinationVerificationConfigMismatch, err)
	}
	return nil
}

func callInboundPayloadHash(ctx context.Context, caller ContractCaller, endpoint common.Address, packet db.PacketRecord, anchor *rpcquorum.StateAnchor) (common.Hash, error) {
	if endpoint == (common.Address{}) {
		return common.Hash{}, errors.New("endpoint address is required")
	}
	data, err := endpointViewABI.Pack(
		"inboundPayloadHash",
		packet.Receiver,
		packet.SrcEID,
		common.BytesToHash(packet.Sender.Bytes()),
		packet.Nonce.Uint64(),
	)
	if err != nil {
		return common.Hash{}, err
	}
	result, err := rpcquorum.CallAtAnchor(ctx, caller, ethereum.CallMsg{To: &endpoint, Data: data}, anchor)
	if err != nil {
		return common.Hash{}, err
	}
	values, err := endpointViewABI.Unpack("inboundPayloadHash", result)
	if err != nil {
		return common.Hash{}, err
	}
	if len(values) != 1 {
		return common.Hash{}, fmt.Errorf("inboundPayloadHash returned %d values, want 1", len(values))
	}
	value, ok := values[0].([32]byte)
	if !ok {
		return common.Hash{}, fmt.Errorf("inboundPayloadHash returned %T, want bytes32", values[0])
	}
	return common.BytesToHash(value[:]), nil
}

func callReceiveLibrary(ctx context.Context, caller ContractCaller, endpoint, receiver common.Address, srcEID uint32, anchor *rpcquorum.StateAnchor) (common.Address, error) {
	if endpoint == (common.Address{}) {
		return common.Address{}, errors.New("endpoint address is required")
	}
	if receiver == (common.Address{}) {
		return common.Address{}, errors.New("receiver address is required")
	}
	data, err := endpointViewABI.Pack("getReceiveLibrary", receiver, srcEID)
	if err != nil {
		return common.Address{}, err
	}
	result, err := rpcquorum.CallAtAnchor(ctx, caller, ethereum.CallMsg{To: &endpoint, Data: data}, anchor)
	if err != nil {
		return common.Address{}, err
	}
	values, err := endpointViewABI.Unpack("getReceiveLibrary", result)
	if err != nil {
		return common.Address{}, err
	}
	if len(values) != 2 {
		return common.Address{}, fmt.Errorf("getReceiveLibrary returned %d values, want 2", len(values))
	}
	receiveLib, ok := values[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("getReceiveLibrary lib returned %T, want address", values[0])
	}
	return receiveLib, nil
}

type receiveUlnConfig struct {
	Confirmations        uint64           `abi:"confirmations"`
	RequiredDVNCount     uint8            `abi:"requiredDVNCount"`
	OptionalDVNCount     uint8            `abi:"optionalDVNCount"`
	OptionalDVNThreshold uint8            `abi:"optionalDVNThreshold"`
	RequiredDVNs         []common.Address `abi:"requiredDVNs"`
	OptionalDVNs         []common.Address `abi:"optionalDVNs"`
}

func callReceiveUlnConfig(ctx context.Context, caller ContractCaller, receiveLib, receiver common.Address, srcEID uint32, anchor *rpcquorum.StateAnchor) (receiveUlnConfig, error) {
	if receiveLib == (common.Address{}) {
		return receiveUlnConfig{}, errors.New("receive lib address is required")
	}
	if receiver == (common.Address{}) {
		return receiveUlnConfig{}, errors.New("receiver address is required")
	}
	data, err := receiveUlnViewABI.Pack("getUlnConfig", receiver, srcEID)
	if err != nil {
		return receiveUlnConfig{}, err
	}
	result, err := rpcquorum.CallAtAnchor(ctx, caller, ethereum.CallMsg{To: &receiveLib, Data: data}, anchor)
	if err != nil {
		return receiveUlnConfig{}, err
	}
	values, err := receiveUlnViewABI.Unpack("getUlnConfig", result)
	if err != nil {
		return receiveUlnConfig{}, err
	}
	if len(values) != 1 {
		return receiveUlnConfig{}, fmt.Errorf("getUlnConfig returned %d values, want 1", len(values))
	}
	return receiveUlnConfigFromABI(values[0])
}

func receiveUlnConfigFromABI(value any) (receiveUlnConfig, error) {
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Struct {
		return receiveUlnConfig{}, fmt.Errorf("getUlnConfig returned %T, want tuple struct", value)
	}
	requiredDVNs, ok := reflected.FieldByName("RequiredDVNs").Interface().([]common.Address)
	if !ok {
		return receiveUlnConfig{}, fmt.Errorf("getUlnConfig requiredDVNs has type %T", reflected.FieldByName("RequiredDVNs").Interface())
	}
	optionalDVNs, ok := reflected.FieldByName("OptionalDVNs").Interface().([]common.Address)
	if !ok {
		return receiveUlnConfig{}, fmt.Errorf("getUlnConfig optionalDVNs has type %T", reflected.FieldByName("OptionalDVNs").Interface())
	}
	return receiveUlnConfig{
		Confirmations:        reflected.FieldByName("Confirmations").Uint(),
		RequiredDVNCount:     uint8(reflected.FieldByName("RequiredDVNCount").Uint()),
		OptionalDVNCount:     uint8(reflected.FieldByName("OptionalDVNCount").Uint()),
		OptionalDVNThreshold: uint8(reflected.FieldByName("OptionalDVNThreshold").Uint()),
		RequiredDVNs:         append([]common.Address(nil), requiredDVNs...),
		OptionalDVNs:         append([]common.Address(nil), optionalDVNs...),
	}, nil
}

func validateReceiveUlnConfig(config receiveUlnConfig, confirmations uint64, requiredDVNs []common.Address) error {
	// ReceiveUln302 accepts a DVN verification whose submitted confirmations meet or exceed this threshold.
	if confirmations < config.Confirmations {
		return fmt.Errorf("assigned confirmations %d are below receive uln required confirmations %d", confirmations, config.Confirmations)
	}
	if config.RequiredDVNCount != uint8(len(config.RequiredDVNs)) {
		return fmt.Errorf("receive uln requiredDVNCount %d does not match requiredDVNs length %d", config.RequiredDVNCount, len(config.RequiredDVNs))
	}
	if config.OptionalDVNCount != 0 && config.OptionalDVNCount != nilDVNCount {
		return fmt.Errorf("receive uln optionalDVNCount %d is not disabled", config.OptionalDVNCount)
	}
	if config.OptionalDVNThreshold != 0 {
		return fmt.Errorf("receive uln optionalDVNThreshold %d is not zero", config.OptionalDVNThreshold)
	}
	if len(config.OptionalDVNs) != 0 {
		return fmt.Errorf("receive uln optional DVNs are configured: %v", config.OptionalDVNs)
	}
	// An unapproved, stale, or missing entry silently changes the verification quorum,
	// so the on-chain set must match the approved set exactly (order ignored).
	if !equalAddressSets(config.RequiredDVNs, requiredDVNs) {
		return fmt.Errorf("receive uln required DVNs %v do not match configured required set %v", config.RequiredDVNs, requiredDVNs)
	}
	return nil
}

// equalAddressSets reports whether both slices hold the same addresses ignoring order.
func equalAddressSets(a, b []common.Address) bool {
	if len(a) != len(b) {
		return false
	}
	sortedA := slices.Clone(a)
	sortedB := slices.Clone(b)
	compare := func(x, y common.Address) int { return bytes.Compare(x.Bytes(), y.Bytes()) }
	slices.SortFunc(sortedA, compare)
	slices.SortFunc(sortedB, compare)
	return slices.Equal(sortedA, sortedB)
}

func callHashLookup(ctx context.Context, caller ContractCaller, receiveLib common.Address, headerHash, payloadHash common.Hash, dvn common.Address, anchor *rpcquorum.StateAnchor) (bool, uint64, error) {
	if receiveLib == (common.Address{}) {
		return false, 0, errors.New("receive lib address is required")
	}
	if dvn == (common.Address{}) {
		return false, 0, errors.New("open dvn address is required")
	}
	data, err := receiveUlnViewABI.Pack("hashLookup", headerHash, payloadHash, dvn)
	if err != nil {
		return false, 0, err
	}
	result, err := rpcquorum.CallAtAnchor(ctx, caller, ethereum.CallMsg{To: &receiveLib, Data: data}, anchor)
	if err != nil {
		return false, 0, err
	}
	values, err := receiveUlnViewABI.Unpack("hashLookup", result)
	if err != nil {
		return false, 0, err
	}
	if len(values) != 2 {
		return false, 0, fmt.Errorf("hashLookup returned %d values, want 2", len(values))
	}
	submitted, ok := values[0].(bool)
	if !ok {
		return false, 0, fmt.Errorf("hashLookup submitted returned %T, want bool", values[0])
	}
	observedConfirmations, ok := values[1].(uint64)
	if !ok {
		return false, 0, fmt.Errorf("hashLookup confirmations returned %T, want uint64", values[1])
	}
	return submitted, observedConfirmations, nil
}

func (w *Worker) buildVerifyTx(packet db.PacketRecord, pathway chain.Pathway, confirmations uint64) (db.TxRequest, error) {
	if w.registry == nil {
		return db.TxRequest{}, workerloop.Fatal(errors.New("dvn active mode requires chain registry"))
	}
	settings, ok := w.settings[packet.DstEID]
	if !ok {
		return db.TxRequest{}, workerloop.Fatal(fmt.Errorf("dvn active mode requires tx settings for destination eid %d", packet.DstEID))
	}
	if settings.SignerID == "" {
		return db.TxRequest{}, workerloop.Fatal(errors.New("dvn active mode requires signer id"))
	}
	return BuildVerifyTx(packet, pathway.DestinationWorkers.OpenDVN, pathway.ReceiveLib, confirmations, settings.SignerID)
}

func (w *Worker) markQuorumConflict(ctx context.Context, packet db.PacketRecord, err error) error {
	payload, marshalErr := json.Marshal(map[string]any{
		"tx_hash": packet.SrcTxHash.Hex(),
		"error":   err.Error(),
	})
	if marshalErr != nil {
		return marshalErr
	}
	if err := w.store.MarkDVNQuorumConflict(ctx, packet.GUID, string(packets.DVNQuorumChecking), err.Error(), payload); err != nil {
		return err
	}
	if err := w.store.PausePathwayForPacket(ctx, packet.GUID); err != nil {
		return err
	}
	w.logger.Warn("dvn quorum conflict paused pathway", "guid", packet.GUID, "src_eid", packet.SrcEID, "dst_eid", packet.DstEID, "from_status", string(packets.DVNQuorumChecking), "to_status", string(packets.DVNQuorumConflict), "error", err.Error())
	return nil
}

func (w *Worker) markDestinationConfigMismatch(ctx context.Context, item db.DVNWorkItem, expectedStatus string, cause error) (bool, error) {
	if err := w.store.MarkDVNManualReviewAndPausePathway(ctx, item.Packet.GUID, expectedStatus, cause.Error(), item.Job.QuorumResult); err != nil {
		return false, err
	}
	w.logger.Warn("dvn destination config mismatch paused pathway", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", expectedStatus, "to_status", string(packets.DVNManualReview), "error", cause.Error())
	return true, nil
}

func (w *Worker) deferDVNWorkError(ctx context.Context, item db.DVNWorkItem, status, reason string, cause error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if workerloop.IsFatal(cause) {
		return false, cause
	}
	if err := w.store.DeferDVNJob(ctx, item.Packet.GUID, status, loopInterval); err != nil {
		return false, err
	}
	w.logger.Warn("deferred dvn job after processing error", "reason", reason, "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status, "error", cause.Error())
	return true, nil
}

func isDestinationVerificationConfigMismatch(err error) bool {
	return errors.Is(err, errDestinationVerificationConfigMismatch)
}

func (w *Worker) markReorgDetected(ctx context.Context, packet db.PacketRecord, err error) error {
	payload, marshalErr := json.Marshal(map[string]any{
		"tx_hash": packet.SrcTxHash.Hex(),
		"error":   err.Error(),
	})
	if marshalErr != nil {
		return marshalErr
	}
	if err := w.store.MarkDVNReorgDetected(ctx, packet.GUID, string(packets.DVNQuorumChecking), err.Error(), payload); err != nil {
		return err
	}
	w.logger.Warn("dvn source reorg detected", "guid", packet.GUID, "src_eid", packet.SrcEID, "dst_eid", packet.DstEID, "from_status", string(packets.DVNQuorumChecking), "to_status", string(packets.DVNReorgDetected), "tx_hash", packet.SrcTxHash)
	return nil
}

func (w *Worker) head(eid uint32) HeadReader {
	if w == nil {
		return nil
	}
	return w.heads[eid]
}

func (w *Worker) receipt(eid uint32) ReceiptReader {
	if w == nil {
		return nil
	}
	return w.receipts[eid]
}

func (w *Worker) caller(eid uint32) ContractCaller {
	if w == nil {
		return nil
	}
	return w.callers[eid]
}

func hasRequiredConfirmations(blockNumber, confirmationsRequired, head uint64) bool {
	if confirmationsRequired == 0 {
		return false
	}
	if head < blockNumber {
		return false
	}
	return head-blockNumber+1 >= confirmationsRequired
}

// QuorumReport is the persisted shadow-mode evidence for a packet that would be verified.
type QuorumReport struct {
	TxHash      string `json:"tx_hash"`
	BlockNumber uint64 `json:"block_number"`
	BlockHash   string `json:"block_hash"`
	LogIndex    uint   `json:"log_index"`
	GUID        string `json:"guid"`
	PayloadHash string `json:"payload_hash"`
}

// errSourceReceiptReorg marks a source receipt whose PacketSent log was mined at
// a different block than the one the indexer recorded, i.e. the source tx was
// re-included by a reorg. The confirmation gate must be re-evaluated for the new
// block rather than treating the evidence as a quorum conflict.
var errSourceReceiptReorg = errors.New("source receipt block does not match indexed block")

func verifySourceReceiptForEndpoint(packet db.PacketRecord, receipt *gethtypes.Receipt, endpoint common.Address) (QuorumReport, error) {
	if receipt == nil {
		return QuorumReport{}, errors.New("source receipt is missing")
	}
	if receipt.TxHash != packet.SrcTxHash {
		return QuorumReport{}, fmt.Errorf("receipt tx hash %s does not match packet source tx %s", receipt.TxHash, packet.SrcTxHash)
	}
	if receipt.Status != gethtypes.ReceiptStatusSuccessful {
		return QuorumReport{}, fmt.Errorf("source tx receipt status is %d", receipt.Status)
	}
	// A reorg that re-includes the source tx at a different block also shifts the
	// block-global log index, so the lookup below would miss it. Compare the
	// receipt's own block first and route a mismatch to reorg re-evaluation
	// instead of a quorum conflict.
	if receipt.BlockNumber != nil && receipt.BlockNumber.IsUint64() && receipt.BlockNumber.Uint64() != packet.SrcBlockNumber {
		return QuorumReport{}, fmt.Errorf("%w: source receipt block %d does not match indexed block %d", errSourceReceiptReorg, receipt.BlockNumber.Uint64(), packet.SrcBlockNumber)
	}
	for _, log := range receipt.Logs {
		if log == nil || log.Index != packet.SrcLogIndex {
			continue
		}
		if endpoint != (common.Address{}) && log.Address != endpoint {
			return QuorumReport{}, fmt.Errorf("source receipt PacketSent address %s does not match endpoint %s", log.Address, endpoint)
		}
		if log.BlockNumber != packet.SrcBlockNumber {
			return QuorumReport{}, fmt.Errorf("%w: source receipt block %d does not match indexed block %d", errSourceReceiptReorg, log.BlockNumber, packet.SrcBlockNumber)
		}
		record, err := indexer.PacketRecordFromSentLog(*log)
		if err != nil {
			return QuorumReport{}, err
		}
		if err := validateReceiptPacket(packet, record); err != nil {
			return QuorumReport{}, err
		}
		return QuorumReport{
			TxHash:      receipt.TxHash.Hex(),
			BlockNumber: log.BlockNumber,
			BlockHash:   log.BlockHash.Hex(),
			LogIndex:    log.Index,
			GUID:        packet.GUID.Hex(),
			PayloadHash: packet.PayloadHash.Hex(),
		}, nil
	}
	return QuorumReport{}, fmt.Errorf("source receipt missing PacketSent log index %d", packet.SrcLogIndex)
}

// reorgedPacketRecord locates the packet's PacketSent log inside a re-included
// source receipt and returns the packet with the receipt's block number and
// log index. The stored log index is useless after a re-inclusion (the
// block-global index shifts), so the log is matched by decoding each PacketSent
// event and comparing the full packet contents instead.
func reorgedPacketRecord(packet db.PacketRecord, receipt *gethtypes.Receipt, endpoint common.Address) (db.PacketRecord, bool) {
	if receipt == nil {
		return db.PacketRecord{}, false
	}
	for _, log := range receipt.Logs {
		if log == nil || log.BlockNumber == 0 {
			continue
		}
		if endpoint != (common.Address{}) && log.Address != endpoint {
			continue
		}
		record, err := indexer.PacketRecordFromSentLog(*log)
		if err != nil || record.GUID != packet.GUID {
			continue
		}
		if err := validateReceiptPacket(packet, record); err != nil {
			continue
		}
		updated := packet
		updated.SrcBlockNumber = log.BlockNumber
		updated.SrcLogIndex = log.Index
		return updated, true
	}
	return db.PacketRecord{}, false
}

// BuildVerifyCalldata ABI-encodes OpenDVN.submitVerification for active DVN mode.
func BuildVerifyCalldata(packet db.PacketRecord, receiveLib common.Address, confirmations uint64) ([]byte, error) {
	if err := packet.Validate(); err != nil {
		return nil, err
	}
	if receiveLib == (common.Address{}) {
		return nil, errors.New("receive lib address is required")
	}
	if confirmations == 0 {
		return nil, errors.New("dvn confirmations required is required")
	}
	return lzabi.PackOpenDVNSubmitVerification(receiveLib, packet.PacketHeader, packet.PayloadHash, confirmations)
}

// BuildVerifyTx creates the outbox request for OpenDVN.submitVerification.
func BuildVerifyTx(packet db.PacketRecord, openDVN, receiveLib common.Address, confirmations uint64, signerID string) (db.TxRequest, error) {
	if openDVN == (common.Address{}) {
		return db.TxRequest{}, errors.New("destination open dvn address is required")
	}
	if signerID == "" {
		return db.TxRequest{}, errors.New("signer id is required")
	}
	calldata, err := BuildVerifyCalldata(packet, receiveLib, confirmations)
	if err != nil {
		return db.TxRequest{}, err
	}
	return db.TxRequest{
		ChainEID: packet.DstEID,
		Purpose:  TxPurposeVerify,
		GUID:     packet.GUID.Bytes(),
		To:       openDVN,
		Calldata: calldata,
		Value:    new(big.Int),
		SignerID: signerID,
	}, nil
}

func validateReceiptPacket(expected, actual db.PacketRecord) error {
	if actual.GUID != expected.GUID {
		return fmt.Errorf("receipt packet guid %s does not match stored guid %s", actual.GUID, expected.GUID)
	}
	if actual.SrcEID != expected.SrcEID || actual.DstEID != expected.DstEID {
		return fmt.Errorf("receipt packet pathway %d -> %d does not match stored pathway %d -> %d", actual.SrcEID, actual.DstEID, expected.SrcEID, expected.DstEID)
	}
	if actual.Sender != expected.Sender || actual.Receiver != expected.Receiver {
		return fmt.Errorf("receipt packet oapps %s -> %s do not match stored oapps %s -> %s", actual.Sender, actual.Receiver, expected.Sender, expected.Receiver)
	}
	if actual.SendLib != expected.SendLib {
		return fmt.Errorf("receipt send lib %s does not match stored send lib %s", actual.SendLib, expected.SendLib)
	}
	if actual.PayloadHash != expected.PayloadHash {
		return fmt.Errorf("receipt payload hash %s does not match stored payload hash %s", actual.PayloadHash, expected.PayloadHash)
	}
	if !bytes.Equal(actual.EncodedPacket, expected.EncodedPacket) {
		return errors.New("receipt encoded packet does not match stored encoded packet")
	}
	return nil
}
