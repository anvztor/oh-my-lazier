package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

const loopInterval = 5 * time.Second

var errAwaitingConfirmations = errors.New("awaiting confirmation depth")

// confirmedReadBlock returns the block that is chain.Confirmations deep, for
// reading terminal on-chain state that must not be written to durable storage
// until it can no longer be reorged out. A nil block with no error means the
// chain has no confirmation gate (read latest); errAwaitingConfirmations means
// the chain has not produced enough blocks yet for anything to be confirmed.
func (w *Worker) confirmedReadBlock(ctx context.Context, eid uint32) (*big.Int, error) {
	configuredChain, err := w.registry.Get(eid)
	if err != nil {
		return nil, err
	}
	if configuredChain.Confirmations == 0 {
		return nil, nil
	}
	caller := w.caller(eid)
	if caller == nil {
		return nil, fmt.Errorf("missing destination caller for eid %d", eid)
	}
	head, err := caller.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}
	if head < configuredChain.Confirmations {
		return nil, errAwaitingConfirmations
	}
	return new(big.Int).SetUint64(head - configuredChain.Confirmations), nil
}

// latestAnchor resolves one verified canonical tip anchor for a logical
// multi-read check: serial reads must not each re-establish the head quorum
// (a black-holed minority provider would charge every read a probe deadline),
// and the pinned block hash keeps the whole check on the verified branch even
// if the tip reorgs mid-check.
func latestAnchor(ctx context.Context, caller ContractCaller) (*rpcquorum.StateAnchor, error) {
	if caller == nil {
		return nil, errors.New("destination caller is required")
	}
	head, err := caller.CheckHead(ctx)
	if err != nil {
		return nil, err
	}
	return rpcquorum.AnchorFromHead(head)
}

// commitConfirmedOnChain reports whether the packet's commit is still present at
// the confirmation-deep block, so a shallow (reorg-vulnerable) commit is not
// written as terminal state. A not-yet-deep chain reports false (defer).
func (w *Worker) commitConfirmedOnChain(ctx context.Context, eid uint32, endpoint, receiveLib common.Address, packet db.PacketRecord) (bool, error) {
	confBlock, err := w.confirmedReadBlock(ctx, eid)
	if errors.Is(err, errAwaitingConfirmations) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	state, err := CheckCommitState(ctx, w.caller(eid), endpoint, receiveLib, packet, &rpcquorum.StateAnchor{Number: confBlock})
	if err != nil {
		return false, err
	}
	return state == CommitCommitted, nil
}

// deliveryConfirmedOnChain reports whether the packet's delivery is still
// present at the confirmation-deep block.
func (w *Worker) deliveryConfirmedOnChain(ctx context.Context, eid uint32, endpoint common.Address, packet db.PacketRecord) (bool, error) {
	confBlock, err := w.confirmedReadBlock(ctx, eid)
	if errors.Is(err, errAwaitingConfirmations) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	state, err := CheckDeliveryState(ctx, w.caller(eid), endpoint, packet, &rpcquorum.StateAnchor{Number: confBlock})
	if err != nil {
		return false, err
	}
	return state == DeliveryDelivered, nil
}

// Store is the durable executor state required by the worker.
type Store interface {
	ListExecutorWork(ctx context.Context, status string, limit int) ([]db.ExecutorWorkItem, error)
	MarkExecutorWaitingDVNVerification(ctx context.Context, guid common.Hash, expectedStatus string) error
	MarkExecutorVerifiable(ctx context.Context, guid common.Hash, expectedStatus string) error
	MarkExecutorCommittedFromChain(ctx context.Context, guid common.Hash, expectedStatus string) error
	MarkExecutorExecutable(ctx context.Context, guid common.Hash) error
	MarkExecutorManualReview(ctx context.Context, guid common.Hash, expectedStatus, reason string) error
	MarkExecutorDeliveredFromChain(ctx context.Context, guid common.Hash, expectedStatus string) error
	EnqueueExecutorTx(ctx context.Context, guid common.Hash, expectedStatus, nextStatus string, request db.TxRequest) (int64, error)
	DeferExecutorJob(ctx context.Context, guid common.Hash, expectedStatus string, delay time.Duration) error
}

// Worker runs executor commit and delivery workflows.
type Worker struct {
	store    Store
	registry *chain.Registry
	callers  map[uint32]ContractCaller
	logger   *slog.Logger
}

// New creates an executor worker.
func New(store Store, registry *chain.Registry, logger *slog.Logger) *Worker {
	callers := make(map[uint32]ContractCaller)
	if registry != nil {
		for _, configuredChain := range registry.All() {
			if configuredChain.RPC != nil {
				callers[configuredChain.EID] = configuredChain.RPC
			}
		}
	}
	return NewWithCallers(store, registry, callers, logger)
}

// NewWithCallers creates an executor worker with explicit chain call clients.
func NewWithCallers(store Store, registry *chain.Registry, callers map[uint32]ContractCaller, logger *slog.Logger) *Worker {
	copiedCallers := make(map[uint32]ContractCaller, len(callers))
	maps.Copy(copiedCallers, callers)
	return &Worker{store: store, registry: registry, callers: copiedCallers, logger: logger}
}

// RunCommitter starts the commitVerification enqueue loop.
func (w *Worker) RunCommitter(ctx context.Context) error {
	w.logger.Info("executor committer loop started")
	return w.runLoop(ctx, w.ProcessCommitterOnce)
}

// RunDeliverer starts the lzReceive delivery loop.
func (w *Worker) RunDeliverer(ctx context.Context) error {
	w.logger.Info("executor deliverer loop started")
	return w.runLoop(ctx, w.ProcessDelivererOnce)
}

// ProcessCommitterOnce enqueues one commitVerification transaction for a verifiable packet.
func (w *Worker) ProcessCommitterOnce(ctx context.Context) (bool, error) {
	if processed, err := w.processCommitReadinessStatus(ctx, string(packets.ExecutorAssigned)); err != nil || processed {
		return processed, err
	}
	if processed, err := w.processCommitReadinessStatus(ctx, string(packets.ExecutorWaitingDVNVerification)); err != nil || processed {
		return processed, err
	}
	work, err := w.store.ListExecutorWork(ctx, string(packets.ExecutorVerifiable), 1)
	if err != nil {
		return false, err
	}
	if len(work) == 0 {
		return false, nil
	}
	item := work[0]
	pathway, err := w.registry.Pathway(item.Packet.SrcEID, item.Packet.DstEID, item.Packet.Sender, item.Packet.Receiver)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorVerifiable), "pathway_lookup_error", err)
	}
	if !pathway.Enabled {
		if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, string(packets.ExecutorVerifiable), loopInterval); err != nil {
			return false, err
		}
		w.logger.Debug("skipped executor commit workflow", "reason", "pathway_disabled", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", item.Job.Status)
		return true, nil
	}
	dstChain, err := w.registry.Get(item.Packet.DstEID)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorVerifiable), "destination_chain_lookup_error", err)
	}
	anchor, err := latestAnchor(ctx, w.caller(item.Packet.DstEID))
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorVerifiable), "commit_readiness_error", err)
	}
	state, err := CheckCommitState(ctx, w.caller(item.Packet.DstEID), dstChain.EndpointAddress, pathway.ReceiveLib, item.Packet, anchor)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorVerifiable), "commit_readiness_error", err)
	}
	switch state {
	case CommitCommitted:
		confirmed, err := w.commitConfirmedOnChain(ctx, item.Packet.DstEID, dstChain.EndpointAddress, pathway.ReceiveLib, item.Packet)
		if err != nil {
			return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorVerifiable), "commit_confirmation_error", err)
		}
		if !confirmed {
			if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, string(packets.ExecutorVerifiable), loopInterval); err != nil {
				return false, err
			}
			w.logger.Debug("deferred executor commit below confirmation depth", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", string(packets.ExecutorVerifiable))
			return true, nil
		}
		if err := w.store.MarkExecutorCommittedFromChain(ctx, item.Packet.GUID, string(packets.ExecutorVerifiable)); err != nil {
			return false, err
		}
		w.logger.Info("executor commit already completed on chain", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", string(packets.ExecutorVerifiable), "to_status", string(packets.ExecutorCommitted))
		return true, nil
	case CommitVerifiable:
	default:
		if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, string(packets.ExecutorVerifiable), loopInterval); err != nil {
			return false, err
		}
		w.logger.Debug("skipped executor commit workflow", "reason", "commit_not_verifiable", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", item.Job.Status, "commit_state", commitStateLabel(state))
		return true, nil
	}
	request, err := BuildCommitVerificationTx(item.Packet, pathway.ReceiveLib, dstChain.TxRoles.Executor.SignerID)
	if err != nil {
		return false, err
	}
	id, err := w.store.EnqueueExecutorTx(ctx, item.Packet.GUID, string(packets.ExecutorVerifiable), string(packets.ExecutorCommitTxEnqueued), request)
	if errors.Is(err, db.ErrTxSendScopeInactive) {
		// The pathway or chain was paused/disabled between work selection and
		// this enqueue; the job keeps its status and resumes after unpause.
		w.logger.Debug("skipped executor commit tx enqueue", "reason", "send_scope_inactive", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	w.logger.Info("enqueued executor commit tx", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", string(packets.ExecutorVerifiable), "to_status", string(packets.ExecutorCommitTxEnqueued), "tx_outbox_id", id)
	return true, nil
}

func (w *Worker) processCommitReadinessStatus(ctx context.Context, status string) (bool, error) {
	work, err := w.store.ListExecutorWork(ctx, status, 1)
	if err != nil || len(work) == 0 {
		return false, err
	}
	item := work[0]
	pathway, err := w.registry.Pathway(item.Packet.SrcEID, item.Packet.DstEID, item.Packet.Sender, item.Packet.Receiver)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, status, "pathway_lookup_error", err)
	}
	if !pathway.Enabled {
		if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, status, loopInterval); err != nil {
			return false, err
		}
		w.logger.Debug("skipped executor commit readiness", "reason", "pathway_disabled", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status)
		return true, nil
	}
	dstChain, err := w.registry.Get(item.Packet.DstEID)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, status, "destination_chain_lookup_error", err)
	}
	anchor, err := latestAnchor(ctx, w.caller(item.Packet.DstEID))
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, status, "commit_readiness_error", err)
	}
	state, err := CheckCommitState(ctx, w.caller(item.Packet.DstEID), dstChain.EndpointAddress, pathway.ReceiveLib, item.Packet, anchor)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, status, "commit_readiness_error", err)
	}
	switch state {
	case CommitCommitted:
		confirmed, err := w.commitConfirmedOnChain(ctx, item.Packet.DstEID, dstChain.EndpointAddress, pathway.ReceiveLib, item.Packet)
		if err != nil {
			return w.deferExecutorWorkError(ctx, item, status, "commit_confirmation_error", err)
		}
		if !confirmed {
			if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, status, loopInterval); err != nil {
				return false, err
			}
			w.logger.Debug("deferred executor commit below confirmation depth", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status)
			return true, nil
		}
		if err := w.store.MarkExecutorCommittedFromChain(ctx, item.Packet.GUID, status); err != nil {
			return false, err
		}
		w.logger.Info("executor commit already completed on chain", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.ExecutorCommitted))
		return true, nil
	case CommitVerifiable:
		if err := w.store.MarkExecutorVerifiable(ctx, item.Packet.GUID, status); err != nil {
			return false, err
		}
		w.logger.Info("executor job became commit-verifiable", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.ExecutorVerifiable))
		return true, nil
	case CommitNotVerifiable:
	}
	if status == string(packets.ExecutorAssigned) {
		if err := w.store.MarkExecutorWaitingDVNVerification(ctx, item.Packet.GUID, status); err != nil {
			return false, err
		}
		w.logger.Info("executor job waiting for dvn verification", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.ExecutorWaitingDVNVerification))
		return true, nil
	}
	if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, status, loopInterval); err != nil {
		return false, err
	}
	w.logger.Debug("skipped executor commit readiness", "reason", "commit_not_verifiable", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status, "commit_state", commitStateLabel(state))
	return true, nil
}

// ProcessDelivererOnce enqueues one lzReceive transaction for an executable packet.
func (w *Worker) ProcessDelivererOnce(ctx context.Context) (bool, error) {
	if processed, err := w.processExecutableReadiness(ctx); err != nil || processed {
		return processed, err
	}
	if processed, err := w.processDelivererStatus(ctx, string(packets.ExecutorExecutable)); err != nil || processed {
		return processed, err
	}
	return w.processDelivererStatus(ctx, string(packets.ExecutorLzReceiveFailed))
}

func (w *Worker) processExecutableReadiness(ctx context.Context) (bool, error) {
	work, err := w.store.ListExecutorWork(ctx, string(packets.ExecutorCommitted), 1)
	if err != nil || len(work) == 0 {
		return false, err
	}
	item := work[0]
	dstChain, err := w.registry.Get(item.Packet.DstEID)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorCommitted), "destination_chain_lookup_error", err)
	}
	anchor, err := latestAnchor(ctx, w.caller(item.Packet.DstEID))
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorCommitted), "delivery_readiness_error", err)
	}
	state, err := CheckDeliveryState(ctx, w.caller(item.Packet.DstEID), dstChain.EndpointAddress, item.Packet, anchor)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorCommitted), "delivery_readiness_error", err)
	}
	switch state {
	case DeliveryDelivered:
		confirmed, err := w.deliveryConfirmedOnChain(ctx, item.Packet.DstEID, dstChain.EndpointAddress, item.Packet)
		if err != nil {
			return w.deferExecutorWorkError(ctx, item, string(packets.ExecutorCommitted), "delivery_confirmation_error", err)
		}
		if !confirmed {
			if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, string(packets.ExecutorCommitted), loopInterval); err != nil {
				return false, err
			}
			w.logger.Debug("deferred executor delivery below confirmation depth", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", string(packets.ExecutorCommitted))
			return true, nil
		}
		if err := w.store.MarkExecutorDeliveredFromChain(ctx, item.Packet.GUID, string(packets.ExecutorCommitted)); err != nil {
			return false, err
		}
		w.logger.Info("executor lzReceive already completed on chain", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", string(packets.ExecutorCommitted), "to_status", string(packets.ExecutorDelivered))
		return true, nil
	case DeliveryExecutable:
	default:
		if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, string(packets.ExecutorCommitted), loopInterval); err != nil {
			return false, err
		}
		w.logger.Debug("skipped executor executable readiness", "reason", "delivery_not_executable", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", item.Job.Status, "delivery_state", deliveryStateLabel(state))
		return true, nil
	}
	if err := w.store.MarkExecutorExecutable(ctx, item.Packet.GUID); err != nil {
		return false, err
	}
	w.logger.Info("executor job became lzReceive-executable", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", string(packets.ExecutorCommitted), "to_status", string(packets.ExecutorExecutable))
	return true, nil
}

func (w *Worker) processDelivererStatus(ctx context.Context, status string) (bool, error) {
	work, err := w.store.ListExecutorWork(ctx, status, 1)
	if err != nil || len(work) == 0 {
		return false, err
	}
	item := work[0]
	dstChain, err := w.registry.Get(item.Packet.DstEID)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, status, "destination_chain_lookup_error", err)
	}
	anchor, err := latestAnchor(ctx, w.caller(item.Packet.DstEID))
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, status, "delivery_readiness_error", err)
	}
	state, err := CheckDeliveryState(ctx, w.caller(item.Packet.DstEID), dstChain.EndpointAddress, item.Packet, anchor)
	if err != nil {
		return w.deferExecutorWorkError(ctx, item, status, "delivery_readiness_error", err)
	}
	switch state {
	case DeliveryDelivered:
		confirmed, err := w.deliveryConfirmedOnChain(ctx, item.Packet.DstEID, dstChain.EndpointAddress, item.Packet)
		if err != nil {
			return w.deferExecutorWorkError(ctx, item, status, "delivery_confirmation_error", err)
		}
		if !confirmed {
			if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, status, loopInterval); err != nil {
				return false, err
			}
			w.logger.Debug("deferred executor delivery below confirmation depth", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status)
			return true, nil
		}
		if err := w.store.MarkExecutorDeliveredFromChain(ctx, item.Packet.GUID, status); err != nil {
			return false, err
		}
		w.logger.Info("executor lzReceive already completed on chain", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.ExecutorDelivered))
		return true, nil
	case DeliveryExecutable:
	default:
		if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, status, loopInterval); err != nil {
			return false, err
		}
		w.logger.Debug("skipped executor delivery workflow", "reason", "delivery_not_executable", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status, "delivery_state", deliveryStateLabel(state))
		return true, nil
	}
	if status == string(packets.ExecutorLzReceiveFailed) && item.Job.RetryCount >= db.MaxLzReceiveDeliveryAttempts {
		return w.markExecutorManualReview(ctx, item, status, fmt.Errorf("lzReceive reverted %d times, exceeding the %d-attempt retry budget", item.Job.RetryCount, db.MaxLzReceiveDeliveryAttempts))
	}
	request, err := BuildLzReceiveTx(item.Packet, dstChain.EndpointAddress, dstChain.TxRoles.Executor.SignerID)
	if err != nil {
		return w.markExecutorManualReview(ctx, item, status, err)
	}
	id, err := w.store.EnqueueExecutorTx(ctx, item.Packet.GUID, status, string(packets.ExecutorLzReceiveTxEnqueued), request)
	if errors.Is(err, db.ErrTxSendScopeInactive) {
		// The pathway or chain was paused/disabled between work selection and
		// this enqueue; the job keeps its status and resumes after unpause.
		w.logger.Debug("skipped executor lzReceive tx enqueue", "reason", "send_scope_inactive", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	w.logger.Info("enqueued executor lzReceive tx", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.ExecutorLzReceiveTxEnqueued), "tx_outbox_id", id)
	return true, nil
}

func commitStateLabel(state CommitState) string {
	switch state {
	case CommitNotVerifiable:
		return "not_verifiable"
	case CommitVerifiable:
		return "verifiable"
	case CommitCommitted:
		return "committed"
	default:
		return "unknown"
	}
}

func deliveryStateLabel(state DeliveryState) string {
	switch state {
	case DeliveryNotExecutable:
		return "not_executable"
	case DeliveryExecutable:
		return "executable"
	case DeliveryDelivered:
		return "delivered"
	default:
		return "unknown"
	}
}

func (w *Worker) runLoop(ctx context.Context, process func(context.Context) (bool, error)) error {
	for {
		processed, err := process(ctx)
		if err != nil {
			return err
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

func (w *Worker) deferExecutorWorkError(ctx context.Context, item db.ExecutorWorkItem, status, reason string, cause error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := w.store.DeferExecutorJob(ctx, item.Packet.GUID, status, loopInterval); err != nil {
		return false, err
	}
	w.logger.Warn("deferred executor job after processing error", "reason", reason, "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "status", status, "error", cause.Error())
	return true, nil
}

func (w *Worker) markExecutorManualReview(ctx context.Context, item db.ExecutorWorkItem, status string, cause error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := w.store.MarkExecutorManualReview(ctx, item.Packet.GUID, status, cause.Error()); err != nil {
		return false, err
	}
	w.logger.Warn("executor job requires manual review after deterministic build error", "guid", item.Packet.GUID, "src_eid", item.Packet.SrcEID, "dst_eid", item.Packet.DstEID, "from_status", status, "to_status", string(packets.ExecutorManualReview), "error", cause.Error())
	return true, nil
}

func (w *Worker) caller(eid uint32) ContractCaller {
	if w == nil {
		return nil
	}
	return w.callers[eid]
}
