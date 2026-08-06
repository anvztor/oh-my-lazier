package indexer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/lz"
	"github.com/islishude/oh-my-lazier/go/internal/lzabi"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/jackc/pgx/v5"
)

// ExecutorReceiptStore persists executor destination-chain event outcomes.
type ExecutorReceiptStore interface {
	MarkExecutorCommittedObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus string) error
	MarkExecutorDeliveredObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus string) error
	MarkExecutorReceiveFailedObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus, reason string) error
}

// ExecutorDestinationStore loads known packets and persists destination-chain executor outcomes.
type ExecutorDestinationStore interface {
	ExecutorReceiptStore
	SourcePacketSkipStore
	GetExecutorJob(ctx context.Context, guid common.Hash) (db.ExecutorJobRecord, error)
	GetPacket(ctx context.Context, guid common.Hash) (db.PacketRecord, error)
	GetPacketByDestination(ctx context.Context, dstEID, srcEID uint32, sender, receiver common.Address, nonce uint64) (db.PacketRecord, error)
}

// DVNDestinationStore loads known packets and persists destination-chain DVN verification outcomes.
type DVNDestinationStore interface {
	SourcePacketSkipStore
	GetDVNJob(ctx context.Context, guid common.Hash) (db.DVNJobRecord, error)
	GetPacketByVerification(ctx context.Context, dstEID uint32, packetHeader []byte, payloadHash common.Hash) (db.PacketRecord, error)
	MarkDVNVerifiedObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus string) error
}

// SourcePacketSkipStore loads source-chain skip tombstones used to disambiguate missing local source state.
type SourcePacketSkipStore interface {
	GetSourcePacketSkip(ctx context.Context, role string, srcEID, dstEID uint32, sender, receiver common.Address, nonce uint64) (db.SourcePacketSkip, error)
}

// DestinationStore persists all destination-chain event outcomes.
type DestinationStore interface {
	ExecutorDestinationStore
	DVNDestinationStore
}

type destinationLogObserver struct {
	executorApplied func(packet db.PacketRecord, job db.ExecutorJobRecord, log gethtypes.Log)
	executorSkipped func(reason string, packet db.PacketRecord, job db.ExecutorJobRecord, log gethtypes.Log)
	dvnApplied      func(packet db.PacketRecord, job db.DVNJobRecord, log gethtypes.Log)
	dvnSkipped      func(reason string, packet db.PacketRecord, job db.DVNJobRecord, log gethtypes.Log)
}

type destinationApplyResult struct {
	applied int
	pending bool
}

// ApplyExecutorDestinationLogs applies known EndpointV2 destination logs for one destination chain.
func ApplyExecutorDestinationLogs(ctx context.Context, store ExecutorDestinationStore, dstEID uint32, logs []gethtypes.Log) (int, error) {
	result, err := applyExecutorDestinationLogs(ctx, store, dstEID, nil, common.Address{}, logs, destinationLogObserver{})
	return result.applied, err
}

func applyExecutorDestinationLogs(ctx context.Context, store ExecutorDestinationStore, dstEID uint32, pathways []chain.Pathway, expectedExecutor common.Address, logs []gethtypes.Log, observer destinationLogObserver) (destinationApplyResult, error) {
	if store == nil {
		return destinationApplyResult{}, fmt.Errorf("executor destination store is required")
	}
	if dstEID == 0 {
		return destinationApplyResult{}, fmt.Errorf("destination eid is required")
	}
	result := destinationApplyResult{}
	for _, log := range logs {
		packet, ok, err := packetForDestinationLog(ctx, store, dstEID, log)
		if errors.Is(err, pgx.ErrNoRows) {
			pathway, matches, matchErr := executorDestinationLogPathway(pathways, dstEID, expectedExecutor, log)
			if matchErr != nil {
				return result, matchErr
			}
			if matches && pathway.Enabled {
				skipped, skipErr := executorSourcePacketWasSkipped(ctx, store, dstEID, expectedExecutor, log)
				if skipErr != nil {
					return result, skipErr
				}
				if !skipped {
					result.pending = true
					if observer.executorSkipped != nil {
						observer.executorSkipped("pending_source_packet", db.PacketRecord{}, db.ExecutorJobRecord{}, log)
					}
					continue
				}
			}
			if observer.executorSkipped != nil {
				reason := "unknown_packet"
				if matches && pathway.Enabled {
					reason = "skipped_source_packet"
				} else if matches {
					reason = "pathway_disabled"
				} else if len(pathways) > 0 {
					reason = "unknown_pathway"
				}
				observer.executorSkipped(reason, db.PacketRecord{}, db.ExecutorJobRecord{}, log)
			}
			continue
		}
		if err != nil {
			return result, err
		}
		if !ok {
			if observer.executorSkipped != nil {
				observer.executorSkipped("unsupported_event", db.PacketRecord{}, db.ExecutorJobRecord{}, log)
			}
			continue
		}
		if packet.DstEID != dstEID {
			return result, fmt.Errorf("packet %s destination eid %d does not match indexed chain %d", packet.GUID, packet.DstEID, dstEID)
		}
		if !executorDestinationLogExecutorAllowed(log, expectedExecutor) {
			if observer.executorSkipped != nil {
				observer.executorSkipped("unexpected_executor", packet, db.ExecutorJobRecord{}, log)
			}
			continue
		}
		job, err := store.GetExecutorJob(ctx, packet.GUID)
		if errors.Is(err, pgx.ErrNoRows) {
			if pathway, matches := pathwayForPacket(pathways, packet); matches && pathway.Enabled {
				skipped, skipErr := sourcePacketWasSkipped(ctx, store, sourceRoleExecutor, destinationPacketIdentity{
					SrcEID:   packet.SrcEID,
					DstEID:   packet.DstEID,
					Sender:   packet.Sender,
					Receiver: packet.Receiver,
					Nonce:    packet.Nonce.Uint64(),
				})
				if skipErr != nil {
					return result, skipErr
				}
				if skipped {
					if observer.executorSkipped != nil {
						observer.executorSkipped("skipped_source_packet", packet, db.ExecutorJobRecord{}, log)
					}
					continue
				}
				result.pending = true
				if observer.executorSkipped != nil {
					observer.executorSkipped("pending_executor_job", packet, db.ExecutorJobRecord{}, log)
				}
				continue
			}
			if observer.executorSkipped != nil {
				observer.executorSkipped("missing_executor_job", packet, db.ExecutorJobRecord{}, log)
			}
			continue
		} else if err != nil {
			return result, err
		}
		if executorDestinationLogAlreadyApplied(job.Status, log.Topics[0]) {
			if observer.executorSkipped != nil {
				observer.executorSkipped("already_applied", packet, job, log)
			}
			continue
		}
		didApply, err := ApplyExecutorDestinationLog(ctx, store, packet, job, log)
		if err != nil {
			return result, err
		}
		if didApply {
			if observer.executorApplied != nil {
				observer.executorApplied(packet, job, log)
			}
			result.applied++
		}
	}
	return result, nil
}

func executorDestinationLogAlreadyApplied(status string, topic common.Hash) bool {
	switch topic {
	case lzabi.PacketVerifiedTopic():
		// MANUAL_REVIEW mirrors the LzReceiveAlert branch: a parked job keeps
		// its parked state. A lagging indexer replaying an old commit event
		// must not un-park the job — that would bypass the delivery retry cap
		// and could deliver a message the operator explicitly canceled.
		switch status {
		case string(packets.ExecutorCommitted),
			string(packets.ExecutorExecutable),
			string(packets.ExecutorLzReceiveTxEnqueued),
			string(packets.ExecutorDelivered),
			string(packets.ExecutorLzReceiveFailed),
			string(packets.ExecutorManualReview):
			return true
		}
	case lzabi.PacketDeliveredTopic():
		return status == string(packets.ExecutorDelivered)
	case lzabi.LzReceiveAlertTopic():
		// MANUAL_REVIEW mirrors the receipt applier's already-applied set: a
		// parked job keeps its parked state, and the historical alert must not
		// error out and wedge the destination cursor.
		return status == string(packets.ExecutorLzReceiveFailed) ||
			status == string(packets.ExecutorDelivered) ||
			status == string(packets.ExecutorManualReview)
	}
	return false
}

// ApplyDVNDestinationLogs applies ReceiveUln302 PayloadVerified logs for configured pathway OpenDVNs.
func ApplyDVNDestinationLogs(ctx context.Context, store DVNDestinationStore, dstEID uint32, pathways []chain.Pathway, logs []gethtypes.Log) (int, error) {
	result, err := applyDVNDestinationLogs(ctx, store, dstEID, pathways, logs, destinationLogObserver{})
	return result.applied, err
}

func applyDVNDestinationLogs(ctx context.Context, store DVNDestinationStore, dstEID uint32, pathways []chain.Pathway, logs []gethtypes.Log, observer destinationLogObserver) (destinationApplyResult, error) {
	if store == nil {
		return destinationApplyResult{}, fmt.Errorf("dvn destination store is required")
	}
	if dstEID == 0 {
		return destinationApplyResult{}, fmt.Errorf("destination eid is required")
	}
	result := destinationApplyResult{}
	for _, log := range logs {
		if len(log.Topics) == 0 || log.Topics[0] != lzabi.PayloadVerifiedTopic() {
			if observer.dvnSkipped != nil {
				observer.dvnSkipped("unsupported_event", db.PacketRecord{}, db.DVNJobRecord{}, log)
			}
			continue
		}
		event, err := lzabi.DecodePayloadVerified(log)
		if err != nil {
			return result, err
		}
		packet, err := store.GetPacketByVerification(ctx, dstEID, event.Header, event.ProofHash)
		if errors.Is(err, pgx.ErrNoRows) {
			pathway, matches := pathwayForPayloadVerified(pathways, dstEID, event)
			if matches && pathway.Enabled && event.DVN == pathway.DestinationWorkers.OpenDVN {
				skipped, skipErr := dvnSourcePacketWasSkipped(ctx, store, dstEID, event)
				if skipErr != nil {
					return result, skipErr
				}
				if skipped {
					if observer.dvnSkipped != nil {
						observer.dvnSkipped("skipped_source_packet", db.PacketRecord{}, db.DVNJobRecord{}, log)
					}
					continue
				}
				result.pending = true
				if observer.dvnSkipped != nil {
					observer.dvnSkipped("pending_source_packet", db.PacketRecord{}, db.DVNJobRecord{}, log)
				}
				continue
			}
			if observer.dvnSkipped != nil {
				observer.dvnSkipped("unknown_pathway", db.PacketRecord{}, db.DVNJobRecord{}, log)
			}
			continue
		}
		if err != nil {
			return result, err
		}
		if packet.DstEID != dstEID {
			return result, fmt.Errorf("packet %s destination eid %d does not match indexed chain %d", packet.GUID, packet.DstEID, dstEID)
		}
		pathway, ok := pathwayForPacket(pathways, packet)
		if !ok {
			if observer.dvnSkipped != nil {
				observer.dvnSkipped("unknown_pathway", packet, db.DVNJobRecord{}, log)
			}
			continue
		}
		if event.DVN != pathway.DestinationWorkers.OpenDVN {
			if observer.dvnSkipped != nil {
				observer.dvnSkipped("unexpected_worker", packet, db.DVNJobRecord{}, log)
			}
			continue
		}
		if err := validatePayloadVerified(packet, event); err != nil {
			return result, err
		}
		job, err := store.GetDVNJob(ctx, packet.GUID)
		if errors.Is(err, pgx.ErrNoRows) {
			if pathway.Enabled {
				result.pending = true
				if observer.dvnSkipped != nil {
					observer.dvnSkipped("pending_dvn_job", packet, db.DVNJobRecord{}, log)
				}
			} else if observer.dvnSkipped != nil {
				observer.dvnSkipped("missing_dvn_job", packet, db.DVNJobRecord{}, log)
			}
			continue
		}
		if err != nil {
			return result, err
		}
		if job.Status == string(packets.DVNVerified) {
			if observer.dvnSkipped != nil {
				observer.dvnSkipped("already_applied", packet, job, log)
			}
			continue
		}
		if !dvnCanApplyPayloadVerified(job.Status) {
			if observer.dvnSkipped != nil {
				observer.dvnSkipped("status_not_applicable", packet, job, log)
			}
			continue
		}
		if err := store.MarkDVNVerifiedObserved(ctx, packet.GUID, log.TxHash, job.Status); err != nil {
			return result, err
		}
		if observer.dvnApplied != nil {
			observer.dvnApplied(packet, job, log)
		}
		result.applied++
	}
	return result, nil
}

type destinationPacketIdentity struct {
	SrcEID   uint32
	DstEID   uint32
	Sender   common.Address
	Receiver common.Address
	Nonce    uint64
}

func executorSourcePacketWasSkipped(ctx context.Context, store ExecutorDestinationStore, dstEID uint32, expectedExecutor common.Address, log gethtypes.Log) (bool, error) {
	identity, ok, err := executorDestinationLogIdentity(dstEID, expectedExecutor, log)
	if err != nil || !ok {
		return false, err
	}
	return sourcePacketWasSkipped(ctx, store, sourceRoleExecutor, identity)
}

func dvnSourcePacketWasSkipped(ctx context.Context, store DVNDestinationStore, dstEID uint32, event lzabi.PayloadVerified) (bool, error) {
	header, err := lz.DecodePacketV1Header(event.Header)
	if err != nil {
		return false, nil
	}
	return sourcePacketWasSkipped(ctx, store, sourceRoleDVN, destinationPacketIdentity{
		SrcEID:   header.SrcEID,
		DstEID:   dstEID,
		Sender:   header.Sender,
		Receiver: header.Receiver,
		Nonce:    header.Nonce,
	})
}

func sourcePacketWasSkipped(ctx context.Context, store SourcePacketSkipStore, role string, identity destinationPacketIdentity) (bool, error) {
	_, err := store.GetSourcePacketSkip(ctx, role, identity.SrcEID, identity.DstEID, identity.Sender, identity.Receiver, identity.Nonce)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func pathwayForPacket(pathways []chain.Pathway, packet db.PacketRecord) (chain.Pathway, bool) {
	for _, pathway := range pathways {
		if pathway.SrcEID == packet.SrcEID &&
			pathway.DstEID == packet.DstEID &&
			pathway.SrcOApp == packet.Sender &&
			pathway.DstOApp == packet.Receiver {
			return pathway, true
		}
	}
	return chain.Pathway{}, false
}

func pathwayForDestinationIdentity(pathways []chain.Pathway, dstEID, srcEID uint32, sender, receiver common.Address) (chain.Pathway, bool) {
	for _, pathway := range pathways {
		if pathway.SrcEID == srcEID &&
			pathway.DstEID == dstEID &&
			pathway.SrcOApp == sender &&
			pathway.DstOApp == receiver {
			return pathway, true
		}
	}
	return chain.Pathway{}, false
}

func executorDestinationLogIdentity(dstEID uint32, expectedExecutor common.Address, log gethtypes.Log) (destinationPacketIdentity, bool, error) {
	if len(log.Topics) == 0 {
		return destinationPacketIdentity{}, false, nil
	}
	switch log.Topics[0] {
	case lzabi.PacketVerifiedTopic():
		event, err := lzabi.DecodePacketVerified(log)
		if err != nil {
			return destinationPacketIdentity{}, false, err
		}
		return destinationPacketIdentity{SrcEID: event.Origin.SrcEID, DstEID: dstEID, Sender: originSenderAddress(event.Origin), Receiver: event.Receiver, Nonce: event.Origin.Nonce}, true, nil
	case lzabi.PacketDeliveredTopic():
		event, err := lzabi.DecodePacketDelivered(log)
		if err != nil {
			return destinationPacketIdentity{}, false, err
		}
		return destinationPacketIdentity{SrcEID: event.Origin.SrcEID, DstEID: dstEID, Sender: originSenderAddress(event.Origin), Receiver: event.Receiver, Nonce: event.Origin.Nonce}, true, nil
	case lzabi.LzReceiveAlertTopic():
		event, err := lzabi.DecodeLzReceiveAlert(log)
		if err != nil {
			return destinationPacketIdentity{}, false, err
		}
		if expectedExecutor != (common.Address{}) && event.Executor != expectedExecutor {
			return destinationPacketIdentity{}, false, nil
		}
		return destinationPacketIdentity{SrcEID: event.Origin.SrcEID, DstEID: dstEID, Sender: originSenderAddress(event.Origin), Receiver: event.Receiver, Nonce: event.Origin.Nonce}, true, nil
	default:
		return destinationPacketIdentity{}, false, nil
	}
}

func executorDestinationLogPathway(pathways []chain.Pathway, dstEID uint32, expectedExecutor common.Address, log gethtypes.Log) (chain.Pathway, bool, error) {
	if len(pathways) == 0 {
		return chain.Pathway{}, false, nil
	}
	identity, ok, err := executorDestinationLogIdentity(dstEID, expectedExecutor, log)
	if err != nil {
		return chain.Pathway{}, false, err
	}
	if !ok {
		return chain.Pathway{}, false, nil
	}
	pathway, ok := pathwayForDestinationIdentity(pathways, identity.DstEID, identity.SrcEID, identity.Sender, identity.Receiver)
	return pathway, ok, nil
}

func executorDestinationLogExecutorAllowed(log gethtypes.Log, expectedExecutor common.Address) bool {
	if expectedExecutor == (common.Address{}) || len(log.Topics) == 0 || log.Topics[0] != lzabi.LzReceiveAlertTopic() {
		return true
	}
	event, err := lzabi.DecodeLzReceiveAlert(log)
	return err == nil && event.Executor == expectedExecutor
}

func pathwayForPayloadVerified(pathways []chain.Pathway, dstEID uint32, event lzabi.PayloadVerified) (chain.Pathway, bool) {
	header, err := lz.DecodePacketV1Header(event.Header)
	if err != nil {
		return chain.Pathway{}, false
	}
	return pathwayForDestinationIdentity(pathways, dstEID, header.SrcEID, header.Sender, header.Receiver)
}

func executorDestinationEventName(topic common.Hash) string {
	switch topic {
	case lzabi.PacketVerifiedTopic():
		return "PacketVerified"
	case lzabi.PacketDeliveredTopic():
		return "PacketDelivered"
	case lzabi.LzReceiveAlertTopic():
		return "LzReceiveAlert"
	default:
		return "unknown"
	}
}

func executorDestinationTargetStatus(topic common.Hash) string {
	switch topic {
	case lzabi.PacketVerifiedTopic():
		return string(packets.ExecutorCommitted)
	case lzabi.PacketDeliveredTopic():
		return string(packets.ExecutorDelivered)
	case lzabi.LzReceiveAlertTopic():
		return string(packets.ExecutorLzReceiveFailed)
	default:
		return ""
	}
}

func logTopic(log gethtypes.Log) common.Hash {
	if len(log.Topics) == 0 {
		return common.Hash{}
	}
	return log.Topics[0]
}

// ApplyExecutorDestinationLog applies one validated destination EndpointV2 log to executor state.
func ApplyExecutorDestinationLog(ctx context.Context, store ExecutorReceiptStore, packet db.PacketRecord, job db.ExecutorJobRecord, log gethtypes.Log) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("executor receipt store is required")
	}
	if err := packet.Validate(); err != nil {
		return false, err
	}
	if job.GUID != packet.GUID {
		return false, fmt.Errorf("executor job %s does not match packet %s", job.GUID, packet.GUID)
	}
	if len(log.Topics) == 0 {
		return false, nil
	}
	if executorDestinationLogAlreadyApplied(job.Status, log.Topics[0]) {
		return false, nil
	}
	switch log.Topics[0] {
	case lzabi.PacketVerifiedTopic():
		event, err := lzabi.DecodePacketVerified(log)
		if err != nil {
			return false, err
		}
		if err := validatePacketVerified(packet, event); err != nil {
			return false, err
		}
		if !executorCanApplyPacketVerified(job.Status) {
			return false, fmt.Errorf("executor PacketVerified cannot apply from status %s", job.Status)
		}
		return true, store.MarkExecutorCommittedObserved(ctx, packet.GUID, log.TxHash, job.Status)
	case lzabi.PacketDeliveredTopic():
		event, err := lzabi.DecodePacketDelivered(log)
		if err != nil {
			return false, err
		}
		if err := validatePacketDelivered(packet, event); err != nil {
			return false, err
		}
		return true, store.MarkExecutorDeliveredObserved(ctx, packet.GUID, log.TxHash, job.Status)
	case lzabi.LzReceiveAlertTopic():
		event, err := lzabi.DecodeLzReceiveAlert(log)
		if err != nil {
			return false, err
		}
		if err := validateLzReceiveAlert(packet, event); err != nil {
			return false, err
		}
		if !executorCanApplyLzReceiveAlert(job.Status) {
			return false, fmt.Errorf("executor LzReceiveAlert cannot apply from status %s", job.Status)
		}
		return true, store.MarkExecutorReceiveFailedObserved(ctx, packet.GUID, log.TxHash, job.Status, hex.EncodeToString(event.Reason))
	default:
		return false, nil
	}
}

func executorCanApplyPacketVerified(status string) bool {
	switch status {
	case string(packets.ExecutorAssigned),
		string(packets.ExecutorWaitingDVNVerification),
		string(packets.ExecutorVerifiable),
		string(packets.ExecutorCommitTxEnqueued):
		return true
	default:
		return false
	}
}

func executorCanApplyLzReceiveAlert(status string) bool {
	switch status {
	case string(packets.ExecutorCommitTxEnqueued),
		string(packets.ExecutorCommitted),
		string(packets.ExecutorExecutable),
		string(packets.ExecutorLzReceiveTxEnqueued):
		return true
	default:
		return false
	}
}

func dvnCanApplyPayloadVerified(status string) bool {
	switch status {
	case string(packets.DVNVerified),
		string(packets.DVNReorgDetected):
		return false
	default:
		return status != ""
	}
}

func packetForDestinationLog(ctx context.Context, store ExecutorDestinationStore, dstEID uint32, log gethtypes.Log) (db.PacketRecord, bool, error) {
	if len(log.Topics) == 0 {
		return db.PacketRecord{}, false, nil
	}
	switch log.Topics[0] {
	case lzabi.PacketVerifiedTopic():
		event, err := lzabi.DecodePacketVerified(log)
		if err != nil {
			return db.PacketRecord{}, false, err
		}
		packet, err := store.GetPacketByDestination(ctx, dstEID, event.Origin.SrcEID, originSenderAddress(event.Origin), event.Receiver, event.Origin.Nonce)
		return packet, true, err
	case lzabi.PacketDeliveredTopic():
		event, err := lzabi.DecodePacketDelivered(log)
		if err != nil {
			return db.PacketRecord{}, false, err
		}
		packet, err := store.GetPacketByDestination(ctx, dstEID, event.Origin.SrcEID, originSenderAddress(event.Origin), event.Receiver, event.Origin.Nonce)
		return packet, true, err
	case lzabi.LzReceiveAlertTopic():
		event, err := lzabi.DecodeLzReceiveAlert(log)
		if err != nil {
			return db.PacketRecord{}, false, err
		}
		packet, err := store.GetPacket(ctx, event.GUID)
		return packet, true, err
	default:
		return db.PacketRecord{}, false, nil
	}
}

func validatePacketVerified(packet db.PacketRecord, event lzabi.PacketVerified) error {
	if err := validateOrigin(packet, event.Origin); err != nil {
		return err
	}
	if event.Receiver != packet.Receiver {
		return fmt.Errorf("PacketVerified receiver %s does not match packet receiver %s", event.Receiver, packet.Receiver)
	}
	if event.PayloadHash != packet.PayloadHash {
		return fmt.Errorf("PacketVerified payload hash %s does not match packet payload hash %s", event.PayloadHash, packet.PayloadHash)
	}
	return nil
}

func validatePacketDelivered(packet db.PacketRecord, event lzabi.PacketDelivered) error {
	if err := validateOrigin(packet, event.Origin); err != nil {
		return err
	}
	if event.Receiver != packet.Receiver {
		return fmt.Errorf("PacketDelivered receiver %s does not match packet receiver %s", event.Receiver, packet.Receiver)
	}
	return nil
}

func validateLzReceiveAlert(packet db.PacketRecord, event lzabi.LzReceiveAlert) error {
	if err := validateOrigin(packet, event.Origin); err != nil {
		return err
	}
	if event.Receiver != packet.Receiver {
		return fmt.Errorf("LzReceiveAlert receiver %s does not match packet receiver %s", event.Receiver, packet.Receiver)
	}
	if event.GUID != packet.GUID {
		return fmt.Errorf("LzReceiveAlert guid %s does not match packet guid %s", event.GUID, packet.GUID)
	}
	if string(event.Message) != string(packet.Message) {
		return fmt.Errorf("LzReceiveAlert message does not match packet message")
	}
	return nil
}

func validatePayloadVerified(packet db.PacketRecord, event lzabi.PayloadVerified) error {
	if string(event.Header) != string(packet.PacketHeader) {
		return fmt.Errorf("PayloadVerified header does not match packet header")
	}
	if event.ProofHash != packet.PayloadHash {
		return fmt.Errorf("PayloadVerified proof hash %s does not match packet payload hash %s", event.ProofHash, packet.PayloadHash)
	}
	if event.Confirmations == nil || event.Confirmations.Sign() <= 0 {
		return fmt.Errorf("PayloadVerified confirmations must be positive")
	}
	return nil
}

func validateOrigin(packet db.PacketRecord, origin lzabi.Origin) error {
	if origin.SrcEID != packet.SrcEID {
		return fmt.Errorf("origin source eid %d does not match packet source eid %d", origin.SrcEID, packet.SrcEID)
	}
	if origin.Sender != common.BytesToHash(packet.Sender.Bytes()) {
		return fmt.Errorf("origin sender %s does not match packet sender %s", origin.Sender, packet.Sender)
	}
	if packet.Nonce == nil || !packet.Nonce.IsUint64() {
		return fmt.Errorf("packet nonce %s does not fit uint64", packet.Nonce)
	}
	if origin.Nonce != packet.Nonce.Uint64() {
		return fmt.Errorf("origin nonce %d does not match packet nonce %s", origin.Nonce, packet.Nonce)
	}
	return nil
}

func originSenderAddress(origin lzabi.Origin) common.Address {
	return common.BytesToAddress(origin.Sender.Bytes()[12:])
}
