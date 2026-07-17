package indexer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/lzabi"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
	"github.com/islishude/oh-my-lazier/go/internal/workerloop"
	"github.com/jackc/pgx/v5"
)

const (
	defaultPollInterval    = 5 * time.Second
	defaultBackfillRange   = uint64(500)
	defaultQueryBlockRange = uint64(500)
)

// DefaultProgressLogInterval is the default minimum interval between indexer progress Info logs.
const DefaultProgressLogInterval = time.Minute

const (
	// ExecutorSourceStream tracks source-chain OpenExecutor assignments.
	ExecutorSourceStream = "executor_source"
	// ExecutorDestinationStream tracks destination-chain executor outcomes.
	ExecutorDestinationStream = "executor_destination"
	// DVNSourceStream tracks source-chain OpenDVN assignments.
	DVNSourceStream = "dvn_source"
	// DVNDestinationStream tracks destination-chain OpenDVN verification outcomes.
	DVNDestinationStream = "dvn_destination"
)

const (
	sourceRoleExecutor = "executor"
	sourceRoleDVN      = "dvn"
)

type cursorSkipReason string

const (
	cursorSkipCaughtUp               cursorSkipReason = "cursor_caught_up"
	cursorSkipStartBlockNotConfirmed cursorSkipReason = "start_block_not_confirmed"
)

// StreamSet selects the durable indexer streams this process advances.
type StreamSet struct {
	ExecutorSource      bool
	ExecutorDestination bool
	DVNSource           bool
	DVNDestination      bool
}

// StreamsForRoles returns the indexer streams required by enabled worker roles.
func StreamsForRoles(executorEnabled, dvnEnabled bool) StreamSet {
	return StreamSet{
		ExecutorSource:      executorEnabled,
		ExecutorDestination: executorEnabled,
		DVNSource:           dvnEnabled,
		DVNDestination:      dvnEnabled,
	}
}

// Empty reports whether no indexer stream is enabled.
func (s StreamSet) Empty() bool {
	return !s.ExecutorSource && !s.ExecutorDestination && !s.DVNSource && !s.DVNDestination
}

func allStreams() StreamSet {
	return StreamSet{
		ExecutorSource:      true,
		ExecutorDestination: true,
		DVNSource:           true,
		DVNDestination:      true,
	}
}

// Store persists indexed executor source records and destination outcomes.
type Store interface {
	DestinationStore
	GetIndexerCursor(ctx context.Context, chainEID uint32, stream string) (uint64, error)
	UpdateIndexerCursor(ctx context.Context, chainEID uint32, stream string, lastBlock uint64) error
	RecordSourcePacketSkip(ctx context.Context, skip db.SourcePacketSkip) error
	UpsertPacket(ctx context.Context, packet db.PacketRecord) error
	UpsertExecutorJob(ctx context.Context, job db.ExecutorJobRecord) error
	UpsertDVNJob(ctx context.Context, job db.DVNJobRecord) error
}

// LogClient reads chain heads and historical EVM logs.
type LogClient interface {
	BlockNumber(ctx context.Context) (uint64, error)
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]gethtypes.Log, error)
}

// MetricsRecorder records process-local indexer polling outcomes.
type MetricsRecorder interface {
	RecordIndexerPoll(chainEID uint32, chainName string, observedHeadBlock uint64, confirmedToBlock uint64, sourceTransactions int, dvnTransactions int, destinationLogs int, duration time.Duration, err error)
}

// Indexer watches one chain for LayerZero and worker contract events.
type Indexer struct {
	chain               chain.Chain
	sourcePathways      []chain.Pathway
	destinationPathways []chain.Pathway
	destinationEID      uint32
	store               Store
	client              LogClient
	pollInterval        time.Duration
	backfillRange       uint64
	queryBlockRange     uint64
	progressLogInterval time.Duration
	lastProgressLogs    map[string]time.Time
	logger              *slog.Logger
	metrics             MetricsRecorder
	streams             StreamSet
	now                 func() time.Time
}

// New creates an indexer for one configured chain.
func New(configuredChain chain.Chain, pathways []chain.Pathway, store Store, logger *slog.Logger) *Indexer {
	return NewWithClient(configuredChain, pathways, store, configuredChain.RPC, logger)
}

// NewWithClient creates an indexer with an explicit log client for tests.
func NewWithClient(configuredChain chain.Chain, pathways []chain.Pathway, store Store, client LogClient, logger *slog.Logger) *Indexer {
	sourcePathways := make([]chain.Pathway, 0)
	destinationPathways := make([]chain.Pathway, 0)
	for _, pathway := range pathways {
		if pathway.SrcEID == configuredChain.EID {
			sourcePathways = append(sourcePathways, pathway)
		}
		if pathway.DstEID == configuredChain.EID {
			destinationPathways = append(destinationPathways, pathway)
		}
	}
	queryBlockRange := configuredChain.IndexerQueryBlockRange
	if queryBlockRange == 0 {
		queryBlockRange = defaultQueryBlockRange
	}
	return &Indexer{
		chain:               configuredChain,
		sourcePathways:      sourcePathways,
		destinationPathways: destinationPathways,
		destinationEID:      configuredChain.EID,
		store:               store,
		client:              client,
		pollInterval:        defaultPollInterval,
		backfillRange:       defaultBackfillRange,
		queryBlockRange:     queryBlockRange,
		progressLogInterval: DefaultProgressLogInterval,
		lastProgressLogs:    make(map[string]time.Time),
		logger:              logger,
		streams:             allStreams(),
		now:                 time.Now,
	}
}

// WithStreams selects the durable streams this indexer advances.
func (i *Indexer) WithStreams(streams StreamSet) *Indexer {
	i.streams = streams
	return i
}

// WithMetrics records process-local indexer polling metrics.
func (i *Indexer) WithMetrics(metrics MetricsRecorder) *Indexer {
	i.metrics = metrics
	return i
}

// WithProgressLogInterval sets the minimum interval between indexer progress Info logs.
func (i *Indexer) WithProgressLogInterval(interval time.Duration) *Indexer {
	i.progressLogInterval = interval
	return i
}

// Run starts the chain indexer loop until the context is canceled.
func (i *Indexer) Run(ctx context.Context) error {
	i.logger.Info("indexer loop started", "chain", i.chain.Name, "eid", i.chain.EID)
	return i.runPollingLoop(ctx)
}

// ProcessOnce backfills one confirmed log window for source and destination executor events.
func (i *Indexer) ProcessOnce(ctx context.Context) (ProcessResult, error) {
	if i.store == nil {
		return ProcessResult{}, errors.New("indexer store is required")
	}
	if i.client == nil {
		return ProcessResult{}, errors.New("indexer log client is required")
	}
	head, err := i.client.BlockNumber(ctx)
	if err != nil {
		return ProcessResult{}, err
	}
	if head < i.chain.Confirmations {
		return ProcessResult{ObservedHeadBlock: head}, nil
	}
	confirmedTo := head - i.chain.Confirmations
	result := ProcessResult{ConfirmedToBlock: confirmedTo, ObservedHeadBlock: head}
	sourceWindowSet := false
	destinationWindowSet := false
	if i.streams.ExecutorSource {
		from, to, ok, skipReason, err := i.cursorWindow(ctx, ExecutorSourceStream, confirmedTo)
		if err != nil {
			return ProcessResult{}, err
		}
		if !ok {
			i.logCursorSkip(ExecutorSourceStream, skipReason, confirmedTo)
		} else {
			source, dvn, err := i.processSourceWindow(ctx, from, to, sourceRoleExecutor)
			if err != nil {
				return ProcessResult{}, err
			}
			if err := i.store.UpdateIndexerCursor(ctx, i.chain.EID, ExecutorSourceStream, to); err != nil {
				return ProcessResult{}, err
			}
			result.SourceFromBlock = from
			result.SourceToBlock = to
			sourceWindowSet = true
			result.SourceTransactions += source
			result.DVNTransactions += dvn
			result.addStreamProgress(ExecutorSourceStream, from, to, source, dvn, 0)
		}
	}
	if i.streams.DVNSource {
		from, to, ok, skipReason, err := i.cursorWindow(ctx, DVNSourceStream, confirmedTo)
		if err != nil {
			return ProcessResult{}, err
		}
		if !ok {
			i.logCursorSkip(DVNSourceStream, skipReason, confirmedTo)
		} else {
			source, dvn, err := i.processSourceWindow(ctx, from, to, sourceRoleDVN)
			if err != nil {
				return ProcessResult{}, err
			}
			if err := i.store.UpdateIndexerCursor(ctx, i.chain.EID, DVNSourceStream, to); err != nil {
				return ProcessResult{}, err
			}
			if !sourceWindowSet {
				result.SourceFromBlock = from
				result.SourceToBlock = to
			}
			result.SourceTransactions += source
			result.DVNTransactions += dvn
			result.addStreamProgress(DVNSourceStream, from, to, source, dvn, 0)
		}
	}
	if i.streams.ExecutorDestination {
		from, to, ok, skipReason, err := i.cursorWindow(ctx, ExecutorDestinationStream, confirmedTo)
		if err != nil {
			return ProcessResult{}, err
		}
		if !ok {
			i.logCursorSkip(ExecutorDestinationStream, skipReason, confirmedTo)
		} else {
			destination, err := i.processDestinationWindow(ctx, from, to, sourceRoleExecutor)
			if err != nil {
				return ProcessResult{}, err
			}
			result.DestinationFromBlock = from
			result.DestinationToBlock = to
			destinationWindowSet = true
			result.DestinationLogs += destination.applied
			if destination.pending {
				i.logger.Debug("deferred indexer destination cursor", "chain", i.chain.Name, "eid", i.chain.EID, "stream", ExecutorDestinationStream, "reason", "pending_source_state", "from_block", from, "to_block", to)
			} else {
				if err := i.store.UpdateIndexerCursor(ctx, i.chain.EID, ExecutorDestinationStream, to); err != nil {
					return ProcessResult{}, err
				}
				result.addStreamProgress(ExecutorDestinationStream, from, to, 0, 0, destination.applied)
			}
		}
	}
	if i.streams.DVNDestination {
		from, to, ok, skipReason, err := i.cursorWindow(ctx, DVNDestinationStream, confirmedTo)
		if err != nil {
			return ProcessResult{}, err
		}
		if !ok {
			i.logCursorSkip(DVNDestinationStream, skipReason, confirmedTo)
		} else {
			destination, err := i.processDestinationWindow(ctx, from, to, sourceRoleDVN)
			if err != nil {
				return ProcessResult{}, err
			}
			if !destinationWindowSet {
				result.DestinationFromBlock = from
				result.DestinationToBlock = to
			}
			result.DestinationLogs += destination.applied
			if destination.pending {
				i.logger.Debug("deferred indexer destination cursor", "chain", i.chain.Name, "eid", i.chain.EID, "stream", DVNDestinationStream, "reason", "pending_source_state", "from_block", from, "to_block", to)
			} else {
				if err := i.store.UpdateIndexerCursor(ctx, i.chain.EID, DVNDestinationStream, to); err != nil {
					return ProcessResult{}, err
				}
				result.addStreamProgress(DVNDestinationStream, from, to, 0, 0, destination.applied)
			}
		}
	}
	return result, nil
}

// ProcessResult summarizes one indexer polling pass.
type ProcessResult struct {
	ConfirmedToBlock     uint64
	ObservedHeadBlock    uint64
	SourceFromBlock      uint64
	SourceToBlock        uint64
	DestinationFromBlock uint64
	DestinationToBlock   uint64
	SourceTransactions   int
	DVNTransactions      int
	DestinationLogs      int
	streamProgress       [4]streamProgress
	streamProgressCount  int
}

type streamProgress struct {
	stream             string
	fromBlock          uint64
	toBlock            uint64
	sourceTransactions int
	dvnTransactions    int
	destinationLogs    int
}

func (r *ProcessResult) addStreamProgress(stream string, from, to uint64, sourceTransactions, dvnTransactions, destinationLogs int) {
	if r.streamProgressCount >= len(r.streamProgress) {
		return
	}
	r.streamProgress[r.streamProgressCount] = streamProgress{
		stream:             stream,
		fromBlock:          from,
		toBlock:            to,
		sourceTransactions: sourceTransactions,
		dvnTransactions:    dvnTransactions,
		destinationLogs:    destinationLogs,
	}
	r.streamProgressCount++
}

func (i *Indexer) processSourceWindow(ctx context.Context, from, to uint64, role string) (int, int, error) {
	if len(i.sourcePathways) == 0 {
		return 0, 0, nil
	}
	executorProcessed := 0
	dvnProcessed := 0
	for chunkFrom := from; chunkFrom <= to; {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		chunkTo := i.chunkToBlock(chunkFrom, to)
		logs, err := i.client.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: blockNumber(chunkFrom),
			ToBlock:   blockNumber(chunkTo),
			Addresses: i.sourceAddresses(role),
			Topics:    [][]common.Hash{i.sourceTopics(role)},
		})
		if err != nil {
			return executorProcessed, dvnProcessed, err
		}
		executor, dvn, err := i.processSourceLogs(ctx, logs, role)
		if err != nil {
			return executorProcessed, dvnProcessed, err
		}
		executorProcessed += executor
		dvnProcessed += dvn
		if chunkTo == to {
			break
		}
		chunkFrom = chunkTo + 1
	}
	return executorProcessed, dvnProcessed, nil
}

func (i *Indexer) processSourceLogs(ctx context.Context, logs []gethtypes.Log, role string) (int, int, error) {
	var current sourceTxLogs
	executorProcessed := 0
	dvnProcessed := 0
	for _, log := range logs {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if len(current.logs) > 0 && current.txHash != log.TxHash {
			executor, dvn, err := i.processSourceTxLogs(ctx, current, role)
			if err != nil {
				return executorProcessed, dvnProcessed, err
			}
			executorProcessed += executor
			dvnProcessed += dvn
			current.reset()
		}
		current.append(log)
	}
	if len(current.logs) == 0 {
		return executorProcessed, dvnProcessed, nil
	}
	executor, dvn, err := i.processSourceTxLogs(ctx, current, role)
	if err != nil {
		return executorProcessed, dvnProcessed, err
	}
	return executorProcessed + executor, dvnProcessed + dvn, nil
}

func (i *Indexer) processSourceTxLogs(ctx context.Context, tx sourceTxLogs, role string) (int, int, error) {
	executorProcessed := 0
	dvnProcessed := 0
	relevantLogs, err := i.sourceTxLogsForPathways(ctx, tx.logs, role)
	if err != nil || len(relevantLogs) == 0 {
		return 0, 0, err
	}
	// Executor fee and packet logs also identify assignments made to workers whose
	// contract logs are outside this indexer's configured address filter.
	if role == sourceRoleExecutor {
		processed, err := i.processExecutorSourceTx(ctx, relevantLogs)
		if err != nil {
			return executorProcessed, dvnProcessed, err
		}
		executorProcessed += processed
	}
	if role == sourceRoleDVN {
		processed, err := i.processDVNSourceTx(ctx, relevantLogs)
		if err != nil {
			return executorProcessed, dvnProcessed, err
		}
		dvnProcessed += processed
	}
	return executorProcessed, dvnProcessed, nil
}

type sourceTxLogs struct {
	txHash common.Hash
	logs   []gethtypes.Log
}

func (l *sourceTxLogs) append(log gethtypes.Log) {
	if len(l.logs) == 0 {
		l.txHash = log.TxHash
	}
	l.logs = append(l.logs, log)
}

func (l *sourceTxLogs) reset() {
	l.logs = l.logs[:0]
}

func (i *Indexer) sourceTxLogsForPathways(ctx context.Context, logs []gethtypes.Log, role string) ([]gethtypes.Log, error) {
	ordered, err := orderedSourceTxLogs(logs)
	if err != nil {
		return nil, err
	}
	relevant := make([]gethtypes.Log, 0, len(ordered))
	segment := make([]gethtypes.Log, 0)
	for _, log := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment = append(segment, log)
		if !logHasTopic(log, lzabi.PacketSentTopic()) {
			continue
		}
		if log.Address != i.chain.EndpointAddress {
			segment = segment[:0]
			continue
		}
		route, event, err := packetRouteFromSentLog(log)
		if err != nil {
			return nil, err
		}
		pathway, ok := i.sourcePathwayIdentity(route.SrcEID, route.DstEID, route.Sender, route.Receiver)
		if !ok {
			segment = segment[:0]
			continue
		}
		if event.SendLibrary != pathway.SendLib {
			packet, err := PacketRecordFromSentLog(log)
			if err != nil {
				return nil, err
			}
			if err := i.recordSourcePacketSkip(ctx, role, packet, "unexpected_send_library", common.Address{}); err != nil {
				return nil, err
			}
			i.logger.Debug("skipped source packet", "role", role, "reason", "unexpected_send_library", "guid", packet.GUID, "src_eid", packet.SrcEID, "dst_eid", packet.DstEID, "tx_hash", packet.SrcTxHash, "send_library", event.SendLibrary, "expected_send_library", pathway.SendLib)
			segment = segment[:0]
			continue
		}
		relevant = append(relevant, segment...)
		segment = segment[:0]
	}
	assignmentTopic := lzabi.ExecutorJobAssignedTopic()
	if role == sourceRoleDVN {
		assignmentTopic = lzabi.DVNJobAssignedTopic()
	}
	for _, log := range segment {
		if logHasTopic(log, assignmentTopic) {
			relevant = append(relevant, segment...)
			break
		}
	}
	return relevant, nil
}

func (i *Indexer) chunkToBlock(from, limit uint64) uint64 {
	if i.queryBlockRange == 0 {
		return limit
	}
	to := from + i.queryBlockRange - 1
	if to < from || to > limit {
		return limit
	}
	return to
}

func (i *Indexer) processExecutorSourceTx(ctx context.Context, txLogs []gethtypes.Log) (int, error) {
	records, gaps, err := decodeExecutorSourceTxLogs(txLogs)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		pathway, ok := i.sourcePathway(record.Packet)
		if !ok {
			i.logger.Debug("skipped executor source assignment", "reason", "unknown_pathway", "guid", record.Packet.GUID, "src_eid", record.Packet.SrcEID, "dst_eid", record.Packet.DstEID, "tx_hash", record.Packet.SrcTxHash)
			continue
		}
		if !pathway.Enabled {
			if err := i.recordSourcePacketSkip(ctx, sourceRoleExecutor, record.Packet, "pathway_disabled", record.Executor); err != nil {
				return processed, err
			}
			i.logger.Debug("skipped executor source assignment", "reason", "pathway_disabled", "guid", record.Packet.GUID, "src_eid", record.Packet.SrcEID, "dst_eid", record.Packet.DstEID, "tx_hash", record.Packet.SrcTxHash)
			continue
		}
		if record.Executor != pathway.SourceWorkers.OpenExecutor {
			if err := i.recordSourcePacketSkip(ctx, sourceRoleExecutor, record.Packet, "unexpected_worker", record.Executor); err != nil {
				return processed, err
			}
			i.logger.Debug("skipped executor source assignment", "reason", "unexpected_worker", "guid", record.Packet.GUID, "src_eid", record.Packet.SrcEID, "dst_eid", record.Packet.DstEID, "tx_hash", record.Packet.SrcTxHash, "worker", record.Executor, "expected_worker", pathway.SourceWorkers.OpenExecutor)
			continue
		}
		record.Packet.Status = record.ExecutorJob.Status
		if err := i.store.UpsertPacket(ctx, record.Packet); err != nil {
			return processed, err
		}
		if err := i.store.UpsertExecutorJob(ctx, record.ExecutorJob); err != nil {
			return processed, err
		}
		i.logger.Info("indexed executor source assignment", "guid", record.Packet.GUID, "src_eid", record.Packet.SrcEID, "dst_eid", record.Packet.DstEID, "tx_hash", record.Packet.SrcTxHash, "block_number", record.Packet.SrcBlockNumber, "log_index", record.Packet.SrcLogIndex, "status", record.ExecutorJob.Status)
		processed++
	}
	if err := i.recordExecutorSourceGaps(ctx, gaps); err != nil {
		return processed, err
	}
	return processed, nil
}

func (i *Indexer) recordExecutorSourceGaps(ctx context.Context, gaps []executorSourcePacketGap) error {
	for _, gap := range gaps {
		if err := ctx.Err(); err != nil {
			return err
		}
		pathway, ok := i.sourcePathway(gap.Packet)
		if !ok {
			i.logger.Debug("skipped executor source packet", "reason", "unknown_pathway", "guid", gap.Packet.GUID, "src_eid", gap.Packet.SrcEID, "dst_eid", gap.Packet.DstEID, "tx_hash", gap.Packet.SrcTxHash)
			continue
		}
		if pathway.Enabled && gap.Executor == pathway.SourceWorkers.OpenExecutor {
			return fmt.Errorf("packet %s paid configured executor %s but its assignment log is missing", gap.Packet.GUID, gap.Executor)
		}
		reason := "unexpected_worker"
		if !pathway.Enabled {
			reason = "pathway_disabled"
		}
		if err := i.recordSourcePacketSkip(ctx, sourceRoleExecutor, gap.Packet, reason, gap.Executor); err != nil {
			return err
		}
		i.logger.Debug("skipped executor source packet", "reason", reason, "guid", gap.Packet.GUID, "src_eid", gap.Packet.SrcEID, "dst_eid", gap.Packet.DstEID, "tx_hash", gap.Packet.SrcTxHash, "worker", gap.Executor, "expected_worker", pathway.SourceWorkers.OpenExecutor)
	}
	return nil
}

func (i *Indexer) processDVNSourceTx(ctx context.Context, txLogs []gethtypes.Log) (int, error) {
	records, gaps, err := decodeDVNSourceTxLogsForEndpoint(txLogs, i.chain.EndpointAddress)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		pathway, ok := i.sourcePathway(record.Packet)
		if !ok {
			i.logger.Debug("skipped dvn source assignment", "reason", "unknown_pathway", "guid", record.Packet.GUID, "src_eid", record.Packet.SrcEID, "dst_eid", record.Packet.DstEID, "tx_hash", record.Packet.SrcTxHash)
			continue
		}
		if !pathway.Enabled {
			if err := i.recordSourcePacketSkip(ctx, sourceRoleDVN, record.Packet, "pathway_disabled", record.DVN); err != nil {
				return processed, err
			}
			i.logger.Debug("skipped dvn source assignment", "reason", "pathway_disabled", "guid", record.Packet.GUID, "src_eid", record.Packet.SrcEID, "dst_eid", record.Packet.DstEID, "tx_hash", record.Packet.SrcTxHash)
			continue
		}
		if record.DVN != pathway.SourceWorkers.OpenDVN {
			if err := i.recordSourcePacketSkip(ctx, sourceRoleDVN, record.Packet, "unexpected_worker", record.DVN); err != nil {
				return processed, err
			}
			i.logger.Debug("skipped dvn source assignment", "reason", "unexpected_worker", "guid", record.Packet.GUID, "src_eid", record.Packet.SrcEID, "dst_eid", record.Packet.DstEID, "tx_hash", record.Packet.SrcTxHash, "worker", record.DVN, "expected_worker", pathway.SourceWorkers.OpenDVN)
			continue
		}
		if err := i.store.UpsertPacket(ctx, record.Packet); err != nil {
			return processed, err
		}
		if err := i.store.UpsertDVNJob(ctx, record.DVNJob); err != nil {
			return processed, err
		}
		i.logger.Info("indexed dvn source assignment", "guid", record.Packet.GUID, "src_eid", record.Packet.SrcEID, "dst_eid", record.Packet.DstEID, "tx_hash", record.Packet.SrcTxHash, "block_number", record.Packet.SrcBlockNumber, "log_index", record.Packet.SrcLogIndex, "status", record.DVNJob.Status)
		processed++
	}
	if err := i.recordDVNSourceGaps(ctx, gaps); err != nil {
		return processed, err
	}
	return processed, nil
}

func (i *Indexer) recordDVNSourceGaps(ctx context.Context, gaps []dvnSourcePacketGap) error {
	for _, gap := range gaps {
		pathway, ok := i.sourcePathway(gap.Packet)
		if !ok {
			continue
		}
		if !pathway.Enabled {
			if err := i.recordSourcePacketSkip(ctx, sourceRoleDVN, gap.Packet, "pathway_disabled", common.Address{}); err != nil {
				return err
			}
			continue
		}
		worker, includesExpected, err := dvnGapWorker(*gap.Fee, pathway.SourceWorkers.OpenDVN)
		if err != nil {
			return fmt.Errorf("packet %s: %w", gap.Packet.GUID, err)
		}
		if includesExpected {
			return fmt.Errorf("packet %s paid configured dvn %s but its assignment log is missing", gap.Packet.GUID, pathway.SourceWorkers.OpenDVN)
		}
		if err := i.recordSourcePacketSkip(ctx, sourceRoleDVN, gap.Packet, "unexpected_worker", worker); err != nil {
			return err
		}
		i.logger.Debug("skipped dvn source packet", "reason", "unexpected_worker", "guid", gap.Packet.GUID, "src_eid", gap.Packet.SrcEID, "dst_eid", gap.Packet.DstEID, "tx_hash", gap.Packet.SrcTxHash, "worker", worker, "expected_worker", pathway.SourceWorkers.OpenDVN)
	}
	return nil
}

func dvnGapWorker(fee lzabi.DVNFeePaid, expected common.Address) (common.Address, bool, error) {
	dvns := append(append([]common.Address{}, fee.RequiredDVNs...), fee.OptionalDVNs...)
	if len(dvns) != len(fee.Fees) {
		return common.Address{}, false, fmt.Errorf("dvn fee worker count %d does not match fee count %d", len(dvns), len(fee.Fees))
	}
	worker := common.Address{}
	for _, dvn := range dvns {
		if worker == (common.Address{}) {
			worker = dvn
		}
		if dvn == expected {
			return worker, true, nil
		}
	}
	return worker, false, nil
}

func (i *Indexer) recordSourcePacketSkip(ctx context.Context, role string, packet db.PacketRecord, reason string, worker common.Address) error {
	if packet.Nonce == nil || !packet.Nonce.IsUint64() {
		return fmt.Errorf("packet %s nonce is invalid for source skip tombstone", packet.GUID)
	}
	return i.store.RecordSourcePacketSkip(ctx, db.SourcePacketSkip{
		Role:           role,
		SrcEID:         packet.SrcEID,
		DstEID:         packet.DstEID,
		Nonce:          packet.Nonce.Uint64(),
		Sender:         packet.Sender,
		Receiver:       packet.Receiver,
		GUID:           packet.GUID,
		SrcTxHash:      packet.SrcTxHash,
		SrcBlockNumber: packet.SrcBlockNumber,
		SrcLogIndex:    packet.SrcLogIndex,
		Reason:         reason,
		Worker:         worker,
	})
}

func (i *Indexer) processDestinationWindow(ctx context.Context, from, to uint64, role string) (destinationApplyResult, error) {
	result := destinationApplyResult{}
	for chunkFrom := from; chunkFrom <= to; {
		chunkTo := i.chunkToBlock(chunkFrom, to)
		logs, err := i.client.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: blockNumber(chunkFrom),
			ToBlock:   blockNumber(chunkTo),
			Addresses: i.destinationAddresses(role),
			Topics:    [][]common.Hash{i.destinationTopics(role)},
		})
		if err != nil {
			return result, err
		}
		switch role {
		case sourceRoleExecutor:
			executorApplied, err := i.applyExecutorDestinationLogs(ctx, logs)
			result.applied += executorApplied.applied
			result.pending = result.pending || executorApplied.pending
			if err != nil {
				return result, err
			}
		case sourceRoleDVN:
			dvnApplied, err := i.applyDVNDestinationLogs(ctx, logs)
			result.applied += dvnApplied.applied
			result.pending = result.pending || dvnApplied.pending
			if err != nil {
				return result, err
			}
		}
		if chunkTo == to {
			break
		}
		chunkFrom = chunkTo + 1
	}
	return result, nil
}

func (i *Indexer) applyExecutorDestinationLogs(ctx context.Context, logs []gethtypes.Log) (destinationApplyResult, error) {
	expectedExecutor := common.HexToAddress(i.chain.TxRoles.Executor.SignerID)
	return applyExecutorDestinationLogs(ctx, i.store, i.destinationEID, i.destinationPathways, expectedExecutor, logs, destinationLogObserver{
		executorApplied: func(packet db.PacketRecord, job db.ExecutorJobRecord, log gethtypes.Log) {
			topic := logTopic(log)
			i.logger.Info(
				"indexed executor destination event",
				"event", executorDestinationEventName(topic),
				"guid", packet.GUID,
				"src_eid", packet.SrcEID,
				"dst_eid", packet.DstEID,
				"tx_hash", log.TxHash,
				"from_status", job.Status,
				"to_status", executorDestinationTargetStatus(topic),
			)
		},
		executorSkipped: func(reason string, packet db.PacketRecord, job db.ExecutorJobRecord, log gethtypes.Log) {
			i.logger.Debug(
				"skipped executor destination event",
				"reason", reason,
				"event", executorDestinationEventName(logTopic(log)),
				"guid", packet.GUID,
				"src_eid", packet.SrcEID,
				"dst_eid", packet.DstEID,
				"tx_hash", log.TxHash,
				"status", job.Status,
			)
		},
	})
}

func (i *Indexer) applyDVNDestinationLogs(ctx context.Context, logs []gethtypes.Log) (destinationApplyResult, error) {
	return applyDVNDestinationLogs(ctx, i.store, i.destinationEID, i.destinationPathways, logs, destinationLogObserver{
		dvnApplied: func(packet db.PacketRecord, job db.DVNJobRecord, log gethtypes.Log) {
			i.logger.Info(
				"indexed dvn destination event",
				"event", "PayloadVerified",
				"guid", packet.GUID,
				"src_eid", packet.SrcEID,
				"dst_eid", packet.DstEID,
				"tx_hash", log.TxHash,
				"from_status", job.Status,
				"to_status", string(packets.DVNVerified),
			)
		},
		dvnSkipped: func(reason string, packet db.PacketRecord, job db.DVNJobRecord, log gethtypes.Log) {
			i.logger.Debug(
				"skipped dvn destination event",
				"reason", reason,
				"event", "PayloadVerified",
				"guid", packet.GUID,
				"src_eid", packet.SrcEID,
				"dst_eid", packet.DstEID,
				"tx_hash", log.TxHash,
				"status", job.Status,
			)
		},
	})
}

func (i *Indexer) runPollingLoop(ctx context.Context) error {
	if i.pollInterval <= 0 {
		return workerloop.Fatal(errors.New("indexer poll interval must be positive"))
	}
	if i.store == nil {
		return workerloop.Fatal(errors.New("indexer store is required"))
	}
	if i.client == nil {
		return workerloop.Fatal(errors.New("indexer log client is required"))
	}
	if err := i.pollOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(i.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := i.pollOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func (i *Indexer) pollOnce(ctx context.Context) error {
	start := time.Now()
	result, err := i.ProcessOnce(ctx)
	duration := time.Since(start)
	if i.metrics != nil {
		i.metrics.RecordIndexerPoll(
			i.chain.EID,
			i.chain.Name,
			result.ObservedHeadBlock,
			result.ConfirmedToBlock,
			result.SourceTransactions,
			result.DVNTransactions,
			result.DestinationLogs,
			duration,
			err,
		)
		// Surface each provider's quorum classification (including the sticky
		// log-conflict dimension) after every poll, so a flagged provider is
		// visible before its conflicts ever escalate to a stalled cursor.
		if statusSource, ok := i.client.(interface{ Providers() []rpcquorum.Provider }); ok {
			if recorder, ok := i.metrics.(interface {
				RecordRPCProviders(chainEID uint32, chainName string, providers []rpcquorum.Provider)
			}); ok {
				recorder.RecordRPCProviders(i.chain.EID, i.chain.Name, statusSource.Providers())
			}
		}
	}
	if err == nil {
		i.logPollSuccess(result, duration)
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	i.logger.Warn("indexer poll failed; retrying on next interval", "chain", i.chain.Name, "eid", i.chain.EID, "duration", duration, "error", err)
	return nil
}

func (i *Indexer) logCursorSkip(stream string, reason cursorSkipReason, confirmedTo uint64) {
	if reason == "" {
		return
	}
	i.logger.Debug("skipped indexer stream", "chain", i.chain.Name, "eid", i.chain.EID, "stream", stream, "reason", string(reason), "confirmed_to_block", confirmedTo)
}

func (i *Indexer) logPollSuccess(result ProcessResult, duration time.Duration) {
	if result.ObservedHeadBlock < i.chain.Confirmations {
		i.logger.Debug(
			"indexer poll waiting for confirmations",
			"chain", i.chain.Name,
			"eid", i.chain.EID,
			"observed_head_block", result.ObservedHeadBlock,
			"confirmations", i.chain.Confirmations,
			"duration", duration,
		)
		if i.shouldLogProgressInfo() {
			i.logger.Info(
				"indexer progress",
				"chain", i.chain.Name,
				"eid", i.chain.EID,
				"status", "waiting_for_confirmations",
				"observed_head_block", result.ObservedHeadBlock,
				"confirmations", i.chain.Confirmations,
				"duration", duration,
			)
		}
		return
	}
	for _, progress := range result.streamProgress[:result.streamProgressCount] {
		i.logger.Debug(
			"indexer stream advanced",
			"chain", i.chain.Name,
			"eid", i.chain.EID,
			"stream", progress.stream,
			"from_block", progress.fromBlock,
			"to_block", progress.toBlock,
			"confirmed_to_block", result.ConfirmedToBlock,
			"lag_blocks", lagBlocks(result.ConfirmedToBlock, progress.toBlock),
			"source_transactions", progress.sourceTransactions,
			"dvn_transactions", progress.dvnTransactions,
			"destination_logs", progress.destinationLogs,
			"duration", duration,
		)
	}
	i.logger.Debug(
		"indexer poll completed",
		"chain", i.chain.Name,
		"eid", i.chain.EID,
		"observed_head_block", result.ObservedHeadBlock,
		"confirmed_to_block", result.ConfirmedToBlock,
		"streams_advanced", result.streamProgressCount,
		"source_transactions", result.SourceTransactions,
		"dvn_transactions", result.DVNTransactions,
		"destination_logs", result.DestinationLogs,
		"duration", duration,
	)
	if i.shouldLogProgressInfo() {
		i.logProgressSummary(result, duration)
	}
}

func (i *Indexer) shouldLogProgressInfo() bool {
	if i.progressLogInterval <= 0 {
		return false
	}
	if i.lastProgressLogs == nil {
		i.lastProgressLogs = make(map[string]time.Time)
	}
	const key = "progress"
	now := i.currentTime()
	last, ok := i.lastProgressLogs[key]
	if !ok || !now.Before(last.Add(i.progressLogInterval)) {
		i.lastProgressLogs[key] = now
		return true
	}
	return false
}

func (i *Indexer) currentTime() time.Time {
	if i.now == nil {
		return time.Now()
	}
	return i.now()
}

func (i *Indexer) logProgressSummary(result ProcessResult, duration time.Duration) {
	args := []any{
		"chain", i.chain.Name,
		"eid", i.chain.EID,
		"observed_head_block", result.ObservedHeadBlock,
		"confirmed_to_block", result.ConfirmedToBlock,
		"streams_advanced", result.streamProgressCount,
		"source_transactions", result.SourceTransactions,
		"dvn_transactions", result.DVNTransactions,
		"destination_logs", result.DestinationLogs,
		"duration", duration,
	}
	if result.streamProgressCount > 0 {
		streams := make([]string, 0, result.streamProgressCount)
		var fromBlock uint64
		var toBlock uint64
		var lag uint64
		for index, progress := range result.streamProgress[:result.streamProgressCount] {
			streams = append(streams, progress.stream)
			if index == 0 || progress.fromBlock < fromBlock {
				fromBlock = progress.fromBlock
			}
			if progress.toBlock > toBlock {
				toBlock = progress.toBlock
			}
			if progressLag := lagBlocks(result.ConfirmedToBlock, progress.toBlock); progressLag > lag {
				lag = progressLag
			}
		}
		args = append(args,
			"streams", strings.Join(streams, ","),
			"from_block", fromBlock,
			"to_block", toBlock,
			"lag_blocks", lag,
		)
	}
	i.logger.Info("indexer progress", args...)
}

func lagBlocks(confirmedTo, indexedTo uint64) uint64 {
	if indexedTo >= confirmedTo {
		return 0
	}
	return confirmedTo - indexedTo
}

func (i *Indexer) destinationAddresses(role string) []common.Address {
	seen := make(map[common.Address]struct{})
	switch role {
	case sourceRoleExecutor:
		seen[i.chain.EndpointAddress] = struct{}{}
	case sourceRoleDVN:
		for _, pathway := range i.destinationPathways {
			seen[pathway.ReceiveLib] = struct{}{}
		}
	}
	addresses := make([]common.Address, 0, len(seen))
	for address := range seen {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(a, b int) bool {
		return hex.EncodeToString(addresses[a].Bytes()) < hex.EncodeToString(addresses[b].Bytes())
	})
	return addresses
}

func (i *Indexer) sourceAddresses(role string) []common.Address {
	seen := map[common.Address]struct{}{
		i.chain.EndpointAddress: {},
	}
	for _, pathway := range i.sourcePathways {
		seen[pathway.SendLib] = struct{}{}
		switch role {
		case sourceRoleExecutor:
			seen[pathway.SourceWorkers.OpenExecutor] = struct{}{}
		case sourceRoleDVN:
			seen[pathway.SourceWorkers.OpenDVN] = struct{}{}
		}
	}
	addresses := make([]common.Address, 0, len(seen))
	for address := range seen {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(a, b int) bool {
		return hex.EncodeToString(addresses[a].Bytes()) < hex.EncodeToString(addresses[b].Bytes())
	})
	return addresses
}

func (i *Indexer) sourceTopics(role string) []common.Hash {
	switch role {
	case sourceRoleExecutor:
		return []common.Hash{
			lzabi.PacketSentTopic(),
			lzabi.ExecutorFeePaidTopic(),
			lzabi.ExecutorJobAssignedTopic(),
		}
	case sourceRoleDVN:
		return []common.Hash{
			lzabi.PacketSentTopic(),
			lzabi.DVNFeePaidTopic(),
			lzabi.DVNJobAssignedTopic(),
		}
	default:
		return nil
	}
}

func (i *Indexer) destinationTopics(role string) []common.Hash {
	switch role {
	case sourceRoleExecutor:
		return []common.Hash{
			lzabi.PacketVerifiedTopic(),
			lzabi.PacketDeliveredTopic(),
			lzabi.LzReceiveAlertTopic(),
		}
	case sourceRoleDVN:
		return []common.Hash{
			lzabi.PayloadVerifiedTopic(),
		}
	default:
		return nil
	}
}

func (i *Indexer) sourcePathway(packet db.PacketRecord) (chain.Pathway, bool) {
	return i.sourcePathwayIdentity(packet.SrcEID, packet.DstEID, packet.Sender, packet.Receiver)
}

func (i *Indexer) sourcePathwayIdentity(srcEID, dstEID uint32, sender, receiver common.Address) (chain.Pathway, bool) {
	for _, pathway := range i.sourcePathways {
		if pathway.SrcEID == srcEID && pathway.DstEID == dstEID && pathway.SrcOApp == sender && pathway.DstOApp == receiver {
			return pathway, true
		}
	}
	return chain.Pathway{}, false
}

func (i *Indexer) cursorWindow(ctx context.Context, stream string, confirmedTo uint64) (uint64, uint64, bool, cursorSkipReason, error) {
	cursor, err := i.store.GetIndexerCursor(ctx, i.chain.EID, stream)
	cursorExists := true
	if errors.Is(err, pgx.ErrNoRows) {
		cursorExists = false
	} else if err != nil {
		return 0, 0, false, "", err
	}
	from := uint64(0)
	if cursorExists {
		if cursor >= confirmedTo {
			return 0, 0, false, cursorSkipCaughtUp, nil
		}
		from = cursor + 1
	} else {
		from = i.chain.StartBlockNumber
		if from > confirmedTo {
			return 0, 0, false, cursorSkipStartBlockNotConfirmed, nil
		}
	}
	to := confirmedTo
	if i.backfillRange > 0 && to-from+1 > i.backfillRange {
		to = from + i.backfillRange - 1
	}
	return from, to, true, "", nil
}

func blockNumber(number uint64) *big.Int {
	return new(big.Int).SetUint64(number)
}
