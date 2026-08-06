package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/bigutil"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/readiness"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
	"github.com/islishude/oh-my-lazier/go/internal/workerloop"
)

// StatsProvider supplies read-only worker state for metrics rendering.
type StatsProvider interface {
	Stats(context.Context) (db.StatsSnapshot, error)
}

// RuntimeProvider supplies process-local metrics that are not stored in Postgres.
type RuntimeProvider interface {
	RuntimeSnapshot() RuntimeSnapshot
}

// RuntimeSnapshot is a process-local worker metrics snapshot.
type RuntimeSnapshot struct {
	Indexers         []IndexerRuntimeStat
	LoopRetries      []LoopRetryRuntimeStat
	SignerBalances   []SignerBalanceRuntimeStat
	RPCProviders     []RPCProviderRuntimeStat
	PricingSnapshots []PricingSnapshotRuntimeStat
}

// PricingSnapshotRuntimeStat is the latest observed on-chain price snapshot
// for one priced pathway, sampled when the bot reads its gate baseline. The
// age is computed from the stored on-chain updatedAt at snapshot-render time,
// so it keeps advancing even when later cycles skip the pathway (pending
// write, market-source failure) and the near-stale alert still crosses its
// threshold as the snapshot actually expires.
type PricingSnapshotRuntimeStat struct {
	SrcEID             uint32
	DstEID             uint32
	PriceFeed          string
	UpdatedAtUnix      int64
	StaleAfterSeconds  uint64
	AgeSeconds         float64
	TimeToStaleSeconds float64
}

// RPCProviderRuntimeStat is one RPC provider's latest quorum classification.
type RPCProviderRuntimeStat struct {
	ChainEID      uint32
	ChainName     string
	ProviderID    string
	Status        string
	LogConflict   bool
	StateConflict bool
}

// IndexerRuntimeStat summarizes one in-process indexer loop.
type IndexerRuntimeStat struct {
	ChainEID                uint32
	ChainName               string
	PollIntervalSeconds     float64
	StartedUnix             int64
	PollSuccess             bool
	SuccessPolls            uint64
	ErrorPolls              uint64
	FailureSinceUnix        int64
	LastPollUnix            int64
	LastSuccessUnix         int64
	LastErrorUnix           int64
	LastPollDurationSeconds float64
	ObservedHeadBlock       uint64
	ConfirmedToBlock        uint64
	SourceTransactions      uint64
	DVNTransactions         uint64
	DestinationLogs         uint64
}

// LoopRetryRuntimeStat summarizes supervisor retry attempts for one worker loop.
type LoopRetryRuntimeStat struct {
	Name          string
	Retries       uint64
	LastRetryUnix int64
}

// SignerBalanceRuntimeStat summarizes one signer's native gas balance on one chain.
type SignerBalanceRuntimeStat struct {
	ChainEID                uint32
	SignerID                string
	PollSuccess             bool
	BalanceWei              *big.Int
	MinNativeBalanceWei     *big.Int
	LastSuccessUnix         int64
	LastErrorUnix           int64
	LastPollDurationSeconds float64
}

// Registry records process-local worker metrics.
type Registry struct {
	mu             sync.Mutex
	indexers       map[indexerKey]*IndexerRuntimeStat
	loopRetries    map[string]*LoopRetryRuntimeStat
	signerBalances map[signerBalanceKey]*SignerBalanceRuntimeStat
	rpcProviders   map[uint32][]RPCProviderRuntimeStat
	pricing        map[pricingSnapshotKey]*PricingSnapshotRuntimeStat
	now            func() time.Time
}

type pricingSnapshotKey struct {
	srcEID    uint32
	dstEID    uint32
	priceFeed string
}

type indexerKey struct {
	chainEID  uint32
	chainName string
}

type signerBalanceKey struct {
	chainEID uint32
	signerID string
}

// NewRegistry creates an empty process-local metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		indexers:       make(map[indexerKey]*IndexerRuntimeStat),
		loopRetries:    make(map[string]*LoopRetryRuntimeStat),
		signerBalances: make(map[signerBalanceKey]*SignerBalanceRuntimeStat),
		rpcProviders:   make(map[uint32][]RPCProviderRuntimeStat),
		now:            time.Now,
	}
}

// RegisterIndexer records an indexer before its first polling attempt starts.
func (r *Registry) RegisterIndexer(chainEID uint32, chainName string, pollInterval time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.indexerStatLocked(chainEID, chainName, pollInterval, r.now().Unix())
}

// RecordIndexerPoll records one indexer polling attempt.
func (r *Registry) RecordIndexerPoll(chainEID uint32, chainName string, pollInterval time.Duration, observedHeadBlock uint64, confirmedToBlock uint64, sourceTransactions int, dvnTransactions int, destinationLogs int, duration time.Duration, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now().Unix()
	stat := r.indexerStatLocked(chainEID, chainName, pollInterval, now)
	stat.LastPollDurationSeconds = duration.Seconds()
	stat.LastPollUnix = now
	stat.ObservedHeadBlock = observedHeadBlock
	stat.ConfirmedToBlock = confirmedToBlock
	if err != nil {
		stat.PollSuccess = false
		stat.ErrorPolls++
		if stat.FailureSinceUnix == 0 {
			stat.FailureSinceUnix = now
		}
		stat.LastErrorUnix = now
		return
	}
	stat.PollSuccess = true
	stat.SuccessPolls++
	stat.FailureSinceUnix = 0
	stat.LastSuccessUnix = now
	stat.SourceTransactions += uint64(sourceTransactions)
	stat.DVNTransactions += uint64(dvnTransactions)
	stat.DestinationLogs += uint64(destinationLogs)
}

func (r *Registry) indexerStatLocked(chainEID uint32, chainName string, pollInterval time.Duration, now int64) *IndexerRuntimeStat {
	key := indexerKey{chainEID: chainEID, chainName: chainName}
	stat := r.indexers[key]
	if stat == nil {
		stat = &IndexerRuntimeStat{
			ChainEID:    chainEID,
			ChainName:   chainName,
			StartedUnix: now,
		}
		r.indexers[key] = stat
	}
	stat.PollIntervalSeconds = pollInterval.Seconds()
	return stat
}

// RecordRPCProviders records one chain's latest per-provider quorum
// classification (head status plus the sticky log-conflict dimension). The
// state is keyed by chain EID alone so multiple reporting components (indexer
// polls, the signer balance monitor) overwrite one series per chain instead of
// emitting duplicate gauge lines.
func (r *Registry) RecordRPCProviders(chainEID uint32, chainName string, providers []rpcquorum.Provider) {
	if r == nil {
		return
	}
	stats := make([]RPCProviderRuntimeStat, 0, len(providers))
	for _, provider := range providers {
		stats = append(stats, RPCProviderRuntimeStat{
			ChainEID:      chainEID,
			ChainName:     chainName,
			ProviderID:    provider.ID,
			Status:        string(provider.Status),
			LogConflict:   provider.LogConflict,
			StateConflict: provider.StateConflict,
		})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rpcProviders[chainEID] = stats
}

// RecordPricingSnapshot records the latest on-chain price snapshot facts for
// one priced pathway, sampled when the pricing bot reads its gate baseline
// from chain. The timestamp is the on-chain updatedAt, never a receipt time,
// so a confirmed batch whose entries were superseded cannot fake freshness;
// the age itself is derived at render time so it keeps advancing between
// samples.
func (r *Registry) RecordPricingSnapshot(srcEID, dstEID uint32, priceFeed common.Address, updatedAt time.Time, staleAfter time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pricing == nil {
		r.pricing = make(map[pricingSnapshotKey]*PricingSnapshotRuntimeStat)
	}
	key := pricingSnapshotKey{srcEID: srcEID, dstEID: dstEID, priceFeed: priceFeed.Hex()}
	r.pricing[key] = &PricingSnapshotRuntimeStat{
		SrcEID:            srcEID,
		DstEID:            dstEID,
		PriceFeed:         priceFeed.Hex(),
		UpdatedAtUnix:     updatedAt.Unix(),
		StaleAfterSeconds: uint64(staleAfter / time.Second),
	}
}

// RecordLoopRetry records one supervisor retry after a worker loop returned an error.
func (r *Registry) RecordLoopRetry(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	stat := r.loopRetries[name]
	if stat == nil {
		stat = &LoopRetryRuntimeStat{Name: name}
		r.loopRetries[name] = stat
	}
	stat.Retries++
	stat.LastRetryUnix = r.now().Unix()
}

// RecordSignerBalance records one signer native-balance polling attempt.
func (r *Registry) RecordSignerBalance(chainEID uint32, signerID string, balance, minNativeBalanceWei *big.Int, duration time.Duration, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	key := signerBalanceKey{chainEID: chainEID, signerID: signerID}
	stat := r.signerBalances[key]
	if stat == nil {
		stat = &SignerBalanceRuntimeStat{ChainEID: chainEID, SignerID: signerID}
		r.signerBalances[key] = stat
	}
	stat.MinNativeBalanceWei = bigutil.Clone(minNativeBalanceWei)
	stat.LastPollDurationSeconds = duration.Seconds()
	now := r.now().Unix()
	if err != nil {
		stat.PollSuccess = false
		stat.LastErrorUnix = now
		return
	}
	stat.PollSuccess = true
	stat.BalanceWei = bigutil.Clone(balance)
	stat.LastSuccessUnix = now
}

// RuntimeSnapshot returns a stable copy of process-local metrics.
func (r *Registry) RuntimeSnapshot() RuntimeSnapshot {
	if r == nil {
		return RuntimeSnapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	snapshot := RuntimeSnapshot{
		Indexers:       make([]IndexerRuntimeStat, 0, len(r.indexers)),
		LoopRetries:    make([]LoopRetryRuntimeStat, 0, len(r.loopRetries)),
		SignerBalances: make([]SignerBalanceRuntimeStat, 0, len(r.signerBalances)),
	}
	for _, stat := range r.indexers {
		snapshot.Indexers = append(snapshot.Indexers, *stat)
	}
	for _, stat := range r.loopRetries {
		snapshot.LoopRetries = append(snapshot.LoopRetries, *stat)
	}
	for _, stat := range r.signerBalances {
		copied := *stat
		copied.BalanceWei = bigutil.Clone(stat.BalanceWei)
		copied.MinNativeBalanceWei = bigutil.Clone(stat.MinNativeBalanceWei)
		snapshot.SignerBalances = append(snapshot.SignerBalances, copied)
	}
	for _, stats := range r.rpcProviders {
		snapshot.RPCProviders = append(snapshot.RPCProviders, stats...)
	}
	nowUnix := r.now().Unix()
	for _, stat := range r.pricing {
		copied := *stat
		copied.AgeSeconds = float64(nowUnix - copied.UpdatedAtUnix)
		if copied.AgeSeconds < 0 {
			copied.AgeSeconds = 0
		}
		copied.TimeToStaleSeconds = float64(copied.StaleAfterSeconds) - copied.AgeSeconds
		snapshot.PricingSnapshots = append(snapshot.PricingSnapshots, copied)
	}
	sort.Slice(snapshot.PricingSnapshots, func(a, b int) bool {
		left, right := snapshot.PricingSnapshots[a], snapshot.PricingSnapshots[b]
		if left.SrcEID != right.SrcEID {
			return left.SrcEID < right.SrcEID
		}
		if left.DstEID != right.DstEID {
			return left.DstEID < right.DstEID
		}
		return left.PriceFeed < right.PriceFeed
	})
	sort.Slice(snapshot.Indexers, func(a, b int) bool {
		if snapshot.Indexers[a].ChainEID != snapshot.Indexers[b].ChainEID {
			return snapshot.Indexers[a].ChainEID < snapshot.Indexers[b].ChainEID
		}
		return snapshot.Indexers[a].ChainName < snapshot.Indexers[b].ChainName
	})
	sort.Slice(snapshot.LoopRetries, func(a, b int) bool {
		return snapshot.LoopRetries[a].Name < snapshot.LoopRetries[b].Name
	})
	sort.Slice(snapshot.SignerBalances, func(a, b int) bool {
		if snapshot.SignerBalances[a].ChainEID != snapshot.SignerBalances[b].ChainEID {
			return snapshot.SignerBalances[a].ChainEID < snapshot.SignerBalances[b].ChainEID
		}
		return snapshot.SignerBalances[a].SignerID < snapshot.SignerBalances[b].SignerID
	})
	sort.Slice(snapshot.RPCProviders, func(a, b int) bool {
		if snapshot.RPCProviders[a].ChainEID != snapshot.RPCProviders[b].ChainEID {
			return snapshot.RPCProviders[a].ChainEID < snapshot.RPCProviders[b].ChainEID
		}
		return snapshot.RPCProviders[a].ProviderID < snapshot.RPCProviders[b].ProviderID
	})
	return snapshot
}

// Server exposes worker health and metrics endpoints.
type Server struct {
	server   *http.Server
	logger   *slog.Logger
	provider StatsProvider
}

// New creates a metrics HTTP server.
func New(listenAddress string, provider StatsProvider, logger *slog.Logger, runtime ...RuntimeProvider) *Server {
	return NewWithReadiness(listenAddress, provider, logger, readiness.Services{ExecutorEnabled: true, DVNEnabled: true}, runtime...)
}

// NewWithReadiness creates a metrics HTTP server with role-aware readiness checks.
func NewWithReadiness(listenAddress string, provider StatsProvider, logger *slog.Logger, services readiness.Services, runtime ...RuntimeProvider) *Server {
	handler := HandlerWithReadiness(provider, services, runtime...)
	return &Server{
		server:   &http.Server{Addr: listenAddress, Handler: handler},
		logger:   logger,
		provider: provider,
	}
}

// Handler builds the health and metrics HTTP handler.
func Handler(provider StatsProvider, runtime ...RuntimeProvider) http.Handler {
	return HandlerWithReadiness(provider, readiness.Services{ExecutorEnabled: true, DVNEnabled: true}, runtime...)
}

// HandlerWithReadiness builds the health and metrics HTTP handler with role-aware readiness.
func HandlerWithReadiness(provider StatsProvider, services readiness.Services, runtime ...RuntimeProvider) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		snapshot, err := provider.Stats(r.Context())
		if err != nil {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		report := readiness.EvaluateWithServices(snapshot, services)
		if !report.Ready {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snapshot, err := provider.Stats(r.Context())
		runtimeSnapshot := collectRuntime(runtime)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(renderPrometheus(snapshot, err == nil, runtimeSnapshot)))
	})
	return mux
}

// RenderPrometheus converts a stats snapshot to Prometheus text exposition format.
func RenderPrometheus(snapshot db.StatsSnapshot) string {
	return renderPrometheus(snapshot, true, RuntimeSnapshot{})
}

func renderPrometheus(snapshot db.StatsSnapshot, dbSnapshotAvailable bool, runtime RuntimeSnapshot) string {
	var output strings.Builder
	output.WriteString("# HELP laz_worker_info Static worker process information.\n")
	output.WriteString("# TYPE laz_worker_info gauge\n")
	output.WriteString("laz_worker_info 1\n")
	output.WriteString("# HELP laz_metrics_db_snapshot_available Whether the metrics endpoint could read the DB-backed stats snapshot.\n")
	output.WriteString("# TYPE laz_metrics_db_snapshot_available gauge\n")
	fmt.Fprintf(&output, "laz_metrics_db_snapshot_available %d\n", boolGauge(dbSnapshotAvailable))
	if dbSnapshotAvailable {
		renderDBMetrics(&output, snapshot)
	}
	renderRuntimeMetrics(&output, runtime)
	return output.String()
}

func renderDBMetrics(output *strings.Builder, snapshot db.StatsSnapshot) {
	output.WriteString("# HELP laz_chain_enabled Whether a configured chain is enabled.\n")
	output.WriteString("# TYPE laz_chain_enabled gauge\n")
	for _, stat := range snapshot.Chains {
		fmt.Fprintf(output, "laz_chain_enabled{eid=%q,name=%s} %d\n", strconv.FormatUint(uint64(stat.EID), 10), label(stat.Name), boolGauge(stat.Enabled))
	}
	// A record removed from configuration keeps paused = true as a safety
	// state for a possible re-enable, but readiness and the durable loops
	// ignore disabled records entirely — exporting their pause would page
	// operators forever about a scope nothing acts on.
	output.WriteString("# HELP laz_chain_paused Whether an enabled chain is paused by safety logic.\n")
	output.WriteString("# TYPE laz_chain_paused gauge\n")
	for _, stat := range snapshot.Chains {
		fmt.Fprintf(output, "laz_chain_paused{eid=%q,name=%s} %d\n", strconv.FormatUint(uint64(stat.EID), 10), label(stat.Name), boolGauge(stat.Enabled && stat.Paused))
	}
	output.WriteString("# HELP laz_pathway_paused Whether an enabled pathway is paused by safety logic.\n")
	output.WriteString("# TYPE laz_pathway_paused gauge\n")
	for _, stat := range snapshot.Pathways {
		fmt.Fprintf(output, "laz_pathway_paused{src_eid=%q,dst_eid=%q} %d\n", uint32Label(stat.SrcEID), uint32Label(stat.DstEID), boolGauge(stat.Enabled && stat.Paused))
	}
	output.WriteString("# HELP laz_packets_total Packets by source, destination, and status.\n")
	output.WriteString("# TYPE laz_packets_total gauge\n")
	for _, stat := range snapshot.Packets {
		fmt.Fprintf(output, "laz_packets_total{src_eid=%q,dst_eid=%q,status=%s} %d\n", uint32Label(stat.SrcEID), uint32Label(stat.DstEID), label(stat.Status), stat.Count)
	}
	output.WriteString("# HELP laz_executor_jobs_total Executor jobs by status.\n")
	output.WriteString("# TYPE laz_executor_jobs_total gauge\n")
	for _, stat := range snapshot.ExecutorJobs {
		fmt.Fprintf(output, "laz_executor_jobs_total{status=%s} %d\n", label(stat.Status), stat.Count)
	}
	output.WriteString("# HELP laz_dvn_jobs_total DVN jobs by status.\n")
	output.WriteString("# TYPE laz_dvn_jobs_total gauge\n")
	for _, stat := range snapshot.DVNJobs {
		fmt.Fprintf(output, "laz_dvn_jobs_total{status=%s} %d\n", label(stat.Status), stat.Count)
	}
	output.WriteString("# HELP laz_tx_outbox_total Transaction outbox rows by chain, status, and retry state.\n")
	output.WriteString("# TYPE laz_tx_outbox_total gauge\n")
	for _, stat := range snapshot.TxOutbox {
		fmt.Fprintf(output, "laz_tx_outbox_total{chain_eid=%q,status=%s,retry_state=%s} %d\n", uint32Label(stat.ChainEID), label(stat.Status), label(stat.RetryState), stat.Count)
	}
	output.WriteString("# HELP laz_tx_outbox_held_total Blocked or cancel-pending signer lanes by chain, signer, and hold reason.\n")
	output.WriteString("# TYPE laz_tx_outbox_held_total gauge\n")
	for _, stat := range snapshot.TxOutboxHeld {
		fmt.Fprintf(output, "laz_tx_outbox_held_total{chain_eid=%q,signer=%s,reason=%s} %d\n", uint32Label(stat.ChainEID), label(stat.SignerID), label(stat.HeldReason), stat.Count)
	}
	output.WriteString("# HELP laz_tx_outbox_held_oldest_age_seconds Age of the oldest blocked or cancel-pending row by chain, signer, and hold reason.\n")
	output.WriteString("# TYPE laz_tx_outbox_held_oldest_age_seconds gauge\n")
	for _, stat := range snapshot.TxOutboxHeld {
		fmt.Fprintf(output, "laz_tx_outbox_held_oldest_age_seconds{chain_eid=%q,signer=%s,reason=%s} %d\n", uint32Label(stat.ChainEID), label(stat.SignerID), label(stat.HeldReason), stat.OldestAgeSeconds)
	}
	output.WriteString("# HELP laz_tx_outbox_orphaned_total Send-state rows with no active attempt (a broken invariant) by chain, signer, and status.\n")
	output.WriteString("# TYPE laz_tx_outbox_orphaned_total gauge\n")
	for _, stat := range snapshot.TxOutboxOrphaned {
		fmt.Fprintf(output, "laz_tx_outbox_orphaned_total{chain_eid=%q,signer=%s,status=%s} %d\n", uint32Label(stat.ChainEID), label(stat.SignerID), label(stat.Status), stat.Count)
	}
	output.WriteString("# HELP laz_tx_outbox_orphaned_oldest_age_seconds Age of the oldest send-state row with no active attempt by chain, signer, and status.\n")
	output.WriteString("# TYPE laz_tx_outbox_orphaned_oldest_age_seconds gauge\n")
	for _, stat := range snapshot.TxOutboxOrphaned {
		fmt.Fprintf(output, "laz_tx_outbox_orphaned_oldest_age_seconds{chain_eid=%q,signer=%s,status=%s} %d\n", uint32Label(stat.ChainEID), label(stat.SignerID), label(stat.Status), stat.OldestAgeSeconds)
	}
	output.WriteString("# HELP laz_tx_receipt_gas_cost_dst_wei Mined transaction receipt gas cost in destination-chain native wei by chain and outbox purpose.\n")
	output.WriteString("# TYPE laz_tx_receipt_gas_cost_dst_wei gauge\n")
	for _, stat := range snapshot.TxReceiptGasCosts {
		fmt.Fprintf(output, "laz_tx_receipt_gas_cost_dst_wei{chain_eid=%q,purpose=%s} %s\n", uint32Label(stat.ChainEID), label(stat.Purpose), stat.GasCostDstWei)
	}
	output.WriteString("# HELP laz_worker_fee_revenue_src_wei Worker assignment revenue in source-chain native wei by role and pathway.\n")
	output.WriteString("# TYPE laz_worker_fee_revenue_src_wei gauge\n")
	for _, stat := range snapshot.WorkerFees {
		fmt.Fprintf(output, "laz_worker_fee_revenue_src_wei{role=%s,src_eid=%q,dst_eid=%q} %s\n", label(stat.Role), uint32Label(stat.SrcEID), uint32Label(stat.DstEID), stat.RevenueSrcWei)
	}
	output.WriteString("# HELP laz_worker_fee_actual_gas_cost_src_wei Actual mined worker transaction gas cost converted to source-chain native wei by role and pathway.\n")
	output.WriteString("# TYPE laz_worker_fee_actual_gas_cost_src_wei gauge\n")
	for _, stat := range snapshot.WorkerFees {
		fmt.Fprintf(output, "laz_worker_fee_actual_gas_cost_src_wei{role=%s,src_eid=%q,dst_eid=%q} %s\n", label(stat.Role), uint32Label(stat.SrcEID), uint32Label(stat.DstEID), stat.ActualGasCostSrcWei)
	}
	output.WriteString("# HELP laz_worker_fee_gross_margin_src_wei Worker assignment revenue minus actual mined gas cost in source-chain native wei by role and pathway.\n")
	output.WriteString("# TYPE laz_worker_fee_gross_margin_src_wei gauge\n")
	for _, stat := range snapshot.WorkerFees {
		fmt.Fprintf(output, "laz_worker_fee_gross_margin_src_wei{role=%s,src_eid=%q,dst_eid=%q} %s\n", label(stat.Role), uint32Label(stat.SrcEID), uint32Label(stat.DstEID), stat.GrossMarginSrcWei)
	}
	output.WriteString("# HELP laz_worker_fee_negative_margin_jobs Worker jobs whose priced mined gas cost exceeds assignment revenue.\n")
	output.WriteString("# TYPE laz_worker_fee_negative_margin_jobs gauge\n")
	for _, stat := range snapshot.WorkerFees {
		fmt.Fprintf(output, "laz_worker_fee_negative_margin_jobs{role=%s,src_eid=%q,dst_eid=%q} %d\n", label(stat.Role), uint32Label(stat.SrcEID), uint32Label(stat.DstEID), stat.NegativeMarginJobs)
	}
	output.WriteString("# HELP laz_worker_fee_unpriced_receipts Mined worker transaction receipts whose destination gas cost is not yet priced in source-chain native wei.\n")
	output.WriteString("# TYPE laz_worker_fee_unpriced_receipts gauge\n")
	for _, stat := range snapshot.WorkerFees {
		fmt.Fprintf(output, "laz_worker_fee_unpriced_receipts{role=%s,src_eid=%q,dst_eid=%q} %d\n", label(stat.Role), uint32Label(stat.SrcEID), uint32Label(stat.DstEID), stat.UnpricedReceipts)
	}
	output.WriteString("# HELP laz_pricing_pending Pending pricing snapshot transactions that can still land on chain, by source chain.\n")
	output.WriteString("# TYPE laz_pricing_pending gauge\n")
	for _, stat := range snapshot.PricingPending {
		fmt.Fprintf(output, "laz_pricing_pending{chain_eid=%q} %d\n", uint32Label(stat.ChainEID), stat.Count)
	}
	output.WriteString("# HELP laz_pricing_pending_oldest_age_seconds Age of the oldest pending pricing snapshot transaction by source chain.\n")
	output.WriteString("# TYPE laz_pricing_pending_oldest_age_seconds gauge\n")
	for _, stat := range snapshot.PricingPending {
		fmt.Fprintf(output, "laz_pricing_pending_oldest_age_seconds{chain_eid=%q} %d\n", uint32Label(stat.ChainEID), stat.OldestAgeSeconds)
	}
	output.WriteString("# HELP laz_indexer_cursor_last_block Last indexed block by chain and stream.\n")
	output.WriteString("# TYPE laz_indexer_cursor_last_block gauge\n")
	for _, stat := range snapshot.IndexerCursors {
		fmt.Fprintf(output, "laz_indexer_cursor_last_block{chain_eid=%q,stream=%s} %d\n", uint32Label(stat.ChainEID), label(stat.Stream), stat.LastBlock)
	}
}

func renderRuntimeMetrics(output *strings.Builder, snapshot RuntimeSnapshot) {
	output.WriteString("# HELP laz_pricing_snapshot_age_seconds Age of the on-chain price snapshot per priced pathway, from on-chain updatedAt.\n")
	output.WriteString("# TYPE laz_pricing_snapshot_age_seconds gauge\n")
	for _, stat := range snapshot.PricingSnapshots {
		fmt.Fprintf(output, "laz_pricing_snapshot_age_seconds{src_eid=%q,dst_eid=%q,price_feed=%s} %.0f\n", uint32Label(stat.SrcEID), uint32Label(stat.DstEID), label(stat.PriceFeed), stat.AgeSeconds)
	}
	output.WriteString("# HELP laz_pricing_snapshot_time_to_stale_seconds Seconds until the on-chain price snapshot crosses its staleAfter cutoff (negative when already stale).\n")
	output.WriteString("# TYPE laz_pricing_snapshot_time_to_stale_seconds gauge\n")
	for _, stat := range snapshot.PricingSnapshots {
		fmt.Fprintf(output, "laz_pricing_snapshot_time_to_stale_seconds{src_eid=%q,dst_eid=%q,price_feed=%s} %.0f\n", uint32Label(stat.SrcEID), uint32Label(stat.DstEID), label(stat.PriceFeed), stat.TimeToStaleSeconds)
	}
	output.WriteString("# HELP laz_rpc_provider_status RPC provider quorum head classification (1 for the current status).\n")
	output.WriteString("# TYPE laz_rpc_provider_status gauge\n")
	for _, stat := range snapshot.RPCProviders {
		fmt.Fprintf(output, "laz_rpc_provider_status{chain_eid=%q,provider=%s,status=%s} 1\n", uint32Label(stat.ChainEID), label(stat.ProviderID), label(stat.Status))
	}
	output.WriteString("# HELP laz_rpc_provider_state_conflict Whether the provider's last comparable state read disagreed with the state-read quorum (sticky until it agrees again).\n")
	output.WriteString("# TYPE laz_rpc_provider_state_conflict gauge\n")
	for _, stat := range snapshot.RPCProviders {
		fmt.Fprintf(output, "laz_rpc_provider_state_conflict{chain_eid=%q,provider=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ProviderID), boolGauge(stat.StateConflict))
	}
	output.WriteString("# HELP laz_rpc_provider_log_conflict Whether the provider's last log window disagreed with the log quorum (sticky until it agrees again).\n")
	output.WriteString("# TYPE laz_rpc_provider_log_conflict gauge\n")
	for _, stat := range snapshot.RPCProviders {
		fmt.Fprintf(output, "laz_rpc_provider_log_conflict{chain_eid=%q,provider=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ProviderID), boolGauge(stat.LogConflict))
	}
	output.WriteString("# HELP laz_worker_loop_retries_total Worker loop restart attempts after returned errors.\n")
	output.WriteString("# TYPE laz_worker_loop_retries_total counter\n")
	for _, stat := range snapshot.LoopRetries {
		fmt.Fprintf(output, "laz_worker_loop_retries_total{name=%s} %d\n", label(stat.Name), stat.Retries)
	}
	output.WriteString("# HELP laz_worker_loop_last_retry_timestamp_seconds Unix timestamp for the most recent worker loop retry.\n")
	output.WriteString("# TYPE laz_worker_loop_last_retry_timestamp_seconds gauge\n")
	for _, stat := range snapshot.LoopRetries {
		fmt.Fprintf(output, "laz_worker_loop_last_retry_timestamp_seconds{name=%s} %d\n", label(stat.Name), stat.LastRetryUnix)
	}
	output.WriteString("# HELP laz_indexer_poll_success Whether the most recent indexer poll succeeded.\n")
	output.WriteString("# TYPE laz_indexer_poll_success gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_poll_success{chain_eid=%q,name=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), boolGauge(stat.PollSuccess))
	}
	output.WriteString("# HELP laz_indexer_poll_interval_seconds Configured interval between indexer polling attempts.\n")
	output.WriteString("# TYPE laz_indexer_poll_interval_seconds gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_poll_interval_seconds{chain_eid=%q,name=%s} %s\n", uint32Label(stat.ChainEID), label(stat.ChainName), floatGauge(stat.PollIntervalSeconds))
	}
	output.WriteString("# HELP laz_indexer_start_timestamp_seconds Unix timestamp when the indexer loop was first registered.\n")
	output.WriteString("# TYPE laz_indexer_start_timestamp_seconds gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_start_timestamp_seconds{chain_eid=%q,name=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.StartedUnix)
	}
	output.WriteString("# HELP laz_indexer_polls_total Indexer polling attempts by result.\n")
	output.WriteString("# TYPE laz_indexer_polls_total counter\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_polls_total{chain_eid=%q,name=%s,result=\"success\"} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.SuccessPolls)
		fmt.Fprintf(output, "laz_indexer_polls_total{chain_eid=%q,name=%s,result=\"error\"} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.ErrorPolls)
	}
	output.WriteString("# HELP laz_indexer_last_poll_timestamp_seconds Unix timestamp for the most recent completed indexer poll.\n")
	output.WriteString("# TYPE laz_indexer_last_poll_timestamp_seconds gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_last_poll_timestamp_seconds{chain_eid=%q,name=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.LastPollUnix)
	}
	output.WriteString("# HELP laz_indexer_last_success_timestamp_seconds Unix timestamp for the most recent successful indexer poll.\n")
	output.WriteString("# TYPE laz_indexer_last_success_timestamp_seconds gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_last_success_timestamp_seconds{chain_eid=%q,name=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.LastSuccessUnix)
	}
	output.WriteString("# HELP laz_indexer_failure_since_timestamp_seconds Unix timestamp when the current sequence of failed indexer polls began, or zero when healthy.\n")
	output.WriteString("# TYPE laz_indexer_failure_since_timestamp_seconds gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_failure_since_timestamp_seconds{chain_eid=%q,name=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.FailureSinceUnix)
	}
	output.WriteString("# HELP laz_indexer_last_error_timestamp_seconds Unix timestamp for the most recent failed indexer poll.\n")
	output.WriteString("# TYPE laz_indexer_last_error_timestamp_seconds gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_last_error_timestamp_seconds{chain_eid=%q,name=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.LastErrorUnix)
	}
	output.WriteString("# HELP laz_indexer_last_poll_duration_seconds Duration of the most recent indexer poll.\n")
	output.WriteString("# TYPE laz_indexer_last_poll_duration_seconds gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_last_poll_duration_seconds{chain_eid=%q,name=%s} %s\n", uint32Label(stat.ChainEID), label(stat.ChainName), floatGauge(stat.LastPollDurationSeconds))
	}
	output.WriteString("# HELP laz_indexer_observed_head_block Most recent chain head observed by an indexer poll.\n")
	output.WriteString("# TYPE laz_indexer_observed_head_block gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_observed_head_block{chain_eid=%q,name=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.ObservedHeadBlock)
	}
	output.WriteString("# HELP laz_indexer_confirmed_to_block Most recent confirmed block upper bound used by an indexer poll.\n")
	output.WriteString("# TYPE laz_indexer_confirmed_to_block gauge\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_confirmed_to_block{chain_eid=%q,name=%s} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.ConfirmedToBlock)
	}
	output.WriteString("# HELP laz_indexer_processed_total Items processed by indexer polls.\n")
	output.WriteString("# TYPE laz_indexer_processed_total counter\n")
	for _, stat := range snapshot.Indexers {
		fmt.Fprintf(output, "laz_indexer_processed_total{chain_eid=%q,name=%s,kind=\"source_transactions\"} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.SourceTransactions)
		fmt.Fprintf(output, "laz_indexer_processed_total{chain_eid=%q,name=%s,kind=\"dvn_transactions\"} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.DVNTransactions)
		fmt.Fprintf(output, "laz_indexer_processed_total{chain_eid=%q,name=%s,kind=\"destination_logs\"} %d\n", uint32Label(stat.ChainEID), label(stat.ChainName), stat.DestinationLogs)
	}
	output.WriteString("# HELP laz_signer_native_balance_wei Last observed signer native-token balance in wei.\n")
	output.WriteString("# TYPE laz_signer_native_balance_wei gauge\n")
	for _, stat := range snapshot.SignerBalances {
		if stat.BalanceWei != nil {
			fmt.Fprintf(output, "laz_signer_native_balance_wei{chain_eid=%q,signer=%s} %s\n", uint32Label(stat.ChainEID), label(stat.SignerID), stat.BalanceWei.String())
		}
	}
	output.WriteString("# HELP laz_signer_min_native_balance_wei Configured minimum signer native-token balance in wei.\n")
	output.WriteString("# TYPE laz_signer_min_native_balance_wei gauge\n")
	for _, stat := range snapshot.SignerBalances {
		if stat.MinNativeBalanceWei != nil {
			fmt.Fprintf(output, "laz_signer_min_native_balance_wei{chain_eid=%q,signer=%s} %s\n", uint32Label(stat.ChainEID), label(stat.SignerID), stat.MinNativeBalanceWei.String())
		}
	}
	output.WriteString("# HELP laz_signer_balance_poll_success Whether the most recent signer native-balance poll succeeded.\n")
	output.WriteString("# TYPE laz_signer_balance_poll_success gauge\n")
	for _, stat := range snapshot.SignerBalances {
		fmt.Fprintf(output, "laz_signer_balance_poll_success{chain_eid=%q,signer=%s} %d\n", uint32Label(stat.ChainEID), label(stat.SignerID), boolGauge(stat.PollSuccess))
	}
	output.WriteString("# HELP laz_signer_balance_last_success_timestamp_seconds Unix timestamp for the most recent successful signer balance poll.\n")
	output.WriteString("# TYPE laz_signer_balance_last_success_timestamp_seconds gauge\n")
	for _, stat := range snapshot.SignerBalances {
		fmt.Fprintf(output, "laz_signer_balance_last_success_timestamp_seconds{chain_eid=%q,signer=%s} %d\n", uint32Label(stat.ChainEID), label(stat.SignerID), stat.LastSuccessUnix)
	}
	output.WriteString("# HELP laz_signer_balance_last_error_timestamp_seconds Unix timestamp for the most recent failed signer balance poll.\n")
	output.WriteString("# TYPE laz_signer_balance_last_error_timestamp_seconds gauge\n")
	for _, stat := range snapshot.SignerBalances {
		fmt.Fprintf(output, "laz_signer_balance_last_error_timestamp_seconds{chain_eid=%q,signer=%s} %d\n", uint32Label(stat.ChainEID), label(stat.SignerID), stat.LastErrorUnix)
	}
	output.WriteString("# HELP laz_signer_balance_last_poll_duration_seconds Duration of the most recent signer balance poll.\n")
	output.WriteString("# TYPE laz_signer_balance_last_poll_duration_seconds gauge\n")
	for _, stat := range snapshot.SignerBalances {
		fmt.Fprintf(output, "laz_signer_balance_last_poll_duration_seconds{chain_eid=%q,signer=%s} %s\n", uint32Label(stat.ChainEID), label(stat.SignerID), floatGauge(stat.LastPollDurationSeconds))
	}
}

func collectRuntime(providers []RuntimeProvider) RuntimeSnapshot {
	if len(providers) == 0 || providers[0] == nil {
		return RuntimeSnapshot{}
	}
	return providers[0].RuntimeSnapshot()
}

// Run serves metrics until the context is canceled or the listener fails.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("metrics server started", "addr", s.server.Addr)
		errCh <- s.server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		_ = s.server.Shutdown(context.Background())
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return workerloop.Fatal(err)
	}
}

func uint32Label(value uint32) string {
	return strconv.FormatUint(uint64(value), 10)
}

func boolGauge(value bool) int {
	if value {
		return 1
	}
	return 0
}

func floatGauge(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func label(value string) string {
	return strconv.Quote(value)
}
