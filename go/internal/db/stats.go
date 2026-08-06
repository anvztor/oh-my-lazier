package db

import (
	"context"
	"fmt"
)

// StatsSnapshot is a point-in-time summary used by the HTTP metrics endpoint.
type StatsSnapshot struct {
	Chains            []ChainStat
	Pathways          []PathwayStat
	Packets           []PacketStat
	ExecutorJobs      []StatusStat
	DVNJobs           []StatusStat
	TxOutbox          []TxOutboxStat
	TxOutboxHeld      []TxOutboxHeldStat
	TxOutboxOrphaned  []TxOutboxOrphanedStat
	TxReceiptGasCosts []TxReceiptGasCostStat
	WorkerFees        []WorkerFeeStat
	IndexerCursors    []IndexerCursorStat
	PricingPending    []PricingPendingStat
}

// PricingPendingStat counts one chain's pricing snapshot transactions that can
// still land on chain. OldestAge counts from the oldest pending row's
// creation: a pending write normally resolves within a confirmation window, so
// an old one means the feed is gated while its snapshot keeps aging toward the
// on-chain staleness cutoff.
type PricingPendingStat struct {
	ChainEID         uint32
	Count            int64
	OldestAgeSeconds uint64
}

// TxOutboxOrphanedStat counts rows in a send state that no longer have an
// active attempt. The signing path writes active_attempt_id and the status
// together, so 'signed' or 'broadcast' with a NULL active attempt is a broken
// invariant, not a transient state: receipt polling joins the active attempt
// and never sees such a row again, a 'signed' one keeps its nonce and blocks
// the signer lane, and a 'broadcast' one leaves its workflow job enqueued
// forever. The known producer is the 002 schema upgrade applied while rows
// were in flight; recovery is `txretry -action cancel-nonce` per row.
//
// A 'held' row is deliberately NOT counted: an exhausted pre-sign budget parks
// a row that never signed an attempt, which is legitimate. Rows on disabled
// (retired) chains are excluded too, matching the readiness escalation.
type TxOutboxOrphanedStat struct {
	ChainEID         uint32
	SignerID         string
	Status           string
	Count            int64
	OldestAgeSeconds uint64
}

// ChainStat summarizes one configured chain.
type ChainStat struct {
	EID     uint32
	Name    string
	Enabled bool
	Paused  bool
}

// PathwayStat summarizes one configured pathway.
type PathwayStat struct {
	SrcEID  uint32
	DstEID  uint32
	Enabled bool
	Paused  bool
}

// PacketStat counts packets by source, destination, and packet state.
type PacketStat struct {
	SrcEID uint32
	DstEID uint32
	Status string
	Count  uint64
}

// StatusStat counts rows by state for tables without chain-specific labels.
type StatusStat struct {
	Status string
	Count  uint64
}

// TxOutboxStat counts transaction requests by chain and state.
type TxOutboxStat struct {
	ChainEID   uint32
	Status     string
	RetryState string
	Count      uint64
}

// TxOutboxHeldStat details blocked signer lanes: held rows by chain, signer,
// and hold reason, plus rows with pending operator cancel intent. OldestAge is
// the age of the oldest matching row so alerting can page on stuck lanes.
type TxOutboxHeldStat struct {
	ChainEID         uint32
	SignerID         string
	HeldReason       string
	Count            uint64
	OldestAgeSeconds uint64
}

// TxReceiptGasCostStat sums mined receipt gas costs by destination chain and outbox purpose.
type TxReceiptGasCostStat struct {
	ChainEID      uint32
	Purpose       string
	GasCostDstWei string
}

// WorkerFeeStat summarizes worker revenue and actual gas cost by role and pathway.
type WorkerFeeStat struct {
	Role                string
	SrcEID              uint32
	DstEID              uint32
	RevenueSrcWei       string
	ActualGasCostSrcWei string
	GrossMarginSrcWei   string
	NegativeMarginJobs  uint64
	UnpricedReceipts    uint64
}

// IndexerCursorStat exposes durable indexer cursor progress.
type IndexerCursorStat struct {
	ChainEID  uint32
	Stream    string
	LastBlock uint64
}

// statsEnabledPacketScopeSQL keeps packet-linked statistics to enabled scopes:
// the packet's exact pathway and both endpoint chains must still be configured.
// A scope removed from configuration is disabled but keeps its durable rows,
// and readiness plus the paging alerts must not report history nothing acts on
// (paused scopes stay visible — a pause is temporary safety, not removal).
// Held signer-lane statistics deliberately do not use this filter: a held row
// physically blocks the shared signer lane for every pathway on its chain.
// The placeholder is the packets table alias.
const statsEnabledPacketScopeSQL = `
	EXISTS (
		SELECT 1 FROM pathways pw
		WHERE pw.src_eid = %[1]s.src_eid AND pw.dst_eid = %[1]s.dst_eid
			AND pw.src_oapp = %[1]s.sender AND pw.dst_oapp = %[1]s.receiver
			AND pw.enabled
	)
	AND NOT EXISTS (SELECT 1 FROM chains c WHERE c.eid IN (%[1]s.src_eid, %[1]s.dst_eid) AND NOT c.enabled)`

// Stats returns a read-only worker summary for health and metrics reporting.
func (s *Store) Stats(ctx context.Context) (StatsSnapshot, error) {
	chains, err := s.chainStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	pathways, err := s.pathwayStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	packets, err := s.packetStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	executorJobs, err := s.statusStats(ctx, "executor_jobs")
	if err != nil {
		return StatsSnapshot{}, err
	}
	dvnJobs, err := s.statusStats(ctx, "dvn_jobs")
	if err != nil {
		return StatsSnapshot{}, err
	}
	txOutbox, err := s.txOutboxStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	txOutboxHeld, err := s.txOutboxHeldStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	txOutboxOrphaned, err := s.txOutboxOrphanedStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	txReceiptGasCosts, err := s.txReceiptGasCostStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	workerFees, err := s.workerFeeStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	indexerCursors, err := s.indexerCursorStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	pricingPending, err := s.pricingPendingStats(ctx)
	if err != nil {
		return StatsSnapshot{}, err
	}
	return StatsSnapshot{
		Chains:            chains,
		Pathways:          pathways,
		Packets:           packets,
		ExecutorJobs:      executorJobs,
		DVNJobs:           dvnJobs,
		TxOutbox:          txOutbox,
		TxOutboxHeld:      txOutboxHeld,
		TxOutboxOrphaned:  txOutboxOrphaned,
		TxReceiptGasCosts: txReceiptGasCosts,
		WorkerFees:        workerFees,
		PricingPending:    pricingPending,
		IndexerCursors:    indexerCursors,
	}, nil
}

func (s *Store) chainStats(ctx context.Context) ([]ChainStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT eid, name, enabled, paused
		FROM chains
		ORDER BY eid
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []ChainStat
	for rows.Next() {
		var stat ChainStat
		if err := rows.Scan(&stat.EID, &stat.Name, &stat.Enabled, &stat.Paused); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

func (s *Store) pathwayStats(ctx context.Context) ([]PathwayStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT src_eid, dst_eid, enabled, paused
		FROM pathways
		ORDER BY src_eid, dst_eid, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []PathwayStat
	for rows.Next() {
		var stat PathwayStat
		if err := rows.Scan(&stat.SrcEID, &stat.DstEID, &stat.Enabled, &stat.Paused); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

func (s *Store) packetStats(ctx context.Context) ([]PacketStat, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT p.src_eid, p.dst_eid, p.status, count(*)::bigint
		FROM packets p
		WHERE %s
		GROUP BY p.src_eid, p.dst_eid, p.status
		ORDER BY p.src_eid, p.dst_eid, p.status
	`, fmt.Sprintf(statsEnabledPacketScopeSQL, "p")))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []PacketStat
	for rows.Next() {
		var stat PacketStat
		if err := rows.Scan(&stat.SrcEID, &stat.DstEID, &stat.Status, &stat.Count); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

func (s *Store) statusStats(ctx context.Context, table string) ([]StatusStat, error) {
	switch table {
	case "executor_jobs", "dvn_jobs":
	default:
		return nil, fmt.Errorf("unsupported stats table %q", table)
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT j.status, count(*)::bigint
		FROM %s j
		JOIN packets p ON p.guid = j.guid
		WHERE %s
		GROUP BY j.status
		ORDER BY j.status
	`, table, fmt.Sprintf(statsEnabledPacketScopeSQL, "p")))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []StatusStat
	for rows.Next() {
		var stat StatusStat
		if err := rows.Scan(&stat.Status, &stat.Count); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

// HeldRepriceExhausted is a synthetic stats-only reason: a reprice-required
// hold whose automatic replacement budget is exhausted. The replacement
// selector no longer picks the row, so only operator action (txretry replace
// or cancel-nonce) can move the lane — readiness treats it like a manual hold.
const HeldRepriceExhausted = "reprice_exhausted"

// HeldCancelRequested is a synthetic stats-only reason: a row with pending
// operator cancel intent, regardless of its held reason. Its age counts from
// the immutable cancel_requested_at, so deferrals (fee-cap blocks, receipts
// awaiting confirmation depth) cannot hide a cancel that never converges.
const HeldCancelRequested = "cancel_requested"

// txOutboxHeldStats surfaces every blocked or cancel-pending signer lane. A
// pending cancel is reported under the synthetic reason 'cancel_requested' in
// addition to its held reason, because a cancel can also sit on non-held rows.
// A reprice hold past the automatic replacement cap is reported as the
// synthetic 'reprice_exhausted' reason, mirroring the replacement selector's
// counting domain: an active cancel attempt is bumped and capped by
// cancel-kind attempts, every other row by replacement-kind attempts. A
// cancel-intent row whose active attempt is not yet a cancel is exempt — the
// cancel pipeline still owns its next step.
func (s *Store) txOutboxHeldStats(ctx context.Context) ([]TxOutboxHeldStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT o.chain_eid, o.signer_id,
			CASE
				WHEN o.held_reason = $1
					AND (o.cancel_requested_at IS NULL OR COALESCE(act.kind, '') = $5)
					AND (
						SELECT count(*) FROM tx_attempts r
						WHERE r.outbox_id = o.id
							AND r.kind = CASE WHEN COALESCE(act.kind, '') = $5 THEN $5 ELSE $2 END
					) >= $3
				THEN $4
				ELSE o.held_reason
			END,
			count(*)::bigint,
			-- For reprice holds the age counts from the ACTIVE ATTEMPT's
			-- creation — the lane's last real signing progress. The row's
			-- updated_at is refreshed by every underpriced result and preflight
			-- deferral, so a fee-cap-stuck lane that can never land a bump
			-- would otherwise report a perpetual one-minute age and the
			-- readiness stall detection could never trigger.
			COALESCE(floor(extract(epoch FROM now() - min(
				CASE WHEN o.held_reason = $1 THEN COALESCE(act.created_at, o.updated_at) ELSE o.updated_at END
			)))::bigint, 0)
		FROM tx_outbox o
		LEFT JOIN tx_attempts act ON act.id = o.active_attempt_id
		WHERE o.status = 'held'
		GROUP BY 1, 2, 3
		UNION ALL
		SELECT chain_eid, signer_id, 'cancel_requested',
			count(*)::bigint,
			COALESCE(floor(extract(epoch FROM now() - min(cancel_requested_at)))::bigint, 0)
		FROM tx_outbox
		WHERE cancel_requested_at IS NOT NULL
			AND status NOT IN ('confirmed', 'failed')
		GROUP BY chain_eid, signer_id
		ORDER BY 1, 2, 3
	`, HeldRepriceRequired, TxAttemptReplacement, TxMaxReplacements, HeldRepriceExhausted, TxAttemptCancel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stats []TxOutboxHeldStat
	for rows.Next() {
		var stat TxOutboxHeldStat
		var oldest int64
		if err := rows.Scan(&stat.ChainEID, &stat.SignerID, &stat.HeldReason, &stat.Count, &oldest); err != nil {
			return nil, err
		}
		if oldest > 0 {
			stat.OldestAgeSeconds = uint64(oldest)
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

// txOutboxOrphanedStats surfaces send-state rows whose active attempt is
// missing. The predicate is an invariant check, not a threshold: the signing
// path sets active_attempt_id and the status in one statement, so any row here
// means durable send state was lost and the lane needs operator recovery.
// The age counts from updated_at — the moment the row entered its send state,
// since nothing touches it afterwards.
func (s *Store) txOutboxOrphanedStats(ctx context.Context) ([]TxOutboxOrphanedStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_eid, signer_id, status, count(*)::bigint,
			COALESCE(floor(extract(epoch FROM now() - min(updated_at)))::bigint, 0)
		FROM tx_outbox
		WHERE status IN ($1, $2) AND active_attempt_id IS NULL
			-- Disabled (retired) chains are excluded so the metric matches the
			-- readiness escalation, which only considers enabled chains: their
			-- retained rows cannot be acted on and would page forever.
			AND EXISTS (SELECT 1 FROM chains c WHERE c.eid = tx_outbox.chain_eid AND c.enabled)
		GROUP BY chain_eid, signer_id, status
		ORDER BY chain_eid, signer_id, status
	`, TxStatusSigned, TxStatusBroadcast)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stats []TxOutboxOrphanedStat
	for rows.Next() {
		var stat TxOutboxOrphanedStat
		var oldest int64
		if err := rows.Scan(&stat.ChainEID, &stat.SignerID, &stat.Status, &stat.Count, &oldest); err != nil {
			return nil, err
		}
		if oldest > 0 {
			stat.OldestAgeSeconds = uint64(oldest)
		}
		stats = append(stats, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return stats, nil
}

func (s *Store) txOutboxStats(ctx context.Context) ([]TxOutboxStat, error) {
	// Packet-linked rows follow the packet's scope; rows without a packet
	// (pricing) follow their send chain.
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			tx.chain_eid,
			tx.status,
			CASE
				WHEN tx.status <> $1 THEN ''
				WHEN EXISTS (
					SELECT 1
					FROM tx_outbox child
					WHERE child.retry_of_id = tx.id
				) THEN $2
				WHEN tx.failure_kind IS NULL THEN $2
				-- A mined cancel or a resolved externally-consumed nonce is a
				-- COMPLETED operator action, not an exhausted retry: RetryFailedTx
				-- rejects both, so classifying them exhausted would hold /readyz
				-- red and page LazTxOutboxFailed forever with nothing to act on.
				WHEN tx.failure_kind IN ($6, $7) THEN $2
				-- A failed pricing row is terminal by design (its calldata
				-- carries a time-bound observation and is never re-signed);
				-- the bot supersedes it with a fresh observation, and the
				-- pricing snapshot-age alerts own the outcome. The exception
				-- is a row still HOLDING an unconsumed nonce (legacy upgraded
				-- data): RetryFailedTx keeps it retryable because the nonce
				-- gap can wedge the signer lane, so it falls through to the
				-- generic classification and pages instead of hiding. A legacy
				-- receipt failure consumed its nonce when it mined, so it is
				-- superseded like every other terminal pricing failure.
				WHEN tx.purpose = $8 AND (tx.nonce IS NULL OR tx.receipt_outcome IS NOT NULL OR tx.failure_kind = $9) THEN $2
				WHEN tx.failure_kind IS NOT NULL AND tx.attempts < $3 AND tx.next_retry_at IS NOT NULL THEN $4
				ELSE $5
			END AS retry_state,
			count(*)::bigint
		FROM tx_outbox tx
		LEFT JOIN packets p ON p.guid = tx.guid
		WHERE CASE WHEN p.guid IS NULL
			THEN EXISTS (SELECT 1 FROM chains c WHERE c.eid = tx.chain_eid AND c.enabled)
			ELSE %s
		END
		GROUP BY tx.chain_eid, tx.status, retry_state
		ORDER BY tx.chain_eid, tx.status, retry_state
	`, fmt.Sprintf(statsEnabledPacketScopeSQL, "p")), TxStatusFailed, TxOutboxRetryStateSuperseded, TxAutoRetryMaxAttempts, TxOutboxRetryStateRetrying, TxOutboxRetryStateExhausted, TxFailureCanceled, TxFailureNonceConsumedExternally, TxPurposePricingSetPriceSnapshot, TxFailureReceiptFailed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []TxOutboxStat
	for rows.Next() {
		var stat TxOutboxStat
		if err := rows.Scan(&stat.ChainEID, &stat.Status, &stat.RetryState, &stat.Count); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

func (s *Store) indexerCursorStats(ctx context.Context) ([]IndexerCursorStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_eid, stream, last_block
		FROM indexer_cursors
		ORDER BY chain_eid, stream
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []IndexerCursorStat
	for rows.Next() {
		var stat IndexerCursorStat
		if err := rows.Scan(&stat.ChainEID, &stat.Stream, &stat.LastBlock); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

func (s *Store) pricingPendingStats(ctx context.Context) ([]PricingPendingStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_eid, count(*)::bigint,
			COALESCE(floor(extract(epoch FROM now() - min(created_at)))::bigint, 0)
		FROM tx_outbox
		WHERE purpose = $1
			-- Retryable legacy failures (an unconsumed nonce gap) gate their
			-- feed exactly like pending rows and need the same stall pressure:
			-- only the operator's in-place requeue can recover them.
			AND (status = ANY($2) OR `+txRetryableLegacyPricingSQL+`)
			-- Disabled (retired) chains are excluded like every other outbox
			-- statistic; their in-flight rows cannot be acted on and would page
			-- the pending-stall alert forever.
			AND EXISTS (SELECT 1 FROM chains c WHERE c.eid = tx_outbox.chain_eid AND c.enabled)
		GROUP BY chain_eid
		ORDER BY chain_eid
	`, TxPurposePricingSetPriceSnapshot, txPricingPendingStatuses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stats []PricingPendingStat
	for rows.Next() {
		var stat PricingPendingStat
		var oldest int64
		if err := rows.Scan(&stat.ChainEID, &stat.Count, &oldest); err != nil {
			return nil, err
		}
		if oldest > 0 {
			stat.OldestAgeSeconds = uint64(oldest)
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}
