package readiness

import (
	"fmt"

	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
)

const (
	executorSourceStream = "executor_source"
	executorDestStream   = "executor_destination"
	dvnSourceStream      = "dvn_source"
	dvnDestStream        = "dvn_destination"

	// repriceHeldStallSeconds is how long a held(reprice_required) lane may age
	// before readiness treats it as stalled. Healthy automatic repricing cycles
	// on a one-minute cooldown and escalates to reprice_exhausted within five
	// attempts, so fifteen minutes of the same hold means the bump cannot land
	// (typically the configured fee cap) and the operator must intervene.
	repriceHeldStallSeconds = 15 * 60
)

// Services selects which worker role state should be evaluated for this process.
type Services struct {
	ExecutorEnabled bool
	DVNEnabled      bool
}

// Issue is one failed pre-migration readiness check.
type Issue struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Report is the pre-migration readiness verdict derived from DB-backed state.
type Report struct {
	Ready  bool             `json:"ready"`
	Issues []Issue          `json:"issues"`
	Stats  db.StatsSnapshot `json:"stats"`
}

// Evaluate checks worker durable state against the mainnet readiness runbook gates.
func Evaluate(snapshot db.StatsSnapshot) Report {
	return EvaluateWithServices(snapshot, Services{ExecutorEnabled: true, DVNEnabled: true})
}

// EvaluateWithServices checks worker durable state for the roles enabled in this process.
func EvaluateWithServices(snapshot db.StatsSnapshot, services Services) Report {
	var issues []Issue
	activeChains := make(map[uint32]struct{})
	requiredCursors := make(map[uint32]map[string]struct{})
	for _, chain := range snapshot.Chains {
		if !chain.Enabled {
			continue
		}
		activeChains[chain.EID] = struct{}{}
		if chain.Paused {
			issues = append(issues, Issue{
				Code:    "chain_paused",
				Message: fmt.Sprintf("chain %d (%s) is paused", chain.EID, chain.Name),
			})
		}
	}
	for _, pathway := range snapshot.Pathways {
		if !pathway.Enabled {
			continue
		}
		if _, ok := activeChains[pathway.SrcEID]; !ok {
			continue
		}
		if _, ok := activeChains[pathway.DstEID]; !ok {
			continue
		}
		if services.ExecutorEnabled {
			requireCursor(requiredCursors, pathway.SrcEID, executorSourceStream)
			requireCursor(requiredCursors, pathway.DstEID, executorDestStream)
		}
		if services.DVNEnabled {
			requireCursor(requiredCursors, pathway.SrcEID, dvnSourceStream)
			requireCursor(requiredCursors, pathway.DstEID, dvnDestStream)
		}
		if pathway.Paused {
			issues = append(issues, Issue{
				Code:    "pathway_paused",
				Message: fmt.Sprintf("pathway %d -> %d is paused", pathway.SrcEID, pathway.DstEID),
			})
		}
	}
	for _, outbox := range snapshot.TxOutbox {
		if outbox.Status != db.TxStatusFailed || outbox.Count == 0 {
			continue
		}
		if outbox.RetryState != db.TxOutboxRetryStateExhausted {
			continue
		}
		if _, ok := activeChains[outbox.ChainEID]; !ok {
			continue
		}
		issues = append(issues, Issue{
			Code:    "failed_outbox",
			Message: fmt.Sprintf("chain %d has %d exhausted failed tx_outbox rows", outbox.ChainEID, outbox.Count),
		})
	}
	for _, held := range snapshot.TxOutboxHeld {
		if held.Count == 0 {
			continue
		}
		if _, ok := activeChains[held.ChainEID]; !ok {
			continue
		}
		// Only operator-action holds block readiness: a held row parks the
		// signer lane so no higher nonce is signed until it is resolved.
		// reprice_required (below the automatic replacement cap) and
		// nonce_reconcile_required self-heal through the automatic replacement
		// and nonce reconciliation loops, and cancel_requested is an
		// operator-initiated cancel already converging; a reprice hold past
		// the cap surfaces as the synthetic reprice_exhausted reason and needs
		// the operator.
		switch held.HeldReason {
		case db.HeldManual, db.HeldNonceConsumedExternally, db.HeldRepriceExhausted, db.HeldBroadcastExhausted:
			issues = append(issues, Issue{
				Code:    "held_signer_lane",
				Message: fmt.Sprintf("chain %d signer %s has %d held(%s) tx_outbox rows blocking the nonce lane", held.ChainEID, held.SignerID, held.Count, held.HeldReason),
			})
		case db.HeldRepriceRequired:
			// Self-healing only while the automatic reprice can actually land
			// a bump. When the configured fee cap blocks the mandatory bump,
			// every attempt defers before inserting anything, the replacement
			// count never reaches reprice_exhausted, and the lane stays
			// blocked with no synthetic escalation — so a reprice hold that
			// has aged far past its one-minute cooldown cycle is stalled and
			// needs the operator (a cap fix plus replace, or a cancel).
			if held.OldestAgeSeconds > repriceHeldStallSeconds {
				issues = append(issues, Issue{
					Code:    "held_signer_lane",
					Message: fmt.Sprintf("chain %d signer %s has %d held(reprice_required) tx_outbox rows stalled for %ds, likely blocked by the fee cap", held.ChainEID, held.SignerID, held.Count, held.OldestAgeSeconds),
				})
			}
		}
	}
	for _, packet := range snapshot.Packets {
		if packet.Status != string(packets.ExecutorManualReview) || packet.Count == 0 {
			continue
		}
		if _, ok := activeChains[packet.SrcEID]; !ok {
			continue
		}
		if _, ok := activeChains[packet.DstEID]; !ok {
			continue
		}
		issues = append(issues, Issue{
			Code:    "packet_manual_review",
			Message: fmt.Sprintf("pathway %d -> %d has %d packets requiring manual review", packet.SrcEID, packet.DstEID, packet.Count),
		})
	}
	if services.ExecutorEnabled {
		for _, job := range snapshot.ExecutorJobs {
			if job.Count == 0 {
				continue
			}
			switch job.Status {
			case string(packets.ExecutorLzReceiveFailed):
				issues = append(issues, Issue{
					Code:    "executor_lz_receive_failed",
					Message: fmt.Sprintf("executor has %d failed lzReceive jobs", job.Count),
				})
			case string(packets.ExecutorManualReview):
				issues = append(issues, Issue{
					Code:    "executor_manual_review",
					Message: fmt.Sprintf("executor has %d jobs requiring manual review", job.Count),
				})
			}
		}
	}
	if services.DVNEnabled {
		for _, job := range snapshot.DVNJobs {
			if job.Count == 0 {
				continue
			}
			switch job.Status {
			case string(packets.DVNQuorumConflict):
				issues = append(issues, Issue{
					Code:    "dvn_quorum_conflict",
					Message: fmt.Sprintf("dvn has %d quorum conflict jobs", job.Count),
				})
			case string(packets.DVNReorgDetected):
				issues = append(issues, Issue{
					Code:    "dvn_reorg_detected",
					Message: fmt.Sprintf("dvn has %d jobs waiting for reorg rollback", job.Count),
				})
			case string(packets.DVNManualReview):
				issues = append(issues, Issue{
					Code:    "dvn_manual_review",
					Message: fmt.Sprintf("dvn has %d jobs requiring manual review", job.Count),
				})
			}
		}
	}
	cursorProgress := make(map[uint32]map[string]uint64)
	for _, cursor := range snapshot.IndexerCursors {
		if _, ok := activeChains[cursor.ChainEID]; !ok {
			continue
		}
		if cursorProgress[cursor.ChainEID] == nil {
			cursorProgress[cursor.ChainEID] = make(map[string]uint64)
		}
		cursorProgress[cursor.ChainEID][cursor.Stream] = cursor.LastBlock
	}
	for chainEID, streams := range requiredCursors {
		for stream := range streams {
			lastBlock, ok := cursorProgress[chainEID][stream]
			if !ok {
				issues = append(issues, Issue{
					Code:    "indexer_cursor_missing",
					Message: fmt.Sprintf("chain %d is missing indexer cursor %q", chainEID, stream),
				})
				continue
			}
			if lastBlock == 0 {
				issues = append(issues, Issue{
					Code:    "indexer_cursor_unstarted",
					Message: fmt.Sprintf("chain %d indexer cursor %q has not advanced", chainEID, stream),
				})
			}
		}
	}
	return Report{Ready: len(issues) == 0, Issues: issues, Stats: snapshot}
}

func requireCursor(required map[uint32]map[string]struct{}, chainEID uint32, stream string) {
	if required[chainEID] == nil {
		required[chainEID] = make(map[string]struct{})
	}
	required[chainEID][stream] = struct{}{}
}
