package readiness

import (
	"testing"

	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
)

func TestEvaluateAcceptsCleanActiveState(t *testing.T) {
	report := Evaluate(db.StatsSnapshot{
		Chains: []db.ChainStat{
			{EID: 40161, Name: "ethereum-sepolia", Enabled: true},
			{EID: 40449, Name: "hoodi", Enabled: true},
		},
		Pathways: []db.PathwayStat{
			{SrcEID: 40161, DstEID: 40449, Enabled: true},
		},
		TxOutbox: []db.TxOutboxStat{
			{ChainEID: 40449, Status: db.TxStatusConfirmed, Count: 2},
		},
		IndexerCursors: []db.IndexerCursorStat{
			{ChainEID: 40161, Stream: executorSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: executorDestStream, LastBlock: 100},
			{ChainEID: 40161, Stream: dvnSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: dvnDestStream, LastBlock: 100},
		},
	})

	if !report.Ready {
		t.Fatalf("ready = false, issues = %+v", report.Issues)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("issues = %+v, want none", report.Issues)
	}
}

func TestEvaluateRejectsPausedActiveStateAndFailedOutbox(t *testing.T) {
	report := Evaluate(db.StatsSnapshot{
		Chains: []db.ChainStat{
			{EID: 40161, Name: "ethereum-sepolia", Enabled: true, Paused: true},
			{EID: 40449, Name: "hoodi", Enabled: true},
		},
		Pathways: []db.PathwayStat{
			{SrcEID: 40161, DstEID: 40449, Enabled: true, Paused: true},
		},
		TxOutbox: []db.TxOutboxStat{
			{ChainEID: 40449, Status: db.TxStatusFailed, RetryState: db.TxOutboxRetryStateExhausted, Count: 3},
		},
		IndexerCursors: []db.IndexerCursorStat{
			{ChainEID: 40161, Stream: executorSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: executorDestStream, LastBlock: 100},
			{ChainEID: 40161, Stream: dvnSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: dvnDestStream, LastBlock: 100},
		},
	})

	if report.Ready {
		t.Fatal("ready = true, want false")
	}
	wantCodes := []string{"chain_paused", "pathway_paused", "failed_outbox"}
	if len(report.Issues) != len(wantCodes) {
		t.Fatalf("issues = %+v, want %d issues", report.Issues, len(wantCodes))
	}
	for i, want := range wantCodes {
		if report.Issues[i].Code != want {
			t.Fatalf("issue[%d].code = %q, want %q", i, report.Issues[i].Code, want)
		}
	}
}

func TestEvaluateIgnoresRetryingFailedOutbox(t *testing.T) {
	report := Evaluate(db.StatsSnapshot{
		Chains: []db.ChainStat{
			{EID: 40161, Name: "ethereum-sepolia", Enabled: true},
			{EID: 40449, Name: "hoodi", Enabled: true},
		},
		Pathways: []db.PathwayStat{
			{SrcEID: 40161, DstEID: 40449, Enabled: true},
		},
		TxOutbox: []db.TxOutboxStat{
			{ChainEID: 40449, Status: db.TxStatusFailed, RetryState: db.TxOutboxRetryStateRetrying, Count: 2},
			{ChainEID: 40449, Status: db.TxStatusFailed, RetryState: db.TxOutboxRetryStateSuperseded, Count: 1},
		},
		IndexerCursors: []db.IndexerCursorStat{
			{ChainEID: 40161, Stream: executorSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: executorDestStream, LastBlock: 100},
			{ChainEID: 40161, Stream: dvnSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: dvnDestStream, LastBlock: 100},
		},
	})

	if !report.Ready {
		t.Fatalf("ready = false, issues = %+v", report.Issues)
	}
}

// TestEvaluateRejectsOperatorActionHeldLanes covers held signer lanes: a
// held(manual) or held(nonce_consumed_externally) row blocks every higher
// nonce for its signer, so readiness must fail until the operator resolves it,
// while the self-healing hold reasons and cancel intents stay green.
func TestEvaluateRejectsOperatorActionHeldLanes(t *testing.T) {
	base := db.StatsSnapshot{
		Chains: []db.ChainStat{
			{EID: 40161, Name: "ethereum-sepolia", Enabled: true},
			{EID: 40449, Name: "hoodi", Enabled: true},
		},
		Pathways: []db.PathwayStat{
			{SrcEID: 40161, DstEID: 40449, Enabled: true},
		},
		IndexerCursors: []db.IndexerCursorStat{
			{ChainEID: 40161, Stream: executorSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: executorDestStream, LastBlock: 100},
			{ChainEID: 40161, Stream: dvnSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: dvnDestStream, LastBlock: 100},
		},
	}

	blocking := base
	blocking.TxOutboxHeld = []db.TxOutboxHeldStat{
		{ChainEID: 40449, SignerID: "0x1111", HeldReason: db.HeldManual, Count: 1},
		{ChainEID: 40449, SignerID: "0x2222", HeldReason: db.HeldNonceConsumedExternally, Count: 2},
		{ChainEID: 40449, SignerID: "0x3333", HeldReason: db.HeldRepriceExhausted, Count: 1},
		{ChainEID: 40449, SignerID: "0x4444", HeldReason: db.HeldBroadcastExhausted, Count: 1},
		// An aged reprice hold cannot be self-healing: the mandatory bump is
		// blocked (typically the fee cap) or it would have escalated already.
		{ChainEID: 40449, SignerID: "0x5555", HeldReason: db.HeldRepriceRequired, Count: 1, OldestAgeSeconds: 3600},
		// The cancel age counts from the immutable request time, so an aged
		// pending cancel is stalled (typically fee-cap blocked), not converging.
		{ChainEID: 40449, SignerID: "0x6666", HeldReason: db.HeldCancelRequested, Count: 1, OldestAgeSeconds: 3600},
	}
	report := Evaluate(blocking)
	if report.Ready {
		t.Fatal("ready = true with operator-action held lanes, want false")
	}
	if len(report.Issues) != 6 {
		t.Fatalf("issues = %+v, want 6 held_signer_lane issues", report.Issues)
	}
	for _, issue := range report.Issues {
		if issue.Code != "held_signer_lane" {
			t.Fatalf("issue code = %q, want held_signer_lane", issue.Code)
		}
	}

	orphaned := base
	orphaned.TxOutboxOrphaned = []db.TxOutboxOrphanedStat{
		// A send-state row with no active attempt is never produced by a
		// healthy path, so it escalates immediately with no age threshold.
		{ChainEID: 40449, SignerID: "0x7777", Status: db.TxStatusSigned, Count: 1, OldestAgeSeconds: 5},
		{ChainEID: 40449, SignerID: "0x7777", Status: db.TxStatusBroadcast, Count: 2, OldestAgeSeconds: 5},
		// A disabled chain's rows cannot be acted on and must not page.
		{ChainEID: 50505, SignerID: "0x8888", Status: db.TxStatusSigned, Count: 1},
	}
	orphanedReport := Evaluate(orphaned)
	if orphanedReport.Ready {
		t.Fatal("ready = true with orphaned send-state rows, want false")
	}
	if len(orphanedReport.Issues) != 2 {
		t.Fatalf("issues = %+v, want 2 orphaned_outbox_row issues", orphanedReport.Issues)
	}
	for _, issue := range orphanedReport.Issues {
		if issue.Code != "orphaned_outbox_row" {
			t.Fatalf("issue code = %q, want orphaned_outbox_row", issue.Code)
		}
	}

	selfHealing := base
	selfHealing.TxOutboxHeld = []db.TxOutboxHeldStat{
		{ChainEID: 40449, SignerID: "0x1111", HeldReason: db.HeldRepriceRequired, Count: 1},
		{ChainEID: 40449, SignerID: "0x1111", HeldReason: db.HeldNonceReconcileRequired, Count: 1},
		{ChainEID: 40449, SignerID: "0x1111", HeldReason: db.HeldCancelRequested, Count: 1, OldestAgeSeconds: 60},
	}
	report = Evaluate(selfHealing)
	if !report.Ready {
		t.Fatalf("ready = false for self-healing holds, issues = %+v", report.Issues)
	}

	inactiveChain := base
	inactiveChain.TxOutboxHeld = []db.TxOutboxHeldStat{
		{ChainEID: 49999, SignerID: "0x1111", HeldReason: db.HeldManual, Count: 1},
	}
	report = Evaluate(inactiveChain)
	if !report.Ready {
		t.Fatalf("ready = false for a held lane on an inactive chain, issues = %+v", report.Issues)
	}

	pricingStalled := base
	pricingStalled.PricingPending = []db.PricingPendingStat{
		// A fresh pending write is the normal confirmation window.
		{ChainEID: 40161, Count: 1, OldestAgeSeconds: 60},
		// A stalled one gates the feed while the snapshot ages toward stale.
		{ChainEID: 40449, Count: 2, OldestAgeSeconds: 400},
	}
	report = Evaluate(pricingStalled)
	if report.Ready {
		t.Fatal("ready = true with a stalled pending pricing tx, want false")
	}
	if len(report.Issues) != 1 || report.Issues[0].Code != "pricing_pending_stalled" {
		t.Fatalf("issues = %+v, want one pricing_pending_stalled issue", report.Issues)
	}
}

func TestEvaluateRejectsMissingOrUnstartedRequiredIndexerCursors(t *testing.T) {
	report := Evaluate(db.StatsSnapshot{
		Chains: []db.ChainStat{
			{EID: 40161, Name: "ethereum-sepolia", Enabled: true},
			{EID: 40449, Name: "hoodi", Enabled: true},
		},
		Pathways: []db.PathwayStat{
			{SrcEID: 40161, DstEID: 40449, Enabled: true},
			{SrcEID: 40449, DstEID: 40161, Enabled: true},
		},
		IndexerCursors: []db.IndexerCursorStat{
			{ChainEID: 40161, Stream: executorSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: executorDestStream, LastBlock: 0},
		},
	})

	if report.Ready {
		t.Fatal("ready = true, want false")
	}
	gotCodes := make(map[string]int)
	for _, issue := range report.Issues {
		gotCodes[issue.Code]++
	}
	if gotCodes["indexer_cursor_missing"] != 6 {
		t.Fatalf("missing cursor issues = %d, want 6; issues = %+v", gotCodes["indexer_cursor_missing"], report.Issues)
	}
	if gotCodes["indexer_cursor_unstarted"] != 1 {
		t.Fatalf("unstarted cursor issues = %d, want 1; issues = %+v", gotCodes["indexer_cursor_unstarted"], report.Issues)
	}
}

func TestEvaluateRejectsUnsafeDurableJobStates(t *testing.T) {
	report := Evaluate(db.StatsSnapshot{
		Chains: []db.ChainStat{
			{EID: 40161, Name: "ethereum-sepolia", Enabled: true},
			{EID: 40449, Name: "hoodi", Enabled: true},
		},
		Pathways: []db.PathwayStat{
			{SrcEID: 40161, DstEID: 40449, Enabled: true},
		},
		Packets: []db.PacketStat{
			{SrcEID: 40161, DstEID: 40449, Status: string(packets.ExecutorManualReview), Count: 2},
		},
		ExecutorJobs: []db.StatusStat{
			{Status: string(packets.ExecutorLzReceiveFailed), Count: 1},
			{Status: string(packets.ExecutorManualReview), Count: 1},
		},
		DVNJobs: []db.StatusStat{
			{Status: string(packets.DVNQuorumConflict), Count: 1},
			{Status: string(packets.DVNReorgDetected), Count: 1},
			{Status: string(packets.DVNManualReview), Count: 1},
		},
		IndexerCursors: []db.IndexerCursorStat{
			{ChainEID: 40161, Stream: executorSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: executorDestStream, LastBlock: 100},
			{ChainEID: 40161, Stream: dvnSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: dvnDestStream, LastBlock: 100},
		},
	})

	if report.Ready {
		t.Fatal("ready = true, want false")
	}
	gotCodes := make(map[string]int)
	for _, issue := range report.Issues {
		gotCodes[issue.Code]++
	}
	for _, want := range []string{
		"packet_manual_review",
		"executor_lz_receive_failed",
		"executor_manual_review",
		"dvn_quorum_conflict",
		"dvn_reorg_detected",
		"dvn_manual_review",
	} {
		if gotCodes[want] != 1 {
			t.Fatalf("issue %q count = %d, want 1; issues = %+v", want, gotCodes[want], report.Issues)
		}
	}
}

func TestEvaluateWithServicesRequiresOnlyEnabledRoleState(t *testing.T) {
	snapshot := db.StatsSnapshot{
		Chains: []db.ChainStat{
			{EID: 40161, Name: "ethereum-sepolia", Enabled: true},
			{EID: 40449, Name: "hoodi", Enabled: true},
		},
		Pathways: []db.PathwayStat{
			{SrcEID: 40161, DstEID: 40449, Enabled: true},
		},
		ExecutorJobs: []db.StatusStat{
			{Status: string(packets.ExecutorLzReceiveFailed), Count: 1},
		},
		DVNJobs: []db.StatusStat{
			{Status: string(packets.DVNQuorumConflict), Count: 1},
		},
		IndexerCursors: []db.IndexerCursorStat{
			{ChainEID: 40161, Stream: dvnSourceStream, LastBlock: 100},
			{ChainEID: 40449, Stream: dvnDestStream, LastBlock: 100},
		},
	}

	report := EvaluateWithServices(snapshot, Services{DVNEnabled: true})
	if report.Ready {
		t.Fatal("ready = true, want dvn conflict issue")
	}
	if len(report.Issues) != 1 || report.Issues[0].Code != "dvn_quorum_conflict" {
		t.Fatalf("issues = %+v, want only dvn conflict", report.Issues)
	}

	snapshot.DVNJobs = nil
	report = EvaluateWithServices(snapshot, Services{DVNEnabled: true})
	if !report.Ready {
		t.Fatalf("ready = false, issues = %+v", report.Issues)
	}
}

func TestEvaluateIgnoresDisabledState(t *testing.T) {
	report := Evaluate(db.StatsSnapshot{
		Chains: []db.ChainStat{
			{EID: 40161, Name: "disabled-source", Enabled: false, Paused: true},
			{EID: 40449, Name: "active-destination", Enabled: true},
		},
		Pathways: []db.PathwayStat{
			{SrcEID: 40161, DstEID: 40449, Enabled: true, Paused: true},
		},
		TxOutbox: []db.TxOutboxStat{
			{ChainEID: 40161, Status: db.TxStatusFailed, RetryState: db.TxOutboxRetryStateExhausted, Count: 3},
		},
	})

	if !report.Ready {
		t.Fatalf("ready = false, issues = %+v", report.Issues)
	}
}
