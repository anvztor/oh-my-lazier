package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// txRetryScopeDeferDelay is how far a failed row's automatic retry is pushed
// while its send scope is paused; each due cycle re-defers until unpause.
const txRetryScopeDeferDelay = time.Minute

// ErrTxSendScopeInactive reports that a transaction's send scope (its packet
// pathway or its chain) is paused, disabled, or removed from config. Callers
// treat it as a normal skip: the work stays where it is and resumes once the
// scope is active again.
var ErrTxSendScopeInactive = errors.New("tx send scope is paused or disabled")

// TxPurposePricingSetPriceSnapshot is the pricing bot's outbox purpose; it is
// the only chain-scoped purpose (no packet, gated by its send chain alone).
const TxPurposePricingSetPriceSnapshot = "pricing_set_price_snapshot"

const (
	txPurposeExecutorCommitVerification = "executor_commit_verification"
	txPurposeDVNVerify                  = "dvn_verify"
)

type txSendScope int

const (
	// txSendScopeNone marks purposes with no pause carrier; only rows that never
	// reach the send pipeline gates (test fixtures, direct seeds) carry one, and
	// the signing gate fails closed on them.
	txSendScopeNone txSendScope = iota
	txSendScopeChain
	txSendScopePacket
)

// txSendScopeKind is the closed purpose→scope mapping. The three packet
// purposes require a valid GUID, packet, and exact pathway; pricing requires an
// empty GUID and is gated by its chain; anything else has no defined scope and
// the gates fail closed.
func txSendScopeKind(purpose string) txSendScope {
	switch purpose {
	case TxPurposePricingSetPriceSnapshot:
		return txSendScopeChain
	case txPurposeExecutorCommitVerification, txPurposeExecutorLzReceive, txPurposeDVNVerify:
		return txSendScopePacket
	default:
		return txSendScopeNone
	}
}

// lockTxSendScope validates that the transaction's send scope is active and
// share-locks its pause carriers (chains in eid order, then the pathway) for
// the rest of the surrounding transaction, so a concurrent pause/disable
// writer serializes against this decision instead of racing it: whichever
// commits first is visible to the other. Lock order across the codebase is
// advisory lock → outbox/job rows → chains (eid ascending) → pathway.
//
// It returns ErrTxSendScopeInactive when the scope is paused, disabled, or
// gone from config, and a non-sentinel error on malformed rows (unknown
// purpose, missing or mismatched GUID/packet) so misconfigured work fails
// closed instead of being signed.
func lockTxSendScope(ctx context.Context, tx pgx.Tx, chainEID uint32, purpose string, guid []byte) error {
	switch txSendScopeKind(purpose) {
	case txSendScopeChain:
		if len(guid) != 0 {
			return fmt.Errorf("purpose %s must not carry a packet guid", purpose)
		}
		return lockChainSendScope(ctx, tx, chainEID)
	case txSendScopePacket:
		return lockPacketSendScope(ctx, tx, chainEID, guid)
	default:
		return fmt.Errorf("purpose %q has no send scope; refusing to sign", purpose)
	}
}

func lockChainSendScope(ctx context.Context, tx pgx.Tx, chainEID uint32) error {
	var enabled, paused bool
	err := tx.QueryRow(ctx, `
		SELECT enabled, paused FROM chains WHERE eid = $1 FOR SHARE
	`, chainEID).Scan(&enabled, &paused)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTxSendScopeInactive
	}
	if err != nil {
		return err
	}
	if !enabled || paused {
		return ErrTxSendScopeInactive
	}
	return nil
}

func lockPacketSendScope(ctx context.Context, tx pgx.Tx, chainEID uint32, guid []byte) error {
	if len(guid) != common.HashLength {
		return fmt.Errorf("packet-scoped tx requires a %d-byte guid, got %d bytes", common.HashLength, len(guid))
	}
	var srcEID, dstEID uint32
	var sender, receiver []byte
	err := tx.QueryRow(ctx, `
		SELECT src_eid, dst_eid, sender, receiver FROM packets WHERE guid = $1
	`, guid).Scan(&srcEID, &dstEID, &sender, &receiver)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("packet %s not found for send scope", common.BytesToHash(guid))
	}
	if err != nil {
		return err
	}
	// Every packet purpose sends on the packet's destination chain; a row whose
	// chain does not match could otherwise pass the pathway check while actually
	// spending on a different, possibly paused, chain.
	if chainEID != dstEID {
		return fmt.Errorf("outbox chain %d does not match packet %s destination %d", chainEID, common.BytesToHash(guid), dstEID)
	}

	// Chains first, ascending eid, so every scope locker and SyncConfig acquire
	// chain row locks in the same total order.
	eids := []uint32{srcEID, dstEID}
	if eids[0] > eids[1] {
		eids[0], eids[1] = eids[1], eids[0]
	}
	want := 2
	if eids[0] == eids[1] {
		eids = eids[:1]
		want = 1
	}
	rows, err := tx.Query(ctx, `
		SELECT enabled, paused FROM chains WHERE eid = ANY($1) ORDER BY eid FOR SHARE
	`, eids)
	if err != nil {
		return err
	}
	locked := 0
	active := true
	for rows.Next() {
		var enabled, paused bool
		if err := rows.Scan(&enabled, &paused); err != nil {
			rows.Close()
			return err
		}
		locked++
		if !enabled || paused {
			active = false
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if locked != want || !active {
		return ErrTxSendScopeInactive
	}

	var pwEnabled, pwPaused bool
	err = tx.QueryRow(ctx, `
		SELECT enabled, paused FROM pathways
		WHERE src_eid = $1 AND dst_eid = $2 AND src_oapp = $3 AND dst_oapp = $4
		FOR SHARE
	`, srcEID, dstEID, sender, receiver).Scan(&pwEnabled, &pwPaused)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTxSendScopeInactive
	}
	if err != nil {
		return err
	}
	if !pwEnabled || pwPaused {
		return ErrTxSendScopeInactive
	}
	return nil
}

// txSendScopeActiveSQL is the lock-free selection-side twin of lockTxSendScope
// for filtering candidate rows (o = tx_outbox): rows that already hold a nonce
// are in flight and always pass, and rows without one pass only when their
// closed-mapped scope is active. Unknown purposes match no branch and are
// filtered out, mirroring the signing gate's fail-closed behavior.
const txSendScopeActiveSQL = `(
	o.nonce IS NOT NULL
	OR (
		o.purpose = 'pricing_set_price_snapshot' AND o.guid IS NULL
		AND EXISTS (
			SELECT 1 FROM chains c
			WHERE c.eid = o.chain_eid AND c.enabled AND NOT c.paused
		)
	)
	OR (
		o.purpose IN ('executor_commit_verification', 'executor_lz_receive', 'dvn_verify')
		AND EXISTS (
			SELECT 1 FROM packets p
			JOIN pathways pw ON pw.src_eid = p.src_eid AND pw.dst_eid = p.dst_eid
				AND pw.src_oapp = p.sender AND pw.dst_oapp = p.receiver
			WHERE p.guid = o.guid AND p.dst_eid = o.chain_eid
				AND pw.enabled AND NOT pw.paused
				AND NOT EXISTS (
					SELECT 1 FROM chains c
					WHERE c.eid IN (p.src_eid, p.dst_eid) AND (NOT c.enabled OR c.paused)
				)
		)
	)
)`
