package db

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// txPricingPendingStatuses are the outbox statuses in which a pricing snapshot
// transaction can still land on chain, including held lanes and rows under a
// pending operator cancel. A failed pricing row is terminal: pricing rows are
// excluded from every retry path because their calldata carries a time-bound
// market observation that must never be re-signed later.
var txPricingPendingStatuses = []string{
	TxStatusQueued, TxStatusNonceAssigned, TxStatusSigned, TxStatusBroadcast, TxStatusHeld,
}

// txRetryableLegacyPricingSQL matches a failed pricing row that still OWNS an
// unconsumed nonce (pre-attempt-cutover sign/broadcast failure kinds on an
// upgraded database). Such a row gates its feed like a pending one: the bot
// must not enqueue fresh work above the nonce gap (a later transaction cannot
// mine before it, deadlocking both), and the operator's in-place requeue is
// its only recovery. Canceled/externally-consumed rows are terminal operator
// resolutions, and receipt failures consumed their nonce when they mined.
const txRetryableLegacyPricingSQL = `(
	status = 'failed' AND nonce IS NOT NULL AND receipt_outcome IS NULL
	AND COALESCE(failure_kind, '') NOT IN ('canceled', 'nonce_consumed_externally', 'receipt_failed')
)`

// ErrPricingPendingExists reports that another pricing snapshot transaction
// for the same feed is still pending, so a new one must not be enqueued.
var ErrPricingPendingExists = errors.New("a pricing snapshot tx for this feed is still pending")

// PendingPricingTx is one pricing outbox row that can still land on chain.
type PendingPricingTx struct {
	ID        int64
	SignerID  string
	To        common.Address
	Calldata  []byte
	Status    string
	CreatedAt time.Time
}

// ListPendingPricingTxs returns every pricing outbox row on the chain that can
// still land on chain, across all signers: after a signer rotation an old
// signer's broadcast row can still mine, so it must keep gating new writes.
func (s *Store) ListPendingPricingTxs(ctx context.Context, chainEID uint32) ([]PendingPricingTx, error) {
	if chainEID == 0 {
		return nil, errors.New("chain eid is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, signer_id, to_address, calldata, status, created_at
		FROM tx_outbox
		WHERE chain_eid = $1 AND purpose = $2
			AND (status = ANY($3) OR `+txRetryableLegacyPricingSQL+`)
		ORDER BY id
	`, chainEID, TxPurposePricingSetPriceSnapshot, txPricingPendingStatuses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []PendingPricingTx
	for rows.Next() {
		var row PendingPricingTx
		var toBytes []byte
		if err := rows.Scan(&row.ID, &row.SignerID, &toBytes, &row.Calldata, &row.Status, &row.CreatedAt); err != nil {
			return nil, err
		}
		row.To = common.BytesToAddress(toBytes)
		pending = append(pending, row)
	}
	return pending, rows.Err()
}

// EnqueuePricingSnapshotTx inserts a pricing snapshot transaction after
// re-checking, under a per-(chain, feed) advisory lock, that no pending row
// for the same feed exists. Two bot instances can otherwise both observe an
// empty pending set and enqueue duplicate snapshots for one feed.
func (s *Store) EnqueuePricingSnapshotTx(ctx context.Context, request TxRequest) (int64, error) {
	if request.ChainEID == 0 {
		return 0, errors.New("chain eid is required")
	}
	if request.Purpose != TxPurposePricingSetPriceSnapshot {
		return 0, errors.New("purpose must be the pricing snapshot purpose")
	}
	if request.To == (common.Address{}) {
		return 0, errors.New("to address is required")
	}
	if request.SignerID == "" {
		return 0, errors.New("signer id is required")
	}
	value := request.Value
	if value == nil {
		value = new(big.Int)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock($1::integer, hashtext($2)::integer)",
		int32(request.ChainEID), "pricing_feed:"+request.To.Hex(),
	); err != nil {
		return 0, err
	}
	if err := lockTxSendScope(ctx, tx, request.ChainEID, request.Purpose, request.GUID); err != nil {
		return 0, err
	}
	var pendingExists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM tx_outbox
			WHERE chain_eid = $1 AND purpose = $2 AND to_address = $3
				AND (status = ANY($4) OR `+txRetryableLegacyPricingSQL+`)
		)
	`, request.ChainEID, request.Purpose, addressBytes(request.To), txPricingPendingStatuses).Scan(&pendingExists); err != nil {
		return 0, err
	}
	if pendingExists {
		return 0, ErrPricingPendingExists
	}
	var id int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO tx_outbox (
			chain_eid, purpose, guid, to_address, calldata, value,
			signer_id, status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`, request.ChainEID, request.Purpose, optionalBytes(request.GUID), addressBytes(request.To), request.Calldata, value.String(), request.SignerID, TxStatusQueued).Scan(&id); err != nil {
		return 0, err
	}
	return id, tx.Commit(ctx)
}

// ListPendingPricingSigners returns the distinct signers that still own
// pending pricing rows on the chain, plus signers owning retryable legacy
// failures (a failed row still holding an unconsumed nonce with no pinned
// receipt). A signer rotated out of the pricing config must keep a
// transaction-manager target until its rows drain — including the legacy rows
// an operator may requeue in place while the worker is running, which would
// otherwise become pending with no target able to process them until restart.
func (s *Store) ListPendingPricingSigners(ctx context.Context, chainEID uint32) ([]string, error) {
	if chainEID == 0 {
		return nil, errors.New("chain eid is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT signer_id
		FROM tx_outbox
		WHERE chain_eid = $1 AND purpose = $2
			AND (status = ANY($3) OR `+txRetryableLegacyPricingSQL+`)
		ORDER BY signer_id
	`, chainEID, TxPurposePricingSetPriceSnapshot, txPricingPendingStatuses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var signers []string
	for rows.Next() {
		var signer string
		if err := rows.Scan(&signer); err != nil {
			return nil, err
		}
		signers = append(signers, signer)
	}
	return signers, rows.Err()
}
