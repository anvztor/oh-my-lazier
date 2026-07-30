package txmgr

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"time"

	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/signer"
)

const (
	defaultPollInterval = 5 * time.Second

	// DefaultStaleBroadcastReplacementAfter is the production default before same-nonce replacement.
	DefaultStaleBroadcastReplacementAfter = 15 * time.Minute
	// DefaultPreSignRPCTimeout bounds the estimate-gas and fee-quote preflight for one attempt.
	DefaultPreSignRPCTimeout = 30 * time.Second
	// DefaultSignTimeout bounds one KMS or keystore SignTx call so a hung signer
	// backend cannot outlive the signing lease.
	DefaultSignTimeout = 30 * time.Second
	// DefaultSigningLeaseTTL covers the preflight, signing, and the attempt insert.
	DefaultSigningLeaseTTL = 90 * time.Second
	// DefaultSendTimeout bounds one SendTransaction call; the broadcast lease lets
	// another instance replay the persisted raw if this one hangs or dies.
	DefaultSendTimeout = 15 * time.Second
	// DefaultBroadcastLeaseTTL is how long a claimed attempt stays reserved for one sender.
	DefaultBroadcastLeaseTTL = 45 * time.Second
	// DefaultNonceReconcileInterval is the backoff between confirmed-nonce
	// reconciliation passes for a signer lane with held rows.
	DefaultNonceReconcileInterval = time.Minute
)

// Options controls tx manager runtime behavior.
type Options struct {
	// StaleBroadcastReplacementAfter is how long a broadcast row can lack a receipt before same-nonce replacement.
	StaleBroadcastReplacementAfter time.Duration
	// PreSignRPCTimeout bounds the estimate-gas and fee-quote preflight for one attempt.
	PreSignRPCTimeout time.Duration
	// SignTimeout bounds one SignTx call.
	SignTimeout time.Duration
	// SigningLeaseTTL is the outbox signing lease duration; it must cover the
	// preflight, the signing call, and the durable attempt insert.
	SigningLeaseTTL time.Duration
	// SendTimeout bounds one SendTransaction call.
	SendTimeout time.Duration
	// BroadcastLeaseTTL is the attempt broadcast lease duration.
	BroadcastLeaseTTL time.Duration
	// NonceReconcileInterval is the backoff between confirmed-nonce
	// reconciliation passes for one signer lane.
	NonceReconcileInterval time.Duration
}

// Target binds one configured chain RPC client to the signer that should consume its tx_outbox rows.
type Target struct {
	ChainEID uint32
	// ChainName labels provider status metrics reported through the balance
	// monitor; it plays no role in transaction processing.
	ChainName string
	ChainID   *big.Int
	Signer    signer.Signer
	Client    ChainClient
	// Confirmations is the number of blocks a receipt must be buried under
	// before its terminal workflow state is applied, mirroring the indexer's
	// confirmation gate so a short reorg cannot leave the database in a terminal
	// state for a transaction the chain rolled back. Zero disables the gate.
	Confirmations       uint64
	FeePolicies         map[string]FeePolicy
	MinNativeBalanceWei *big.Int
}

// Manager owns transaction outbox processing and nonce assignment.
type Manager struct {
	store        *db.Store
	targets      []Target
	pollInterval time.Duration
	options      Options
	logger       *slog.Logger
}

// New creates a transaction manager using the shared store.
func New(store *db.Store, logger *slog.Logger) *Manager {
	return NewWithTargets(store, nil, logger)
}

// NewWithOptions creates a transaction manager with runtime options.
func NewWithOptions(store *db.Store, logger *slog.Logger, options Options) *Manager {
	return NewWithTargetsAndOptions(store, nil, logger, options)
}

// NewWithTargets creates a transaction manager with configured chain/signing targets.
func NewWithTargets(store *db.Store, targets []Target, logger *slog.Logger) *Manager {
	return NewWithTargetsAndOptions(store, targets, logger, Options{})
}

// NewWithTargetsAndOptions creates a transaction manager with configured targets and runtime options.
func NewWithTargetsAndOptions(store *db.Store, targets []Target, logger *slog.Logger, options Options) *Manager {
	copiedTargets := make([]Target, len(targets))
	copy(copiedTargets, targets)
	manager := &Manager{
		store:        store,
		targets:      copiedTargets,
		pollInterval: defaultPollInterval,
		options:      normalizeOptions(options),
		logger:       logger,
	}
	return manager
}

func normalizeOptions(options Options) Options {
	if options.StaleBroadcastReplacementAfter <= 0 {
		options.StaleBroadcastReplacementAfter = DefaultStaleBroadcastReplacementAfter
	}
	if options.PreSignRPCTimeout <= 0 {
		options.PreSignRPCTimeout = DefaultPreSignRPCTimeout
	}
	if options.SignTimeout <= 0 {
		options.SignTimeout = DefaultSignTimeout
	}
	if options.SigningLeaseTTL <= 0 {
		options.SigningLeaseTTL = DefaultSigningLeaseTTL
	}
	if options.SendTimeout <= 0 {
		options.SendTimeout = DefaultSendTimeout
	}
	if options.BroadcastLeaseTTL <= 0 {
		options.BroadcastLeaseTTL = DefaultBroadcastLeaseTTL
	}
	if options.NonceReconcileInterval <= 0 {
		options.NonceReconcileInterval = DefaultNonceReconcileInterval
	}
	return options
}

// Run starts the transaction manager loop until the context is canceled.
func (m *Manager) Run(ctx context.Context) error {
	return m.runLoop(ctx, m.processOnce)
}

func (m *Manager) runLoop(ctx context.Context, processOnce func(context.Context) (bool, error)) error {
	m.logger.Info("tx manager loop started", "targets", len(m.targets))
	for {
		processed, err := processOnce(ctx)
		if err != nil {
			return err
		}
		if processed {
			continue
		}
		timer := time.NewTimer(m.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *Manager) processOnce(ctx context.Context) (bool, error) {
	processed := false
	for _, target := range m.targets {
		signerID := "<nil>"
		if target.Signer != nil {
			signerID = target.Signer.Address().Hex()
		}
		// One durable action per target per pass; the hot loop reruns immediately
		// while anything was processed. Receipts run first so a mined tx stops
		// replacements and higher nonces; broadcasting persisted raws precedes
		// creating new signed work.
		id, err := m.ProcessReceipts(ctx, target, 1)
		if errors.Is(err, ErrNoReceiptUpdate) {
			// No mined receipt yet; broadcast work may still be due.
		} else if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return processed, ctxErr
			}
			m.logger.Warn("tx receipt processing failed", "chain_eid", target.ChainEID, "signer", signerID, "error", err.Error())
			continue
		} else {
			processed = true
			m.logger.Info("processed tx receipt", "id", id, "chain_eid", target.ChainEID, "signer", signerID)
			continue
		}
		id, err = m.ProcessNonceReconciliation(ctx, target)
		if errors.Is(err, db.ErrNoNonceReconcileWork) {
			// No held lane due for reconciliation; cancel work may be due.
		} else if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return processed, ctxErr
			}
			m.logger.Warn("nonce reconciliation failed", "chain_eid", target.ChainEID, "signer", signerID, "error", err.Error())
			continue
		} else {
			processed = true
			m.logger.Info("processed nonce reconciliation", "id", id, "chain_eid", target.ChainEID, "signer", signerID)
			continue
		}
		id, err = m.ProcessCancelRequest(ctx, target)
		if errors.Is(err, db.ErrNoCancelWork) || errors.Is(err, db.ErrOutboxLeaseLost) || errors.Is(err, db.ErrActiveAttemptChanged) {
			// No due cancel request, or normal contention; broadcast work may be due.
		} else if errors.Is(err, ErrTxDeferred) {
			// The cancel fee would exceed configured caps; the request was pushed back.
			continue
		} else if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return processed, ctxErr
			}
			m.logger.Warn("cancel request processing failed", "chain_eid", target.ChainEID, "signer", signerID, "error", err.Error())
			continue
		} else {
			processed = true
			m.logger.Info("processed cancel request", "id", id, "chain_eid", target.ChainEID, "signer", signerID)
			continue
		}
		id, err = m.ProcessBroadcast(ctx, target)
		if errors.Is(err, db.ErrNoBroadcastCandidate) || errors.Is(err, db.ErrSignerLaneBlocked) || errors.Is(err, db.ErrOutboxLeaseLost) {
			// Normal contention or no due attempt; replacement work may be due.
		} else if errors.Is(err, db.ErrBroadcastLaneHeld) {
			// Parking an exhausted lane is durable progress worth a hot rerun.
			processed = true
			m.logger.Info("held exhausted broadcast lane", "chain_eid", target.ChainEID, "signer", signerID)
			continue
		} else if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return processed, ctxErr
			}
			m.logger.Warn("tx broadcast processing failed", "chain_eid", target.ChainEID, "signer", signerID, "error", err.Error())
			continue
		} else {
			processed = true
			m.logger.Info("processed tx broadcast", "id", id, "chain_eid", target.ChainEID, "signer", signerID)
			continue
		}
		id, err = m.ProcessStaleBroadcastReplacement(ctx, target)
		if errors.Is(err, db.ErrNoStaleBroadcastReplacement) {
			// No stale pending broadcast; failed retries may still be due.
		} else if errors.Is(err, ErrTxDeferred) {
			// Replacement fee would exceed configured caps; keep polling the original tx hash.
			continue
		} else if errors.Is(err, db.ErrOutboxLeaseLost) || errors.Is(err, db.ErrActiveAttemptChanged) {
			// Another instance won the replacement race; nothing to do here.
			continue
		} else if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return processed, ctxErr
			}
			m.logger.Warn("stale tx replacement processing failed", "chain_eid", target.ChainEID, "signer", signerID, "error", err.Error())
			continue
		} else {
			processed = true
			m.logger.Info("processed stale broadcast tx replacement", "id", id, "chain_eid", target.ChainEID, "signer", signerID)
			continue
		}
		id, err = m.ProcessFailedRetry(ctx, target)
		if errors.Is(err, db.ErrNoFailedTxRetry) {
			// No due failed retry; queued work may still be available.
		} else if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return processed, ctxErr
			}
			m.logger.Warn("failed tx retry processing failed", "chain_eid", target.ChainEID, "signer", signerID, "error", err.Error())
			continue
		} else {
			processed = true
			m.logger.Info("requeued failed tx outbox row", "id", id, "chain_eid", target.ChainEID, "signer", signerID)
			continue
		}
		id, err = m.ProcessNext(ctx, target)
		if errors.Is(err, ErrNoQueuedTx) || errors.Is(err, ErrTxDeferred) ||
			errors.Is(err, db.ErrSignerLaneBlocked) || errors.Is(err, db.ErrOutboxLeaseLost) ||
			errors.Is(err, db.ErrTxSendScopeInactive) {
			// No signable work, a fee deferral, normal multi-instance contention,
			// or a pause/disable that landed between selection and the claim.
			continue
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return processed, ctxErr
			}
			m.logger.Warn("queued tx processing failed", "chain_eid", target.ChainEID, "signer", signerID, "error", err.Error())
			continue
		}
		processed = true
		m.logger.Info("processed tx outbox row", "id", id, "chain_eid", target.ChainEID, "signer", signerID)
	}
	return processed, nil
}
