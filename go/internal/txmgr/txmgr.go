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
)

// Options controls tx manager runtime behavior.
type Options struct {
	// StaleBroadcastReplacementAfter is how long a broadcast row can lack a receipt before same-nonce replacement.
	StaleBroadcastReplacementAfter time.Duration
}

// Target binds one configured chain RPC client to the signer that should consume its tx_outbox rows.
type Target struct {
	ChainEID uint32
	ChainID  *big.Int
	Signer   signer.Signer
	Client   ChainClient
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
		id, err := m.ProcessReceipts(ctx, target, 1)
		if errors.Is(err, ErrNoReceiptUpdate) {
			// No mined receipt yet; queued work may still be available.
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
		id, err = m.ProcessStaleBroadcastReplacement(ctx, target)
		if errors.Is(err, db.ErrNoStaleBroadcastReplacement) {
			// No stale pending broadcast; failed retries may still be due.
		} else if errors.Is(err, ErrTxDeferred) {
			// Replacement fee would exceed configured caps; keep polling the original tx hash.
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
		if errors.Is(err, ErrNoQueuedTx) {
			continue
		}
		if errors.Is(err, ErrTxDeferred) {
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
