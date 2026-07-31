package txmgr

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"time"

	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

const defaultBalancePollInterval = time.Minute

// balanceReadTimeout bounds one BalanceAt read. The read goes to a single
// cached-healthy provider, and a hung endpoint must not stall the poll loop
// past the next cycle — the head probe that precedes it is what reclassifies
// such a provider away for the following read.
const balanceReadTimeout = 15 * time.Second

// BalanceRecorder records signer balance polling results.
type BalanceRecorder interface {
	RecordSignerBalance(chainEID uint32, signerID string, balance, minNativeBalanceWei *big.Int, duration time.Duration, err error)
}

// BalanceMonitor polls native balances for transaction-signing targets.
type BalanceMonitor struct {
	targets      []Target
	recorder     BalanceRecorder
	logger       *slog.Logger
	pollInterval time.Duration
}

// NewBalanceMonitor creates a signer balance monitor for tx manager targets.
func NewBalanceMonitor(targets []Target, recorder BalanceRecorder, logger *slog.Logger) *BalanceMonitor {
	copiedTargets := make([]Target, len(targets))
	copy(copiedTargets, targets)
	return &BalanceMonitor{
		targets:      copiedTargets,
		recorder:     recorder,
		logger:       logger,
		pollInterval: defaultBalancePollInterval,
	}
}

// Run polls configured signer balances until the context is canceled.
func (m *BalanceMonitor) Run(ctx context.Context) error {
	if err := m.pollOnce(ctx); err != nil {
		return err
	}
	for {
		timer := time.NewTimer(m.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if err := m.pollOnce(ctx); err != nil {
			return err
		}
	}
}

func (m *BalanceMonitor) pollOnce(ctx context.Context) error {
	for _, target := range m.targets {
		if target.Signer == nil {
			return errors.New("balance monitor target signer is required")
		}
		if target.Client == nil {
			return errors.New("balance monitor target client is required")
		}
		signerID := target.Signer.Address().Hex()
		// Surface each provider's quorum classification before touching the
		// balance: in a deployment without indexers (for example pricing-only)
		// the balance monitor is the loop that runs for every
		// transaction-sending quorum client, and without this report the
		// provider conflict alert could never fire. Providers() only returns
		// cached classifications and BalanceAt is a single-provider read, so
		// run a bounded head quorum probe first — its per-call
		// reclassification keeps the exported statuses live after startup,
		// steers the balance read below away from a hung provider, and must
		// happen even when that read stalls or fails.
		if m.recorder != nil {
			if statusSource, ok := target.Client.(interface{ Providers() []rpcquorum.Provider }); ok {
				if recorder, ok := m.recorder.(interface {
					RecordRPCProviders(chainEID uint32, chainName string, providers []rpcquorum.Provider)
				}); ok {
					if prober, ok := target.Client.(interface {
						CheckHead(ctx context.Context) (rpcquorum.HeadResult, error)
					}); ok {
						if _, headErr := prober.CheckHead(ctx); headErr != nil {
							m.logger.Warn("balance poll head quorum probe failed", "chain_eid", target.ChainEID, "error", headErr.Error())
						}
					}
					recorder.RecordRPCProviders(target.ChainEID, target.ChainName, statusSource.Providers())
				}
			}
		}
		started := time.Now()
		balanceCtx, cancelBalance := context.WithTimeout(ctx, balanceReadTimeout)
		balance, err := target.Client.BalanceAt(balanceCtx, target.Signer.Address(), nil)
		cancelBalance()
		if err == nil && balance == nil {
			err = errors.New("signer balance is required")
		}
		duration := time.Since(started)
		if m.recorder != nil {
			m.recorder.RecordSignerBalance(target.ChainEID, signerID, balance, target.MinNativeBalanceWei, duration, err)
		}
		if err != nil {
			m.logger.Warn("failed signer balance poll", "chain_eid", target.ChainEID, "signer", signerID, "error", err.Error())
			continue
		}
		if target.MinNativeBalanceWei != nil && balance.Cmp(target.MinNativeBalanceWei) < 0 {
			m.logger.Warn("low signer native balance", "chain_eid", target.ChainEID, "signer", signerID, "balance_wei", balance.String(), "min_native_balance_wei", target.MinNativeBalanceWei.String())
		}
	}
	return nil
}
