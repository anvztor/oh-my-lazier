package txmgr

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

// providerReportingClient adds the optional Providers() and CheckHead()
// surfaces the balance monitor probes for, mirroring the production rpcquorum
// client: CheckHead reclassifies providers and Providers() returns the cached
// classification, so the fake swaps in refreshed statuses when probed.
type providerReportingClient struct {
	*fakeChainClient
	providers []rpcquorum.Provider
	// probedProviders replaces providers on CheckHead, mimicking the
	// per-call reclassification of the real quorum client.
	probedProviders []rpcquorum.Provider
	headErr         error
	headProbes      int
}

func (c *providerReportingClient) Providers() []rpcquorum.Provider {
	return c.providers
}

func (c *providerReportingClient) CheckHead(context.Context) (rpcquorum.HeadResult, error) {
	c.headProbes++
	if c.probedProviders != nil {
		c.providers = c.probedProviders
	}
	return rpcquorum.HeadResult{}, c.headErr
}

type recordingBalanceRecorder struct {
	balanceChains  []uint32
	providerChains []uint32
	providerNames  []string
	providers      [][]rpcquorum.Provider
	// events captures the call order: the provider report must land before the
	// balance read so a hung balance provider cannot suppress it.
	events []string
}

func (r *recordingBalanceRecorder) RecordSignerBalance(chainEID uint32, signerID string, balance, minNativeBalanceWei *big.Int, duration time.Duration, err error) {
	r.balanceChains = append(r.balanceChains, chainEID)
	r.events = append(r.events, "balance")
}

func (r *recordingBalanceRecorder) RecordRPCProviders(chainEID uint32, chainName string, providers []rpcquorum.Provider) {
	r.providerChains = append(r.providerChains, chainEID)
	r.providerNames = append(r.providerNames, chainName)
	r.providers = append(r.providers, providers)
	r.events = append(r.events, "providers")
}

// TestBalanceMonitorReportsProviderStatuses covers deployments without any
// indexer (for example pricing-only): the balance monitor must surface each
// quorum client's provider classification, or the provider conflict alert
// could never fire there.
func TestBalanceMonitorReportsProviderStatuses(t *testing.T) {
	signer := newTestKeystoreSigner(t)
	// The startup cache says both providers are healthy; only the head quorum
	// probe run by the poll reveals the conflict. Recording the probed state
	// proves the monitor refreshes classifications instead of exporting the
	// stale cache forever.
	client := &providerReportingClient{
		fakeChainClient: &fakeChainClient{balance: big.NewInt(1)},
		providers: []rpcquorum.Provider{
			{ID: "provider[0]", Status: rpcquorum.ProviderHealthy},
			{ID: "provider[1]", Status: rpcquorum.ProviderHealthy},
		},
		probedProviders: []rpcquorum.Provider{
			{ID: "provider[0]", Status: rpcquorum.ProviderHealthy},
			{ID: "provider[1]", Status: rpcquorum.ProviderConflict, LogConflict: true},
		},
	}
	recorder := &recordingBalanceRecorder{}
	monitor := NewBalanceMonitor([]Target{{
		ChainEID:  40161,
		ChainName: "ethereum-sepolia",
		Signer:    signer,
		Client:    client,
	}}, recorder, discardLogger())

	if err := monitor.pollOnce(t.Context()); err != nil {
		t.Fatalf("pollOnce() error = %v", err)
	}
	if len(recorder.balanceChains) != 1 || recorder.balanceChains[0] != 40161 {
		t.Fatalf("balance records = %v, want one for chain 40161", recorder.balanceChains)
	}
	if len(recorder.providerChains) != 1 || recorder.providerChains[0] != 40161 {
		t.Fatalf("provider records = %v, want one for chain 40161", recorder.providerChains)
	}
	if recorder.providerNames[0] != "ethereum-sepolia" {
		t.Fatalf("provider record chain name = %q, want ethereum-sepolia", recorder.providerNames[0])
	}
	reported := recorder.providers[0]
	if len(reported) != 2 {
		t.Fatalf("reported providers = %d, want 2", len(reported))
	}
	if client.headProbes != 1 {
		t.Fatalf("head probes = %d, want 1 per poll", client.headProbes)
	}
	if reported[0].Status != rpcquorum.ProviderHealthy || reported[1].Status != rpcquorum.ProviderConflict || !reported[1].LogConflict {
		t.Fatalf("reported provider statuses = %+v, want the probed healthy and conflict(log)", reported)
	}
	// The provider report precedes the balance read: a hung balance provider
	// must not be able to keep the classifications off the metrics endpoint.
	if len(recorder.events) != 2 || recorder.events[0] != "providers" || recorder.events[1] != "balance" {
		t.Fatalf("recorder events = %v, want providers before balance", recorder.events)
	}
}

// TestBalanceMonitorReportsProvidersWhenHeadProbeFails keeps the report alive
// through a failing quorum round: the degraded classifications the probe left
// behind are exactly what the alerts need to see.
func TestBalanceMonitorReportsProvidersWhenHeadProbeFails(t *testing.T) {
	signer := newTestKeystoreSigner(t)
	client := &providerReportingClient{
		fakeChainClient: &fakeChainClient{balance: big.NewInt(1)},
		providers: []rpcquorum.Provider{
			{ID: "provider[0]", Status: rpcquorum.ProviderHealthy},
		},
		probedProviders: []rpcquorum.Provider{
			{ID: "provider[0]", Status: rpcquorum.ProviderUnavailable},
		},
		headErr: errors.New("head quorum unavailable"),
	}
	recorder := &recordingBalanceRecorder{}
	monitor := NewBalanceMonitor([]Target{{
		ChainEID:  40161,
		ChainName: "ethereum-sepolia",
		Signer:    signer,
		Client:    client,
	}}, recorder, discardLogger())

	if err := monitor.pollOnce(t.Context()); err != nil {
		t.Fatalf("pollOnce() error = %v", err)
	}
	if len(recorder.providers) != 1 || recorder.providers[0][0].Status != rpcquorum.ProviderUnavailable {
		t.Fatalf("reported providers = %+v, want the probe's unavailable classification", recorder.providers)
	}
}

// TestBalanceMonitorSkipsProviderReportWithoutStatusSurface keeps the report
// strictly optional: a client without Providers() records balances only.
func TestBalanceMonitorSkipsProviderReportWithoutStatusSurface(t *testing.T) {
	signer := newTestKeystoreSigner(t)
	recorder := &recordingBalanceRecorder{}
	monitor := NewBalanceMonitor([]Target{{
		ChainEID:  40161,
		ChainName: "ethereum-sepolia",
		Signer:    signer,
		Client:    &fakeChainClient{balance: big.NewInt(1)},
	}}, recorder, discardLogger())

	if err := monitor.pollOnce(t.Context()); err != nil {
		t.Fatalf("pollOnce() error = %v", err)
	}
	if len(recorder.balanceChains) != 1 {
		t.Fatalf("balance records = %v, want one", recorder.balanceChains)
	}
	if len(recorder.providerChains) != 0 {
		t.Fatalf("provider records = %v, want none", recorder.providerChains)
	}
}
