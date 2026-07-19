package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gethkeystore "github.com/ethereum/go-ethereum/accounts/keystore"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/configcheck"
	"github.com/islishude/oh-my-lazier/go/internal/txmgr"
	"github.com/islishude/oh-my-lazier/go/internal/workerloop"
)

func TestTxTargetsLoadsKeystoreSignerForEveryChain(t *testing.T) {
	stubReadTxHeader(t, &gethtypes.Header{})

	dir := t.TempDir()
	const password = "test-password"
	account, err := gethkeystore.StoreKey(dir, password, gethkeystore.StandardScryptN, gethkeystore.StandardScryptP)
	if err != nil {
		t.Fatalf("StoreKey() error = %v", err)
	}
	t.Setenv("KEYSTORE_PASSWORD", password)

	cfg := testConfig(account.Address.Hex(), filepath.Clean(account.URL.Path))
	registry, err := chain.NewRegistry(cfg.Chains, cfg.Pathways)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	worker, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	targets, err := worker.txTargets(t.Context(), registry)
	if err != nil {
		t.Fatalf("txTargets() error = %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets = %d, want one per chain", len(targets))
	}
	for _, target := range targets {
		if target.Signer.Address() != account.Address {
			t.Fatalf("target signer = %s, want %s", target.Signer.Address(), account.Address)
		}
		if target.ChainID == nil || target.ChainID.Sign() <= 0 {
			t.Fatalf("target chain id = %v, want positive", target.ChainID)
		}
		if target.Client == nil {
			t.Fatal("target client is nil")
		}
		if len(target.FeePolicies) != 2 {
			t.Fatalf("target fee policies for chain %d = %d, want 2", target.ChainEID, len(target.FeePolicies))
		}
	}
}

func TestTxTargetsRejectsDynamicFeeChainWithoutPriorityCap(t *testing.T) {
	stubReadTxHeader(t, &gethtypes.Header{BaseFee: bigOne()})

	dir := t.TempDir()
	const password = "test-password"
	account, err := gethkeystore.StoreKey(dir, password, gethkeystore.StandardScryptN, gethkeystore.StandardScryptP)
	if err != nil {
		t.Fatalf("StoreKey() error = %v", err)
	}
	t.Setenv("KEYSTORE_PASSWORD", password)

	cfg := testConfig(account.Address.Hex(), filepath.Clean(account.URL.Path))
	cfg.Chains[0].TxRoles.Executor.MaxPriorityFeePerGasWei = ""
	registry, err := chain.NewRegistry(cfg.Chains, cfg.Pathways)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	worker, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = worker.txTargets(t.Context(), registry)
	if err == nil {
		t.Fatal("txTargets() error = nil, want dynamic priority cap error")
	}
	if !strings.Contains(err.Error(), "max_priority_fee_per_gas_wei is required") {
		t.Fatalf("txTargets() error = %v, want priority cap error", err)
	}
}

func TestTxTargetsSelectsTargetsForEnabledRoles(t *testing.T) {
	stubReadTxHeader(t, &gethtypes.Header{})

	dir := t.TempDir()
	const password = "test-password"
	account, err := gethkeystore.StoreKey(dir, password, gethkeystore.StandardScryptN, gethkeystore.StandardScryptP)
	if err != nil {
		t.Fatalf("StoreKey() error = %v", err)
	}
	t.Setenv("KEYSTORE_PASSWORD", password)

	tests := []struct {
		name         string
		mutate       func(*config.Config)
		wantPurposes map[uint32][]string
	}{
		{
			name: "executor only by default",
			wantPurposes: map[uint32][]string{
				40161: {"executor_commit_verification", "executor_lz_receive"},
				40449: {"executor_commit_verification", "executor_lz_receive"},
			},
		},
		{
			name: "dvn only active",
			mutate: func(cfg *config.Config) {
				cfg.Services.Executor.Enabled = new(false)
				cfg.Pathways[0].DVN.Mode = config.DVNModeActive
				cfg.Chains[1].TxRoles.DVN = testDVNRole(config.MustEVMAddress(account.Address.Hex()))
			},
			wantPurposes: map[uint32][]string{
				40449: {"dvn_verify"},
			},
		},
		{
			name: "pricing only",
			mutate: func(cfg *config.Config) {
				cfg.Services.Executor.Enabled = new(false)
				cfg.Services.DVN.Enabled = new(false)
				cfg.Pricing = testPricingConfig()
				cfg.Pricing.Signer = config.MustEVMAddress(account.Address.Hex())
			},
			wantPurposes: map[uint32][]string{
				40161: {"pricing_set_price_snapshot"},
				40449: {"pricing_set_price_snapshot"},
			},
		},
		{
			name: "no tx targets",
			mutate: func(cfg *config.Config) {
				cfg.Services.Executor.Enabled = new(false)
				cfg.Services.DVN.Enabled = new(false)
				cfg.Signers = nil
			},
			wantPurposes: map[uint32][]string{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig(account.Address.Hex(), filepath.Clean(account.URL.Path))
			if test.mutate != nil {
				test.mutate(&cfg)
			}
			registry, err := chain.NewRegistry(cfg.Chains, cfg.Pathways)
			if err != nil {
				t.Fatalf("NewRegistry() error = %v", err)
			}
			worker, err := New(cfg, discardLogger())
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			targets, err := worker.txTargets(t.Context(), registry)
			if err != nil {
				t.Fatalf("txTargets() error = %v", err)
			}
			got := purposesByChain(targets)
			if !equalPurposeSets(got, test.wantPurposes) {
				t.Fatalf("purposes = %#v, want %#v", got, test.wantPurposes)
			}
		})
	}
}

func TestTxTargetsUsesPerChainPricingPolicies(t *testing.T) {
	stubReadTxHeader(t, &gethtypes.Header{})

	dir := t.TempDir()
	const password = "test-password"
	account, err := gethkeystore.StoreKey(dir, password, gethkeystore.StandardScryptN, gethkeystore.StandardScryptP)
	if err != nil {
		t.Fatalf("StoreKey() error = %v", err)
	}
	t.Setenv("KEYSTORE_PASSWORD", password)

	cfg := testConfig(account.Address.Hex(), filepath.Clean(account.URL.Path))
	cfg.Pricing = testPricingConfig()
	cfg.Pricing.Signer = config.MustEVMAddress(account.Address.Hex())
	cfg.Pricing.Chains[0].TxPolicy = config.PricingTxPolicyConfig{
		MaxFeePerGasWei:         "3000000000",
		MaxPriorityFeePerGasWei: "1500000000",
		MinNativeBalanceWei:     "200000000000000000",
	}
	cfg.Pricing.Chains[1].TxPolicy = config.PricingTxPolicyConfig{
		MaxFeePerGasWei:         "4000000000",
		MaxPriorityFeePerGasWei: "2000000000",
		MinNativeBalanceWei:     "300000000000000000",
	}
	for i := range cfg.Chains {
		cfg.Chains[i].TxRoles.Executor.MinNativeBalanceWei = "100000000000000000"
	}
	registry, err := chain.NewRegistry(cfg.Chains, cfg.Pathways)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	worker, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	targets, err := worker.txTargets(t.Context(), registry)
	if err != nil {
		t.Fatalf("txTargets() error = %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets = %d, want 2", len(targets))
	}
	want := map[uint32]struct {
		maxFee     int64
		priority   int64
		minBalance *big.Int
	}{
		40161: {maxFee: 3_000_000_000, priority: 1_500_000_000, minBalance: big.NewInt(200_000_000_000_000_000)},
		40449: {maxFee: 4_000_000_000, priority: 2_000_000_000, minBalance: big.NewInt(300_000_000_000_000_000)},
	}
	for _, target := range targets {
		expected, ok := want[target.ChainEID]
		if !ok {
			t.Fatalf("unexpected target chain %d", target.ChainEID)
		}
		policy, ok := target.FeePolicies["pricing_set_price_snapshot"]
		if !ok {
			t.Fatalf("target %d missing pricing fee policy", target.ChainEID)
		}
		if policy.ConfiguredMaxFeePerGas.Cmp(big.NewInt(expected.maxFee)) != 0 ||
			policy.ConfiguredMaxPriorityFeePerGas.Cmp(big.NewInt(expected.priority)) != 0 {
			t.Fatalf("target %d pricing fee policy = %+v, want max=%d priority=%d", target.ChainEID, policy, expected.maxFee, expected.priority)
		}
		if target.MinNativeBalanceWei == nil || target.MinNativeBalanceWei.Cmp(expected.minBalance) != 0 {
			t.Fatalf("target %d min native balance = %v, want %s", target.ChainEID, target.MinNativeBalanceWei, expected.minBalance)
		}
	}
}

func TestRunDefersTransientMarketSourceFailureDuringStartup(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
	cfg.DatabaseURL = "postgres://%"
	cfg.Pricing = testPricingConfig()
	cfg.Pricing.CoinGeckoBaseURL = server.URL
	worker, err := NewWithOptions(cfg, discardLogger(), Options{SkipOnchainCheck: true})
	if err != nil {
		t.Fatalf("NewWithOptions() error = %v", err)
	}
	worker.pricingHTTPClient = server.Client()
	err = worker.Run(t.Context())
	if err == nil {
		t.Fatal("Run() error = nil, want invalid database URL error")
	}
	if strings.Contains(err.Error(), "source configuration") {
		t.Fatalf("Run() error = %q, want transient source failure deferred", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("market source configuration requests during startup = %d, want 2", got)
	}
}

func TestRunRetriesIncompleteMarketSourcePayloadDuringStartup(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_, _ = w.Write([]byte(`{"ethereum":{"usd":2000,"last_updated_at":1700000000}}`))
	}))
	defer server.Close()

	cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
	cfg.DatabaseURL = "postgres://%"
	cfg.Pricing = testPricingConfig()
	cfg.Pricing.CoinGeckoBaseURL = server.URL
	worker, err := NewWithOptions(cfg, discardLogger(), Options{SkipOnchainCheck: true})
	if err != nil {
		t.Fatalf("NewWithOptions() error = %v", err)
	}
	worker.pricingHTTPClient = server.Client()
	err = worker.Run(t.Context())
	if err == nil {
		t.Fatal("Run() error = nil, want invalid database URL error")
	}
	if strings.Contains(err.Error(), "source configuration") || strings.Contains(err.Error(), "returned no price") {
		t.Fatalf("Run() error = %q, want incomplete source response recovered", err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("market source configuration requests during startup = %d, want 3", got)
	}
}

func TestRunRejectsMissingMarketIDDuringStartup(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
	cfg.DatabaseURL = "postgres://%"
	cfg.Pricing = testPricingConfig()
	cfg.Pricing.CoinGeckoBaseURL = server.URL
	worker, err := NewWithOptions(cfg, discardLogger(), Options{SkipOnchainCheck: true})
	if err != nil {
		t.Fatalf("NewWithOptions() error = %v", err)
	}
	worker.pricingHTTPClient = server.Client()
	err = worker.Run(t.Context())
	if err == nil {
		t.Fatal("Run() error = nil, want missing market ID configuration error")
	}
	if !strings.Contains(err.Error(), "coingecko returned no price for ethereum") {
		t.Fatalf("Run() error = %q, want missing CoinGecko ID error", err)
	}
	if got := requests.Load(); got != 6 {
		t.Fatalf("market source configuration requests during startup = %d, want 6", got)
	}
}

func TestRunRejectsInvalidMarketSourceConfigBeforeDatabase(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		apiKeyEnv string
		want      string
	}{
		{
			name:    "unsupported scheme",
			baseURL: "ftp://pricing-user:pricing-password@pricing-secret.example/private-api-key",
			want:    "pricing coingecko_base_url must be an absolute HTTPS URL",
		},
		{
			name:      "authenticated HTTP",
			baseURL:   "http://pricing-user:pricing-password@pricing-secret.example/private-api-key",
			apiKeyEnv: "COINGECKO_API_KEY",
			want:      "pricing coingecko_base_url must be an absolute HTTPS URL",
		},
		{
			name:    "keyless HTTP",
			baseURL: "http://pricing-user:pricing-password@pricing-secret.example/private-api-key",
			want:    "pricing coingecko_base_url must be an absolute HTTPS URL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.apiKeyEnv != "" {
				t.Setenv(test.apiKeyEnv, "test-key")
			}
			cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
			cfg.DatabaseURL = "postgres://%"
			cfg.Pricing = testPricingConfig()
			cfg.Pricing.CoinGeckoBaseURL = test.baseURL
			cfg.Pricing.CoinGeckoAPIKeyEnv = test.apiKeyEnv
			worker, err := NewWithOptions(cfg, discardLogger(), Options{SkipOnchainCheck: true})
			if err != nil {
				t.Fatalf("NewWithOptions() error = %v", err)
			}
			err = worker.Run(t.Context())
			if err == nil {
				t.Fatal("Run() error = nil, want invalid CoinGecko base URL error")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Run() error = %q, want %q", err, test.want)
			}
			for _, secret := range []string{test.baseURL, "pricing-user", "pricing-password", "pricing-secret.example", "private-api-key"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("Run() error leaked %q: %s", secret, err)
				}
			}
		})
	}
}

func TestNewRejectsNilLogger(t *testing.T) {
	_, err := New(testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json"), nil)
	if err == nil {
		t.Fatal("New() error = nil, want logger error")
	}
	if !strings.Contains(err.Error(), "logger is required") {
		t.Fatalf("New() error = %v, want logger error", err)
	}
}

func TestNewWithOptionsDefaultsIndexerProgressLogInterval(t *testing.T) {
	worker, err := NewWithOptions(testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json"), discardLogger(), Options{})
	if err != nil {
		t.Fatalf("NewWithOptions() error = %v", err)
	}
	if worker.options.IndexerProgressLogInterval != DefaultIndexerProgressLogInterval {
		t.Fatalf("indexer progress log interval = %s, want %s", worker.options.IndexerProgressLogInterval, DefaultIndexerProgressLogInterval)
	}
	if !worker.options.IndexerProgressLogIntervalSet {
		t.Fatal("IndexerProgressLogIntervalSet = false, want true")
	}
	if worker.options.SkipOnchainCheck {
		t.Fatal("SkipOnchainCheck = true, want false")
	}
}

func TestNewWithOptionsAcceptsIndexerProgressLogInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{name: "custom interval", interval: 10 * time.Second},
		{name: "disabled", interval: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker, err := NewWithOptions(testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json"), discardLogger(), Options{
				IndexerProgressLogInterval:    test.interval,
				IndexerProgressLogIntervalSet: true,
			})
			if err != nil {
				t.Fatalf("NewWithOptions() error = %v", err)
			}
			if worker.options.IndexerProgressLogInterval != test.interval {
				t.Fatalf("indexer progress log interval = %s, want %s", worker.options.IndexerProgressLogInterval, test.interval)
			}
		})
	}
}

func TestNewWithOptionsRejectsNegativeIndexerProgressLogInterval(t *testing.T) {
	_, err := NewWithOptions(testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json"), discardLogger(), Options{
		IndexerProgressLogInterval:    -time.Second,
		IndexerProgressLogIntervalSet: true,
	})
	if err == nil {
		t.Fatal("NewWithOptions() error = nil, want interval error")
	}
	if !strings.Contains(err.Error(), "indexer progress log interval") {
		t.Fatalf("NewWithOptions() error = %v, want interval error", err)
	}
}

func TestRunChecksOnChainConfigBeforeDatabaseSync(t *testing.T) {
	originalCheck := checkOnChainConfig
	defer func() { checkOnChainConfig = originalCheck }()
	calls := 0
	checkOnChainConfig = func(_ context.Context, _ *chain.Registry, _ ...configcheck.Option) (configcheck.Report, error) {
		calls++
		return configcheck.Report{
			Issues: []configcheck.Issue{{Path: "chains[40161].chain_id", Message: "wrong"}},
		}, nil
	}

	cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
	cfg.DatabaseURL = "postgres://invalid:invalid@127.0.0.1:1/db?sslmode=disable"
	worker, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	err = worker.Run(t.Context())
	if err == nil {
		t.Fatal("Run() error = nil, want on-chain config error")
	}
	if calls != 1 {
		t.Fatalf("checkOnChainConfig calls = %d, want 1", calls)
	}
	if !strings.Contains(err.Error(), "on-chain config check failed") {
		t.Fatalf("Run() error = %v, want on-chain config error", err)
	}
}

func TestRunSkipOnchainCheckBypassesOnChainConfigCheck(t *testing.T) {
	originalCheck := checkOnChainConfig
	defer func() { checkOnChainConfig = originalCheck }()
	calls := 0
	checkOnChainConfig = func(_ context.Context, _ *chain.Registry, _ ...configcheck.Option) (configcheck.Report, error) {
		calls++
		return configcheck.Report{
			Issues: []configcheck.Issue{{Path: "chains[40161].chain_id", Message: "wrong"}},
		}, nil
	}

	cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
	cfg.DatabaseURL = "postgres://invalid:invalid@127.0.0.1:1/db?sslmode=disable"
	worker, err := NewWithOptions(cfg, discardLogger(), Options{SkipOnchainCheck: true})
	if err != nil {
		t.Fatalf("NewWithOptions() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err = worker.Run(ctx)
	if err == nil {
		t.Fatal("Run() error = nil, want database error after skipped check")
	}
	if calls != 0 {
		t.Fatalf("checkOnChainConfig calls = %d, want 0", calls)
	}
	if strings.Contains(err.Error(), "on-chain config check failed") {
		t.Fatalf("Run() error = %v, want database/setup error after skipped check", err)
	}
}

func TestRunSkipOnchainCheckStillRejectsInvalidLocalConfig(t *testing.T) {
	originalCheck := checkOnChainConfig
	defer func() { checkOnChainConfig = originalCheck }()
	calls := 0
	checkOnChainConfig = func(_ context.Context, _ *chain.Registry, _ ...configcheck.Option) (configcheck.Report, error) {
		calls++
		return configcheck.Report{OK: true}, nil
	}

	cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
	cfg.Pathways[0].DstEID = 49999
	worker, err := NewWithOptions(cfg, discardLogger(), Options{SkipOnchainCheck: true})
	if err != nil {
		t.Fatalf("NewWithOptions() error = %v", err)
	}
	err = worker.Run(t.Context())
	if err == nil {
		t.Fatal("Run() error = nil, want local config validation error")
	}
	if calls != 0 {
		t.Fatalf("checkOnChainConfig calls = %d, want 0", calls)
	}
	if !strings.Contains(err.Error(), "pathway destination eid 49999 is not configured") {
		t.Fatalf("Run() error = %v, want local config validation error", err)
	}
}

func TestTxManagerOptionsUsesConfiguredStaleBroadcastReplacementAfter(t *testing.T) {
	cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
	cfg.TxManager.StaleBroadcastReplacementAfterSeconds = 7
	worker, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	options := worker.txManagerOptions()
	if options.StaleBroadcastReplacementAfter != 7*time.Second {
		t.Fatalf("stale broadcast replacement after = %s, want 7s", options.StaleBroadcastReplacementAfter)
	}
}

func TestRunPriceOnceRejectsDisabledPricing(t *testing.T) {
	worker, err := New(testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json"), discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	err = worker.RunPriceOnce(t.Context())
	if err == nil {
		t.Fatal("RunPriceOnce() error = nil, want disabled pricing error")
	}
	if !strings.Contains(err.Error(), "pricing is disabled") {
		t.Fatalf("RunPriceOnce() error = %v, want disabled pricing error", err)
	}
}

func TestRunPriceOnceChecksOnChainConfigBeforeDatabaseSync(t *testing.T) {
	originalCheck := checkOnChainConfig
	defer func() { checkOnChainConfig = originalCheck }()
	checkOnChainConfig = func(_ context.Context, _ *chain.Registry, _ ...configcheck.Option) (configcheck.Report, error) {
		return configcheck.Report{
			Issues: []configcheck.Issue{{Path: "chains[40161].chain_id", Message: "wrong"}},
		}, nil
	}

	cfg := testConfig("0x9999999999999999999999999999999999999999", "/unused/keystore.json")
	cfg.DatabaseURL = "postgres://invalid:invalid@127.0.0.1:1/db?sslmode=disable"
	cfg.Pricing = testPricingConfig()
	worker, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	err = worker.RunPriceOnce(t.Context())
	if err == nil {
		t.Fatal("RunPriceOnce() error = nil, want on-chain config error")
	}
	if !strings.Contains(err.Error(), "on-chain config check failed") {
		t.Fatalf("RunPriceOnce() error = %v, want on-chain config error", err)
	}
}

func TestLoadKMSAWSConfigUsesDefaultCredentialChain(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_SESSION_TOKEN", "test-session-token")

	awsConfig, err := loadKMSAWSConfig(t.Context(), config.KMSSignerConfig{
		Region: "us-west-2",
	})
	if err != nil {
		t.Fatalf("loadKMSAWSConfig() error = %v", err)
	}
	if awsConfig.Region != "us-west-2" {
		t.Fatalf("Region = %q, want us-west-2", awsConfig.Region)
	}
	credentials, err := awsConfig.Credentials.Retrieve(t.Context())
	if err != nil {
		t.Fatalf("Credentials.Retrieve() error = %v", err)
	}
	if credentials.AccessKeyID != "test-access-key" {
		t.Fatal("AccessKeyID did not match default credential chain env value")
	}
	if credentials.SecretAccessKey != "test-secret-key" {
		t.Fatal("SecretAccessKey did not match default credential chain env value")
	}
	if credentials.SessionToken != "test-session-token" {
		t.Fatal("SessionToken did not match default credential chain env value")
	}
}

func TestSuperviseLoopRestartsReturnedErrorsUntilContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := make(chan int, 2)
	done := make(chan struct{})
	errCh := make(chan error, 1)
	retries := &recordingLoopRetries{}
	go func() {
		defer close(done)
		attempts := 0
		errCh <- superviseLoop(ctx, "test", 0, discardLogger(), retries, func(context.Context) error {
			attempts++
			calls <- attempts
			if attempts == 2 {
				cancel()
				return context.Canceled
			}
			return errors.New("temporary loop error")
		})
	}()

	for want := 1; want <= 2; want++ {
		select {
		case got := <-calls:
			if got != want {
				t.Fatalf("attempt = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("attempt %d did not run", want)
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("superviseLoop did not stop after context cancellation")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("superviseLoop() error = %v, want nil", err)
	}
	if len(retries.names) != 1 || retries.names[0] != "test" {
		t.Fatalf("retry metrics = %#v, want one test retry", retries.names)
	}
}

func TestSuperviseLoopReturnsFatalErrorWithoutRetry(t *testing.T) {
	wantErr := errors.New("bad loop configuration")
	retries := &recordingLoopRetries{}
	calls := 0

	err := superviseLoop(context.Background(), "test", 0, discardLogger(), retries, func(context.Context) error {
		calls++
		return workerloop.Fatal(wantErr)
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("superviseLoop() error = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if len(retries.names) != 0 {
		t.Fatalf("retry metrics = %#v, want none", retries.names)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type recordingLoopRetries struct {
	names []string
}

func (r *recordingLoopRetries) RecordLoopRetry(name string) {
	r.names = append(r.names, name)
}

func testConfig(signerID, keystorePath string) config.Config {
	signerAddress := config.MustEVMAddress(signerID)
	return config.Config{
		DatabaseURL: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		TxManager: config.TxManagerConfig{
			StaleBroadcastReplacementAfterSeconds: 900,
		},
		Signers: []config.SignerConfig{
			{
				ID:   signerAddress,
				Type: "keystore",
				Keystore: config.KeystoreSignerConfig{
					Path:        keystorePath,
					PasswordEnv: "KEYSTORE_PASSWORD",
				},
			},
		},
		Chains: []config.ChainConfig{
			{
				EID:                        40161,
				Name:                       "ethereum-sepolia",
				Family:                     config.ChainFamilyEVM,
				ChainID:                    11155111,
				EndpointAddress:            config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
				Confirmations:              12,
				RPCURLs:                    []string{"http://localhost:8545"},
				IndexerQueryBlockRange:     500,
				IndexerPollIntervalSeconds: 5,
				TxRoles: config.ChainTxRolesConfig{
					Executor: testExecutorRole(signerAddress),
				},
			},
			{
				EID:                        40449,
				Name:                       "hoodi",
				Family:                     config.ChainFamilyEVM,
				ChainID:                    560048,
				EndpointAddress:            config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
				Confirmations:              12,
				RPCURLs:                    []string{"http://localhost:8546"},
				IndexerQueryBlockRange:     500,
				IndexerPollIntervalSeconds: 5,
				TxRoles: config.ChainTxRolesConfig{
					Executor: testExecutorRole(signerAddress),
				},
			},
		},
		Pathways: []config.PathwayConfig{
			{
				SrcEID:     40161,
				DstEID:     40449,
				SrcOApp:    config.MustEVMAddress("0x7777777777777777777777777777777777777777"),
				DstOApp:    config.MustEVMAddress("0x8888888888888888888888888888888888888888"),
				SendLib:    config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
				ReceiveLib: config.MustEVMAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				SourceWorkers: config.WorkerContractsConfig{
					OpenExecutor: config.MustEVMAddress("0x2222222222222222222222222222222222222222"),
					OpenDVN:      config.MustEVMAddress("0x3333333333333333333333333333333333333333"),
					PriceFeed:    config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
				},
				DestinationWorkers: config.DestinationWorkerContractsConfig{
					OpenDVN: config.MustEVMAddress("0x6666666666666666666666666666666666666666"),
				},
				DVN:             config.PathwayDVNConfig{Mode: config.DVNModeShadow},
				Pricing:         testPathwayPricingConfig(),
				Enabled:         true,
				MaxMessageSize:  10000,
				MinLzReceiveGas: 100000,
				MaxLzReceiveGas: 300000,
			},
		},
	}
}

func stubReadTxHeader(t *testing.T, header *gethtypes.Header) {
	t.Helper()
	original := readTxHeader
	t.Cleanup(func() { readTxHeader = original })
	readTxHeader = func(context.Context, txmgr.ChainClient) (*gethtypes.Header, error) {
		return header, nil
	}
}

func bigOne() *big.Int {
	return big.NewInt(1)
}

func testExecutorRole(signer config.EVMAddress) config.ExecutorTxRoleConfig {
	return config.ExecutorTxRoleConfig{
		Signer:                  signer,
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func testDVNRole(signer config.EVMAddress) config.DVNTxRoleConfig {
	return config.DVNTxRoleConfig{
		Signer:                  signer,
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func testPricingConfig() config.PricingConfig {
	return config.PricingConfig{
		Enabled:                     true,
		Signer:                      config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
		IntervalSeconds:             300,
		StaleAfterSeconds:           1800,
		MaxDeviationBps:             500,
		SourceRequestTimeoutSeconds: 10,
		GasSpikeBps:                 1000,
		Chains: []config.PricingChainConfig{
			{
				EID:               40161,
				TxPolicy:          testPricingTxPolicy(),
				NativeAssetID:     "eth",
				DataFeePerByteWei: "0",
				PrimarySource:     "coingecko",
				CoinGecko:         config.CoinGeckoPricingConfig{ID: "ethereum", MaxAgeSeconds: 180},
			},
			{
				EID:               40449,
				TxPolicy:          testPricingTxPolicy(),
				NativeAssetID:     "hoodi-eth",
				DataFeePerByteWei: "0",
				PrimarySource:     "coingecko",
				CoinGecko:         config.CoinGeckoPricingConfig{ID: "ethereum", MaxAgeSeconds: 180},
			},
		},
	}
}

func testPricingTxPolicy() config.PricingTxPolicyConfig {
	return config.PricingTxPolicyConfig{
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func testPathwayPricingConfig() config.PathwayPricingConfig {
	return config.PathwayPricingConfig{
		ExecutorFee: config.WorkerFeeModelConfig{FixedFeeWei: "1000", DstGasOverhead: 50000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 100},
		DVNFee:      config.WorkerFeeModelConfig{FixedFeeWei: "2000", DstGasOverhead: 150000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 200},
	}
}

func purposesByChain(targets []txmgr.Target) map[uint32][]string {
	out := make(map[uint32][]string, len(targets))
	for _, target := range targets {
		for purpose := range target.FeePolicies {
			out[target.ChainEID] = append(out[target.ChainEID], purpose)
		}
	}
	for eid := range out {
		sortStrings(out[eid])
	}
	return out
}

func equalPurposeSets(a, b map[uint32][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for eid, aPurposes := range a {
		bPurposes, ok := b[eid]
		if !ok {
			return false
		}
		sortStrings(bPurposes)
		if len(aPurposes) != len(bPurposes) {
			return false
		}
		for idx := range aPurposes {
			if aPurposes[idx] != bPurposes[idx] {
				return false
			}
		}
	}
	return true
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
