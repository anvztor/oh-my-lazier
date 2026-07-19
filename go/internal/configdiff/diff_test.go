package configdiff

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/islishude/oh-my-lazier/go/internal/config"
)

func TestDiffUsesSemanticKeysForLists(t *testing.T) {
	before := validConfig()
	after := validConfig()
	after.Services.Executor.Enabled = new(false)
	after.Pathways[1].SourceWorkers.OpenExecutor = config.MustEVMAddress("0x7777777777777777777777777777777777777777")
	after.Pathways[0].MaxMessageSize = 20000
	after.Pricing = validPricingConfig()
	after.Pricing.Chains[1].CoinGecko.MaxAgeSeconds = 300

	changes := Diff(before, after)

	paths := make([]string, 0, len(changes))
	for _, change := range changes {
		paths = append(paths, change.Path)
	}
	want := []string{
		"services",
		"pricing.chains[40449]",
		"pathways[40161:40449:0x7777777777777777777777777777777777777777:0x8888888888888888888888888888888888888888]",
		"pathways[40449:40161:0x8888888888888888888888888888888888888888:0x7777777777777777777777777777777777777777]",
	}
	if strings.Join(paths, "\n") != strings.Join(want, "\n") {
		t.Fatalf("paths = %#v, want %#v", paths, want)
	}
}

func TestRenderTextReportsNoConfigChanges(t *testing.T) {
	output := RenderText(nil)
	if output != "no config changes\n" {
		t.Fatalf("RenderText(nil) = %q", output)
	}
}

func TestDiffReportsPerChainPricingTxPolicy(t *testing.T) {
	before := validConfig()
	after := validConfig()
	after.Pricing.Chains[1].TxPolicy.MaxFeePerGasWei = "3000000000"

	changes := Diff(before, after)
	if len(changes) != 1 || changes[0].Path != "pricing.chains[40449]" {
		t.Fatalf("changes = %#v, want pricing chain transaction policy change", changes)
	}
}

func TestDiffReportsPerChainIndexerPollInterval(t *testing.T) {
	before := validConfig()
	after := validConfig()
	after.Chains[0].IndexerPollIntervalSeconds = 9

	changes := Diff(before, after)
	if len(changes) != 1 || changes[0].Path != "chains[40161]" {
		t.Fatalf("changes = %#v, want ethereum chain indexer poll interval change", changes)
	}
	output := RenderText(changes)
	if !strings.Contains(output, `"IndexerPollIntervalSeconds":5`) ||
		!strings.Contains(output, `"IndexerPollIntervalSeconds":9`) {
		t.Fatalf("output missing indexer poll interval change:\n%s", output)
	}
}

func TestRenderTextIncludesChangedPath(t *testing.T) {
	before := validConfig()
	after := validConfig()
	after.Pathways[0].DVN.Mode = config.DVNModeActive

	output := RenderText(Diff(before, after))

	if !strings.Contains(output, "pathways[40161:40449:0x7777777777777777777777777777777777777777:0x8888888888888888888888888888888888888888]\n") {
		t.Fatalf("output missing pathway path:\n%s", output)
	}
	if !strings.Contains(output, `"Mode":"shadow"`) || !strings.Contains(output, `"Mode":"active"`) {
		t.Fatalf("output missing before/after values:\n%s", output)
	}
}

func TestDiffIncludesTxManagerAndSignerBackendChanges(t *testing.T) {
	before := validConfig()
	after := validConfig()
	signerID := config.MustEVMAddress("0x9999999999999999999999999999999999999999")
	before.TxManager.StaleBroadcastReplacementAfterSeconds = 900
	after.TxManager.StaleBroadcastReplacementAfterSeconds = 60
	before.Signers = []config.SignerConfig{
		{
			ID:   signerID,
			Type: "keystore",
			Keystore: config.KeystoreSignerConfig{
				Path:        "/run/secrets/worker.json",
				PasswordEnv: "KEYSTORE_PASSWORD",
			},
		},
	}
	after.Signers = []config.SignerConfig{
		{
			ID:   signerID,
			Type: "kms",
			KMS: config.KMSSignerConfig{
				KeyID:   "approved-key",
				Region:  "us-east-1",
				Address: signerID,
			},
		},
	}

	changes := Diff(before, after)
	paths := make([]string, 0, len(changes))
	for _, change := range changes {
		paths = append(paths, change.Path)
	}
	want := []string{
		"tx_manager",
		"signers[0x9999999999999999999999999999999999999999]",
	}
	if strings.Join(paths, "\n") != strings.Join(want, "\n") {
		t.Fatalf("paths = %#v, want %#v", paths, want)
	}
}

func TestDiffIgnoresSignerReordering(t *testing.T) {
	before := validConfig()
	after := validConfig()
	first := config.SignerConfig{
		ID:   config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
		Type: "keystore",
		Keystore: config.KeystoreSignerConfig{
			Path:        "/run/secrets/first.json",
			PasswordEnv: "FIRST_PASSWORD",
		},
	}
	second := config.SignerConfig{
		ID:   config.MustEVMAddress("0x2222222222222222222222222222222222222222"),
		Type: "keystore",
		Keystore: config.KeystoreSignerConfig{
			Path:        "/run/secrets/second.json",
			PasswordEnv: "SECOND_PASSWORD",
		},
	}
	before.Signers = []config.SignerConfig{first, second}
	after.Signers = []config.SignerConfig{second, first}

	if changes := Diff(before, after); len(changes) != 0 {
		t.Fatalf("Diff() changes = %+v, want signer reordering ignored", changes)
	}
}

func TestDiffIncludesSignerAdditionsAndRemovals(t *testing.T) {
	signer := config.SignerConfig{
		ID:   config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
		Type: "keystore",
		Keystore: config.KeystoreSignerConfig{
			Path:        "/run/secrets/worker.json",
			PasswordEnv: "KEYSTORE_PASSWORD",
		},
	}
	tests := []struct {
		name       string
		before     []config.SignerConfig
		after      []config.SignerConfig
		wantBefore bool
		wantAfter  bool
	}{
		{name: "addition", after: []config.SignerConfig{signer}, wantAfter: true},
		{name: "removal", before: []config.SignerConfig{signer}, wantBefore: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := validConfig()
			after := validConfig()
			before.Signers = test.before
			after.Signers = test.after

			changes := Diff(before, after)
			if len(changes) != 1 {
				t.Fatalf("Diff() changes = %+v, want one signer change", changes)
			}
			change := changes[0]
			if change.Path != "signers[0x9999999999999999999999999999999999999999]" {
				t.Fatalf("change path = %q, want signer semantic path", change.Path)
			}
			if (change.Before != nil) != test.wantBefore || (change.After != nil) != test.wantAfter {
				t.Fatalf("change = %+v, want before=%t after=%t", change, test.wantBefore, test.wantAfter)
			}
		})
	}
}

func TestDiffRedactsDatabaseAndRPCURLCredentials(t *testing.T) {
	before := validConfig()
	after := validConfig()
	before.DatabaseURL = "postgres://worker:before-password@db.internal:5432/worker?sslmode=disable&password=before-query-password&api_key=before-db-key"
	after.DatabaseURL = "postgres://worker:after-password@db.internal:5432/worker?sslmode=require&password=after-query-password&api_key=after-db-key"
	before.Chains[0].RPCURLs = []string{"https://before-user:before-password@before-secret.example/v2/before-path-key?api_key=before-query-key"}
	after.Chains[0].RPCURLs = []string{"https://after-user:after-password@after-secret.example/v2/after-path-key?api_key=after-query-key"}

	changes := Diff(before, after)
	encoded, err := json.Marshal(changes)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	output := string(encoded) + RenderText(changes)
	for _, secret := range []string{
		"before-password",
		"after-password",
		"before-user",
		"after-user",
		"before-secret",
		"after-secret",
		"before-path-key",
		"after-path-key",
		"before-query-key",
		"after-query-key",
		"before-query-password",
		"after-query-password",
		"before-db-key",
		"after-db-key",
	} {
		if strings.Contains(output, secret) {
			t.Fatalf("config diff leaked %q:\n%s", secret, output)
		}
	}
	if !strings.Contains(output, `"path":"database_url"`) || !strings.Contains(output, `"path":"chains[40161]"`) {
		t.Fatalf("config diff omitted redacted changes:\n%s", output)
	}
	if !strings.Contains(output, "[REDACTED]") {
		t.Fatalf("config diff missing redaction marker:\n%s", output)
	}
	if !strings.Contains(output, "sslmode=disable") || !strings.Contains(output, "sslmode=require") {
		t.Fatalf("config diff omitted security-relevant database parameters:\n%s", output)
	}
}

func TestDiffFullyRedactsOpaqueDatabaseURLs(t *testing.T) {
	before := validConfig()
	after := validConfig()
	before.DatabaseURL = "postgres:before-secret-token"
	after.DatabaseURL = "postgres:after-secret-token"

	changes := Diff(before, after)
	encoded, err := json.Marshal(changes)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	output := string(encoded) + RenderText(changes)
	for _, secret := range []string{"before-secret-token", "after-secret-token"} {
		if strings.Contains(output, secret) {
			t.Fatalf("config diff leaked opaque database value %q:\n%s", secret, output)
		}
	}
	if !strings.Contains(output, `"path":"database_url"`) || strings.Count(output, "[REDACTED]") < 2 {
		t.Fatalf("config diff omitted opaque database redaction:\n%s", output)
	}
}

func TestDiffRedactsPricingAndSignerEndpointURLsWithoutHidingChanges(t *testing.T) {
	before := validConfig()
	after := validConfig()
	before.Pricing.CoinMarketCapBaseURL = "before-token://before-coinmarketcap.example/before-secret"
	after.Pricing.CoinMarketCapBaseURL = "after-token:after-opaque-secret"
	before.Pricing.CoinGeckoBaseURL = "https://before-coingecko.example/before-token"
	after.Pricing.CoinGeckoBaseURL = "https://after-coingecko.example/after-token"
	signerID := config.MustEVMAddress("0x9999999999999999999999999999999999999999")
	before.Signers = []config.SignerConfig{
		{
			ID:   signerID,
			Type: "kms",
			KMS: config.KMSSignerConfig{
				KeyID:    "approved-key",
				Region:   "us-east-1",
				Address:  signerID,
				Endpoint: "https://before-kms-user:before-kms-password@before-kms.example/before-kms-path?token=before-kms-query",
			},
		},
	}
	after.Signers = []config.SignerConfig{
		{
			ID:   signerID,
			Type: "kms",
			KMS: config.KMSSignerConfig{
				KeyID:    "approved-key",
				Region:   "us-east-1",
				Address:  signerID,
				Endpoint: "https://after-kms-user:after-kms-password@after-kms.example/after-kms-path?token=after-kms-query",
			},
		},
	}

	changes := Diff(before, after)
	encoded, err := json.Marshal(changes)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	output := string(encoded) + RenderText(changes)
	for _, secret := range []string{
		"before-token", "before-coinmarketcap", "before-secret", "after-token", "after-opaque-secret",
		"before-coingecko", "before-token", "after-coingecko", "after-token",
		"before-kms-user", "before-kms-password", "before-kms", "before-kms-path", "before-kms-query",
		"after-kms-user", "after-kms-password", "after-kms", "after-kms-path", "after-kms-query",
	} {
		if strings.Contains(output, secret) {
			t.Fatalf("config diff leaked %q:\n%s", secret, output)
		}
	}
	for _, path := range []string{
		`"path":"pricing"`,
		`"path":"signers[0x9999999999999999999999999999999999999999]"`,
	} {
		if !strings.Contains(output, path) {
			t.Fatalf("config diff omitted redacted change %s:\n%s", path, output)
		}
	}
	if !strings.Contains(output, "https://[REDACTED]") || !strings.Contains(output, "[REDACTED]") {
		t.Fatalf("config diff missing endpoint redaction markers:\n%s", output)
	}
}

func TestDiffRedactsInvalidSecretEnvironmentReferences(t *testing.T) {
	before := validConfig()
	after := validConfig()
	before.Pricing.CoinMarketCapAPIKeyEnv = "before-cmc-secret=abc123"
	after.Pricing.CoinMarketCapAPIKeyEnv = "after-cmc-secret=def456"
	before.Pricing.CoinGeckoAPIKeyEnv = "before-cg-secret=ghi789"
	after.Pricing.CoinGeckoAPIKeyEnv = "after-cg-secret=jkl012"
	signerID := config.MustEVMAddress("0x9999999999999999999999999999999999999999")
	before.Signers = []config.SignerConfig{{
		ID: signerID, Type: "keystore", Keystore: config.KeystoreSignerConfig{
			Path: "/run/secrets/worker.json", PasswordEnv: "before-password-secret=mno345",
		},
	}}
	after.Signers = []config.SignerConfig{{
		ID: signerID, Type: "keystore", Keystore: config.KeystoreSignerConfig{
			Path: "/run/secrets/worker.json", PasswordEnv: "after-password-secret=pqr678",
		},
	}}

	changes := Diff(before, after)
	encoded, err := json.Marshal(changes)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	output := string(encoded) + RenderText(changes)
	for _, secret := range []string{
		"before-cmc-secret", "abc123", "after-cmc-secret", "def456",
		"before-cg-secret", "ghi789", "after-cg-secret", "jkl012",
		"before-password-secret", "mno345", "after-password-secret", "pqr678",
	} {
		if strings.Contains(output, secret) {
			t.Fatalf("config diff leaked secret environment reference %q:\n%s", secret, output)
		}
	}
	for _, path := range []string{
		`"path":"pricing"`,
		`"path":"signers[0x9999999999999999999999999999999999999999]"`,
	} {
		if !strings.Contains(output, path) {
			t.Fatalf("config diff omitted redacted change %s:\n%s", path, output)
		}
	}
	if strings.Count(output, "[REDACTED]") < 6 {
		t.Fatalf("config diff missing secret environment reference redaction:\n%s", output)
	}
}

func TestDiffRedactsKeystorePasswordFileValues(t *testing.T) {
	before := validConfig()
	after := validConfig()
	signerID := config.MustEVMAddress("0x9999999999999999999999999999999999999999")
	before.Signers = []config.SignerConfig{{
		ID: signerID, Type: "keystore", Keystore: config.KeystoreSignerConfig{
			Path: "/run/secrets/worker.json", PasswordFile: "before-plaintext-password=abc123",
		},
	}}
	after.Signers = []config.SignerConfig{{
		ID: signerID, Type: "keystore", Keystore: config.KeystoreSignerConfig{
			Path: "/run/secrets/worker.json", PasswordFile: "after-plaintext-password=def456",
		},
	}}

	changes := Diff(before, after)
	encoded, err := json.Marshal(changes)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	output := string(encoded) + RenderText(changes)
	for _, secret := range []string{
		"before-plaintext-password", "abc123", "after-plaintext-password", "def456",
	} {
		if strings.Contains(output, secret) {
			t.Fatalf("config diff leaked password_file value %q:\n%s", secret, output)
		}
	}
	if !strings.Contains(output, `"path":"signers[0x9999999999999999999999999999999999999999]"`) || strings.Count(output, "[REDACTED]") < 4 {
		t.Fatalf("config diff missing password_file redaction markers:\n%s", output)
	}
}

func TestEndpointURLRedactionUsesContextSpecificSchemeAllowLists(t *testing.T) {
	tests := []struct {
		name   string
		redact func(string) string
		raw    string
		want   string
	}{
		{name: "http endpoint", redact: redactHTTPURL, raw: "https://user:password@secret.example/private", want: "https://[REDACTED]"},
		{name: "http rejects websocket", redact: redactHTTPURL, raw: "wss://secret.example/private", want: "[REDACTED]"},
		{name: "http rejects unknown scheme", redact: redactHTTPURL, raw: "secret-token://example.com", want: "[REDACTED]"},
		{name: "http rejects opaque URL", redact: redactHTTPURL, raw: "https:secret-token", want: "[REDACTED]"},
		{name: "http rejects missing host", redact: redactHTTPURL, raw: "https:///secret-token", want: "[REDACTED]"},
		{name: "rpc websocket endpoint", redact: redactRPCURL, raw: "wss://user:password@secret.example/private", want: "wss://[REDACTED]"},
		{name: "empty endpoint", redact: redactHTTPURL, raw: "", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.redact(test.raw); got != test.want {
				t.Fatalf("redact(%q) = %q, want %q", test.raw, got, test.want)
			}
		})
	}
}

func validConfig() config.Config {
	return config.Config{
		DatabaseURL: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		Metrics:     config.MetricsConfig{ListenAddress: ":9090"},
		Pricing: config.PricingConfig{
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
		},
		Chains: []config.ChainConfig{
			{
				EID:                        40161,
				Name:                       "ethereum-sepolia",
				Family:                     config.ChainFamilyEVM,
				ChainID:                    11155111,
				EndpointAddress:            config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
				Confirmations:              12,
				IndexerPollIntervalSeconds: 5,
				RPCURLs:                    []string{"http://localhost:8545"},
				TxRoles: config.ChainTxRolesConfig{
					Executor: testExecutorRole(),
				},
			},
			{
				EID:                        40449,
				Name:                       "hoodi",
				Family:                     config.ChainFamilyEVM,
				ChainID:                    560048,
				EndpointAddress:            config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
				Confirmations:              12,
				IndexerPollIntervalSeconds: 5,
				RPCURLs:                    []string{"http://localhost:8546"},
				TxRoles: config.ChainTxRolesConfig{
					Executor: testExecutorRole(),
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
				DVN:            config.PathwayDVNConfig{Mode: config.DVNModeShadow},
				Pricing:        testPathwayPricingConfig(),
				Enabled:        true,
				MaxMessageSize: 10000,
			},
			{
				SrcEID:     40449,
				DstEID:     40161,
				SrcOApp:    config.MustEVMAddress("0x8888888888888888888888888888888888888888"),
				DstOApp:    config.MustEVMAddress("0x7777777777777777777777777777777777777777"),
				SendLib:    config.MustEVMAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ReceiveLib: config.MustEVMAddress("0xcccccccccccccccccccccccccccccccccccccccc"),
				SourceWorkers: config.WorkerContractsConfig{
					OpenExecutor: config.MustEVMAddress("0x5555555555555555555555555555555555555555"),
					OpenDVN:      config.MustEVMAddress("0x6666666666666666666666666666666666666666"),
					PriceFeed:    config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
				},
				DestinationWorkers: config.DestinationWorkerContractsConfig{
					OpenDVN: config.MustEVMAddress("0x3333333333333333333333333333333333333333"),
				},
				DVN:            config.PathwayDVNConfig{Mode: config.DVNModeShadow},
				Pricing:        testPathwayPricingConfig(),
				Enabled:        true,
				MaxMessageSize: 10000,
			},
		},
	}
}

func testExecutorRole() config.ExecutorTxRoleConfig {
	return config.ExecutorTxRoleConfig{
		Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func testPricingTxPolicy() config.PricingTxPolicyConfig {
	return config.PricingTxPolicyConfig{
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func validPricingConfig() config.PricingConfig {
	cfg := validConfig()
	return cfg.Pricing
}

func testPathwayPricingConfig() config.PathwayPricingConfig {
	return config.PathwayPricingConfig{
		ExecutorFee: config.WorkerFeeModelConfig{FixedFeeWei: "1000", DstGasOverhead: 50000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 100},
		DVNFee:      config.WorkerFeeModelConfig{FixedFeeWei: "2000", DstGasOverhead: 150000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 200},
	}
}
