package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestValidateAcceptsSepoliaPathways(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestIsValidEnvironmentVariableNameMatchesDeployProfileSyntax(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{value: "API_KEY_2", want: true},
		{value: "_INTERNAL_KEY", want: true},
		{value: "", want: false},
		{value: "api_key", want: false},
		{value: "2_API_KEY", want: false},
		{value: "API-KEY", want: false},
		{value: "API KEY", want: false},
	}

	for _, test := range tests {
		if got := IsValidEnvironmentVariableName(test.value); got != test.want {
			t.Errorf("IsValidEnvironmentVariableName(%q) = %t, want %t", test.value, got, test.want)
		}
	}
}

func TestValidateRejectsMissingWorkerContractAddress(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"source_dvn": func(cfg *Config) {
			cfg.Pathways[0].SourceWorkers.OpenDVN = EVMAddress{}
		},
		"source_price_feed": func(cfg *Config) {
			cfg.Pathways[0].SourceWorkers.PriceFeed = EVMAddress{}
		},
		"destination": func(cfg *Config) {
			cfg.Pathways[0].DestinationWorkers.OpenDVN = EVMAddress{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want missing worker contract error")
			}
		})
	}
}

func TestValidateRejectsUnsupportedChainFamilies(t *testing.T) {
	for name, family := range map[string]ChainFamily{
		"missing": "",
		"solana":  "solana",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Chains[0].Family = family
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want unsupported chain family error")
			}
		})
	}
}

func TestEVMAddressUnmarshalRejectsInvalidValues(t *testing.T) {
	for name, body := range map[string]string{
		"empty":    `address: ""`,
		"invalid":  `address: not-an-address`,
		"sequence": `address: []`,
	} {
		t.Run(name, func(t *testing.T) {
			var target struct {
				Address EVMAddress `yaml:"address"`
			}
			if err := yaml.Unmarshal([]byte(body), &target); err == nil {
				t.Fatal("Unmarshal() error = nil, want invalid evm address error")
			}
		})
	}
}

func TestValidateRejectsInvalidPathwayGasBounds(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"missingMax": func(cfg *Config) {
			cfg.Pathways[0].MaxLzReceiveGas = 0
		},
		"minExceedsMax": func(cfg *Config) {
			cfg.Pathways[0].MinLzReceiveGas = 300000
			cfg.Pathways[0].MaxLzReceiveGas = 200000
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want invalid pathway gas bounds error")
			}
		})
	}
}

func TestValidateAcceptsConfiguredConfirmations(t *testing.T) {
	cfg := validConfig()
	cfg.Chains[0].Confirmations = 6
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsMissingConfirmations(t *testing.T) {
	cfg := validConfig()
	cfg.Chains[0].Confirmations = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing confirmations error")
	}
}

func TestValidateRejectsDurationOverflow(t *testing.T) {
	overflow := maxDurationSeconds + 1
	tests := []struct {
		name   string
		field  string
		mutate func(*Config)
	}{
		{
			name:  "tx manager stale broadcast replacement",
			field: "stale_broadcast_replacement_after_seconds",
			mutate: func(cfg *Config) {
				cfg.TxManager.StaleBroadcastReplacementAfterSeconds = overflow
			},
		},
		{
			name:  "indexer poll interval",
			field: "indexer_poll_interval_seconds",
			mutate: func(cfg *Config) {
				cfg.Chains[0].IndexerPollIntervalSeconds = overflow
			},
		},
		{
			name:  "pricing interval",
			field: "interval_seconds",
			mutate: func(cfg *Config) {
				cfg.Pricing = validPricingConfig()
				cfg.Pricing.IntervalSeconds = overflow
			},
		},
		{
			name:  "pricing stale after",
			field: "stale_after_seconds",
			mutate: func(cfg *Config) {
				cfg.Pricing = validPricingConfig()
				cfg.Pricing.StaleAfterSeconds = overflow
			},
		},
		{
			name:  "pricing source request timeout",
			field: "source_request_timeout_seconds",
			mutate: func(cfg *Config) {
				cfg.Pricing = validPricingConfig()
				cfg.Pricing.SourceRequestTimeoutSeconds = overflow
			},
		},
		{
			name:  "coinmarketcap max age",
			field: "coinmarketcap.max_age_seconds",
			mutate: func(cfg *Config) {
				cfg.Pricing = validPricingConfig()
				cfg.Pricing.CoinMarketCapAPIKeyEnv = "COINMARKETCAP_API_KEY"
				for idx := range cfg.Pricing.Chains {
					cfg.Pricing.Chains[idx].PrimarySource = "coinmarketcap"
					cfg.Pricing.Chains[idx].CoinMarketCap = CoinMarketCapPricingConfig{ID: 1027, MaxAgeSeconds: 180}
					cfg.Pricing.Chains[idx].CoinGecko = CoinGeckoPricingConfig{}
				}
				cfg.Pricing.Chains[0].CoinMarketCap.MaxAgeSeconds = overflow
			},
		},
		{
			name:  "coingecko max age",
			field: "coingecko.max_age_seconds",
			mutate: func(cfg *Config) {
				cfg.Pricing = validPricingConfig()
				cfg.Pricing.Chains[0].CoinGecko.MaxAgeSeconds = overflow
			},
		},
		{
			name:  "chainlink max age",
			field: "chainlink.max_age_seconds",
			mutate: func(cfg *Config) {
				cfg.Pricing = validPricingConfig()
				for idx := range cfg.Pricing.Chains {
					cfg.Pricing.Chains[idx].PrimarySource = "chainlink"
					cfg.Pricing.Chains[idx].CoinGecko = CoinGeckoPricingConfig{}
					cfg.Pricing.Chains[idx].Chainlink = ChainlinkPricingConfig{
						FeedAddress:         MustEVMAddress("0x1111111111111111111111111111111111111111"),
						ExpectedDescription: "ETH / USD",
						MaxAgeSeconds:       3600,
					}
				}
				cfg.Pricing.Chains[0].Chainlink.MaxAgeSeconds = overflow
			},
		},
		{
			name:  "uniswap max block age",
			field: "uniswap.max_block_age_seconds",
			mutate: func(cfg *Config) {
				cfg.Pricing = validPricingConfig()
				for idx := range cfg.Pricing.Chains {
					cfg.Pricing.Chains[idx].SanitySources = []string{"uniswap"}
					cfg.Pricing.Chains[idx].Uniswap = UniswapPricingConfig{
						PoolAddress:              MustEVMAddress("0x1111111111111111111111111111111111111111"),
						TokenIn:                  MustEVMAddress("0x2222222222222222222222222222222222222222"),
						TokenOut:                 MustEVMAddress("0x3333333333333333333333333333333333333333"),
						TWAPWindowSeconds:        MinUniswapTWAPWindowSeconds,
						MaxBlockAgeSeconds:       120,
						MinHarmonicMeanLiquidity: "1000000",
					}
				}
				cfg.Pricing.Chains[0].Uniswap.MaxBlockAgeSeconds = overflow
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want duration overflow error")
			}
			if !strings.Contains(err.Error(), test.field) {
				t.Fatalf("Validate() error = %q, want field %q", err, test.field)
			}
		})
	}
}

func TestValidateRejectsMissingTxManagerStaleBroadcastReplacementAfter(t *testing.T) {
	cfg := validConfig()
	cfg.TxManager.StaleBroadcastReplacementAfterSeconds = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing tx manager stale replacement duration error")
	}
}

func TestValidateAcceptsSupportedRPCURLFormats(t *testing.T) {
	for _, rpcURL := range []string{
		"http://localhost:8545",
		"https://rpc.example.com",
		"ws://localhost:8546",
		"wss://rpc.example.com/ws",
		"/var/run/geth.ipc",
	} {
		t.Run(rpcURL, func(t *testing.T) {
			cfg := validConfig()
			cfg.Chains[0].RPCURLs = []string{rpcURL}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestValidateRejectsInvalidRPCURLFormats(t *testing.T) {
	for name, rpcURL := range map[string]string{
		"empty":       "",
		"whitespace":  " http://localhost:8545",
		"missingHost": "https:///rpc",
		"unknown":     "ftp://localhost/rpc",
		"relativeIPC": "geth.ipc",
		"ipcScheme":   "ipc:///var/run/geth.ipc",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Chains[0].RPCURLs = []string{rpcURL}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want invalid rpc url error")
			}
		})
	}
}

func TestValidateRejectsMalformedSecretBearingRPCURLsWithoutEchoingThem(t *testing.T) {
	const secret = "SUPER_SECRET_API_KEY"
	for name, rpcURL := range map[string]string{
		"invalid port":   "https://host:443:443/v2/" + secret,
		"invalid escape": "https://host/v2/" + secret + "%zz",
		"invalid IPv6":   "ws://[::1/" + secret,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Chains[0].RPCURLs = []string{rpcURL}
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want malformed rpc url error")
			}
			if !strings.Contains(err.Error(), "rpc_urls[0] is invalid: value is malformed") {
				t.Fatalf("Validate() error = %q, want fixed malformed URL error", err)
			}
			for _, sensitive := range []string{rpcURL, secret, "/v2/"} {
				if strings.Contains(err.Error(), sensitive) {
					t.Fatalf("Validate() error leaked %q: %s", sensitive, err)
				}
			}
		})
	}
}

func TestValidateRejectsMissingExecutorSigner(t *testing.T) {
	cfg := validConfig()
	cfg.Chains[0].TxRoles.Executor.Signer = EVMAddress{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing executor signer error")
	}
}

func TestValidateRejectsUnknownExecutorSigner(t *testing.T) {
	cfg := validConfig()
	cfg.Chains[0].TxRoles.Executor.Signer = MustEVMAddress("0x1111111111111111111111111111111111111111")
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want unknown executor signer error")
	}
}

func TestValidateRejectsIncompleteExecutorFeePolicy(t *testing.T) {
	cfg := validConfig()
	cfg.Chains[0].TxRoles.Executor.MaxFeePerGasWei = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing executor max fee error")
	}
}

func TestValidateRejectsMissingExecutorMinNativeBalance(t *testing.T) {
	cfg := validConfig()
	cfg.Chains[0].TxRoles.Executor.MinNativeBalanceWei = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing executor min native balance error")
	}
}

func TestValidateRejectsInvalidExecutorMinNativeBalance(t *testing.T) {
	for name, value := range map[string]string{
		"zero":    "0",
		"invalid": "abc",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Chains[0].TxRoles.Executor.MinNativeBalanceWei = value
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want invalid executor min native balance error")
			}
		})
	}
}

func TestValidateRoleAwareSignerRequirements(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{
			name: "executor disabled allows missing executor signer and fees",
			mutate: func(cfg *Config) {
				cfg.Services.Executor.Enabled = new(false)
				for i := range cfg.Chains {
					cfg.Chains[i].TxRoles.Executor = ExecutorTxRoleConfig{}
				}
			},
		},
		{
			name: "dvn disabled allows active pathway without dvn signer and fees",
			mutate: func(cfg *Config) {
				cfg.Services.DVN.Enabled = new(false)
				cfg.Pathways[0].DVN.Mode = DVNModeActive
				for i := range cfg.Chains {
					cfg.Chains[i].TxRoles.DVN = DVNTxRoleConfig{}
				}
			},
		},
		{
			name: "empty signers allowed when no enabled service needs signing",
			mutate: func(cfg *Config) {
				cfg.Services.Executor.Enabled = new(false)
				cfg.Services.DVN.Enabled = new(false)
				cfg.Signers = nil
				for i := range cfg.Chains {
					cfg.Chains[i].TxRoles.Executor = ExecutorTxRoleConfig{}
					cfg.Chains[i].TxRoles.DVN = DVNTxRoleConfig{}
				}
			},
		},
		{
			name: "empty signers rejected for executor",
			mutate: func(cfg *Config) {
				cfg.Services.DVN.Enabled = new(false)
				cfg.Signers = nil
			},
			wantErr: true,
		},
		{
			name: "empty signers rejected for active dvn",
			mutate: func(cfg *Config) {
				cfg.Services.Executor.Enabled = new(false)
				cfg.Pathways[0].DVN.Mode = DVNModeActive
				cfg.Signers = nil
			},
			wantErr: true,
		},
		{
			name: "empty signers rejected for pricing",
			mutate: func(cfg *Config) {
				cfg.Services.Executor.Enabled = new(false)
				cfg.Services.DVN.Enabled = new(false)
				cfg.Pricing = validPricingConfig()
				cfg.Signers = nil
			},
			wantErr: true,
		},
		{
			name: "pricing only allows missing worker tx roles with pricing signer",
			mutate: func(cfg *Config) {
				cfg.Services.Executor.Enabled = new(false)
				cfg.Services.DVN.Enabled = new(false)
				cfg.Pricing = validPricingConfig()
				for i := range cfg.Chains {
					cfg.Chains[i].TxRoles.Executor = ExecutorTxRoleConfig{}
					cfg.Chains[i].TxRoles.DVN = DVNTxRoleConfig{}
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)
			err := cfg.Validate()
			if test.wantErr && err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestValidateAcceptsActiveDVNConfig(t *testing.T) {
	cfg := validConfig()
	cfg.Pathways[0].DVN.Mode = DVNModeActive
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateAcceptsShadowDVNWithoutSigner(t *testing.T) {
	cfg := validConfig()
	for i := range cfg.Chains {
		cfg.Chains[i].TxRoles.DVN.Signer = EVMAddress{}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsActiveDVNWithoutSigner(t *testing.T) {
	cfg := validConfig()
	cfg.Pathways[0].DVN.Mode = DVNModeActive
	cfg.Chains[1].TxRoles.DVN.Signer = EVMAddress{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing active dvn signer error")
	}
}

func TestValidateRejectsActiveDVNWithoutFees(t *testing.T) {
	cfg := validConfig()
	cfg.Pathways[0].DVN.Mode = DVNModeActive
	cfg.Chains[1].TxRoles.DVN.MaxFeePerGasWei = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing active dvn fee error")
	}
}

func TestValidateRejectsActiveDVNWithoutMinNativeBalance(t *testing.T) {
	cfg := validConfig()
	cfg.Pathways[0].DVN.Mode = DVNModeActive
	cfg.Chains[1].TxRoles.DVN.MinNativeBalanceWei = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing active dvn min native balance error")
	}
}

func TestValidateAcceptsActiveDVNWithoutPriorityFeeCap(t *testing.T) {
	cfg := validConfig()
	cfg.Pathways[0].DVN.Mode = DVNModeActive
	cfg.Chains[1].TxRoles.DVN.MaxPriorityFeePerGasWei = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsKeystoreSignerWithoutPasswordSource(t *testing.T) {
	cfg := validConfig()
	cfg.Signers[0].Keystore.PasswordEnv = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing keystore password source error")
	}
}

func TestValidateAcceptsAbsoluteKeystorePasswordFile(t *testing.T) {
	cfg := validConfig()
	cfg.Signers[0].Keystore.PasswordEnv = ""
	cfg.Signers[0].Keystore.PasswordFile = "/run/secrets/keystore-password"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsInvalidKeystorePasswordFileWithoutEcho(t *testing.T) {
	const secret = "actual-keystore-password=abc123"
	cfg := validConfig()
	cfg.Signers[0].Keystore.PasswordEnv = ""
	cfg.Signers[0].Keystore.PasswordFile = secret
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want invalid password file path error")
	}
	if !strings.Contains(err.Error(), "signers[0].keystore.password_file must be an absolute file path") {
		t.Fatalf("Validate() error = %q, want password file path error", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "abc123") {
		t.Fatalf("Validate() error leaked password_file value: %q", err)
	}
}

func TestValidateRejectsInvalidSecretEnvironmentVariableNamesWithoutEcho(t *testing.T) {
	const secret = "sk-live.actual-secret=abc123"
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name: "keystore password",
			mutate: func(cfg *Config) {
				cfg.Signers[0].Keystore.PasswordEnv = secret
			},
			want: "signers[0].keystore.password_env must be an uppercase environment variable name",
		},
		{
			name: "coinmarketcap api key",
			mutate: func(cfg *Config) {
				cfg.Pricing.CoinMarketCapAPIKeyEnv = secret
			},
			want: "pricing.coinmarketcap_api_key_env must be an uppercase environment variable name",
		},
		{
			name: "coingecko api key",
			mutate: func(cfg *Config) {
				cfg.Pricing.CoinGeckoAPIKeyEnv = secret
			},
			want: "pricing.coingecko_api_key_env must be an uppercase environment variable name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want invalid environment variable name error")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %q, want %q", err, test.want)
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "abc123") {
				t.Fatalf("Validate() error leaked secret environment reference: %q", err)
			}
		})
	}
}

func TestValidateRejectsMismatchedKMSSignerAddress(t *testing.T) {
	cfg := validConfig()
	cfg.Signers[0] = SignerConfig{
		ID:   MustEVMAddress("0x9999999999999999999999999999999999999999"),
		Type: "kms",
		KMS: KMSSignerConfig{
			KeyID:   "test-key",
			Region:  "us-east-1",
			Address: MustEVMAddress("0x1111111111111111111111111111111111111111"),
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want mismatched kms address error")
	}
}

func TestValidateRejectsDuplicatePathway(t *testing.T) {
	cfg := validConfig()
	cfg.Pathways = append(cfg.Pathways, cfg.Pathways[0])
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want duplicate pathway error")
	}
}

func TestValidateAcceptsEnabledPricing(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsInsecureMarketDataBaseURLsWithoutEchoingThem(t *testing.T) {
	const secretBaseURL = "http://pricing-user:pricing-password@pricing-secret.example/private-api-key"
	tests := []struct {
		name   string
		field  string
		mutate func(*PricingConfig)
	}{
		{
			name:  "coinmarketcap",
			field: "pricing coinmarketcap_base_url",
			mutate: func(pricing *PricingConfig) {
				pricing.CoinMarketCapBaseURL = secretBaseURL
			},
		},
		{
			name:  "coingecko",
			field: "pricing coingecko_base_url",
			mutate: func(pricing *PricingConfig) {
				pricing.CoinGeckoBaseURL = secretBaseURL
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Pricing = validPricingConfig()
			test.mutate(&cfg.Pricing)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want insecure market-data base URL error")
			}
			if !strings.Contains(err.Error(), test.field+" must be an absolute HTTPS URL") {
				t.Fatalf("Validate() error = %q, want HTTPS requirement", err)
			}
			for _, secret := range []string{secretBaseURL, "pricing-user", "pricing-password", "pricing-secret.example", "private-api-key"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("Validate() error leaked %q: %s", secret, err)
				}
			}
		})
	}
}

func TestValidateRejectsEnabledPricingWithoutMinNativeBalance(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pricing.Chains[0].TxPolicy.MinNativeBalanceWei = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing per-chain pricing min native balance error")
	}
}

func TestValidateRejectsPricingStaleAfterAboveContractMaximum(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pricing.StaleAfterSeconds = MaxPriceSnapshotStaleAfterSeconds + 1
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want OpenPriceFeed stale-after maximum error")
	}
}

func TestValidateRejectsEnabledPricingWithoutPathwayFees(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pathways[0].Pricing = PathwayPricingConfig{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing pathway pricing error")
	}
}

func TestValidateRejectsInvalidPathwayPricingFeeModel(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*PathwayPricingConfig)
	}{
		{
			name: "negative fixed fee",
			mutate: func(pricing *PathwayPricingConfig) {
				pricing.ExecutorFee.FixedFeeWei = "-1"
			},
		},
		{
			name: "invalid fixed fee",
			mutate: func(pricing *PathwayPricingConfig) {
				pricing.ExecutorFee.FixedFeeWei = "abc"
			},
		},
		{
			name: "missing data size overhead",
			mutate: func(pricing *PathwayPricingConfig) {
				pricing.ExecutorFee.DataSizeOverheadBytes = nil
			},
		},
		{
			name: "margin too high",
			mutate: func(pricing *PathwayPricingConfig) {
				pricing.DVNFee.MarginBps = 10_001
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Pricing = validPricingConfig()
			test.mutate(&cfg.Pathways[0].Pricing)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want invalid pathway pricing error")
			}
		})
	}
}

func TestValidateAllowsOmittedPathwayPricingWhenPricingDisabled(t *testing.T) {
	cfg := validConfig()
	for i := range cfg.Pathways {
		cfg.Pathways[i].Pricing = PathwayPricingConfig{}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsIncompletePricingChains(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pricing.Chains = cfg.Pricing.Chains[:1]
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want incomplete pricing chains error")
	}
}

func TestValidateRejectsMissingPricingChainDataFee(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pricing.Chains[0].DataFeePerByteWei = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing data fee per byte error")
	}
}

func TestValidateAcceptsSameNativePricingWithoutMarketSources(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = sameNativePricingConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsSameNativePricingWithMarketSources(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = sameNativePricingConfig()
	cfg.Pricing.Chains[0].PrimarySource = "coingecko"
	cfg.Pricing.Chains[0].CoinGecko = CoinGeckoPricingConfig{ID: "ethereum", MaxAgeSeconds: 180}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want unused same-native market source error")
	}
}

func TestValidateRejectsUppercasePricingNativeAssetID(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = sameNativePricingConfig()
	cfg.Pricing.Chains[0].NativeAssetID = "ETH"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want uppercase native asset id error")
	}
}

func TestValidateRejectsCrossAssetPricingWithoutMarketSources(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = sameNativePricingConfig()
	cfg.Pricing.Chains[1].NativeAssetID = "hoodi-eth"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing market source error")
	}
}

func TestValidateAcceptsPricingWithoutPriorityFeeCap(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	for i := range cfg.Pricing.Chains {
		cfg.Pricing.Chains[i].TxPolicy.MaxPriorityFeePerGasWei = ""
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateAcceptsCoinMarketCapPrimaryPricing(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pricing.CoinMarketCapAPIKeyEnv = "COINMARKETCAP_API_KEY"
	for i := range cfg.Pricing.Chains {
		cfg.Pricing.Chains[i].PrimarySource = "coinmarketcap"
		cfg.Pricing.Chains[i].SanitySources = nil
		cfg.Pricing.Chains[i].CoinMarketCap = CoinMarketCapPricingConfig{ID: 1027, MaxAgeSeconds: 180}
		cfg.Pricing.Chains[i].CoinGecko = CoinGeckoPricingConfig{}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsCoinMarketCapPrimaryWithoutAPIKeyEnv(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	for i := range cfg.Pricing.Chains {
		cfg.Pricing.Chains[i].PrimarySource = "coinmarketcap"
		cfg.Pricing.Chains[i].SanitySources = nil
		cfg.Pricing.Chains[i].CoinMarketCap = CoinMarketCapPricingConfig{ID: 1027, MaxAgeSeconds: 180}
		cfg.Pricing.Chains[i].CoinGecko = CoinGeckoPricingConfig{}
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing coinmarketcap api key env error")
	}
}

func TestValidateRejectsCoinMarketCapSanityWithoutAPIKeyEnv(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pricing.Chains[0].CoinMarketCap = CoinMarketCapPricingConfig{ID: 1027, MaxAgeSeconds: 180}
	cfg.Pricing.Chains[0].SanitySources = []string{"coinmarketcap"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing coinmarketcap api key env error")
	}
}

func TestValidateAcceptsCoinGeckoPrimaryPricing(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	for i := range cfg.Pricing.Chains {
		cfg.Pricing.Chains[i].PrimarySource = "coingecko"
		cfg.Pricing.Chains[i].SanitySources = nil
		cfg.Pricing.Chains[i].CoinGecko = CoinGeckoPricingConfig{ID: "ethereum", MaxAgeSeconds: 180}
		cfg.Pricing.Chains[i].Uniswap = UniswapPricingConfig{}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsUniswapPrimaryPricing(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pricing.Chains[0].PrimarySource = "uniswap"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want unsupported uniswap primary error")
	}
}

func TestValidateAcceptsOptionalChainlinkSanityPricing(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	for i := range cfg.Pricing.Chains {
		cfg.Pricing.Chains[i].SanitySources = []string{"chainlink"}
		cfg.Pricing.Chains[i].Chainlink = ChainlinkPricingConfig{
			FeedAddress:         MustEVMAddress("0x1111111111111111111111111111111111111111"),
			ExpectedDescription: "ETH / USD",
			MaxAgeSeconds:       3600,
		}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateAcceptsChainlinkPrimaryPricing(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	for i := range cfg.Pricing.Chains {
		cfg.Pricing.Chains[i].PrimarySource = "chainlink"
		cfg.Pricing.Chains[i].CoinGecko = CoinGeckoPricingConfig{}
		cfg.Pricing.Chains[i].Chainlink = ChainlinkPricingConfig{
			FeedAddress:         MustEVMAddress("0x1111111111111111111111111111111111111111"),
			ExpectedDescription: "ETH / USD",
			MaxAgeSeconds:       3600,
		}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateUniswapTWAPWindowBounds(t *testing.T) {
	tests := []struct {
		name    string
		window  uint64
		wantErr bool
	}{
		{name: "below minimum", window: MinUniswapTWAPWindowSeconds - 1, wantErr: true},
		{name: "at minimum", window: MinUniswapTWAPWindowSeconds},
		{name: "at uint32 maximum", window: math.MaxUint32},
		{name: "above uint32 maximum", window: uint64(math.MaxUint32) + 1, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Pricing = validPricingConfig()
			for i := range cfg.Pricing.Chains {
				cfg.Pricing.Chains[i].SanitySources = []string{"uniswap"}
				cfg.Pricing.Chains[i].Uniswap = UniswapPricingConfig{
					PoolAddress:              MustEVMAddress("0x1111111111111111111111111111111111111111"),
					TokenIn:                  MustEVMAddress("0x2222222222222222222222222222222222222222"),
					TokenOut:                 MustEVMAddress("0x3333333333333333333333333333333333333333"),
					TWAPWindowSeconds:        MinUniswapTWAPWindowSeconds,
					MaxBlockAgeSeconds:       120,
					MinHarmonicMeanLiquidity: "1000000",
				}
			}
			cfg.Pricing.Chains[0].Uniswap.TWAPWindowSeconds = test.window
			err := cfg.Validate()
			if test.wantErr && err == nil {
				t.Fatal("Validate() error = nil, want TWAP window bound error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestValidateRejectsConfiguredButUnreferencedPricingSource(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	cfg.Pricing.Chains[0].Chainlink = ChainlinkPricingConfig{
		FeedAddress:         MustEVMAddress("0x1111111111111111111111111111111111111111"),
		ExpectedDescription: "ETH / USD",
		MaxAgeSeconds:       3600,
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want unreferenced source error")
	}
}

func TestLoadStaticIgnoresDatabaseURLEnvOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := []byte(`database_url: postgres://file:file@localhost:5432/file?sslmode=disable
signers:
  - id: "0x9999999999999999999999999999999999999999"
    type: keystore
    keystore:
      path: /run/secrets/executor-keystore.json
      password_env: KEYSTORE_PASSWORD
chains:
  - eid: 40161
    name: ethereum-sepolia
    family: evm
    chain_id: 11155111
    endpoint_address: "0x1111111111111111111111111111111111111111"
    confirmations: 12
    rpc_urls:
      - http://localhost:8545
    tx_roles:
      executor:
        signer: "0x9999999999999999999999999999999999999999"
        max_fee_per_gas_wei: "2000000000"
        max_priority_fee_per_gas_wei: "1000000000"
        min_native_balance_wei: "100000000000000000"
      dvn:
        signer: "0x9999999999999999999999999999999999999999"
        max_fee_per_gas_wei: "2000000000"
        max_priority_fee_per_gas_wei: "1000000000"
        min_native_balance_wei: "100000000000000000"
  - eid: 40449
    name: hoodi
    family: evm
    chain_id: 560048
    endpoint_address: "0x4444444444444444444444444444444444444444"
    confirmations: 12
    rpc_urls:
      - http://localhost:8546
    tx_roles:
      executor:
        signer: "0x9999999999999999999999999999999999999999"
        max_fee_per_gas_wei: "2000000000"
        max_priority_fee_per_gas_wei: "1000000000"
        min_native_balance_wei: "100000000000000000"
      dvn:
        signer: "0x9999999999999999999999999999999999999999"
        max_fee_per_gas_wei: "2000000000"
        max_priority_fee_per_gas_wei: "1000000000"
        min_native_balance_wei: "100000000000000000"
pathways:
  - src_eid: 40161
    dst_eid: 40449
    src_oapp: "0x7777777777777777777777777777777777777777"
    dst_oapp: "0x8888888888888888888888888888888888888888"
    send_lib: "0x9999999999999999999999999999999999999999"
    receive_lib: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    source_workers:
      open_executor: "0x2222222222222222222222222222222222222222"
      open_dvn: "0x3333333333333333333333333333333333333333"
      price_feed: "0x4444444444444444444444444444444444444444"
    destination_workers:
      open_dvn: "0x6666666666666666666666666666666666666666"
    enabled: true
    max_message_size: 10000
    min_lz_receive_gas: 100000
    max_lz_receive_gas: 300000
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("DATABASE_URL", "postgres://env:env@localhost:5432/env?sslmode=disable")

	staticConfig, err := LoadStatic(path)
	if err != nil {
		t.Fatalf("LoadStatic() error = %v", err)
	}
	if staticConfig.DatabaseURL != "postgres://file:file@localhost:5432/file?sslmode=disable" {
		t.Fatalf("LoadStatic() database_url = %q, want file value", staticConfig.DatabaseURL)
	}
	if staticConfig.Chains[0].StartBlockNumber != 0 {
		t.Fatalf("LoadStatic() start_block_number = %d, want default 0", staticConfig.Chains[0].StartBlockNumber)
	}
	if staticConfig.Chains[0].IndexerQueryBlockRange != 500 {
		t.Fatalf("LoadStatic() indexer_query_block_range = %d, want default 500", staticConfig.Chains[0].IndexerQueryBlockRange)
	}
	if staticConfig.Chains[0].IndexerPollIntervalSeconds != 5 {
		t.Fatalf("LoadStatic() indexer_poll_interval_seconds = %d, want default 5", staticConfig.Chains[0].IndexerPollIntervalSeconds)
	}
	if staticConfig.TxManager.StaleBroadcastReplacementAfterSeconds != 900 {
		t.Fatalf("LoadStatic() tx_manager.stale_broadcast_replacement_after_seconds = %d, want 900", staticConfig.TxManager.StaleBroadcastReplacementAfterSeconds)
	}
	if staticConfig.Pathways[0].DVN.Mode != DVNModeShadow {
		t.Fatalf("LoadStatic() pathway dvn mode = %q, want %q", staticConfig.Pathways[0].DVN.Mode, DVNModeShadow)
	}
	if !staticConfig.ExecutorEnabled() || !staticConfig.DVNEnabled() {
		t.Fatal("LoadStatic() omitted services should default executor and dvn to enabled")
	}
	workerConfig, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if workerConfig.DatabaseURL != "postgres://env:env@localhost:5432/env?sslmode=disable" {
		t.Fatalf("Load() database_url = %q, want env override", workerConfig.DatabaseURL)
	}
}

func TestLoadStaticDefaultsAndAcceptsIndexerPollIntervalSeconds(t *testing.T) {
	cfg := validConfig()
	cfg.Chains[0].IndexerPollIntervalSeconds = 0
	cfg.Chains[1].IndexerPollIntervalSeconds = 17
	body, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	loaded, err := LoadStatic(path)
	if err != nil {
		t.Fatalf("LoadStatic() error = %v", err)
	}
	if loaded.Chains[0].IndexerPollIntervalSeconds != 5 {
		t.Fatalf("default indexer poll interval = %d, want 5", loaded.Chains[0].IndexerPollIntervalSeconds)
	}
	if loaded.Chains[1].IndexerPollIntervalSeconds != 17 {
		t.Fatalf("custom indexer poll interval = %d, want 17", loaded.Chains[1].IndexerPollIntervalSeconds)
	}
}

func TestLoadStaticRejectsInvalidUnsignedIntegers(t *testing.T) {
	cfg := validConfig()
	cfg.Pricing = validPricingConfig()
	body, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	tests := []struct {
		name      string
		oldValue  string
		newValue  string
		wantField string
	}{
		{
			name:      "negative indexer poll interval",
			oldValue:  "indexer_poll_interval_seconds: 5",
			newValue:  "indexer_poll_interval_seconds: -1",
			wantField: "indexer_poll_interval_seconds",
		},
		{
			name:      "floating point indexer poll interval",
			oldValue:  "indexer_poll_interval_seconds: 5",
			newValue:  "indexer_poll_interval_seconds: 1.5",
			wantField: "chains[0].indexer_poll_interval_seconds",
		},
		{
			name:      "string indexer poll interval",
			oldValue:  "indexer_poll_interval_seconds: 5",
			newValue:  `indexer_poll_interval_seconds: "5"`,
			wantField: "chains[0].indexer_poll_interval_seconds",
		},
		{
			name:      "floating point pricing source timeout",
			oldValue:  "source_request_timeout_seconds: 10",
			newValue:  "source_request_timeout_seconds: 1.5",
			wantField: "pricing.source_request_timeout_seconds",
		},
		{
			name:      "floating point pricing chain eid",
			oldValue:  "eid: 40161",
			newValue:  "eid: 40161.5",
			wantField: "pricing.chains[0].eid",
		},
		{
			name:      "floating point uint16 fee margin",
			oldValue:  "margin_bps: 100",
			newValue:  "margin_bps: 100.5",
			wantField: "pathways[0].pricing.executor_fee.margin_bps",
		},
		{
			name:      "floating point pointer uint64",
			oldValue:  "data_size_overhead_bytes: 0",
			newValue:  "data_size_overhead_bytes: 0.5",
			wantField: "pathways[0].pricing.executor_fee.data_size_overhead_bytes",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := strings.Replace(
				string(body),
				test.oldValue,
				test.newValue,
				1,
			)
			if invalid == string(body) {
				t.Fatalf("test fixture does not contain %q", test.oldValue)
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
			_, err := LoadStatic(path)
			if err == nil {
				t.Fatal("LoadStatic() error = nil, want invalid unsigned integer error")
			}
			if !strings.Contains(err.Error(), test.wantField) {
				t.Fatalf("LoadStatic() error = %q, want field %q", err, test.wantField)
			}
		})
	}
}

func TestLoadStaticRejectsRetiredPricingFields(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "sanity fallback", body: "pricing:\n  allow_sanity_fallback: true\n"},
		{name: "global max fee", body: "pricing:\n  max_fee_per_gas_wei: \"2000000000\"\n"},
		{name: "global priority fee", body: "pricing:\n  max_priority_fee_per_gas_wei: \"1000000000\"\n"},
		{name: "global minimum balance", body: "pricing:\n  min_native_balance_wei: \"100000000000000000\"\n"},
		{name: "coinmarketcap symbol", body: "pricing:\n  chains:\n    - eid: 1\n      coinmarketcap_symbol: ETH\n"},
		{name: "flat coingecko id", body: "pricing:\n  chains:\n    - eid: 1\n      coingecko_id: ethereum\n"},
		{name: "quoter", body: "pricing:\n  chains:\n    - eid: 1\n      uniswap:\n        quoter_address: 0x1111111111111111111111111111111111111111\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
			if _, err := LoadStatic(path); err == nil {
				t.Fatal("LoadStatic() error = nil, want retired field error")
			}
		})
	}
}

func validConfig() Config {
	return Config{
		DatabaseURL: "postgres://user:pass@localhost:5432/db?sslmode=disable",
		TxManager: TxManagerConfig{
			StaleBroadcastReplacementAfterSeconds: 900,
		},
		Signers: []SignerConfig{
			{
				ID:   MustEVMAddress("0x9999999999999999999999999999999999999999"),
				Type: "keystore",
				Keystore: KeystoreSignerConfig{
					Path:        "/run/secrets/executor-keystore.json",
					PasswordEnv: "KEYSTORE_PASSWORD",
				},
			},
		},
		Chains: []ChainConfig{
			{
				EID:                        40161,
				Name:                       "ethereum-sepolia",
				Family:                     ChainFamilyEVM,
				ChainID:                    11155111,
				EndpointAddress:            MustEVMAddress("0x1111111111111111111111111111111111111111"),
				Confirmations:              12,
				IndexerQueryBlockRange:     500,
				IndexerPollIntervalSeconds: 5,
				RPCURLs:                    []string{"http://localhost:8545"},
				TxRoles: ChainTxRolesConfig{
					Executor: validExecutorTxRoleConfig(),
					DVN:      validDVNTxRoleConfig(),
				},
			},
			{
				EID:                        40449,
				Name:                       "hoodi",
				Family:                     ChainFamilyEVM,
				ChainID:                    560048,
				EndpointAddress:            MustEVMAddress("0x4444444444444444444444444444444444444444"),
				Confirmations:              12,
				IndexerQueryBlockRange:     500,
				IndexerPollIntervalSeconds: 5,
				RPCURLs:                    []string{"http://localhost:8546"},
				TxRoles: ChainTxRolesConfig{
					Executor: validExecutorTxRoleConfig(),
					DVN:      validDVNTxRoleConfig(),
				},
			},
		},
		Pathways: []PathwayConfig{
			{
				SrcEID:     40161,
				DstEID:     40449,
				SrcOApp:    MustEVMAddress("0x7777777777777777777777777777777777777777"),
				DstOApp:    MustEVMAddress("0x8888888888888888888888888888888888888888"),
				SendLib:    MustEVMAddress("0x9999999999999999999999999999999999999999"),
				ReceiveLib: MustEVMAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				SourceWorkers: WorkerContractsConfig{
					OpenExecutor: MustEVMAddress("0x2222222222222222222222222222222222222222"),
					OpenDVN:      MustEVMAddress("0x3333333333333333333333333333333333333333"),
					PriceFeed:    MustEVMAddress("0x4444444444444444444444444444444444444444"),
				},
				DestinationWorkers: DestinationWorkerContractsConfig{
					OpenDVN: MustEVMAddress("0x6666666666666666666666666666666666666666"),
				},
				DVN:             PathwayDVNConfig{Mode: DVNModeShadow},
				Pricing:         validPathwayPricingConfig(),
				Enabled:         true,
				MaxMessageSize:  10000,
				MinLzReceiveGas: 100000,
				MaxLzReceiveGas: 300000,
			},
			{
				SrcEID:     40449,
				DstEID:     40161,
				SrcOApp:    MustEVMAddress("0x8888888888888888888888888888888888888888"),
				DstOApp:    MustEVMAddress("0x7777777777777777777777777777777777777777"),
				SendLib:    MustEVMAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				ReceiveLib: MustEVMAddress("0xcccccccccccccccccccccccccccccccccccccccc"),
				SourceWorkers: WorkerContractsConfig{
					OpenExecutor: MustEVMAddress("0x5555555555555555555555555555555555555555"),
					OpenDVN:      MustEVMAddress("0x6666666666666666666666666666666666666666"),
					PriceFeed:    MustEVMAddress("0x9999999999999999999999999999999999999999"),
				},
				DestinationWorkers: DestinationWorkerContractsConfig{
					OpenDVN: MustEVMAddress("0x3333333333333333333333333333333333333333"),
				},
				DVN:             PathwayDVNConfig{Mode: DVNModeShadow},
				Pricing:         validPathwayPricingConfig(),
				Enabled:         true,
				MaxMessageSize:  10000,
				MinLzReceiveGas: 100000,
				MaxLzReceiveGas: 300000,
			},
		},
	}
}

func validExecutorTxRoleConfig() ExecutorTxRoleConfig {
	return ExecutorTxRoleConfig{
		Signer:                  MustEVMAddress("0x9999999999999999999999999999999999999999"),
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func validDVNTxRoleConfig() DVNTxRoleConfig {
	return DVNTxRoleConfig{
		Signer:                  MustEVMAddress("0x9999999999999999999999999999999999999999"),
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func validPricingConfig() PricingConfig {
	return PricingConfig{
		Enabled:                     true,
		Signer:                      MustEVMAddress("0x9999999999999999999999999999999999999999"),
		IntervalSeconds:             300,
		StaleAfterSeconds:           1800,
		MaxDeviationBps:             500,
		SourceRequestTimeoutSeconds: 10,
		GasSpikeBps:                 1000,
		Chains: []PricingChainConfig{
			{
				EID:               40161,
				TxPolicy:          validPricingTxPolicyConfig(),
				NativeAssetID:     "eth",
				DataFeePerByteWei: "0",
				PrimarySource:     "coingecko",
				CoinGecko:         CoinGeckoPricingConfig{ID: "ethereum", MaxAgeSeconds: 180},
			},
			{
				EID:               40449,
				TxPolicy:          validPricingTxPolicyConfig(),
				NativeAssetID:     "hoodi-eth",
				DataFeePerByteWei: "0",
				PrimarySource:     "coingecko",
				CoinGecko:         CoinGeckoPricingConfig{ID: "ethereum", MaxAgeSeconds: 180},
			},
		},
	}
}

func sameNativePricingConfig() PricingConfig {
	pricing := validPricingConfig()
	for idx, chain := range pricing.Chains {
		pricing.Chains[idx] = PricingChainConfig{
			EID:               chain.EID,
			TxPolicy:          chain.TxPolicy,
			NativeAssetID:     "eth",
			DataFeePerByteWei: "0",
		}
	}
	return pricing
}

func validPricingTxPolicyConfig() PricingTxPolicyConfig {
	return PricingTxPolicyConfig{
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func validPathwayPricingConfig() PathwayPricingConfig {
	return PathwayPricingConfig{
		ExecutorFee: WorkerFeeModelConfig{FixedFeeWei: "1000", DstGasOverhead: 50000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 100},
		DVNFee:      WorkerFeeModelConfig{FixedFeeWei: "2000", DstGasOverhead: 150000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 200},
	}
}
