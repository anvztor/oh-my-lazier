package pricing

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	gethabi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/islishude/oh-my-lazier/go/internal/config"
)

func TestValidateSourceConfigurationsRejectsDeterministicMismatches(t *testing.T) {
	tests := []struct {
		name  string
		build func(*testing.T) ConfiguredPriceReader
		want  string
	}{
		{
			name: "coinmarketcap rejected id",
			build: func(t *testing.T) ConfiguredPriceReader {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
				}))
				t.Cleanup(server.Close)
				client, err := NewCoinMarketCapClient(server.URL, "", server.Client())
				if err != nil {
					t.Fatalf("NewCoinMarketCapClient() error = %v", err)
				}
				reader, err := NewCoinMarketCapPriceReader(client, 999999999)
				if err != nil {
					t.Fatalf("NewCoinMarketCapPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coinmarketcap", Reader: reader}
			},
			want: "coinmarketcap price request returned HTTP 400",
		},
		{
			name: "coinmarketcap empty successful payload",
			build: func(t *testing.T) ConfiguredPriceReader {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"data":[]}`))
				}))
				t.Cleanup(server.Close)
				client, err := NewCoinMarketCapClient(server.URL, "", server.Client())
				if err != nil {
					t.Fatalf("NewCoinMarketCapClient() error = %v", err)
				}
				reader, err := NewCoinMarketCapPriceReader(client, 1027)
				if err != nil {
					t.Fatalf("NewCoinMarketCapPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coinmarketcap", Reader: reader}
			},
			want: "coinmarketcap returned no unique price for id 1027",
		},
		{
			name: "coingecko missing id",
			build: func(t *testing.T) ConfiguredPriceReader {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{}`))
				}))
				t.Cleanup(server.Close)
				client, err := NewCoinGeckoClient(server.URL, "", server.Client())
				if err != nil {
					t.Fatalf("NewCoinGeckoClient() error = %v", err)
				}
				reader, err := NewCoinGeckoPriceReader(client, "missing-asset")
				if err != nil {
					t.Fatalf("NewCoinGeckoPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coingecko", Reader: reader}
			},
			want: "coingecko returned no price for missing-asset",
		},
		{
			name: "chainlink description mismatch",
			build: func(t *testing.T) ConfiguredPriceReader {
				caller := newFakeEVMReader(t, time.Unix(1_700_000_000, 0))
				caller.addResponse(t, chainlinkAggregatorV3ABI.Methods["description"].ID, chainlinkAggregatorV3ABI.Methods["description"].Outputs, "BTC / USD")
				reader, err := NewChainlinkClient(caller, caller, ChainlinkConfig{
					FeedAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"), ExpectedDescription: "ETH / USD",
				})
				if err != nil {
					t.Fatalf("NewChainlinkClient() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "chainlink", Reader: reader}
			},
			want: `chainlink description "BTC / USD" does not match expected "ETH / USD"`,
		},
		{
			name: "chainlink EOA response",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredChainlinkTestReader(t, staticEVMResponseReader{})
			},
			want: "chainlink description response is incompatible with the AggregatorV3 ABI",
		},
		{
			name: "chainlink wrong ABI response",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredChainlinkTestReader(t, staticEVMResponseReader{response: []byte{0x01}})
			},
			want: "chainlink description response is incompatible with the AggregatorV3 ABI",
		},
		{
			name: "chainlink wrong ABI revert",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredChainlinkTestReader(t, staticEVMResponseReader{err: contractRevertRPCError{}})
			},
			want: "chainlink description response is incompatible with the AggregatorV3 ABI",
		},
		{
			name: "uniswap token pair mismatch",
			build: func(t *testing.T) ConfiguredPriceReader {
				caller := newFakeEVMReader(t, time.Unix(1_700_000_000, 0))
				caller.addResponse(t, uniswapV3PoolABI.Methods["token0"].ID, uniswapV3PoolABI.Methods["token0"].Outputs, common.HexToAddress("0x4444444444444444444444444444444444444444"))
				caller.addResponse(t, uniswapV3PoolABI.Methods["token1"].ID, uniswapV3PoolABI.Methods["token1"].Outputs, common.HexToAddress("0x5555555555555555555555555555555555555555"))
				reader, err := NewUniswapV3Client(caller, caller, UniswapV3Config{
					PoolAddress:              common.HexToAddress("0x1111111111111111111111111111111111111111"),
					TokenIn:                  common.HexToAddress("0x2222222222222222222222222222222222222222"),
					TokenOut:                 common.HexToAddress("0x3333333333333333333333333333333333333333"),
					TWAPWindowSeconds:        uint32(config.MinUniswapTWAPWindowSeconds),
					MinHarmonicMeanLiquidity: big.NewInt(1),
				})
				if err != nil {
					t.Fatalf("NewUniswapV3Client() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "uniswap", Reader: reader}
			},
			want: "uniswap pool tokens do not match configured token pair",
		},
		{
			name: "uniswap EOA response",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredUniswapTestReader(t, staticEVMResponseReader{})
			},
			want: "uniswap token0 response is incompatible with the pool ABI",
		},
		{
			name: "uniswap wrong ABI response",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredUniswapTestReader(t, staticEVMResponseReader{response: []byte{0x01}})
			},
			want: "uniswap token0 response is incompatible with the pool ABI",
		},
		{
			name: "uniswap wrong ABI revert",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredUniswapTestReader(t, staticEVMResponseReader{err: contractRevertRPCError{}})
			},
			want: "uniswap token0 response is incompatible with the pool ABI",
		},
		{
			name: "uniswap insufficient observation history",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredUniswapObservationErrorReader(t, contractRevertRPCError{reason: "OLD"})
			},
			want: "uniswap pool observation history is shorter than the configured 1800-second twap window",
		},
		{
			name: "uniswap insufficient observation history from revert data",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredUniswapObservationErrorReader(t, contractRevertDataRPCError{data: encodedRevertReason(t, "OLD")})
			},
			want: "uniswap pool observation history is shorter than the configured 1800-second twap window",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := test.build(t)
			err := ValidateSourceConfigurations(t.Context(), map[uint32]ChainSources{
				40161: {Primary: reader},
			}, PriceSelectionPolicy{SourceRequestTimeout: time.Second})
			if err == nil {
				t.Fatal("ValidateSourceConfigurations() error = nil, want deterministic configuration error")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateSourceConfigurations() error = %q, want %q", err, test.want)
			}
		})
	}
}

func TestValidateSourceConfigurationsRetriesIncompleteMarketDataPayload(t *testing.T) {
	tests := []struct {
		name       string
		incomplete string
		complete   string
		build      func(*testing.T, string, *http.Client) ConfiguredPriceReader
	}{
		{
			name:       "coinmarketcap",
			incomplete: `{"data":[]}`,
			complete:   `{"data":[{"id":1027,"quote":[{"symbol":"USD","price":2000,"last_updated":"2023-11-14T22:13:20Z"}]}]}`,
			build: func(t *testing.T, baseURL string, httpClient *http.Client) ConfiguredPriceReader {
				client, err := NewCoinMarketCapClient(baseURL, "", httpClient)
				if err != nil {
					t.Fatalf("NewCoinMarketCapClient() error = %v", err)
				}
				reader, err := NewCoinMarketCapPriceReader(client, 1027)
				if err != nil {
					t.Fatalf("NewCoinMarketCapPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coinmarketcap", Reader: reader}
			},
		},
		{
			name:       "coingecko",
			incomplete: `{}`,
			complete:   `{"ethereum":{"usd":2000,"last_updated_at":1700000000}}`,
			build: func(t *testing.T, baseURL string, httpClient *http.Client) ConfiguredPriceReader {
				client, err := NewCoinGeckoClient(baseURL, "", httpClient)
				if err != nil {
					t.Fatalf("NewCoinGeckoClient() error = %v", err)
				}
				reader, err := NewCoinGeckoPriceReader(client, "ethereum")
				if err != nil {
					t.Fatalf("NewCoinGeckoPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coingecko", Reader: reader}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if requests.Add(1) == 1 {
					_, _ = w.Write([]byte(test.incomplete))
					return
				}
				_, _ = w.Write([]byte(test.complete))
			}))
			t.Cleanup(server.Close)

			reader := test.build(t, server.URL, server.Client())
			err := ValidateSourceConfigurations(t.Context(), map[uint32]ChainSources{
				40161: {Primary: reader},
			}, PriceSelectionPolicy{SourceRequestTimeout: time.Second})
			if err != nil {
				t.Fatalf("ValidateSourceConfigurations() error = %v, want retry success", err)
			}
			if got := requests.Load(); got != 2 {
				t.Fatalf("configuration requests = %d, want 2", got)
			}
		})
	}
}

func TestValidateSourceConfigurationsDefersTransientFailures(t *testing.T) {
	tests := []struct {
		name    string
		build   func(*testing.T) ConfiguredPriceReader
		timeout time.Duration
		want    string
	}{
		{
			name: "coinmarketcap server failure",
			build: func(t *testing.T) ConfiguredPriceReader {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusServiceUnavailable)
				}))
				t.Cleanup(server.Close)
				client, err := NewCoinMarketCapClient(server.URL, "", server.Client())
				if err != nil {
					t.Fatalf("NewCoinMarketCapClient() error = %v", err)
				}
				reader, err := NewCoinMarketCapPriceReader(client, 1027)
				if err != nil {
					t.Fatalf("NewCoinMarketCapPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coinmarketcap", Reader: reader}
			},
			timeout: time.Second,
			want:    "unavailable",
		},
		{
			name: "coinmarketcap forbidden",
			build: func(t *testing.T) ConfiguredPriceReader {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}))
				t.Cleanup(server.Close)
				client, err := NewCoinMarketCapClient(server.URL, "", server.Client())
				if err != nil {
					t.Fatalf("NewCoinMarketCapClient() error = %v", err)
				}
				reader, err := NewCoinMarketCapPriceReader(client, 1027)
				if err != nil {
					t.Fatalf("NewCoinMarketCapPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coinmarketcap", Reader: reader}
			},
			timeout: time.Second,
			want:    "unavailable",
		},
		{
			name: "coingecko rate limit",
			build: func(t *testing.T) ConfiguredPriceReader {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusTooManyRequests)
				}))
				t.Cleanup(server.Close)
				client, err := NewCoinGeckoClient(server.URL, "", server.Client())
				if err != nil {
					t.Fatalf("NewCoinGeckoClient() error = %v", err)
				}
				reader, err := NewCoinGeckoPriceReader(client, "ethereum")
				if err != nil {
					t.Fatalf("NewCoinGeckoPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coingecko", Reader: reader}
			},
			timeout: time.Second,
			want:    "unavailable",
		},
		{
			name: "coingecko not found",
			build: func(t *testing.T) ConfiguredPriceReader {
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				}))
				t.Cleanup(server.Close)
				client, err := NewCoinGeckoClient(server.URL, "", server.Client())
				if err != nil {
					t.Fatalf("NewCoinGeckoClient() error = %v", err)
				}
				reader, err := NewCoinGeckoPriceReader(client, "ethereum")
				if err != nil {
					t.Fatalf("NewCoinGeckoPriceReader() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "coingecko", Reader: reader}
			},
			timeout: time.Second,
			want:    "unavailable",
		},
		{
			name: "chainlink rpc failure",
			build: func(t *testing.T) ConfiguredPriceReader {
				reader, err := NewChainlinkClient(unavailableEVMReader{}, unavailableEVMReader{}, ChainlinkConfig{
					FeedAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"), ExpectedDescription: "ETH / USD",
				})
				if err != nil {
					t.Fatalf("NewChainlinkClient() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "chainlink", Reader: reader}
			},
			timeout: time.Second,
			want:    "unavailable",
		},
		{
			name: "uniswap rpc failure",
			build: func(t *testing.T) ConfiguredPriceReader {
				reader, err := NewUniswapV3Client(unavailableEVMReader{}, unavailableEVMReader{}, UniswapV3Config{
					PoolAddress:              common.HexToAddress("0x1111111111111111111111111111111111111111"),
					TokenIn:                  common.HexToAddress("0x2222222222222222222222222222222222222222"),
					TokenOut:                 common.HexToAddress("0x3333333333333333333333333333333333333333"),
					TWAPWindowSeconds:        uint32(config.MinUniswapTWAPWindowSeconds),
					MinHarmonicMeanLiquidity: big.NewInt(1),
				})
				if err != nil {
					t.Fatalf("NewUniswapV3Client() error = %v", err)
				}
				return ConfiguredPriceReader{Name: "uniswap", Reader: reader}
			},
			timeout: time.Second,
			want:    "unavailable",
		},
		{
			name: "uniswap observe non-OLD revert",
			build: func(t *testing.T) ConfiguredPriceReader {
				return configuredUniswapObservationErrorReader(t, contractRevertRPCError{reason: "I"})
			},
			timeout: time.Second,
			want:    "unavailable",
		},
		{
			name: "timeout",
			build: func(*testing.T) ConfiguredPriceReader {
				return ConfiguredPriceReader{Name: "blocking", Reader: blockingSourceConfigurationReader{}}
			},
			timeout: 20 * time.Millisecond,
			want:    "timeout",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := test.build(t)
			var failures []PriceSourceFailure
			err := ValidateSourceConfigurations(t.Context(), map[uint32]ChainSources{
				40161: {Primary: reader},
			}, PriceSelectionPolicy{
				SourceRequestTimeout: test.timeout,
				OnSourceFailure: func(failure PriceSourceFailure) {
					failures = append(failures, failure)
				},
			})
			if err != nil {
				t.Fatalf("ValidateSourceConfigurations() error = %v, want transient failure deferred", err)
			}
			if len(failures) != 1 || failures[0].Source != reader.Name || failures[0].Category != test.want {
				t.Fatalf("source failures = %#v, want source=%s category=%s", failures, reader.Name, test.want)
			}
		})
	}
}

func TestValidateSourceConfigurationsPropagatesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := ValidateSourceConfigurations(ctx, map[uint32]ChainSources{
		40161: {Primary: ConfiguredPriceReader{Name: "blocking", Reader: blockingSourceConfigurationReader{}}},
	}, PriceSelectionPolicy{SourceRequestTimeout: time.Second})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ValidateSourceConfigurations() error = %v, want context canceled", err)
	}
}

type unavailableEVMReader struct{}

func (unavailableEVMReader) CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error) {
	return nil, errors.New("rpc unavailable")
}

func (unavailableEVMReader) CallContractAtHash(context.Context, ethereum.CallMsg, common.Hash) ([]byte, error) {
	return nil, errors.New("rpc unavailable")
}

func (unavailableEVMReader) HeaderByNumber(context.Context, *big.Int) (*gethtypes.Header, error) {
	return nil, errors.New("rpc unavailable")
}

type staticEVMResponseReader struct {
	response []byte
	err      error
}

func (r staticEVMResponseReader) CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error) {
	return append([]byte(nil), r.response...), r.err
}

func (r staticEVMResponseReader) CallContractAtHash(context.Context, ethereum.CallMsg, common.Hash) ([]byte, error) {
	return append([]byte(nil), r.response...), r.err
}

func (staticEVMResponseReader) HeaderByNumber(context.Context, *big.Int) (*gethtypes.Header, error) {
	return &gethtypes.Header{Number: big.NewInt(1), Time: 1}, nil
}

type contractRevertRPCError struct {
	reason string
}

func (e contractRevertRPCError) Error() string {
	if e.reason == "" {
		return "execution reverted"
	}
	return "execution reverted: " + e.reason
}

func (contractRevertRPCError) ErrorCode() int { return 3 }

type contractRevertDataRPCError struct {
	data any
}

func (contractRevertDataRPCError) Error() string  { return "execution reverted" }
func (contractRevertDataRPCError) ErrorCode() int { return 3 }
func (e contractRevertDataRPCError) ErrorData() any {
	return e.data
}

func encodedRevertReason(t *testing.T, reason string) string {
	t.Helper()
	stringType, err := gethabi.NewType("string", "", nil)
	if err != nil {
		t.Fatalf("NewType() error = %v", err)
	}
	encoded, err := (gethabi.Arguments{{Type: stringType}}).Pack(reason)
	if err != nil {
		t.Fatalf("Pack() error = %v", err)
	}
	return hexutil.Encode(append([]byte{0x08, 0xc3, 0x79, 0xa0}, encoded...))
}

type methodErrorCallReader struct {
	fallback CallContractReader
	selector string
	err      error
}

func (r methodErrorCallReader) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	if len(call.Data) >= 4 && string(call.Data[:4]) == r.selector {
		return nil, r.err
	}
	return r.fallback.CallContract(ctx, call, blockNumber)
}

func (r methodErrorCallReader) CallContractAtHash(ctx context.Context, call ethereum.CallMsg, blockHash common.Hash) ([]byte, error) {
	if len(call.Data) >= 4 && string(call.Data[:4]) == r.selector {
		return nil, r.err
	}
	return r.fallback.CallContractAtHash(ctx, call, blockHash)
}

func configuredChainlinkTestReader(t *testing.T, caller staticEVMResponseReader) ConfiguredPriceReader {
	t.Helper()
	reader, err := NewChainlinkClient(caller, caller, ChainlinkConfig{
		FeedAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"), ExpectedDescription: "ETH / USD",
	})
	if err != nil {
		t.Fatalf("NewChainlinkClient() error = %v", err)
	}
	return ConfiguredPriceReader{Name: "chainlink", Reader: reader}
}

func configuredUniswapTestReader(t *testing.T, caller staticEVMResponseReader) ConfiguredPriceReader {
	t.Helper()
	reader, err := NewUniswapV3Client(caller, caller, UniswapV3Config{
		PoolAddress:              common.HexToAddress("0x1111111111111111111111111111111111111111"),
		TokenIn:                  common.HexToAddress("0x2222222222222222222222222222222222222222"),
		TokenOut:                 common.HexToAddress("0x3333333333333333333333333333333333333333"),
		TWAPWindowSeconds:        uint32(config.MinUniswapTWAPWindowSeconds),
		MinHarmonicMeanLiquidity: big.NewInt(1),
	})
	if err != nil {
		t.Fatalf("NewUniswapV3Client() error = %v", err)
	}
	return ConfiguredPriceReader{Name: "uniswap", Reader: reader}
}

func configuredUniswapObservationErrorReader(t *testing.T, observeErr error) ConfiguredPriceReader {
	t.Helper()
	tokenIn := common.HexToAddress("0x2222222222222222222222222222222222222222")
	tokenOut := common.HexToAddress("0x3333333333333333333333333333333333333333")
	responses := newFakeEVMReader(t, time.Unix(1_700_000_000, 0))
	responses.addResponse(t, uniswapV3PoolABI.Methods["token0"].ID, uniswapV3PoolABI.Methods["token0"].Outputs, tokenIn)
	responses.addResponse(t, uniswapV3PoolABI.Methods["token1"].ID, uniswapV3PoolABI.Methods["token1"].Outputs, tokenOut)
	caller := methodErrorCallReader{
		fallback: responses,
		selector: string(uniswapV3PoolABI.Methods["observe"].ID),
		err:      observeErr,
	}
	reader, err := NewUniswapV3Client(caller, responses, UniswapV3Config{
		PoolAddress:              common.HexToAddress("0x1111111111111111111111111111111111111111"),
		TokenIn:                  tokenIn,
		TokenOut:                 tokenOut,
		TWAPWindowSeconds:        uint32(config.MinUniswapTWAPWindowSeconds),
		MinHarmonicMeanLiquidity: big.NewInt(1),
	})
	if err != nil {
		t.Fatalf("NewUniswapV3Client() error = %v", err)
	}
	return ConfiguredPriceReader{Name: "uniswap", Reader: reader}
}

type blockingSourceConfigurationReader struct{}

func (blockingSourceConfigurationReader) PriceUSD(context.Context) (SourcePrice, error) {
	return SourcePrice{}, errors.New("price read is not used by source configuration validation")
}

func (blockingSourceConfigurationReader) validateSourceConfiguration(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
