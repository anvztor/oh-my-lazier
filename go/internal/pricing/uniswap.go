package pricing

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/islishude/oh-my-lazier/go/internal/abiutil"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

var (
	//go:embed abis/uniswap_v3_pool.json
	uniswapV3PoolABIJSON string
	//go:embed abis/erc20_metadata.json
	erc20MetadataABIJSON string

	uniswapV3PoolABI = abiutil.MustParse(uniswapV3PoolABIJSON)
	erc20MetadataABI = abiutil.MustParse(erc20MetadataABIJSON)
)

const (
	minUniswapV3Tick = -887272
	maxUniswapV3Tick = 887272
)

// CallContractReader reads EVM call results, by number or pinned block hash.
type CallContractReader interface {
	CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	CallContractAtHash(ctx context.Context, call ethereum.CallMsg, blockHash common.Hash) ([]byte, error)
}

// HeaderReader reads EVM block headers.
type HeaderReader interface {
	HeaderByNumber(ctx context.Context, number *big.Int) (*gethtypes.Header, error)
}

// UniswapV3Client reads native/USD sanity prices from a configured V3 pool TWAP.
type UniswapV3Client struct {
	caller       CallContractReader
	headers      HeaderReader
	pool         common.Address
	tokenIn      common.Address
	tokenOut     common.Address
	window       uint32
	minLiquidity *big.Int
}

// UniswapV3Config configures one native-token to stablecoin V3 pool TWAP.
type UniswapV3Config struct {
	PoolAddress              common.Address
	TokenIn                  common.Address
	TokenOut                 common.Address
	TWAPWindowSeconds        uint32
	MinHarmonicMeanLiquidity *big.Int
}

// NewUniswapV3Client creates a Uniswap V3 pool TWAP-backed price client.
func NewUniswapV3Client(caller CallContractReader, headers HeaderReader, cfg UniswapV3Config) (*UniswapV3Client, error) {
	if caller == nil || headers == nil {
		return nil, errors.New("uniswap caller and header reader are required")
	}
	if cfg.PoolAddress == (common.Address{}) {
		return nil, errors.New("uniswap pool address is required")
	}
	if cfg.TokenIn == (common.Address{}) || cfg.TokenOut == (common.Address{}) || cfg.TokenIn == cfg.TokenOut {
		return nil, errors.New("uniswap token addresses must be non-zero and distinct")
	}
	if uint64(cfg.TWAPWindowSeconds) < config.MinUniswapTWAPWindowSeconds {
		return nil, fmt.Errorf("uniswap twap window must be at least %d seconds", config.MinUniswapTWAPWindowSeconds)
	}
	if cfg.MinHarmonicMeanLiquidity == nil || cfg.MinHarmonicMeanLiquidity.Sign() <= 0 {
		return nil, errors.New("uniswap minimum harmonic mean liquidity must be positive")
	}
	return &UniswapV3Client{
		caller:       caller,
		headers:      headers,
		pool:         cfg.PoolAddress,
		tokenIn:      cfg.TokenIn,
		tokenOut:     cfg.TokenOut,
		window:       cfg.TWAPWindowSeconds,
		minLiquidity: new(big.Int).Set(cfg.MinHarmonicMeanLiquidity),
	}, nil
}

// PriceUSD fetches a native-token USD sanity price from the configured V3 pool TWAP.
func (c *UniswapV3Client) PriceUSD(ctx context.Context) (SourcePrice, error) {
	header, err := c.headers.HeaderByNumber(ctx, nil)
	if err != nil {
		return SourcePrice{}, wrapPriceSourceRequestError("uniswap", "header", err)
	}
	if header == nil || header.Time == 0 {
		return SourcePrice{}, errors.New("uniswap returned invalid latest block header")
	}
	// Every read in this observation anchors to the canonical header fetched
	// above — pinned by the header's own hash, so one PriceUSD stays a single
	// consistent state view even across a tip reorg, and the quorum client
	// does not re-establish a head anchor per call.
	anchor := &rpcquorum.StateAnchor{Number: header.Number, Hash: header.Hash()}
	if err := c.validatePoolTokensAt(ctx, anchor); err != nil {
		return SourcePrice{}, err
	}
	inDecimals, err := c.readDecimalsAt(ctx, c.tokenIn, anchor)
	if err != nil {
		return SourcePrice{}, err
	}
	outDecimals, err := c.readDecimalsAt(ctx, c.tokenOut, anchor)
	if err != nil {
		return SourcePrice{}, err
	}
	meanTick, harmonicLiquidity, err := c.observeAt(ctx, anchor)
	if err != nil {
		return SourcePrice{}, err
	}
	if harmonicLiquidity.Cmp(c.minLiquidity) < 0 {
		return SourcePrice{}, fmt.Errorf("uniswap harmonic mean liquidity %s is below minimum %s", harmonicLiquidity, c.minLiquidity)
	}
	baseAmount := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(inDecimals)), nil)
	quoteAmount, err := quoteAtTick(meanTick, baseAmount, c.tokenIn, c.tokenOut)
	if err != nil {
		return SourcePrice{}, err
	}
	if quoteAmount.Sign() <= 0 {
		return SourcePrice{}, errors.New("uniswap twap returned non-positive quote")
	}
	price := new(big.Rat).SetInt(quoteAmount)
	price.Quo(price, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(outDecimals)), nil)))
	return SourcePrice{Source: "uniswap", USD: price, ObservedAt: time.Unix(int64(header.Time), 0)}, nil
}

func (c *UniswapV3Client) validateSourceConfiguration(ctx context.Context) error {
	if err := c.validatePoolTokens(ctx); err != nil {
		return err
	}
	if _, _, err := c.observe(ctx); err != nil {
		if isPriceSourceContractRevertReason(err, "OLD") {
			return newPriceSourceConfigurationError(fmt.Errorf("uniswap pool observation history is shorter than the configured %d-second twap window", c.window))
		}
		return err
	}
	return nil
}

func (c *UniswapV3Client) validatePoolTokens(ctx context.Context) error {
	return c.validatePoolTokensAt(ctx, nil)
}

func (c *UniswapV3Client) validatePoolTokensAt(ctx context.Context, anchor *rpcquorum.StateAnchor) error {
	token0, err := c.readAddress(ctx, c.pool, uniswapV3PoolABI, "token0", anchor)
	if err != nil {
		return classifySourceIdentityCallError("uniswap token0 response is incompatible with the pool ABI", err)
	}
	token1, err := c.readAddress(ctx, c.pool, uniswapV3PoolABI, "token1", anchor)
	if err != nil {
		return classifySourceIdentityCallError("uniswap token1 response is incompatible with the pool ABI", err)
	}
	directOrder := token0 == c.tokenIn && token1 == c.tokenOut
	reverseOrder := token0 == c.tokenOut && token1 == c.tokenIn
	if !directOrder && !reverseOrder {
		return newPriceSourceConfigurationError(errors.New("uniswap pool tokens do not match configured token pair"))
	}
	return nil
}

func (c *UniswapV3Client) observe(ctx context.Context) (int, *big.Int, error) {
	return c.observeAt(ctx, nil)
}

func (c *UniswapV3Client) observeAt(ctx context.Context, anchor *rpcquorum.StateAnchor) (int, *big.Int, error) {
	calldata, err := uniswapV3PoolABI.Pack("observe", []uint32{c.window, 0})
	if err != nil {
		return 0, nil, err
	}
	result, err := rpcquorum.CallAtAnchor(ctx, c.caller, ethereum.CallMsg{To: &c.pool, Data: calldata}, anchor)
	if err != nil {
		return 0, nil, wrapPriceSourceRequestError("uniswap", "observe", err)
	}
	decoded, err := uniswapV3PoolABI.Unpack("observe", result)
	if err != nil {
		return 0, nil, err
	}
	if len(decoded) != 2 {
		return 0, nil, fmt.Errorf("uniswap observe returned %d values", len(decoded))
	}
	ticks, ok := decoded[0].([]*big.Int)
	if !ok || len(ticks) != 2 {
		return 0, nil, errors.New("uniswap observe returned invalid tick cumulatives")
	}
	secondsPerLiquidity, ok := decoded[1].([]*big.Int)
	if !ok || len(secondsPerLiquidity) != 2 {
		return 0, nil, errors.New("uniswap observe returned invalid liquidity cumulatives")
	}
	tickDelta := new(big.Int).Sub(ticks[1], ticks[0])
	window := new(big.Int).SetUint64(uint64(c.window))
	mean, remainder := new(big.Int), new(big.Int)
	mean.QuoRem(tickDelta, window, remainder)
	if tickDelta.Sign() < 0 && remainder.Sign() != 0 {
		mean.Sub(mean, big.NewInt(1))
	}
	if !mean.IsInt64() || mean.Int64() < minUniswapV3Tick || mean.Int64() > maxUniswapV3Tick {
		return 0, nil, errors.New("uniswap observe returned out-of-range mean tick")
	}
	liquidityDelta := new(big.Int).Sub(secondsPerLiquidity[1], secondsPerLiquidity[0])
	if liquidityDelta.Sign() <= 0 {
		return 0, nil, errors.New("uniswap observe returned invalid liquidity delta")
	}
	maxUint160 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 160), big.NewInt(1))
	numerator := new(big.Int).Mul(window, maxUint160)
	denominator := new(big.Int).Lsh(liquidityDelta, 32)
	harmonicLiquidity := new(big.Int).Quo(numerator, denominator)
	if harmonicLiquidity.Sign() <= 0 {
		return 0, nil, errors.New("uniswap observe returned non-positive harmonic liquidity")
	}
	return int(mean.Int64()), harmonicLiquidity, nil
}

func (c *UniswapV3Client) readDecimalsAt(ctx context.Context, token common.Address, anchor *rpcquorum.StateAnchor) (uint8, error) {
	values, err := c.call(ctx, token, erc20MetadataABI, "decimals", anchor)
	if err != nil {
		return 0, err
	}
	if len(values) != 1 {
		return 0, errors.New("erc20 decimals returned invalid result")
	}
	decimals, ok := values[0].(uint8)
	if !ok || decimals > 36 {
		return 0, errors.New("erc20 returned unsupported decimals")
	}
	return decimals, nil
}

func (c *UniswapV3Client) readAddress(ctx context.Context, target common.Address, contractABI interface {
	Pack(string, ...any) ([]byte, error)
	Unpack(string, []byte) ([]any, error)
}, method string, anchor *rpcquorum.StateAnchor) (common.Address, error) {
	values, err := c.call(ctx, target, contractABI, method, anchor)
	if err != nil {
		return common.Address{}, err
	}
	if len(values) != 1 {
		return common.Address{}, fmt.Errorf("uniswap %s returned invalid result", method)
	}
	address, ok := values[0].(common.Address)
	if !ok || address == (common.Address{}) {
		return common.Address{}, fmt.Errorf("uniswap %s returned invalid address", method)
	}
	return address, nil
}

func (c *UniswapV3Client) call(ctx context.Context, target common.Address, contractABI interface {
	Pack(string, ...any) ([]byte, error)
	Unpack(string, []byte) ([]any, error)
}, method string, anchor *rpcquorum.StateAnchor) ([]any, error) {
	calldata, err := contractABI.Pack(method)
	if err != nil {
		return nil, err
	}
	result, err := rpcquorum.CallAtAnchor(ctx, c.caller, ethereum.CallMsg{To: &target, Data: calldata}, anchor)
	if err != nil {
		return nil, wrapPriceSourceRequestError("uniswap", method, err)
	}
	return contractABI.Unpack(method, result)
}

func quoteAtTick(tick int, baseAmount *big.Int, baseToken, quoteToken common.Address) (*big.Int, error) {
	if baseAmount == nil || baseAmount.Sign() <= 0 {
		return nil, errors.New("uniswap base amount must be positive")
	}
	sqrtRatioX96, err := sqrtRatioAtTick(tick)
	if err != nil {
		return nil, err
	}
	ratioX192 := new(big.Int).Mul(sqrtRatioX96, sqrtRatioX96)
	twoX192 := new(big.Int).Lsh(big.NewInt(1), 192)
	if bytes.Compare(baseToken[:], quoteToken[:]) < 0 {
		return new(big.Int).Quo(new(big.Int).Mul(ratioX192, baseAmount), twoX192), nil
	}
	return new(big.Int).Quo(new(big.Int).Mul(twoX192, baseAmount), ratioX192), nil
}

func sqrtRatioAtTick(tick int) (*big.Int, error) {
	if tick < minUniswapV3Tick || tick > maxUniswapV3Tick {
		return nil, errors.New("uniswap tick is out of range")
	}
	absTick := tick
	if absTick < 0 {
		absTick = -absTick
	}
	ratio := new(big.Int)
	if absTick&1 != 0 {
		ratio.Set(mustBigHex("fffcb933bd6fad37aa2d162d1a594001"))
	} else {
		ratio.Lsh(big.NewInt(1), 128)
	}
	for _, factor := range []struct {
		mask int
		hex  string
	}{
		{0x2, "fff97272373d413259a46990580e213a"}, {0x4, "fff2e50f5f656932ef12357cf3c7fdcc"},
		{0x8, "ffe5caca7e10e4e61c3624eaa0941cd0"}, {0x10, "ffcb9843d60f6159c9db58835c926644"},
		{0x20, "ff973b41fa98c081472e6896dfb254c0"}, {0x40, "ff2ea16466c96a3843ec78b326b52861"},
		{0x80, "fe5dee046a99a2a811c461f1969c3053"}, {0x100, "fcbe86c7900a88aedcffc83b479aa3a4"},
		{0x200, "f987a7253ac413176f2b074cf7815e54"}, {0x400, "f3392b0822b70005940c7a398e4b70f3"},
		{0x800, "e7159475a2c29b7443b29c7fa6e889d9"}, {0x1000, "d097f3bdfd2022b8845ad8f792aa5825"},
		{0x2000, "a9f746462d870fdf8a65dc1f90e061e5"}, {0x4000, "70d869a156d2a1b890bb3df62baf32f7"},
		{0x8000, "31be135f97d08fd981231505542fcfa6"}, {0x10000, "9aa508b5b7a84e1c677de54f3e99bc9"},
		{0x20000, "5d6af8dedb81196699c329225ee604"}, {0x40000, "2216e584f5fa1ea926041bedfe98"},
		{0x80000, "48a170391f7dc42444e8fa2"},
	} {
		if absTick&factor.mask != 0 {
			ratio.Mul(ratio, mustBigHex(factor.hex))
			ratio.Rsh(ratio, 128)
		}
	}
	if tick > 0 {
		maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
		ratio.Quo(maxUint256, ratio)
	}
	remainderMask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 32), big.NewInt(1))
	hasRemainder := new(big.Int).And(new(big.Int).Set(ratio), remainderMask).Sign() != 0
	ratio.Rsh(ratio, 32)
	if hasRemainder {
		ratio.Add(ratio, big.NewInt(1))
	}
	return ratio, nil
}

func mustBigHex(value string) *big.Int {
	parsed, ok := new(big.Int).SetString(value, 16)
	if !ok {
		panic("invalid embedded hexadecimal integer")
	}
	return parsed
}
