package configcheck

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/abiutil"
	"github.com/islishude/oh-my-lazier/go/internal/bigutil"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

const (
	configTypeExecutor = uint32(1)
	configTypeULN      = uint32(2)
	nilDVNCount        = uint8(255)
)

var (
	endpointABI      = abiutil.MustParse(endpointABIJSON)
	oappABI          = abiutil.MustParse(oappABIJSON)
	workerABI        = abiutil.MustParse(workerABIJSON)
	configDecoderABI = abiutil.MustParse(configDecoderABIJSON)
)

//go:embed abis/endpoint.json
var endpointABIJSON string

//go:embed abis/oapp.json
var oappABIJSON string

//go:embed abis/worker.json
var workerABIJSON string

//go:embed abis/config_decoder.json
var configDecoderABIJSON string

// ChainClient is the on-chain read surface required by the config checker.
type ChainClient interface {
	CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error)
	ChainID(context.Context) (*big.Int, error)
	CodeAt(context.Context, common.Address, *big.Int) ([]byte, error)
}

type chainIDValidator interface {
	ValidateChainID(context.Context, *big.Int) error
}

// anchoredChainClient pins nil-block reads to one verified canonical anchor
// so every read in a chain's config check shares a single consistent state
// view. When the underlying client supports hash addressing, the anchor's
// block hash pins the reads to the verified branch even across a tip reorg.
type anchoredChainClient struct {
	ChainClient
	block *big.Int
	hash  common.Hash
}

type hashAddressedClient interface {
	CallContractAtHash(ctx context.Context, call ethereum.CallMsg, blockHash common.Hash) ([]byte, error)
	CodeAtHash(ctx context.Context, account common.Address, blockHash common.Hash) ([]byte, error)
}

// CallContract substitutes the verified anchor for nil block numbers.
func (a anchoredChainClient) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	if blockNumber == nil {
		if hashed, ok := a.ChainClient.(hashAddressedClient); ok && a.hash != (common.Hash{}) {
			return hashed.CallContractAtHash(ctx, call, a.hash)
		}
		blockNumber = a.block
	}
	return a.ChainClient.CallContract(ctx, call, blockNumber)
}

// CodeAt substitutes the verified anchor for nil block numbers.
func (a anchoredChainClient) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	if blockNumber == nil {
		if hashed, ok := a.ChainClient.(hashAddressedClient); ok && a.hash != (common.Hash{}) {
			return hashed.CodeAtHash(ctx, account, a.hash)
		}
		blockNumber = a.block
	}
	return a.ChainClient.CodeAt(ctx, account, blockNumber)
}

// Issue describes one config mismatch against on-chain state.
type Issue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// Report is the complete on-chain config check result.
type Report struct {
	OK     bool    `json:"ok"`
	Issues []Issue `json:"issues"`
}

func (r Report) issueLabel() string {
	if len(r.Issues) == 1 {
		return "issue"
	}
	return "issues"
}

// RenderText renders a report for operator runbooks and startup errors.
func RenderText(report Report) string {
	if report.OK {
		return "on-chain config check passed\n"
	}
	var out strings.Builder
	fmt.Fprintf(&out, "on-chain config check failed (%d %s)\n", len(report.Issues), report.issueLabel())
	for i, issue := range report.Issues {
		fmt.Fprintf(&out, "[%d] %s\n", i+1, issue.Path)
		fmt.Fprintf(&out, "    %s\n", issue.Message)
	}
	return out.String()
}

// Check validates a registry against the registry's configured RPC clients.
func Check(ctx context.Context, registry *chain.Registry, opts ...Option) (Report, error) {
	clients := make(map[uint32]ChainClient)
	for _, configured := range registry.All() {
		clients[configured.EID] = configured.RPC
	}
	return CheckWithClients(ctx, registry, clients, opts...)
}

// CheckWithClients validates a registry against supplied chain clients.
func CheckWithClients(ctx context.Context, registry *chain.Registry, clients map[uint32]ChainClient, opts ...Option) (Report, error) {
	if registry == nil {
		return Report{}, errors.New("registry is required")
	}
	checkOptions := applyOptions(opts)
	checker := checker{registry: registry, clients: clients, pricingSigner: checkOptions.pricingSigner}
	if err := checker.run(ctx); err != nil {
		return Report{}, err
	}
	return Report{OK: len(checker.issues) == 0, Issues: checker.issues}, nil
}

// Option configures optional config checks that are not encoded in the registry.
type Option func(*options)

type options struct {
	pricingSigner common.Address
}

// WithPricingSigner requires each configured source OpenPriceFeed to authorize the pricing signer.
func WithPricingSigner(signer common.Address) Option {
	return func(options *options) {
		options.pricingSigner = signer
	}
}

func applyOptions(opts []Option) options {
	var result options
	for _, opt := range opts {
		if opt != nil {
			opt(&result)
		}
	}
	return result
}

type checker struct {
	registry      *chain.Registry
	clients       map[uint32]ChainClient
	pricingSigner common.Address
	invalidChains map[uint32]struct{}
	issues        []Issue
}

func (c *checker) run(ctx context.Context) error {
	c.invalidChains = make(map[uint32]struct{})
	for _, configured := range c.registry.All() {
		client, err := c.client(configured.EID)
		if err != nil {
			return err
		}
		if err := c.checkChain(ctx, client, configured); err != nil {
			return err
		}
	}
	for _, pathway := range c.registry.Pathways() {
		if c.chainInvalid(pathway.SrcEID) || c.chainInvalid(pathway.DstEID) {
			continue
		}
		if err := c.checkPathway(ctx, pathway); err != nil {
			return err
		}
	}
	return nil
}

func (c *checker) checkChain(ctx context.Context, client ChainClient, configured chain.Chain) error {
	if validator, ok := client.(chainIDValidator); ok {
		if err := validator.ValidateChainID(ctx, configured.ChainID); err != nil {
			if rpcquorum.IsChainIDMismatch(err) {
				c.add(fmt.Sprintf("chains[%d].rpc_urls", configured.EID), "%s", err)
				c.markChainInvalid(configured.EID)
				return nil
			}
			return fmt.Errorf("validate chain %d rpc chain_id: %w", configured.EID, err)
		}
	}
	// Establish the quorum head before the first on-chain config read, so the
	// provider trust state is set by the configured majority rather than by
	// whichever provider happens to be listed first. The verified head also
	// anchors every subsequent read for this chain: dozens of serial config
	// reads must not each re-establish a head quorum (a black-holed minority
	// provider would charge every read a probe deadline), and one immutable
	// height keeps the whole check a single consistent state view.
	if headChecker, ok := client.(interface {
		CheckHead(context.Context) (rpcquorum.HeadResult, error)
	}); ok {
		head, err := headChecker.CheckHead(ctx)
		if err != nil {
			return fmt.Errorf("check chain %d rpc head quorum: %w", configured.EID, err)
		}
		if head.Number != nil {
			c.clients[configured.EID] = anchoredChainClient{ChainClient: client, block: new(big.Int).Set(head.Number), hash: common.HexToHash(head.Hash)}
			client = c.clients[configured.EID]
		}
	}
	actualChainID, err := client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("read chain %d chain_id: %w", configured.EID, err)
	}
	if actualChainID == nil || actualChainID.Cmp(configured.ChainID) != 0 {
		c.add(fmt.Sprintf("chains[%d].chain_id", configured.EID), "on-chain chain_id %s does not match configured %s", bigString(actualChainID), configured.ChainID)
		c.markChainInvalid(configured.EID)
		return nil
	}
	actualEID, err := callUint32(ctx, client, endpointABI, configured.EndpointAddress, "eid")
	if err != nil {
		return fmt.Errorf("read chain %d endpoint eid: %w", configured.EID, err)
	}
	if actualEID != configured.EID {
		c.add(fmt.Sprintf("chains[%d].eid", configured.EID), "endpoint eid %d does not match configured %d", actualEID, configured.EID)
	}
	contracts := map[string]common.Address{
		"endpoint_address": configured.EndpointAddress,
	}
	for label, address := range contracts {
		code, err := client.CodeAt(ctx, address, nil)
		if err != nil {
			return fmt.Errorf("read chain %d code at %s: %w", configured.EID, address, err)
		}
		if len(code) == 0 {
			c.add(fmt.Sprintf("chains[%d].%s", configured.EID, label), "no contract code at %s", address)
		}
	}
	return nil
}

func (c *checker) checkPathway(ctx context.Context, pathway chain.Pathway) error {
	srcChain, err := c.registry.Get(pathway.SrcEID)
	if err != nil {
		return err
	}
	dstChain, err := c.registry.Get(pathway.DstEID)
	if err != nil {
		return err
	}
	srcClient, err := c.client(pathway.SrcEID)
	if err != nil {
		return err
	}
	dstClient, err := c.client(pathway.DstEID)
	if err != nil {
		return err
	}
	base := fmt.Sprintf("pathways[%d:%d:%s:%s]", pathway.SrcEID, pathway.DstEID, pathway.SrcOApp, pathway.DstOApp)
	if err := c.checkOApp(ctx, srcClient, base+".src_oapp", pathway.SrcOApp, srcChain.EndpointAddress, pathway.DstEID, pathway.DstOApp); err != nil {
		return err
	}
	if err := c.checkOApp(ctx, dstClient, base+".dst_oapp", pathway.DstOApp, dstChain.EndpointAddress, pathway.SrcEID, pathway.SrcOApp); err != nil {
		return err
	}
	if err := c.checkLibraries(ctx, srcClient, dstClient, base, srcChain, dstChain, pathway); err != nil {
		return err
	}
	if err := c.checkWorkers(ctx, srcClient, dstClient, base, srcChain, dstChain, pathway); err != nil {
		return err
	}
	return nil
}

func (c *checker) checkOApp(ctx context.Context, client ChainClient, path string, oapp, endpoint common.Address, remoteEID uint32, remoteOApp common.Address) error {
	code, err := client.CodeAt(ctx, oapp, nil)
	if err != nil {
		return fmt.Errorf("read code at OApp %s: %w", oapp, err)
	}
	if len(code) == 0 {
		c.add(path, "no contract code at %s", oapp)
	}
	actualEndpoint, err := callAddress(ctx, client, oappABI, oapp, "endpoint")
	if err != nil {
		return fmt.Errorf("read %s endpoint: %w", oapp, err)
	}
	if actualEndpoint != endpoint {
		c.add(path+".endpoint", "oapp endpoint %s does not match configured endpoint %s", actualEndpoint, endpoint)
	}
	peer, err := callHash(ctx, client, oappABI, oapp, "peers", remoteEID)
	if err != nil {
		return fmt.Errorf("read %s peer %d: %w", oapp, remoteEID, err)
	}
	expectedPeer := common.BytesToHash(remoteOApp.Bytes())
	if peer != expectedPeer {
		c.add(path+".peers", "peer for eid %d is %s, want %s", remoteEID, peer, expectedPeer)
	}
	return nil
}

func (c *checker) checkLibraries(ctx context.Context, srcClient, dstClient ChainClient, base string, srcChain, dstChain chain.Chain, pathway chain.Pathway) error {
	if err := c.requireCode(ctx, srcClient, base+".send_lib", pathway.SrcEID, pathway.SendLib); err != nil {
		return err
	}
	if err := c.requireCode(ctx, dstClient, base+".receive_lib", pathway.DstEID, pathway.ReceiveLib); err != nil {
		return err
	}
	sendLib, err := callAddress(ctx, srcClient, endpointABI, srcChain.EndpointAddress, "getSendLibrary", pathway.SrcOApp, pathway.DstEID)
	if err != nil {
		return fmt.Errorf("read send library for %s: %w", base, err)
	}
	if sendLib != pathway.SendLib {
		c.add(base+".send_lib", "endpoint send library %s does not match configured %s", sendLib, pathway.SendLib)
	}
	receiveValues, err := callValues(ctx, dstClient, endpointABI, dstChain.EndpointAddress, "getReceiveLibrary", pathway.DstOApp, pathway.SrcEID)
	if err != nil {
		return fmt.Errorf("read receive library for %s: %w", base, err)
	}
	receiveLib, ok := receiveValues[0].(common.Address)
	if !ok {
		return fmt.Errorf("getReceiveLibrary returned %T, want address", receiveValues[0])
	}
	if receiveLib != pathway.ReceiveLib {
		c.add(base+".receive_lib", "endpoint receive library %s does not match configured %s", receiveLib, pathway.ReceiveLib)
	}
	executorConfigBytes, err := callBytes(ctx, srcClient, endpointABI, srcChain.EndpointAddress, "getConfig", pathway.SrcOApp, pathway.SendLib, pathway.DstEID, configTypeExecutor)
	if err != nil {
		return fmt.Errorf("read executor config for %s: %w", base, err)
	}
	executorConfig, err := decodeExecutorConfig(executorConfigBytes)
	if err != nil {
		return fmt.Errorf("decode executor config for %s: %w", base, err)
	}
	if executorConfig.Executor != pathway.SourceWorkers.OpenExecutor {
		c.add(base+".executor_config.executor", "executor config points to %s, want %s", executorConfig.Executor, pathway.SourceWorkers.OpenExecutor)
	}
	if uint64(executorConfig.MaxMessageSize) != pathway.MaxMessageSize {
		c.add(base+".executor_config.max_message_size", "executor max message size %d does not match configured %d", executorConfig.MaxMessageSize, pathway.MaxMessageSize)
	}
	sendULNConfig, err := c.readULNConfig(ctx, srcClient, srcChain.EndpointAddress, pathway.SrcOApp, pathway.SendLib, pathway.DstEID, base+".send_uln_config")
	if err != nil {
		return err
	}
	c.compareULNConfig(base+".send_uln_config", sendULNConfig, srcChain.Confirmations, pathway.SendRequiredDVNs)
	receiveULNConfig, err := c.readULNConfig(ctx, dstClient, dstChain.EndpointAddress, pathway.DstOApp, pathway.ReceiveLib, pathway.SrcEID, base+".receive_uln_config")
	if err != nil {
		return err
	}
	c.compareULNConfig(base+".receive_uln_config", receiveULNConfig, dstChain.Confirmations, pathway.ReceiveRequiredDVNs)
	// ReceiveUln302 rejects verifications whose assigned confirmations fall below its own
	// threshold, and DVN jobs are assigned the send-side value, so this relationship must
	// hold on chain regardless of what either side was configured to match.
	if sendULNConfig.Confirmations < receiveULNConfig.Confirmations {
		c.add(
			base+".uln_confirmations",
			"send uln confirmations %d are below receive uln confirmations %d; every DVN verification for this pathway would be rejected",
			sendULNConfig.Confirmations, receiveULNConfig.Confirmations,
		)
	}
	return nil
}

func (c *checker) checkWorkers(ctx context.Context, srcClient, dstClient ChainClient, base string, srcChain, dstChain chain.Chain, pathway chain.Pathway) error {
	if err := c.requireCode(ctx, srcClient, base+".source_workers.price_feed", srcChain.EID, pathway.SourceWorkers.PriceFeed); err != nil {
		return err
	}
	if c.pricingSigner != (common.Address{}) {
		allowed, err := callBool(ctx, srcClient, workerABI, pathway.SourceWorkers.PriceFeed, "submitters", c.pricingSigner)
		if err != nil {
			return fmt.Errorf("read price feed submitter for %s: %w", base, err)
		}
		if !allowed {
			c.add(base+".source_workers.price_feed.submitter", "price feed does not authorize pricing signer %s", c.pricingSigner)
		}
	}
	workers := []struct {
		label string
		addr  common.Address
		fee   config.WorkerFeeModelConfig
	}{
		{label: "open_executor", addr: pathway.SourceWorkers.OpenExecutor, fee: pathway.Pricing.ExecutorFee},
		{label: "open_dvn", addr: pathway.SourceWorkers.OpenDVN, fee: pathway.Pricing.DVNFee},
	}
	for _, selected := range workers {
		label := selected.label
		worker := selected.addr
		if err := c.requireCode(ctx, srcClient, base+".source_workers."+label, srcChain.EID, worker); err != nil {
			return err
		}
		priceFeed, err := callAddress(ctx, srcClient, workerABI, worker, "priceFeed")
		if err != nil {
			return fmt.Errorf("read %s priceFeed for %s: %w", label, base, err)
		}
		if priceFeed != pathway.SourceWorkers.PriceFeed {
			c.add(base+".source_workers."+label+".price_feed", "%s priceFeed %s does not match configured %s", label, priceFeed, pathway.SourceWorkers.PriceFeed)
		}
		allowed, err := callBool(ctx, srcClient, workerABI, worker, "allowedSendLib", pathway.SendLib)
		if err != nil {
			return fmt.Errorf("read %s allowedSendLib for %s: %w", label, base, err)
		}
		if !allowed {
			c.add(base+".source_workers."+label+".allowed_send_lib", "%s does not allow send lib %s", label, pathway.SendLib)
		}
		config, err := callPathwayConfig(ctx, srcClient, worker, pathway.DstEID, pathway.SrcOApp)
		if err != nil {
			return fmt.Errorf("read %s pathwayConfig for %s: %w", label, base, err)
		}
		if config.Enabled != pathway.Enabled {
			c.add(base+".source_workers."+label+".enabled", "worker enabled %t does not match configured %t", config.Enabled, pathway.Enabled)
		}
		if config.MaxMessageSize == nil || config.MaxMessageSize.Uint64() != pathway.MaxMessageSize {
			c.add(base+".source_workers."+label+".max_message_size", "worker max message size %s does not match configured %d", bigString(config.MaxMessageSize), pathway.MaxMessageSize)
		}
		if config.MinLzReceiveGas == nil || config.MinLzReceiveGas.Uint64() != pathway.MinLzReceiveGas {
			c.add(base+".source_workers."+label+".min_lz_receive_gas", "worker min lz receive gas %s does not match configured %d", bigString(config.MinLzReceiveGas), pathway.MinLzReceiveGas)
		}
		if config.MaxLzReceiveGas == nil || config.MaxLzReceiveGas.Uint64() != pathway.MaxLzReceiveGas {
			c.add(base+".source_workers."+label+".max_lz_receive_gas", "worker max lz receive gas %s does not match configured %d", bigString(config.MaxLzReceiveGas), pathway.MaxLzReceiveGas)
		}
		if selected.fee.FixedFeeWei != "" {
			actualFee, err := callFeeModel(ctx, srcClient, worker, pathway.DstEID)
			if err != nil {
				return fmt.Errorf("read %s feeModel for %s: %w", label, base, err)
			}
			c.compareFeeModel(base+".source_workers."+label+".fee_model", actualFee, selected.fee)
		}
	}
	if err := c.requireCode(ctx, dstClient, base+".destination_workers.open_dvn", dstChain.EID, pathway.DestinationWorkers.OpenDVN); err != nil {
		return err
	}
	if pathway.DVNMode == "active" {
		if dstChain.TxRoles.DVN.SignerID == "" {
			c.add(base+".destination_workers.open_dvn.verifiers", "destination chain dvn signer is required for active dvn pathways")
			return nil
		}
		verifier := common.HexToAddress(dstChain.TxRoles.DVN.SignerID)
		allowed, err := callBool(ctx, dstClient, workerABI, pathway.DestinationWorkers.OpenDVN, "verifiers", verifier)
		if err != nil {
			return fmt.Errorf("read destination open_dvn verifier authorization for %s: %w", base, err)
		}
		if !allowed {
			c.add(base+".destination_workers.open_dvn.verifiers", "dvn signer %s is not authorized on destination OpenDVN %s", verifier, pathway.DestinationWorkers.OpenDVN)
		}
	}
	return nil
}

func (c *checker) readULNConfig(ctx context.Context, client ChainClient, endpoint, oapp, library common.Address, remoteEID uint32, path string) (ulnConfig, error) {
	configBytes, err := callBytes(ctx, client, endpointABI, endpoint, "getConfig", oapp, library, remoteEID, configTypeULN)
	if err != nil {
		return ulnConfig{}, fmt.Errorf("read %s: %w", path, err)
	}
	config, err := decodeULNConfig(configBytes)
	if err != nil {
		return ulnConfig{}, fmt.Errorf("decode %s: %w", path, err)
	}
	return config, nil
}

func (c *checker) compareULNConfig(path string, config ulnConfig, confirmations uint64, requiredDVNs []common.Address) {
	if config.Confirmations != confirmations {
		c.add(path+".confirmations", "confirmations %d does not match configured %d", config.Confirmations, confirmations)
	}
	if config.RequiredDVNCount != uint8(len(config.RequiredDVNs)) {
		c.add(path+".required_dvn_count", "requiredDVNCount %d does not match requiredDVNs length %d", config.RequiredDVNCount, len(config.RequiredDVNs))
	}
	if config.OptionalDVNCount != 0 && config.OptionalDVNCount != nilDVNCount {
		c.add(path+".optional_dvn_count", "optionalDVNCount %d is not disabled", config.OptionalDVNCount)
	}
	if config.OptionalDVNThreshold != 0 {
		c.add(path+".optional_dvn_threshold", "optionalDVNThreshold %d is not zero", config.OptionalDVNThreshold)
	}
	if len(config.OptionalDVNs) != 0 {
		c.add(path+".optional_dvns", "optional DVNs are configured: %s", addressesString(config.OptionalDVNs))
	}
	// An unapproved, stale, or missing entry silently changes the pathway's verification
	// quorum, so the on-chain set must match the approved set exactly (order ignored).
	if !equalAddressSets(config.RequiredDVNs, requiredDVNs) {
		c.add(path+".required_dvns", "required DVNs %s do not match the configured required set %s", addressesString(config.RequiredDVNs), addressesString(requiredDVNs))
	}
}

// equalAddressSets reports whether both slices hold the same addresses ignoring order.
func equalAddressSets(a, b []common.Address) bool {
	if len(a) != len(b) {
		return false
	}
	sortedA := slices.Clone(a)
	sortedB := slices.Clone(b)
	compare := func(x, y common.Address) int { return bytes.Compare(x.Bytes(), y.Bytes()) }
	slices.SortFunc(sortedA, compare)
	slices.SortFunc(sortedB, compare)
	return slices.Equal(sortedA, sortedB)
}

func (c *checker) requireCode(ctx context.Context, client ChainClient, path string, eid uint32, address common.Address) error {
	code, err := client.CodeAt(ctx, address, nil)
	if err != nil {
		return fmt.Errorf("read chain %d code at %s: %w", eid, address, err)
	}
	if len(code) == 0 {
		c.add(path, "no contract code at %s", address)
	}
	return nil
}

func (c *checker) client(eid uint32) (ChainClient, error) {
	client := c.clients[eid]
	if client == nil {
		return nil, fmt.Errorf("chain %d client is required", eid)
	}
	return client, nil
}

func (c *checker) add(path, format string, args ...any) {
	c.issues = append(c.issues, Issue{Path: path, Message: fmt.Sprintf(format, args...)})
}

func (c *checker) markChainInvalid(eid uint32) {
	if c.invalidChains == nil {
		c.invalidChains = make(map[uint32]struct{})
	}
	c.invalidChains[eid] = struct{}{}
}

func (c *checker) chainInvalid(eid uint32) bool {
	_, ok := c.invalidChains[eid]
	return ok
}

type executorConfig struct {
	MaxMessageSize uint32
	Executor       common.Address
}

type ulnConfig struct {
	Confirmations        uint64
	RequiredDVNCount     uint8
	OptionalDVNCount     uint8
	OptionalDVNThreshold uint8
	RequiredDVNs         []common.Address
	OptionalDVNs         []common.Address
}

type pathwayConfig struct {
	Enabled         bool
	MaxMessageSize  *big.Int
	MinLzReceiveGas *big.Int
	MaxLzReceiveGas *big.Int
}

type feeModel struct {
	FixedFee              *big.Int
	DstGasOverhead        uint64
	DataSizeOverheadBytes uint64
	MarginBps             uint16
}

func callPathwayConfig(ctx context.Context, caller ChainClient, to common.Address, dstEID uint32, sender common.Address) (pathwayConfig, error) {
	values, err := callValues(ctx, caller, workerABI, to, "pathwayConfig", dstEID, sender)
	if err != nil {
		return pathwayConfig{}, err
	}
	if len(values) != 4 {
		return pathwayConfig{}, fmt.Errorf("pathwayConfig returned %d values, want 4", len(values))
	}
	enabled, ok := values[0].(bool)
	if !ok {
		return pathwayConfig{}, fmt.Errorf("pathwayConfig enabled returned %T, want bool", values[0])
	}
	maxMessageSize, ok := values[1].(*big.Int)
	if !ok {
		return pathwayConfig{}, fmt.Errorf("pathwayConfig maxMessageSize returned %T, want *big.Int", values[1])
	}
	minGas, ok := values[2].(*big.Int)
	if !ok {
		return pathwayConfig{}, fmt.Errorf("pathwayConfig minLzReceiveGas returned %T, want *big.Int", values[2])
	}
	maxGas, ok := values[3].(*big.Int)
	if !ok {
		return pathwayConfig{}, fmt.Errorf("pathwayConfig maxLzReceiveGas returned %T, want *big.Int", values[3])
	}
	return pathwayConfig{
		Enabled:         enabled,
		MaxMessageSize:  maxMessageSize,
		MinLzReceiveGas: minGas,
		MaxLzReceiveGas: maxGas,
	}, nil
}

func callFeeModel(ctx context.Context, caller ChainClient, to common.Address, dstEID uint32) (feeModel, error) {
	values, err := callValues(ctx, caller, workerABI, to, "feeModel", dstEID)
	if err != nil {
		return feeModel{}, err
	}
	if len(values) != 4 {
		return feeModel{}, fmt.Errorf("feeModel returned %d values, want 4", len(values))
	}
	abiBaseFee, ok := values[0].(*big.Int)
	if !ok {
		return feeModel{}, fmt.Errorf("feeModel baseFee returned %T, want *big.Int", values[0])
	}
	overhead, ok := uint64Value(values[1])
	if !ok {
		return feeModel{}, fmt.Errorf("feeModel dstGasOverhead returned %T, want uint64", values[1])
	}
	dataOverhead, ok := uint64Value(values[2])
	if !ok {
		return feeModel{}, fmt.Errorf("feeModel dataSizeOverheadBytes returned %T, want uint64", values[2])
	}
	margin, ok := uint16Value(values[3])
	if !ok {
		return feeModel{}, fmt.Errorf("feeModel marginBps returned %T, want uint16", values[3])
	}
	return feeModel{
		FixedFee:              abiBaseFee,
		DstGasOverhead:        overhead,
		DataSizeOverheadBytes: dataOverhead,
		MarginBps:             margin,
	}, nil
}

func (c *checker) compareFeeModel(path string, actual feeModel, expected config.WorkerFeeModelConfig) {
	expectedFixedFee, err := bigutil.ParseDecimal("configured fixed_fee_wei", expected.FixedFeeWei)
	if err != nil {
		c.add(path+".fixed_fee_wei", "configured fixed_fee_wei %q is not a decimal integer", expected.FixedFeeWei)
		return
	}
	if actual.FixedFee == nil || actual.FixedFee.Cmp(expectedFixedFee) != 0 {
		c.add(path+".fixed_fee_wei", "worker ABI baseFee %s does not match configured fixed_fee_wei %s", bigString(actual.FixedFee), expectedFixedFee)
	}
	if actual.DstGasOverhead != expected.DstGasOverhead {
		c.add(path+".dst_gas_overhead", "worker destination gas overhead %d does not match configured %d", actual.DstGasOverhead, expected.DstGasOverhead)
	}
	if expected.DataSizeOverheadBytes == nil {
		c.add(path+".data_size_overhead_bytes", "configured data_size_overhead_bytes is required")
	} else if actual.DataSizeOverheadBytes != *expected.DataSizeOverheadBytes {
		c.add(path+".data_size_overhead_bytes", "worker data size overhead %d does not match configured %d", actual.DataSizeOverheadBytes, *expected.DataSizeOverheadBytes)
	}
	if actual.MarginBps != expected.MarginBps {
		c.add(path+".margin_bps", "worker margin bps %d does not match configured %d", actual.MarginBps, expected.MarginBps)
	}
}

func decodeExecutorConfig(data []byte) (executorConfig, error) {
	values, err := configDecoderABI.Unpack("executorConfig", data)
	if err != nil {
		return executorConfig{}, err
	}
	if len(values) != 1 {
		return executorConfig{}, fmt.Errorf("executor config returned %d values, want 1", len(values))
	}
	reflected := reflect.ValueOf(values[0])
	if reflected.Kind() != reflect.Struct {
		return executorConfig{}, fmt.Errorf("executor config returned %T, want tuple struct", values[0])
	}
	executor, ok := reflected.FieldByName("Executor").Interface().(common.Address)
	if !ok {
		return executorConfig{}, fmt.Errorf("executor config executor returned %T, want address", reflected.FieldByName("Executor").Interface())
	}
	return executorConfig{
		MaxMessageSize: uint32(reflected.FieldByName("MaxMessageSize").Uint()),
		Executor:       executor,
	}, nil
}

func decodeULNConfig(data []byte) (ulnConfig, error) {
	values, err := configDecoderABI.Unpack("ulnConfig", data)
	if err != nil {
		return ulnConfig{}, err
	}
	if len(values) != 1 {
		return ulnConfig{}, fmt.Errorf("uln config returned %d values, want 1", len(values))
	}
	reflected := reflect.ValueOf(values[0])
	if reflected.Kind() != reflect.Struct {
		return ulnConfig{}, fmt.Errorf("uln config returned %T, want tuple struct", values[0])
	}
	requiredDVNs, ok := reflected.FieldByName("RequiredDVNs").Interface().([]common.Address)
	if !ok {
		return ulnConfig{}, fmt.Errorf("uln config requiredDVNs returned %T, want []common.Address", reflected.FieldByName("RequiredDVNs").Interface())
	}
	optionalDVNs, ok := reflected.FieldByName("OptionalDVNs").Interface().([]common.Address)
	if !ok {
		return ulnConfig{}, fmt.Errorf("uln config optionalDVNs returned %T, want []common.Address", reflected.FieldByName("OptionalDVNs").Interface())
	}
	return ulnConfig{
		Confirmations:        reflected.FieldByName("Confirmations").Uint(),
		RequiredDVNCount:     uint8(reflected.FieldByName("RequiredDVNCount").Uint()),
		OptionalDVNCount:     uint8(reflected.FieldByName("OptionalDVNCount").Uint()),
		OptionalDVNThreshold: uint8(reflected.FieldByName("OptionalDVNThreshold").Uint()),
		RequiredDVNs:         append([]common.Address(nil), requiredDVNs...),
		OptionalDVNs:         append([]common.Address(nil), optionalDVNs...),
	}, nil
}

func callUint32(ctx context.Context, caller ChainClient, contractABI abi.ABI, to common.Address, method string, args ...any) (uint32, error) {
	values, err := callValues(ctx, caller, contractABI, to, method, args...)
	if err != nil {
		return 0, err
	}
	value, ok := values[0].(uint32)
	if !ok {
		return 0, fmt.Errorf("%s returned %T, want uint32", method, values[0])
	}
	return value, nil
}

func callAddress(ctx context.Context, caller ChainClient, contractABI abi.ABI, to common.Address, method string, args ...any) (common.Address, error) {
	values, err := callValues(ctx, caller, contractABI, to, method, args...)
	if err != nil {
		return common.Address{}, err
	}
	value, ok := values[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("%s returned %T, want address", method, values[0])
	}
	return value, nil
}

func callHash(ctx context.Context, caller ChainClient, contractABI abi.ABI, to common.Address, method string, args ...any) (common.Hash, error) {
	values, err := callValues(ctx, caller, contractABI, to, method, args...)
	if err != nil {
		return common.Hash{}, err
	}
	value, ok := values[0].([32]byte)
	if !ok {
		return common.Hash{}, fmt.Errorf("%s returned %T, want bytes32", method, values[0])
	}
	return common.BytesToHash(value[:]), nil
}

func callBool(ctx context.Context, caller ChainClient, contractABI abi.ABI, to common.Address, method string, args ...any) (bool, error) {
	values, err := callValues(ctx, caller, contractABI, to, method, args...)
	if err != nil {
		return false, err
	}
	value, ok := values[0].(bool)
	if !ok {
		return false, fmt.Errorf("%s returned %T, want bool", method, values[0])
	}
	return value, nil
}

func callBytes(ctx context.Context, caller ChainClient, contractABI abi.ABI, to common.Address, method string, args ...any) ([]byte, error) {
	values, err := callValues(ctx, caller, contractABI, to, method, args...)
	if err != nil {
		return nil, err
	}
	value, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("%s returned %T, want bytes", method, values[0])
	}
	return value, nil
}

func callValues(ctx context.Context, caller ChainClient, contractABI abi.ABI, to common.Address, method string, args ...any) ([]any, error) {
	data, err := contractABI.Pack(method, args...)
	if err != nil {
		return nil, err
	}
	result, err := caller.CallContract(ctx, ethereum.CallMsg{To: &to, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	values, err := contractABI.Unpack(method, result)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%s returned no values", method)
	}
	return values, nil
}

func uint64Value(value any) (uint64, bool) {
	switch typed := value.(type) {
	case uint64:
		return typed, true
	case *big.Int:
		if typed == nil || !typed.IsUint64() {
			return 0, false
		}
		return typed.Uint64(), true
	default:
		return 0, false
	}
}

func uint16Value(value any) (uint16, bool) {
	switch typed := value.(type) {
	case uint16:
		return typed, true
	case uint64:
		if typed > ^uint64(0)>>48 {
			return 0, false
		}
		return uint16(typed), true
	case *big.Int:
		if typed == nil || !typed.IsUint64() || typed.Uint64() > ^uint64(0)>>48 {
			return 0, false
		}
		return uint16(typed.Uint64()), true
	default:
		return 0, false
	}
}

func addressesString(addresses []common.Address) string {
	return "[" + strings.Join(sortedAddressHex(addresses), ",") + "]"
}

func sortedAddressHex(addresses []common.Address) []string {
	out := make([]string, 0, len(addresses))
	for _, address := range addresses {
		out = append(out, hex.EncodeToString(address.Bytes()))
	}
	sort.Strings(out)
	return out
}

func bigString(value *big.Int) string {
	if value == nil {
		return "<nil>"
	}
	return value.String()
}
