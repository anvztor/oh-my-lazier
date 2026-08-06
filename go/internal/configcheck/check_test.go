package configcheck

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/bigutil"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

func TestCheckWithClientsAcceptsMatchingOnChainState(t *testing.T) {
	registry, clients := testRegistryAndClients(t)
	report, err := CheckWithClients(t.Context(), registry, chainClients(clients))
	if err != nil {
		t.Fatalf("CheckWithClients() error = %v", err)
	}
	if !report.OK {
		t.Fatalf("CheckWithClients() ok = false, issues = %+v", report.Issues)
	}
}

func TestCheckWithClientsReportsMismatches(t *testing.T) {
	for name, mutate := range map[string]func(map[uint32]*fakeChainClient){
		"chainID": func(clients map[uint32]*fakeChainClient) {
			clients[40161].chainID = big.NewInt(1)
		},
		"missingCode": func(clients map[uint32]*fakeChainClient) {
			delete(clients[40161].code, common.HexToAddress("0x9999999999999999999999999999999999999999"))
		},
		"peer": func(clients map[uint32]*fakeChainClient) {
			clients[40161].peers[common.HexToAddress("0x7777777777777777777777777777777777777777")][40449] = common.Hash{}
		},
		"sendLibrary": func(clients map[uint32]*fakeChainClient) {
			clients[40161].sendLibraries[pathKey(common.HexToAddress("0x7777777777777777777777777777777777777777"), 40449)] = common.HexToAddress("0x1212121212121212121212121212121212121212")
		},
		"requiredDVNs": func(clients map[uint32]*fakeChainClient) {
			clients[40161].ulnConfigs[configKey(common.HexToAddress("0x7777777777777777777777777777777777777777"), common.HexToAddress("0x9999999999999999999999999999999999999999"), 40449)] = ulnConfig{
				Confirmations:        12,
				RequiredDVNCount:     1,
				OptionalDVNCount:     nilDVNCount,
				OptionalDVNThreshold: 0,
				RequiredDVNs:         []common.Address{common.HexToAddress("0x3333333333333333333333333333333333333333")},
			}
		},
		// The on-chain set keeps the right size and still contains the local OpenDVN,
		// so only the exact-set comparison can catch the unapproved peer entry.
		"unapprovedPeerDVN": func(clients map[uint32]*fakeChainClient) {
			clients[40161].ulnConfigs[configKey(common.HexToAddress("0x7777777777777777777777777777777777777777"), common.HexToAddress("0x9999999999999999999999999999999999999999"), 40449)] = ulnConfig{
				Confirmations:        12,
				RequiredDVNCount:     2,
				OptionalDVNCount:     nilDVNCount,
				OptionalDVNThreshold: 0,
				RequiredDVNs: []common.Address{
					common.HexToAddress("0x3333333333333333333333333333333333333333"),
					common.HexToAddress("0x1212121212121212121212121212121212121212"),
				},
			}
		},
		// A third required DVN beyond the approved set changes the verification quorum
		// even though every approved entry is present.
		"extraRequiredDVN": func(clients map[uint32]*fakeChainClient) {
			clients[40161].ulnConfigs[configKey(common.HexToAddress("0x7777777777777777777777777777777777777777"), common.HexToAddress("0x9999999999999999999999999999999999999999"), 40449)] = ulnConfig{
				Confirmations:        12,
				RequiredDVNCount:     3,
				OptionalDVNCount:     nilDVNCount,
				OptionalDVNThreshold: 0,
				RequiredDVNs: []common.Address{
					common.HexToAddress("0x3333333333333333333333333333333333333333"),
					common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd"),
					common.HexToAddress("0x1212121212121212121212121212121212121212"),
				},
			}
		},
		"workerGas": func(clients map[uint32]*fakeChainClient) {
			cfg := clients[40161].workerPathways[workerPathwayKey(common.HexToAddress("0x2222222222222222222222222222222222222222"), 40449, common.HexToAddress("0x7777777777777777777777777777777777777777"))]
			cfg.MaxLzReceiveGas = big.NewInt(1)
			clients[40161].workerPathways[workerPathwayKey(common.HexToAddress("0x2222222222222222222222222222222222222222"), 40449, common.HexToAddress("0x7777777777777777777777777777777777777777"))] = cfg
		},
		"priceFeed": func(clients map[uint32]*fakeChainClient) {
			clients[40161].priceFeeds[common.HexToAddress("0x2222222222222222222222222222222222222222")] = common.HexToAddress("0x1212121212121212121212121212121212121212")
		},
		"feeModel": func(clients map[uint32]*fakeChainClient) {
			model := clients[40161].workerFeeModels[workerFeeModelKey(common.HexToAddress("0x2222222222222222222222222222222222222222"), 40449)]
			model.MarginBps = 999
			clients[40161].workerFeeModels[workerFeeModelKey(common.HexToAddress("0x2222222222222222222222222222222222222222"), 40449)] = model
		},
	} {
		t.Run(name, func(t *testing.T) {
			registry, clients := testRegistryAndClients(t)
			mutate(clients)
			report, err := CheckWithClients(t.Context(), registry, chainClients(clients))
			if err != nil {
				t.Fatalf("CheckWithClients() error = %v", err)
			}
			if report.OK || len(report.Issues) == 0 {
				t.Fatalf("CheckWithClients() report = %+v, want mismatch issues", report)
			}
		})
	}
}

func TestCheckWithClientsReportsSendConfirmationsBelowReceive(t *testing.T) {
	registry, clients := testRegistryAndClients(t)
	key := configKey(common.HexToAddress("0x7777777777777777777777777777777777777777"), common.HexToAddress("0x9999999999999999999999999999999999999999"), 40449)
	uln := clients[40161].ulnConfigs[key]
	uln.Confirmations = 5
	clients[40161].ulnConfigs[key] = uln
	report, err := CheckWithClients(t.Context(), registry, chainClients(clients))
	if err != nil {
		t.Fatalf("CheckWithClients() error = %v", err)
	}
	if report.OK {
		t.Fatal("CheckWithClients() ok = true, want uln confirmations mismatch")
	}
	found := false
	for _, issue := range report.Issues {
		if strings.HasSuffix(issue.Path, ".uln_confirmations") {
			found = true
			if !strings.Contains(issue.Message, "below receive uln confirmations") {
				t.Fatalf("issue message = %q", issue.Message)
			}
		}
	}
	if !found {
		t.Fatalf("issues = %+v, want a .uln_confirmations issue", report.Issues)
	}
}

func TestCheckWithClientsReportsRPCChainIDMismatch(t *testing.T) {
	registry, clients := testRegistryAndClients(t)
	genericClients := chainClients(clients)
	genericClients[40161] = validatingChainClient{
		ChainClient: clients[40161],
		err: &rpcquorum.ChainIDMismatchError{
			ChainName: "ethereum-sepolia",
			Expected:  big.NewInt(11155111),
			Details:   []string{"provider http://wrong returned 560048"},
		},
	}

	report, err := CheckWithClients(t.Context(), registry, genericClients)
	if err != nil {
		t.Fatalf("CheckWithClients() error = %v", err)
	}
	if report.OK {
		t.Fatal("CheckWithClients() ok = true, want mismatch")
	}
	if len(report.Issues) != 1 {
		t.Fatalf("issues = %+v, want one rpc_urls issue", report.Issues)
	}
	if report.Issues[0].Path != "chains[40161].rpc_urls" {
		t.Fatalf("issue path = %q", report.Issues[0].Path)
	}
	if !strings.Contains(report.Issues[0].Message, "provider http://wrong returned 560048") {
		t.Fatalf("issue message = %q", report.Issues[0].Message)
	}
}

func TestCheckWithClientsReportsMissingDestinationVerifierAuthorization(t *testing.T) {
	cfg := testConfig()
	cfg.Pathways[0].DVN.Mode = config.DVNModeActive
	registry, clients := testRegistryAndClientsForConfig(t, cfg)
	pathway, err := registry.Pathway(
		cfg.Pathways[0].SrcEID,
		cfg.Pathways[0].DstEID,
		cfg.Pathways[0].SrcOApp.Common(),
		cfg.Pathways[0].DstOApp.Common(),
	)
	if err != nil {
		t.Fatalf("Pathway() error = %v", err)
	}
	dstChain, _ := registry.Get(pathway.DstEID)
	verifier := common.HexToAddress(dstChain.TxRoles.DVN.SignerID)
	delete(clients[pathway.DstEID].verifiers, verifierKey(pathway.DestinationWorkers.OpenDVN, verifier))

	report, err := CheckWithClients(t.Context(), registry, chainClients(clients))
	if err != nil {
		t.Fatalf("CheckWithClients() error = %v", err)
	}
	if report.OK {
		t.Fatal("CheckWithClients() ok = true, want verifier mismatch")
	}
	if len(report.Issues) != 1 {
		t.Fatalf("issues = %+v, want one verifier issue", report.Issues)
	}
	if !strings.Contains(report.Issues[0].Path, "destination_workers.open_dvn.verifiers") {
		t.Fatalf("issue path = %q", report.Issues[0].Path)
	}
}

func TestCheckWithClientsReportsMissingPricingSubmitterAuthorization(t *testing.T) {
	registry, clients := testRegistryAndClients(t)
	signer := common.HexToAddress("0x9999999999999999999999999999999999999999")
	for _, pathway := range registry.Pathways() {
		clients[pathway.SrcEID].priceSubmitters[priceSubmitterKey(pathway.SourceWorkers.PriceFeed, signer)] = true
	}
	pathways := registry.Pathways()
	delete(clients[pathways[0].SrcEID].priceSubmitters, priceSubmitterKey(pathways[0].SourceWorkers.PriceFeed, signer))

	report, err := CheckWithClients(t.Context(), registry, chainClients(clients), WithPricingSigner(signer))
	if err != nil {
		t.Fatalf("CheckWithClients() error = %v", err)
	}
	if report.OK {
		t.Fatal("CheckWithClients() ok = true, want submitter mismatch")
	}
	if len(report.Issues) != 1 {
		t.Fatalf("issues = %+v, want one submitter issue", report.Issues)
	}
	if !strings.Contains(report.Issues[0].Path, "source_workers.price_feed.submitter") {
		t.Fatalf("issue path = %q", report.Issues[0].Path)
	}
}

func TestRenderTextReportsPassedCheck(t *testing.T) {
	output := RenderText(Report{OK: true})
	const want = "on-chain config check passed\n"
	if output != want {
		t.Fatalf("RenderText() = %q, want %q", output, want)
	}
}

func TestRenderTextReportsSingleIssue(t *testing.T) {
	output := RenderText(Report{
		Issues: []Issue{{Path: "chains[40161].chain_id", Message: "wrong"}},
	})
	const want = "on-chain config check failed (1 issue)\n" +
		"[1] chains[40161].chain_id\n" +
		"    wrong\n"
	if output != want {
		t.Fatalf("RenderText() = %q, want %q", output, want)
	}
}

func TestRenderTextReportsMultipleIssues(t *testing.T) {
	output := RenderText(Report{
		Issues: []Issue{
			{
				Path:    "chains[40161].rpc_urls",
				Message: "rpc chain_id mismatch for chain ethereum-sepolia, expected 11155111: provider http://wrong returned 560048",
			},
			{
				Path:    "pathways[40161:40449:0x7777777777777777777777777777777777777777:0x8888888888888888888888888888888888888888].source_workers.open_executor",
				Message: "worker pathway max_lz_receive_gas 1 does not match configured 200000",
			},
		},
	})
	const want = "on-chain config check failed (2 issues)\n" +
		"[1] chains[40161].rpc_urls\n" +
		"    rpc chain_id mismatch for chain ethereum-sepolia, expected 11155111: provider http://wrong returned 560048\n" +
		"[2] pathways[40161:40449:0x7777777777777777777777777777777777777777:0x8888888888888888888888888888888888888888].source_workers.open_executor\n" +
		"    worker pathway max_lz_receive_gas 1 does not match configured 200000\n"
	if output != want {
		t.Fatalf("RenderText() = %q, want %q", output, want)
	}
}

func testRegistryAndClients(t *testing.T) (*chain.Registry, map[uint32]*fakeChainClient) {
	t.Helper()
	return testRegistryAndClientsForConfig(t, testConfig())
}

func testRegistryAndClientsForConfig(t *testing.T, cfg config.Config) (*chain.Registry, map[uint32]*fakeChainClient) {
	t.Helper()
	registry, err := chain.NewRegistry(cfg.Chains, cfg.Pathways)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	clients := map[uint32]*fakeChainClient{
		40161: newFakeChainClient(11155111, 40161),
		40449: newFakeChainClient(560048, 40449),
	}
	for _, configured := range registry.All() {
		client := clients[configured.EID]
		client.code[configured.EndpointAddress] = true
	}
	for _, pathway := range registry.Pathways() {
		srcChain, _ := registry.Get(pathway.SrcEID)
		dstChain, _ := registry.Get(pathway.DstEID)
		src := clients[pathway.SrcEID]
		dst := clients[pathway.DstEID]
		src.addOApp(pathway.SrcOApp, srcChain.EndpointAddress, pathway.DstEID, pathway.DstOApp)
		dst.addOApp(pathway.DstOApp, dstChain.EndpointAddress, pathway.SrcEID, pathway.SrcOApp)
		src.code[pathway.SendLib] = true
		dst.code[pathway.ReceiveLib] = true
		src.code[pathway.SourceWorkers.PriceFeed] = true
		src.sendLibraries[pathKey(pathway.SrcOApp, pathway.DstEID)] = pathway.SendLib
		dst.receiveLibraries[pathKey(pathway.DstOApp, pathway.SrcEID)] = pathway.ReceiveLib
		src.executorConfigs[configKey(pathway.SrcOApp, pathway.SendLib, pathway.DstEID)] = executorConfig{
			MaxMessageSize: uint32(pathway.MaxMessageSize),
			Executor:       pathway.SourceWorkers.OpenExecutor,
		}
		src.ulnConfigs[configKey(pathway.SrcOApp, pathway.SendLib, pathway.DstEID)] = expectedULNConfig(srcChain, pathway.SourceWorkers.OpenDVN)
		dst.ulnConfigs[configKey(pathway.DstOApp, pathway.ReceiveLib, pathway.SrcEID)] = expectedULNConfig(dstChain, pathway.DestinationWorkers.OpenDVN)
		for _, worker := range []common.Address{pathway.SourceWorkers.OpenExecutor, pathway.SourceWorkers.OpenDVN} {
			src.code[worker] = true
			src.priceFeeds[worker] = pathway.SourceWorkers.PriceFeed
			src.allowedSendLibs[workerSendLibKey(worker, pathway.SendLib)] = true
			src.workerPathways[workerPathwayKey(worker, pathway.DstEID, pathway.SrcOApp)] = pathwayConfig{
				Enabled:         pathway.Enabled,
				MaxMessageSize:  new(big.Int).SetUint64(pathway.MaxMessageSize),
				MinLzReceiveGas: new(big.Int).SetUint64(pathway.MinLzReceiveGas),
				MaxLzReceiveGas: new(big.Int).SetUint64(pathway.MaxLzReceiveGas),
			}
		}
		if pathway.Pricing.ExecutorFee.FixedFeeWei != "" {
			src.workerFeeModels[workerFeeModelKey(pathway.SourceWorkers.OpenExecutor, pathway.DstEID)] = testFeeModel(t, pathway.Pricing.ExecutorFee)
		}
		if pathway.Pricing.DVNFee.FixedFeeWei != "" {
			src.workerFeeModels[workerFeeModelKey(pathway.SourceWorkers.OpenDVN, pathway.DstEID)] = testFeeModel(t, pathway.Pricing.DVNFee)
		}
		dst.code[pathway.DestinationWorkers.OpenDVN] = true
		if pathway.DVNMode == config.DVNModeActive {
			verifier := common.HexToAddress(dstChain.TxRoles.DVN.SignerID)
			dst.verifiers[verifierKey(pathway.DestinationWorkers.OpenDVN, verifier)] = true
		}
	}
	report, err := CheckWithClients(context.Background(), registry, chainClients(clients))
	if err != nil {
		t.Fatalf("baseline CheckWithClients() error = %v", err)
	}
	if !report.OK {
		t.Fatalf("baseline CheckWithClients() issues = %+v", report.Issues)
	}
	return registry, clients
}

type validatingChainClient struct {
	ChainClient
	err error
}

func (v validatingChainClient) ValidateChainID(context.Context, *big.Int) error {
	return v.err
}

type fakeChainClient struct {
	chainID          *big.Int
	eid              uint32
	code             map[common.Address]bool
	oappEndpoints    map[common.Address]common.Address
	peers            map[common.Address]map[uint32]common.Hash
	sendLibraries    map[string]common.Address
	receiveLibraries map[string]common.Address
	executorConfigs  map[string]executorConfig
	ulnConfigs       map[string]ulnConfig
	allowedSendLibs  map[string]bool
	workerPathways   map[string]pathwayConfig
	priceFeeds       map[common.Address]common.Address
	priceSubmitters  map[string]bool
	workerFeeModels  map[string]feeModel
	verifiers        map[string]bool
}

func newFakeChainClient(chainID int64, eid uint32) *fakeChainClient {
	return &fakeChainClient{
		chainID:          big.NewInt(chainID),
		eid:              eid,
		code:             make(map[common.Address]bool),
		oappEndpoints:    make(map[common.Address]common.Address),
		peers:            make(map[common.Address]map[uint32]common.Hash),
		sendLibraries:    make(map[string]common.Address),
		receiveLibraries: make(map[string]common.Address),
		executorConfigs:  make(map[string]executorConfig),
		ulnConfigs:       make(map[string]ulnConfig),
		allowedSendLibs:  make(map[string]bool),
		workerPathways:   make(map[string]pathwayConfig),
		priceFeeds:       make(map[common.Address]common.Address),
		priceSubmitters:  make(map[string]bool),
		workerFeeModels:  make(map[string]feeModel),
		verifiers:        make(map[string]bool),
	}
}

func (f *fakeChainClient) ChainID(context.Context) (*big.Int, error) {
	return new(big.Int).Set(f.chainID), nil
}

func (f *fakeChainClient) CodeAt(_ context.Context, address common.Address, _ *big.Int) ([]byte, error) {
	if f.code[address] {
		return []byte{0x01}, nil
	}
	return nil, nil
}

func (f *fakeChainClient) CallContract(_ context.Context, call ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	if call.To == nil {
		return nil, errors.New("missing call target")
	}
	if method, err := endpointABI.MethodById(call.Data[:4]); err == nil {
		args, err := method.Inputs.Unpack(call.Data[4:])
		if err != nil {
			return nil, err
		}
		return f.callEndpoint(*call.To, method, args)
	}
	if method, err := oappABI.MethodById(call.Data[:4]); err == nil {
		args, err := method.Inputs.Unpack(call.Data[4:])
		if err != nil {
			return nil, err
		}
		return f.callOApp(*call.To, method, args)
	}
	if method, err := workerABI.MethodById(call.Data[:4]); err == nil {
		args, err := method.Inputs.Unpack(call.Data[4:])
		if err != nil {
			return nil, err
		}
		return f.callWorker(*call.To, method, args)
	}
	return nil, errors.New("unknown method")
}

func (f *fakeChainClient) callEndpoint(_ common.Address, method *abi.Method, args []any) ([]byte, error) {
	switch method.Name {
	case "eid":
		return method.Outputs.Pack(f.eid)
	case "getSendLibrary":
		return method.Outputs.Pack(f.sendLibraries[pathKey(args[0].(common.Address), args[1].(uint32))])
	case "getReceiveLibrary":
		return method.Outputs.Pack(f.receiveLibraries[pathKey(args[0].(common.Address), args[1].(uint32))], false)
	case "getConfig":
		oapp := args[0].(common.Address)
		library := args[1].(common.Address)
		remoteEID := args[2].(uint32)
		configType := args[3].(uint32)
		key := configKey(oapp, library, remoteEID)
		switch configType {
		case configTypeExecutor:
			encoded, err := configDecoderABI.Methods["executorConfig"].Outputs.Pack(f.executorConfigs[key])
			if err != nil {
				return nil, err
			}
			return method.Outputs.Pack(encoded)
		case configTypeULN:
			encoded, err := configDecoderABI.Methods["ulnConfig"].Outputs.Pack(f.ulnConfigs[key])
			if err != nil {
				return nil, err
			}
			return method.Outputs.Pack(encoded)
		default:
			return nil, errors.New("unknown config type")
		}
	default:
		return nil, errors.New("unknown endpoint method")
	}
}

func (f *fakeChainClient) callOApp(to common.Address, method *abi.Method, args []any) ([]byte, error) {
	switch method.Name {
	case "endpoint":
		return method.Outputs.Pack(f.oappEndpoints[to])
	case "peers":
		return method.Outputs.Pack(f.peers[to][args[0].(uint32)])
	default:
		return nil, errors.New("unknown oapp method")
	}
}

func (f *fakeChainClient) callWorker(to common.Address, method *abi.Method, args []any) ([]byte, error) {
	switch method.Name {
	case "allowedSendLib":
		return method.Outputs.Pack(f.allowedSendLibs[workerSendLibKey(to, args[0].(common.Address))])
	case "pathwayConfig":
		config := f.workerPathways[workerPathwayKey(to, args[0].(uint32), args[1].(common.Address))]
		return method.Outputs.Pack(config.Enabled, config.MaxMessageSize, config.MinLzReceiveGas, config.MaxLzReceiveGas)
	case "priceFeed":
		return method.Outputs.Pack(f.priceFeeds[to])
	case "submitters":
		return method.Outputs.Pack(f.priceSubmitters[priceSubmitterKey(to, args[0].(common.Address))])
	case "feeModel":
		model := f.workerFeeModels[workerFeeModelKey(to, args[0].(uint32))]
		return method.Outputs.Pack(model.FixedFee, model.DstGasOverhead, model.DataSizeOverheadBytes, model.MarginBps)
	case "verifiers":
		return method.Outputs.Pack(f.verifiers[verifierKey(to, args[0].(common.Address))])
	default:
		return nil, errors.New("unknown worker method")
	}
}

func (f *fakeChainClient) addOApp(oapp, endpoint common.Address, remoteEID uint32, remoteOApp common.Address) {
	f.code[oapp] = true
	f.oappEndpoints[oapp] = endpoint
	if f.peers[oapp] == nil {
		f.peers[oapp] = make(map[uint32]common.Hash)
	}
	f.peers[oapp][remoteEID] = common.BytesToHash(remoteOApp.Bytes())
}

func expectedULNConfig(configured chain.Chain, openDVN common.Address) ulnConfig {
	return ulnConfig{
		Confirmations:        configured.Confirmations,
		RequiredDVNCount:     2,
		OptionalDVNCount:     nilDVNCount,
		OptionalDVNThreshold: 0,
		RequiredDVNs:         []common.Address{openDVN, externalDVN(configured.EID)},
	}
}

func externalDVN(eid uint32) common.Address {
	switch eid {
	case 40161:
		return common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd")
	case 40449:
		return common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	default:
		return common.HexToAddress("0xffffffffffffffffffffffffffffffffffffffff")
	}
}

func testConfig() config.Config {
	return config.Config{
		Chains: []config.ChainConfig{
			{
				EID:             40161,
				Name:            "ethereum-sepolia",
				Family:          config.ChainFamilyEVM,
				ChainID:         11155111,
				EndpointAddress: config.MustEVMAddress("0x1111111111111111111111111111111111111111"),
				Confirmations:   12,
				RPCURLs:         []string{"http://localhost:8545"},
				TxRoles: config.ChainTxRolesConfig{
					Executor: testExecutorRole(),
					DVN:      testDVNRole(),
				},
			},
			{
				EID:             40449,
				Name:            "hoodi",
				Family:          config.ChainFamilyEVM,
				ChainID:         560048,
				EndpointAddress: config.MustEVMAddress("0x4444444444444444444444444444444444444444"),
				Confirmations:   12,
				RPCURLs:         []string{"http://localhost:8546"},
				TxRoles: config.ChainTxRolesConfig{
					Executor: testExecutorRole(),
					DVN:      testDVNRole(),
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
				SendRequiredDVNs: []config.EVMAddress{
					config.MustEVMAddress("0x3333333333333333333333333333333333333333"),
					config.MustEVMAddress("0xdddddddddddddddddddddddddddddddddddddddd"),
				},
				ReceiveRequiredDVNs: []config.EVMAddress{
					config.MustEVMAddress("0x6666666666666666666666666666666666666666"),
					config.MustEVMAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
				},
				DVN:             config.PathwayDVNConfig{Mode: config.DVNModeShadow},
				Pricing:         testPathwayPricingConfig(),
				Enabled:         true,
				MaxMessageSize:  10000,
				MinLzReceiveGas: 100000,
				MaxLzReceiveGas: 300000,
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
				SendRequiredDVNs: []config.EVMAddress{
					config.MustEVMAddress("0x6666666666666666666666666666666666666666"),
					config.MustEVMAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
				},
				ReceiveRequiredDVNs: []config.EVMAddress{
					config.MustEVMAddress("0x3333333333333333333333333333333333333333"),
					config.MustEVMAddress("0xdddddddddddddddddddddddddddddddddddddddd"),
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

func testExecutorRole() config.ExecutorTxRoleConfig {
	return config.ExecutorTxRoleConfig{
		Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func testDVNRole() config.DVNTxRoleConfig {
	return config.DVNTxRoleConfig{
		Signer:                  config.MustEVMAddress("0x9999999999999999999999999999999999999999"),
		MaxFeePerGasWei:         "2000000000",
		MaxPriorityFeePerGasWei: "1000000000",
		MinNativeBalanceWei:     "100000000000000000",
	}
}

func testPathwayPricingConfig() config.PathwayPricingConfig {
	return config.PathwayPricingConfig{
		ExecutorFee: config.WorkerFeeModelConfig{FixedFeeWei: "1000", DstGasOverhead: 50_000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 100},
		DVNFee:      config.WorkerFeeModelConfig{FixedFeeWei: "2000", DstGasOverhead: 150_000, DataSizeOverheadBytes: new(uint64(0)), MarginBps: 200},
	}
}

func pathKey(oapp common.Address, eid uint32) string {
	return oapp.Hex() + ":" + big.NewInt(int64(eid)).String()
}

func configKey(oapp, library common.Address, eid uint32) string {
	return oapp.Hex() + ":" + library.Hex() + ":" + big.NewInt(int64(eid)).String()
}

func workerSendLibKey(worker, sendLib common.Address) string {
	return worker.Hex() + ":" + sendLib.Hex()
}

func workerPathwayKey(worker common.Address, dstEID uint32, sender common.Address) string {
	return worker.Hex() + ":" + big.NewInt(int64(dstEID)).String() + ":" + sender.Hex()
}

func workerFeeModelKey(worker common.Address, dstEID uint32) string {
	return worker.Hex() + ":" + big.NewInt(int64(dstEID)).String()
}

func priceSubmitterKey(priceFeed, submitter common.Address) string {
	return priceFeed.Hex() + ":" + submitter.Hex()
}

func verifierKey(openDVN, verifier common.Address) string {
	return openDVN.Hex() + ":" + verifier.Hex()
}

func testFeeModel(t *testing.T, cfg config.WorkerFeeModelConfig) feeModel {
	t.Helper()
	fixedFee, err := bigutil.ParseDecimal("fixed_fee_wei", cfg.FixedFeeWei)
	if err != nil {
		t.Fatalf("invalid fixed fee %q", cfg.FixedFeeWei)
	}
	if cfg.DataSizeOverheadBytes == nil {
		t.Fatal("missing data size overhead")
	}
	return feeModel{
		FixedFee:              fixedFee,
		DstGasOverhead:        cfg.DstGasOverhead,
		DataSizeOverheadBytes: *cfg.DataSizeOverheadBytes,
		MarginBps:             cfg.MarginBps,
	}
}

func chainClients(clients map[uint32]*fakeChainClient) map[uint32]ChainClient {
	genericClients := make(map[uint32]ChainClient, len(clients))
	for eid, client := range clients {
		genericClients[eid] = client
	}
	return genericClients
}
