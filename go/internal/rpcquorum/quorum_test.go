package rpcquorum

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

func TestReceiptFingerprintIncludesLogEvidence(t *testing.T) {
	receipt := testReceipt()
	mutated := testReceipt()
	mutated.Logs[0].Data = []byte{0x01, 0x02}

	if receiptFingerprint(receipt) == receiptFingerprint(mutated) {
		t.Fatal("receipt fingerprint ignored log data")
	}
}

func TestReceiptFingerprintMatchesEquivalentReceipts(t *testing.T) {
	left := testReceipt()
	right := testReceipt()

	if receiptFingerprint(left) != receiptFingerprint(right) {
		t.Fatalf("equivalent receipt fingerprints differ:\nleft:  %s\nright: %s", receiptFingerprint(left), receiptFingerprint(right))
	}
}

func TestIsReceiptConflict(t *testing.T) {
	err := &ReceiptConflictError{TxHash: common.HexToHash("0x1234")}
	if !IsReceiptConflict(err) {
		t.Fatal("IsReceiptConflict() = false, want true")
	}
}

func TestValidateProviderChainIDsAcceptsAllExpected(t *testing.T) {
	err := validateProviderChainIDs("testnet", big.NewInt(11155111), []providerChainID{
		{ProviderID: "provider-a", ChainID: big.NewInt(11155111)},
		{ProviderID: "provider-b", ChainID: big.NewInt(11155111)},
	})
	if err != nil {
		t.Fatalf("validateProviderChainIDs() error = %v", err)
	}
}

func TestValidateProviderChainIDsRejectsUnexpectedProviderChainID(t *testing.T) {
	err := validateProviderChainIDs("testnet", big.NewInt(11155111), []providerChainID{
		{ProviderID: "provider-a", ChainID: big.NewInt(11155111)},
		{ProviderID: "provider-b", ChainID: big.NewInt(560048)},
	})
	if err == nil {
		t.Fatal("validateProviderChainIDs() error = nil, want mismatch")
	}
	if !IsChainIDMismatch(err) {
		t.Fatalf("IsChainIDMismatch() = false for %T", err)
	}
	if !strings.Contains(err.Error(), "provider-b returned 560048") {
		t.Fatalf("error = %q, want provider detail", err)
	}
}

func TestValidateChainIDRedactsProviderURLOnRequestFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	const user = "rpc-user"
	const password = "rpc-password"
	const apiKey = "rpc-api-key"
	rawURL := strings.Replace(server.URL, "http://", "http://"+user+":"+password+"@", 1) + "?api_key=" + apiKey
	client := New("testnet", []string{rawURL})
	defer client.Close()

	err := client.ValidateChainID(context.Background(), big.NewInt(1))
	if err == nil {
		t.Fatal("ValidateChainID() error = nil, want request failure")
	}
	if !strings.Contains(err.Error(), "provider[0] eth_chainId failed") {
		t.Fatalf("ValidateChainID() error = %q, want redacted provider failure", err)
	}
	for _, secret := range []string{user, password, apiKey, rawURL} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("ValidateChainID() leaked %q in error %q", secret, err)
		}
	}
}

func TestProviderOperationErrorRedactsCauseAndPreservesIdentity(t *testing.T) {
	cause := testRPCError{message: "upstream included rpc-secret-token", code: 3}
	err := wrapProviderOperationError(2, "eth_getLogs", cause)
	if err.Error() != "provider[2] eth_getLogs failed" {
		t.Fatalf("error = %q, want redacted provider operation", err)
	}
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is() = false, want wrapped cause identity")
	}
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) || rpcErr.ErrorCode() != 3 {
		t.Fatalf("errors.As() did not preserve rpc error: %v", err)
	}
	if strings.Contains(err.Error(), "rpc-secret-token") {
		t.Fatalf("error leaked cause: %q", err)
	}
	canceled := wrapProviderOperationError(2, "eth_getLogs", context.Canceled)
	if !errors.Is(canceled, context.Canceled) {
		t.Fatalf("errors.Is(context.Canceled) = false for %v", canceled)
	}
}

func TestProvidersReturnRedactedIdentities(t *testing.T) {
	const secretURL = "https://rpc-user:rpc-password@rpc-secret.example/v2/rpc-api-key"
	client := New("testnet", []string{secretURL})
	providers := client.Providers()
	if len(providers) != 1 {
		t.Fatalf("Providers() length = %d, want 1", len(providers))
	}
	if providers[0].ID != "provider[0]" || providers[0].Status != ProviderHealthy {
		t.Fatalf("Providers()[0] = %+v, want redacted healthy provider", providers[0])
	}
	if strings.Contains(providers[0].ID, "rpc-secret") {
		t.Fatalf("provider id leaked configured URL: %q", providers[0].ID)
	}
}

func TestCheckHeadConflictRedactsProviderURLs(t *testing.T) {
	const firstURL = "https://first-user:first-password@first-secret.example/v2/first-api-key"
	const secondURL = "https://second-user:second-password@second-secret.example/v2/second-api-key"
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: firstURL, status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: testHeader(0x01)})},
		{url: secondURL, status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: testHeader(0x02)})},
	}}

	_, err := client.CheckHead(context.Background())
	if err == nil || !IsHeadConflict(err) {
		t.Fatalf("CheckHead() error = %v, want head conflict", err)
	}
	assertRedactedProviderError(t, err, []string{firstURL, secondURL, "first-password", "second-api-key"}, "provider[0]", "provider[1]")
}

func TestTransactionReceiptConflictRedactsProviderURLs(t *testing.T) {
	const firstURL = "https://first-user:first-password@first-secret.example/v2/first-api-key"
	const secondURL = "https://second-user:second-password@second-secret.example/v2/second-api-key"
	firstReceipt := testReceipt()
	secondReceipt := testReceipt()
	secondReceipt.Status = gethtypes.ReceiptStatusFailed
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: firstURL, status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: firstReceipt})},
		{url: secondURL, status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: secondReceipt})},
	}}

	_, err := client.TransactionReceipt(context.Background(), firstReceipt.TxHash)
	if err == nil || !IsReceiptConflict(err) {
		t.Fatalf("TransactionReceipt() error = %v, want receipt conflict", err)
	}
	assertRedactedProviderError(t, err, []string{firstURL, secondURL, "first-password", "second-api-key"}, "provider[0]", "provider[1]")
}

func TestTransactionReceiptPartialNotFoundRedactsProviderURLs(t *testing.T) {
	const firstURL = "https://first-user:first-password@first-secret.example/v2/first-api-key"
	const secondURL = "https://second-user:second-password@second-secret.example/v2/second-api-key"
	receipt := testReceipt()
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: firstURL, status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: receipt})},
		{url: secondURL, status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
	}}

	_, err := client.TransactionReceipt(context.Background(), receipt.TxHash)
	if err == nil || !IsReceiptConflict(err) {
		t.Fatalf("TransactionReceipt() error = %v, want partial not-found conflict", err)
	}
	assertRedactedProviderError(t, err, []string{firstURL, secondURL, "first-password", "second-api-key"}, "provider[1]")
}

func TestTransactionReceiptTransientErrorRedactsProviderURL(t *testing.T) {
	const secretURL = "https://rpc-user:rpc-password@rpc-secret.example/v2/rpc-api-key"
	cause := errors.New("upstream echoed rpc-api-key")
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: secretURL, status: ProviderHealthy, client: newTestEthClient(t, testEthService{err: cause})},
	}}

	_, err := client.TransactionReceipt(context.Background(), common.HexToHash("0x1234"))
	if err == nil {
		t.Fatal("TransactionReceipt() error = nil, want transient failure")
	}
	assertRedactedProviderError(t, err, []string{secretURL, "rpc-password", "rpc-api-key"}, "provider[0]", "eth_getTransactionReceipt")
}

func TestCheckHeadUsesMajorityReachedHeight(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	liarTip := testHeaderAt(44, 0x02)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: liarTip, headers: map[int64]*gethtypes.Header{42: canonical}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
	}}

	head, err := client.CheckHead(context.Background())
	if err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	// A single inflated tip cannot lift the trusted head: the canonical height is
	// the one a configured majority has reached.
	if head.Number.Uint64() != 42 {
		t.Fatalf("head number = %s, want the majority-reached 42", head.Number)
	}
	if head.Hash != canonical.Hash().Hex() {
		t.Fatalf("head hash = %s, want the canonical %s", head.Hash, canonical.Hash().Hex())
	}
	for index, provider := range client.Providers() {
		if provider.Status != ProviderHealthy {
			t.Fatalf("provider[%d] status = %q, want healthy", index, provider.Status)
		}
	}
	number, err := client.BlockNumber(context.Background())
	if err != nil || number != 42 {
		t.Fatalf("BlockNumber() = (%d, %v), want the quorum head 42", number, err)
	}
}

func TestCheckHeadMarksMinorityForkConflictWithoutFailing(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	forked := testHeaderAt(42, 0x03)
	liarTip := testHeaderAt(44, 0x02)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: liarTip, headers: map[int64]*gethtypes.Header{42: forked}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
	}}

	head, err := client.CheckHead(context.Background())
	if err != nil {
		t.Fatalf("CheckHead() error = %v (a minority dissenter must not fail the chain)", err)
	}
	if head.Hash != canonical.Hash().Hex() {
		t.Fatalf("head hash = %s, want the majority hash", head.Hash)
	}
	providers := client.Providers()
	if providers[0].Status != ProviderConflict {
		t.Fatalf("provider[0] status = %q, want conflict", providers[0].Status)
	}
	if providers[1].Status != ProviderHealthy || providers[2].Status != ProviderHealthy {
		t.Fatalf("majority statuses = %q/%q, want healthy", providers[1].Status, providers[2].Status)
	}
}

func TestCheckHeadQuorumUnavailableOnInsufficientResponders(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{err: errors.New("boom")})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{err: errors.New("boom")})},
	}}

	// The quorum threshold is fixed on the CONFIGURED provider count: one
	// responder out of three must never degenerate into single-node trust.
	_, err := client.CheckHead(context.Background())
	if !IsQuorumUnavailable(err) {
		t.Fatalf("CheckHead() error = %v, want quorum unavailable", err)
	}
	providers := client.Providers()
	if providers[1].Status != ProviderUnavailable || providers[2].Status != ProviderUnavailable {
		t.Fatalf("failed providers = %q/%q, want unavailable (no stale healthy)", providers[1].Status, providers[2].Status)
	}
	// The lone responder's answer was never majority-verified, so a failed
	// round must downgrade it as well; otherwise single-source reads would
	// silently degrade to one unverified endpoint.
	if providers[0].Status != ProviderUnavailable {
		t.Fatalf("unverified responder status = %q, want unavailable", providers[0].Status)
	}
	if _, err := client.firstHealthyProvider(); err == nil {
		t.Fatal("firstHealthyProvider() succeeded after a failed quorum round")
	}
}

func TestCheckHeadSingleProviderDegenerate(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: []gethtypes.Log{testWindowLog(0)}})},
	}}
	head, err := client.CheckHead(context.Background())
	if err != nil {
		t.Fatalf("CheckHead() error = %v (N=1 means q=1)", err)
	}
	if head.Number.Uint64() != 42 {
		t.Fatalf("head number = %s, want 42", head.Number)
	}
	logs, err := client.FilterLogs(context.Background(), boundedLogQuery(41))
	if err != nil || len(logs) != 1 {
		t.Fatalf("FilterLogs() = (%d, %v), want the single provider's window", len(logs), err)
	}
}

func TestCheckHeadTwoProvidersRequireBothAndUseLowerHeight(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	ahead := testHeaderAt(44, 0x02)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: ahead, headers: map[int64]*gethtypes.Header{42: canonical}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
	}}
	head, err := client.CheckHead(context.Background())
	if err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	// N=2 means q=2: the canonical height is the lower tip both have reached,
	// and both must agree on its hash.
	if head.Number.Uint64() != 42 {
		t.Fatalf("head number = %s, want the height both providers reached (42)", head.Number)
	}
	for index, provider := range client.Providers() {
		if provider.Status != ProviderHealthy {
			t.Fatalf("provider[%d] status = %q, want healthy", index, provider.Status)
		}
	}
}

func TestCheckHeadBoundedByHangingProvider(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := &Client{chainName: "testnet", probeTimeout: 100 * time.Millisecond, providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{hang: true})},
	}}
	start := time.Now()
	head, err := client.CheckHead(context.Background())
	if err != nil {
		t.Fatalf("CheckHead() error = %v (a hanging provider must not stall the majority)", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("CheckHead took %s, want the hanging provider cut off by the probe deadline", elapsed)
	}
	if head.Number.Uint64() != 42 {
		t.Fatalf("head number = %s, want 42", head.Number)
	}
	if status := client.Providers()[2].Status; status != ProviderUnavailable {
		t.Fatalf("hanging provider status = %q, want unavailable", status)
	}
}

func TestCheckHeadClassifiesLaggingProvider(t *testing.T) {
	canonical := testHeaderAt(43, 0x01)
	lagging := testHeaderAt(41, 0x04)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "c", status: ProviderLagging, client: newTestEthClient(t, testEthService{header: lagging})},
	}}

	head, err := client.CheckHead(context.Background())
	if err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	if head.Number.Uint64() != 43 {
		t.Fatalf("head number = %s, want 43", head.Number)
	}
	providers := client.Providers()
	if providers[2].Status != ProviderLagging {
		t.Fatalf("provider[2] status = %q, want lagging", providers[2].Status)
	}
}

func TestFirstHealthyProviderPrefersNearCanonicalTip(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	aheadTip := testHeaderAt(45, 0x02)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: aheadTip, headers: map[int64]*gethtypes.Header{42: canonical}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
	}}
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	index, err := client.firstHealthyProvider()
	if err != nil {
		t.Fatalf("firstHealthyProvider() error = %v", err)
	}
	if index != 1 {
		t.Fatalf("preferred provider = %d, want the near-canonical provider 1", index)
	}
}

func TestHeaderByNumberLatestReturnsVerifiedCanonicalHeader(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
	}}
	header, err := client.HeaderByNumber(context.Background(), nil)
	if err != nil {
		t.Fatalf("HeaderByNumber(nil) error = %v", err)
	}
	if header.Hash() != canonical.Hash() {
		t.Fatalf("header hash = %s, want the verified canonical %s", header.Hash(), canonical.Hash())
	}
}

func testWindowLog(index uint) gethtypes.Log {
	return gethtypes.Log{
		Address:     common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Topics:      []common.Hash{common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")},
		Data:        []byte{0x01},
		BlockNumber: 40,
		TxHash:      common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
		TxIndex:     0,
		BlockHash:   common.HexToHash("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
		Index:       index,
	}
}

func boundedLogQuery(to int64) ethereum.FilterQuery {
	return ethereum.FilterQuery{FromBlock: big.NewInt(40), ToBlock: big.NewInt(to)}
}

func TestFilterLogsAdoptsMajorityAndFlagsMinority(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	full := []gethtypes.Log{testWindowLog(0), testWindowLog(1)}
	missing := []gethtypes.Log{testWindowLog(0)}
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: full})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: full})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: missing})},
	}}
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	logs, err := client.FilterLogs(context.Background(), boundedLogQuery(41))
	if err != nil {
		t.Fatalf("FilterLogs() error = %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("logs = %d, want the majority window of 2 (a silent dropper must not win)", len(logs))
	}
	providers := client.Providers()
	if providers[2].LogConflict != true {
		t.Fatal("provider[2] log conflict = false, want the dropped-log minority flagged")
	}
	if providers[0].LogConflict || providers[1].LogConflict {
		t.Fatal("majority providers flagged with log conflict")
	}
	// A head check must not clear the sticky log-conflict dimension.
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead(second) error = %v", err)
	}
	if !client.Providers()[2].LogConflict {
		t.Fatal("head check cleared an unresolved log conflict")
	}
}

func TestFilterLogsOrderEquivalenceReturnsCanonicalOrder(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	ordered := []gethtypes.Log{testWindowLog(0), testWindowLog(1)}
	reversed := []gethtypes.Log{testWindowLog(1), testWindowLog(0)}
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: reversed})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: ordered})},
	}}
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	logs, err := client.FilterLogs(context.Background(), boundedLogQuery(41))
	if err != nil {
		t.Fatalf("FilterLogs() error = %v (equal sets in different orders must agree)", err)
	}
	if len(logs) != 2 || logs[0].Index != 0 || logs[1].Index != 1 {
		t.Fatalf("logs order = %+v, want canonical (blockNumber, txIndex, logIndex) node order", logs)
	}
	for index, provider := range client.Providers() {
		if provider.LogConflict {
			t.Fatalf("provider[%d] flagged for a pure ordering difference", index)
		}
	}
}

func TestFilterLogsEmptyMajorityBeatsNonEmptyMinority(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: []gethtypes.Log{testWindowLog(0)}})},
	}}
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	logs, err := client.FilterLogs(context.Background(), boundedLogQuery(41))
	if err != nil {
		t.Fatalf("FilterLogs() error = %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("logs = %d, want the empty majority window (a fabricating minority must not win)", len(logs))
	}
	providers := client.Providers()
	if !providers[2].LogConflict {
		t.Fatal("fabricating provider not flagged with log conflict")
	}
	if providers[0].LogConflict || providers[1].LogConflict {
		t.Fatal("empty-majority providers flagged")
	}
}

func TestFilterLogsErrorRetryRecovers(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	full := []gethtypes.Log{testWindowLog(0)}
	flaky := &logsScript{steps: []logsStep{{err: errors.New("boom")}, {logs: full}}}
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: full})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logsScript: flaky})},
	}}
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	logs, err := client.FilterLogs(context.Background(), boundedLogQuery(41))
	if err != nil {
		t.Fatalf("FilterLogs() error = %v (one transient error must be absorbed by the bounded retry)", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs = %d, want 1", len(logs))
	}
	if client.Providers()[1].LogConflict {
		t.Fatal("recovered provider flagged with log conflict")
	}
}

func TestFilterLogsDivergenceRetryConverges(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	full := []gethtypes.Log{testWindowLog(0), testWindowLog(1)}
	converging := &logsScript{steps: []logsStep{{logs: []gethtypes.Log{testWindowLog(0)}}, {logs: full}}}
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: full})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: full})},
		{url: "c", status: ProviderHealthy, logConflict: true, client: newTestEthClient(t, testEthService{header: canonical, logsScript: converging})},
	}}
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	logs, err := client.FilterLogs(context.Background(), boundedLogQuery(41))
	if err != nil {
		t.Fatalf("FilterLogs() error = %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("logs = %d, want 2", len(logs))
	}
	// The diverging first answer converged on the single re-query, so the
	// provider is not flagged and its earlier sticky flag clears.
	if client.Providers()[2].LogConflict {
		t.Fatal("converged provider still flagged with log conflict")
	}
}

func TestLogWindowFingerprintInjectiveAcrossTopicDataBoundary(t *testing.T) {
	topic := common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	withTopic := testWindowLog(0)
	withTopic.Topics = []common.Hash{topic}
	withTopic.Data = []byte{0x01}
	// The delimiter-based encoding this replaced digested identical bytes for a
	// log whose data starts with the topic bytes.
	folded := testWindowLog(0)
	folded.Topics = nil
	folded.Data = append(append(append([]byte{}, topic.Bytes()...), '|'), 0x01)
	if logWindowFingerprint([]gethtypes.Log{withTopic}) == logWindowFingerprint([]gethtypes.Log{folded}) {
		t.Fatal("fingerprint collides across the topic/data boundary")
	}
}

func TestFilterLogsConflictWithoutMajority(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "https://secret-a.example/key-a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: []gethtypes.Log{testWindowLog(0)}})},
		{url: "https://secret-b.example/key-b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: []gethtypes.Log{testWindowLog(1)}})},
	}}
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	_, err := client.FilterLogs(context.Background(), boundedLogQuery(41))
	if !IsLogConflict(err) {
		t.Fatalf("FilterLogs() error = %v, want log conflict", err)
	}
	assertRedactedProviderError(t, err, []string{"secret-a.example", "key-b"}, "provider[0]", "provider[1]")
}

func TestFilterLogsRequiresSnapshotAndBoundedWindow(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
	}}
	if _, err := client.FilterLogs(context.Background(), boundedLogQuery(41)); err == nil {
		t.Fatal("FilterLogs without a head snapshot succeeded")
	}
	if _, err := client.CheckHead(context.Background()); err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	if _, err := client.FilterLogs(context.Background(), ethereum.FilterQuery{}); err == nil {
		t.Fatal("FilterLogs without bounds succeeded")
	}
	if _, err := client.FilterLogs(context.Background(), boundedLogQuery(43)); err == nil {
		t.Fatal("FilterLogs beyond the quorum head succeeded")
	}
}

func TestFilterLogsQuorumUnavailableWhenTooFewReachedWindow(t *testing.T) {
	// A fresh snapshot always has at least quorum tips at the canonical height;
	// this exercises the defensive branch for a stale snapshot whose recorded
	// tips no longer cover the queried window.
	canonical := testHeaderAt(42, 0x01)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: []gethtypes.Log{testWindowLog(0)}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical, logs: []gethtypes.Log{testWindowLog(0)}})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: canonical})},
	}}
	client.storeHeadSnapshot(&headSnapshot{number: big.NewInt(42), hash: canonical.Hash(), tips: map[int]*big.Int{0: big.NewInt(42)}})
	_, err := client.FilterLogs(context.Background(), boundedLogQuery(42))
	if !IsQuorumUnavailable(err) {
		t.Fatalf("FilterLogs() error = %v, want quorum unavailable", err)
	}
}

type testEthService struct {
	header  *gethtypes.Header
	headers map[int64]*gethtypes.Header
	receipt *gethtypes.Receipt
	logs    []gethtypes.Log
	logsErr error
	err     error
	// hang blocks header requests until the caller's context expires.
	hang bool
	// logsScript overrides logs/logsErr with per-call scripted answers.
	logsScript *logsScript
}

// logsScript serves scripted GetLogs answers call by call; the last step
// repeats forever.
type logsScript struct {
	mu    sync.Mutex
	steps []logsStep
}

type logsStep struct {
	logs []gethtypes.Log
	err  error
}

func (s *logsScript) next() ([]gethtypes.Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	step := s.steps[0]
	if len(s.steps) > 1 {
		s.steps = s.steps[1:]
	}
	return step.logs, step.err
}

type testRPCError struct {
	message string
	code    int
}

func (e testRPCError) Error() string {
	return e.message
}

func (e testRPCError) ErrorCode() int {
	return e.code
}

func (s testEthService) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, _ bool) (*gethtypes.Header, error) {
	if s.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.err != nil {
		return nil, s.err
	}
	if number != rpc.LatestBlockNumber && s.headers != nil {
		return s.headers[int64(number)], nil
	}
	return s.header, nil
}

func (s testEthService) GetLogs(context.Context, map[string]interface{}) ([]gethtypes.Log, error) {
	if s.logsScript != nil {
		return s.logsScript.next()
	}
	if s.logsErr != nil {
		return nil, s.logsErr
	}
	return s.logs, nil
}

func (s testEthService) GetTransactionReceipt(context.Context, common.Hash) (any, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.receipt == nil {
		return nil, nil
	}
	return s.receipt, nil
}

func newTestEthClient(t *testing.T, service testEthService) *ethclient.Client {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatalf("RegisterName() error = %v", err)
	}
	rpcClient := rpc.DialInProc(server)
	client := ethclient.NewClient(rpcClient)
	t.Cleanup(func() {
		client.Close()
		server.Stop()
	})
	return client
}

func testHeaderAt(number int64, extra byte) *gethtypes.Header {
	header := testHeader(extra)
	header.Number = big.NewInt(number)
	return header
}

func testHeader(extra byte) *gethtypes.Header {
	return &gethtypes.Header{
		ParentHash:  common.HexToHash("0x1111"),
		UncleHash:   gethtypes.EmptyUncleHash,
		Coinbase:    common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Root:        common.HexToHash("0x3333"),
		TxHash:      gethtypes.EmptyTxsHash,
		ReceiptHash: gethtypes.EmptyReceiptsHash,
		Difficulty:  big.NewInt(1),
		Number:      big.NewInt(42),
		GasLimit:    30_000_000,
		Time:        1_700_000_000,
		Extra:       []byte{extra},
		BaseFee:     big.NewInt(1_000_000_000),
	}
}

func assertRedactedProviderError(t *testing.T, err error, secrets []string, expected ...string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaked %q: %q", secret, err)
		}
	}
	for _, value := range expected {
		if !strings.Contains(err.Error(), value) {
			t.Fatalf("error = %q, want %q", err, value)
		}
	}
}

func testReceipt() *gethtypes.Receipt {
	txHash := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	return &gethtypes.Receipt{
		TxHash:      txHash,
		Status:      gethtypes.ReceiptStatusSuccessful,
		BlockNumber: big.NewInt(99),
		BlockHash:   common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		Logs: []*gethtypes.Log{{
			Address:     common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Topics:      []common.Hash{common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")},
			Data:        []byte{0x01},
			TxHash:      txHash,
			BlockNumber: 99,
			BlockHash:   common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			Index:       7,
		}},
	}
}
