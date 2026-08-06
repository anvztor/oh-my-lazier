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
	"github.com/ethereum/go-ethereum/common/hexutil"
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
	// Both providers are known caught up, so the NotFound is a genuine dissent.
	client.storeHeadSnapshot(&headSnapshot{
		number: big.NewInt(0).Add(receipt.BlockNumber, big.NewInt(1)),
		tips:   map[int]*big.Int{0: big.NewInt(0).Add(receipt.BlockNumber, big.NewInt(1)), 1: big.NewInt(0).Add(receipt.BlockNumber, big.NewInt(1))},
	})

	_, err := client.TransactionReceipt(context.Background(), receipt.TxHash)
	if err == nil || !IsReceiptConflict(err) {
		t.Fatalf("TransactionReceipt() error = %v, want partial not-found conflict", err)
	}
	assertRedactedProviderError(t, err, []string{firstURL, secondURL, "first-password", "second-api-key"}, "provider[1]")
}

func TestTransactionReceiptUnknownTipNotFoundIsUnavailable(t *testing.T) {
	receipt := testReceipt()
	// Without any head snapshot the dissenting provider's tip is unknown: it
	// cannot be proven caught up, so the round is undecided, not a conflict
	// that would pause a pathway.
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: receipt})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
	}}
	_, err := client.TransactionReceipt(context.Background(), receipt.TxHash)
	if !IsQuorumUnavailable(err) {
		t.Fatalf("TransactionReceipt(unknown tip) error = %v, want quorum unavailable", err)
	}
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

func TestTransactionReceiptRequiresMajorityAgreement(t *testing.T) {
	honest := testReceipt()
	fabricated := testReceipt()
	fabricated.Status = gethtypes.ReceiptStatusFailed
	// One endpoint fabricating receipt content for a canonical block is
	// outvoted by the fixed majority, even though it is classified healthy.
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: honest})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: honest})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: fabricated})},
	}}
	receipt, err := client.TransactionReceipt(context.Background(), honest.TxHash)
	if err != nil {
		t.Fatalf("TransactionReceipt() error = %v", err)
	}
	if receipt.Status != honest.Status {
		t.Fatalf("receipt status = %d, want the majority answer", receipt.Status)
	}

	// The vote spans every CONFIGURED provider: a degraded classification must
	// not shrink the trust set to a single endpoint.
	loneHealthy := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderUnavailable, client: newTestEthClient(t, testEthService{receipt: honest})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: fabricated})},
		{url: "c", status: ProviderUnavailable, client: newTestEthClient(t, testEthService{receipt: honest})},
	}}
	receipt, err = loneHealthy.TransactionReceipt(context.Background(), honest.TxHash)
	if err != nil {
		t.Fatalf("TransactionReceipt(lone healthy) error = %v", err)
	}
	if receipt.Status != honest.Status {
		t.Fatalf("receipt status = %d, want the configured majority to outvote the lone healthy fabricator", receipt.Status)
	}
}

func TestTransactionReceiptExcusesLaggingNotFound(t *testing.T) {
	receipt := testReceipt()
	receipt.BlockNumber = big.NewInt(120)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: receipt})},
		{url: "b", status: ProviderLagging, client: newTestEthClient(t, testEthService{})},
	}}
	// The laggard's tip is below the receipt's block: its NotFound is excused
	// lag, so the round is undecided (retry later), not a conflict that would
	// pause a pathway.
	client.storeHeadSnapshot(&headSnapshot{
		number: big.NewInt(119),
		tips:   map[int]*big.Int{0: big.NewInt(125), 1: big.NewInt(110)},
	})
	_, err := client.TransactionReceipt(context.Background(), receipt.TxHash)
	if !IsQuorumUnavailable(err) {
		t.Fatalf("TransactionReceipt(lagging dissent) error = %v, want quorum unavailable", err)
	}

	// A caught-up provider claiming NotFound is a genuine dissent.
	client.storeHeadSnapshot(&headSnapshot{
		number: big.NewInt(130),
		tips:   map[int]*big.Int{0: big.NewInt(130), 1: big.NewInt(130)},
	})
	_, err = client.TransactionReceipt(context.Background(), receipt.TxHash)
	if !IsReceiptConflict(err) {
		t.Fatalf("TransactionReceipt(caught-up dissent) error = %v, want receipt conflict", err)
	}
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

func TestCheckHeadStepsDownToHonestMajorityHeight(t *testing.T) {
	honestTip := testHeaderAt(100, 0x01)
	honestPrev := testHeaderAt(99, 0x02)
	forkTip := testHeaderAt(100, 0x03)
	forkPrev := testHeaderAt(99, 0x04)
	// One forked endpoint claims the top height while an honest provider lags
	// one block: the 1-1 split at 100 must fall through to the honest
	// majority agreement at 99 instead of pausing the chain.
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: honestTip, headers: map[int64]*gethtypes.Header{99: honestPrev}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: honestPrev})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: forkTip, headers: map[int64]*gethtypes.Header{99: forkPrev}})},
	}}

	head, err := client.CheckHead(context.Background())
	if err != nil {
		t.Fatalf("CheckHead() error = %v", err)
	}
	if head.Number.Uint64() != 99 {
		t.Fatalf("head number = %s, want the honest-majority 99", head.Number)
	}
	if head.Hash != honestPrev.Hash().Hex() {
		t.Fatalf("head hash = %s, want the honest %s", head.Hash, honestPrev.Hash().Hex())
	}
	providers := client.Providers()
	// The round proved the chain only through 99: the honest-but-ahead
	// provider's tip is an unverified descendant and must not stay healthy
	// serving single-source latest reads — only the fully verified tip does.
	if providers[0].Status != ProviderUnavailable {
		t.Fatalf("ahead provider status = %q, want unavailable after a step-down", providers[0].Status)
	}
	if providers[1].Status != ProviderHealthy {
		t.Fatalf("verified-tip provider status = %q, want healthy", providers[1].Status)
	}
	if providers[2].Status != ProviderConflict {
		t.Fatalf("forked provider status = %q, want conflict", providers[2].Status)
	}
}

func TestCanonicalHashAtRequiresMajorityAgreement(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	fork := testHeaderAt(42, 0x02)
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{headers: map[int64]*gethtypes.Header{42: canonical}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{headers: map[int64]*gethtypes.Header{42: canonical}})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{headers: map[int64]*gethtypes.Header{42: fork}})},
	}}
	hash, err := client.CanonicalHashAt(context.Background(), big.NewInt(42))
	if err != nil {
		t.Fatalf("CanonicalHashAt() error = %v", err)
	}
	if hash != canonical.Hash() {
		t.Fatalf("hash = %s, want the majority %s", hash, canonical.Hash())
	}

	split := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{headers: map[int64]*gethtypes.Header{42: canonical}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{headers: map[int64]*gethtypes.Header{42: fork}})},
	}}
	if _, err := split.CanonicalHashAt(context.Background(), big.NewInt(42)); !IsHeadConflict(err) {
		t.Fatalf("CanonicalHashAt(split) error = %v, want head conflict", err)
	}

	failing := testEthService{err: testRPCError{message: "boom", code: -32000}}
	degraded := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{headers: map[int64]*gethtypes.Header{42: canonical}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, failing)},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, failing)},
	}}
	if _, err := degraded.CanonicalHashAt(context.Background(), big.NewInt(42)); !IsQuorumUnavailable(err) {
		t.Fatalf("CanonicalHashAt(degraded) error = %v, want quorum unavailable", err)
	}
}

func TestCheckHeadStillConflictsWithoutAgreementAtAnyHeight(t *testing.T) {
	chainATip := testHeaderAt(100, 0x01)
	chainAPrev := testHeaderAt(99, 0x02)
	chainBTip := testHeaderAt(100, 0x03)
	chainBPrev := testHeaderAt(99, 0x04)
	chainCTip := testHeaderAt(99, 0x05)
	// Three providers on three different chains never agree at any candidate
	// height; the conflict classification must survive the step-down search.
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: chainATip, headers: map[int64]*gethtypes.Header{99: chainAPrev}})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: chainBTip, headers: map[int64]*gethtypes.Header{99: chainBPrev}})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{header: chainCTip})},
	}}

	_, err := client.CheckHead(context.Background())
	if !IsHeadConflict(err) {
		t.Fatalf("CheckHead() error = %v, want head conflict", err)
	}
	// The failed round must not leave any provider healthy: the sub-top-level
	// responder was never majority-verified, and a stale healthy status would
	// hand it the single-source reads that follow.
	for index, provider := range client.Providers() {
		if provider.Status == ProviderHealthy {
			t.Fatalf("provider[%d] status = healthy after an unresolved conflict, want conflict or unavailable", index)
		}
	}
}

func TestNonceAtRequiresMajorityAgreement(t *testing.T) {
	account := common.HexToAddress("0x4141414141414141414141414141414141414141")
	// A single provider lying about the confirmed nonce is outvoted; the
	// reconciler must never park lanes from a fabricated value.
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 900})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 7})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 7})},
	}}
	nonce, err := client.NonceAt(context.Background(), account, big.NewInt(42))
	if err != nil {
		t.Fatalf("NonceAt() error = %v", err)
	}
	if nonce != 7 {
		t.Fatalf("nonce = %d, want the majority 7", nonce)
	}
}

func TestNonceAtConflictWithoutMajorityValue(t *testing.T) {
	account := common.HexToAddress("0x4141414141414141414141414141414141414141")
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 7})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 8})},
	}}
	_, err := client.NonceAt(context.Background(), account, big.NewInt(42))
	if !IsNonceConflict(err) {
		t.Fatalf("NonceAt() error = %v, want nonce quorum conflict", err)
	}
	for _, detail := range []string{"provider[0] returned nonce 7", "provider[1] returned nonce 8"} {
		if !strings.Contains(err.Error(), detail) {
			t.Fatalf("error = %q, want detail %q", err, detail)
		}
	}
}

func TestNonceAtQuorumUnavailableOnInsufficientResponders(t *testing.T) {
	account := common.HexToAddress("0x4141414141414141414141414141414141414141")
	failing := testEthService{nonceErr: testRPCError{message: "boom with rpc-secret", code: -32000}}
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 7})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, failing)},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, failing)},
	}}
	_, err := client.NonceAt(context.Background(), account, big.NewInt(42))
	if !IsQuorumUnavailable(err) {
		t.Fatalf("NonceAt() error = %v, want quorum unavailable", err)
	}
	if strings.Contains(err.Error(), "rpc-secret") {
		t.Fatalf("error leaked provider failure detail: %q", err)
	}
}

func TestNonceAtSingleProviderDegenerate(t *testing.T) {
	account := common.HexToAddress("0x4141414141414141414141414141414141414141")
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 7})},
	}}
	nonce, err := client.NonceAt(context.Background(), account, big.NewInt(42))
	if err != nil || nonce != 7 {
		t.Fatalf("NonceAt() = (%d, %v), want (7, nil)", nonce, err)
	}
}

func TestNonceAtReturnsOnMajorityWithoutWaitingForHangingProvider(t *testing.T) {
	account := common.HexToAddress("0x4141414141414141414141414141414141414141")
	// The probe deadline is deliberately long: only the early return on q
	// identical votes can finish this test quickly. The reconciler shares one
	// RPC budget across the whole pass, so waiting out a hung straggler here
	// would starve its receipt reads.
	client := &Client{chainName: "testnet", probeTimeout: 30 * time.Second, providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 7})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{nonce: 7})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{hang: true})},
	}}
	start := time.Now()
	nonce, err := client.NonceAt(context.Background(), account, big.NewInt(42))
	if err != nil || nonce != 7 {
		t.Fatalf("NonceAt() = (%d, %v), want (7, nil)", nonce, err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("NonceAt() took %s, a satisfied majority must not wait for the hanging probe", elapsed)
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
	header   *gethtypes.Header
	headers  map[int64]*gethtypes.Header
	receipt  *gethtypes.Receipt
	logs     []gethtypes.Log
	logsErr  error
	err      error
	nonce    uint64
	nonceErr error
	// hang blocks header requests until the caller's context expires.
	hang bool
	// logsScript overrides logs/logsErr with per-call scripted answers.
	logsScript *logsScript
	// state-read fakes for the quorum voting paths.
	callResult   []byte
	callErr      error
	callHang     bool
	callDelay    time.Duration
	callObserved *observedBlockRef
	codeResult   []byte
	codeErr      error
	estimateGas  uint64
	estimateErr  error
	pendingNonce uint64
	pendingErr   error
}

// observedBlockRef records the block reference a state read was served at.
type observedBlockRef struct {
	mu   sync.Mutex
	refs []rpc.BlockNumberOrHash
}

func (o *observedBlockRef) record(ref rpc.BlockNumberOrHash) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.refs = append(o.refs, ref)
}

func (o *observedBlockRef) all() []rpc.BlockNumberOrHash {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]rpc.BlockNumberOrHash(nil), o.refs...)
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

func (s testEthService) GetTransactionCount(ctx context.Context, _ common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Uint64, error) {
	if s.hang {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if number, ok := blockNrOrHash.Number(); ok && number == rpc.PendingBlockNumber {
		if s.pendingErr != nil {
			return 0, s.pendingErr
		}
		return hexutil.Uint64(s.pendingNonce), nil
	}
	if s.nonceErr != nil {
		return 0, s.nonceErr
	}
	return hexutil.Uint64(s.nonce), nil
}

func (s testEthService) Call(ctx context.Context, _ map[string]interface{}, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	if s.callObserved != nil {
		s.callObserved.record(blockNrOrHash)
	}
	if s.callDelay > 0 {
		select {
		case <-time.After(s.callDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.callHang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.callErr != nil {
		return nil, s.callErr
	}
	return hexutil.Bytes(s.callResult), nil
}

func (s testEthService) GetCode(_ context.Context, _ common.Address, _ rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	if s.codeErr != nil {
		return nil, s.codeErr
	}
	return hexutil.Bytes(s.codeResult), nil
}

func (s testEthService) EstimateGas(_ context.Context, _ map[string]interface{}) (hexutil.Uint64, error) {
	if s.estimateErr != nil {
		return 0, s.estimateErr
	}
	return hexutil.Uint64(s.estimateGas), nil
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

func TestTransactionReceiptNotFoundMajorityBeatsDivergentMinorities(t *testing.T) {
	first := testReceipt()
	second := testReceipt()
	second.Status = gethtypes.ReceiptStatusFailed
	// A caught-up fixed majority saying "absent" must win even when the two
	// dissenters return DIFFERENT fabricated receipts — otherwise a minority
	// could manufacture a pathway-pausing conflict at will.
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "d", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: first})},
		{url: "e", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: second})},
	}}
	caughtUp := big.NewInt(0).Add(first.BlockNumber, big.NewInt(2))
	client.storeHeadSnapshot(&headSnapshot{
		number: caughtUp,
		tips:   map[int]*big.Int{0: caughtUp, 1: caughtUp, 2: caughtUp, 3: caughtUp, 4: caughtUp},
	})
	_, err := client.TransactionReceipt(context.Background(), first.TxHash)
	if !errors.Is(err, ethereum.NotFound) {
		t.Fatalf("TransactionReceipt() error = %v, want the NotFound majority", err)
	}
}

func TestTransactionReceiptInflatedCandidateBlockDegradesToUnavailable(t *testing.T) {
	first := testReceipt()
	inflated := testReceipt()
	inflated.Status = gethtypes.ReceiptStatusFailed
	inflated.BlockNumber = big.NewInt(10_000)
	// A minority fabricating a receipt at an absurd height excuses every
	// honest NotFound (their tips cannot cover it). That must drain the
	// comparable-evidence pool into unavailability — never manufacture a
	// pathway-pausing conflict.
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "d", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: first})},
		{url: "e", status: ProviderHealthy, client: newTestEthClient(t, testEthService{receipt: inflated})},
	}}
	honestTip := big.NewInt(1_000)
	client.storeHeadSnapshot(&headSnapshot{
		number: honestTip,
		tips:   map[int]*big.Int{0: honestTip, 1: honestTip, 2: honestTip, 3: honestTip, 4: honestTip},
	})
	_, err := client.TransactionReceipt(context.Background(), first.TxHash)
	if !IsQuorumUnavailable(err) {
		t.Fatalf("TransactionReceipt(inflated candidate) error = %v, want quorum unavailable", err)
	}
	if IsReceiptConflict(err) {
		t.Fatal("an inflated candidate block manufactured a receipt conflict")
	}
}

func TestTransactionReceiptAtExcludesLaggingNegatives(t *testing.T) {
	failing := testEthService{err: testRPCError{message: "boom", code: -32000}}
	// No receipt candidate at all: two lagging NotFounds plus one caught-up
	// NotFound and two transient failures must NOT produce an authoritative
	// absence for a tx known to live at block 500 — only caught-up providers
	// can vote it absent.
	client := &Client{chainName: "testnet", providers: []configuredProvider{
		{url: "a", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "b", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "c", status: ProviderHealthy, client: newTestEthClient(t, testEthService{})},
		{url: "d", status: ProviderHealthy, client: newTestEthClient(t, failing)},
		{url: "e", status: ProviderHealthy, client: newTestEthClient(t, failing)},
	}}
	client.storeHeadSnapshot(&headSnapshot{
		number: big.NewInt(600),
		tips:   map[int]*big.Int{0: big.NewInt(400), 1: big.NewInt(450), 2: big.NewInt(600), 3: big.NewInt(600), 4: big.NewInt(600)},
	})
	minBlock := big.NewInt(500)
	if _, err := client.TransactionReceiptAt(context.Background(), common.HexToHash("0x5001"), minBlock); errors.Is(err, ethereum.NotFound) {
		t.Fatal("lagging NotFounds formed an authoritative absence")
	}

	// With every responder caught up, the same answers ARE authoritative.
	client.storeHeadSnapshot(&headSnapshot{
		number: big.NewInt(600),
		tips:   map[int]*big.Int{0: big.NewInt(600), 1: big.NewInt(600), 2: big.NewInt(600), 3: big.NewInt(600), 4: big.NewInt(600)},
	})
	if _, err := client.TransactionReceiptAt(context.Background(), common.HexToHash("0x5001"), minBlock); !errors.Is(err, ethereum.NotFound) {
		t.Fatalf("caught-up NotFound majority = %v, want ethereum.NotFound", err)
	}
}

// testCodedRevertError is a data-carrying revert reported under an arbitrary
// RPC error code (geth-family endpoints disagree on 3 vs -32000).
type testCodedRevertError struct {
	message string
	code    int
	data    any
}

func (e testCodedRevertError) Error() string  { return e.message }
func (e testCodedRevertError) ErrorCode() int { return e.code }
func (e testCodedRevertError) ErrorData() any { return e.data }

type testRevertError struct {
	message string
	data    string
}

func (e testRevertError) Error() string  { return e.message }
func (e testRevertError) ErrorCode() int { return 3 }
func (e testRevertError) ErrorData() any { return e.data }

func stateReadTestClient(t *testing.T, services ...testEthService) *Client {
	t.Helper()
	providers := make([]configuredProvider, len(services))
	for index, service := range services {
		providers[index] = configuredProvider{url: string(rune('a' + index)), status: ProviderHealthy, client: newTestEthClient(t, service)}
	}
	return &Client{chainName: "testnet", providers: providers}
}

func TestCallContractMajorityWinsAndMarksDissenter(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	agreed := []byte{0x0a}
	// The agreeing majority answers after a short delay so the dissenter's
	// vote always lands before the early-exit cancellation.
	client := stateReadTestClient(t,
		testEthService{header: canonical, callResult: agreed, callDelay: 50 * time.Millisecond},
		testEthService{header: canonical, callResult: agreed, callDelay: 50 * time.Millisecond},
		testEthService{header: canonical, callResult: []byte{0x0b}},
	)

	result, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if err != nil {
		t.Fatalf("CallContract() error = %v", err)
	}
	if string(result) != string(agreed) {
		t.Fatalf("result = %x, want the majority answer", result)
	}
	providers := client.Providers()
	if providers[2].StateConflict != true {
		t.Fatal("dissenting provider must carry the state conflict flag")
	}
	if providers[0].StateConflict || providers[1].StateConflict {
		t.Fatal("agreeing providers must not carry the state conflict flag")
	}
	// State reads never move the head status dimension.
	for index, provider := range providers {
		if provider.Status != ProviderHealthy {
			t.Fatalf("provider[%d] head status = %q, want untouched healthy", index, provider.Status)
		}
	}
}

func TestCallContractSplitIsConflictNotUnavailable(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, callResult: []byte{0x01}},
		testEthService{header: canonical, callResult: []byte{0x02}},
		testEthService{header: canonical, callResult: []byte{0x03}},
	)

	_, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if !IsStateReadConflict(err) {
		t.Fatalf("CallContract() error = %v, want state read conflict", err)
	}
	if IsQuorumUnavailable(err) {
		t.Fatal("a comparable split must not read as unavailability")
	}
	for index, provider := range client.Providers() {
		if !provider.StateConflict {
			t.Fatalf("provider[%d] must be marked in a no-majority split", index)
		}
	}
}

func TestCallContractTooFewComparableIsUnavailable(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, callResult: []byte{0x01}},
		testEthService{header: canonical, callErr: testRPCError{message: "boom", code: -32000}},
		testEthService{header: canonical, callErr: testRPCError{message: "boom", code: -32000}},
	)

	_, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if !IsQuorumUnavailable(err) {
		t.Fatalf("CallContract() error = %v, want quorum unavailable", err)
	}
}

func TestCallContractRevertMajorityKeepsClassification(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	revert := testRevertError{message: "execution reverted: denied", data: "0x08c379a0"}
	client := stateReadTestClient(t,
		testEthService{header: canonical, callErr: revert},
		testEthService{header: canonical, callErr: revert},
		testEthService{header: canonical, callResult: []byte{0x01}},
	)

	_, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if err == nil {
		t.Fatal("CallContract() error = nil, want the majority revert")
	}
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) || rpcErr.ErrorCode() != 3 {
		t.Fatalf("majority revert must keep its rpc error identity, got %v", err)
	}
	if !IsVotedRevert(err) {
		t.Fatalf("majority revert must carry the voted-revert marker, got %v", err)
	}
}

func TestCallContractMessageOnlyRevertMajorityWins(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	revert := testRPCError{message: "execution reverted: denied", code: -32000}
	client := stateReadTestClient(t,
		testEthService{header: canonical, callErr: revert},
		testEthService{header: canonical, callErr: revert},
		testEthService{header: canonical, callResult: []byte{0x01}},
	)

	_, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if err == nil {
		t.Fatal("CallContract() error = nil, want the majority message-only revert")
	}
	if IsQuorumUnavailable(err) || IsStateReadConflict(err) {
		t.Fatalf("a message-only revert majority must win the vote, got %v", err)
	}
	// Downstream terminal classification walks the unwrap chain for the
	// original rpc error text; it must survive the provider redaction wrapper.
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) || !strings.Contains(strings.ToLower(rpcErr.Error()), "execution reverted") {
		t.Fatalf("majority message-only revert lost its revert text: %v", err)
	}
}

func TestCallContractDataLessCode3ReasonsDoNotMerge(t *testing.T) {
	// Two data-less code-3 reverts with different reasons must not merge into
	// a false revert majority over a successful answer: the identity is the
	// normalized message, never the shared RPC code.
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, callErr: testRPCError{message: "execution reverted: A", code: 3}},
		testEthService{header: canonical, callErr: testRPCError{message: "execution reverted: B", code: 3}},
		testEthService{header: canonical, callResult: []byte{0x01}},
	)

	_, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if !IsStateReadConflict(err) {
		t.Fatalf("CallContract() error = %v, want conflict (shared code 3 must not merge distinct reasons)", err)
	}
}

func TestCallContractSameRevertDataMergesAcrossRPCCodes(t *testing.T) {
	// The same canonical revert data reported under different RPC codes is one
	// semantic answer and must reach the revert majority.
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, callErr: testRevertError{message: "execution reverted: denied", data: "0x08c379a0"}},
		testEthService{header: canonical, callErr: testCodedRevertError{message: "execution reverted: denied", code: -32000, data: "0x08c379a0"}},
		testEthService{header: canonical, callResult: []byte{0x01}},
	)

	_, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if err == nil {
		t.Fatal("CallContract() error = nil, want the majority revert")
	}
	if IsStateReadConflict(err) || IsQuorumUnavailable(err) {
		t.Fatalf("same revert data across rpc codes must merge into a revert majority, got %v", err)
	}
}

func TestEstimateGasNonRevertDataDoesNotFormRevertMajority(t *testing.T) {
	// Two differently-coded transient failures carrying identical
	// provider-specific ErrorData must stay non-comparable: merging them would
	// out-vote the healthy estimate and terminally fail the queued transaction
	// as an estimate revert.
	canonical := testHeaderAt(42, 0x01)
	diagnostic := map[string]string{"reason": "rate limited"}
	client := stateReadTestClient(t,
		testEthService{header: canonical, estimateErr: testCodedRevertError{message: "backend unavailable", code: -32603, data: diagnostic}},
		testEthService{header: canonical, estimateErr: testCodedRevertError{message: "upstream busy", code: -32000, data: diagnostic}},
		testEthService{header: canonical, estimateGas: 100},
	)

	_, err := client.EstimateGas(context.Background(), ethereum.CallMsg{})
	if !IsQuorumUnavailable(err) {
		t.Fatalf("EstimateGas() error = %v, want quorum unavailable (non-revert data must not vote)", err)
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		t.Fatalf("unavailable error leaked an rpc revert identity: %v", err)
	}
}

func TestRevertEvidenceAcrossClientShapes(t *testing.T) {
	// One table over the revert shapes the common clients emit and the
	// non-revert failures that must never be mistaken for them.
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"geth code 3", testRevertError{message: "execution reverted: denied", data: "0x08c379a0"}, true},
		{"geth message only", testRPCError{message: "execution reverted", code: -32000}, true},
		{"erigon message", testRPCError{message: "execution reverted: ERC20: insufficient balance", code: -32000}, true},
		{"ganache vm exception", testRPCError{message: "VM Exception while processing transaction: revert MyError", code: -32000}, true},
		{"hardhat reverted", testRPCError{message: "Error: VM Exception while processing transaction: reverted with reason string 'x'", code: -32603}, true},
		{"bare revert", testRPCError{message: "revert", code: -32015}, true},
		{"nethermind vm execution error with revert data", testCodedRevertError{message: "VM execution error.", code: -32015, data: "0x08c379a0"}, true},
		{"nethermind vm execution error without data", testRPCError{message: "VM execution error.", code: -32015}, false},
		{"vm execution error with opaque data", testCodedRevertError{message: "VM execution error.", code: -32015, data: map[string]string{"id": "1"}}, false},
		{"transport failure", testRPCError{message: "backend unavailable", code: -32603}, false},
		{"hex diagnostic without claim", testCodedRevertError{message: "upstream busy", code: -32000, data: "0xaabbcc"}, false},
		{"unrelated wording", testRPCError{message: "reverting proxy misconfigured", code: -32000}, false},
		{"non-rpc error", errors.New("execution reverted"), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isDeterministicRevert(test.err); got != test.want {
				t.Fatalf("isDeterministicRevert(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestEstimateGasNonGethRevertMajorityWins(t *testing.T) {
	// A majority of non-geth endpoints reporting the same revert must reach
	// terminal revert handling, not degrade to quorum-unavailable (which would
	// keep a deterministically failing outbox row queued and retrying).
	canonical := testHeaderAt(42, 0x01)
	revert := testCodedRevertError{message: "VM execution error.", code: -32015, data: "0x08c379a0"}
	client := stateReadTestClient(t,
		testEthService{header: canonical, estimateErr: revert},
		testEthService{header: canonical, estimateErr: revert},
		testEthService{header: canonical, estimateGas: 100},
	)

	_, err := client.EstimateGas(context.Background(), ethereum.CallMsg{})
	if err == nil {
		t.Fatal("EstimateGas() error = nil, want the majority revert")
	}
	if IsQuorumUnavailable(err) || IsEstimateGasConflict(err) {
		t.Fatalf("non-geth revert majority must win the vote, got %v", err)
	}
	// Downstream terminal classification consumes the quorum's verdict instead
	// of re-deriving it from this client's wording.
	if !IsVotedRevert(err) {
		t.Fatalf("majority revert must carry the voted-revert marker, got %v", err)
	}
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("voted revert marker must unwrap to the provider rpc error: %v", err)
	}
}

func TestEstimateGasHexDiagnosticWithoutRevertClaimDoesNotVote(t *testing.T) {
	// Syntactically valid hex ErrorData is not revert semantics: an opaque
	// diagnostic (a request or block hash) returned under non-revert codes and
	// messages, even in differing case, must not form a revert majority that
	// would terminally fail the queued transaction.
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, estimateErr: testCodedRevertError{message: "backend unavailable", code: -32603, data: "0xAABBCC"}},
		testEthService{header: canonical, estimateErr: testCodedRevertError{message: "upstream busy", code: -32000, data: "0xaabbcc"}},
		testEthService{header: canonical, estimateGas: 100},
	)

	_, err := client.EstimateGas(context.Background(), ethereum.CallMsg{})
	if !IsQuorumUnavailable(err) {
		t.Fatalf("EstimateGas() error = %v, want quorum unavailable (hex diagnostics must not vote)", err)
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		t.Fatalf("unavailable error leaked an rpc revert identity: %v", err)
	}
}

func TestCallContractMessageOnlyRevertReasonsDoNotMerge(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, callErr: testRPCError{message: "execution reverted: A", code: -32000}},
		testEthService{header: canonical, callErr: testRPCError{message: "execution reverted: B", code: -32000}},
		testEthService{header: canonical, callResult: []byte{0x01}},
	)

	_, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if !IsStateReadConflict(err) {
		t.Fatalf("CallContract() error = %v, want conflict (distinct revert reasons must not merge)", err)
	}
}

func TestEstimateGasMessageOnlyRevertMajorityWins(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	revert := testRPCError{message: "execution reverted: denied", code: -32000}
	client := stateReadTestClient(t,
		testEthService{header: canonical, estimateErr: revert},
		testEthService{header: canonical, estimateErr: revert},
		testEthService{header: canonical, estimateGas: 100},
	)

	_, err := client.EstimateGas(context.Background(), ethereum.CallMsg{})
	if err == nil {
		t.Fatal("EstimateGas() error = nil, want the majority message-only revert")
	}
	if IsQuorumUnavailable(err) || IsEstimateGasConflict(err) {
		t.Fatalf("a message-only revert majority must win the vote, got %v", err)
	}
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) || !strings.Contains(strings.ToLower(rpcErr.Error()), "execution reverted") {
		t.Fatalf("majority message-only revert lost its revert text: %v", err)
	}
}

func TestCallContractMixedRevertsDoNotMerge(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, callErr: testRevertError{message: "execution reverted: A", data: "0x01"}},
		testEthService{header: canonical, callErr: testRevertError{message: "execution reverted: B", data: "0x02"}},
		testEthService{header: canonical, callResult: []byte{0x01}},
	)

	_, err := client.CallContract(context.Background(), ethereum.CallMsg{}, big.NewInt(42))
	if !IsStateReadConflict(err) {
		t.Fatalf("CallContract() error = %v, want conflict (distinct reverts must not merge)", err)
	}
	// The aggregate error must not expose any minority revert identity, so
	// downstream deterministic-revert classification cannot misfire.
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		t.Fatalf("conflict error leaked an rpc revert identity: %v", err)
	}
}

func TestCallContractNilAnchorsToCanonicalHash(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	observedA := &observedBlockRef{}
	observedB := &observedBlockRef{}
	observedC := &observedBlockRef{}
	agreed := []byte{0x0a}
	client := stateReadTestClient(t,
		testEthService{header: canonical, callResult: agreed, callObserved: observedA},
		testEthService{header: canonical, callResult: agreed, callObserved: observedB},
		testEthService{header: canonical, callResult: agreed, callObserved: observedC},
	)

	if _, err := client.CallContract(context.Background(), ethereum.CallMsg{}, nil); err != nil {
		t.Fatalf("CallContract(nil) error = %v", err)
	}
	want := canonical.Hash()
	for index, observed := range []*observedBlockRef{observedA, observedB, observedC} {
		refs := observed.all()
		if len(refs) == 0 {
			continue // canceled straggler after early majority
		}
		hash, ok := refs[0].Hash()
		if !ok || hash != want {
			t.Fatalf("provider[%d] served block ref %+v, want the canonical hash %s", index, refs[0], want)
		}
	}
}

func TestCodeAtNormalizesEmptyCode(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, codeResult: nil},
		testEthService{header: canonical, codeResult: []byte{}},
		testEthService{header: canonical, codeResult: []byte{0x60}},
	)

	code, err := client.CodeAt(context.Background(), common.Address{}, big.NewInt(42))
	if err != nil {
		t.Fatalf("CodeAt() error = %v", err)
	}
	if len(code) != 0 {
		t.Fatalf("code = %x, want the empty-code majority", code)
	}
}

func TestEstimateGasBoundedAggregation(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	tests := []struct {
		name      string
		estimates [3]uint64
		want      uint64
	}{
		// The lone honest high estimate survives inside the bounded set.
		{name: "honestHighSurvives", estimates: [3]uint64{50, 50, 100}, want: 100},
		// Amplification is capped by the bounded set around the upper median.
		{name: "amplificationBounded", estimates: [3]uint64{100, 200, 200}, want: 200},
		// An extreme outlier is excluded and marked, majority remains.
		{name: "outlierExcluded", estimates: [3]uint64{100, 100, 10_000}, want: 100},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := stateReadTestClient(t,
				testEthService{header: canonical, estimateGas: test.estimates[0]},
				testEthService{header: canonical, estimateGas: test.estimates[1]},
				testEthService{header: canonical, estimateGas: test.estimates[2]},
			)
			gas, err := client.EstimateGas(context.Background(), ethereum.CallMsg{})
			if err != nil {
				t.Fatalf("EstimateGas() error = %v", err)
			}
			if gas != test.want {
				t.Fatalf("EstimateGas() = %d, want %d", gas, test.want)
			}
		})
	}
}

func TestEstimateGasRevertMajorityKeepsClassification(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	revert := testRevertError{message: "execution reverted: denied", data: "0x08c379a0"}
	client := stateReadTestClient(t,
		testEthService{header: canonical, estimateErr: revert},
		testEthService{header: canonical, estimateErr: revert},
		testEthService{header: canonical, estimateGas: 100},
	)

	_, err := client.EstimateGas(context.Background(), ethereum.CallMsg{})
	if err == nil {
		t.Fatal("EstimateGas() error = nil, want the majority revert")
	}
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) || rpcErr.ErrorCode() != 3 {
		t.Fatalf("majority revert must keep its rpc error identity, got %v", err)
	}
}

func TestEstimateGasSplitDoesNotExposeRevert(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	client := stateReadTestClient(t,
		testEthService{header: canonical, estimateErr: testRevertError{message: "execution reverted: A", data: "0x01"}},
		testEthService{header: canonical, estimateGas: 100},
		testEthService{header: canonical, estimateErr: testRPCError{message: "boom", code: -32000}},
	)

	_, err := client.EstimateGas(context.Background(), ethereum.CallMsg{})
	if err == nil {
		t.Fatal("EstimateGas() error = nil, want conflict")
	}
	if !IsEstimateGasConflict(err) {
		t.Fatalf("EstimateGas() error = %v, want estimate conflict", err)
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		t.Fatalf("conflict error leaked an rpc revert identity: %v", err)
	}
}

func TestEstimateGasRejectsAboveBlockGasLimit(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	over := canonical.GasLimit + 1
	client := stateReadTestClient(t,
		testEthService{header: canonical, estimateGas: over},
		testEthService{header: canonical, estimateGas: over},
		testEthService{header: canonical, estimateGas: over},
	)

	if _, err := client.EstimateGas(context.Background(), ethereum.CallMsg{}); !IsEstimateGasConflict(err) {
		t.Fatalf("EstimateGas() error = %v, want block-gas-limit refusal", err)
	}
}

func TestPendingNonceAtRequiresIdenticalMajorityAboveConfirmed(t *testing.T) {
	canonical := testHeaderAt(42, 0x01)
	agree := stateReadTestClient(t,
		testEthService{header: canonical, nonce: 7, pendingNonce: 9},
		testEthService{header: canonical, nonce: 7, pendingNonce: 9},
		testEthService{header: canonical, nonce: 7, pendingNonce: 11},
	)
	nonce, err := agree.PendingNonceAt(context.Background(), common.Address{})
	if err != nil || nonce != 9 {
		t.Fatalf("PendingNonceAt() = (%d, %v), want the identical majority 9", nonce, err)
	}

	split := stateReadTestClient(t,
		testEthService{header: canonical, nonce: 7, pendingNonce: 9},
		testEthService{header: canonical, nonce: 7, pendingNonce: 10},
		testEthService{header: canonical, nonce: 7, pendingNonce: 11},
	)
	if _, err := split.PendingNonceAt(context.Background(), common.Address{}); !IsStateReadConflict(err) {
		t.Fatalf("PendingNonceAt(split) error = %v, want fail-closed conflict", err)
	}

	// A majority pending below the confirmed majority nonce is inconsistent
	// and must fail closed instead of bootstrapping a cursor into the past.
	below := stateReadTestClient(t,
		testEthService{header: canonical, nonce: 7, pendingNonce: 5},
		testEthService{header: canonical, nonce: 7, pendingNonce: 5},
		testEthService{header: canonical, nonce: 7, pendingNonce: 5},
	)
	if _, err := below.PendingNonceAt(context.Background(), common.Address{}); !IsStateReadConflict(err) {
		t.Fatalf("PendingNonceAt(below confirmed) error = %v, want fail-closed conflict", err)
	}
}
