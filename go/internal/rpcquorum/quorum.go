package rpcquorum

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// defaultProbeTimeout bounds every per-provider request made inside a quorum
// round (head probes, canonical-height votes, log windows), so one hanging
// provider can delay a round by at most this much instead of freezing startup,
// the indexer, and every canonical-head read behind headMu forever.
const defaultProbeTimeout = 15 * time.Second

var _ interface {
	BlockNumber(context.Context) (uint64, error)
	BalanceAt(context.Context, common.Address, *big.Int) (*big.Int, error)
	CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error)
	ChainID(context.Context) (*big.Int, error)
	CheckHead(context.Context) (HeadResult, error)
	CodeAt(context.Context, common.Address, *big.Int) ([]byte, error)
	EstimateGas(context.Context, ethereum.CallMsg) (uint64, error)
	FilterLogs(context.Context, ethereum.FilterQuery) ([]gethtypes.Log, error)
	HeaderByNumber(context.Context, *big.Int) (*gethtypes.Header, error)
	NonceAt(context.Context, common.Address, *big.Int) (uint64, error)
	PendingNonceAt(context.Context, common.Address) (uint64, error)
	SendTransaction(context.Context, *gethtypes.Transaction) error
	SuggestGasPrice(context.Context) (*big.Int, error)
	SuggestGasTipCap(context.Context) (*big.Int, error)
	TransactionReceipt(context.Context, common.Hash) (*gethtypes.Receipt, error)
} = (*Client)(nil)

// ProviderStatus is the worker's health classification for one RPC provider.
type ProviderStatus string

const (
	// ProviderHealthy means the provider agrees with quorum checks.
	ProviderHealthy ProviderStatus = "healthy"
	// ProviderLagging means the provider is behind the selected chain head.
	ProviderLagging ProviderStatus = "lagging"
	// ProviderConflict means the provider disagrees on canonical chain data.
	ProviderConflict ProviderStatus = "conflict"
	// ProviderUnavailable means the provider failed its most recent quorum
	// probe, or its answer could not be majority-verified because the round
	// itself failed. A provider never retains a stale healthy classification
	// across a failed request or a failed quorum round, so single-source reads
	// cannot silently degrade to one unverified endpoint.
	ProviderUnavailable ProviderStatus = "unavailable"
)

// Provider describes one redacted RPC provider identity and its current health status.
type Provider struct {
	ID     string
	Status ProviderStatus
	// LogConflict is a separate dimension from the head status: it marks a
	// provider whose last log-window answer disagreed with the log quorum, and
	// only a later agreeing log window clears it (head checks never do).
	LogConflict bool
}

type configuredProvider struct {
	url         string
	status      ProviderStatus
	logConflict bool
	client      *ethclient.Client
}

type providerOperationError struct {
	providerID string
	operation  string
	cause      error
}

func (e *providerOperationError) Error() string {
	if e == nil {
		return "rpc provider operation failed"
	}
	return fmt.Sprintf("%s %s failed", e.providerID, e.operation)
}

func (e *providerOperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func providerID(index int) string {
	return fmt.Sprintf("provider[%d]", index)
}

func wrapProviderOperationError(index int, operation string, err error) error {
	if err == nil {
		return nil
	}
	return &providerOperationError{providerID: providerID(index), operation: operation, cause: err}
}

// Client coordinates multiple RPC providers for one chain.
type Client struct {
	chainName string
	mu        sync.Mutex
	providers []configuredProvider
	// headMu serializes CheckHead so a slow older probe cannot overwrite a newer
	// snapshot; head is the latest successful quorum snapshot.
	headMu sync.Mutex
	head   *headSnapshot
	// probeTimeout is the per-provider deadline inside quorum rounds.
	probeTimeout time.Duration
}

// headSnapshot is the immutable result of one successful quorum head check.
type headSnapshot struct {
	number *big.Int
	hash   common.Hash
	// tips are the observed tip numbers of the providers that responded.
	tips map[int]*big.Int
}

// HeadResult is the canonical head selected by quorum checks.
type HeadResult struct {
	Number *big.Int
	Hash   string
}

// HeadConflictError reports a same-height block hash disagreement between RPC providers.
type HeadConflictError struct {
	ChainName string
	Number    *big.Int
	Details   []string
}

// Error returns the provider disagreement details.
func (e *HeadConflictError) Error() string {
	if e == nil {
		return "rpc head quorum conflict"
	}
	number := "<unknown>"
	if e.Number != nil {
		number = e.Number.String()
	}
	if len(e.Details) == 0 {
		return fmt.Sprintf("rpc head quorum conflict for chain %s at block %s", e.ChainName, number)
	}
	return fmt.Sprintf("rpc head quorum conflict for chain %s at block %s: %s", e.ChainName, number, strings.Join(e.Details, "; "))
}

// IsHeadConflict reports whether err is a head quorum conflict.
func IsHeadConflict(err error) bool {
	var conflict *HeadConflictError
	return errors.As(err, &conflict)
}

// QuorumUnavailableError reports that fewer providers than the fixed configured
// majority produced a usable answer; it is an availability problem, not a fork.
type QuorumUnavailableError struct {
	ChainName string
	Details   []string
}

// Error returns the availability details.
func (e *QuorumUnavailableError) Error() string {
	if e == nil {
		return "rpc quorum unavailable"
	}
	if len(e.Details) == 0 {
		return fmt.Sprintf("rpc quorum unavailable for chain %s", e.ChainName)
	}
	return fmt.Sprintf("rpc quorum unavailable for chain %s: %s", e.ChainName, strings.Join(e.Details, "; "))
}

// IsQuorumUnavailable reports whether err is a quorum availability failure.
func IsQuorumUnavailable(err error) bool {
	var unavailable *QuorumUnavailableError
	return errors.As(err, &unavailable)
}

// LogConflictError reports a bounded log-window disagreement where no fixed
// configured majority of providers returned the same normalized log sequence.
type LogConflictError struct {
	ChainName string
	FromBlock uint64
	ToBlock   uint64
	Details   []string
}

// Error returns the provider disagreement details.
func (e *LogConflictError) Error() string {
	if e == nil {
		return "rpc log quorum conflict"
	}
	if len(e.Details) == 0 {
		return fmt.Sprintf("rpc log quorum conflict for chain %s blocks [%d, %d]", e.ChainName, e.FromBlock, e.ToBlock)
	}
	return fmt.Sprintf("rpc log quorum conflict for chain %s blocks [%d, %d]: %s", e.ChainName, e.FromBlock, e.ToBlock, strings.Join(e.Details, "; "))
}

// IsLogConflict reports whether err is a log quorum conflict.
func IsLogConflict(err error) bool {
	var conflict *LogConflictError
	return errors.As(err, &conflict)
}

// ReceiptConflictError reports a source-chain receipt disagreement between RPC providers.
type ReceiptConflictError struct {
	TxHash  common.Hash
	Details []string
}

// Error returns the provider disagreement details.
func (e *ReceiptConflictError) Error() string {
	if e == nil {
		return "rpc receipt quorum conflict"
	}
	if len(e.Details) == 0 {
		return fmt.Sprintf("rpc receipt quorum conflict for tx %s", e.TxHash)
	}
	return fmt.Sprintf("rpc receipt quorum conflict for tx %s: %s", e.TxHash, strings.Join(e.Details, "; "))
}

// IsReceiptConflict reports whether err is a receipt quorum conflict.
func IsReceiptConflict(err error) bool {
	var conflict *ReceiptConflictError
	return errors.As(err, &conflict)
}

// ChainIDMismatchError reports configured RPC providers that do not match the expected EVM chain ID.
type ChainIDMismatchError struct {
	ChainName string
	Expected  *big.Int
	Details   []string
}

// Error returns provider chain ID mismatch details.
func (e *ChainIDMismatchError) Error() string {
	if e == nil {
		return "rpc chain_id mismatch"
	}
	expected := "<unknown>"
	if e.Expected != nil {
		expected = e.Expected.String()
	}
	if len(e.Details) == 0 {
		return fmt.Sprintf("rpc chain_id mismatch for chain %s, expected %s", e.ChainName, expected)
	}
	return fmt.Sprintf("rpc chain_id mismatch for chain %s, expected %s: %s", e.ChainName, expected, strings.Join(e.Details, "; "))
}

// IsChainIDMismatch reports whether err is a provider chain ID mismatch.
func IsChainIDMismatch(err error) bool {
	var mismatch *ChainIDMismatchError
	return errors.As(err, &mismatch)
}

// New constructs a quorum client from configured RPC URLs.
func New(chainName string, urls []string) *Client {
	providers := make([]configuredProvider, 0, len(urls))
	for _, url := range urls {
		providers = append(providers, configuredProvider{url: url, status: ProviderHealthy})
	}
	return &Client{chainName: chainName, providers: providers, probeTimeout: defaultProbeTimeout}
}

// Providers returns a copy of the configured provider statuses.
func (c *Client) Providers() []Provider {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Provider, len(c.providers))
	for index, provider := range c.providers {
		out[index] = Provider{ID: providerID(index), Status: provider.status, LogConflict: provider.logConflict}
	}
	return out
}

// Close releases cached RPC provider connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.providers {
		if c.providers[i].client != nil {
			c.providers[i].client.Close()
			c.providers[i].client = nil
		}
	}
}

// CheckHead establishes the quorum head: the canonical number is the height a
// fixed configured majority of providers has reached (the q-th highest tip,
// q = floor(N/2)+1 over the CONFIGURED provider count, never recomputed from
// this round's responders), and the canonical hash must be reported at that
// height by at least q providers. A single provider can therefore neither lift
// the trusted head with an inflated tip nor stall it, and a minority fork
// dissenter is marked conflicted without failing the chain; only the absence
// of any majority hash is a HeadConflictError. Too few usable responses is a
// QuorumUnavailableError, not a fork. Provider health is reclassified on every
// call (request failures never retain a stale healthy status), and the
// resulting snapshot pins the tips used to prefer near-canonical providers for
// single-source reads.
func (c *Client) CheckHead(ctx context.Context) (HeadResult, error) {
	c.headMu.Lock()
	defer c.headMu.Unlock()
	providers := c.snapshotProviders()
	total := len(providers)
	if total == 0 {
		return HeadResult{}, errors.New("no rpc providers configured")
	}
	quorum := total/2 + 1

	// Tip probe: every configured provider concurrently, each under its own
	// deadline; goroutines only fill their own slot, all aggregation happens
	// single-threaded after Wait.
	type tipProbe struct {
		header *gethtypes.Header
		err    error
	}
	probes := make([]tipProbe, total)
	var wg sync.WaitGroup
	for index := range providers {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			probeCtx, cancel := c.probeContext(ctx)
			defer cancel()
			header, err := c.headerByNumberFromProvider(probeCtx, index, nil)
			probes[index] = tipProbe{header: header, err: err}
		}(index)
	}
	wg.Wait()

	statuses := make(map[int]ProviderStatus, total)
	tips := make(map[int]*big.Int, total)
	tipHashes := make(map[int]common.Hash, total)
	var failureDetails []string
	for index, probe := range probes {
		if probe.err != nil || probe.header == nil || probe.header.Number == nil {
			statuses[index] = ProviderUnavailable
			failureDetails = append(failureDetails, fmt.Sprintf("%s head unavailable", providerID(index)))
			continue
		}
		tips[index] = new(big.Int).Set(probe.header.Number)
		tipHashes[index] = probe.header.Hash()
	}
	if len(tips) < quorum {
		markUnverifiedUnavailable(statuses, total)
		c.applyProviderStatuses(statuses)
		return HeadResult{}, &QuorumUnavailableError{
			ChainName: c.chainName,
			Details:   append(failureDetails, fmt.Sprintf("%d of %d configured providers responded, quorum is %d", len(tips), total, quorum)),
		}
	}

	// Canonical number: the height a configured majority has reached.
	numbers := make([]*big.Int, 0, len(tips))
	for _, tip := range tips {
		numbers = append(numbers, tip)
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i].Cmp(numbers[j]) > 0 })
	canonicalNumber := new(big.Int).Set(numbers[quorum-1])

	// Canonical hash: every responder at or above the canonical height votes
	// with its block hash at exactly that height (tips reuse the probe header;
	// higher tips fetch the historical header concurrently into per-slot
	// results, merged only after Wait).
	votes := make(map[int]common.Hash, len(tips))
	var fetchIndices []int
	for index, tip := range tips {
		switch tip.Cmp(canonicalNumber) {
		case -1:
			statuses[index] = ProviderLagging
		case 0:
			votes[index] = tipHashes[index]
		default:
			fetchIndices = append(fetchIndices, index)
		}
	}
	fetchedHeaders := make([]*gethtypes.Header, len(fetchIndices))
	fetchedErrs := make([]error, len(fetchIndices))
	var voteWG sync.WaitGroup
	for slot, index := range fetchIndices {
		voteWG.Add(1)
		go func(slot, index int) {
			defer voteWG.Done()
			probeCtx, cancel := c.probeContext(ctx)
			defer cancel()
			fetchedHeaders[slot], fetchedErrs[slot] = c.headerByNumberFromProvider(probeCtx, index, canonicalNumber)
		}(slot, index)
	}
	voteWG.Wait()
	for slot, index := range fetchIndices {
		if fetchedErrs[slot] != nil || fetchedHeaders[slot] == nil {
			statuses[index] = ProviderUnavailable
			failureDetails = append(failureDetails, fmt.Sprintf("%s canonical header unavailable", providerID(index)))
			continue
		}
		votes[index] = fetchedHeaders[slot].Hash()
	}
	if len(votes) < quorum {
		markUnverifiedUnavailable(statuses, total)
		c.applyProviderStatuses(statuses)
		return HeadResult{}, &QuorumUnavailableError{
			ChainName: c.chainName,
			Details:   append(failureDetails, fmt.Sprintf("%d of %d configured providers served the canonical height, quorum is %d", len(votes), total, quorum)),
		}
	}
	voteCounts := make(map[common.Hash]int, len(votes))
	for _, hash := range votes {
		voteCounts[hash]++
	}
	var canonicalHash common.Hash
	best := 0
	for hash, count := range voteCounts {
		if count > best {
			best = count
			canonicalHash = hash
		}
	}
	if best < quorum {
		details := make([]string, 0, len(votes))
		for index, hash := range votes {
			details = append(details, fmt.Sprintf("%s returned %s", providerID(index), hash))
			statuses[index] = ProviderConflict
		}
		sort.Strings(details)
		c.applyProviderStatuses(statuses)
		return HeadResult{}, &HeadConflictError{
			ChainName: c.chainName,
			Number:    new(big.Int).Set(canonicalNumber),
			Details:   details,
		}
	}
	for index, hash := range votes {
		if hash == canonicalHash {
			statuses[index] = ProviderHealthy
		} else {
			statuses[index] = ProviderConflict
		}
	}

	c.applyProviderStatuses(statuses)
	c.storeHeadSnapshot(&headSnapshot{number: new(big.Int).Set(canonicalNumber), hash: canonicalHash, tips: tips})
	return HeadResult{Number: new(big.Int).Set(canonicalNumber), Hash: canonicalHash.Hex()}, nil
}

// markUnverifiedUnavailable downgrades every provider a failed quorum round
// left unclassified: a responder whose answer was never majority-verified must
// not keep a stale healthy status and serve single-source reads afterwards.
// probeContext bounds one per-provider quorum request; a zero-value Client
// still gets the default deadline.
func (c *Client) probeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := c.probeTimeout
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func markUnverifiedUnavailable(statuses map[int]ProviderStatus, total int) {
	for index := 0; index < total; index++ {
		if _, ok := statuses[index]; !ok {
			statuses[index] = ProviderUnavailable
		}
	}
}

func (c *Client) applyProviderStatuses(statuses map[int]ProviderStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for index, status := range statuses {
		if index < 0 || index >= len(c.providers) {
			continue
		}
		c.providers[index].status = status
	}
}

func (c *Client) storeHeadSnapshot(snapshot *headSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.head = snapshot
}

func (c *Client) headSnapshotRef() *headSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.head
}

// CallContract performs an eth_call against the first currently healthy provider.
func (c *Client) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return nil, err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	result, err := client.CallContract(ctx, call, blockNumber)
	return result, wrapProviderOperationError(index, "eth_call", err)
}

// BlockNumber returns the quorum canonical head number: the height a fixed
// configured majority of providers has reached. Confirmation-depth gates built
// on it can no longer be lifted by a single provider's inflated tip.
func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	head, err := c.CheckHead(ctx)
	if err != nil {
		return 0, err
	}
	if head.Number == nil || !head.Number.IsUint64() {
		return 0, fmt.Errorf("quorum head number for chain %s is not a uint64", c.chainName)
	}
	return head.Number.Uint64(), nil
}

// ChainID returns the first healthy provider's native EVM chain ID.
func (c *Client) ChainID(ctx context.Context) (*big.Int, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return nil, err
	}
	return c.chainIDFromProvider(ctx, index)
}

// BalanceAt returns the first healthy provider's native token balance for an account.
func (c *Client) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return nil, err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	result, err := client.BalanceAt(ctx, account, blockNumber)
	return result, wrapProviderOperationError(index, "eth_getBalance", err)
}

// ValidateChainID verifies every configured provider reports the expected EVM chain ID.
func (c *Client) ValidateChainID(ctx context.Context, expected *big.Int) error {
	if expected == nil {
		return errors.New("expected chain id is required")
	}
	providers := c.snapshotProviders()
	if len(providers) == 0 {
		return errors.New("no rpc providers configured")
	}
	ids := make([]providerChainID, 0, len(providers))
	var providerErrs []error
	for index := range providers {
		chainID, err := c.chainIDFromProvider(ctx, index)
		if err != nil {
			providerErrs = append(providerErrs, err)
			continue
		}
		ids = append(ids, providerChainID{ProviderID: providerID(index), ChainID: chainID})
	}
	if len(providerErrs) > 0 {
		return errors.Join(providerErrs...)
	}
	return validateProviderChainIDs(c.chainName, expected, ids)
}

// CodeAt returns contract code at the first healthy provider's selected block.
func (c *Client) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return nil, err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	result, err := client.CodeAt(ctx, account, blockNumber)
	return result, wrapProviderOperationError(index, "eth_getCode", err)
}

// EstimateGas returns the first healthy provider's gas limit estimate.
func (c *Client) EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return 0, err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return 0, err
	}
	result, err := client.EstimateGas(ctx, call)
	return result, wrapProviderOperationError(index, "eth_estimateGas", err)
}

// FilterLogs returns a bounded log window only when a fixed configured
// majority of the providers that had reached the window end (per the latest
// quorum head snapshot) return the exact same normalized log sequence. The
// minority is marked with a sticky log-conflict flag (a separate dimension a
// later head check never clears); fewer than quorum usable responses is a
// QuorumUnavailableError and no majority sequence is a LogConflictError, so a
// single provider that fabricates or silently drops logs stalls the consumer
// instead of poisoning or losing indexed state. Callers must run CheckHead
// first (the indexer's head read does) and stay at or below its canonical head.
func (c *Client) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]gethtypes.Log, error) {
	if query.FromBlock == nil || query.ToBlock == nil {
		return nil, errors.New("filter logs requires a bounded from/to block range")
	}
	snapshot := c.headSnapshotRef()
	if snapshot == nil {
		return nil, fmt.Errorf("no quorum head snapshot for chain %s; CheckHead must precede FilterLogs", c.chainName)
	}
	if query.ToBlock.Cmp(snapshot.number) > 0 {
		return nil, fmt.Errorf("filter logs to block %s is beyond the quorum head %s for chain %s", query.ToBlock, snapshot.number, c.chainName)
	}
	total := len(c.snapshotProviders())
	if total == 0 {
		return nil, errors.New("no rpc providers configured")
	}
	quorum := total/2 + 1
	participants := make([]int, 0, total)
	for index, tip := range snapshot.tips {
		if tip != nil && tip.Cmp(query.ToBlock) >= 0 {
			participants = append(participants, index)
		}
	}
	sort.Ints(participants)
	if len(participants) < quorum {
		return nil, &QuorumUnavailableError{
			ChainName: c.chainName,
			Details:   []string{fmt.Sprintf("%d of %d configured providers had reached block %s at the last head check, quorum is %d", len(participants), total, query.ToBlock, quorum)},
		}
	}

	// Round 1: every participant concurrently, per-slot results only.
	results := make([]logWindowResult, len(participants))
	var wg sync.WaitGroup
	for slot, index := range participants {
		wg.Add(1)
		go func(slot, index int) {
			defer wg.Done()
			results[slot] = c.queryLogWindow(ctx, index, query)
		}(slot, index)
	}
	wg.Wait()

	// Round 2: providers whose first answer diverged from the round-1 majority
	// (or every responder, when no majority formed) get exactly one re-query
	// before they are judged, so a transiently inconsistent backend converges
	// instead of being flagged; errors already had their bounded retry inside
	// queryLogWindow.
	majority, best := majorityLogFingerprint(results)
	retried := false
	for slot, index := range participants {
		if results[slot].err != nil {
			continue
		}
		if best >= quorum && results[slot].fingerprint == majority {
			continue
		}
		results[slot] = c.queryLogWindow(ctx, index, query)
		retried = true
	}
	if retried {
		majority, best = majorityLogFingerprint(results)
	}

	fingerprints := make(map[int]string, len(results))
	var successes int
	for slot, index := range participants {
		if results[slot].err != nil {
			continue
		}
		successes++
		fingerprints[index] = results[slot].fingerprint
	}
	if successes < quorum {
		return nil, &QuorumUnavailableError{
			ChainName: c.chainName,
			Details:   []string{fmt.Sprintf("%d of %d configured providers answered the log window [%s, %s], quorum is %d", successes, total, query.FromBlock, query.ToBlock, quorum)},
		}
	}
	if best < quorum {
		details := make([]string, 0, len(fingerprints))
		for index, fingerprint := range fingerprints {
			details = append(details, fmt.Sprintf("%s returned window digest %s", providerID(index), fingerprint))
		}
		sort.Strings(details)
		c.applyLogConflicts(fingerprints, "")
		return nil, &LogConflictError{
			ChainName: c.chainName,
			FromBlock: query.FromBlock.Uint64(),
			ToBlock:   query.ToBlock.Uint64(),
			Details:   details,
		}
	}
	c.applyLogConflicts(fingerprints, majority)
	for slot := range results {
		if results[slot].err == nil && results[slot].fingerprint == majority {
			// The stored window is already canonically sorted, so consumers see
			// node order (blockNumber, txIndex, logIndex) regardless of which
			// provider's response order won the vote.
			return results[slot].logs, nil
		}
	}
	return nil, errors.New("log quorum majority result missing")
}

// logWindowResult is one provider's normalized answer for a bounded log window.
type logWindowResult struct {
	logs        []gethtypes.Log
	fingerprint string
	err         error
}

// queryLogWindow fetches one provider's bounded log window under the probe
// deadline, with one bounded retry absorbing transient blips, and normalizes
// the answer into canonical order plus its injective fingerprint.
func (c *Client) queryLogWindow(ctx context.Context, index int, query ethereum.FilterQuery) logWindowResult {
	logs, err := c.filterLogsFromProvider(ctx, index, query)
	if err != nil {
		logs, err = c.filterLogsFromProvider(ctx, index, query)
	}
	if err != nil {
		return logWindowResult{err: err}
	}
	sorted := normalizeLogWindow(logs)
	return logWindowResult{logs: sorted, fingerprint: logWindowFingerprint(sorted)}
}

// majorityLogFingerprint returns the most common fingerprint among successful
// results and its vote count.
func majorityLogFingerprint(results []logWindowResult) (string, int) {
	votes := make(map[string]int, len(results))
	for _, result := range results {
		if result.err != nil {
			continue
		}
		votes[result.fingerprint]++
	}
	var majority string
	best := 0
	for fingerprint, count := range votes {
		if count > best {
			best = count
			majority = fingerprint
		}
	}
	return majority, best
}

// applyLogConflicts marks providers that disagreed with the majority window and
// clears the flag for providers that agreed. An empty majority marks every
// participant (no sequence reached quorum).
func (c *Client) applyLogConflicts(fingerprints map[int]string, majority string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for index, fingerprint := range fingerprints {
		if index < 0 || index >= len(c.providers) {
			continue
		}
		c.providers[index].logConflict = majority == "" || fingerprint != majority
	}
}

func (c *Client) filterLogsFromProvider(ctx context.Context, index int, query ethereum.FilterQuery) ([]gethtypes.Log, error) {
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	probeCtx, cancel := c.probeContext(ctx)
	defer cancel()
	result, err := client.FilterLogs(probeCtx, query)
	return result, wrapProviderOperationError(index, "eth_getLogs", err)
}

// normalizeLogWindow returns the canonical (blockNumber, txIndex, logIndex)
// ordering of a log window (duplicates preserved), so equal log sets vote
// identically regardless of a provider's response order — and the majority
// window handed to consumers is always in node order, which the source indexer
// relies on when it groups one transaction's logs by contiguity.
func normalizeLogWindow(logs []gethtypes.Log) []gethtypes.Log {
	sorted := make([]gethtypes.Log, len(logs))
	copy(sorted, logs)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].BlockNumber != sorted[j].BlockNumber {
			return sorted[i].BlockNumber < sorted[j].BlockNumber
		}
		if sorted[i].TxIndex != sorted[j].TxIndex {
			return sorted[i].TxIndex < sorted[j].TxIndex
		}
		if sorted[i].Index != sorted[j].Index {
			return sorted[i].Index < sorted[j].Index
		}
		return sorted[i].BlockHash.Hex() < sorted[j].BlockHash.Hex()
	})
	return sorted
}

// logWindowFingerprint digests a canonically ordered log window with an
// injective, length-prefixed binary encoding: the log count, fixed-width
// numeric and hash fields, the topic count before the topics, and the data
// length before the data, so no two distinct windows can share an encoding
// (unlike delimiter-based formats, where a topic can masquerade as a data
// prefix). The full SHA-256 digest is compared and only the digest ever leaves
// this function, keeping conflict details free of raw log payloads.
func logWindowFingerprint(logs []gethtypes.Log) string {
	digest := sha256.New()
	var scratch [8]byte
	writeUint := func(value uint64) {
		binary.BigEndian.PutUint64(scratch[:], value)
		digest.Write(scratch[:])
	}
	writeUint(uint64(len(logs)))
	for _, log := range logs {
		writeUint(log.BlockNumber)
		digest.Write(log.BlockHash[:])
		writeUint(uint64(log.TxIndex))
		digest.Write(log.TxHash[:])
		writeUint(uint64(log.Index))
		digest.Write(log.Address[:])
		writeUint(uint64(len(log.Topics)))
		for _, topic := range log.Topics {
			digest.Write(topic[:])
		}
		writeUint(uint64(len(log.Data)))
		digest.Write(log.Data)
		if log.Removed {
			digest.Write([]byte{1})
		} else {
			digest.Write([]byte{0})
		}
	}
	return fmt.Sprintf("%x/%d", digest.Sum(nil), len(logs))
}

// SuggestGasPrice returns the first healthy provider's legacy gas price estimate.
func (c *Client) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return nil, err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	result, err := client.SuggestGasPrice(ctx)
	return result, wrapProviderOperationError(index, "eth_gasPrice", err)
}

// SuggestGasTipCap returns the first healthy provider's EIP-1559 priority-fee estimate.
func (c *Client) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return nil, err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	result, err := client.SuggestGasTipCap(ctx)
	return result, wrapProviderOperationError(index, "eth_maxPriorityFeePerGas", err)
}

// HeaderByNumber returns a header. A nil number returns the full header of the
// quorum canonical head, verified against the majority hash, so latest-header
// consumers (fee quotes, confirmation depth) cannot be steered by one provider;
// explicit historical numbers read from the first healthy provider.
func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*gethtypes.Header, error) {
	if number != nil {
		index, err := c.firstHealthyProvider()
		if err != nil {
			return nil, err
		}
		return c.headerByNumberFromProvider(ctx, index, number)
	}
	head, err := c.CheckHead(ctx)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, index := range c.healthyProvidersByTipDistance() {
		header, err := func() (*gethtypes.Header, error) {
			probeCtx, cancel := c.probeContext(ctx)
			defer cancel()
			return c.headerByNumberFromProvider(probeCtx, index, head.Number)
		}()
		if err != nil {
			lastErr = err
			continue
		}
		if header != nil && header.Hash().Hex() == head.Hash {
			return header, nil
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no healthy provider served the canonical header for chain %s", c.chainName)
}

// PendingNonceAt returns the first healthy provider's pending account nonce.
func (c *Client) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return 0, err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return 0, err
	}
	result, err := client.PendingNonceAt(ctx, account)
	return result, wrapProviderOperationError(index, "eth_getTransactionCount", err)
}

// NonceAt returns the first healthy provider's account nonce at the given block
// (nil means latest); the nonce reconciler pins it to a confirmed block.
func (c *Client) NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error) {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return 0, err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return 0, err
	}
	result, err := client.NonceAt(ctx, account, blockNumber)
	return result, wrapProviderOperationError(index, "eth_getTransactionCount", err)
}

// SendTransaction broadcasts a signed transaction through the first healthy provider.
func (c *Client) SendTransaction(ctx context.Context, tx *gethtypes.Transaction) error {
	index, err := c.firstHealthyProvider()
	if err != nil {
		return err
	}
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return err
	}
	return wrapProviderOperationError(index, "eth_sendRawTransaction", client.SendTransaction(ctx, tx))
}

// TransactionReceipt returns a receipt only when healthy providers agree on the receipt.
func (c *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (*gethtypes.Receipt, error) {
	var canonical *gethtypes.Receipt
	var canonicalFingerprint string
	canonicalProviderIndex := -1
	var transientErrs []error
	var notFoundProviders []string
	for index, provider := range c.snapshotProviders() {
		if provider.status != ProviderHealthy {
			continue
		}
		receipt, err := c.transactionReceiptFromProvider(ctx, index, txHash)
		if err != nil {
			if errors.Is(err, ethereum.NotFound) {
				notFoundProviders = append(notFoundProviders, providerID(index))
				continue
			}
			transientErrs = append(transientErrs, err)
			continue
		}
		fingerprint := receiptFingerprint(receipt)
		if canonical == nil {
			canonical = receipt
			canonicalFingerprint = fingerprint
			canonicalProviderIndex = index
			continue
		}
		if fingerprint != canonicalFingerprint {
			return nil, &ReceiptConflictError{
				TxHash: txHash,
				Details: []string{
					fmt.Sprintf("%s returned %s", providerID(index), fingerprint),
					fmt.Sprintf("%s returned %s", providerID(canonicalProviderIndex), canonicalFingerprint),
				},
			}
		}
	}
	if canonical != nil && len(notFoundProviders) > 0 {
		return nil, &ReceiptConflictError{
			TxHash:  txHash,
			Details: []string{fmt.Sprintf("providers missing mined receipt: %s", strings.Join(notFoundProviders, ", "))},
		}
	}
	if len(transientErrs) > 0 {
		return nil, errors.Join(transientErrs...)
	}
	if canonical == nil {
		if len(notFoundProviders) > 0 {
			return nil, ethereum.NotFound
		}
		return nil, errors.New("no healthy rpc providers configured")
	}
	return canonical, nil
}

func (c *Client) transactionReceiptFromProvider(ctx context.Context, index int, txHash common.Hash) (*gethtypes.Receipt, error) {
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	receipt, err := client.TransactionReceipt(ctx, txHash)
	return receipt, wrapProviderOperationError(index, "eth_getTransactionReceipt", err)
}

func (c *Client) chainIDFromProvider(ctx context.Context, index int) (*big.Int, error) {
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	chainID, err := client.ChainID(ctx)
	return chainID, wrapProviderOperationError(index, "eth_chainId", err)
}

func (c *Client) headerByNumberFromProvider(ctx context.Context, index int, number *big.Int) (*gethtypes.Header, error) {
	client, err := c.providerClient(ctx, index)
	if err != nil {
		return nil, err
	}
	header, err := client.HeaderByNumber(ctx, number)
	return header, wrapProviderOperationError(index, "eth_getBlockByNumber", err)
}

// firstHealthyProvider selects the healthy provider whose observed tip is
// closest to the quorum canonical head, so an inflated-tip node never becomes
// the preferred single source for reads.
func (c *Client) firstHealthyProvider() (int, error) {
	order := c.healthyProvidersByTipDistance()
	if len(order) == 0 {
		return 0, errors.New("no healthy rpc providers configured")
	}
	return order[0], nil
}

func (c *Client) healthyProvidersByTipDistance() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	indices := make([]int, 0, len(c.providers))
	for index, provider := range c.providers {
		if provider.status == ProviderHealthy {
			indices = append(indices, index)
		}
	}
	if c.head == nil {
		return indices
	}
	tips := c.head.tips
	sort.SliceStable(indices, func(i, j int) bool {
		tipI, okI := tips[indices[i]]
		tipJ, okJ := tips[indices[j]]
		if !okI || tipI == nil {
			return false
		}
		if !okJ || tipJ == nil {
			return true
		}
		return tipI.Cmp(tipJ) < 0
	})
	return indices
}

func (c *Client) snapshotProviders() []configuredProvider {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]configuredProvider, len(c.providers))
	copy(out, c.providers)
	return out
}

func (c *Client) providerClient(ctx context.Context, index int) (*ethclient.Client, error) {
	c.mu.Lock()
	if index < 0 || index >= len(c.providers) {
		c.mu.Unlock()
		return nil, fmt.Errorf("provider index %d out of range", index)
	}
	if c.providers[index].client != nil {
		client := c.providers[index].client
		c.mu.Unlock()
		return client, nil
	}
	url := c.providers[index].url
	c.mu.Unlock()

	client, err := ethclient.DialContext(ctx, url)
	if err != nil {
		return nil, wrapProviderOperationError(index, "connect", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.providers[index].client != nil {
		client.Close()
		return c.providers[index].client, nil
	}
	c.providers[index].client = client
	return client, nil
}

func receiptFingerprint(receipt *gethtypes.Receipt) string {
	if receipt == nil {
		return "<nil>"
	}
	var builder strings.Builder
	builder.WriteString(receipt.TxHash.Hex())
	builder.WriteString("|status=")
	fmt.Fprint(&builder, receipt.Status)
	builder.WriteString("|block=")
	if receipt.BlockNumber != nil {
		builder.WriteString(receipt.BlockNumber.String())
	}
	builder.WriteString("|block_hash=")
	builder.WriteString(receipt.BlockHash.Hex())
	builder.WriteString("|logs=")
	fmt.Fprint(&builder, len(receipt.Logs))
	for _, log := range receipt.Logs {
		if log == nil {
			builder.WriteString("|<nil>")
			continue
		}
		builder.WriteString("|")
		builder.WriteString(log.Address.Hex())
		builder.WriteString("/")
		builder.WriteString(log.TxHash.Hex())
		builder.WriteString("/")
		fmt.Fprint(&builder, log.Index)
		builder.WriteString("/")
		fmt.Fprint(&builder, log.BlockNumber)
		builder.WriteString("/")
		builder.WriteString(log.BlockHash.Hex())
		builder.WriteString("/")
		builder.WriteString(common.Bytes2Hex(log.Data))
		for _, topic := range log.Topics {
			builder.WriteString("/")
			builder.WriteString(topic.Hex())
		}
	}
	return builder.String()
}

type providerChainID struct {
	ProviderID string
	ChainID    *big.Int
}

func validateProviderChainIDs(chainName string, expected *big.Int, ids []providerChainID) error {
	if expected == nil {
		return errors.New("expected chain id is required")
	}
	if len(ids) == 0 {
		return errors.New("no rpc providers configured")
	}
	var details []string
	for _, item := range ids {
		switch {
		case item.ChainID == nil:
			details = append(details, fmt.Sprintf("%s returned <nil>", item.ProviderID))
		case item.ChainID.Cmp(expected) != 0:
			details = append(details, fmt.Sprintf("%s returned %s", item.ProviderID, item.ChainID))
		}
	}
	if len(details) > 0 {
		return &ChainIDMismatchError{
			ChainName: chainName,
			Expected:  new(big.Int).Set(expected),
			Details:   details,
		}
	}
	return nil
}
