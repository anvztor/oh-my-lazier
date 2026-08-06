package rpcquorum

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// VotedRevertError marks a provider error that the fixed configured majority
// agreed is a deterministic EVM revert. Downstream terminal classification
// (txmgr's estimate-gas handling) tests this marker instead of re-deriving
// revert semantics from provider text: the quorum layer already made that
// decision by vote, and two independent text classifiers drift apart.
type VotedRevertError struct {
	// Operation is the RPC method whose vote produced this revert.
	Operation string
	// Err is the winning provider error.
	Err error
}

// Error renders the winning provider error unchanged.
func (e *VotedRevertError) Error() string {
	return e.Err.Error()
}

// Unwrap exposes the original provider error, so rpc.Error and rpc.DataError
// inspection keeps working through the marker.
func (e *VotedRevertError) Unwrap() error {
	return e.Err
}

// IsVotedRevert reports whether err carries a majority-agreed revert verdict.
func IsVotedRevert(err error) bool {
	var voted *VotedRevertError
	return errors.As(err, &voted)
}

// StateReadConflictError reports a state read (eth_call, eth_getCode,
// eth_getTransactionCount pending) where a fixed majority of providers gave
// comparable answers but no single answer reached the majority. It is distinct
// from QuorumUnavailableError: the chain answered, the providers disagree.
type StateReadConflictError struct {
	ChainName string
	Operation string
	Votes     map[string]int
}

// Error renders the conflict without any provider payloads or URLs.
func (e *StateReadConflictError) Error() string {
	return fmt.Sprintf("chain %s %s state read has no majority answer across %d distinct results", e.ChainName, e.Operation, len(e.Votes))
}

// IsStateReadConflict reports whether err is a state-read quorum conflict.
func IsStateReadConflict(err error) bool {
	var conflict *StateReadConflictError
	return errors.As(err, &conflict)
}

// EstimateGasConflictError reports gas estimates whose comparable answers
// reached the fixed majority without a usable bounded consensus.
type EstimateGasConflictError struct {
	ChainName string
	Detail    string
}

// Error renders the conflict without provider identities or payloads.
func (e *EstimateGasConflictError) Error() string {
	return fmt.Sprintf("chain %s gas estimate has no bounded majority: %s", e.ChainName, e.Detail)
}

// IsEstimateGasConflict reports whether err is a gas-estimate quorum conflict.
func IsEstimateGasConflict(err error) bool {
	var conflict *EstimateGasConflictError
	return errors.As(err, &conflict)
}

// stateReadProbe is one provider's classified answer to a state read.
type stateReadProbe struct {
	// payload is the successful result (nil-normalized to empty).
	payload []byte
	// revertErr is the provider's original error for a deterministic EVM
	// revert; it stays wrapped so error-chain classification keeps working
	// when a revert wins the vote.
	revertErr error
	// transientErr is any non-comparable failure (timeout, transport, unknown
	// RPC error); it only counts toward unavailability.
	transientErr error
	responded    bool
}

// comparable reports whether the probe carries a votable answer.
func (p stateReadProbe) comparable() bool {
	return p.responded && p.transientErr == nil
}

// stateReadFingerprint keys a comparable answer: success payloads by their
// SHA-256, deterministic reverts by their revert identity.
func (p stateReadProbe) fingerprint() string {
	if p.revertErr != nil {
		return revertFingerprint(p.revertErr)
	}
	sum := sha256.Sum256(p.payload)
	return "s:" + hex.EncodeToString(sum[:])
}

// revertEvidence reports whether a provider error claims that the EVM itself
// rejected the call, rather than reporting a node-level or transport failure.
// Three admissible forms, covering the shapes the common clients emit:
//
//	geth / erigon:     code 3, or "execution reverted[: reason]"
//	ganache / hardhat: "VM Exception while processing transaction: revert ..."
//	nethermind:        "VM execution error." WITH canonical revert data
//
// Attached ErrorData is never evidence on its own: providers put opaque
// diagnostics there (request ids, block hashes) under transient failures, and
// a hex-shaped diagnostic must not establish a deterministic revert. The
// Nethermind form therefore requires the execution claim AND validated ABI
// revert bytes together — neither of which a transport failure carries.
func revertEvidence(err error) bool {
	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	if rpcErr.ErrorCode() == 3 {
		return true
	}
	message := strings.ToLower(rpcErr.Error())
	if containsRevertWord(message) {
		return true
	}
	if strings.Contains(message, "vm execution error") {
		_, ok := canonicalRevertData(err)
		return ok
	}
	return false
}

// containsRevertWord reports whether the message uses "revert" or "reverted"
// as a standalone word — the only forms EVM clients emit in revert messages.
// Matching the bare substring would admit unrelated wording ("reverting proxy
// misconfigured"), while matching only "execution reverted" would drop the
// ganache/hardhat and bare-"revert" shapes.
func containsRevertWord(message string) bool {
	for index := 0; ; {
		offset := strings.Index(message[index:], "revert")
		if offset < 0 {
			return false
		}
		start := index + offset
		end := start + len("revert")
		for end < len(message) && isASCIILetter(message[end]) {
			end++
		}
		suffix := message[start+len("revert") : end]
		precededByLetter := start > 0 && isASCIILetter(message[start-1])
		switch suffix {
		case "", "ed":
			if !precededByLetter {
				return true
			}
		}
		index = end
	}
}

func isASCIILetter(char byte) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
}

// canonicalRevertData extracts an EVM revert payload only when the provider
// attached genuine ABI-encoded revert bytes: a 0x-prefixed even-length hex
// string or a non-empty byte slice. It refines the identity of a probe that
// already carries revert evidence; it never admits one on its own.
func canonicalRevertData(err error) (string, bool) {
	var dataErr rpc.DataError
	if !errors.As(err, &dataErr) {
		return "", false
	}
	switch payload := dataErr.ErrorData().(type) {
	case string:
		normalized := strings.ToLower(strings.TrimSpace(payload))
		// "0x" alone carries no return data (a bare revert); fall through to
		// the message identity rather than merging every bare revert.
		if len(normalized) <= 2 || !strings.HasPrefix(normalized, "0x") {
			return "", false
		}
		body := normalized[2:]
		if len(body)%2 != 0 {
			return "", false
		}
		if _, decodeErr := hex.DecodeString(body); decodeErr != nil {
			return "", false
		}
		return normalized, true
	case []byte:
		if len(payload) == 0 {
			return "", false
		}
		return "0x" + hex.EncodeToString(payload), true
	default:
		return "", false
	}
}

// isDeterministicRevert reports whether a provider error is a comparable EVM
// execution revert rather than a transport or availability failure. Only the
// provider's own revert claim admits a probe — RPC error code 3, or an
// "execution reverted" message (the shape geth-family endpoints return on
// some call/estimate paths). An RPC error carrying ErrorData but no revert
// claim stays non-comparable, so an opaque diagnostic can never masquerade as
// a deterministic revert downstream.
func isDeterministicRevert(err error) bool {
	return revertEvidence(err)
}

// revertFingerprint keys a probe that already carries revert evidence by what
// the EVM actually returned: canonical revert data when present, the
// normalized provider message otherwise. The RPC error code is deliberately
// excluded from the identity — providers report semantically identical
// reverts under different codes (3 vs -32000), and a shared code with no data
// must never merge distinct revert reasons into a false majority.
func revertFingerprint(err error) string {
	if data, ok := canonicalRevertData(err); ok {
		return "rd:" + data
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		return "rm:" + strings.ToLower(strings.TrimSpace(rpcErr.Error()))
	}
	// Not reachable for probes admitted by isDeterministicRevert; the wrapped
	// message includes the provider index, so unknown shapes never merge.
	return "rm:" + strings.ToLower(strings.TrimSpace(err.Error()))
}

// voteStateRead runs one comparable-result vote across every configured
// provider: q = floor(N/2)+1 over the CONFIGURED count. A success payload or a
// deterministic revert reaching q returns immediately and cancels the
// stragglers. Head statuses are never touched by state reads — a historical
// read agreeing across providers must not re-promote a lagging or forked
// provider — only the separate sticky state-conflict dimension moves.
func (c *Client) voteStateRead(ctx context.Context, operation string, read func(ctx context.Context, client *ethclient.Client) ([]byte, error)) ([]byte, error) {
	providers := c.snapshotProviders()
	total := len(providers)
	if total == 0 {
		return nil, errors.New("no rpc providers configured")
	}
	quorum := total/2 + 1

	probes := make([]stateReadProbe, total)
	probeCtx, cancelProbes := context.WithCancel(ctx)
	defer cancelProbes()
	completions := make(chan int, total)
	for index := range providers {
		go func(index int) {
			perProbeCtx, cancel := c.probeContext(probeCtx)
			defer cancel()
			client, err := c.providerClient(perProbeCtx, index)
			if err != nil {
				probes[index] = stateReadProbe{transientErr: wrapProviderOperationError(index, operation, err)}
				completions <- index
				return
			}
			payload, err := read(perProbeCtx, client)
			switch {
			case err == nil:
				if payload == nil {
					payload = []byte{}
				}
				probes[index] = stateReadProbe{payload: payload, responded: true}
			case isDeterministicRevert(err):
				probes[index] = stateReadProbe{revertErr: wrapProviderOperationError(index, operation, err), responded: true}
			default:
				probes[index] = stateReadProbe{transientErr: wrapProviderOperationError(index, operation, err)}
			}
			completions <- index
		}(index)
	}

	votes := make(map[string]int, total)
	winners := make(map[string]stateReadProbe, total)
	fingerprints := make(map[int]string, total)
	comparable := 0
	var transientErrs []error
	majorityFingerprint := ""
	for completed := 0; completed < total; completed++ {
		index := <-completions
		probe := probes[index]
		if !probe.comparable() {
			transientErrs = append(transientErrs, probe.transientErr)
			continue
		}
		comparable++
		fingerprint := probe.fingerprint()
		fingerprints[index] = fingerprint
		votes[fingerprint]++
		if _, ok := winners[fingerprint]; !ok {
			winners[fingerprint] = probe
		}
		if votes[fingerprint] >= quorum && majorityFingerprint == "" {
			majorityFingerprint = fingerprint
			cancelProbes()
		}
	}

	c.applyStateConflicts(fingerprints, majorityFingerprint)
	if majorityFingerprint != "" {
		winner := winners[majorityFingerprint]
		if winner.revertErr != nil {
			return nil, &VotedRevertError{Operation: operation, Err: winner.revertErr}
		}
		return winner.payload, nil
	}
	if comparable >= quorum {
		// The chain answered but the providers disagree. The aggregate error
		// deliberately wraps NO provider revert: a quorum split must never be
		// classified downstream as a deterministic revert.
		return nil, &StateReadConflictError{ChainName: c.chainName, Operation: operation, Votes: votes}
	}
	return nil, &QuorumUnavailableError{
		ChainName: c.chainName,
		Details:   stateReadFailureDetails(operation, quorum, comparable, transientErrs),
	}
}

// stateReadFailureDetails summarizes an unavailable state read without leaking
// provider URLs or payloads: provider operation errors are already redacted to
// provider indexes when wrapped.
func stateReadFailureDetails(operation string, quorum, comparable int, transientErrs []error) []string {
	details := make([]string, 0, len(transientErrs)+1)
	for _, err := range transientErrs {
		if err != nil {
			details = append(details, err.Error())
		}
	}
	details = append(details, fmt.Sprintf("%s has %d comparable answers, quorum is %d", operation, comparable, quorum))
	return details
}

// applyStateConflicts marks comparable dissenters against the majority answer
// and clears providers that agreed. No majority marks every comparable
// participant. Head statuses are intentionally untouched.
func (c *Client) applyStateConflicts(fingerprints map[int]string, majority string) {
	if len(fingerprints) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for index, fingerprint := range fingerprints {
		if index < 0 || index >= len(c.providers) {
			continue
		}
		c.providers[index].stateConflict = majority == "" || fingerprint != majority
	}
}

// StateAnchor pins one logical multi-read check to a single verified state
// view. Hash addressing is authoritative when set: a tip pin must not follow
// a mid-check reorg onto another branch that happens to occupy the same
// height. Number-only anchors address confirmation-deep reads whose branch
// identity is already settled.
type StateAnchor struct {
	Number *big.Int
	Hash   common.Hash
}

// AnchorFromHead converts a verified quorum head into a hash-pinned anchor.
func AnchorFromHead(head HeadResult) (*StateAnchor, error) {
	if head.Number == nil {
		return nil, errors.New("verified head has no block number")
	}
	anchor := &StateAnchor{Number: new(big.Int).Set(head.Number)}
	if head.Hash != "" {
		anchor.Hash = common.HexToHash(head.Hash)
	}
	return anchor, nil
}

// AnchoredCaller is the minimal read surface for anchor-routed contract calls.
type AnchoredCaller interface {
	CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	CallContractAtHash(ctx context.Context, call ethereum.CallMsg, blockHash common.Hash) ([]byte, error)
}

// CallAtAnchor routes one read through the anchor: block-hash addressing when
// the anchor carries a hash, block-number addressing for confirmation-deep
// anchors, and the caller's own canonical head anchoring when anchor is nil.
func CallAtAnchor(ctx context.Context, caller AnchoredCaller, call ethereum.CallMsg, anchor *StateAnchor) ([]byte, error) {
	if anchor == nil {
		return caller.CallContract(ctx, call, nil)
	}
	if anchor.Hash != (common.Hash{}) {
		return caller.CallContractAtHash(ctx, call, anchor.Hash)
	}
	return caller.CallContract(ctx, call, anchor.Number)
}

// CallContract executes an eth_call agreed by the fixed configured majority.
// A nil block number anchors the call to the canonical head's block HASH, so a
// same-height minority fork cannot serve its own branch state into the vote;
// an explicit block number is voted at that height (callers anchoring a whole
// logical operation verify the height once through CheckHead).
func (c *Client) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	if blockNumber == nil {
		head, err := c.CheckHead(ctx)
		if err != nil {
			return nil, err
		}
		anchor := common.HexToHash(head.Hash)
		return c.voteStateRead(ctx, "eth_call", func(ctx context.Context, client *ethclient.Client) ([]byte, error) {
			return client.CallContractAtHash(ctx, call, anchor)
		})
	}
	return c.voteStateRead(ctx, "eth_call", func(ctx context.Context, client *ethclient.Client) ([]byte, error) {
		return client.CallContract(ctx, call, blockNumber)
	})
}

// CallContractAtHash executes an eth_call pinned to an explicit block hash and
// agreed by the fixed configured majority. Hash addressing keeps every read of
// a logical multi-read check on the branch its anchor was verified on.
func (c *Client) CallContractAtHash(ctx context.Context, call ethereum.CallMsg, blockHash common.Hash) ([]byte, error) {
	return c.voteStateRead(ctx, "eth_call", func(ctx context.Context, client *ethclient.Client) ([]byte, error) {
		return client.CallContractAtHash(ctx, call, blockHash)
	})
}

// CodeAt returns the contract code agreed by the fixed configured majority,
// voted by code hash with nil and empty code normalized to the same answer.
// A nil block number anchors to the canonical head's block hash.
func (c *Client) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	if blockNumber == nil {
		head, err := c.CheckHead(ctx)
		if err != nil {
			return nil, err
		}
		anchor := common.HexToHash(head.Hash)
		return c.voteStateRead(ctx, "eth_getCode", func(ctx context.Context, client *ethclient.Client) ([]byte, error) {
			return client.CodeAtHash(ctx, account, anchor)
		})
	}
	return c.voteStateRead(ctx, "eth_getCode", func(ctx context.Context, client *ethclient.Client) ([]byte, error) {
		return client.CodeAt(ctx, account, blockNumber)
	})
}

// CodeAtHash returns the contract code at an explicit block hash agreed by
// the fixed configured majority.
func (c *Client) CodeAtHash(ctx context.Context, account common.Address, blockHash common.Hash) ([]byte, error) {
	return c.voteStateRead(ctx, "eth_getCode", func(ctx context.Context, client *ethclient.Client) ([]byte, error) {
		return client.CodeAtHash(ctx, account, blockHash)
	})
}

// EstimateGas aggregates gas estimates across the fixed configured majority.
// Estimates run on each provider's own latest state (anchoring to a historical
// block would fake reverts for time-sensitive calldata), so exact equality is
// not expected: successful estimates form a bounded set around the upper
// median m — only values in [ceil(m/2), 2m] count — and the maximum in-bounds
// value is returned, so a lone honest high estimate survives while a lone
// hostile one is capped at 2x. A deterministic revert wins only when the same
// revert identity reaches the majority, preserving downstream terminal
// classification. The result must not exceed the canonical block gas limit.
func (c *Client) EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error) {
	header, err := c.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, err
	}
	blockGasLimit := header.GasLimit

	providers := c.snapshotProviders()
	total := len(providers)
	if total == 0 {
		return 0, errors.New("no rpc providers configured")
	}
	quorum := total/2 + 1

	type gasProbe struct {
		gas          uint64
		revertErr    error
		transientErr error
		responded    bool
	}
	probes := make([]gasProbe, total)
	probeCtx, cancelProbes := context.WithCancel(ctx)
	defer cancelProbes()
	completions := make(chan int, total)
	for index := range providers {
		go func(index int) {
			perProbeCtx, cancel := c.probeContext(probeCtx)
			defer cancel()
			client, err := c.providerClient(perProbeCtx, index)
			if err != nil {
				probes[index] = gasProbe{transientErr: wrapProviderOperationError(index, "eth_estimateGas", err)}
				completions <- index
				return
			}
			gas, err := client.EstimateGas(perProbeCtx, call)
			switch {
			case err == nil:
				probes[index] = gasProbe{gas: gas, responded: true}
			case isDeterministicRevert(err):
				probes[index] = gasProbe{revertErr: wrapProviderOperationError(index, "eth_estimateGas", err), responded: true}
			default:
				probes[index] = gasProbe{transientErr: wrapProviderOperationError(index, "eth_estimateGas", err)}
			}
			completions <- index
		}(index)
	}

	type successVote struct {
		index int
		gas   uint64
	}
	var successes []successVote
	revertVotes := make(map[string]int, total)
	revertWinners := make(map[string]error, total)
	comparable := 0
	var transientErrs []error
	for completed := 0; completed < total; completed++ {
		index := <-completions
		probe := probes[index]
		switch {
		case probe.transientErr != nil:
			transientErrs = append(transientErrs, probe.transientErr)
		case probe.revertErr != nil:
			comparable++
			fingerprint := revertFingerprint(probe.revertErr)
			revertVotes[fingerprint]++
			if _, ok := revertWinners[fingerprint]; !ok {
				revertWinners[fingerprint] = probe.revertErr
			}
		default:
			comparable++
			successes = append(successes, successVote{index: index, gas: probe.gas})
		}
	}

	// Every comparable probe participates in the conflict classification for
	// every outcome: a reverting dissenter against a bounded success majority
	// (or vice versa) must light the sticky state-conflict flag, or the
	// provider-conflict alert misses the faulty endpoint.
	classification := make(map[int]string, total)
	for index, probe := range probes {
		switch {
		case probe.revertErr != nil:
			classification[index] = revertFingerprint(probe.revertErr)
		case probe.responded:
			classification[index] = "s:pending"
		}
	}
	for fingerprint, count := range revertVotes {
		if count >= quorum {
			c.applyStateConflicts(classification, fingerprint)
			return 0, &VotedRevertError{Operation: "eth_estimateGas", Err: revertWinners[fingerprint]}
		}
	}
	if len(successes) >= quorum {
		sorted := make([]uint64, len(successes))
		for i, vote := range successes {
			sorted[i] = vote.gas
		}
		sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
		median := sorted[len(sorted)/2]
		lower := (median + 1) / 2
		upper := median * 2
		inBounds := make([]uint64, 0, len(successes))
		for _, vote := range successes {
			if vote.gas < lower || vote.gas > upper {
				classification[vote.index] = "s:out-of-bounds"
				continue
			}
			classification[vote.index] = "s:bounded"
			inBounds = append(inBounds, vote.gas)
		}
		c.applyStateConflicts(classification, "s:bounded")
		if len(inBounds) < quorum {
			return 0, &EstimateGasConflictError{ChainName: c.chainName, Detail: fmt.Sprintf("only %d of %d estimates fall within the bounded set, need %d", len(inBounds), len(successes), quorum)}
		}
		result := inBounds[0]
		for _, gas := range inBounds[1:] {
			if gas > result {
				result = gas
			}
		}
		if result > blockGasLimit {
			return 0, &EstimateGasConflictError{ChainName: c.chainName, Detail: fmt.Sprintf("bounded estimate %d exceeds the canonical block gas limit %d", result, blockGasLimit)}
		}
		return result, nil
	}
	if comparable >= quorum {
		// No outcome reached the majority: every comparable probe is marked.
		c.applyStateConflicts(classification, "")
		return 0, &EstimateGasConflictError{ChainName: c.chainName, Detail: fmt.Sprintf("neither %d successes nor any revert identity reached the majority of %d", len(successes), quorum)}
	}
	return 0, &QuorumUnavailableError{
		ChainName: c.chainName,
		Details:   stateReadFailureDetails("eth_estimateGas", quorum, comparable, transientErrs),
	}
}
