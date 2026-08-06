package txmgr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

func TestIsEstimateGasRevertHonorsQuorumVerdict(t *testing.T) {
	// A quorum-voted revert is terminal regardless of the client's wording:
	// ganache's message-only shape carries no code 3 and no hex data, so the
	// text classifier alone would leave the row queued and retrying forever.
	ganache := errors.New("VM Exception while processing transaction: revert MyError")
	if isEstimateGasRevert(ganache) {
		t.Fatal("a plain non-rpc error must not be terminal without the quorum verdict")
	}
	voted := fmt.Errorf("estimate outbox tx 1: %w", &rpcquorum.VotedRevertError{Operation: "eth_estimateGas", Err: ganache})
	if !isEstimateGasRevert(voted) {
		t.Fatal("quorum-voted revert must classify as a terminal estimate revert")
	}
}

func TestClassifyBroadcastError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"accepted nil", nil, db.SendErrorAccepted},
		{"already known", errors.New("already known"), db.SendErrorAccepted},
		{"nonce too low", errors.New("nonce too low"), db.SendErrorNonceTooLow},
		{"nonce too high", errors.New("nonce too high"), db.SendErrorNonceTooHigh},
		{"replacement underpriced", errors.New("replacement transaction underpriced"), db.SendErrorUnderpriced},
		{"underpriced", errors.New("transaction underpriced"), db.SendErrorUnderpriced},
		{"base fee", errors.New("max fee per gas less than block base fee"), db.SendErrorUnderpriced},
		{"insufficient funds", errors.New("insufficient funds for gas * price + value"), db.SendErrorRetryableEnv},
		{"txpool full", errors.New("txpool is full"), db.SendErrorRetryableEnv},
		{"intrinsic gas", errors.New("intrinsic gas too low"), db.SendErrorDefinitive},
		{"invalid sender", errors.New("invalid sender"), db.SendErrorDefinitive},
		{"eip155", errors.New("only replay-protected (EIP-155) transactions allowed over RPC"), db.SendErrorDefinitive},
		{"context canceled", context.Canceled, db.SendErrorAmbiguous},
		{"deadline", context.DeadlineExceeded, db.SendErrorAmbiguous},
		{"eof", io.EOF, db.SendErrorAmbiguous},
		{"wrapped eof", fmt.Errorf("post: %w", io.ErrUnexpectedEOF), db.SendErrorAmbiguous},
		{"timeout text", errors.New("Post \"http://node\": context deadline exceeded (Client.Timeout)"), db.SendErrorAmbiguous},
		{"unknown", errors.New("some brand new node error"), db.SendErrorAmbiguous},
		{"json-rpc 500", errors.New("500 Internal Server Error"), db.SendErrorAmbiguous},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, detail := classifyBroadcastError(test.err)
			if got != test.want {
				t.Fatalf("classifyBroadcastError(%v) = %q, want %q", test.err, got, test.want)
			}
			if test.err != nil && detail == "" {
				t.Fatalf("classifyBroadcastError(%v) returned an empty detail", test.err)
			}
		})
	}
}

// opaqueWrapError mirrors the rpcquorum provider wrapper: Error() carries only
// a canonical message and the node's text is reachable solely via Unwrap().
type opaqueWrapError struct {
	cause error
}

func (e *opaqueWrapError) Error() string { return "provider[0] eth_sendRawTransaction failed" }

func (e *opaqueWrapError) Unwrap() error { return e.cause }

func TestClassifyBroadcastErrorMatchesWrappedCauses(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"wrapped nonce too low", &opaqueWrapError{cause: errors.New("nonce too low")}, db.SendErrorNonceTooLow},
		{"wrapped nonce too high", &opaqueWrapError{cause: errors.New("nonce too high")}, db.SendErrorNonceTooHigh},
		{"wrapped underpriced", &opaqueWrapError{cause: errors.New("replacement transaction underpriced")}, db.SendErrorUnderpriced},
		{"wrapped insufficient funds", &opaqueWrapError{cause: errors.New("insufficient funds for gas * price + value")}, db.SendErrorRetryableEnv},
		{"wrapped already known", &opaqueWrapError{cause: errors.New("already known")}, db.SendErrorAccepted},
		{"wrapped definitive", &opaqueWrapError{cause: errors.New("intrinsic gas too low")}, db.SendErrorDefinitive},
		{"wrapped unknown stays ambiguous", &opaqueWrapError{cause: errors.New("some brand new node error")}, db.SendErrorAmbiguous},
		{"doubly wrapped", fmt.Errorf("broadcast: %w", &opaqueWrapError{cause: errors.New("nonce too low")}), db.SendErrorNonceTooLow},
		{"joined errors", errors.Join(errors.New("first provider timed out"), &opaqueWrapError{cause: errors.New("transaction underpriced")}), db.SendErrorUnderpriced},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, detail := classifyBroadcastError(test.err)
			if got != test.want {
				t.Fatalf("classifyBroadcastError(%v) = %q, want %q", test.err, got, test.want)
			}
			if detail == "" {
				t.Fatalf("classifyBroadcastError(%v) returned an empty detail", test.err)
			}
		})
	}
}

func TestClassifyBroadcastErrorDetailIsCanonical(t *testing.T) {
	// The detail must never echo the raw error, which can embed RPC URLs or keys.
	raw := errors.New("Post \"https://rpc.example/v1/SECRET-KEY\": nonce too low")
	class, detail := classifyBroadcastError(raw)
	if class != db.SendErrorNonceTooLow {
		t.Fatalf("class = %q, want nonce_too_low", class)
	}
	if detail != "nonce too low" {
		t.Fatalf("detail = %q, want the canonical phrase only", detail)
	}
	_, unknownDetail := classifyBroadcastError(errors.New("weird https://rpc.example/v1/SECRET-KEY failure"))
	if unknownDetail != "unrecognized broadcast error" {
		t.Fatalf("unknown detail = %q, want the fixed placeholder", unknownDetail)
	}
}
