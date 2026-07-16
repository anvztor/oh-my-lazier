package txmgr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/islishude/oh-my-lazier/go/internal/db"
)

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
			if got := classifyBroadcastError(test.err); got != test.want {
				t.Fatalf("classifyBroadcastError(%v) = %q, want %q", test.err, got, test.want)
			}
		})
	}
}
