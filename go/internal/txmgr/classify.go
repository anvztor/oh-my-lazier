package txmgr

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/islishude/oh-my-lazier/go/internal/db"
)

// classifyBroadcastError maps a SendTransaction result to a durable send-error
// class (see db.SendError*). The classification is deliberately conservative:
// only tested, exact node phrases are recognized, and anything unrecognized is
// treated as ambiguous (possibly accepted) so an already-submitted transaction is
// never dropped from receipt tracking. err == nil means the node accepted the
// transaction.
func classifyBroadcastError(err error) string {
	if err == nil {
		return db.SendErrorAccepted
	}
	// Transport and cancellation errors leave acceptance undetermined.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return db.SendErrorAmbiguous
	}

	msg := strings.ToLower(err.Error())
	contains := func(phrase string) bool { return strings.Contains(msg, phrase) }

	switch {
	// The node already has this exact transaction.
	case contains("already known"):
		return db.SendErrorAccepted

	case contains("nonce too low"):
		return db.SendErrorNonceTooLow
	case contains("nonce too high"):
		return db.SendErrorNonceTooHigh

	// Fee/repricing rejections: the raw stays valid and can be repriced later.
	case contains("replacement transaction underpriced"),
		contains("transaction underpriced"),
		contains("transaction gas price below minimum"),
		contains("max fee per gas less than block base fee"),
		contains("max priority fee per gas higher than max fee per gas"):
		return db.SendErrorUnderpriced

	// Environmental rejections that may recover; keep the nonce and retry later.
	case contains("insufficient funds"),
		contains("txpool is full"),
		contains("account limit exceeded"):
		return db.SendErrorRetryableEnv

	// Deterministic rejections of this transaction.
	case contains("intrinsic gas too low"),
		contains("exceeds block gas limit"),
		contains("invalid sender"),
		contains("sender not an eoa"),
		contains("transaction type not supported"),
		contains("only replay-protected"),
		contains("oversized data"),
		contains("negative value"),
		contains("gas limit reached"):
		return db.SendErrorDefinitive

	default:
		// Unknown transport/RPC failure: assume the transaction may have been
		// accepted and let receipt polling decide.
		return db.SendErrorAmbiguous
	}
}
