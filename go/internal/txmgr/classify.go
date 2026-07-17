package txmgr

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/islishude/oh-my-lazier/go/internal/db"
)

// classifyBroadcastError maps a SendTransaction result to a durable send-error
// class (see db.SendError*) and a canonical, secret-free detail string. The
// classification is deliberately conservative: only tested, exact node phrases
// are recognized, and anything unrecognized is treated as ambiguous (possibly
// accepted) so an already-submitted transaction is never dropped from receipt
// tracking. Only the canonical detail is persisted or logged — raw node errors
// can embed RPC URLs or API keys. err == nil means the node accepted the
// transaction.
func classifyBroadcastError(err error) (class, detail string) {
	if err == nil {
		return db.SendErrorAccepted, ""
	}
	// Transport and cancellation errors leave acceptance undetermined.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return db.SendErrorAmbiguous, "transport failure before acknowledgement"
	}

	msg := strings.ToLower(err.Error())
	match := func(phrases ...string) string {
		for _, phrase := range phrases {
			if strings.Contains(msg, phrase) {
				return phrase
			}
		}
		return ""
	}

	// The node already has this exact transaction.
	if phrase := match("already known"); phrase != "" {
		return db.SendErrorAccepted, phrase
	}
	if phrase := match("nonce too low"); phrase != "" {
		return db.SendErrorNonceTooLow, phrase
	}
	if phrase := match("nonce too high"); phrase != "" {
		return db.SendErrorNonceTooHigh, phrase
	}
	// Fee/repricing rejections: the raw stays valid and can be repriced later.
	if phrase := match(
		"replacement transaction underpriced",
		"transaction underpriced",
		"transaction gas price below minimum",
		"max fee per gas less than block base fee",
		"max priority fee per gas higher than max fee per gas",
	); phrase != "" {
		return db.SendErrorUnderpriced, phrase
	}
	// Environmental rejections that may recover; keep the nonce and retry later.
	if phrase := match(
		"insufficient funds",
		"txpool is full",
		"account limit exceeded",
	); phrase != "" {
		return db.SendErrorRetryableEnv, phrase
	}
	// Deterministic rejections of this transaction.
	if phrase := match(
		"intrinsic gas too low",
		"exceeds block gas limit",
		"invalid sender",
		"sender not an eoa",
		"transaction type not supported",
		"only replay-protected",
		"oversized data",
		"negative value",
		"gas limit reached",
	); phrase != "" {
		return db.SendErrorDefinitive, phrase
	}

	// Unknown transport/RPC failure: assume the transaction may have been
	// accepted and let receipt polling decide.
	return db.SendErrorAmbiguous, "unrecognized broadcast error"
}
