package pricing

import (
	"errors"
	"fmt"
	"strings"

	gethabi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

type priceSourceRequestError struct {
	source    string
	operation string
	cause     error
}

func (e *priceSourceRequestError) Error() string {
	if e == nil {
		return "price source request failed"
	}
	return fmt.Sprintf("%s price request %s failed", e.source, e.operation)
}

func (e *priceSourceRequestError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func wrapPriceSourceRequestError(source, operation string, err error) error {
	if err == nil {
		return nil
	}
	return &priceSourceRequestError{source: source, operation: operation, cause: err}
}

func isPriceSourceRequestError(err error) bool {
	var requestError *priceSourceRequestError
	return errors.As(err, &requestError)
}

func isPriceSourceContractRevert(err error) bool {
	var rpcError rpc.Error
	if errors.As(err, &rpcError) && rpcError.ErrorCode() == 3 {
		return true
	}
	if dataError, ok := errors.AsType[rpc.DataError](err); ok {
		switch data := dataError.ErrorData().(type) {
		case string:
			normalized := strings.ToLower(strings.TrimSpace(data))
			if strings.HasPrefix(normalized, "0x") || strings.Contains(normalized, "execution reverted") {
				return true
			}
		case []byte:
			if len(data) > 0 {
				return true
			}
		}
	}
	for current := err; current != nil; current = errors.Unwrap(current) {
		if strings.Contains(strings.ToLower(current.Error()), "execution reverted") {
			return true
		}
	}
	return false
}

func isPriceSourceContractRevertReason(err error, expected string) bool {
	if dataError, ok := errors.AsType[rpc.DataError](err); ok {
		if reason, ok := priceSourceContractRevertReasonData(dataError.ErrorData()); ok && reason == expected {
			return true
		}
	}
	for current := err; current != nil; current = errors.Unwrap(current) {
		if reason, ok := priceSourceContractRevertReasonMessage(current.Error()); ok && reason == expected {
			return true
		}
	}
	return false
}

func priceSourceContractRevertReasonData(data any) (string, bool) {
	var encoded []byte
	switch value := data.(type) {
	case string:
		decoded, err := hexutil.Decode(strings.TrimSpace(value))
		if err != nil {
			return priceSourceContractRevertReasonMessage(value)
		}
		encoded = decoded
	case []byte:
		encoded = value
	default:
		return "", false
	}
	reason, err := gethabi.UnpackRevert(encoded)
	if err != nil {
		return "", false
	}
	return reason, true
}

func priceSourceContractRevertReasonMessage(message string) (string, bool) {
	const marker = "execution reverted"
	trimmed := strings.TrimSpace(message)
	index := strings.LastIndex(strings.ToLower(trimmed), marker)
	if index < 0 {
		return "", false
	}
	reason := strings.TrimSpace(trimmed[index+len(marker):])
	reason = strings.TrimSpace(strings.TrimPrefix(reason, ":"))
	reason = strings.Trim(reason, `"'`)
	if reason == "" {
		return "", false
	}
	return reason, true
}
