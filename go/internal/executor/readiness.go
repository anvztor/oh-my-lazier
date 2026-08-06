package executor

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/islishude/oh-my-lazier/go/internal/db"
	"github.com/islishude/oh-my-lazier/go/internal/lzabi"
	"github.com/islishude/oh-my-lazier/go/internal/rpcquorum"
)

var (
	endpointViewABI   = lzabi.EndpointV2ABI()
	receiveUlnViewABI = lzabi.ReceiveUln302ABI()
)

var (
	emptyPayloadHash common.Hash
	nilPayloadHash   = common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
)

// ContractCaller is the verified read surface used by executor readiness
// checks: anchor-routed eth_call (by number or pinned block hash), the head
// number for confirmation-depth math, and the verified head for tip anchors.
type ContractCaller interface {
	CallContract(ctx context.Context, call ethereum.CallMsg, anchor *big.Int) ([]byte, error)
	CallContractAtHash(ctx context.Context, call ethereum.CallMsg, blockHash common.Hash) ([]byte, error)
	BlockNumber(ctx context.Context) (uint64, error)
	CheckHead(ctx context.Context) (rpcquorum.HeadResult, error)
}

// CommitState describes destination-chain commitVerification readiness.
type CommitState int

const (
	// CommitNotVerifiable means commitVerification is not ready and not already completed.
	CommitNotVerifiable CommitState = iota
	// CommitVerifiable means commitVerification can be enqueued.
	CommitVerifiable
	// CommitCommitted means the endpoint already stores the packet payload hash.
	CommitCommitted
)

// DeliveryState describes destination-chain lzReceive readiness.
type DeliveryState int

const (
	// DeliveryNotExecutable means lzReceive is not ready and not already delivered.
	DeliveryNotExecutable DeliveryState = iota
	// DeliveryExecutable means lzReceive can be enqueued.
	DeliveryExecutable
	// DeliveryDelivered means the endpoint already cleared the packet payload.
	DeliveryDelivered
)

// IsCommitVerifiable checks the EndpointV2 and ReceiveUln302 readiness gates before commit enqueue.
func IsCommitVerifiable(ctx context.Context, caller ContractCaller, endpoint, receiveLib common.Address, packet db.PacketRecord) (bool, error) {
	state, err := CheckCommitState(ctx, caller, endpoint, receiveLib, packet, nil)
	return state == CommitVerifiable, err
}

// CheckCommitState reconciles on-chain commit state before deciding whether commitVerification should be enqueued.
func CheckCommitState(ctx context.Context, caller ContractCaller, endpoint, receiveLib common.Address, packet db.PacketRecord, anchor *rpcquorum.StateAnchor) (CommitState, error) {
	if err := packet.Validate(); err != nil {
		return CommitNotVerifiable, err
	}
	if caller == nil {
		return CommitNotVerifiable, errors.New("contract caller is required")
	}
	if endpoint == (common.Address{}) {
		return CommitNotVerifiable, errors.New("endpoint address is required")
	}
	if receiveLib == (common.Address{}) {
		return CommitNotVerifiable, errors.New("receive lib address is required")
	}
	if packet.PayloadHash == emptyPayloadHash {
		return CommitNotVerifiable, nil
	}
	origin := originFromPacket(packet)
	payloadHash, err := callEndpointHash(ctx, caller, endpoint, "inboundPayloadHash", anchor, packet.Receiver, packet.SrcEID, origin.Sender, origin.Nonce)
	if err != nil {
		return CommitNotVerifiable, err
	}
	if payloadHash == packet.PayloadHash {
		return CommitCommitted, nil
	}
	validReceiveLib, err := callBool(ctx, caller, endpointViewABI, endpoint, "isValidReceiveLibrary", anchor, packet.Receiver, packet.SrcEID, receiveLib)
	if err != nil {
		return CommitNotVerifiable, err
	}
	if !validReceiveLib {
		return CommitNotVerifiable, nil
	}
	endpointVerifiable, err := callBool(ctx, caller, endpointViewABI, endpoint, "verifiable", anchor, origin, packet.Receiver)
	if err != nil {
		return CommitNotVerifiable, err
	}
	if !endpointVerifiable {
		return CommitNotVerifiable, nil
	}
	config, err := callUlnConfig(ctx, caller, receiveLib, packet.Receiver, packet.SrcEID, anchor)
	if err != nil {
		return CommitNotVerifiable, err
	}
	verifiable, err := callBool(ctx, caller, receiveUlnViewABI, receiveLib, "verifiable", anchor, config, crypto.Keccak256Hash(packet.PacketHeader), packet.PayloadHash)
	if err != nil || !verifiable {
		return CommitNotVerifiable, err
	}
	return CommitVerifiable, nil
}

// IsLzReceiveExecutable checks whether EndpointV2 state allows this packet to be delivered.
func IsLzReceiveExecutable(ctx context.Context, caller ContractCaller, endpoint common.Address, packet db.PacketRecord) (bool, error) {
	state, err := CheckDeliveryState(ctx, caller, endpoint, packet, nil)
	return state == DeliveryExecutable, err
}

// CheckDeliveryState reconciles on-chain delivery state before deciding whether lzReceive should be enqueued.
func CheckDeliveryState(ctx context.Context, caller ContractCaller, endpoint common.Address, packet db.PacketRecord, anchor *rpcquorum.StateAnchor) (DeliveryState, error) {
	if err := packet.Validate(); err != nil {
		return DeliveryNotExecutable, err
	}
	if caller == nil {
		return DeliveryNotExecutable, errors.New("contract caller is required")
	}
	if endpoint == (common.Address{}) {
		return DeliveryNotExecutable, errors.New("endpoint address is required")
	}
	origin := originFromPacket(packet)
	payloadHash, err := callEndpointHash(ctx, caller, endpoint, "inboundPayloadHash", anchor, packet.Receiver, packet.SrcEID, origin.Sender, origin.Nonce)
	if err != nil {
		return DeliveryNotExecutable, err
	}
	lazyInboundNonce, err := callUint64(ctx, caller, endpoint, "lazyInboundNonce", anchor, packet.Receiver, packet.SrcEID, origin.Sender)
	if err != nil {
		return DeliveryNotExecutable, err
	}
	if payloadHash == emptyPayloadHash && origin.Nonce <= lazyInboundNonce {
		return DeliveryDelivered, nil
	}
	if payloadHash != packet.PayloadHash {
		return DeliveryNotExecutable, nil
	}
	if payloadHash == emptyPayloadHash || payloadHash == nilPayloadHash {
		return DeliveryNotExecutable, nil
	}
	inboundNonce, err := callUint64(ctx, caller, endpoint, "inboundNonce", anchor, packet.Receiver, packet.SrcEID, origin.Sender)
	if err != nil {
		return DeliveryNotExecutable, err
	}
	if origin.Nonce > inboundNonce {
		return DeliveryNotExecutable, nil
	}
	return DeliveryExecutable, nil
}

type ulnConfig struct {
	Confirmations        uint64           `abi:"confirmations"`
	RequiredDVNCount     uint8            `abi:"requiredDVNCount"`
	OptionalDVNCount     uint8            `abi:"optionalDVNCount"`
	OptionalDVNThreshold uint8            `abi:"optionalDVNThreshold"`
	RequiredDVNs         []common.Address `abi:"requiredDVNs"`
	OptionalDVNs         []common.Address `abi:"optionalDVNs"`
}

func originFromPacket(packet db.PacketRecord) endpointOrigin {
	return endpointOrigin{
		SrcEID: packet.SrcEID,
		Sender: common.BytesToHash(
			packet.Sender.Bytes(),
		),
		Nonce: packet.Nonce.Uint64(),
	}
}

func callUlnConfig(ctx context.Context, caller ContractCaller, receiveLib, receiver common.Address, srcEID uint32, anchor *rpcquorum.StateAnchor) (ulnConfig, error) {
	data, err := receiveUlnViewABI.Pack("getUlnConfig", receiver, srcEID)
	if err != nil {
		return ulnConfig{}, err
	}
	result, err := rpcquorum.CallAtAnchor(ctx, caller, ethereum.CallMsg{To: &receiveLib, Data: data}, anchor)
	if err != nil {
		return ulnConfig{}, err
	}
	values, err := receiveUlnViewABI.Unpack("getUlnConfig", result)
	if err != nil {
		return ulnConfig{}, err
	}
	if len(values) != 1 {
		return ulnConfig{}, fmt.Errorf("getUlnConfig returned %d values, want 1", len(values))
	}
	return ulnConfigFromABI(values[0])
}

func ulnConfigFromABI(value any) (ulnConfig, error) {
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Struct {
		return ulnConfig{}, fmt.Errorf("getUlnConfig returned %T, want tuple struct", value)
	}
	config := ulnConfig{
		Confirmations:        uint64Field(reflected, "Confirmations"),
		RequiredDVNCount:     uint8Field(reflected, "RequiredDVNCount"),
		OptionalDVNCount:     uint8Field(reflected, "OptionalDVNCount"),
		OptionalDVNThreshold: uint8Field(reflected, "OptionalDVNThreshold"),
	}
	requiredDVNs, ok := reflected.FieldByName("RequiredDVNs").Interface().([]common.Address)
	if !ok {
		return ulnConfig{}, fmt.Errorf("getUlnConfig requiredDVNs has type %T", reflected.FieldByName("RequiredDVNs").Interface())
	}
	optionalDVNs, ok := reflected.FieldByName("OptionalDVNs").Interface().([]common.Address)
	if !ok {
		return ulnConfig{}, fmt.Errorf("getUlnConfig optionalDVNs has type %T", reflected.FieldByName("OptionalDVNs").Interface())
	}
	config.RequiredDVNs = append([]common.Address(nil), requiredDVNs...)
	config.OptionalDVNs = append([]common.Address(nil), optionalDVNs...)
	return config, nil
}

func uint64Field(value reflect.Value, name string) uint64 {
	return value.FieldByName(name).Uint()
}

func uint8Field(value reflect.Value, name string) uint8 {
	return uint8(value.FieldByName(name).Uint())
}

func callBool(ctx context.Context, caller ContractCaller, contractABI abiLike, to common.Address, method string, anchor *rpcquorum.StateAnchor, args ...any) (bool, error) {
	values, err := callAndUnpack(ctx, caller, contractABI, to, method, anchor, args...)
	if err != nil {
		return false, err
	}
	value, ok := values[0].(bool)
	if !ok {
		return false, fmt.Errorf("%s returned %T, want bool", method, values[0])
	}
	return value, nil
}

func callEndpointHash(ctx context.Context, caller ContractCaller, to common.Address, method string, anchor *rpcquorum.StateAnchor, args ...any) (common.Hash, error) {
	values, err := callAndUnpack(ctx, caller, endpointViewABI, to, method, anchor, args...)
	if err != nil {
		return common.Hash{}, err
	}
	value, ok := values[0].([32]byte)
	if !ok {
		return common.Hash{}, fmt.Errorf("%s returned %T, want bytes32", method, values[0])
	}
	return common.BytesToHash(value[:]), nil
}

func callUint64(ctx context.Context, caller ContractCaller, to common.Address, method string, anchor *rpcquorum.StateAnchor, args ...any) (uint64, error) {
	values, err := callAndUnpack(ctx, caller, endpointViewABI, to, method, anchor, args...)
	if err != nil {
		return 0, err
	}
	value, ok := values[0].(uint64)
	if !ok {
		return 0, fmt.Errorf("%s returned %T, want uint64", method, values[0])
	}
	return value, nil
}

type abiLike interface {
	Pack(name string, args ...any) ([]byte, error)
	Unpack(name string, data []byte) ([]any, error)
}

func callAndUnpack(ctx context.Context, caller ContractCaller, contractABI abiLike, to common.Address, method string, anchor *rpcquorum.StateAnchor, args ...any) ([]any, error) {
	data, err := contractABI.Pack(method, args...)
	if err != nil {
		return nil, err
	}
	result, err := rpcquorum.CallAtAnchor(ctx, caller, ethereum.CallMsg{To: &to, Data: data}, anchor)
	if err != nil {
		return nil, err
	}
	values, err := contractABI.Unpack(method, result)
	if err != nil {
		return nil, err
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("%s returned %d values, want 1", method, len(values))
	}
	return values, nil
}
