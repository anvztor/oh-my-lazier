package kms

import (
	"context"
	"crypto/ecdsa"
	"encoding/asn1"
	"math/big"
	"strings"
	"testing"

	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestValidateKeyRequiresSecp256k1(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	client := &fakeClient{keySpec: kmstypes.KeySpecEccNistP256, key: key}
	signer := New(client, "test-key", crypto.PubkeyToAddress(key.PublicKey))

	if err := signer.ValidateKey(t.Context()); err == nil {
		t.Fatal("ValidateKey() error = nil, want key spec error")
	}
}

func TestValidateKeyRejectsAddressMismatch(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	other, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	signer := New(&fakeClient{keySpec: kmstypes.KeySpecEccSecgP256k1, key: key}, "test-key", crypto.PubkeyToAddress(other.PublicKey))

	err = signer.ValidateKey(t.Context())
	if err == nil {
		t.Fatal("ValidateKey() error = nil, want address mismatch error")
	}
	if want := crypto.PubkeyToAddress(key.PublicKey).Hex(); !strings.Contains(err.Error(), want) {
		t.Fatalf("ValidateKey() error = %q, want derived address %s", err, want)
	}
}

func TestParseDERSignatureRejectsTrailingBytes(t *testing.T) {
	der, err := derFromSignature(big.NewInt(1), big.NewInt(2))
	if err != nil {
		t.Fatalf("derFromSignature() error = %v", err)
	}
	der = append(der, 0x00)

	if _, _, err := parseDERSignature(der); err == nil {
		t.Fatal("parseDERSignature() error = nil, want trailing bytes error")
	}
}

func TestSignHashRecoversExpectedAddress(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	address := crypto.PubkeyToAddress(key.PublicKey)
	signer := New(&fakeClient{keySpec: kmstypes.KeySpecEccSecgP256k1, key: key}, "test-key", address)
	if err := signer.ValidateKey(t.Context()); err != nil {
		t.Fatalf("ValidateKey() error = %v", err)
	}

	digest := crypto.Keccak256Hash([]byte("kms digest"))
	signature, err := signer.SignHash(t.Context(), digest)
	if err != nil {
		t.Fatalf("SignHash() error = %v", err)
	}
	pub, err := crypto.SigToPub(digest.Bytes(), signature)
	if err != nil {
		t.Fatalf("SigToPub() error = %v", err)
	}
	if got := crypto.PubkeyToAddress(*pub); got != address {
		t.Fatalf("recovered address = %s, want %s", got, address)
	}
	s := new(big.Int).SetBytes(signature[32:64])
	if s.Cmp(secp256k1HalfN) > 0 {
		t.Fatalf("signature s is high: %s", s)
	}
}

func TestSignHashRejectsWrongRecoveredAddress(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	signer := New(
		&fakeClient{keySpec: kmstypes.KeySpecEccSecgP256k1, key: key},
		"test-key",
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
	)

	if _, err := signer.SignHash(t.Context(), crypto.Keccak256Hash([]byte("kms digest"))); err == nil {
		t.Fatal("SignHash() error = nil, want recovery validation error")
	}
}

func TestSignHashNormalizesHighS(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	address := crypto.PubkeyToAddress(key.PublicKey)
	signer := New(&fakeClient{keySpec: kmstypes.KeySpecEccSecgP256k1, key: key, forceHighS: true}, "test-key", address)

	signature, err := signer.SignHash(t.Context(), crypto.Keccak256Hash([]byte("kms high s")))
	if err != nil {
		t.Fatalf("SignHash() error = %v", err)
	}
	s := new(big.Int).SetBytes(signature[32:64])
	if s.Cmp(secp256k1HalfN) > 0 {
		t.Fatalf("signature s is high after normalization: %s", s)
	}
}

func TestSignTxSignsDynamicFeeTransaction(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	address := crypto.PubkeyToAddress(key.PublicKey)
	signer := New(&fakeClient{keySpec: kmstypes.KeySpecEccSecgP256k1, key: key}, "test-key", address)
	chainID := big.NewInt(11155111)
	tx := gethtypes.NewTx(&gethtypes.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     3,
		GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(2_000_000_000),
		Gas:       100_000,
		To:        new(common.HexToAddress("0x2222222222222222222222222222222222222222")),
		Value:     big.NewInt(1),
		Data:      []byte{0x01, 0x02},
	})

	signed, err := signer.SignTx(t.Context(), tx, chainID)
	if err != nil {
		t.Fatalf("SignTx() error = %v", err)
	}
	from, err := gethtypes.Sender(gethtypes.LatestSignerForChainID(chainID), signed)
	if err != nil {
		t.Fatalf("Sender() error = %v", err)
	}
	if from != address {
		t.Fatalf("sender = %s, want %s", from, address)
	}
}

func TestSignTxSignsLegacyTransaction(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	address := crypto.PubkeyToAddress(key.PublicKey)
	signer := New(&fakeClient{keySpec: kmstypes.KeySpecEccSecgP256k1, key: key}, "test-key", address)
	chainID := big.NewInt(11155111)
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	tx := gethtypes.NewTx(&gethtypes.LegacyTx{
		Nonce:    3,
		GasPrice: big.NewInt(2_000_000_000),
		Gas:      100_000,
		To:       &to,
		Value:    big.NewInt(1),
		Data:     []byte{0x01, 0x02},
	})

	signed, err := signer.SignTx(t.Context(), tx, chainID)
	if err != nil {
		t.Fatalf("SignTx() error = %v", err)
	}
	if signed.Type() != gethtypes.LegacyTxType {
		t.Fatalf("signed tx type = %d, want %d", signed.Type(), gethtypes.LegacyTxType)
	}
	from, err := gethtypes.Sender(gethtypes.LatestSignerForChainID(chainID), signed)
	if err != nil {
		t.Fatalf("Sender() error = %v", err)
	}
	if from != address {
		t.Fatalf("sender = %s, want %s", from, address)
	}
}

type fakeClient struct {
	keySpec    kmstypes.KeySpec
	key        *ecdsa.PrivateKey
	forceHighS bool
}

func (f *fakeClient) GetPublicKey(context.Context, string) ([]byte, kmstypes.KeySpec, error) {
	point := crypto.FromECDSAPub(&f.key.PublicKey)
	der, err := asn1.Marshal(subjectPublicKeyInfo{
		Algorithm:        asn1.RawValue{Tag: asn1.TagSequence, IsCompound: true, Bytes: []byte{}},
		SubjectPublicKey: asn1.BitString{Bytes: point, BitLength: 8 * len(point)},
	})
	if err != nil {
		return nil, "", err
	}
	return der, f.keySpec, nil
}

func (f *fakeClient) SignDigest(_ context.Context, _ string, digest common.Hash) ([]byte, error) {
	signature, err := crypto.Sign(digest.Bytes(), f.key)
	if err != nil {
		return nil, err
	}
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:64])
	if f.forceHighS && s.Cmp(secp256k1HalfN) <= 0 {
		s.Sub(secp256k1N, s)
	}
	return derFromSignature(r, s)
}
