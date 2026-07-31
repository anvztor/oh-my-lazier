package kms

import (
	"context"
	"crypto/ecdsa"
	"encoding/asn1"
	"errors"
	"fmt"
	"math/big"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

const (
	ethereumSignatureLength = 65
)

var (
	secp256k1N     = crypto.S256().Params().N
	secp256k1HalfN = new(big.Int).Rsh(new(big.Int).Set(secp256k1N), 1)
)

// Client is the KMS API boundary used by Signer.
type Client interface {
	GetPublicKey(ctx context.Context, keyID string) ([]byte, kmstypes.KeySpec, error)
	SignDigest(ctx context.Context, keyID string, digest common.Hash) ([]byte, error)
}

// SDKClient adapts the AWS SDK v2 KMS client to Client.
type SDKClient struct {
	client *awskms.Client
}

// NewSDKClient creates a KMS client backed by AWS SDK v2.
func NewSDKClient(cfg aws.Config) *SDKClient {
	return &SDKClient{client: awskms.NewFromConfig(cfg)}
}

// NewSDKClientWithEndpoint creates a KMS client for an AWS-compatible endpoint such as Rustack.
func NewSDKClientWithEndpoint(cfg aws.Config, endpoint string) *SDKClient {
	return &SDKClient{client: awskms.NewFromConfig(cfg, func(o *awskms.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})}
}

// GetPublicKey returns the KMS DER-encoded public key and asymmetric key spec.
func (c *SDKClient) GetPublicKey(ctx context.Context, keyID string) ([]byte, kmstypes.KeySpec, error) {
	out, err := c.client.GetPublicKey(ctx, &awskms.GetPublicKeyInput{KeyId: aws.String(keyID)})
	if err != nil {
		return nil, "", err
	}
	return out.PublicKey, out.KeySpec, nil
}

// SignDigest signs a 32-byte digest with AWS KMS ECDSA_SHA_256.
func (c *SDKClient) SignDigest(ctx context.Context, keyID string, digest common.Hash) ([]byte, error) {
	out, err := c.client.Sign(ctx, &awskms.SignInput{
		KeyId:            aws.String(keyID),
		Message:          digest.Bytes(),
		MessageType:      kmstypes.MessageTypeDigest,
		SigningAlgorithm: kmstypes.SigningAlgorithmSpecEcdsaSha256,
	})
	if err != nil {
		return nil, err
	}
	return out.Signature, nil
}

// Signer signs Ethereum payloads through AWS KMS.
type Signer struct {
	client  Client
	keyID   string
	address common.Address
}

// New creates a KMS signer for an expected Ethereum address.
func New(client Client, keyID string, address common.Address) *Signer {
	return &Signer{client: client, keyID: keyID, address: address}
}

// ValidateKey confirms the configured KMS key is ECC_SECG_P256K1 and that its
// public key resolves to the configured Ethereum address, so a key/address
// mismatch fails fast at startup instead of at the first signature recovery.
func (s *Signer) ValidateKey(ctx context.Context) error {
	if s.client == nil {
		return errors.New("kms client is required")
	}
	der, spec, err := s.client.GetPublicKey(ctx, s.keyID)
	if err != nil {
		return err
	}
	if spec != kmstypes.KeySpecEccSecgP256k1 {
		return fmt.Errorf("kms key %s has key spec %s, want %s", s.keyID, spec, kmstypes.KeySpecEccSecgP256k1)
	}
	publicKey, err := parseKMSPublicKey(der)
	if err != nil {
		return fmt.Errorf("kms key %s has an invalid public key: %w", s.keyID, err)
	}
	if derived := crypto.PubkeyToAddress(*publicKey); derived != s.address {
		return fmt.Errorf("kms key %s resolves to address %s, want %s", s.keyID, derived, s.address)
	}
	return nil
}

type subjectPublicKeyInfo struct {
	Algorithm        asn1.RawValue
	SubjectPublicKey asn1.BitString
}

// parseKMSPublicKey extracts the secp256k1 public key from a DER-encoded
// SubjectPublicKeyInfo as returned by AWS KMS GetPublicKey.
func parseKMSPublicKey(der []byte) (*ecdsa.PublicKey, error) {
	var spki subjectPublicKeyInfo
	rest, err := asn1.Unmarshal(der, &spki)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, errors.New("public key has trailing DER bytes")
	}
	if len(spki.SubjectPublicKey.Bytes) == 0 {
		return nil, errors.New("public key is empty")
	}
	return crypto.UnmarshalPubkey(spki.SubjectPublicKey.Bytes)
}

// Address returns the Ethereum address expected to recover from KMS signatures.
func (s *Signer) Address() common.Address {
	return s.address
}

// SignHash requests an ECDSA signature for a raw digest and returns an Ethereum signature.
func (s *Signer) SignHash(ctx context.Context, digest common.Hash) ([]byte, error) {
	if s.client == nil {
		return nil, errors.New("kms client is required")
	}
	derSignature, err := s.client.SignDigest(ctx, s.keyID, digest)
	if err != nil {
		return nil, err
	}
	r, sigS, err := parseDERSignature(derSignature)
	if err != nil {
		return nil, err
	}
	return recoverEthereumSignature(digest, r, sigS, s.address)
}

// SignTx signs an Ethereum transaction with AWS KMS.
func (s *Signer) SignTx(ctx context.Context, tx *gethtypes.Transaction, chainID *big.Int) (*gethtypes.Transaction, error) {
	signer := gethtypes.LatestSignerForChainID(chainID)
	signature, err := s.SignHash(ctx, signer.Hash(tx))
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(signer, signature)
}

// Type returns the signer backend name.
func (s *Signer) Type() string {
	return "kms"
}

type ecdsaSignature struct {
	R *big.Int
	S *big.Int
}

func parseDERSignature(der []byte) (*big.Int, *big.Int, error) {
	var parsed ecdsaSignature
	rest, err := asn1.Unmarshal(der, &parsed)
	if err != nil {
		return nil, nil, err
	}
	if len(rest) != 0 {
		return nil, nil, errors.New("kms signature has trailing DER bytes")
	}
	if parsed.R == nil || parsed.S == nil || parsed.R.Sign() <= 0 || parsed.S.Sign() <= 0 {
		return nil, nil, errors.New("kms signature contains invalid r or s")
	}
	if parsed.R.Cmp(secp256k1N) >= 0 || parsed.S.Cmp(secp256k1N) >= 0 {
		return nil, nil, errors.New("kms signature r or s exceeds secp256k1 order")
	}
	return parsed.R, parsed.S, nil
}

func recoverEthereumSignature(digest common.Hash, r, sigS *big.Int, expected common.Address) ([]byte, error) {
	normalizedS := new(big.Int).Set(sigS)
	if normalizedS.Cmp(secp256k1HalfN) > 0 {
		normalizedS.Sub(secp256k1N, normalizedS)
	}
	for recoveryID := byte(0); recoveryID <= 1; recoveryID++ {
		signature := make([]byte, ethereumSignatureLength)
		copy(signature[32-len(r.Bytes()):32], r.Bytes())
		copy(signature[64-len(normalizedS.Bytes()):64], normalizedS.Bytes())
		signature[64] = recoveryID
		pub, err := crypto.SigToPub(digest.Bytes(), signature)
		if err != nil {
			continue
		}
		if crypto.PubkeyToAddress(*pub) == expected {
			return signature, nil
		}
	}
	return nil, fmt.Errorf("kms signature did not recover expected address %s", expected)
}

func derFromSignature(r, sigS *big.Int) ([]byte, error) {
	if r == nil || sigS == nil {
		return nil, errors.New("r and s are required")
	}
	return asn1.Marshal(ecdsaSignature{R: r, S: sigS})
}
