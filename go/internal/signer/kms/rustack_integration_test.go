package kms

import (
	"context"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestRustackKMSIntegrationSignsEthereumTransaction(t *testing.T) {
	endpoint := os.Getenv("RUSTACK_KMS_ENDPOINT")
	if endpoint == "" {
		t.Skip("RUSTACK_KMS_ENDPOINT is not set")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}
	rawClient := awskms.NewFromConfig(cfg, func(o *awskms.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	createOut, err := rawClient.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  kmstypes.KeySpecEccSecgP256k1,
		KeyUsage: kmstypes.KeyUsageTypeSignVerify,
	})
	if err != nil {
		t.Fatalf("CreateKey() error = %v", err)
	}
	if createOut.KeyMetadata == nil || createOut.KeyMetadata.KeyId == nil {
		t.Fatal("CreateKey() returned no key id")
	}
	keyID := *createOut.KeyMetadata.KeyId

	publicKeyOut, err := rawClient.GetPublicKey(ctx, &awskms.GetPublicKeyInput{KeyId: aws.String(keyID)})
	if err != nil {
		t.Fatalf("GetPublicKey() error = %v", err)
	}
	publicKey, err := parseKMSPublicKey(publicKeyOut.PublicKey)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	address := crypto.PubkeyToAddress(*publicKey)

	signer := New(NewSDKClientWithEndpoint(cfg, endpoint), keyID, address)
	if err := signer.ValidateKey(ctx); err != nil {
		t.Fatalf("ValidateKey() error = %v", err)
	}
	chainID := big.NewInt(11155111)
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	tx := gethtypes.NewTx(&gethtypes.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     1,
		GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(2_000_000_000),
		Gas:       100_000,
		To:        &to,
		Value:     big.NewInt(1),
		Data:      []byte{0x01, 0x02},
	})
	signed, err := signer.SignTx(ctx, tx, chainID)
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
