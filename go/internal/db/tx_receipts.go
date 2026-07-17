package db

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// TxReceiptFacts records the mined receipt values used for actual gas-cost accounting.
type TxReceiptFacts struct {
	// TxHash is the winning attempt's hash, persisted with the receipt facts so
	// they stay self-contained evidence for the exact transaction the RPC returned.
	TxHash            common.Hash
	Status            uint64
	BlockNumber       uint64
	GasUsed           uint64
	EffectiveGasPrice *big.Int
	GasCostDstWei     *big.Int
}

func validateTxReceiptFacts(facts TxReceiptFacts) error {
	if facts.TxHash == (common.Hash{}) {
		return errors.New("receipt tx hash is required")
	}
	if facts.Status > maxDBNonce {
		return fmt.Errorf("receipt status %d exceeds database integer limit", facts.Status)
	}
	if facts.BlockNumber > maxDBNonce {
		return fmt.Errorf("receipt block number %d exceeds database integer limit", facts.BlockNumber)
	}
	if facts.EffectiveGasPrice == nil || facts.EffectiveGasPrice.Sign() < 0 {
		return errors.New("receipt effective gas price must be non-negative")
	}
	if facts.GasCostDstWei == nil || facts.GasCostDstWei.Sign() < 0 {
		return errors.New("receipt destination gas cost must be non-negative")
	}
	expectedCost := new(big.Int).Mul(new(big.Int).SetUint64(facts.GasUsed), facts.EffectiveGasPrice)
	if expectedCost.Cmp(facts.GasCostDstWei) != 0 {
		return fmt.Errorf("receipt destination gas cost %s does not match gas_used * effective_gas_price %s", facts.GasCostDstWei, expectedCost)
	}
	return nil
}

// MarkTxReceiptCostPriced records the actual receipt gas cost converted into source-chain native wei.
func (s *Store) MarkTxReceiptCostPriced(ctx context.Context, id int64, gasCostSrcWei *big.Int) error {
	if id <= 0 {
		return errors.New("outbox tx id is required")
	}
	if gasCostSrcWei == nil || gasCostSrcWei.Sign() < 0 {
		return errors.New("receipt source gas cost must be non-negative")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE tx_outbox
		SET
			receipt_gas_cost_src_wei = $1,
			receipt_cost_priced_at = now(),
			updated_at = now()
		WHERE id = $2
			AND receipt_gas_cost_dst_wei IS NOT NULL
	`, gasCostSrcWei.String(), id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox tx %d has no receipt gas cost to price", id)
	}
	return nil
}
