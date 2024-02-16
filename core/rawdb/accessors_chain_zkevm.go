package rawdb

import (
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

func DeleteTransactions(db kv.RwTx, txsCount, baseTxId uint64, blockHash *libcommon.Hash) error {
	for id := baseTxId; id < baseTxId+txsCount; id++ {
		txIdKey := make([]byte, 8)
		binary.BigEndian.PutUint64(txIdKey, id)

		var err error
		if blockHash != nil {
			key := append(txIdKey, blockHash.Bytes()...)
			db.Delete(kv.EthTxV3, key)
		} else {
			db.Delete(kv.EthTx, txIdKey)
		}

		if err != nil {
			return fmt.Errorf("error deleting tx: %w", err)
		}
	}

	return nil
}

func TruncateBodies(tx kv.RwTx, blockNum uint64) error {
	if err := tx.ForEach(kv.BlockBody, hexutility.EncodeTs(blockNum), func(k, v []byte) error {
		var body types.BodyForStorage
		if err := rlp.DecodeBytes(v, &body); err != nil {
			return fmt.Errorf("failed to decode body: %w", err)
		}

		txs, err := CanonicalTransactions(tx, body.BaseTxId, body.TxAmount)
		if err != nil {
			return fmt.Errorf("failed to read txs: %w", err)
		}

		blockhash := libcommon.BytesToHash(k)
		// delete body for storage
		deleteBody(tx, blockhash, blockNum)

		// TODO: decrement sequence?
		// decrement txs sequence
		// if err := tx.DecrementSequence(kv.EthTx, uint64(body.TxAmount)); err != nil {
		// 	return fmt.Errorf("failed to decrement sequence: %w", err)
		// }

		// delete transactions
		if err := DeleteTransactions(tx, uint64(len(txs)), body.BaseTxId, &blockhash); err != nil {
			return fmt.Errorf("failed to delete txs: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("TruncateBodies: %w", err)
	}
	return nil
}

func GetBodyTransactions(tx kv.RwTx, fromBlockNum, toBlockNum uint64) (*[]types.Transaction, error) {
	var transactions []types.Transaction
	if err := tx.ForEach(kv.BlockBody, hexutility.EncodeTs(fromBlockNum), func(k, v []byte) error {
		blocNum := binary.BigEndian.Uint64(k[:8])
		if blocNum < fromBlockNum || blocNum > toBlockNum {
			return nil
		}

		var body types.BodyForStorage
		if err := rlp.DecodeBytes(v, &body); err != nil {
			return fmt.Errorf("failed to decode body: %w", err)
		}

		txs, err := CanonicalTransactions(tx, body.BaseTxId, body.TxAmount)
		if err != nil {
			return fmt.Errorf("failed to read txs: %w", err)
		}
		transactions = append(transactions, txs...)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("TruncateBodies: %w", err)
	}
	return &transactions, nil
}

func DeleteForkchoiceFinalized(db kv.Deleter) error {
	if err := db.Delete(kv.LastForkchoice, []byte("finalizedBlockHash")); err != nil {
		return fmt.Errorf("failed to delete LastForkchoice: %w", err)
	}

	return nil
}
