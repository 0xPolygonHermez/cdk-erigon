package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/bitmapdb"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

// GetLogsByHash implements erigon_getLogsByHash. Returns an array of arrays of logs generated by the transactions in the block given by the block's hash.
func (api *ErigonImpl) GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	block, err := api.blockByHashWithSenders(tx, hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	receipts, err := api.getReceipts(ctx, tx, chainConfig, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}

	logs := make([][]*types.Log, len(receipts))
	for i, receipt := range receipts {
		logs[i] = receipt.Logs
	}
	return logs, nil
}

// GetLogs implements erigon_getLogs. Returns an array of logs matching a given filter object.
func (api *ErigonImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.ErigonLogs, error) {
	var begin, end uint64
	erigonLogs := types.ErigonLogs{}

	tx, beginErr := api.db.BeginRo(ctx)
	if beginErr != nil {
		return erigonLogs, beginErr
	}
	defer tx.Rollback()

	if crit.BlockHash != nil {
		number := rawdb.ReadHeaderNumber(tx, *crit.BlockHash)
		if number == nil {
			return nil, fmt.Errorf("block not found: %x", *crit.BlockHash)
		}
		begin = *number
		end = *number
	} else {
		// Convert the RPC block numbers into internal representations
		latest, err := rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return nil, err
		}

		begin = latest
		if crit.FromBlock != nil {
			if crit.FromBlock.Sign() >= 0 {
				begin = crit.FromBlock.Uint64()
			} else if !crit.FromBlock.IsInt64() || crit.FromBlock.Int64() != int64(rpc.LatestBlockNumber) {
				return nil, fmt.Errorf("negative value for FromBlock: %v", crit.FromBlock)
			}
		}
		end = latest
		if crit.ToBlock != nil {
			if crit.ToBlock.Sign() >= 0 {
				end = crit.ToBlock.Uint64()
			} else if !crit.ToBlock.IsInt64() || crit.ToBlock.Int64() != int64(rpc.LatestBlockNumber) {
				return nil, fmt.Errorf("negative value for ToBlock: %v", crit.ToBlock)
			}
		}
	}
	if end < begin {
		return nil, fmt.Errorf("end (%d) < begin (%d)", end, begin)
	}
	if end > roaring.MaxUint32 {
		return nil, fmt.Errorf("end (%d) > MaxUint32", end)
	}
	blockNumbers := bitmapdb.NewBitmap()
	defer bitmapdb.ReturnToPool(blockNumbers)
	if err := applyFilters(blockNumbers, tx, begin, end, crit); err != nil {
		return nil, err
	}
	if blockNumbers.IsEmpty() {
		return erigonLogs, nil
	}

	addrMap := make(map[common.Address]struct{}, len(crit.Addresses))
	for _, v := range crit.Addresses {
		addrMap[v] = struct{}{}
	}
	iter := blockNumbers.Iterator()
	for iter.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		blockNumber := uint64(iter.Next())
		var logIndex uint
		var txIndex uint
		var blockLogs []*types.Log
		it, err := tx.Prefix(kv.Log, hexutility.EncodeTs(blockNumber))
		if err != nil {
			return nil, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return erigonLogs, err
			}
			var logs types.Logs
			if err := cbor.Unmarshal(&logs, bytes.NewReader(v)); err != nil {
				return erigonLogs, fmt.Errorf("receipt unmarshal failed:  %w", err)
			}
			for _, log := range logs {
				log.Index = logIndex
				logIndex++
			}
			filtered := logs.Filter(addrMap, crit.Topics)
			if len(filtered) == 0 {
				continue
			}
			txIndex = uint(binary.BigEndian.Uint32(k[8:]))
			for _, log := range filtered {
				log.TxIndex = txIndex
			}
			blockLogs = append(blockLogs, filtered...)
		}
		if len(blockLogs) == 0 {
			continue
		}

		header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNumber)
		if err != nil {
			return nil, err
		}
		if header == nil {
			return nil, fmt.Errorf("block header not found: %d", blockNumber)
		}
		timestamp := header.Time

		blockHash := header.Hash()
		body, err := api._blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
		if err != nil {
			return nil, err
		}
		if body == nil {
			return nil, fmt.Errorf("block not found %d", blockNumber)
		}
		for _, log := range blockLogs {
			erigonLog := &types.ErigonLog{}
			erigonLog.BlockNumber = blockNumber
			erigonLog.BlockHash = blockHash
			if log.TxIndex == uint(len(body.Transactions)) {
				erigonLog.TxHash = types.ComputeBorTxHash(blockNumber, blockHash)
			} else {
				erigonLog.TxHash = body.Transactions[log.TxIndex].Hash()
			}
			erigonLog.Timestamp = timestamp
			erigonLog.Address = log.Address
			erigonLog.Topics = log.Topics
			erigonLog.Data = log.Data
			erigonLog.Index = log.Index
			erigonLog.Removed = log.Removed
			erigonLog.TxIndex = log.TxIndex
			erigonLogs = append(erigonLogs, erigonLog)
		}
	}

	return erigonLogs, nil
}

// GetLatestLogs implements erigon_getLatestLogs.
// Return specific number of logs or block matching a give filter objects by descend.
// IgnoreTopicsOrder option provide a way to match the logs with addresses and topics without caring the topics's orders
// When IgnoreTopicsOrde option is true, once the logs have a topic that matched, it will be returned no matter what topic position it is in.

// blockCount parameter is for better pagination.
// `crit` filter is the same filter.
//
// Examples:
// {} or nil          matches any topics list
// {{A}}              matches topic A in any positions. Logs with {{B}, {A}} will be matched
func (api *ErigonImpl) GetLatestLogs(ctx context.Context, crit filters.FilterCriteria, logOptions filters.LogFilterOptions) (types.ErigonLogs, error) {
	if logOptions.LogCount != 0 && logOptions.BlockCount != 0 {
		return nil, fmt.Errorf("logs count & block count are ambigious")
	}
	if logOptions.LogCount == 0 && logOptions.BlockCount == 0 {
		logOptions = filters.DefaultLogFilterOptions()
	}
	erigonLogs := types.ErigonLogs{}
	tx, beginErr := api.db.BeginRo(ctx)
	if beginErr != nil {
		return erigonLogs, beginErr
	}
	defer tx.Rollback()
	var latest uint64
	var err error
	if crit.ToBlock != nil {
		if crit.ToBlock.Sign() >= 0 {
			latest = crit.ToBlock.Uint64()
		} else if !crit.ToBlock.IsInt64() || crit.ToBlock.Int64() != int64(rpc.LatestBlockNumber) {
			return nil, fmt.Errorf("negative value for ToBlock: %v", crit.ToBlock)
		}
	} else {
		latest, err = rpchelper.GetLatestBlockNumber(tx)
		//to fetch latest
		latest += 1
		if err != nil {
			return nil, err
		}
	}

	blockNumbers := bitmapdb.NewBitmap()
	defer bitmapdb.ReturnToPool(blockNumbers)
	if err := applyFilters(blockNumbers, tx, 0, latest, crit); err != nil {
		return erigonLogs, err
	}
	if blockNumbers.IsEmpty() {
		return erigonLogs, nil
	}

	addrMap := make(map[common.Address]struct{}, len(crit.Addresses))
	for _, v := range crit.Addresses {
		addrMap[v] = struct{}{}
	}
	topicsMap := make(map[common.Hash]struct{})
	for i := range crit.Topics {
		for j := range crit.Topics {
			topicsMap[crit.Topics[i][j]] = struct{}{}
		}
	}

	// latest logs that match the filter crit
	iter := blockNumbers.ReverseIterator()
	var logCount, blockCount uint64
	for iter.HasNext() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		blockNumber := uint64(iter.Next())
		var logIndex uint
		var txIndex uint
		var blockLogs []*types.Log
		it, err := tx.Prefix(kv.Log, hexutility.EncodeTs(blockNumber))
		if err != nil {
			return nil, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return erigonLogs, err
			}
			var logs types.Logs
			if err := cbor.Unmarshal(&logs, bytes.NewReader(v)); err != nil {
				return erigonLogs, fmt.Errorf("receipt unmarshal failed:  %w", err)
			}
			for _, log := range logs {
				log.Index = logIndex
				logIndex++
			}
			var filtered types.Logs
			if logOptions.IgnoreTopicsOrder {
				filtered = logs.CointainTopics(addrMap, topicsMap)
			} else {
				filtered = logs.Filter(addrMap, crit.Topics)
			}
			if len(filtered) == 0 {
				continue
			}
			txIndex = uint(binary.BigEndian.Uint32(k[8:]))
			for i := range filtered {
				filtered[i].TxIndex = txIndex
			}
			for i := len(filtered) - 1; i >= 0; i-- {
				blockLogs = append(blockLogs, filtered[i])
				logCount++
			}
			if logOptions.LogCount != 0 && logOptions.LogCount == logCount {
				continue
			}
		}
		blockCount++
		if len(blockLogs) == 0 {
			continue
		}

		header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNumber)
		if err != nil {
			return nil, err
		}
		if header == nil {
			return nil, fmt.Errorf("block header not found: %d", blockNumber)
		}
		timestamp := header.Time

		blockHash := header.Hash()

		body, err := api._blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
		if err != nil {
			return nil, err
		}
		if body == nil {
			return nil, fmt.Errorf("block not found %d", blockNumber)
		}
		for _, log := range blockLogs {
			erigonLog := &types.ErigonLog{}
			erigonLog.BlockNumber = blockNumber
			erigonLog.BlockHash = blockHash
			if log.TxIndex == uint(len(body.Transactions)) {
				erigonLog.TxHash = types.ComputeBorTxHash(blockNumber, blockHash)
			} else {
				erigonLog.TxHash = body.Transactions[log.TxIndex].Hash()
			}
			erigonLog.Timestamp = timestamp
			erigonLog.Address = log.Address
			erigonLog.Topics = log.Topics
			erigonLog.Data = log.Data
			erigonLog.Index = log.Index
			erigonLog.Removed = log.Removed
			erigonLogs = append(erigonLogs, erigonLog)
		}

		if logOptions.LogCount != 0 && logOptions.LogCount == logCount {
			return erigonLogs, nil
		}
		if logOptions.BlockCount != 0 && logOptions.BlockCount == blockCount {
			return erigonLogs, nil
		}
	}
	return erigonLogs, nil
}

func (api *ErigonImpl) GetBlockReceiptsByBlockHash(ctx context.Context, cannonicalBlockHash common.Hash) ([]map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	isCanonicalHash, err := rawdb.IsCanonicalHash(tx, cannonicalBlockHash)
	if err != nil {
		return nil, err
	}

	if !isCanonicalHash {
		return nil, fmt.Errorf("the hash %s is not cannonical", cannonicalBlockHash)
	}

	blockNum, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithHash(cannonicalBlockHash, true), tx, api.filters)
	if err != nil {
		return nil, err
	}
	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	receipts, err := api.getReceipts(ctx, tx, chainConfig, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}
	result := make([]map[string]interface{}, 0, len(receipts))
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		result = append(result, marshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true))
	}

	if chainConfig.Bor != nil {
		borTx, _, _, _ := rawdb.ReadBorTransactionForBlock(tx, block)
		if borTx != nil {
			borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), block.NumberU64(), receipts)
			if err != nil {
				return nil, err
			}
			if borReceipt != nil {
				result = append(result, marshalReceipt(borReceipt, borTx, chainConfig, block.HeaderNoCopy(), borReceipt.TxHash, false))
			}
		}
	}

	return result, nil
}

// GetLogsByNumber implements erigon_getLogsByHash. Returns all the logs that appear in a block given the block's hash.
// func (api *ErigonImpl) GetLogsByNumber(ctx context.Context, number rpc.BlockNumber) ([][]*types.Log, error) {
// 	tx, err := api.db.Begin(ctx, false)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer tx.Rollback()

// 	number := rawdb.ReadHeaderNumber(tx, hash)
// 	if number == nil {
// 		return nil, fmt.Errorf("block not found: %x", hash)
// 	}

// 	receipts, err := getReceipts(ctx, tx, *number, hash)
// 	if err != nil {
// 		return nil, fmt.Errorf("getReceipts error: %w", err)
// 	}

// 	logs := make([][]*types.Log, len(receipts))
// 	for i, receipt := range receipts {
// 		logs[i] = receipt.Logs
// 	}
// 	return logs, nil
// }
