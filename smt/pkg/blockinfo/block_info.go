package blockinfo

import (
	"context"
	"encoding/hex"
	"math/big"
	"runtime"
	"sync"

	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/log/v3"

	"github.com/gateway-fm/cdk-erigon-lib/common"
)

type ExecutedTxInfo struct {
	Tx                ethTypes.Transaction
	EffectiveGasPrice uint8
	Receipt           *ethTypes.Receipt
	Signer            *common.Address
}

func BuildBlockInfoTree(
	coinbase *common.Address,
	blockNumber,
	blockTime,
	blockGasLimit,
	blockGasUsed uint64,
	ger common.Hash,
	l1BlockHash common.Hash,
	previousStateRoot common.Hash,
	transactionInfos *[]ExecutedTxInfo,
) (*common.Hash, error) {
	infoTree := NewBlockInfoTree()
	keys, vals, err := infoTree.GenerateBlockHeader(&previousStateRoot, coinbase, blockNumber, blockGasLimit, blockTime, &ger, &l1BlockHash)
	if err != nil {
		return nil, err
	}

	log.Trace("info-tree-header",
		"blockNumber", blockNumber,
		"previousStateRoot", previousStateRoot.String(),
		"coinbase", coinbase.String(),
		"blockGasLimit", blockGasLimit,
		"blockGasUsed", blockGasUsed,
		"blockTime", blockTime,
		"ger", ger.String(),
		"l1BlockHash", l1BlockHash.String(),
	)
	var logIndex int64 = 0

	jobs := make(chan TxParserJob, len(*transactionInfos))
	results := make(chan interface{}, len(*transactionInfos))
	var wg sync.WaitGroup

	for workerId := 1; workerId <= runtime.NumCPU(); workerId++ {
		wg.Add(1)
		go txParserWorker(infoTree, blockNumber, jobs, results, &wg)
	}

	for i, txInfo := range *transactionInfos {
		jobs <- TxParserJob{
			txIdx:    i,
			txInfo:   txInfo,
			logIndex: logIndex,
		}

		logIndex += int64(len(txInfo.Receipt.Logs))
	}
	close(jobs)

	wg.Wait()
	close(results)

	for data := range results {
		switch data.(type) {
		case ParsedKeyVals:
			txKeysVals := data.(ParsedKeyVals)
			keys = append(keys, txKeysVals.keys...)
			vals = append(vals, txKeysVals.vals...)

		default:
			log.Error("not supported data type")
		}
	}

	key, val, err := generateBlockGasUsed(blockGasUsed)
	if err != nil {
		return nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	insertBatchCfg := smt.NewInsertBatchConfig(context.Background(), "block_info_tree", false)
	root, err := infoTree.smt.InsertBatch(insertBatchCfg, keys, vals, nil, nil)
	if err != nil {
		return nil, err
	}
	rootHash := common.BigToHash(root.NewRootScalar.ToBigInt())

	log.Trace("info-tree-root", "block", blockNumber, "root", rootHash.String())

	return &rootHash, nil
}

type BlockInfoTree struct {
	smt *smt.SMT
}

func NewBlockInfoTree() *BlockInfoTree {
	return &BlockInfoTree{
		smt: smt.NewSMT(nil, true),
	}
}
func (b *BlockInfoTree) GetRoot() *big.Int {
	return b.smt.LastRoot()
}

func (b *BlockInfoTree) GenerateBlockHeader(oldBlockHash *common.Hash, coinbase *common.Address, blockNumber, gasLimit, timestamp uint64, ger, l1BlochHash *common.Hash) (keys []*utils.NodeKey, vals []*utils.NodeValue8, err error) {
	keys = make([]*utils.NodeKey, 7)
	vals = make([]*utils.NodeValue8, 7)

	if keys[0], vals[0], err = generateL2BlockHash(oldBlockHash); err != nil {
		return nil, nil, err
	}

	if keys[1], vals[1], err = generateCoinbase(coinbase); err != nil {
		return nil, nil, err
	}

	if keys[2], vals[2], err = generateBlockNumber(blockNumber); err != nil {
		return nil, nil, err
	}

	if keys[3], vals[3], err = generateGasLimit(gasLimit); err != nil {
		return nil, nil, err
	}

	if keys[4], vals[4], err = generateTimestamp(timestamp); err != nil {
		return nil, nil, err
	}

	if keys[5], vals[5], err = generateGer(ger); err != nil {
		return nil, nil, err
	}

	if keys[6], vals[6], err = generateL1BlockHash(l1BlochHash); err != nil {
		return nil, nil, err
	}

	return keys, vals, nil
}

func generateL2BlockHash(blockHash *common.Hash) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if value, err = bigInt2NodeVal8(blockHash.Big()); err != nil {
		return nil, nil, err
	}
	return &BlockHeaderBlockHashKey, value, nil
}

func generateCoinbase(coinbase *common.Address) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if value, err = bigInt2NodeVal8(coinbase.Hash().Big()); err != nil {
		return nil, nil, err
	}

	return &BlockHeaderCoinbaseKey, value, nil
}

func generateGasLimit(gasLimit uint64) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if value, err = bigInt2NodeVal8(big.NewInt(0).SetUint64(gasLimit)); err != nil {
		return nil, nil, err
	}
	return &BlockHeaderGasLimitKey, value, nil
}

func generateBlockNumber(blockNumber uint64) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if value, err = bigInt2NodeVal8(big.NewInt(0).SetUint64(blockNumber)); err != nil {
		return nil, nil, err
	}
	return &BlockHeaderNumberKey, value, nil
}

func generateTimestamp(timestamp uint64) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if value, err = bigInt2NodeVal8(big.NewInt(0).SetUint64(timestamp)); err != nil {
		return nil, nil, err
	}

	return &BlockHeaderTimestampKey, value, nil
}

func generateGer(ger *common.Hash) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if value, err = bigInt2NodeVal8(ger.Big()); err != nil {
		return nil, nil, err
	}

	return &BlockHeaderGerKey, value, nil
}

func generateL1BlockHash(blockHash *common.Hash) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if value, err = bigInt2NodeVal8(blockHash.Big()); err != nil {
		return nil, nil, err
	}

	return &BlockHeaderBlockHashL1Key, value, nil
}

func bigInt2NodeVal8(val *big.Int) (*utils.NodeValue8, error) {
	x := utils.ScalarToArrayBig(val)
	v, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func generateL2TxHash(txIndex *big.Int, l2TxHash *big.Int) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if key, err = KeyTxHash(txIndex); err != nil {
		return nil, nil, err
	}
	if value, err = bigInt2NodeVal8(l2TxHash); err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

func generateTxStatus(txIndex *big.Int, status *big.Int) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if key, err = KeyTxStatus(txIndex); err != nil {
		return nil, nil, err
	}
	if value, err = bigInt2NodeVal8(status); err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

func generateCumulativeGasUsed(txIndex, cumulativeGasUsed *big.Int) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if key, err = KeyCumulativeGasUsed(txIndex); err != nil {
		return nil, nil, err
	}
	if value, err = bigInt2NodeVal8(cumulativeGasUsed); err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

func generateTxLog(txIndex *big.Int, logIndex *big.Int, log *big.Int) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if key, err = KeyTxLogs(txIndex, logIndex); err != nil {
		return nil, nil, err
	}
	if value, err = bigInt2NodeVal8(log); err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

func generateTxEffectivePercentage(txIndex, effectivePercentage *big.Int) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	if key, err = KeyEffectivePercentage(txIndex); err != nil {
		return nil, nil, err
	}
	if value, err = bigInt2NodeVal8(effectivePercentage); err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

func generateBlockGasUsed(gasUsed uint64) (key *utils.NodeKey, value *utils.NodeValue8, err error) {
	gasUsedBig := big.NewInt(0).SetUint64(gasUsed)
	if value, err = bigInt2NodeVal8(gasUsedBig); err != nil {
		return nil, nil, err
	}

	return &BlockHeaderGasUsedKey, value, nil
}

func (b *BlockInfoTree) GenerateBlockTxKeysVals(
	l2TxHash *common.Hash,
	txIndex int,
	receipt *ethTypes.Receipt,
	logIndex int64,
	cumulativeGasUsed uint64,
	effectivePercentage uint8,
) ([]*utils.NodeKey, []*utils.NodeValue8, error) {
	var keys []*utils.NodeKey = make([]*utils.NodeKey, 0, 4+len(receipt.Logs))
	var vals []*utils.NodeValue8 = make([]*utils.NodeValue8, 0, 4+len(receipt.Logs))
	txIndexBig := big.NewInt(int64(txIndex))

	key, val, err := generateL2TxHash(txIndexBig, l2TxHash.Big())
	if err != nil {
		return nil, nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	bigStatus := big.NewInt(0).SetUint64(receipt.Status)
	key, val, err = generateTxStatus(txIndexBig, bigStatus)
	if err != nil {
		return nil, nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	bigCumulativeGasUsed := big.NewInt(0).SetUint64(cumulativeGasUsed)
	key, val, err = generateCumulativeGasUsed(txIndexBig, bigCumulativeGasUsed)
	if err != nil {
		return nil, nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	log.Trace("info-tree-tx-inner",
		"tx-index", txIndex,
		"log-index", logIndex,
		"cumulativeGasUsed", cumulativeGasUsed,
		"effective-percentage", effectivePercentage,
		"receipt-status", receipt.Status,
	)

	// now encode the logs
	for _, rLog := range receipt.Logs {
		reducedTopics := ""
		for _, topic := range rLog.Topics {
			reducedTopics += topic.Hex()[2:]
		}

		logToEncode := "0x" + hex.EncodeToString(rLog.Data) + reducedTopics

		logEncodedBig := utils.HashContractBytecodeBigInt(logToEncode)
		key, val, err = generateTxLog(txIndexBig, big.NewInt(logIndex), logEncodedBig)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, key)
		vals = append(vals, val)

		log.Trace("info-tree-tx-receipt-log",
			"topics", reducedTopics,
			"to-encode", logToEncode,
			"log-index", logIndex,
		)

		// increment log index
		logIndex += 1
	}

	// setTxEffectivePercentage
	bigEffectivePercentage := big.NewInt(0).SetUint64(uint64(effectivePercentage))
	key, val, err = generateTxEffectivePercentage(txIndexBig, bigEffectivePercentage)
	if err != nil {
		return nil, nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	return keys, vals, nil
}

type TxParserJob struct {
	txIdx    int
	txInfo   ExecutedTxInfo
	logIndex int64
}

type ParsedKeyVals struct {
	keys []*utils.NodeKey
	vals []*utils.NodeValue8
}

func txParserWorker(infoTree *BlockInfoTree, blockNumber uint64, jobs <-chan TxParserJob, results chan<- interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		receipt := job.txInfo.Receipt
		t := job.txInfo.Tx

		l2TxHash, err := zktx.ComputeL2TxHash(
			t.GetChainID().ToBig(),
			t.GetValue(),
			t.GetPrice(),
			t.GetNonce(),
			t.GetGas(),
			t.GetTo(),
			job.txInfo.Signer,
			t.GetData(),
		)
		if err != nil {
			log.Error("Fail to ComputeL2TxHash", "TxHash", t.Hash(), "error", err)
		}

		log.Trace("info-tree-tx", "block", blockNumber, "idx", job.txIdx, "hash", l2TxHash.String())

		genKeys, genVals, err := infoTree.GenerateBlockTxKeysVals(&l2TxHash, job.txIdx, receipt, job.logIndex, receipt.CumulativeGasUsed, job.txInfo.EffectiveGasPrice)
		if err != nil {
			log.Error("Fail to GenerateBlockTxKeysVals", "TxHash", t.Hash(), "error", err)
		}

		results <- ParsedKeyVals{
			keys: genKeys,
			vals: genVals,
		}
	}
}
