package blockinfo

import (
	"fmt"
	"math/big"

	ethTypes "github.com/ledgerwatch/erigon/core/types"

	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type BlockInfoTree struct {
	smt *smt.SMT
}

func NewBlockInfoTree() *BlockInfoTree {
	return &BlockInfoTree{
		smt: smt.NewSMT(nil),
	}
}
func (b *BlockInfoTree) GetRoot() *big.Int {
	return b.smt.LastRoot()
}

func (b *BlockInfoTree) InitBlockHeader(oldBlockHash *libcommon.Hash, coinbase *libcommon.Address, blockNumber, gasLimit, timestamp uint64, ger, l1BlochHash *libcommon.Hash) error {
	_, err := setL2BlockHash(b.smt, oldBlockHash)
	if err != nil {
		return err
	}
	_, err = setCoinbase(b.smt, coinbase)
	if err != nil {
		return err
	}

	_, err = setBlockNumber(b.smt, blockNumber)
	if err != nil {
		return err
	}

	_, err = setGasLimit(b.smt, gasLimit)
	if err != nil {
		return err
	}
	_, err = setTimestamp(b.smt, timestamp)
	if err != nil {
		return err
	}
	_, err = setGer(b.smt, ger)
	if err != nil {
		return err
	}
	_, err = setL1BlockHash(b.smt, l1BlochHash)
	if err != nil {
		return err
	}
	return nil
}

func (b *BlockInfoTree) SetBlockTx(
	txIndex int,
	receipt *ethTypes.Receipt,
	logIndex int64,
	cumulativeGasUsed uint64,
	effectivePercentage uint8,
) (*big.Int, error) {
	txIndexBig := big.NewInt(int64(txIndex))
	_, err := setL2TxHash(b.smt, txIndexBig, receipt.TxHash.Big())
	if err != nil {
		return nil, err
	}
	_, err = setTxStatus(b.smt, txIndexBig, big.NewInt(int64(receipt.Status)))
	if err != nil {
		return nil, err
	}
	_, err = setCumulativeGasUsed(b.smt, txIndexBig, big.NewInt(int64(cumulativeGasUsed)))
	if err != nil {
		return nil, err
	}

	// now encode the logs
	for _, log := range receipt.Logs {
		reducedTopics := ""
		for _, topic := range log.Topics {
			reducedTopics += fmt.Sprintf("%x", topic)
		}

		logToEncode := fmt.Sprintf("0x%x%s", log.Data, reducedTopics)

		hash, err := utils.HashContractBytecode(logToEncode)
		if err != nil {
			return nil, err
		}

		logEncodedBig := utils.ConvertHexToBigInt(hash)
		_, err = setTxLog(b.smt, txIndexBig, big.NewInt(logIndex), logEncodedBig)
		if err != nil {
			return nil, err
		}

		// increment log index
		logIndex += 1
	}

	root, err := setTxEffectivePercentage(b.smt, txIndexBig, big.NewInt(int64(effectivePercentage)))
	if err != nil {
		return nil, err
	}

	return root, nil
}

func (b *BlockInfoTree) SetBlockGasUsed(gasUsed uint64) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamGasUsed))
	if err != nil {
		return nil, err
	}
	resp, err := b.smt.InsertKA(key, big.NewInt(int64(gasUsed)))
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func BuildBlockInfoTree(
	smt *smt.SMT,
	txIndex *big.Int,
	logs []*ethTypes.Log,
	logIndex *big.Int,
	status *big.Int,
	l2TxHash *big.Int,
	cumulativeGasUsed *big.Int,
	effectivePercentage *big.Int) (*big.Int, error) {

	_, err := setL2TxHash(smt, txIndex, l2TxHash)
	if err != nil {
		return nil, err
	}
	_, err = setTxStatus(smt, txIndex, status)
	if err != nil {
		return nil, err
	}
	_, err = setCumulativeGasUsed(smt, txIndex, cumulativeGasUsed)
	if err != nil {
		return nil, err
	}

	// now encode the logs
	for _, log := range logs {

		reducedTopics := ""
		for _, topic := range log.Topics {
			reducedTopics += fmt.Sprintf("%x", topic)
		}

		logToEncode := fmt.Sprintf("0x%x%s", log.Data, reducedTopics)

		hash, err := utils.HashContractBytecode(logToEncode)
		if err != nil {
			return nil, err
		}

		logEncodedBig := utils.ConvertHexToBigInt(hash)
		_, err = setTxLog(smt, txIndex, logIndex, logEncodedBig)
		if err != nil {
			return nil, err
		}

		// increment log index
		logIndex.Add(logIndex, big.NewInt(1))
	}

	_, err = setTxEffectivePercentage(smt, txIndex, effectivePercentage)
	if err != nil {
		return nil, err
	}

	return smt.LastRoot(), nil
}

func setL2TxHash(smt *smt.SMT, txIndex *big.Int, l2TxHash *big.Int) (*big.Int, error) {
	key, err := KeyTxHash(txIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, l2TxHash)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setTxStatus(smt *smt.SMT, txIndex *big.Int, status *big.Int) (*big.Int, error) {
	key, err := KeyTxStatus(txIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, status)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setCumulativeGasUsed(smt *smt.SMT, txIndex, cumulativeGasUsed *big.Int) (*big.Int, error) {
	key, err := KeyCumulativeGasUsed(txIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, cumulativeGasUsed)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setTxEffectivePercentage(smt *smt.SMT, txIndex, effectivePercentage *big.Int) (*big.Int, error) {
	key, err := KeyEffectivePercentage(txIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, effectivePercentage)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setTxLog(smt *smt.SMT, txIndex *big.Int, logIndex *big.Int, log *big.Int) (*big.Int, error) {
	key, err := KeyTxLogs(txIndex, logIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, log)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setL2BlockHash(smt *smt.SMT, blockHash *libcommon.Hash) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamBlockHash))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, blockHash.Big())
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setCoinbase(smt *smt.SMT, coinbase *libcommon.Address) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamCoinbase))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, coinbase.Hash().Big())
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setGasLimit(smt *smt.SMT, gasLimit uint64) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamGasLimit))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, big.NewInt(int64(gasLimit)))
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setBlockNumber(smt *smt.SMT, blockNumber uint64) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamNumber))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, big.NewInt(int64(blockNumber)))
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setTimestamp(smt *smt.SMT, timestamp uint64) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamTimestamp))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, big.NewInt(int64(timestamp)))
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setGer(smt *smt.SMT, ger *libcommon.Hash) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamGer))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, ger.Big())
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setL1BlockHash(smt *smt.SMT, blockHash *libcommon.Hash) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamBlockHashL1))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, blockHash.Big())
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}
