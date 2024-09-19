package blockinfo

import (
	"errors"
	"math/big"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

// SMT block header data leaf keys
const IndexBlockHeaderParamBlockHash = 0
const IndexBlockHeaderParamCoinbase = 1
const IndexBlockHeaderParamNumber = 2
const IndexBlockHeaderParamGasLimit = 3
const IndexBlockHeaderParamTimestamp = 4
const IndexBlockHeaderParamGer = 5
const IndexBlockHeaderParamBlockHashL1 = 6
const IndexBlockHeaderParamGasUsed = 7

// generated by KeyBlockHeaderParams so we don't calculate them every time
var (
	BlockHeaderBlockHashKey   = utils.NodeKey{17540094328570681229, 15492539097581145461, 7686481670809850401, 16577991319572125169}
	BlockHeaderCoinbaseKey    = utils.NodeKey{13866806033333411216, 11510953292839890698, 8274877395843603978, 9372332419316597113}
	BlockHeaderNumberKey      = utils.NodeKey{6024064788222257862, 13049342112699253445, 12127984136733687200, 8398043461199794462}
	BlockHeaderGasLimitKey    = utils.NodeKey{5319681466197319121, 14057433120745733551, 5638531288094714593, 17204828339478940337}
	BlockHeaderTimestampKey   = utils.NodeKey{7890158832167317866, 11032486557242372179, 9653801891436451408, 2062577087515942703}
	BlockHeaderGerKey         = utils.NodeKey{16031278424721309229, 4132999715765882778, 6388713709192801251, 10826219431775251904}
	BlockHeaderBlockHashL1Key = utils.NodeKey{5354929451503733866, 3129555839551084896, 2132809659008379950, 8230742270813566472}
	BlockHeaderGasUsedKey     = utils.NodeKey{8577769200631379655, 8682051454686970557, 5016656739138242322, 16717481432904730287}
)

// SMT block header constant keys
const IndexBlockHeaderParam = 7
const IndexBlockHeaderTransactionHash = 8
const IndexBlockHeaderStatus = 9
const IndexBlockHeaderCumulativeGasUsed = 10
const IndexBlockHeaderLogs = 11
const IndexBlockHeaderEffectivePercentage = 12

func KeyBlockHeaderParams(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderParam)
}

func KeyTxLogs(txIndex, logIndex *big.Int) (*utils.NodeKey, error) {
	if txIndex == nil || logIndex == nil {
		return nil, errors.New("nil key")
	}

	txIndexKey := utils.ScalarToArrayBig(txIndex)
	key1 := utils.NodeValue8{txIndexKey[0], txIndexKey[1], txIndexKey[2], txIndexKey[3], txIndexKey[4], txIndexKey[5], big.NewInt(int64(IndexBlockHeaderLogs)), big.NewInt(0)}

	logIndexArray := utils.ScalarToArrayBig(logIndex)
	lia, err := utils.NodeValue8FromBigIntArray(logIndexArray)
	if err != nil {
		return nil, err
	}

	hk0 := utils.Hash(lia.ToUintArray(), utils.BranchCapacity)
	hkRes := utils.Hash(key1.ToUintArray(), hk0)

	return &utils.NodeKey{hkRes[0], hkRes[1], hkRes[2], hkRes[3]}, nil
}

func KeyTxStatus(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderStatus)
}

func KeyCumulativeGasUsed(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderCumulativeGasUsed)
}

func KeyTxHash(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderTransactionHash)
}

func KeyEffectivePercentage(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderEffectivePercentage)
}
