package stages

import (
	"context"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	libstate "github.com/gateway-fm/cdk-erigon-lib/state"

	"math/big"

	"fmt"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	smtNs "github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	verifier "github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/tx"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/txpool"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"
)

const (
	logInterval = 20 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000

	transactionGasLimit = 30000000

	yieldSize = 100 // arbitrary number defining how many transactions to yield from the pool at once
)

var (
	noop            = state.NewNoopWriter()
	blockDifficulty = new(big.Int).SetUint64(0)
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type SequenceBlockCfg struct {
	db            kv.RwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	changeSetHook stagedsync.ChangeSetHook
	chainConfig   *chain.Config
	engine        consensus.Engine
	zkVmConfig    *vm.ZkConfig
	badBlockHalt  bool
	stateStream   bool
	accumulator   *shards.Accumulator
	blockReader   services.FullBlockReader

	dirs             datadir.Dirs
	historyV3        bool
	syncCfg          ethconfig.Sync
	genesis          *types.Genesis
	agg              *libstate.AggregatorV3
	stream           *datastreamer.StreamServer
	datastreamServer *server.DataStreamServer
	zk               *ethconfig.Zk

	txPool   *txpool.TxPool
	txPoolDb kv.RwDB

	legacyVerifier *verifier.LegacyExecutorVerifier
}

func StageSequenceBlocksCfg(
	db kv.RwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	changeSetHook stagedsync.ChangeSetHook,
	chainConfig *chain.Config,
	engine consensus.Engine,
	vmConfig *vm.ZkConfig,
	accumulator *shards.Accumulator,
	stateStream bool,
	badBlockHalt bool,

	historyV3 bool,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	agg *libstate.AggregatorV3,
	stream *datastreamer.StreamServer,
	zk *ethconfig.Zk,

	txPool *txpool.TxPool,
	txPoolDb kv.RwDB,
	legacyVerifier *verifier.LegacyExecutorVerifier,
) SequenceBlockCfg {

	return SequenceBlockCfg{
		db:               db,
		prune:            pm,
		batchSize:        batchSize,
		changeSetHook:    changeSetHook,
		chainConfig:      chainConfig,
		engine:           engine,
		zkVmConfig:       vmConfig,
		dirs:             dirs,
		accumulator:      accumulator,
		stateStream:      stateStream,
		badBlockHalt:     badBlockHalt,
		blockReader:      blockReader,
		genesis:          genesis,
		historyV3:        historyV3,
		syncCfg:          syncCfg,
		agg:              agg,
		stream:           stream,
		datastreamServer: server.NewDataStreamServer(stream, chainConfig.ChainID.Uint64()),
		zk:               zk,
		txPool:           txPool,
		txPoolDb:         txPoolDb,
		legacyVerifier:   legacyVerifier,
	}
}

func (sCfg *SequenceBlockCfg) toErigonExecuteBlockCfg() stagedsync.ExecuteBlockCfg {
	return stagedsync.StageExecuteBlocksCfg(
		sCfg.db,
		sCfg.prune,
		sCfg.batchSize,
		sCfg.changeSetHook,
		sCfg.chainConfig,
		sCfg.engine,
		&sCfg.zkVmConfig.Config,
		sCfg.accumulator,
		sCfg.stateStream,
		sCfg.badBlockHalt,
		sCfg.historyV3,
		sCfg.dirs,
		sCfg.blockReader,
		headerdownload.NewHeaderDownload(1, 1, sCfg.engine, sCfg.blockReader),
		sCfg.genesis,
		sCfg.syncCfg,
		sCfg.agg,
		sCfg.zk,
	)
}

type stageDb struct {
	tx          kv.RwTx
	hermezDb    *hermez_db.HermezDb
	eridb       *db2.EriDb
	stateReader *state.PlainStateReader
	smt         *smtNs.SMT
}

func newStageDb(tx kv.RwTx) *stageDb {
	sdb := &stageDb{}
	sdb.SetTx(tx)
	return sdb
}

func (sdb *stageDb) SetTx(tx kv.RwTx) {
	sdb.tx = tx
	sdb.hermezDb = hermez_db.NewHermezDb(tx)
	sdb.eridb = db2.NewEriDb(tx)
	sdb.stateReader = state.NewPlainStateReader(tx)
	sdb.smt = smtNs.NewSMT(sdb.eridb, false)
}

type nextBatchL1Data struct {
	DecodedData     []zktx.DecodedBatchL2Data
	Coinbase        common.Address
	L1InfoRoot      common.Hash
	IsWorkRemaining bool
	LimitTimestamp  uint64
}

type forkDb interface {
	GetAllForkHistory() ([]uint64, []uint64, error)
	GetLatestForkHistory() (uint64, uint64, error)
	GetForkId(batch uint64) (uint64, error)
	WriteForkIdBlockOnce(forkId, block uint64) error
	WriteForkId(batch, forkId uint64) error
}

func prepareForkId(lastBatch, executionAt uint64, hermezDb forkDb) (uint64, error) {
	var err error
	var latest uint64

	// get all history and find the fork appropriate for the batch we're processing now
	allForks, allBatches, err := hermezDb.GetAllForkHistory()
	if err != nil {
		return 0, err
	}

	nextBatch := lastBatch + 1

	// iterate over the batch boundaries and find the latest fork that applies
	for idx, batch := range allBatches {
		if nextBatch > batch {
			latest = allForks[idx]
		}
	}

	if latest == 0 {
		return 0, fmt.Errorf("could not find a suitable fork for batch %v, cannot start sequencer, check contract configuration", lastBatch+1)
	}

	// now we need to check the last batch to see if we need to update the fork id
	lastBatchFork, err := hermezDb.GetForkId(lastBatch)
	if err != nil {
		return 0, err
	}

	// write the fork height once for the next block at the point of fork upgrade
	if lastBatchFork < latest {
		log.Info("Upgrading fork id", "from", lastBatchFork, "to", latest, "batch", nextBatch)
		if err := hermezDb.WriteForkIdBlockOnce(latest, executionAt+1); err != nil {
			return latest, err
		}
	}

	return latest, nil
}

func prepareHeader(tx kv.RwTx, previousBlockNumber, deltaTimestamp, forcedTimestamp, forkId uint64, coinbase common.Address) (*types.Header, *types.Block, error) {
	parentBlock, err := rawdb.ReadBlockByNumber(tx, previousBlockNumber)
	if err != nil {
		return nil, nil, err
	}

	var newBlockTimestamp uint64

	if forcedTimestamp != math.MaxUint64 {
		newBlockTimestamp = forcedTimestamp
	} else {
		// in the case of normal execution when not in l1 recovery
		// we want to generate the timestamp based on the current time.  When in recovery
		// we will pass a real delta which we then need to apply to the previous block timestamp
		useTimestampOffsetFromParentBlock := deltaTimestamp != math.MaxUint64
		newBlockTimestamp = uint64(time.Now().Unix())
		if useTimestampOffsetFromParentBlock {
			newBlockTimestamp = parentBlock.Time() + deltaTimestamp
		}
	}

	return &types.Header{
		ParentHash: parentBlock.Hash(),
		Coinbase:   coinbase,
		Difficulty: blockDifficulty,
		Number:     new(big.Int).SetUint64(previousBlockNumber + 1),
		GasLimit:   utils.GetBlockGasLimitForFork(forkId),
		Time:       newBlockTimestamp,
	}, parentBlock, nil
}

func prepareL1AndInfoTreeRelatedStuff(sdb *stageDb, decodedBlock *zktx.DecodedBatchL2Data, l1Recovery bool, proposedTimestamp uint64) (
	infoTreeIndexProgress uint64,
	l1TreeUpdate *zktypes.L1InfoTreeUpdate,
	l1TreeUpdateIndex uint64,
	l1BlockHash common.Hash,
	ger common.Hash,
	shouldWriteGerToContract bool,
	err error,
) {
	// if we are in a recovery state and recognise that a l1 info tree index has been reused
	// then we need to not include the GER and L1 block hash into the block info root calculation, so
	// we keep track of this here
	shouldWriteGerToContract = true

	if infoTreeIndexProgress, err = stages.GetStageProgress(sdb.tx, stages.HighestUsedL1InfoIndex); err != nil {
		return
	}

	if l1Recovery {
		l1TreeUpdateIndex = uint64(decodedBlock.L1InfoTreeIndex)
		if l1TreeUpdate, err = sdb.hermezDb.GetL1InfoTreeUpdate(l1TreeUpdateIndex); err != nil {
			return
		}
		if infoTreeIndexProgress >= l1TreeUpdateIndex {
			shouldWriteGerToContract = false
		}
	} else {
		if l1TreeUpdateIndex, l1TreeUpdate, err = calculateNextL1TreeUpdateToUse(infoTreeIndexProgress, sdb.hermezDb, proposedTimestamp); err != nil {
			return
		}
		if l1TreeUpdateIndex > 0 {
			infoTreeIndexProgress = l1TreeUpdateIndex
		}
	}

	// we only want GER and l1 block hash for indexes above 0 - 0 is a special case
	if l1TreeUpdate != nil && l1TreeUpdateIndex > 0 {
		l1BlockHash = l1TreeUpdate.ParentHash
		ger = l1TreeUpdate.GER
	}

	return
}

// will be called at the start of every new block created within a batch to figure out if there is a new GER
// we can use or not.  In the special case that this is the first block we just return 0 as we need to use the
// 0 index first before we can use 1+
func calculateNextL1TreeUpdateToUse(lastInfoIndex uint64, hermezDb *hermez_db.HermezDb, proposedTimestamp uint64) (uint64, *zktypes.L1InfoTreeUpdate, error) {
	// always default to 0 and only update this if the next available index has reached finality
	var nextL1Index uint64 = 0

	// check if the next index is there and if it has reached finality or not
	l1Info, err := hermezDb.GetL1InfoTreeUpdate(lastInfoIndex + 1)
	if err != nil {
		return 0, nil, err
	}

	// ensure that we are above the min timestamp for this index to use it
	if l1Info != nil && l1Info.Timestamp <= proposedTimestamp {
		nextL1Index = l1Info.Index
	}

	return nextL1Index, l1Info, nil
}

func updateSequencerProgress(tx kv.RwTx, newHeight uint64, newBatch uint64, l1InfoIndex uint64) error {
	// now update stages that will be used later on in stageloop.go and other stages. As we're the sequencer
	// we won't have headers stage for example as we're already writing them here
	if err := stages.SaveStageProgress(tx, stages.Execution, newHeight); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, newHeight); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, newBatch); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.HighestUsedL1InfoIndex, l1InfoIndex); err != nil {
		return err
	}

	return nil
}

func doFinishBlockAndUpdateState(
	ctx context.Context,
	cfg SequenceBlockCfg,
	s *stagedsync.StageState,
	sdb *stageDb,
	ibs *state.IntraBlockState,
	header *types.Header,
	parentBlock *types.Block,
	forkId uint64,
	thisBatch uint64,
	ger common.Hash,
	l1BlockHash common.Hash,
	transactions []types.Transaction,
	receipts types.Receipts,
	execResults []*core.ExecutionResult,
	effectiveGases []uint8,
	l1InfoIndex uint64,
	l1Recovery bool,
) (*types.Block, error) {
	thisBlockNumber := header.Number.Uint64()

	if cfg.accumulator != nil {
		cfg.accumulator.StartChange(thisBlockNumber, header.Hash(), nil, false)
	}

	block, err := finaliseBlock(ctx, cfg, s, sdb, ibs, header, parentBlock, forkId, thisBatch, cfg.accumulator, ger, l1BlockHash, transactions, receipts, execResults, effectiveGases, l1Recovery)
	if err != nil {
		return nil, err
	}

	if err := updateSequencerProgress(sdb.tx, thisBlockNumber, thisBatch, l1InfoIndex); err != nil {
		return nil, err
	}

	if cfg.accumulator != nil {
		txs, err := rawdb.RawTransactionsRange(sdb.tx, thisBlockNumber, thisBlockNumber)
		if err != nil {
			return nil, err
		}
		cfg.accumulator.ChangeTransactions(txs)
	}

	return block, nil
}

type batchChecker interface {
	GetL1InfoTreeUpdate(idx uint64) (*zktypes.L1InfoTreeUpdate, error)
}

func checkForBadBatch(
	batchNo uint64,
	hermezDb batchChecker,
	latestTimestamp uint64,
	highestAllowedInfoTreeIndex uint64,
	limitTimestamp uint64,
	decodedBlocks []zktx.DecodedBatchL2Data,
) (bool, error) {
	timestamp := latestTimestamp

	for _, decodedBlock := range decodedBlocks {
		timestamp += uint64(decodedBlock.DeltaTimestamp)

		// now check the limit timestamp we can't have used l1 info tree index from the future
		if timestamp > limitTimestamp {
			log.Error("batch went above the limit timestamp", "batch", batchNo, "timestamp", timestamp, "limit_timestamp", limitTimestamp)
			return true, nil
		}

		if decodedBlock.L1InfoTreeIndex > 0 {
			// first check if we have knowledge of this index or not
			l1Info, err := hermezDb.GetL1InfoTreeUpdate(uint64(decodedBlock.L1InfoTreeIndex))
			if err != nil {
				return false, err
			}
			if l1Info == nil {
				// can't use an index that doesn't exist, so we have a bad batch
				log.Error("batch used info tree index that doesn't exist", "batch", batchNo, "index", decodedBlock.L1InfoTreeIndex)
				return true, nil
			}

			// we have an invalid batch if the block timestamp is lower than the l1 info min timestamp value
			if timestamp < l1Info.Timestamp {
				log.Error("batch used info tree index with timestamp lower than allowed", "batch", batchNo, "index", decodedBlock.L1InfoTreeIndex, "timestamp", timestamp, "min_timestamp", l1Info.Timestamp)
				return true, nil
			}

			// now finally check that the index used is lower or equal to the highest allowed index
			if uint64(decodedBlock.L1InfoTreeIndex) > highestAllowedInfoTreeIndex {
				log.Error("batch used info tree index higher than the current info tree root allows", "batch", batchNo, "index", decodedBlock.L1InfoTreeIndex, "highest_allowed", highestAllowedInfoTreeIndex)
				return true, nil
			}
		}
	}

	return false, nil
}

// hard coded to match in with the smart contract
// https://github.com/0xPolygonHermez/zkevm-contracts/blob/73758334f8568b74e9493fcc530b442bd73325dc/contracts/PolygonZkEVM.sol#L119C63-L119C69
const LIMIT_120_KB = 120_000

type BlockDataChecker struct {
	limit   uint64 // limit amount of bytes
	counter uint64 // counter amount of bytes
}

func NewBlockDataChecker() *BlockDataChecker {
	return &BlockDataChecker{
		limit:   LIMIT_120_KB,
		counter: 0,
	}
}

// adds bytes amounting to the block data and checks if the limit is reached
// if the limit is reached, the data is not added, so this can be reused again for next check
func (bdc *BlockDataChecker) AddBlockStartData(deltaTimestamp, l1InfoTreeIndex uint32) bool {
	blockStartBytesAmount := tx.START_BLOCK_BATCH_L2_DATA_SIZE // tx.GenerateStartBlockBatchL2Data(deltaTimestamp, l1InfoTreeIndex) returns 65 long byte array
	// add in the changeL2Block transaction
	if bdc.counter+blockStartBytesAmount > bdc.limit {
		return true
	}

	bdc.counter += blockStartBytesAmount

	return false
}

func (bdc *BlockDataChecker) AddTransactionData(txL2Data []byte) bool {
	encodedLen := uint64(len(txL2Data))
	if bdc.counter+uint64(encodedLen) > bdc.limit {
		return true
	}

	bdc.counter += encodedLen

	return false
}

func updateStreamAndCheckRollback(
	logPrefix string,
	sdb *stageDb,
	streamWriter *SequencerBatchStreamWriter,
	batchNumber uint64,
	forkId uint64,
	u stagedsync.Unwinder,
) (bool, int, error) {
	committed, remaining, err := streamWriter.CommitNewUpdates(forkId)
	if err != nil {
		return false, remaining, err
	}
	for _, commit := range committed {
		if !commit.Valid {
			// we are about to unwind so place the marker ready for this to happen
			if err = sdb.hermezDb.WriteJustUnwound(batchNumber); err != nil {
				return false, 0, err
			}
			// capture the fork otherwise when the loop starts again to close
			// off the batch it will detect it as a fork upgrade
			if err = sdb.hermezDb.WriteForkId(batchNumber, forkId); err != nil {
				return false, 0, err
			}

			unwindTo := commit.BlockNumber - 1

			// for unwind we supply the block number X-1 of the block we want to remove, but supply the hash of the block
			// causing the unwind.
			unwindHeader := rawdb.ReadHeaderByNumber(sdb.tx, commit.BlockNumber)
			if unwindHeader == nil {
				return false, 0, fmt.Errorf("could not find header for block %d", commit.BlockNumber)
			}

			if err = sdb.tx.Commit(); err != nil {
				return false, 0, err
			}

			log.Warn(fmt.Sprintf("[%s] Block is invalid - rolling back", logPrefix), "badBlock", commit.BlockNumber, "unwindTo", unwindTo, "root", unwindHeader.Root)

			u.UnwindTo(unwindTo, unwindHeader.Hash())
			return true, 0, nil
		}
	}

	return false, remaining, nil
}

func runBatchLastSteps(
	logPrefix string,
	datastreamServer *server.DataStreamServer,
	sdb *stageDb,
	thisBatch uint64,
	lastStartedBn uint64,
	batchCounters *vm.BatchCounterCollector,
) error {
	l1InfoIndex, err := sdb.hermezDb.GetBlockL1InfoTreeIndex(lastStartedBn)
	if err != nil {
		return err
	}

	counters, err := batchCounters.CombineCollectors(l1InfoIndex != 0)
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] counters consumed", logPrefix), "batch", thisBatch, "counts", counters.UsedAsString())

	if err = sdb.hermezDb.WriteBatchCounters(thisBatch, counters.UsedAsMap()); err != nil {
		return err
	}
	if err := sdb.hermezDb.DeleteIsBatchPartiallyProcessed(thisBatch); err != nil {
		return err
	}

	// Local Exit Root (ler): read s/c storage every batch to store the LER for the highest block in the batch
	ler, err := utils.GetBatchLocalExitRootFromSCStorage(thisBatch, sdb.hermezDb.HermezDbReader, sdb.tx)
	if err != nil {
		return err
	}
	// write ler to hermezdb
	if err = sdb.hermezDb.WriteLocalExitRootForBatchNo(thisBatch, ler); err != nil {
		return err
	}

	lastBlock, err := sdb.hermezDb.GetHighestBlockInBatch(thisBatch)
	if err != nil {
		return err
	}
	block, err := rawdb.ReadBlockByNumber(sdb.tx, lastBlock)
	if err != nil {
		return err
	}
	blockRoot := block.Root()
	if err = datastreamServer.WriteBatchEnd(sdb.hermezDb, thisBatch, thisBatch, &blockRoot, &ler); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Finish batch %d...", logPrefix, thisBatch))

	return nil
}
