package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
)

func SpawnSequencingStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	toBlock uint64,
	ctx context.Context,
	cfg SequenceBlockCfg,
	initialCycle bool,
	quiet bool,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting sequencing stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished sequencing stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	sdb := newStageDb(tx)

	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	lastBatch, err := stages.GetStageProgress(tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	forkId, err := prepareForkId(cfg, lastBatch, executionAt, sdb.hermezDb)
	if err != nil {
		return err
	}

	if err := utils.UpdateZkEVMBlockCfg(cfg.chainConfig, sdb.hermezDb, logPrefix); err != nil {
		return err
	}

	// injected batch
	if executionAt == 0 {
		header, parentBlock, err := prepareHeader(tx, executionAt, math.MaxUint64, forkId, cfg.zk.AddressSequencer)
		if err != nil {
			return err
		}

		err = processInjectedInitialBatch(ctx, cfg, s, sdb, forkId, header, parentBlock)
		if err != nil {
			return err
		}

		if freshTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}

		return nil
	}

	var header *types.Header
	var parentBlock *types.Block

	var addedTransactions []types.Transaction
	var addedReceipts []*types.Receipt
	var clonedBatchCounters *vm.BatchCounterCollector

	var decodedBlock zktx.DecodedBatchL2Data
	var deltaTimestamp uint64 = math.MaxUint64
	var decodedBlocks []zktx.DecodedBatchL2Data // only used in l1 recovery
	var blockTransactions []types.Transaction
	var effectiveGases []uint8

	batchTicker := time.NewTicker(cfg.zk.SequencerBatchSealTime)
	defer batchTicker.Stop()
	thisBatch := lastBatch + 1
	batchCounters := vm.NewBatchCounterCollector(sdb.smt.GetDepth(), uint16(forkId))
	runLoopBlocks := true
	lastStartedBn := executionAt - 1
	yielded := mapset.NewSet[[32]byte]()
	coinbase := cfg.zk.AddressSequencer
	l1Recovery := cfg.zk.L1SyncStartBlock > 0
	workRemaining := true
	blockTracking := 0

	if l1Recovery {
		decodedBlocks, coinbase, workRemaining, err = getNextL1BatchData(thisBatch, forkId, sdb.hermezDb)
		if err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] Starting batch %d...", logPrefix, thisBatch))

	for bn := executionAt; runLoopBlocks; bn++ {
		if l1Recovery {
			// have we exhausted the blocks we have to recover?
			if blockTracking >= len(decodedBlocks) {
				runLoopBlocks = false
				break
			}

			decodedBlock = decodedBlocks[blockTracking]
			deltaTimestamp = uint64(decodedBlock.DeltaTimestamp)
			effectiveGases = decodedBlock.EffectiveGasPricePercentages
			blockTransactions = decodedBlock.Transactions
		}

		log.Info(fmt.Sprintf("[%s] Starting block %d...", logPrefix, bn+1))

		reRunBlockAfterOverflow := bn == lastStartedBn
		lastStartedBn = bn

		if !reRunBlockAfterOverflow {
			clonedBatchCounters = batchCounters.Clone()
			addedTransactions = []types.Transaction{}
			addedReceipts = []*types.Receipt{}
			header, parentBlock, err = prepareHeader(tx, bn, deltaTimestamp, forkId, coinbase)
			if err != nil {
				return err
			}
		} else {
			batchCounters = clonedBatchCounters

			// create a copy of the header otherwise the executor will return "state root mismatch error"
			header = &types.Header{
				ParentHash: header.ParentHash,
				Coinbase:   header.Coinbase,
				Difficulty: header.Difficulty,
				Number:     header.Number,
				GasLimit:   header.GasLimit,
				Time:       header.Time,
			}
		}

		thisBlockNumber := header.Number.Uint64()

		infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, err := prepareL1AndInfoTreeRelatedStuff(sdb, &decodedBlock, l1Recovery)
		if err != nil {
			return err
		}

		batchCounters.StartNewBlock()

		ibs := state.New(sdb.stateReader)

		parentRoot := parentBlock.Root()
		if err = handleStateForNewBlockStarting(cfg.chainConfig, sdb.hermezDb, ibs, thisBlockNumber, header.Time, &parentRoot, l1TreeUpdate, shouldWriteGerToContract); err != nil {
			return err
		}

		if !reRunBlockAfterOverflow {
			// start waiting for a new transaction to arrive
			log.Info(fmt.Sprintf("[%s] Waiting for txs from the pool...", logPrefix))

			// we don't care about defer order here we just need to make sure the tickers are stopped to
			// avoid a leak
			logTicker := time.NewTicker(10 * time.Second)
			defer logTicker.Stop()
			blockTicker := time.NewTicker(cfg.zk.SequencerBlockSealTime)
			defer blockTicker.Stop()
			overflow := false

			// start to wait for transactions to come in from the pool and attempt to add them to the current batch.  Once we detect a counter
			// overflow we revert the IBS back to the previous snapshot and don't add the transaction/receipt to the collection that will
			// end up in the finalised block
		LOOP_TRANSACTIONS:
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-logTicker.C:
					log.Info(fmt.Sprintf("[%s] Waiting some more for txs from the pool...", logPrefix))
				case <-blockTicker.C:
					break LOOP_TRANSACTIONS
				case <-batchTicker.C:
					runLoopBlocks = false
					break LOOP_TRANSACTIONS
				default:
					if !l1Recovery {
						cfg.txPool.LockFlusher()
						blockTransactions, err = getNextPoolTransactions(cfg, executionAt, forkId, yielded)
						if err != nil {
							return err
						}
						cfg.txPool.UnlockFlusher()
					}

					for i, transaction := range blockTransactions {
						var receipt *types.Receipt
						var effectiveGas uint8

						if l1Recovery {
							effectiveGas = effectiveGases[i]
						} else {
							effectiveGas = DeriveEffectiveGasPrice(cfg, transaction)
						}

						receipt, overflow, err = attemptAddTransaction(cfg, sdb, ibs, batchCounters, header, parentBlock.Header(), transaction, effectiveGas, l1Recovery)
						if err != nil {
							return err
						}
						if !l1Recovery && overflow {
							log.Info(fmt.Sprintf("[%s] overflowed adding transaction to batch", logPrefix), "batch", thisBatch, "tx-hash", transaction.Hash(), "txs before overflow", len(addedTransactions))
							if len(addedTransactions) == 0 {
								return fmt.Errorf("single transaction %s overflow counters", transaction.Hash())
							}

							// remove from yielded so they can be processed again
							txSize := len(blockTransactions)
							for ; i < txSize; i++ {
								yielded.Remove(transaction.Hash())
							}

							break LOOP_TRANSACTIONS
						}

						addedTransactions = append(addedTransactions, transaction)
						addedReceipts = append(addedReceipts, receipt)
					}

					if l1Recovery {
						// just go into the normal loop waiting for new transactions to signal that the recovery
						// has finished as far as it can go
						if len(blockTransactions) == 0 && !workRemaining {
							log.Info(fmt.Sprintf("[%s] L1 recovery no more transactions to recover", logPrefix))
							continue
						}

						// if we had transactions then break the loop because we don't need to wait for more
						// because we got them from the DB
						runLoopBlocks = false
						break LOOP_TRANSACTIONS
					}
				}
			}
			if !l1Recovery && overflow {
				bn--     // in order to trigger reRunBlockAfterOverflow check
				continue // lets execute the same block again
			}
		} else {
			for idx, transaction := range addedTransactions {
				effectiveGas := DeriveEffectiveGasPrice(cfg, transaction)
				receipt, innerOverflow, err := attemptAddTransaction(cfg, sdb, ibs, batchCounters, header, parentBlock.Header(), transaction, effectiveGas, false)
				if err != nil {
					return err
				}
				if innerOverflow {
					// kill the node at this stage to prevent a batch being created that can't be proven
					panic(fmt.Sprintf("overflowed twice during execution while adding tx with index %d", idx))
				}
				addedReceipts[idx] = receipt
			}
			runLoopBlocks = false // close the batch because there are no counters left
		}

		if err = sdb.hermezDb.WriteBlockL1InfoTreeIndex(thisBlockNumber, l1TreeUpdateIndex); err != nil {
			return err
		}

		if err = doFinishBlockAndUpdateState(ctx, cfg, s, sdb, ibs, header, parentBlock, forkId, thisBatch, ger, l1BlockHash, addedTransactions, addedReceipts, infoTreeIndexProgress); err != nil {
			return err
		}

		log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions...", logPrefix, thisBlockNumber, len(addedTransactions)))
		blockTracking++
	}

	counters, err := batchCounters.CombineCollectors()
	if err != nil {
		return err
	}

	log.Info("counters consumed", "counts", counters.UsedAsString())
	err = sdb.hermezDb.WriteBatchCounters(thisBatch, counters.UsedAsMap())
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Finish batch %d...", logPrefix, thisBatch))

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
