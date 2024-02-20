package stages

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"sort"
)

type SequencerExecutorVerifyCfg struct {
	db       kv.RwDB
	verifier *legacy_executor_verifier.LegacyExecutorVerifier
	txPool   *txpool.TxPool
}

func StageSequencerExecutorVerifyCfg(
	db kv.RwDB,
	verifier *legacy_executor_verifier.LegacyExecutorVerifier,
) SequencerExecutorVerifyCfg {
	return SequencerExecutorVerifyCfg{
		db:       db,
		verifier: verifier,
	}
}

func SpawnSequencerExecutorVerifyStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerExecutorVerifyCfg,
	initialCycle bool,
	quiet bool,
) error {
	var err error
	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hermezDb := hermez_db.NewHermezDb(tx)

	// progress here is at the batch level
	progress, err := stages.GetStageProgress(tx, stages.SequenceExecutorVerify)
	if err != nil {
		return err
	}

	// get the latest responses from the verifier then sort them, so we can make sure we're handling verifications
	// in order
	responses := cfg.verifier.GetAllResponses()

	// sort responses by batch number in ascending order
	sort.Slice(responses, func(i, j int) bool {
		return responses[i].BatchNumber < responses[j].BatchNumber
	})

	for _, response := range responses {
		// ensure that the first response is the next batch based on the current stage progress
		// otherwise just return early until we get it
		if response.BatchNumber != progress+1 {
			return nil
		}

		// now check that we are indeed in a good state to continue
		if !response.Valid {
			// now we need to rollback and handle the error
			// todo [zkevm]!
		}

		// all good so just update the stage progress for now
		if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, response.BatchNumber); err != nil {
			return err
		}

		// now let the verifier know we have got this message, so it can release it
		cfg.verifier.RemoveResponse(response.BatchNumber)
		progress = response.BatchNumber
	}

	// progress here is at the block level
	intersProgress, err := stages.GetStageProgress(tx, stages.IntermediateHashes)
	if err != nil {
		return err
	}

	// we need to get the batch number for the latest block, so we can search for new batches to send for
	// verification
	intersBatch, err := hermezDb.GetBatchNoByL2Block(intersProgress)
	if err != nil {
		return err
	}

	// send off the new batches to the verifier to be processed
	for batch := progress + 1; batch <= intersBatch; batch++ {
		cfg.verifier.AddRequest(legacy_executor_verifier.VerifierRequest{BatchNumber: batch})
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindSequencerExecutorVerifyStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerExecutorVerifyCfg,
	initialCycle bool,
) error {
	return nil
}

func PruneSequencerExecutorVerifyStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg SequencerExecutorVerifyCfg,
	ctx context.Context,
	initialCycle bool,
) error {
	return nil
}
