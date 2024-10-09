package utilities_mode

import (
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/witness"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"context"
	"github.com/ledgerwatch/erigon/common/math"
	"os"
	"github.com/ledgerwatch/log/v3"
)

const (
	ModeSequencerExecutorVerifier = "SequencerExecutorVerifier"
)

type SequencerExecutorVerifier struct {
	tx               kv.Tx
	ctx              context.Context
	witnessGenerator *witness.Generator
	verifier         *legacy_executor_verifier.LegacyExecutorVerifier

	// options
	batchNumber uint64
	witnessFull bool
}

func NewSequencerExecutorVerifier(tx kv.Tx, ctx context.Context, witnessGenerator *witness.Generator, verifier *legacy_executor_verifier.LegacyExecutorVerifier) *SequencerExecutorVerifier {
	return &SequencerExecutorVerifier{
		tx:               tx,
		ctx:              ctx,
		witnessGenerator: witnessGenerator,
		verifier:         verifier,

		// options
		batchNumber: 1550,
		witnessFull: false,
	}
}

func (s *SequencerExecutorVerifier) SequencerExecutorVerify() {
	hdb := hermez_db.NewHermezDbReader(s.tx)
	startBlock, _, err := hdb.GetLowestBlockInBatch(s.batchNumber)
	if err != nil {
		log.Error("failed to get lowest block in batch", "err", err)
		os.Exit(1)
	}
	endBlock, _, err := hdb.GetHighestBlockInBatch(s.batchNumber)
	if err != nil {
		log.Error("failed to get highest block in batch", "err", err)
		os.Exit(1)
	}

	w, err := s.witnessGenerator.GetWitnessByBlockRange(s.tx, s.ctx, startBlock, endBlock, false, s.witnessFull)
	if err != nil {
		log.Error("failed to get witness", "err", err)
		os.Exit(1)
	}

	batchCounters := vm.NewBatchCounterCollector(256, 9, 0.6, true, nil)
	unlimitedCounters := batchCounters.NewCounters().UsedAsMap()
	for k := range unlimitedCounters {
		unlimitedCounters[k] = math.MaxInt32
	}

	blockNos := make([]uint64, 0)
	for i := startBlock; i <= endBlock; i++ {
		blockNos = append(blockNos, i)
	}

	bbn, err := rawdb.ReadBlockByNumber(s.tx, endBlock)
	if err != nil {
		log.Error("failed to read block by number", "err", err)
		os.Exit(1)
	}

	fid, err := hdb.GetForkId(s.batchNumber)
	if err != nil {
		log.Error("failed to get fork id", "err", err)
		os.Exit(1)
	}

	request := &legacy_executor_verifier.VerifierRequest{
		BatchNumber:  s.batchNumber,
		BlockNumbers: blockNos,
		ForkId:       fid,
		StateRoot:    bbn.Root(),
		Counters:     unlimitedCounters,
	}

	l1infotreemints := map[uint64]uint64{}

	batchBytes, err := s.verifier.GetWholeBatchStreamBytes(s.batchNumber, s.tx, blockNos, hdb, l1infotreemints, nil)
	if err != nil {
		log.Error("failed to get whole batch stream bytes", "err", err)
		os.Exit(1)
	}

	err = s.verifier.VerifySync(s.tx, request, w, batchBytes, bbn.Time(), l1infotreemints)
	if err != nil {
		log.Error("failed to verify sync", "err", err)
		os.Exit(1)
	}

	os.Exit(0)
}
