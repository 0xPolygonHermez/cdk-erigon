package legacy_executor_verifier

import (
	"context"
	"sync/atomic"
	"time"

	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	dstypes "github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/log/v3"
)

const (
	ROLLUP_ID = 1 // todo [zkevm] this should be read from config to anticipate more than 1 rollup per manager contract
)

type VerifierRequest struct {
	BatchNumber uint64
	ForkId      uint64
	StateRoot   common.Hash
	CheckCount  int
	Counters    map[string]int
}

type VerifierResponse struct {
	BatchNumber      uint64
	Valid            bool
	Witness          []byte
	ExecutorResponse *executor.ProcessBatchResponseV2
	Error            error
}

var ErrNoExecutorAvailable = fmt.Errorf("no executor available")

type ILegacyExecutor interface {
	Verify(*Payload, *VerifierRequest, common.Hash) (bool, *executor.ProcessBatchResponseV2, error)
	CheckOnline() bool
	QueueLength() int
	CancelAllVerifications()
	AllowAllVerifications()
}

type WitnessGenerator interface {
	GenerateWitness(tx kv.Tx, ctx context.Context, startBlock, endBlock uint64, debug, witnessFull bool) ([]byte, error)
}

type LegacyExecutorVerifier struct {
	db             kv.RwDB
	cfg            ethconfig.Zk
	executors      []ILegacyExecutor
	executorNumber int

	quit chan struct{}

	streamServer     *server.DataStreamServer
	stream           *datastreamer.StreamServer
	witnessGenerator WitnessGenerator
	l1Syncer         *syncer.L1Syncer

	promises     []*Promise[*VerifierResponse]
	addedBatches map[uint64]struct{}
}

func NewLegacyExecutorVerifier(
	cfg ethconfig.Zk,
	executors []ILegacyExecutor,
	chainCfg *chain.Config,
	db kv.RwDB,
	witnessGenerator WitnessGenerator,
	l1Syncer *syncer.L1Syncer,
	stream *datastreamer.StreamServer,
) *LegacyExecutorVerifier {
	streamServer := server.NewDataStreamServer(stream, chainCfg.ChainID.Uint64(), server.ExecutorOperationMode)
	return &LegacyExecutorVerifier{
		db:               db,
		cfg:              cfg,
		executors:        executors,
		executorNumber:   0,
		quit:             make(chan struct{}),
		streamServer:     streamServer,
		stream:           stream,
		witnessGenerator: witnessGenerator,
		l1Syncer:         l1Syncer,
		promises:         make([]*Promise[*VerifierResponse], 0),
		addedBatches:     make(map[uint64]struct{}),
	}
}

var counter = int32(0)

func (v *LegacyExecutorVerifier) AddRequestUnsafe(ctx context.Context, tx kv.RwTx, request *VerifierRequest) (*Promise[*VerifierResponse], error) {
	// if we have no executor config then just skip this step and treat everything as OK
	if !v.HasExecutors() {
		response := &VerifierResponse{
			BatchNumber: request.BatchNumber,
			Valid:       true,
		}
		return &Promise[*VerifierResponse]{
			result: response,
			err:    nil,
		}, nil
	}

	hermezDb := hermez_db.NewHermezDbReader(tx)

	// get the data stream bytes
	blocks, err := hermezDb.GetL2BlockNosByBatch(request.BatchNumber)
	if err != nil {
		return nil, err
	}

	// we might not have blocks yet as the underlying stage loop might still be running and the tx hasn't been
	// committed yet so just requeue the request
	if len(blocks) == 0 {
		request.CheckCount++
		return nil, nil
	}

	l1InfoTreeMinTimestamps := make(map[uint64]uint64)
	streamBytes, err := v.getStreamBytes(request, tx, blocks, hermezDb, l1InfoTreeMinTimestamps)
	if err != nil {
		return nil, err
	}

	witness, err := v.witnessGenerator.GenerateWitness(tx, ctx, blocks[0], blocks[len(blocks)-1], false, v.cfg.WitnessFull)
	if err != nil {
		return nil, err
	}

	log.Debug("witness generated", "data", hex.EncodeToString(witness))

	// executor is perfectly happy with just an empty hash here
	oldAccInputHash := common.HexToHash("0x0")

	// now we need to figure out the timestamp limit for this payload.  It must be:
	// timestampLimit >= currentTimestamp (from batch pre-state) + deltaTimestamp
	// so to ensure we have a good value we can take the timestamp of the last block in the batch
	// and just add 5 minutes
	lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[len(blocks)-1])
	if err != nil {
		return nil, err
	}

	timestampLimit := lastBlock.Time()
	payload := &Payload{
		Witness:                 witness,
		DataStream:              streamBytes,
		Coinbase:                v.cfg.AddressSequencer.String(),
		OldAccInputHash:         oldAccInputHash.Bytes(),
		L1InfoRoot:              nil,
		TimestampLimit:          timestampLimit,
		ForcedBlockhashL1:       []byte{0},
		ContextId:               strconv.Itoa(int(request.BatchNumber)),
		L1InfoTreeMinTimestamps: l1InfoTreeMinTimestamps,
	}

	previousBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0]-1)
	if err != nil {
		return nil, err
	}

	// eager promise will do the work as soon as called in a goroutine, then we can retrieve the result later
	promise := NewPromise[*VerifierResponse](func() (*VerifierResponse, error) {
		p := payload
		r := request
		blockCopy := previousBlock.Copy()

		e := v.getNextOnlineAvailableExecutor()
		if e == nil {
			return nil, ErrNoExecutorAvailable
		}

		ok, executorResponse, executorErr := e.Verify(p, r, blockCopy.Root())
		if executorErr != nil {
			if errors.Is(err, ErrExecutorStateRootMismatch) {
				log.Error("[Verifier] State root mismatch detected", "err", err)
			} else if errors.Is(err, ErrExecutorUnknownError) {
				log.Error("[Verifier] Unexpected error found from executor", "err", err)
			}
		}

		if request.BatchNumber == 3 && atomic.LoadInt32(&counter) == 0 {
			ok = false
			atomic.StoreInt32(&counter, 1)
		}

		response := &VerifierResponse{
			BatchNumber:      request.BatchNumber,
			Valid:            ok,
			Witness:          witness,
			ExecutorResponse: executorResponse,
			Error:            executorErr,
		}

		return response, nil
	})

	// add batch to the list of batches we've added
	v.addedBatches[request.BatchNumber] = struct{}{}

	// add the promise to the list of promises
	v.promises = append(v.promises, promise)
	return promise, nil
}

func (v *LegacyExecutorVerifier) ConsumeResultsUnsafe(tx kv.RwTx) ([]*VerifierResponse, error) {
	hdb := hermez_db.NewHermezDbReader(tx)

	results := make([]*VerifierResponse, 0, len(v.promises))
	for _, promise := range v.promises {
		result, err := promise.GetNonBlocking()
		if result == nil && err == nil {
			break
		}
		if err != nil {
			log.Error("error getting verifier result", "err", err)
		}
		if result != nil {
			if err = v.writeBatchToStream(result, hdb, tx); err != nil {
				log.Error("error getting verifier result", "err", err)
			}
		}

		results = append(results, result)
		delete(v.addedBatches, result.BatchNumber)
	}

	// leave only non-processed promises
	v.promises = v.promises[len(results):]

	return results, nil
}

func (v *LegacyExecutorVerifier) CancelAllRequestsUnsafe() {
	for _, e := range v.executors {
		e.CancelAllVerifications()
	}

	for _, e := range v.executors {
		// lets wait for all threads that are waiting to add to v.openRequests to finish
		for e.QueueLength() > 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}

	for _, e := range v.executors {
		e.AllowAllVerifications()
	}

	v.promises = make([]*Promise[*VerifierResponse], 0)
}

func (v *LegacyExecutorVerifier) HasExecutors() bool {
	return len(v.executors) > 0
}

func (v *LegacyExecutorVerifier) IsRequestAddedUnsafe(batch uint64) bool {
	_, ok := v.addedBatches[batch]
	return ok
}

func (v *LegacyExecutorVerifier) writeBatchToStream(result *VerifierResponse, hdb *hermez_db.HermezDbReader, roTx kv.Tx) error {
	blks, err := hdb.GetL2BlockNosByBatch(result.BatchNumber)
	if err != nil {
		return err
	}

	if err := server.WriteBlocksToStream(roTx, hdb, v.streamServer, v.stream, blks[0], blks[len(blks)-1], "verifier"); err != nil {
		return err
	}
	return nil
}

func (v *LegacyExecutorVerifier) getNextOnlineAvailableExecutor() ILegacyExecutor {
	var exec ILegacyExecutor

	// TODO: find executors with spare capacity

	// attempt to find an executor that is online amongst them all
	for i := 0; i < len(v.executors); i++ {
		v.executorNumber++
		if v.executorNumber >= len(v.executors) {
			v.executorNumber = 0
		}
		temp := v.executors[v.executorNumber]
		if temp.CheckOnline() {
			exec = temp
			break
		}
	}

	return exec
}

func (v *LegacyExecutorVerifier) getStreamBytes(request *VerifierRequest, tx kv.Tx, blocks []uint64, hermezDb *hermez_db.HermezDbReader, l1InfoTreeMinTimestamps map[uint64]uint64) ([]byte, error) {
	lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0]-1)
	if err != nil {
		return nil, err
	}
	var streamBytes []byte

	// as we only ever use the executor verifier for whole batches we can safely assume that the previous batch
	// will always be the request batch - 1 and that the first block in the batch will be at the batch
	// boundary so we will always add in the batch bookmark to the stream
	previousBatch := request.BatchNumber - 1

	for _, blockNumber := range blocks {
		block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
		if err != nil {
			return nil, err
		}

		//TODO: get ger updates between blocks
		gerUpdates := []dstypes.GerUpdate{}

		sBytes, err := v.streamServer.CreateAndBuildStreamEntryBytes(block, hermezDb, lastBlock, request.BatchNumber, previousBatch, true, &gerUpdates, l1InfoTreeMinTimestamps)
		if err != nil {
			return nil, err
		}
		streamBytes = append(streamBytes, sBytes...)
		lastBlock = block
		// we only put in the batch bookmark at the start of the stream data once
		previousBatch = request.BatchNumber
	}

	return streamBytes, nil
}
