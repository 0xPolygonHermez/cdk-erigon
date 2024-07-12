package stages

import (
	verifier "github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"sync"
	"errors"
	"github.com/ledgerwatch/log/v3"
	"fmt"
)

type BatchVerifier struct {
	tx          kv.RwTx
	hasExecutor bool
	forkId      uint64
	mtxPromises *sync.Mutex
	promises    []*verifier.Promise[*verifier.VerifierBundle]
	stop        bool
	errors      chan error
}

func NewBatchVerifier(
	tx kv.RwTx,
	hasExecutors bool,
	forkId uint64,
) *BatchVerifier {
	return &BatchVerifier{
		tx:          tx,
		hasExecutor: hasExecutors,
		forkId:      forkId,
		mtxPromises: &sync.Mutex{},
		promises:    make([]*verifier.Promise[*verifier.VerifierBundle], 0),
		errors:      make(chan error),
	}
}

func (bv *BatchVerifier) AddNewCheck(batchNumber, blockNumber uint64, stateRoot common.Hash, counters map[string]int) {
	request := verifier.NewVerifierRequest(batchNumber, blockNumber, bv.forkId, stateRoot, counters)

	var promise *verifier.Promise[*verifier.VerifierBundle]
	if bv.hasExecutor {
		// todo: implement promise for this
	} else {
		promise = bv.nonExecutorPromise(request)
	}

	bv.appendPromise(promise)
}

func (bv *BatchVerifier) appendPromise(promise *verifier.Promise[*verifier.VerifierBundle]) {
	bv.mtxPromises.Lock()
	defer bv.mtxPromises.Unlock()
	bv.promises = append(bv.promises, promise)
}

func (bv *BatchVerifier) CheckProgress() ([]*verifier.VerifierResponse, error) {
	bv.mtxPromises.Lock()
	defer bv.mtxPromises.Unlock()

	var responsees []*verifier.VerifierResponse

	// not a stop signal, so we can start to process our promises now
	processed := 0
	for idx, promise := range bv.promises {
		bundle, err := promise.TryGet()
		if bundle == nil && err == nil {
			// nothing to process in this promise so we skip it
			break
		}

		if err != nil {
			// let leave it for debug purposes
			// a cancelled promise is removed from v.promises => it should never appear here, that's why let's panic if it happens, because it will indicate for massive error
			if errors.Is(err, verifier.ErrPromiseCancelled) {
				panic("this should never happen")
			}

			log.Error("error on our end while preparing the verification request, re-queueing the task", "err", err)

			if bundle.Request.IsOverdue() {
				// signal an error, the caller can check on this and stop the process if needs be
				return nil, fmt.Errorf("error: batch %d couldn't be processed in 30 minutes", bundle.Request.BatchNumber)
			}

			// re-queue the task - it should be safe to replace the index of the slice here as we only add to it
			// and this watcher process is the only process that removes from it
			bv.promises[idx] = verifier.NewPromise[*verifier.VerifierBundle](promise.Task())

			// break now as we know we can't proceed here until this promise is attempted again
			break
		}

		response := bundle.Response
		if response.Valid {
			processed++
			responsees = append(responsees, response)
		}
	}

	// remove processed promises from the list
	bv.removeProcessedPromises(processed)

	return responsees, nil
}

func (bv *BatchVerifier) removeProcessedPromises(processed int) {
	if processed == 0 {
		return
	}

	if processed == len(bv.promises) {
		bv.promises = make([]*verifier.Promise[*verifier.VerifierBundle], 0)
		return
	}

	bv.promises = bv.promises[processed:]
}

func (bv *BatchVerifier) nonExecutorPromise(request *verifier.VerifierRequest) *verifier.Promise[*verifier.VerifierBundle] {
	return verifier.NewPromiseSync[*verifier.VerifierBundle](func() (*verifier.VerifierBundle, error) {
		response := &verifier.VerifierResponse{
			BatchNumber:      request.BatchNumber,
			BlockNumber:      request.BlockNumber,
			Valid:            true,
			OriginalCounters: request.Counters,
			Witness:          nil,
			ExecutorResponse: nil,
			Error:            nil,
		}
		bundle := verifier.NewVerifierBundle(request, response)
		return bundle, nil
	})
}
