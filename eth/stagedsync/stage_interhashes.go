package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/bits"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"

	state2 "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"

	"github.com/ledgerwatch/erigon/core/state/temporal"

	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/status-im/keycard-go/hexutils"
)

type TrieCfg struct {
	db                kv.RwDB
	checkRoot         bool
	badBlockHalt      bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader
	hd                *headerdownload.HeaderDownload

	historyV3 bool
	agg       *state.AggregatorV3
}

func StageTrieCfg(db kv.RwDB, checkRoot, saveNewHashesToDB, badBlockHalt bool, tmpDir string, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload, historyV3 bool, agg *state.AggregatorV3) TrieCfg {
	return TrieCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,

		historyV3: historyV3,
		agg:       agg,
	}
}

func SpawnIntermediateHashesStage(s *StageState, u Unwinder, tx kv.RwTx, cfg TrieCfg, ctx context.Context, quiet bool) (libcommon.Hash, error) {
	quit := ctx.Done()
	_ = quit

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return trie.EmptyRoot, err
		}
		defer tx.Rollback()
	}

	// max: we have to have executed in order to be able to do interhashes (see execution!)
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return trie.EmptyRoot, nil
	}

	var expectedRootHash libcommon.Hash
	var headerHash libcommon.Hash
	var syncHeadHeader *types.Header
	if cfg.checkRoot {
		syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, tx, to)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", to)
		}
		expectedRootHash = syncHeadHeader.Root
		headerHash = syncHeadHeader.Hash()
	}
	logPrefix := s.LogPrefix()
	if !quiet && to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	}

	var root libcommon.Hash
	shouldRegenerate := to > s.BlockNumber && to-s.BlockNumber > 10 // RetainList is in-memory structure and it will OOM if jump is too big, such big jump anyway invalidate most of existing Intermediate hashes
	if !shouldRegenerate && cfg.historyV3 && to-s.BlockNumber > 10 {
		//incremental can work only on DB data, not on snapshots
		_, n, err := rawdbv3.TxNums.FindBlockNum(tx, cfg.agg.EndTxNumMinimax())
		if err != nil {
			return trie.EmptyRoot, err
		}
		shouldRegenerate = s.BlockNumber < n
	}

	if s.BlockNumber == 0 || shouldRegenerate {
		if root, err = RegenerateIntermediateHashes(logPrefix, tx, cfg, &expectedRootHash, ctx, quit); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		// TODO: run up to the tip of sequenced batches here
		// if we're past the first SMT calculation of verified batches, we should only calculate stateroot for subsequent verified batches
		// vBatchNo, err := adapter.GetHermezMeta(tx, "HermezVerifiedBatch")
		vBatchNo, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return trie.EmptyRoot, err
		}

		lastVBlockHeader, err := cfg.blockReader.HeaderByNumber(ctx, tx, vBatchNo)
		if err != nil {
			return trie.EmptyRoot, err
		}
		expectedRootHash = lastVBlockHeader.Root

		if root, err = ZkIncrementIntermediateHashes(logPrefix, s, tx, vBatchNo, cfg, &expectedRootHash, quit); err != nil {
			return trie.EmptyRoot, err
		}

		// TODO: remove unwind test code
		//fmt.Println("DEBUG: UNWINDING TREE!!!!")
		//to = vBatchNo - 300
		//
		//// get the header and check the hash
		//syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, tx, to)
		//if err != nil {
		//	return trie.EmptyRoot, err
		//}
		//
		//expectedRootHash = syncHeadHeader.Root
		//
		//root, err = UnwindZkSMT(logPrefix, vBatchNo, to, tx, cfg, &expectedRootHash, quit)
		//if err != nil {
		//	return trie.EmptyRoot, err
		//}
		//
		//if expectedRootHash != root {
		//	return trie.EmptyRoot, fmt.Errorf("wrong trie root on unwind")
		//}
	}
	_ = quit

	if cfg.checkRoot && root != expectedRootHash {
		log.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", logPrefix, to, root, expectedRootHash, headerHash))
		if cfg.badBlockHalt {
			return trie.EmptyRoot, fmt.Errorf("wrong trie root")
		}
		if cfg.hd != nil {
			cfg.hd.ReportBadHeaderPoS(headerHash, syncHeadHeader.ParentHash)
		}
		if to > s.BlockNumber {
			unwindTo := (to + s.BlockNumber) / 2 // Binary search for the correct block, biased to the lower numbers
			log.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
			u.UnwindTo(unwindTo, headerHash)
		}
	} else if err = s.Update(tx, to); err != nil {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	return root, err
}

// TODO [zkevm] remove debugging struct
type MyStruct struct {
	Storage map[string]string
	Balance *big.Int
	Nonce   *big.Int
}

var collection = make(map[libcommon.Address]*MyStruct)

func insertAccountStateToKV(db smt.DB, keys []utils.NodeKey, ethAddr string, balance, nonce *big.Int) ([]utils.NodeKey, error) {
	keyBalance, err := utils.KeyEthAddrBalance(ethAddr)
	if err != nil {
		return []utils.NodeKey{}, err
	}
	keyNonce, err := utils.KeyEthAddrNonce(ethAddr)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	x := utils.ScalarToArrayBig(balance)
	valueBalance, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	x = utils.ScalarToArrayBig(nonce)
	valueNonce, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	if !valueBalance.IsZero() {
		keys = append(keys, keyBalance)
		db.InsertAccountValue(keyBalance, *valueBalance)
	}
	if !valueNonce.IsZero() {
		keys = append(keys, keyNonce)
		db.InsertAccountValue(keyNonce, *valueNonce)
	}
	return keys, nil
}

func insertContractBytecodeToKV(db smt.DB, keys []utils.NodeKey, ethAddr string, bytecode string) ([]utils.NodeKey, error) {
	keyContractCode, err := utils.KeyContractCode(ethAddr)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	keyContractLength, err := utils.KeyContractLength(ethAddr)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	hashedBytecode, err := utils.HashContractBytecode(bytecode)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	var parsedBytecode string

	if strings.HasPrefix(bytecode, "0x") {
		parsedBytecode = bytecode[2:]
	} else {
		parsedBytecode = bytecode
	}

	if len(parsedBytecode)%2 != 0 {
		parsedBytecode = "0" + parsedBytecode
	}

	bytecodeLength := len(parsedBytecode) / 2

	bi := utils.ConvertHexToBigInt(hashedBytecode)

	x := utils.ScalarToArrayBig(bi)
	valueContractCode, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	x = utils.ScalarToArrayBig(big.NewInt(int64(bytecodeLength)))
	valueContractLength, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	if !valueContractCode.IsZero() {
		keys = append(keys, keyContractCode)
		db.InsertAccountValue(keyContractCode, *valueContractCode)
	}

	if !valueContractLength.IsZero() {
		keys = append(keys, keyContractLength)
		db.InsertAccountValue(keyContractLength, *valueContractLength)
	}

	return keys, nil
}

func insertContractStorageToKV(db smt.DB, keys []utils.NodeKey, ethAddr string, storage map[string]string) ([]utils.NodeKey, error) {
	a := utils.ConvertHexToBigInt(ethAddr)
	add := utils.ScalarToArrayBig(a)

	for k, v := range storage {
		if v == "" {
			continue
		}

		keyStoragePosition, err := utils.KeyContractStorage(add, k)
		if err != nil {
			return []utils.NodeKey{}, err
		}

		base := 10
		if strings.HasPrefix(v, "0x") {
			v = v[2:]
			base = 16
		}

		val, _ := new(big.Int).SetString(v, base)

		x := utils.ScalarToArrayBig(val)
		parsedValue, err := utils.NodeValue8FromBigIntArray(x)
		if err != nil {
			return []utils.NodeKey{}, err
		}

		if !parsedValue.IsZero() {
			keys = append(keys, keyStoragePosition)
			db.InsertAccountValue(keyStoragePosition, *parsedValue)
		}
	}

	return keys, nil
}

func processAccount(db smt.DB, a *accounts.Account, as map[string]string, inc uint64, psr *state2.PlainStateReader, addr libcommon.Address, keys []utils.NodeKey) ([]utils.NodeKey, error) {

	//fmt.Printf("addr: %x\n account: %+v\n storage: %+v\n", addr, a, as)
	collection[addr] = &MyStruct{
		Storage: as,
		Balance: a.Balance.ToBig(),
		Nonce:   new(big.Int).SetUint64(a.Nonce),
	}

	// get the account balance and nonce
	keys, err := insertAccountStateToKV(db, keys, addr.String(), a.Balance.ToBig(), new(big.Int).SetUint64(a.Nonce))
	if err != nil {
		return []utils.NodeKey{}, err
	}

	// store the contract bytecode
	cc, err := psr.ReadAccountCode(addr, inc, a.CodeHash)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	ach := hexutils.BytesToHex(cc)
	if len(ach) > 0 {
		hexcc := fmt.Sprintf("0x%s", ach)
		keys, err = insertContractBytecodeToKV(db, keys, addr.String(), hexcc)
		if err != nil {
			return []utils.NodeKey{}, err
		}
	}

	if len(as) > 0 {
		// store the account storage
		keys, err = insertContractStorageToKV(db, keys, addr.String(), as)
		if err != nil {
			return []utils.NodeKey{}, err
		}
	}

	return keys, nil
}

func RegenerateIntermediateHashes(logPrefix string, db kv.RwTx, cfg TrieCfg, expectedRootHash *libcommon.Hash, ctx context.Context, quitCh <-chan struct{}) (libcommon.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))

	if err := stages.SaveStageProgress(db, stages.IntermediateHashes, 0); err != nil {
		log.Warn(fmt.Sprint("regenerate SaveStageProgress to zero error: ", err))
	}
	if err := db.ClearBucket("HermezSmt"); err != nil {
		log.Warn(fmt.Sprint("regenerate clear HermezSmt error: ", err))
	}
	if err := db.ClearBucket("HermezSmtLastRoot"); err != nil {
		log.Warn(fmt.Sprint("regenerate clear HermezSmtLastRoot error: ", err))
	}
	if err := db.ClearBucket("HermezSmtAccountValues"); err != nil {
		log.Warn(fmt.Sprint("regenerate clear HermezSmtAccountValues error: ", err))
	}

	eridb, err := db2.NewEriDb(db)
	if err != nil {
		return trie.EmptyRoot, err
	}
	smtIn := smt.NewSMT(eridb)

	var a *accounts.Account
	var addr libcommon.Address
	var as map[string]string
	var inc uint64

	var hash libcommon.Hash
	psr := state2.NewPlainStateReader(db)

	dataCollectStartTime := time.Now()
	log.Info(fmt.Sprintf("[%s] Collecting account data...", logPrefix))
	keys := []utils.NodeKey{}

	stateCt := 0
	err = psr.ForEach(kv.PlainState, nil, func(k, acc []byte) error {
		stateCt++
		return nil
	})
	if err != nil {
		return trie.EmptyRoot, err
	}

	progCt := 0
	progress := make(chan int)
	ctDone := make(chan bool)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var pc int
		var pct int

		for {
			select {
			case newPc := <-progress:
				pc = newPc
				if stateCt > 0 {
					pct = (pc * 100) / stateCt
				}
			case <-ticker.C:
				log.Info(fmt.Sprintf("[%s] Progress: %d/%d (%d%%)", logPrefix, pc, stateCt, pct))
			case <-ctDone:
				return
			}
		}
	}()

	err = psr.ForEach(kv.PlainState, nil, func(k, acc []byte) error {
		progCt++
		progress <- progCt
		var err error
		if len(k) == 20 {
			if a != nil { // don't run process on first loop for first account (or it will miss collecting storage)
				keys, err = processAccount(eridb, a, as, inc, psr, addr, keys)
				if err != nil {
					return err
				}
			}

			a = &accounts.Account{}

			if err = a.DecodeForStorage(acc); err != nil {
				// TODO: not an account?
				as = make(map[string]string)
				return nil
			}
			addr = libcommon.BytesToAddress(k)
			inc = a.Incarnation
			// empty storage of previous account
			as = make(map[string]string)
		} else { // otherwise we're reading storage
			_, incarnation, key := dbutils.PlainParseCompositeStorageKey(k)
			if incarnation != inc {
				return nil
			}

			sk := fmt.Sprintf("0x%032x", key)
			v := fmt.Sprintf("0x%032x", acc)

			as[sk] = fmt.Sprint(trimHexString(v))
		}
		return nil
	})

	close(progress)
	close(ctDone)

	if err != nil {
		return trie.EmptyRoot, err
	}

	// process the final account
	keys, err = processAccount(eridb, a, as, inc, psr, addr, keys)
	if err != nil {
		return trie.EmptyRoot, err
	}

	dataCollectTime := time.Since(dataCollectStartTime)
	log.Info(fmt.Sprintf("[%s] Collecting account data finished in %v", logPrefix, dataCollectTime))

	// generate tree
	_, err = smtIn.GenerateFromKVBulk(logPrefix, keys)
	if err != nil {
		return trie.EmptyRoot, err
	}
	root := smtIn.LastRoot()

	_ = db.ClearBucket("HermezSmtAccountValues")

	// [zkevm] - print state
	// jsonData, err := json.MarshalIndent(collection, "", "    ")
	// if err != nil {
	// 	fmt.Printf("error: %v\n", err)
	// }
	// _ = jsonData
	// fmt.Println(string(jsonData))

	hash = libcommon.BigToHash(root)

	err = eridb.CommitBatch()
	if err != nil {
		return trie.EmptyRoot, err
	}

	// TODO [zkevm] - max - remove printing of roots
	fmt.Println("[zkevm] interhashes - expected root: ", expectedRootHash.Hex())
	fmt.Println("[zkevm] interhashes - actual root: ", hash.Hex())

	if cfg.checkRoot && hash != *expectedRootHash {
		// [zkevm] - check against the rpc get block by number
		// get block number
		ss := libcommon.HexToAddress("0x000000000000000000000000000000005ca1ab1e")
		key := libcommon.HexToHash("0x0")

		txno, err2 := psr.ReadAccountStorage(ss, 1, &key)
		if err2 != nil {
			return trie.EmptyRoot, err
		}
		// convert txno to big int
		bigTxNo := big.NewInt(0)
		bigTxNo.SetBytes(txno)

		fmt.Println("[zkevm] interhashes - txno: ", bigTxNo)

		sr, err2 := stateRootByTxNo(bigTxNo)
		if err2 != nil {
			return trie.EmptyRoot, err
		}

		if hash != *sr {
			log.Warn(fmt.Sprintf("[%s] Wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash))
			return hash, nil
		}

		log.Info("[zkevm] interhashes - trie root matches rpc get block by number")
		*expectedRootHash = *sr
		err = nil
	}
	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex())

	return hash, nil
}

type HashPromoter struct {
	tx               kv.RwTx
	ChangeSetBufSize uint64
	TempDir          string
	logPrefix        string
	quitCh           <-chan struct{}
}

func NewHashPromoter(db kv.RwTx, tempDir string, quitCh <-chan struct{}, logPrefix string) *HashPromoter {
	return &HashPromoter{
		tx:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          tempDir,
		quitCh:           quitCh,
		logPrefix:        logPrefix,
	}
}

func stateRootByTxNo(txNo *big.Int) (*libcommon.Hash, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{txNo.Uint64(), true},
		"id":      1,
	}

	requestBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	response, err := http.Post("https://zkevm-rpc.com", "application/json", bytes.NewBuffer(requestBytes))
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	responseMap := make(map[string]interface{})
	if err := json.Unmarshal(responseBytes, &responseMap); err != nil {
		return nil, err
	}

	result, ok := responseMap["result"].(map[string]interface{})
	if !ok {
		return nil, err
	}

	stateRoot, ok := result["stateRoot"].(string)
	if !ok {
		return nil, err
	}
	h := libcommon.HexToHash(stateRoot)

	return &h, nil
}

func (p *HashPromoter) PromoteOnHistoryV3(logPrefix string, agg *state.AggregatorV3, from, to uint64, storage bool, load func(k []byte, v []byte) error) error {
	nonEmptyMarker := []byte{1}

	agg.SetTx(p.tx)

	txnFrom, err := rawdbv3.TxNums.Min(p.tx, from+1)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)

	if storage {
		compositeKey := make([]byte, length.Hash+length.Hash)
		it, err := p.tx.(kv.TemporalTx).HistoryRange(temporal.StorageHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return err
			}
			addrHash, err := common.HashData(k[:length.Addr])
			if err != nil {
				return err
			}
			secKey, err := common.HashData(k[length.Addr:])
			if err != nil {
				return err
			}
			copy(compositeKey, addrHash[:])
			copy(compositeKey[length.Hash:], secKey[:])
			if len(v) != 0 {
				v = nonEmptyMarker
			}
			if err := load(compositeKey, v); err != nil {
				return err
			}
		}
		return nil
	}

	it, err := p.tx.(kv.TemporalTx).HistoryRange(temporal.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		if len(v) != 0 {
			v = nonEmptyMarker
		}
		if err := load(newK, v); err != nil {
			return err
		}
	}
	return nil
}

func (p *HashPromoter) Promote(logPrefix string, from, to uint64, storage bool, load etl.LoadFunc) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	log.Trace(fmt.Sprintf("[%s] Incremental state promotion of intermediate hashes", logPrefix), "from", from, "to", to, "csbucket", changeSetBucket)

	startkey := hexutility.EncodeTs(from + 1)

	decode := historyv2.Mapper[changeSetBucket].Decode
	var deletedAccounts [][]byte
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		if !storage && len(v) > 0 {

			var oldAccount accounts.Account
			if err := oldAccount.DecodeForStorage(v); err != nil {
				return err
			}

			if oldAccount.Incarnation > 0 {

				newValue, err := p.tx.GetOne(kv.PlainState, k)
				if err != nil {
					return err
				}

				if len(newValue) == 0 { // self-destructed
					deletedAccounts = append(deletedAccounts, newK)
				} else { // turns incarnation to zero
					var newAccount accounts.Account
					if err := newAccount.DecodeForStorage(newValue); err != nil {
						return err
					}
					if newAccount.Incarnation < oldAccount.Incarnation {
						deletedAccounts = append(deletedAccounts, newK)
					}
				}
			}
		}

		return next(dbKey, newK, v)
	}

	var l OldestAppearedLoad
	l.innerLoadFunc = load

	if err := etl.Transform(
		logPrefix,
		p.tx,
		changeSetBucket,
		"",
		p.TempDir,
		extract,
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.quitCh,
		},
	); err != nil {
		return err
	}

	if !storage { // delete Intermediate hashes of deleted accounts
		slices.SortFunc(deletedAccounts, func(a, b []byte) bool { return bytes.Compare(a, b) < 0 })
		for _, k := range deletedAccounts {
			if err := p.tx.ForPrefix(kv.TrieOfStorage, k, func(k, v []byte) error {
				if err := p.tx.Delete(kv.TrieOfStorage, k); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func (p *HashPromoter) UnwindOnHistoryV3(logPrefix string, agg *state.AggregatorV3, unwindFrom, unwindTo uint64, storage bool, load func(k []byte, v []byte)) error {
	txnFrom, err := rawdbv3.TxNums.Min(p.tx, unwindTo)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)
	var deletedAccounts [][]byte

	if storage {
		it, err := p.tx.(kv.TemporalTx).HistoryRange(temporal.StorageHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, _, err := it.Next()
			if err != nil {
				return err
			}
			// Plain state not unwind yet, it means - if key not-exists in PlainState but has value from ChangeSets - then need mark it as "created" in RetainList
			enc, err := p.tx.GetOne(kv.PlainState, k[:20])
			if err != nil {
				return err
			}
			incarnation := uint64(1)
			if len(enc) != 0 {
				oldInc, _ := accounts.DecodeIncarnationFromStorage(enc)
				incarnation = oldInc
			}
			plainKey := dbutils.PlainGenerateCompositeStorageKey(k[:20], incarnation, k[20:])
			value, err := p.tx.GetOne(kv.PlainState, plainKey)
			if err != nil {
				return err
			}
			newK, err := transformPlainStateKey(plainKey)
			if err != nil {
				return err
			}
			load(newK, value)
		}
		return nil
	}

	it, err := p.tx.(kv.TemporalTx).HistoryRange(temporal.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		// Plain state not unwind yet, it means - if key not-exists in PlainState but has value from ChangeSets - then need mark it as "created" in RetainList
		value, err := p.tx.GetOne(kv.PlainState, k)
		if err != nil {
			return err
		}

		if len(value) > 0 {
			oldInc, _ := accounts.DecodeIncarnationFromStorage(value)
			if oldInc > 0 {
				if len(v) == 0 { // self-destructed
					deletedAccounts = append(deletedAccounts, newK)
				} else {
					var newAccount accounts.Account
					if err = accounts.DeserialiseV3(&newAccount, v); err != nil {
						return err
					}
					if newAccount.Incarnation > oldInc {
						deletedAccounts = append(deletedAccounts, newK)
					}
				}
			}
		}

		load(newK, value)
	}

	// delete Intermediate hashes of deleted accounts
	slices.SortFunc(deletedAccounts, func(a, b []byte) bool { return bytes.Compare(a, b) < 0 })
	for _, k := range deletedAccounts {
		if err := p.tx.ForPrefix(kv.TrieOfStorage, k, func(k, v []byte) error {
			if err := p.tx.Delete(kv.TrieOfStorage, k); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (p *HashPromoter) Unwind(logPrefix string, s *StageState, u *UnwindState, storage bool, load etl.LoadFunc) error {
	to := u.UnwindPoint
	var changeSetBucket string

	if storage {
		changeSetBucket = kv.StorageChangeSet
	} else {
		changeSetBucket = kv.AccountChangeSet
	}
	log.Info(fmt.Sprintf("[%s] Unwinding", logPrefix), "from", s.BlockNumber, "to", to, "csbucket", changeSetBucket)

	startkey := hexutility.EncodeTs(to + 1)

	decode := historyv2.Mapper[changeSetBucket].Decode
	var deletedAccounts [][]byte
	extract := func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		_, k, v, err := decode(dbKey, dbValue)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		// Plain state not unwind yet, it means - if key not-exists in PlainState but has value from ChangeSets - then need mark it as "created" in RetainList
		value, err := p.tx.GetOne(kv.PlainState, k)
		if err != nil {
			return err
		}

		if !storage && len(value) > 0 {
			var oldAccount accounts.Account
			if err = oldAccount.DecodeForStorage(value); err != nil {
				return err
			}
			if oldAccount.Incarnation > 0 {
				if len(v) == 0 { // self-destructed
					deletedAccounts = append(deletedAccounts, newK)
				} else {
					var newAccount accounts.Account
					if err = newAccount.DecodeForStorage(v); err != nil {
						return err
					}
					if newAccount.Incarnation > oldAccount.Incarnation {
						deletedAccounts = append(deletedAccounts, newK)
					}
				}
			}
		}
		return next(k, newK, value)
	}

	var l OldestAppearedLoad
	l.innerLoadFunc = load

	if err := etl.Transform(
		logPrefix,
		p.tx,
		changeSetBucket,
		"",
		p.TempDir,
		extract,
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.quitCh,
		},
	); err != nil {
		return err
	}

	if !storage { // delete Intermediate hashes of deleted accounts
		slices.SortFunc(deletedAccounts, func(a, b []byte) bool { return bytes.Compare(a, b) < 0 })
		for _, k := range deletedAccounts {
			if err := p.tx.ForPrefix(kv.TrieOfStorage, k, func(k, v []byte) error {
				if err := p.tx.Delete(kv.TrieOfStorage, k); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}

	return nil
}

func incrementIntermediateHashes(logPrefix string, s *StageState, db kv.RwTx, to uint64, cfg TrieCfg, expectedRootHash libcommon.Hash, quit <-chan struct{}) (libcommon.Hash, error) {
	p := NewHashPromoter(db, cfg.tmpDir, quit, logPrefix)
	rl := trie.NewRetainList(0)
	if cfg.historyV3 {
		cfg.agg.SetTx(db)
		collect := func(k, v []byte) error {
			if len(k) == 32 {
				rl.AddKeyWithMarker(k, len(v) == 0)
				return nil
			}
			accBytes, err := p.tx.GetOne(kv.HashedAccounts, k[:32])
			if err != nil {
				return err
			}
			incarnation := uint64(1)
			if len(accBytes) != 0 {
				incarnation, err = accounts.DecodeIncarnationFromStorage(accBytes)
				if err != nil {
					return err
				}
				if incarnation == 0 {
					return nil
				}
			}
			compositeKey := make([]byte, length.Hash+length.Incarnation+length.Hash)
			copy(compositeKey, k[:32])
			binary.BigEndian.PutUint64(compositeKey[32:], incarnation)
			copy(compositeKey[40:], k[32:])
			rl.AddKeyWithMarker(compositeKey, len(v) == 0)
			return nil
		}
		if err := p.PromoteOnHistoryV3(logPrefix, cfg.agg, s.BlockNumber, to, false, collect); err != nil {
			return trie.EmptyRoot, err
		}
		if err := p.PromoteOnHistoryV3(logPrefix, cfg.agg, s.BlockNumber, to, true, collect); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		collect := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			rl.AddKeyWithMarker(k, len(v) == 0)
			return nil
		}
		if err := p.Promote(logPrefix, s.BlockNumber, to, false, collect); err != nil {
			return trie.EmptyRoot, err
		}
		if err := p.Promote(logPrefix, s.BlockNumber, to, true, collect); err != nil {
			return trie.EmptyRoot, err
		}
	}
	accTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer accTrieCollector.Close()
	accTrieCollectorFunc := accountTrieCollector(accTrieCollector)

	stTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer stTrieCollector.Close()
	stTrieCollectorFunc := storageTrieCollector(stTrieCollector)

	loader := trie.NewFlatDBTrieLoader(logPrefix, rl, accTrieCollectorFunc, stTrieCollectorFunc, false)
	hash, err := loader.CalcTrieRoot(db, quit)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if cfg.checkRoot && hash != expectedRootHash {
		return hash, nil
	}

	if err := accTrieCollector.Load(db, kv.TrieOfAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	if err := stTrieCollector.Load(db, kv.TrieOfStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return trie.EmptyRoot, err
	}
	return hash, nil
}

func UnwindIntermediateHashesStage(u *UnwindState, s *StageState, tx kv.RwTx, cfg TrieCfg, ctx context.Context) (err error) {
	quit := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	syncHeadHeader, err := cfg.blockReader.HeaderByNumber(ctx, tx, u.UnwindPoint)
	if err != nil {
		return err
	}
	if syncHeadHeader == nil {
		return fmt.Errorf("header not found for block number %d", u.UnwindPoint)
	}
	expectedRootHash := syncHeadHeader.Root

	root, err := UnwindZkSMT(s.LogPrefix(), s.BlockNumber, u.UnwindPoint, tx, cfg, &expectedRootHash, quit)
	if err != nil {
		return err
	}
	_ = root

	//logPrefix := s.LogPrefix()
	//if err := unwindIntermediateHashesStageImpl(logPrefix, u, s, tx, cfg, expectedRootHash, quit); err != nil {
	//	return err
	//}
	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindIntermediateHashesForTrieLoader(logPrefix string, rl *trie.RetainList, u *UnwindState, s *StageState, db kv.RwTx, cfg TrieCfg, accTrieCollectorFunc trie.HashCollector2, stTrieCollectorFunc trie.StorageHashCollector2, quit <-chan struct{}) (*trie.FlatDBTrieLoader, error) {
	p := NewHashPromoter(db, cfg.tmpDir, quit, logPrefix)
	if cfg.historyV3 {
		cfg.agg.SetTx(db)
		collect := func(k, v []byte) {
			rl.AddKeyWithMarker(k, len(v) == 0)
		}
		if err := p.UnwindOnHistoryV3(logPrefix, cfg.agg, s.BlockNumber, u.UnwindPoint, false, collect); err != nil {
			return nil, err
		}
		if err := p.UnwindOnHistoryV3(logPrefix, cfg.agg, s.BlockNumber, u.UnwindPoint, true, collect); err != nil {
			return nil, err
		}
	} else {
		collect := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			rl.AddKeyWithMarker(k, len(v) == 0)
			return nil
		}
		if err := p.Unwind(logPrefix, s, u, false /* storage */, collect); err != nil {
			return nil, err
		}
		if err := p.Unwind(logPrefix, s, u, true /* storage */, collect); err != nil {
			return nil, err
		}
	}

	return trie.NewFlatDBTrieLoader(logPrefix, rl, accTrieCollectorFunc, stTrieCollectorFunc, false), nil
}

func unwindIntermediateHashesStageImpl(logPrefix string, u *UnwindState, s *StageState, db kv.RwTx, cfg TrieCfg, expectedRootHash libcommon.Hash, quit <-chan struct{}) error {
	accTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer accTrieCollector.Close()
	accTrieCollectorFunc := accountTrieCollector(accTrieCollector)

	stTrieCollector := etl.NewCollector(logPrefix, cfg.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer stTrieCollector.Close()
	stTrieCollectorFunc := storageTrieCollector(stTrieCollector)

	rl := trie.NewRetainList(0)

	loader, err := UnwindIntermediateHashesForTrieLoader(logPrefix, rl, u, s, db, cfg, accTrieCollectorFunc, stTrieCollectorFunc, quit)
	if err != nil {
		return err
	}

	hash, err := loader.CalcTrieRoot(db, quit)
	if err != nil {
		return err
	}
	if hash != expectedRootHash {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
	}
	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex())
	if err := accTrieCollector.Load(db, kv.TrieOfAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	if err := stTrieCollector.Load(db, kv.TrieOfStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func assertSubset(a, b uint16) {
	if (a & b) != a { // a & b == a - checks whether a is subset of b
		panic(fmt.Errorf("invariant 'is subset' failed: %b, %b", a, b))
	}
}

func accountTrieCollector(collector *etl.Collector) trie.HashCollector2 {
	newV := make([]byte, 0, 1024)
	return func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, _ []byte) error {
		if len(keyHex) == 0 {
			return nil
		}
		if hasState == 0 {
			return collector.Collect(keyHex, nil)
		}
		if bits.OnesCount16(hasHash) != len(hashes)/length.Hash {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/length.Hash))
		}
		assertSubset(hasTree, hasState)
		assertSubset(hasHash, hasState)
		newV = trie.MarshalTrieNode(hasState, hasTree, hasHash, hashes, nil, newV)
		return collector.Collect(keyHex, newV)
	}
}

func storageTrieCollector(collector *etl.Collector) trie.StorageHashCollector2 {
	newK := make([]byte, 0, 128)
	newV := make([]byte, 0, 1024)
	return func(accWithInc []byte, keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		newK = append(append(newK[:0], accWithInc...), keyHex...)
		if hasState == 0 {
			return collector.Collect(newK, nil)
		}
		if len(keyHex) > 0 && hasHash == 0 && hasTree == 0 {
			return nil
		}
		if bits.OnesCount16(hasHash) != len(hashes)/length.Hash {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/length.Hash))
		}
		assertSubset(hasTree, hasState)
		assertSubset(hasHash, hasState)
		newV = trie.MarshalTrieNode(hasState, hasTree, hasHash, hashes, rootHash, newV)
		return collector.Collect(newK, newV)
	}
}

func PruneIntermediateHashesStage(s *PruneState, tx kv.RwTx, cfg TrieCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	s.Done(tx)

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func trimHexString(s string) string {
	if strings.HasPrefix(s, "0x") {
		s = s[2:]
	}

	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return "0x" + s[i:]
		}
	}

	return "0x0"
}

func ZkIncrementIntermediateHashes(logPrefix string, s *StageState, db kv.RwTx, to uint64, cfg TrieCfg, expectedRootHash *libcommon.Hash, quit <-chan struct{}) (libcommon.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))

	fmt.Println("[zkevm] interhashes - previous root @: ", s.BlockNumber)
	fmt.Println("[zkevm] interhashes - calculating root @: ", to)

	eridb, err := db2.NewEriDb(db)
	if err != nil {
		return trie.EmptyRoot, err
	}
	dbSmt := smt.NewSMT(eridb)

	fmt.Println("last root: ", libcommon.BigToHash(dbSmt.LastRoot()))

	eridb.OpenBatch(quit)

	ac, err := db.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer ac.Close()

	sc, err := db.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer sc.Close()

	accChanges := make(map[libcommon.Address]*accounts.Account)
	codeChanges := make(map[libcommon.Address]string)
	storageChanges := make(map[libcommon.Address]map[string]string)

	// NB: changeset tables are zero indexed
	// changeset tables contain historical value at N-1, so we look up values from plainstate
	for i := s.BlockNumber + 1; i <= to; i++ {
		dupSortKey := dbutils.EncodeBlockNumber(i)

		fmt.Println("[zkevm] interhashes - block: ", i)

		// TODO [zkevm]: find out the contractcodelookup

		// i+1 to get state at the beginning of the next batch
		psr := state2.NewPlainState(db, i+1, systemcontracts.SystemContractCodeLookup["Hermez"])

		// collect changes to accounts and code
		for _, v, err := ac.SeekExact(dupSortKey); err == nil && v != nil; _, v, err = ac.NextDup() {
			addr := libcommon.BytesToAddress(v[:length.Addr])

			currAcc, err := psr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			// store the account
			accChanges[addr] = currAcc

			// TODO: figure out if we can optimise for performance by making this optional, only on 'creation' or similar
			cc, err := psr.ReadAccountCode(addr, currAcc.Incarnation, currAcc.CodeHash)
			if err != nil {
				return trie.EmptyRoot, err
			}

			ach := hexutils.BytesToHex(cc)
			if len(ach) > 0 {
				hexcc := fmt.Sprintf("0x%s", ach)
				codeChanges[addr] = hexcc
				if err != nil {
					return trie.EmptyRoot, err
				}
			}
		}

		err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
			changesetKey := sk[length.BlockNum:]
			address, incarnation := dbutils.PlainParseStoragePrefix(changesetKey)

			sstorageKey := sv[:length.Hash]
			stk := libcommon.BytesToHash(sstorageKey)

			value, err := psr.ReadAccountStorage(address, incarnation, &stk)
			if err != nil {
				return err
			}

			stkk := fmt.Sprintf("0x%032x", stk)
			v := fmt.Sprintf("0x%032x", libcommon.BytesToHash(value))

			m := make(map[string]string)
			m[stkk] = v

			if storageChanges[address] == nil {
				storageChanges[address] = make(map[string]string)
			}
			storageChanges[address][stkk] = v
			return nil
		})
		if err != nil {
			return trie.EmptyRoot, err
		}

		// update the tree
		for addr, acc := range accChanges {
			err := updateAccInTree(dbSmt, addr, acc)
			if err != nil {
				return trie.EmptyRoot, err
			}
		}
		for addr, code := range codeChanges {
			err := updateCodeInTree(dbSmt, addr.String(), code)
			if err != nil {
				return trie.EmptyRoot, err
			}
		}

		for addr, storage := range storageChanges {
			err := updateStorageInTree(dbSmt, addr, storage)
			if err != nil {
				return trie.EmptyRoot, err
			}
		}
	}

	err = verifyLastHash(dbSmt, expectedRootHash, &cfg, logPrefix)
	if err != nil {
		eridb.RollbackBatch()
		log.Error("failed to verify hash")
		return trie.EmptyRoot, err
	}

	err = eridb.CommitBatch()
	if err != nil {
		return trie.EmptyRoot, err
	}

	lr := dbSmt.LastRoot()

	hash := libcommon.BigToHash(lr)
	return hash, nil

}

func updateAccInTree(smt *smt.SMT, addr libcommon.Address, acc *accounts.Account) error {
	if acc != nil {
		n := new(big.Int).SetUint64(acc.Nonce)
		_, err := smt.SetAccountState(addr.String(), acc.Balance.ToBig(), n)
		return err
	}

	_, err := smt.SetAccountState(addr.String(), big.NewInt(0), big.NewInt(0))
	return err
}

func updateStorageInTree(smt *smt.SMT, addr libcommon.Address, as map[string]string) error {
	_, err := smt.SetContractStorage(addr.String(), as)
	return err
}

func updateCodeInTree(smt *smt.SMT, addr string, code string) error {
	return smt.SetContractBytecode(addr, code)
}

func verifyLastHash(dbSmt *smt.SMT, expectedRootHash *libcommon.Hash, cfg *TrieCfg, logPrefix string) error {
	hash := libcommon.BigToHash(dbSmt.LastRoot())

	fmt.Println("[zkevm] interhashes - expected root: ", expectedRootHash.Hex())
	fmt.Println("[zkevm] interhashes - actual root: ", hash.Hex())

	if cfg.checkRoot && hash != *expectedRootHash {
		log.Warn(fmt.Sprintf("[%s] Wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash))
		return nil
	}
	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", hash.Hex())
	return nil
}

func UnwindZkSMT(logPrefix string, from, to uint64, db kv.RwTx, cfg TrieCfg, expectedRootHash *libcommon.Hash, quit <-chan struct{}) (libcommon.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Unwind trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Unwind ended", logPrefix))

	eridb, err := db2.NewEriDb(db)
	if err != nil {
		return trie.EmptyRoot, err
	}
	dbSmt := smt.NewSMT(eridb)

	fmt.Println("last root: ", libcommon.BigToHash(dbSmt.LastRoot()))

	eridb.OpenBatch(quit)

	ac, err := db.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer ac.Close()

	sc, err := db.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer sc.Close()

	accChanges := make(map[libcommon.Address]*accounts.Account)
	accDeletes := make([]libcommon.Address, 0)
	codeChanges := make(map[libcommon.Address]string)
	storageChanges := make(map[libcommon.Address]map[string]string)

	currentPsr := state2.NewPlainStateReader(db)

	// walk backwards through the blocks, applying state changes, and deletes
	// PlainState contains data AT the block
	// History tables contain data BEFORE the block - so need a +1 offset

	for i := from; i >= to+1; i-- {
		dupSortKey := dbutils.EncodeBlockNumber(i)

		fmt.Println("[zkevm] interhashes - block: ", i)

		// collect changes to accounts and code
		for _, v, err2 := ac.SeekExact(dupSortKey); err2 == nil && v != nil; _, v, err2 = ac.NextDup() {
			addr := libcommon.BytesToAddress(v[:length.Addr])

			// if the account was created in this changeset we should delete it
			if len(v[length.Addr:]) == 0 {
				codeChanges[addr] = ""
				accDeletes = append(accDeletes, addr)
				continue
			}

			// decode the old acc from the changeset
			oldAcc := new(accounts.Account)
			err = oldAcc.DecodeForStorage(v[length.Addr:])
			if err != nil {
				return trie.EmptyRoot, err
			}

			// currAcc at block we're unwinding from
			currAcc, err := currentPsr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			if oldAcc != nil {
				if oldAcc.Incarnation > 0 {
					if len(v) == 0 { // self-destructed
						accDeletes = append(accDeletes, addr)
					} else {
						if currAcc.Incarnation > oldAcc.Incarnation {
							accDeletes = append(accDeletes, addr)
						}
					}
				}
			}

			if oldAcc != nil {
				// store the account
				fmt.Println("unwinding acc: ", addr.Hex())

				//fmt.Printf("old acc nonce: %d, curr acc nonce: %d\n", oldAcc.Nonce, currAcc.Nonce)
				//fmt.Printf("old acc balance: %d, curr acc balance: %d\n", oldAcc.Balance, currAcc.Balance)
				//fmt.Printf("old acc incarnation: %d, curr acc incarnation: %d\n", oldAcc.Incarnation, currAcc.Incarnation)
				//fmt.Printf("old acc codehash: %x, curr acc codehash: %x\n", oldAcc.CodeHash, currAcc.CodeHash)

				accChanges[addr] = oldAcc

				if oldAcc.CodeHash != currAcc.CodeHash {
					cc, err := currentPsr.ReadAccountCode(addr, oldAcc.Incarnation, oldAcc.CodeHash)
					if err != nil {
						return trie.EmptyRoot, err
					}

					ach := hexutils.BytesToHex(cc)
					if len(ach) > 0 {
						hexcc := fmt.Sprintf("0x%s", ach)
						codeChanges[addr] = hexcc
						if err != nil {
							return trie.EmptyRoot, err
						}
					}
				}
			}
		}

		err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
			changesetKey := sk[length.BlockNum:]
			address, _ := dbutils.PlainParseStoragePrefix(changesetKey)

			sstorageKey := sv[:length.Hash]
			stk := libcommon.BytesToHash(sstorageKey)

			value := []byte{0}
			if len(sv[length.Hash:]) != 0 {
				value = sv[length.Hash:]
			}

			stkk := fmt.Sprintf("0x%032x", stk)
			v := fmt.Sprintf("0x%032x", libcommon.BytesToHash(value))

			m := make(map[string]string)
			m[stkk] = v

			if storageChanges[address] == nil {
				storageChanges[address] = make(map[string]string)
			}
			storageChanges[address][stkk] = v
			return nil
		})
		if err != nil {
			return trie.EmptyRoot, err
		}

		// update the tree
		for addr, acc := range accChanges {
			err := updateAccInTree(dbSmt, addr, acc)
			if err != nil {
				return trie.EmptyRoot, err
			}
		}
		for addr, code := range codeChanges {
			err := updateCodeInTree(dbSmt, addr.String(), code)
			if err != nil {
				return trie.EmptyRoot, err
			}
		}

		for addr, storage := range storageChanges {
			err := updateStorageInTree(dbSmt, addr, storage)
			if err != nil {
				return trie.EmptyRoot, err
			}
		}
	}

	for _, k := range accDeletes {
		fmt.Println("deleting acc: ", k.Hex())
		err := updateAccInTree(dbSmt, k, nil)
		if err != nil {
			return trie.EmptyRoot, err
		}
	}

	err = verifyLastHash(dbSmt, expectedRootHash, &cfg, logPrefix)
	if err != nil {
		eridb.RollbackBatch()
		log.Error("failed to verify hash")
		return trie.EmptyRoot, err
	}

	err = eridb.CommitBatch()
	if err != nil {
		return trie.EmptyRoot, err
	}

	lr := dbSmt.LastRoot()

	hash := libcommon.BigToHash(lr)
	return hash, nil
}
