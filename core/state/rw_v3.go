package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/metrics"
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

const CodeSizeTable = "CodeSize"

var ExecTxsDone = metrics.NewCounter(`exec_txs_done`)

type StateV3 struct {
	domains      *libstate.SharedDomains
	triggerLock  sync.Mutex
	triggers     map[uint64]*exec22.TxTask
	senderTxNums map[common.Address]uint64

	applyPrevAccountBuf []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
}

func NewStateV3(domains *libstate.SharedDomains) *StateV3 {
	return &StateV3{
		domains:             domains,
		triggers:            map[uint64]*exec22.TxTask{},
		senderTxNums:        map[common.Address]uint64{},
		applyPrevAccountBuf: make([]byte, 256),
	}
}

func (rs *StateV3) ReTry(txTask *exec22.TxTask, in *exec22.QueueWithRetry) {
	rs.resetTxTask(txTask)
	in.ReTry(txTask)
}
func (rs *StateV3) AddWork(ctx context.Context, txTask *exec22.TxTask, in *exec22.QueueWithRetry) {
	rs.resetTxTask(txTask)
	in.Add(ctx, txTask)
}
func (rs *StateV3) resetTxTask(txTask *exec22.TxTask) {
	txTask.BalanceIncreaseSet = nil
	returnReadList(txTask.ReadLists)
	txTask.ReadLists = nil
	returnWriteList(txTask.WriteLists)
	txTask.WriteLists = nil
	txTask.Logs = nil
	txTask.TraceFroms = nil
	txTask.TraceTos = nil

	/*
		txTask.ReadLists = nil
		txTask.WriteLists = nil
		txTask.AccountPrevs = nil
		txTask.AccountDels = nil
		txTask.StoragePrevs = nil
		txTask.CodePrevs = nil
	*/
}

func (rs *StateV3) RegisterSender(txTask *exec22.TxTask) bool {
	//TODO: it deadlocks on panic, fix it
	defer func() {
		rec := recover()
		if rec != nil {
			fmt.Printf("panic?: %s,%s\n", rec, dbg.Stack())
		}
	}()
	rs.triggerLock.Lock()
	defer rs.triggerLock.Unlock()
	lastTxNum, deferral := rs.senderTxNums[*txTask.Sender]
	if deferral {
		// Transactions with the same sender have obvious data dependency, no point running it before lastTxNum
		// So we add this data dependency as a trigger
		//fmt.Printf("trigger[%d] sender [%x]<=%x\n", lastTxNum, *txTask.Sender, txTask.Tx.Hash())
		rs.triggers[lastTxNum] = txTask
	}
	//fmt.Printf("senderTxNums[%x]=%d\n", *txTask.Sender, txTask.TxNum)
	rs.senderTxNums[*txTask.Sender] = txTask.TxNum
	return !deferral
}

func (rs *StateV3) CommitTxNum(sender *common.Address, txNum uint64, in *exec22.QueueWithRetry) (count int) {
	ExecTxsDone.Inc()

	if txNum > 0 && txNum%ethconfig.HistoryV3AggregationStep == 0 {
		if _, err := rs.Commitment(txNum, true); err != nil {
			panic(fmt.Errorf("txnum %d: %w", txNum, err))
		}
	}

	rs.triggerLock.Lock()
	defer rs.triggerLock.Unlock()
	if triggered, ok := rs.triggers[txNum]; ok {
		in.ReTry(triggered)
		count++
		delete(rs.triggers, txNum)
	}
	if sender != nil {
		if lastTxNum, ok := rs.senderTxNums[*sender]; ok && lastTxNum == txNum {
			// This is the last transaction so far with this sender, remove
			delete(rs.senderTxNums, *sender)
		}
	}
	return count
}

func (rs *StateV3) applyState(txTask *exec22.TxTask, domains *libstate.SharedDomains) error {
	emptyRemoval := txTask.Rules.IsSpuriousDragon

	skipUpdates := false

	for k, update := range txTask.UpdatesList {
		if skipUpdates {
			continue
		}
		upd := update
		key := txTask.UpdatesKey[k]
		if upd.Flags == commitment.DeleteUpdate {

			prev, err := domains.LatestAccount(key)
			if err != nil {
				return fmt.Errorf("latest account %x: %w", key, err)
			}
			if err := domains.DeleteAccount(key, prev); err != nil {
				return fmt.Errorf("delete account %x: %w", key, err)
			}
			fmt.Printf("apply - delete account %x\n", key)
		} else {
			if upd.Flags&commitment.BalanceUpdate != 0 || upd.Flags&commitment.NonceUpdate != 0 {
				prev, err := domains.LatestAccount(key)
				if err != nil {
					return fmt.Errorf("latest account %x: %w", key, err)
				}
				old := accounts.NewAccount()
				if len(prev) > 0 {
					accounts.DeserialiseV3(&old, prev)
				}

				if upd.Flags&commitment.BalanceUpdate != 0 {
					old.Balance.Set(&upd.Balance)
				}
				if upd.Flags&commitment.NonceUpdate != 0 {
					old.Nonce = upd.Nonce
				}

				acc := UpdateToAccount(upd)
				fmt.Printf("apply - update account %x b %v n %d\n", key, upd.Balance.Uint64(), upd.Nonce)
				if err := domains.UpdateAccountData(key, accounts.SerialiseV3(acc), prev); err != nil {
					return err
				}
			}
			if upd.Flags&commitment.CodeUpdate != 0 {
				if len(upd.CodeValue[:]) == 0 && !bytes.Equal(upd.CodeHashOrStorage[:], emptyCodeHash) {
					continue
				}
				fmt.Printf("apply - update code %x h %x v %x\n", key, upd.CodeHashOrStorage[:], upd.CodeValue[:])
				if err := domains.UpdateAccountCode(key, upd.CodeValue, upd.CodeHashOrStorage[:]); err != nil {
					return err
				}
			}
			if upd.Flags&commitment.StorageUpdate != 0 {
				prev, err := domains.LatestStorage(key[:length.Addr], key[length.Addr:])
				if err != nil {
					return fmt.Errorf("latest code %x: %w", key, err)
				}
				fmt.Printf("apply - storage %x h %x\n", key, upd.CodeHashOrStorage[:upd.ValLength])
				err = domains.WriteAccountStorage(key[:length.Addr], key[length.Addr:], upd.CodeHashOrStorage[:upd.ValLength], prev)
				if err != nil {
					return err
				}
			}
		}
	}
	if !skipUpdates {
		return nil
	}

	// TODO do we really need to use BIS when we store all updates encoded inside
	//  	  writeLists? one exception - block rewards, but they're changing writelist aswell..
	var acc accounts.Account

	for addr, increase := range txTask.BalanceIncreaseSet {
		increase := increase
		addrBytes := addr.Bytes()
		enc0, err := domains.LatestAccount(addrBytes)
		if err != nil {
			return err
		}
		acc.Reset()
		if len(enc0) > 0 {
			if err := accounts.DeserialiseV3(&acc, enc0); err != nil {
				return err
			}
		}
		acc.Balance.Add(&acc.Balance, &increase)
		var enc1 []byte
		if emptyRemoval && acc.Nonce == 0 && acc.Balance.IsZero() && acc.IsEmptyCodeHash() {
			enc1 = nil
		} else {
			enc1 = accounts.SerialiseV3(&acc)
		}

		fmt.Printf("+applied %v b=%d n=%d c=%x\n", hex.EncodeToString(addrBytes), &acc.Balance, acc.Nonce, acc.CodeHash.Bytes())
		if err := domains.UpdateAccountData(addrBytes, enc1, enc0); err != nil {
			return err
		}
	}

	if txTask.WriteLists != nil {
		for table, list := range txTask.WriteLists {
			switch table {
			case kv.AccountDomain:
				for k, key := range list.Keys {
					kb, _ := hex.DecodeString(key)
					prev, err := domains.LatestAccount(kb)
					if err != nil {
						return fmt.Errorf("latest account %x: %w", key, err)
					}
					if list.Vals[k] == nil {
						if err := domains.DeleteAccount(kb, list.Vals[k]); err != nil {
							return err
						}
					} else {
						if err := domains.UpdateAccountData(kb, list.Vals[k], prev); err != nil {
							return err
						}
					}
					if list.Vals[k] == nil {
						fmt.Printf("applied %x deleted\n", kb)
						continue
					}
					accounts.DeserialiseV3(&acc, list.Vals[k])
					fmt.Printf("applied %x b=%d n=%d c=%x\n", kb, &acc.Balance, acc.Nonce, acc.CodeHash.Bytes())

					acc.Reset()
				}
			case kv.CodeDomain:
				for k, key := range list.Keys {
					kb, _ := hex.DecodeString(key)
					fmt.Printf("applied %x c=%x\n", kb, list.Vals[k])
					if err := domains.UpdateAccountCode(kb, list.Vals[k], nil); err != nil {
						return err
					}
				}
			case kv.StorageDomain:
				for k, key := range list.Keys {
					hkey, err := hex.DecodeString(key)
					if err != nil {
						panic(err)
					}
					addr, loc := hkey[:20], hkey[20:]
					prev, err := domains.LatestStorage(addr, loc)
					if err != nil {
						return fmt.Errorf("latest account %x: %w", key, err)
					}
					fmt.Printf("applied %x s=%x\n", hkey, list.Vals[k])
					if err := domains.WriteAccountStorage(addr, loc, list.Vals[k], prev); err != nil {
						return err
					}
				}
			default:
				continue
			}
		}

	}
	return nil
}

func (rs *StateV3) Commitment(txNum uint64, saveState bool) ([]byte, error) {
	//defer agg.BatchHistoryWriteStart().BatchHistoryWriteEnd()

	rs.domains.SetTxNum(txNum)
	return rs.domains.Commit(saveState, false)
}

func (rs *StateV3) Domains() *libstate.SharedDomains {
	return rs.domains
}

func (rs *StateV3) ApplyState4(txTask *exec22.TxTask, agg *libstate.AggregatorV3) error {
	defer agg.BatchHistoryWriteStart().BatchHistoryWriteEnd()

	agg.SetTxNum(txTask.TxNum)
	rs.domains.SetTxNum(txTask.TxNum)
	if err := rs.applyState(txTask, rs.domains); err != nil {
		return err
	}
	returnReadList(txTask.ReadLists)
	returnWriteList(txTask.WriteLists)
	txTask.UpdatesList = txTask.UpdatesList[:0]
	txTask.UpdatesKey = txTask.UpdatesKey[:0]

	txTask.ReadLists, txTask.WriteLists = nil, nil
	return nil
}

func (rs *StateV3) ApplyLogsAndTraces(txTask *exec22.TxTask, agg *libstate.AggregatorV3) error {
	if dbg.DiscardHistory() {
		return nil
	}
	defer agg.BatchHistoryWriteStart().BatchHistoryWriteEnd()

	//for addrS, enc0 := range txTask.AccountPrevs {
	//	if err := agg.AddAccountPrev([]byte(addrS), enc0); err != nil {
	//		return err
	//	}
	//}
	//for compositeS, val := range txTask.StoragePrevs {
	//	composite := []byte(compositeS)
	//	if err := agg.AddStoragePrev(composite[:20], composite[28:], val); err != nil {
	//		return err
	//	}
	//}
	if txTask.TraceFroms != nil {
		for addr := range txTask.TraceFroms {
			if err := agg.AddTraceFrom(addr[:]); err != nil {
				return err
			}
		}
	}
	if txTask.TraceTos != nil {
		for addr := range txTask.TraceTos {
			if err := agg.AddTraceTo(addr[:]); err != nil {
				return err
			}
		}
	}
	for _, log := range txTask.Logs {
		if err := agg.AddLogAddr(log.Address[:]); err != nil {
			return fmt.Errorf("adding event log for addr %x: %w", log.Address, err)
		}
		for _, topic := range log.Topics {
			if err := agg.AddLogTopic(topic[:]); err != nil {
				return fmt.Errorf("adding event log for topic %x: %w", topic, err)
			}
		}
	}
	return nil
}

func recoverCodeHashPlain(acc *accounts.Account, db kv.Tx, key []byte) {
	var address common.Address
	copy(address[:], key)
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		if codeHash, err2 := db.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation)); err2 == nil {
			copy(acc.CodeHash[:], codeHash)
		}
	}
}

func newStateReader(tx kv.Tx) StateReader {
	if ethconfig.EnableHistoryV4InTest {
		return NewReaderV4(tx.(kv.TemporalTx))
	}
	return NewPlainStateReader(tx)
}

func (rs *StateV3) Unwind(ctx context.Context, tx kv.RwTx, txUnwindTo uint64, agg *libstate.AggregatorV3, accumulator *shards.Accumulator) error {
	agg.SetTx(tx)
	var currentInc uint64
	if err := agg.Unwind(ctx, txUnwindTo, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == length.Addr {
			if len(v) > 0 {
				var acc accounts.Account
				if err := accounts.DeserialiseV3(&acc, v); err != nil {
					return fmt.Errorf("%w, %x", err, v)
				}
				var address common.Address
				copy(address[:], k)

				newV := make([]byte, acc.EncodingLengthForStorage())
				acc.EncodeForStorage(newV)
				if accumulator != nil {
					accumulator.ChangeAccount(address, acc.Incarnation, newV)
				}
			} else {
				var address common.Address
				copy(address[:], k)
				if accumulator != nil {
					accumulator.DeleteAccount(address)
				}
			}
			return nil
		}

		var address common.Address
		var location common.Hash
		copy(address[:], k[:length.Addr])
		copy(location[:], k[length.Addr:])
		if accumulator != nil {
			accumulator.ChangeStorage(address, currentInc, location, common.Copy(v))
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (rs *StateV3) DoneCount() uint64 { return ExecTxsDone.Get() }

func (rs *StateV3) SizeEstimate() (r uint64) {
	if rs.domains != nil {
		r += rs.domains.SizeEstimate()
	}
	return r
}

func (rs *StateV3) ReadsValid(readLists map[string]*exec22.KvList) bool {
	rs.domains.RLock()
	defer rs.domains.RUnlock()

	for table, list := range readLists {
		if !rs.domains.ReadsValidBtree(table, list) {
			return false
		}
	}
	return true
}

// StateWriterBufferedV3 - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type StateWriterBufferedV3 struct {
	rs           *StateV3
	upd          *Update4ReadWriter
	trace        bool
	writeLists   map[string]*exec22.KvList
	accountPrevs map[string][]byte
	accountDels  map[string]*accounts.Account
	storagePrevs map[string][]byte
	codePrevs    map[string]uint64
}

func NewStateWriterBufferedV3(rs *StateV3) *StateWriterBufferedV3 {
	return &StateWriterBufferedV3{
		rs:         rs,
		trace:      true,
		writeLists: newWriteList(),
	}
}

func (w *StateWriterBufferedV3) SetTxNum(txNum uint64) {
	w.rs.domains.SetTxNum(txNum)
}

func (w *StateWriterBufferedV3) ResetWriteSet() {
	w.writeLists = newWriteList()
	w.accountPrevs = nil
	w.accountDels = nil
	w.storagePrevs = nil
	w.codePrevs = nil
}

func (w *StateWriterBufferedV3) WriteSet() map[string]*exec22.KvList {
	return w.writeLists
}

func (w *StateWriterBufferedV3) Updates() ([][]byte, []commitment.Update) {
	return w.upd.Updates()
}

func (w *StateWriterBufferedV3) Commit() ([]byte, error) {
	return w.upd.CommitmentUpdates()
}

func (w *StateWriterBufferedV3) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return w.accountPrevs, w.accountDels, w.storagePrevs, w.codePrevs
}

func (w *StateWriterBufferedV3) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addressBytes := address.Bytes()
	addr := hex.EncodeToString(addressBytes)
	//value := make([]byte, accounts.Seri())
	//account.EncodeForStorage(value)
	value := accounts.SerialiseV3(account)
	w.writeLists[kv.AccountDomain].Push(addr, value)
	w.upd.UpdateAccountData(address, original, account)

	if w.trace {
		fmt.Printf("[v3_buff] account [%v]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", addr, &account.Balance, account.Nonce, account.Root, account.CodeHash)
	}

	var prev []byte
	if original.Initialised {
		prev = accounts.SerialiseV3(original)
	}
	if w.accountPrevs == nil {
		w.accountPrevs = map[string][]byte{}
	}
	w.accountPrevs[string(addressBytes)] = prev
	return nil
}

func (w *StateWriterBufferedV3) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	addr := hex.EncodeToString(address.Bytes())
	w.writeLists[kv.CodeDomain].Push(addr, code)

	w.upd.UpdateAccountCode(address, incarnation, codeHash, code)
	if len(code) > 0 {
		if w.trace {
			fmt.Printf("[v3_buff] code [%v] => [%x] value: %x\n", addr, codeHash, code)
		}
		//w.writeLists[kv.PlainContractCode].Push(addr, code)
	}
	if w.codePrevs == nil {
		w.codePrevs = map[string]uint64{}
	}
	//w.codePrevs[addr] = incarnation
	return nil
}

func (w *StateWriterBufferedV3) DeleteAccount(address common.Address, original *accounts.Account) error {
	addr := hex.EncodeToString(address.Bytes())
	w.writeLists[kv.AccountDomain].Push(addr, nil)
	w.upd.DeleteAccount(address, original)
	if w.trace {
		fmt.Printf("[v3_buff] account [%x] deleted\n", address)
	}
	if original.Initialised {
		if w.accountDels == nil {
			w.accountDels = map[string]*accounts.Account{}
		}
		w.accountDels[addr] = original
	}
	return nil
}

func (w *StateWriterBufferedV3) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	composite := dbutils.PlainGenerateCompositeStorageKey(address[:], incarnation, key.Bytes())
	compositeS := hex.EncodeToString(composite)

	w.writeLists[kv.StorageDomain].Push(compositeS, value.Bytes())
	w.upd.WriteAccountStorage(address, incarnation, key, original, value)
	//w.rs.domains.WriteAccountStorage(address.Bytes(), key.Bytes(), value.Bytes(), original.Bytes())
	if w.trace {
		fmt.Printf("[v3_buff] storage [%x] [%x] => [%x]\n", address, key.Bytes(), value.Bytes())
	}

	if w.storagePrevs == nil {
		w.storagePrevs = map[string][]byte{}
	}
	w.storagePrevs[compositeS] = original.Bytes()
	return nil
}

func (w *StateWriterBufferedV3) CreateContract(address common.Address) error { return nil }

type StateReaderV3 struct {
	tx        kv.Tx
	txNum     uint64
	trace     bool
	rs        *StateV3
	composite []byte
	upd       *Update4ReadWriter

	discardReadList bool
	readLists       map[string]*exec22.KvList
}

func NewStateReaderV3(rs *StateV3) *StateReaderV3 {
	return &StateReaderV3{
		rs:        rs,
		trace:     true,
		readLists: newReadList(),
	}
}

func (r *StateReaderV3) SetUpd(rd *Update4ReadWriter) {
	r.upd = rd
}
func (r *StateWriterBufferedV3) SetUpd(rd *Update4ReadWriter) {
	r.upd = rd
}

func (r *StateReaderV3) DiscardReadList()                   { r.discardReadList = true }
func (r *StateReaderV3) SetTxNum(txNum uint64)              { r.txNum = txNum }
func (r *StateReaderV3) SetTx(tx kv.Tx)                     { r.tx = tx }
func (r *StateReaderV3) ReadSet() map[string]*exec22.KvList { return r.readLists }
func (r *StateReaderV3) SetTrace(trace bool)                { r.trace = trace }
func (r *StateReaderV3) ResetReadSet()                      { r.readLists = newReadList() }

func (r *StateReaderV3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addr := address.Bytes()

	a, err := r.upd.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if a == nil {
		acc := accounts.NewAccount()
		enc, err := r.rs.domains.LatestAccount(addr)
		if err != nil {
			return nil, err
		}
		if !r.discardReadList {
			// lifecycle of `r.readList` is less than lifecycle of `r.rs` and `r.tx`, also `r.rs` and `r.tx` do store data immutable way
			r.readLists[kv.AccountDomain].Push(string(addr), enc)
		}
		if len(enc) == 0 {
			return nil, nil
		}
		if err := accounts.DeserialiseV3(&acc, enc); err != nil {
			return nil, err
		}
		a = &acc
	}
	if !r.discardReadList {
		// lifecycle of `r.readList` is less than lifecycle of `r.rs` and `r.tx`, also `r.rs` and `r.tx` do store data immutable way
		r.readLists[kv.AccountDomain].Push(string(addr), accounts.SerialiseV3(a))
	}
	if r.trace {
		if a == nil {
			fmt.Printf("ReadAccountData [%x] => nil, txNum: %d\n", address, r.txNum)
		} else {
			fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], txNum: %d\n", address, a.Nonce, &a.Balance, a.CodeHash, r.txNum)
		}
	}
	return a, nil
}

func (r *StateReaderV3) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	composite := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := r.upd.ReadAccountStorage(address, incarnation, key)
	if enc == nil {
		enc, err = r.rs.domains.LatestStorage(address.Bytes(), key.Bytes())
	}
	if err != nil {
		return nil, err
	}

	if !r.discardReadList {
		r.readLists[kv.StorageDomain].Push(string(composite), enc)
	}
	if r.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [], txNum: %d\n", address, key.Bytes(), r.txNum)
		} else {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [%x], txNum: %d\n", address, key.Bytes(), enc, r.txNum)
		}
	}
	return enc, nil
}

func (r *StateReaderV3) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	addr := address.Bytes()
	enc, err := r.upd.ReadAccountCode(address, incarnation, codeHash)
	if enc == nil {
		enc, err = r.rs.domains.LatestCode(addr)
	}
	if err != nil {
		return nil, err
	}

	if !r.discardReadList {
		r.readLists[kv.CodeDomain].Push(string(addr), enc)
	}
	if r.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], txNum: %d\n", address, enc, r.txNum)
	}
	return enc, nil
}

func (r *StateReaderV3) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	enc, err := r.upd.ReadAccountCode(address, incarnation, codeHash)
	if enc == nil {
		enc, err = r.rs.domains.LatestCode(address.Bytes())
	}
	if err != nil {
		return 0, err
	}
	var sizebuf [8]byte
	binary.BigEndian.PutUint64(sizebuf[:], uint64(len(enc)))
	if !r.discardReadList {
		r.readLists[CodeSizeTable].Push(string(address[:]), sizebuf[:])
	}
	size := len(enc)
	if r.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d], txNum: %d\n", address, size, r.txNum)
	}
	return size, nil
}

func (r *StateReaderV3) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

var writeListPool = sync.Pool{
	New: func() any {
		return map[string]*exec22.KvList{
			kv.AccountDomain:     {},
			kv.StorageDomain:     {},
			kv.CodeDomain:        {},
			kv.PlainContractCode: {},
		}
	},
}

func newWriteList() map[string]*exec22.KvList {
	v := writeListPool.Get().(map[string]*exec22.KvList)
	for _, tbl := range v {
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	return v
}
func returnWriteList(v map[string]*exec22.KvList) {
	if v == nil {
		return
	}
	writeListPool.Put(v)
}

var readListPool = sync.Pool{
	New: func() any {
		return map[string]*exec22.KvList{
			kv.AccountDomain: {},
			kv.CodeDomain:    {},
			CodeSizeTable:    {},
			kv.StorageDomain: {},
		}
	},
}

func newReadList() map[string]*exec22.KvList {
	v := readListPool.Get().(map[string]*exec22.KvList)
	for _, tbl := range v {
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	return v
}
func returnReadList(v map[string]*exec22.KvList) {
	if v == nil {
		return
	}
	readListPool.Put(v)
}
