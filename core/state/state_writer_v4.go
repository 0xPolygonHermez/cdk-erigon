package state

import (
	"context"

	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var _ StateWriter = (*WriterV4)(nil)

type WriterV4 struct {
	tx      kv.TemporalTx
	domains *state.SharedDomains
}

func NewWriterV4(tx kv.TemporalTx, domains *state.SharedDomains) *WriterV4 {
	return &WriterV4{tx: tx, domains: domains}
}

func (w *WriterV4) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	value, origValue := accounts.SerialiseV3(account), accounts.SerialiseV3(original)
	w.domains.SetTx(w.tx.(kv.RwTx))
	//fmt.Printf("v4 account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash)
	return w.domains.UpdateAccountData(address.Bytes(), value, origValue)
}

func (w *WriterV4) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	w.domains.SetTx(w.tx.(kv.RwTx))
	return w.domains.UpdateAccountCode(address.Bytes(), code)
}

func (w *WriterV4) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	w.domains.SetTx(w.tx.(kv.RwTx))
	//fmt.Printf("v4 delete %x\n", address)
	return w.domains.DeleteAccount(address.Bytes(), accounts.SerialiseV3(original))
}

func (w *WriterV4) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	w.domains.SetTx(w.tx.(kv.RwTx))
	return w.domains.WriteAccountStorage(address.Bytes(), key.Bytes(), value.Bytes(), original.Bytes())
}

func (w *WriterV4) CreateContract(address libcommon.Address) (err error) {
	w.domains.SetTx(w.tx.(kv.RwTx))
	err = w.domains.IterateStoragePrefix(w.tx, address[:], func(k, v []byte) {
		if err != nil {
			return
		}
		err = w.domains.WriteAccountStorage(k, nil, nil, v)
	})
	if err != nil {
		return err
	}

	return nil
}
func (w *WriterV4) WriteChangeSets() error { return nil }
func (w *WriterV4) WriteHistory() error    { return nil }

func (w *WriterV4) Commitment(ctx context.Context, saveStateAfter, trace bool) (rootHash []byte, err error) {
	w.domains.SetTx(w.tx.(kv.RwTx))
	return w.domains.ComputeCommitment(ctx, saveStateAfter, trace)
}
func (w *WriterV4) Reset() {
	//w.domains.Commitment.Reset()
	w.domains.ClearRam(true)
}
