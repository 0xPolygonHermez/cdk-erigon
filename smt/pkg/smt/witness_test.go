package smt_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"gotest.tools/v3/assert"
)

func prepareSMT(t *testing.T) (*smt.SMT, *trie.RetainList) {
	contract := libcommon.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	balance := uint256.NewInt(1000000000)
	sKey := libcommon.HexToHash("0x5")
	sVal := uint256.NewInt(0xdeadbeef)

	_, tx := memdb.NewTestTx(t)

	tds := state.NewTrieDbState(libcommon.Hash{}, tx, 0, state.NewPlainStateReader(tx))

	w := tds.TrieStateWriter()

	intraBlockState := state.New(tds)

	tds.StartNewBuffer()

	tds.SetResolveReads(false)

	intraBlockState.CreateAccount(contract, true)

	code := []byte{0x01, 0x02, 0x03, 0x04}
	intraBlockState.SetCode(contract, code)
	intraBlockState.AddBalance(contract, balance)
	intraBlockState.SetState(contract, &sKey, *sVal)

	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tds.TrieStateWriter()); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if err := intraBlockState.CommitBlock(&chain.Rules{}, w); err != nil {
		t.Errorf("error committing block: %v", err)
	}

	rl, err := tds.ResolveSMTRetainList()

	if err != nil {
		t.Errorf("error resolving state trie: %v", err)
	}

	memdb := db.NewMemDb()

	smtTrie := smt.NewSMT(memdb)

	smtTrie.SetAccountState(contract.String(), balance.ToBig(), uint256.NewInt(1).ToBig())
	smtTrie.SetContractBytecode(contract.String(), hex.EncodeToString(code))
	err = memdb.AddCode(code)

	if err != nil {
		t.Errorf("error adding code to memdb: %v", err)
	}

	storage := make(map[string]string, 0)

	// TODO : 0xsharma : change iterations back to 100
	for i := 0; i < 100; i++ {
		k := libcommon.HexToHash(fmt.Sprintf("0x%d", i))
		storage[k.String()] = k.String()
	}

	storage[sKey.String()] = sVal.String()

	smtTrie.SetContractStorage(contract.String(), storage, nil)

	return smtTrie, rl
}

func findNode(t *testing.T, w *trie.Witness, addr libcommon.Address, storageKey libcommon.Hash, nodeType int) []byte {
	for _, operator := range w.Operators {
		switch op := operator.(type) {
		case *trie.OperatorSMTLeafValue:
			if op.NodeType == uint8(nodeType) && bytes.Equal(op.Address, addr.Bytes()) {
				if nodeType == utils.SC_STORAGE {
					if bytes.Equal(op.StorageKey, storageKey.Bytes()) {
						return op.Value
					}
				} else {
					return op.Value
				}
			}
		}
	}

	return nil
}

// TestWitnessToSMT tests that the SMT built from a witness matches the original SMT
func TestWitnessToSMT(t *testing.T) {
	smtTrie, rl := prepareSMT(t)

	witness, err := smt.BuildWitness(smtTrie, rl, context.Background())
	if err != nil {
		t.Errorf("error building witness: %v", err)
	}

	newSMT, err := smt.BuildSMTfromWitness(witness)
	if err != nil {
		t.Errorf("error building SMT from witness: %v", err)
	}

	root, err := newSMT.Db.GetLastRoot()
	if err != nil {
		t.Errorf("error getting last root: %v", err)
	}

	// newSMT.Traverse(context.Background(), root, func(prefix []byte, k utils.NodeKey, v utils.NodeValue12) (bool, error) {
	// 	fmt.Printf("[After] path: %v, hash: %x\n", prefix, libcommon.BigToHash(k.ToBigInt()))
	// 	return true, nil
	// })

	expectedRoot, err := smtTrie.Db.GetLastRoot()
	if err != nil {
		t.Errorf("error getting last root: %v", err)
	}

	// assert that the roots are the same
	if expectedRoot.Cmp(root) != 0 {
		t.Errorf(fmt.Sprintf("SMT root mismatch, expected %x, got %x", expectedRoot.Bytes(), root.Bytes()))
	}
}

// TestWitnessToSMTStateReader tests that the SMT built from a witness matches the state
func TestWitnessToSMTStateReader(t *testing.T) {
	smtTrie, rl := prepareSMT(t)

	expectedRoot, err := smtTrie.Db.GetLastRoot()
	if err != nil {
		t.Errorf("error getting last root: %v", err)
	}

	witness, err := smt.BuildWitness(smtTrie, rl, context.Background())
	if err != nil {
		t.Errorf("error building witness: %v", err)
	}

	newSMT, err := smt.BuildSMTfromWitness(witness)
	if err != nil {
		t.Errorf("error building SMT from witness: %v", err)
	}
	root, err := newSMT.Db.GetLastRoot()
	if err != nil {
		t.Errorf("error building SMT from witness: %v", err)
	}

	if expectedRoot.Cmp(root) != 0 {
		t.Errorf(fmt.Sprintf("SMT root mismatch, expected %x, got %x", expectedRoot.Bytes(), root.Bytes()))
	}

	contract := libcommon.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")

	expectedAcc, _ := smtTrie.ReadAccountData(contract)
	newAcc, _ := newSMT.ReadAccountData(contract)

	expectedAccCode, _ := smtTrie.ReadAccountCode(contract, 0, expectedAcc.CodeHash)
	newAccCode, _ := newSMT.ReadAccountCode(contract, 0, newAcc.CodeHash)
	expectedAccCodeSize, _ := smtTrie.ReadAccountCodeSize(contract, 0, expectedAcc.CodeHash)
	newAccCodeSize, _ := newSMT.ReadAccountCodeSize(contract, 0, newAcc.CodeHash)

	// assert that the account data is the same
	assert.DeepEqual(t, expectedAcc, newAcc)

	// assert that the account code is the same
	if !bytes.Equal(expectedAccCode, newAccCode) {
		t.Error("Account Code Mismatch")
	}

	// assert that the account code size is the same
	assert.Equal(t, expectedAccCodeSize, newAccCodeSize)
}

func TestSMTWitnessRetainList(t *testing.T) {
	smtTrie, rl := prepareSMT(t)

	contract := libcommon.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	sKey := libcommon.HexToHash("0x5")
	sVal := uint256.NewInt(0xdeadbeef)

	witness, err := smt.BuildWitness(smtTrie, rl, context.Background())

	if err != nil {
		t.Errorf("error building witness: %v", err)
	}

	foundCode := findNode(t, witness, contract, libcommon.Hash{}, utils.SC_CODE)
	foundBalance := findNode(t, witness, contract, libcommon.Hash{}, utils.KEY_BALANCE)
	foundNonce := findNode(t, witness, contract, libcommon.Hash{}, utils.KEY_NONCE)
	foundStorage := findNode(t, witness, contract, sKey, utils.SC_STORAGE)

	if foundCode == nil || foundBalance == nil || foundNonce == nil || foundStorage == nil {
		t.Errorf("witness does not contain all expected operators")
	}

	if !bytes.Equal(foundStorage, sVal.Bytes()) {
		t.Errorf("witness contains unexpected storage value")
	}
}

func TestSMTWitnessRetainListEmptyVal(t *testing.T) {
	smtTrie, rl := prepareSMT(t)

	contract := libcommon.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	balance := uint256.NewInt(1000000000)
	sKey := libcommon.HexToHash("0x5")

	// Set nonce to 0
	smtTrie.SetAccountState(contract.String(), balance.ToBig(), uint256.NewInt(0).ToBig())

	witness, err := smt.BuildWitness(smtTrie, rl, context.Background())

	if err != nil {
		t.Errorf("error building witness: %v", err)
	}

	foundCode := findNode(t, witness, contract, libcommon.Hash{}, utils.SC_CODE)
	foundBalance := findNode(t, witness, contract, libcommon.Hash{}, utils.KEY_BALANCE)
	foundNonce := findNode(t, witness, contract, libcommon.Hash{}, utils.KEY_NONCE)
	foundStorage := findNode(t, witness, contract, sKey, utils.SC_STORAGE)

	if foundCode == nil || foundBalance == nil || foundStorage == nil {
		t.Errorf("witness does not contain all expected operators")
	}

	// Nonce should not be in witness
	if foundNonce != nil {
		t.Errorf("witness contains unexpected operator")
	}
}
