package smt_test

import (
	"context"
	"math/big"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/log/v3"

	"golang.org/x/sync/semaphore"
)

type Increments struct {
	Balance  *big.Int
	Nonce    *big.Int
	Address  string
	Bytecode string
	Storage  map[string]string
}

func TestValues(t *testing.T) {
	nodeKey := utils.ScalarToNodeKey(big.NewInt(11))
	path := nodeKey.GetPath()

	nodeKey2 := utils.RemoveKeyBits(nodeKey, 1)
	path2 := nodeKey2.GetPath()
	t.Logf("%+v -> %d", path, path[0])
	t.Logf("%+v -> %d", path2, path2[0])
}

func TestBatchInsert(t *testing.T) {
	ctx := context.Background()

	dbPath := "/tmp/erigon-db"
	os.RemoveAll(dbPath)

	dbOpts := mdbx.NewMDBX(log.Root()).Path(dbPath).Label(kv.ChainDB).GrowthStep(16 * datasize.MB).RoTxsLimiter(semaphore.NewWeighted(128))
	database, err := dbOpts.Open()
	if err != nil {
		t.Fatalf("Cannot create db %e", err)
	}

	migrator := migrations.NewMigrator(kv.ChainDB)
	if err := migrator.VerifyVersion(database); err != nil {
		t.Fatalf("Cannot verify db version %e", err)
	}
	if err = migrator.Apply(database, dbPath); err != nil {
		t.Fatalf("Cannot migrate db %e", err)
	}

	// if err := database.Update(context.Background(), func(tx kv.RwTx) (err error) {
	// 	return params.SetErigonVersion(tx, "test")
	// }); err != nil {
	// 	t.Fatalf("Cannot update db")
	// }

	dbTransaction, err := database.BeginRw(ctx)
	if err != nil {
		t.Fatalf("Cannot craete db transaction")
	}

	db.CreateEriDbBuckets(dbTransaction)
	smtDb := db.NewEriDb(dbTransaction)

	s := smt.NewSMT(smtDb)
	initialTreeSize := int64(1000000)
	keys := []utils.NodeKey{}
	kvMap := map[utils.NodeKey]utils.NodeValue8{}
	rand.Seed(1)
	for i := int64(0); i < initialTreeSize; i++ {
		kvMap[utils.ScalarToNodeKey(big.NewInt(rand.Int63()))] = utils.ScalarToNodeValue8(big.NewInt(rand.Int63()))
	}
	for k, v := range kvMap {
		if !v.IsZero() {
			s.Db.InsertAccountValue(k, v)
			keys = append(keys, k)
		}
	}
	startTime := time.Now()
	_, err = s.GenerateFromKVBulk("", keys)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	t.Logf("Batch insert %d values in %v\n", initialTreeSize, time.Since(startTime))

	incrementTreeSize := 1000
	increments := make([]*Increments, 0)
	for i := 0; i < incrementTreeSize; i++ {
		storage := make(map[string]string)
		addressBytes := make([]byte, 20)
		storageKeyBytes := make([]byte, 20)
		storageValueBytes := make([]byte, 20)
		rand.Read(addressBytes)

		for j := 0; j < 64; j++ {
			rand.Read(storageKeyBytes)
			rand.Read(storageValueBytes)
			storage[libcommon.BytesToAddress(storageKeyBytes).Hex()] = libcommon.BytesToAddress(storageValueBytes).Hex()
		}

		increments = append(increments, &Increments{
			Balance:  big.NewInt(rand.Int63()),
			Nonce:    big.NewInt(rand.Int63()),
			Address:  libcommon.BytesToAddress(addressBytes).Hex(),
			Bytecode: "0x60806040526004361061007b5760003560e01c80639623609d1161004e5780639623609d1461012b57806399a88ec41461013e578063f2fde38b1461015e578063f3b7dead1461017e57600080fd5b8063204e1c7a14610080578063715018a6146100c95780637eff275e146100e05780638da5cb5b14610100575b600080fd5b34801561008c57600080fd5b506100a061009b366004610608565b61019e565b60405173ffffffffffffffffffffffffffffffffffffffff909116815260200160405180910390f35b3480156100d557600080fd5b506100de610255565b005b3480156100ec57600080fd5b506100de6100fb36600461062c565b610269565b34801561010c57600080fd5b5060005473ffffffffffffffffffffffffffffffffffffffff166100a0565b6100de610139366004610694565b6102f7565b34801561014a57600080fd5b506100de61015936600461062c565b61038c565b34801561016a57600080fd5b506100de610179366004610608565b6103e8565b34801561018a57600080fd5b506100a0610199366004610608565b6104a4565b60008060008373ffffffffffffffffffffffffffffffffffffffff166040516101ea907f5c60da1b00000000000000000000000000000000000000000000000000000000815260040190565b600060405180830381855afa9150503d8060008114610225576040519150601f19603f3d011682016040523d82523d6000602084013e61022a565b606091505b50915091508161023957600080fd5b8080602001905181019061024d9190610788565b949350505050565b61025d6104f0565b6102676000610571565b565b6102716104f0565b6040517f8f28397000000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff8281166004830152831690638f283970906024015b600060405180830381600087803b1580156102db57600080fd5b505af11580156102ef573d6000803e3d6000fd5b505050505050565b6102ff6104f0565b6040517f4f1ef28600000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff841690634f1ef28690349061035590869086906004016107a5565b6000604051808303818588803b15801561036e57600080fd5b505af1158015610382573d6000803e3d6000fd5b5050505050505050565b6103946104f0565b6040517f3659cfe600000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff8281166004830152831690633659cfe6906024016102c1565b6103f06104f0565b73ffffffffffffffffffffffffffffffffffffffff8116610498576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152602660248201527f4f776e61626c653a206e6577206f776e657220697320746865207a65726f206160448201527f646472657373000000000000000000000000000000000000000000000000000060648201526084015b60405180910390fd5b6104a181610571565b50565b60008060008373ffffffffffffffffffffffffffffffffffffffff166040516101ea907ff851a44000000000000000000000000000000000000000000000000000000000815260040190565b60005473ffffffffffffffffffffffffffffffffffffffff163314610267576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820181905260248201527f4f776e61626c653a2063616c6c6572206973206e6f7420746865206f776e6572604482015260640161048f565b6000805473ffffffffffffffffffffffffffffffffffffffff8381167fffffffffffffffffffffffff0000000000000000000000000000000000000000831681178455604051919092169283917f8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e09190a35050565b73ffffffffffffffffffffffffffffffffffffffff811681146104a157600080fd5b60006020828403121561061a57600080fd5b8135610625816105e6565b9392505050565b6000806040838503121561063f57600080fd5b823561064a816105e6565b9150602083013561065a816105e6565b809150509250929050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b6000806000606084860312156106a957600080fd5b83356106b4816105e6565b925060208401356106c4816105e6565b9150604084013567ffffffffffffffff808211156106e157600080fd5b818601915086601f8301126106f557600080fd5b81358181111561070757610707610665565b604051601f82017fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0908116603f0116810190838211818310171561074d5761074d610665565b8160405282815289602084870101111561076657600080fd5b8260208601602083013760006020848301015280955050505050509250925092565b60006020828403121561079a57600080fd5b8151610625816105e6565b73ffffffffffffffffffffffffffffffffffffffff8316815260006020604081840152835180604085015260005b818110156107ef578581018301518582016060015282016107d3565b5060006060828601015260607fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0601f83011685010192505050939250505056fea2646970667358221220372a0e10eebea1b7fa43ae4c976994e6ed01d85eedc3637b83f01d3f06be442064736f6c63430008110033",
			Storage:  storage,
		})
	}

	startTime = time.Now()
	for _, increment := range increments {
		_, _ = s.SetAccountState(increment.Address, increment.Balance, increment.Nonce)
		if increment.Bytecode != "" {
			_ = s.SetContractBytecode(increment.Address, increment.Bytecode)
		}
		if len(increment.Storage) > 0 {
			_, _ = s.SetContractStorage(increment.Address, increment.Storage)
		}
	}
	t.Logf("Incremental insert %d values in %v (Accounts %v, Contract %v, Storage %v InsertSingle %v InsertRecalc %v)\n", incrementTreeSize, time.Since(startTime), smt.TimeAccount, smt.TimeContract, smt.TimeStorage, smt.TimeInsertSingle, smt.TimeInsertRecalc)

	dbTransaction.Commit()
	t.Cleanup(func() {
		database.Close()
		os.RemoveAll(dbPath)
	})
}
