package state_accessors

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// InitializeValidatorTable initializes the validator table in the database.
func InitializeStaticTables(tx kv.RwTx, state *state.CachingBeaconState) error {
	var err error
	if err = tx.ClearBucket(kv.ValidatorPublicKeys); err != nil {
		return err
	}
	if err = tx.ClearBucket(kv.HistoricalRoots); err != nil {
		return err
	}
	if err = tx.ClearBucket(kv.HistoricalSummaries); err != nil {
		return err
	}
	state.ForEachValidator(func(v solid.Validator, idx, total int) bool {
		key := base_encoding.Encode64ToBytes4(uint64(idx))
		if err = tx.Append(kv.ValidatorPublicKeys, key, v.PublicKeyBytes()); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	for i := 0; i < int(state.HistoricalRootsLength()); i++ {
		key := base_encoding.Encode64ToBytes4(uint64(i))
		root := state.HistoricalRoot(i)
		if err = tx.Append(kv.HistoricalRoots, key, root[:]); err != nil {
			return err
		}
	}
	var temp []byte
	for i := 0; i < int(state.HistoricalSummariesLength()); i++ {
		temp = temp[:0]
		key := base_encoding.Encode64ToBytes4(uint64(i))
		summary := state.HistoricalSummary(i)
		temp, err = summary.EncodeSSZ(temp)
		if err != nil {
			return err
		}
		if err = tx.Append(kv.HistoricalSummaries, key, temp); err != nil {
			return err
		}
	}
	return err
}

// IncrementValidatorTable increments the validator table in the database, by ignoring all the preverified indices.
func IncrementPublicKeyTable(tx kv.RwTx, state *state.CachingBeaconState, preverifiedIndicies uint64) error {
	valLength := state.ValidatorLength()
	for i := preverifiedIndicies; i < uint64(valLength); i++ {
		key := base_encoding.Encode64ToBytes4(i)
		pubKey, err := state.ValidatorPublicKey(int(i))
		if err != nil {
			return err
		}
		// We put as there could be reorgs and thus some of overwriting
		if err := tx.Put(kv.ValidatorPublicKeys, key, pubKey[:]); err != nil {
			return err
		}
	}
	return nil
}

func IncrementHistoricalRootsTable(tx kv.RwTx, state *state.CachingBeaconState, preverifiedIndicies uint64) error {
	for i := preverifiedIndicies; i < uint64(state.HistoricalRootsLength()); i++ {
		key := base_encoding.Encode64ToBytes4(i)
		root := state.HistoricalRoot(int(i))
		if err := tx.Put(kv.HistoricalRoots, key, root[:]); err != nil {
			return err
		}
	}
	return nil
}

func IncrementHistoricalSummariesTable(tx kv.RwTx, state *state.CachingBeaconState, preverifiedIndicies uint64) error {
	var temp []byte
	var err error
	for i := preverifiedIndicies; i < uint64(state.HistoricalSummariesLength()); i++ {
		temp = temp[:0]
		key := base_encoding.Encode64ToBytes4(i)
		summary := state.HistoricalSummary(int(i))
		temp, err = summary.EncodeSSZ(temp)
		if err != nil {
			return err
		}
		if err = tx.Put(kv.HistoricalSummaries, key, temp); err != nil {
			return err
		}
	}
	return nil
}

func ReadPublicKeyByIndex(tx kv.Tx, index uint64) (libcommon.Bytes48, error) {
	var pks []byte
	var err error
	key := base_encoding.Encode64ToBytes4(index)
	if pks, err = tx.GetOne(kv.ValidatorPublicKeys, key); err != nil {
		return libcommon.Bytes48{}, err
	}
	var ret libcommon.Bytes48
	copy(ret[:], pks)
	return ret, err
}

func GetStateProcessingProgress(tx kv.Tx) (uint64, error) {
	progressByytes, err := tx.GetOne(kv.StatesProcessingProgress, kv.StatesProcessingKey)
	if err != nil {
		return 0, err
	}
	if len(progressByytes) == 0 {
		return 0, nil
	}
	return base_encoding.Decode64FromBytes4(progressByytes), nil
}

func SetStateProcessingProgress(tx kv.RwTx, progress uint64) error {
	return tx.Put(kv.StatesProcessingProgress, kv.StatesProcessingKey, base_encoding.Encode64ToBytes4(progress))
}
