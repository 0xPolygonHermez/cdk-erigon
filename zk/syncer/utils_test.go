package syncer

import (
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func Test_decodeEtrogSequenceBatchesCallData(t *testing.T) {
	input := decodeEtrogSequenceBatchesCallDataTestCases

	for _, tc := range input {
		data := common.FromHex(tc.Input)
		calldata, err := DecodeEtrogSequenceBatchesCallData(data)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, &tc.Expected, calldata)
	}
}

func Test_decodePreEtrogSequenceBatchesCallData(t *testing.T) {
	input := decodePreEtrogSequenceBatchesCallDataTestCases

	for _, tc := range input {
		data := common.FromHex(tc.Input)
		calldata, err := DecodePreEtrogSequenceBatchesCallData(data)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, &tc.Expected, calldata)
	}
}