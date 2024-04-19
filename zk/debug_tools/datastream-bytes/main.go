package main

import (
	"flag"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"context"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/core/rawdb"
	dstypes "github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"fmt"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

var dataDir = ""
var batchNum = 0
var chainId = 1

func main() {
	flag.StringVar(&dataDir, "dataDir", "", "data directory")
	flag.IntVar(&batchNum, "batchNum", 0, "batch number")
	flag.IntVar(&chainId, "chainId", 1, "chain id")
	flag.Parse()

	db := mdbx.MustOpen(dataDir)
	defer db.Close()

	var streamBytes []byte

	err := db.View(context.Background(), func(tx kv.Tx) error {
		hermezDb := hermez_db.NewHermezDbReader(tx)

		streamServer := server.NewDataStreamServer(nil, uint64(chainId), server.StandardOperationMode)

		blocks, err := hermezDb.GetL2BlockNosByBatch(uint64(batchNum))
		if err != nil {
			return err
		}

		if len(blocks) == 0 {
			return fmt.Errorf("no blocks found for batch %d", batchNum)
		}

		lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0]-1)
		if err != nil {
			return err
		}

		previousBatch := batchNum - 1

		for _, blockNumber := range blocks {
			block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
			if err != nil {
				return err
			}

			gerUpdates := []dstypes.GerUpdate{}

			sBytes, err := streamServer.CreateAndBuildStreamEntryBytes(block, hermezDb, lastBlock, uint64(batchNum), uint64(previousBatch), true, &gerUpdates)
			if err != nil {
				return err
			}
			streamBytes = append(streamBytes, sBytes...)
			lastBlock = block
			// we only put in the batch bookmark at the start of the stream data once
			previousBatch = batchNum
		}

		return nil
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("data:\n0x%x\n", streamBytes)
}
