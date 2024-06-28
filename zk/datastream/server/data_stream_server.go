package server

import (
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

type BookmarkType byte

const (
	EtrogBatchNumber = 7
)

type DataStreamServer struct {
	stream  *datastreamer.StreamServer
	chainId uint64
}

type DataStreamEntry interface {
	EntryType() types.EntryType
	Bytes(bigEndian bool) []byte
}

type DataStreamEntryProto interface {
	Marshal() ([]byte, error)
	Type() types.EntryType
}

func NewDataStreamServer(stream *datastreamer.StreamServer, chainId uint64) *DataStreamServer {
	return &DataStreamServer{
		stream:  stream,
		chainId: chainId,
	}
}

func (srv *DataStreamServer) GetChainId() uint64 {
	return srv.chainId
}

type DataStreamEntries struct {
	index   int
	entries []DataStreamEntryProto
}

func (d *DataStreamEntries) Add(entry DataStreamEntryProto) {
	d.entries[d.index] = entry
	d.index++
}

func (d *DataStreamEntries) AddMany(entries []DataStreamEntryProto) {
	for _, e := range entries {
		d.Add(e)
	}
}

func (d *DataStreamEntries) Entries() []DataStreamEntryProto {
	return d.entries
}

func NewDataStreamEntries(size int) *DataStreamEntries {
	return &DataStreamEntries{
		entries: make([]DataStreamEntryProto, size),
	}
}

func (srv *DataStreamServer) CommitEntriesToStreamProto(entries []DataStreamEntryProto) error {
	for _, entry := range entries {
		entryType := entry.Type()

		em, err := entry.Marshal()
		if err != nil {
			return err
		}

		if entryType == types.BookmarkEntryType {
			_, err = srv.stream.AddStreamBookmark(em)
			if err != nil {
				return err
			}
		} else {
			_, err = srv.stream.AddStreamEntry(datastreamer.EntryType(entryType), em)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func createBlockWithBatchCheckStreamEntriesProto(
	chainId uint64,
	block *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	tx kv.Tx,
	lastBlock *eritypes.Block,
	batchNumber uint64,
	lastBatchNumber uint64,
	gers []types.GerUpdateProto,
	l1InfoTreeMinTimestamps map[uint64]uint64,
	isBatchEnd bool,
	transactionsToIncludeByIndex []int, // passing nil here will include all transactions in the blocks
) ([]DataStreamEntryProto, error) {
	// we might have a series of empty batches to account for, so we need to know the gap
	batchGap := batchNumber - lastBatchNumber
	isBatchStart := batchGap > 0

	// filter transactions by indexes that should be included
	filteredTransactions := filterTransactionByIndexes(block.Transactions(), transactionsToIncludeByIndex)

	blockNum := block.NumberU64()

	entryCount := 2                         // l2 block bookmark + l2 block
	entryCount += len(filteredTransactions) // transactions
	entryCount += len(gers)

	var err error

	if isBatchStart {
		// we will add in a batch bookmark and a batch start entry
		entryCount += 2

		// a gap of 1 is normal but if greater than we need to account for the empty batches which will each
		// have a batch bookmark, batch start and batch end
		entryCount += int(3 * (batchGap - 1))
	}

	if isBatchEnd {
		entryCount++
	}

	entries := NewDataStreamEntries(entryCount)

	// batch start
	// BATCH BOOKMARK
	if isBatchStart {
		var entriesProto []DataStreamEntryProto
		if entriesProto, err = createBatchStartEntriesProto(reader, tx, batchNumber, lastBatchNumber, batchGap, chainId, block.Root(), gers); err != nil {
			return nil, err
		}
		entries.AddMany(entriesProto)
	}

	forkId, err := reader.GetForkId(batchNumber)
	if err != nil {
		return nil, err
	}

	deltaTimestamp := block.Time() - lastBlock.Time()
	if blockNum == 1 {
		deltaTimestamp = block.Time()
		l1InfoTreeMinTimestamps[0] = 0
	}

	blockEntries, err := createFullBlockStreamEntriesProto(reader, tx, block, filteredTransactions, forkId, deltaTimestamp, batchNumber, l1InfoTreeMinTimestamps)
	if err != nil {
		return nil, err
	}
	entries.AddMany(blockEntries.Entries())

	if isBatchEnd {
		var batchEndEntries []DataStreamEntryProto
		if batchEndEntries, err = addBatchEndEntriesProto(reader, tx, batchNumber, lastBatchNumber, batchGap, block.Root(), gers); err != nil {
			return nil, err
		}
		entries.AddMany(batchEndEntries)
	}

	return entries.Entries(), nil
}

func createFullBlockStreamEntriesProto(
	reader *hermez_db.HermezDbReader,
	tx kv.Tx,
	block *eritypes.Block,
	filteredTransactions eritypes.Transactions,
	forkId,
	deltaTimestamp,
	batchNumber uint64,
	l1InfoTreeMinTimestamps map[uint64]uint64,
) (*DataStreamEntries, error) {
	entries := NewDataStreamEntries(len(filteredTransactions) + 2)
	blockNum := block.NumberU64()
	// L2 BLOCK BOOKMARK
	entries.Add(newL2BlockBookmarkEntryProto(blockNum))

	ger, err := reader.GetBlockGlobalExitRoot(blockNum)
	if err != nil {
		return nil, err
	}
	l1BlockHash, err := reader.GetBlockL1BlockHash(blockNum)
	if err != nil {
		return nil, err
	}

	l1InfoIndex, err := reader.GetBlockL1InfoTreeIndex(blockNum)
	if err != nil {
		return nil, err
	}

	if l1InfoIndex > 0 {
		// get the l1 info data, so we can add the min timestamp to the map
		l1Info, err := reader.GetL1InfoTreeUpdate(l1InfoIndex)
		if err != nil {
			return nil, err
		}
		if l1Info != nil {
			l1InfoTreeMinTimestamps[l1InfoIndex] = l1Info.Timestamp
		}
	}

	blockInfoRoot, err := reader.GetBlockInfoRoot(blockNum)
	if err != nil {
		return nil, err
	}

	// L2 BLOCK
	entries.Add(newL2BlockProto(block, block.Hash().Bytes(), batchNumber, ger, uint32(deltaTimestamp), uint32(l1InfoIndex), l1BlockHash, l1InfoTreeMinTimestamps[l1InfoIndex], blockInfoRoot))

	var transaction DataStreamEntryProto
	isEtrog := forkId <= EtrogBatchNumber
	for _, tx := range filteredTransactions {
		if transaction, err = createTransactionEntryProto(reader, tx, blockNum, isEtrog); err != nil {
			return nil, err
		}
		entries.Add(transaction)
	}

	return entries, nil
}

func createTransactionEntryProto(
	reader *hermez_db.HermezDbReader,
	tx eritypes.Transaction,
	blockNum uint64,
	isEtrog bool,
) (DataStreamEntryProto, error) {
	effectiveGasPricePercentage, err := reader.GetEffectiveGasPricePercentage(tx.Hash())
	if err != nil {
		return nil, err
	}

	var intermediateRoot libcommon.Hash
	if isEtrog {
		intermediateRoot, err = reader.GetIntermediateTxStateRoot(blockNum, tx.Hash())
		if err != nil {
			return nil, err
		}
	}

	// TRANSACTION

	txProto, err := newTransactionProto(effectiveGasPricePercentage, intermediateRoot, tx, blockNum)
	if err != nil {
		return nil, err
	}

	return txProto, nil
}

func CreateAndBuildStreamEntryBytesProto(
	chainId uint64,
	block *eritypes.Block,
	reader *hermez_db.HermezDbReader,
	tx kv.Tx,
	lastBlock *eritypes.Block,
	batchNumber uint64,
	lastBatchNumber uint64,
	l1InfoTreeMinTimestamps map[uint64]uint64,
	isBatchEnd bool,
	transactionsToIncludeByIndex []int, // passing nil here will include all transactions in the blocks
) ([]byte, error) {
	gersInBetween, err := reader.GetBatchGlobalExitRootsProto(lastBatchNumber, batchNumber)
	if err != nil {
		return nil, err
	}

	entries, err := createBlockWithBatchCheckStreamEntriesProto(chainId, block, reader, tx, lastBlock, batchNumber, lastBatchNumber, gersInBetween, l1InfoTreeMinTimestamps, isBatchEnd, transactionsToIncludeByIndex)
	if err != nil {
		return nil, err
	}

	var result []byte
	for _, entry := range entries {
		b, err := encodeEntryToBytesProto(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, b...)
	}

	return result, nil
}

func (srv *DataStreamServer) GetHighestBlockNumber() (uint64, error) {
	header := srv.stream.GetHeader()

	if header.TotalEntries == 0 {
		return 0, nil
	}

	//find end block entry to delete from it onward
	entryNum := header.TotalEntries - 1
	var err error
	var entry datastreamer.FileEntry
	for {
		entry, err = srv.stream.GetEntry(entryNum)
		if err != nil {
			return 0, err
		}
		if entry.Type == datastreamer.EntryType(2) {
			break
		}
		entryNum -= 1
	}

	l2Block, err := types.UnmarshalL2Block(entry.Data)
	if err != nil {
		return 0, err
	}

	return l2Block.L2BlockNumber, nil
}

// must be done on offline server
// finds the position of the endBlock entry for the given number
// and unwinds the datastream file to it
func (srv *DataStreamServer) UnwindToBlock(blockNumber uint64) error {
	// check if server is online

	// find blockend entry
	bookmark := types.NewBookmarkProto(blockNumber, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
	marshalled, err := bookmark.Marshal()
	if err != nil {
		return err
	}
	entryNum, err := srv.stream.GetBookmark(marshalled)
	if err != nil {
		return err
	}

	//find end block entry to delete from it onward
	for {
		entry, err := srv.stream.GetEntry(entryNum)
		if err != nil {
			return err
		}
		if entry.Type == datastreamer.EntryType(3) {
			break
		}
		entryNum -= 1
	}

	return srv.stream.TruncateFile(entryNum + 1)
}
