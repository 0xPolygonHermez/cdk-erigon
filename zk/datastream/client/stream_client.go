package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/log/v3"
)

type StreamType uint64
type Command uint64

type EntityDefinition struct {
	Name       string
	StreamType StreamType
	Definition reflect.Type
}

const (
	versionProto         = 2 // converted to proto
	versionAddedBlockEnd = 3 // Added block end
	entryChannelSize     = 100000
)

var (
	// ErrFileEntryNotFound denotes error that is returned when the certain file entry is not found in the datastream
	ErrFileEntryNotFound = errors.New("file entry not found")
)

type StreamClient struct {
	ctx          context.Context
	server       string // Server address to connect IP:port
	version      int
	streamType   StreamType
	conn         net.Conn
	id           string        // Client id
	checkTimeout time.Duration // time to wait for data before reporting an error

	header *types.HeaderEntry

	// atomic
	lastWrittenTime      atomic.Int64
	streaming            atomic.Bool
	progress             atomic.Uint64
	stopReadingToChannel atomic.Bool

	// Channels
	entryChan chan interface{}

	// keeps track of the latest fork from the stream to assign to l2 blocks
	currentFork uint64
}

const (
	// StreamTypeSequencer represents a Sequencer stream
	StSequencer StreamType = 1

	// Packet types
	PtPadding = 0
	PtHeader  = 1    // Just for the header page
	PtData    = 2    // Data entry
	PtDataRsp = 0xfe // PtDataRsp is packet type for command response with data
	PtResult  = 0xff // Not stored/present in file (just for client command result)
)

// Creates a new client fo datastream
// server must be in format "url:port"
func NewClient(ctx context.Context, server string, version int, checkTimeout time.Duration, latestDownloadedForkId uint16) *StreamClient {
	c := &StreamClient{
		ctx:          ctx,
		checkTimeout: checkTimeout,
		server:       server,
		version:      version,
		streamType:   StSequencer,
		id:           "",
		entryChan:    make(chan interface{}, 100000),
		currentFork:  uint64(latestDownloadedForkId),
	}

	return c
}

func (c *StreamClient) IsVersion3() bool {
	return c.version >= versionAddedBlockEnd
}

func (c *StreamClient) GetEntryChan() *chan interface{} {
	return &c.entryChan
}

func (c *StreamClient) GetEntryNumberLimit() uint64 {
	return c.header.TotalEntries
}

// GetL2BlockByNumber queries the data stream by sending the L2 block start bookmark for the certain block number
// and streams the changes for that block (including the transactions).
// Note that this function is intended for on demand querying and it disposes the connection after it ends.
func (c *StreamClient) GetL2BlockByNumber(blockNum uint64) (fullBLock *types.FullL2Block, errorCode int, err error) {
	var (
		socketErr error = nil
		connected bool  = c.conn != nil
	)
	count := 0
	for {
		select {
		case <-c.ctx.Done():
			return nil, errorCode, fmt.Errorf("[Datastream client] Context done - stopping")

		default:
		}
		if count > 5 {
			return nil, -1, errors.New("failed to get the L2 block within 5 attempts")
		}
		if connected {
			if fullBLock, errorCode, err, socketErr = c.getL2BlockByNumber(blockNum); err != nil {
				return nil, errorCode, err
			}
			if socketErr == nil {
				break
			}
		}
		time.Sleep(1 * time.Second)
		connected = c.handleSocketError(socketErr)
		count++
	}

	return fullBLock, types.CmdErrOK, nil
}

func (c *StreamClient) getL2BlockByNumber(blockNum uint64) (l2Block *types.FullL2Block, errorCode int, err, socketErr error) {
	var isL2Block bool

	bookmark := types.NewBookmarkProto(blockNum, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
	bookmarkRaw, err := bookmark.Marshal()
	if err != nil {
		return nil, -1, err, nil
	}

	re, err := c.initiateDownloadBookmark(bookmarkRaw)
	if err != nil {
		errorCode := -1
		if re != nil {
			errorCode = int(re.ErrorNum)
		}
		return nil, errorCode, nil, err
	}

	for l2Block == nil {
		select {
		case <-c.ctx.Done():
			errorCode := -1
			if re != nil {
				errorCode = int(re.ErrorNum)
			}
			return l2Block, errorCode, nil, nil
		default:
		}

		parsedEntry, _, err := ReadParsedProto(c)
		if err != nil {
			return nil, -1, nil, err
		}

		l2Block, isL2Block = parsedEntry.(*types.FullL2Block)
		if isL2Block {
			break
		}
	}

	if l2Block.L2BlockNumber != blockNum {
		return nil, -1, fmt.Errorf("expected block number %d but got %d", blockNum, l2Block.L2BlockNumber), nil
	}

	return l2Block, types.CmdErrOK, nil, nil
}

// GetLatestL2Block queries the data stream by reading the header entry and based on total entries field,
// it retrieves the latest File entry that is of EntryTypeL2Block type.
// Note that this function is intended for on demand querying and it disposes the connection after it ends.
func (c *StreamClient) GetLatestL2Block() (l2Block *types.FullL2Block, err error) {
	var (
		socketErr error = nil
		connected bool  = c.conn != nil
	)
	count := 0
	for {
		select {
		case <-c.ctx.Done():
			return nil, fmt.Errorf("[Datastream client] Context done - stopping")
		default:
		}
		if count > 5 {
			return nil, errors.New("failed to get the latest L2 block within 5 attempts")
		}
		if connected {
			if l2Block, err, socketErr = c.getLatestL2Block(); err != nil {
				return nil, err
			}
			if socketErr == nil {
				break
			}
		}

		time.Sleep(1 * time.Second)
		connected = c.handleSocketError(socketErr)
		count++
	}
	return l2Block, nil
}

func (c *StreamClient) getLatestL2Block() (l2Block *types.FullL2Block, err, socketErr error) {
	h, err := c.GetHeader()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get header: %w", err)
	}

	latestEntryNum := h.TotalEntries - 1

	for l2Block == nil && latestEntryNum > 0 {
		if err := c.sendEntryCmdWrapper(latestEntryNum); err != nil {
			return nil, nil, err
		}

		entry, err := c.NextFileEntry()
		if err != nil {
			return nil, nil, err
		}

		if entry.EntryType == types.EntryTypeL2Block {
			if l2Block, err = types.UnmarshalL2Block(entry.Data); err != nil {
				return nil, err, nil
			}
		}

		latestEntryNum--
	}

	if latestEntryNum == 0 {
		return nil, errors.New("no block found"), nil
	}

	return l2Block, nil, nil
}

func (c *StreamClient) GetLastWrittenTimeAtomic() *atomic.Int64 {
	return &c.lastWrittenTime
}
func (c *StreamClient) GetStreamingAtomic() *atomic.Bool {
	return &c.streaming
}
func (c *StreamClient) GetProgressAtomic() *atomic.Uint64 {
	return &c.progress
}

// Opens a TCP connection to the server
func (c *StreamClient) Start() error {
	// Connect to server
	var err error
	c.conn, err = net.Dial("tcp", c.server)
	if err != nil {
		return fmt.Errorf("error connecting to server %s: %v", c.server, err)
	}

	c.id = c.conn.LocalAddr().String()

	return nil
}

func (c *StreamClient) Stop() {
	if c.conn == nil {
		return
	}
	if err := c.sendStopCmd(); err != nil {
		log.Warn(fmt.Sprintf("Failed to send the stop command to the data stream server: %s", err))
	}
	// c.conn.Close()
	// c.conn = nil
}

// Command header: Get status
// Returns the current status of the header.
// If started, terminate the connection.
func (c *StreamClient) GetHeader() (*types.HeaderEntry, error) {
	if err := c.sendHeaderCmd(); err != nil {
		return nil, fmt.Errorf("%s send header error: %v", c.id, err)
	}

	// Read packet
	packet, err := c.readBuffer(1)
	if err != nil {
		return nil, fmt.Errorf("%s read buffer: %v", c.id, err)
	}

	// Check packet type
	if packet[0] != PtResult {
		return nil, fmt.Errorf("%s error expecting result packet type %d and received %d", c.id, PtResult, packet[0])
	}

	// Read server result entry for the command
	r, err := c.readResultEntry(packet)
	if err != nil {
		return nil, fmt.Errorf("%s read result entry error: %v", c.id, err)
	}
	if err := r.GetError(); err != nil {
		return nil, fmt.Errorf("%s got Result error code %d: %v", c.id, r.ErrorNum, err)
	}

	// Read header entry
	h, err := c.readHeaderEntry()
	if err != nil {
		return nil, fmt.Errorf("%s read header entry error: %v", c.id, err)
	}

	c.header = h

	return h, nil
}

// sendEntryCmdWrapper sends CmdEntry command and reads packet type and decodes result entry.
func (c *StreamClient) sendEntryCmdWrapper(entryNum uint64) error {
	if err := c.sendEntryCmd(entryNum); err != nil {
		return err
	}

	if re, err := c.readPacketAndDecodeResultEntry(); err != nil {
		return fmt.Errorf("failed to retrieve the result entry: %w", err)
	} else if err := re.GetError(); err != nil {
		return err
	}

	return nil
}

func (c *StreamClient) ExecutePerFile(bookmark *types.BookmarkProto, function func(file *types.FileEntry) error) error {
	// Get header from server
	header, err := c.GetHeader()
	if err != nil {
		return fmt.Errorf("%s get header error: %v", c.id, err)
	}

	protoBookmark, err := bookmark.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal bookmark: %v", err)
	}

	if _, err := c.initiateDownloadBookmark(protoBookmark); err != nil {
		return err
	}
	count := uint64(0)
	logTicker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-logTicker.C:
			fmt.Println("Entries read count: ", count)
		default:
		}
		if header.TotalEntries == count {
			break
		}
		file, err := c.NextFileEntry()
		if err != nil {
			return fmt.Errorf("reading file entry: %v", err)
		}
		if err := function(file); err != nil {
			return fmt.Errorf("executing function: %v", err)

		}
		count++
	}

	return nil
}

func (c *StreamClient) clearEntryCHannel() {
	defer func() {
		for range c.entryChan {
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			log.Warn("[datastream_client] Channel is already closed")
		}
	}()

	close(c.entryChan)
}

// close old entry chan and read all elements before opening a new one
func (c *StreamClient) RenewEntryChannel() {
	c.clearEntryCHannel()
	c.entryChan = make(chan interface{}, entryChannelSize)
}

func (c *StreamClient) ReadAllEntriesToChannel() (err error) {
	var (
		socketErr error = nil
		connected bool  = c.conn != nil
	)
	count := 0
	for {
		select {
		case <-c.ctx.Done():
			return fmt.Errorf("[Datastream client] Context done - stopping")
		default:
		}
		if count > 5 {
			return errors.New("failed to read all entries within 5 attempts")
		}
		if connected {
			if err, socketErr = c.readAllEntriesToChannel(); err != nil {
				return err
			}
			if socketErr == nil {
				break
			}
		}

		time.Sleep(1 * time.Second)
		connected = c.handleSocketError(socketErr)
		count++
	}

	return nil
}

func (c *StreamClient) handleSocketError(socketErr error) bool {
	if socketErr != nil {
		log.Warn(fmt.Sprintf("Socket error: %s", socketErr))
	}
	if err := c.tryReConnect(); err != nil {
		log.Warn(fmt.Sprintf("Failed to reconnect the datastream client: %s", err))
		return false
	}

	c.RenewEntryChannel()

	return true
}

// reads entries to the end of the stream
// at end will wait for new entries to arrive
func (c *StreamClient) readAllEntriesToChannel() (err, socketErr error) {
	c.streaming.Store(true)
	c.stopReadingToChannel.Store(false)
	defer c.streaming.Store(false)

	var bookmark *types.BookmarkProto
	progress := c.progress.Load()
	if progress == 0 {
		bookmark = types.NewBookmarkProto(0, datastream.BookmarkType_BOOKMARK_TYPE_BATCH)
	} else {
		bookmark = types.NewBookmarkProto(progress+1, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
	}

	protoBookmark, err := bookmark.Marshal()
	if err != nil {
		return err, nil
	}

	// send start command
	if _, socketErr := c.initiateDownloadBookmark(protoBookmark); err != nil {
		return nil, socketErr
	}

	if err := c.readAllFullL2BlocksToChannel(); err != nil {
		return nil, fmt.Errorf("%s read full L2 blocks error: %v", c.id, err)
	}

	return nil, nil
}

// runs the prerequisites for entries download
func (c *StreamClient) initiateDownloadBookmark(bookmark []byte) (*types.ResultEntry, error) {
	// send CmdStartBookmark command
	if err := c.sendBookmarkCmd(bookmark, true); err != nil {
		return nil, err
	}

	re, err := c.afterStartCommand()
	if err != nil {
		return re, fmt.Errorf("after start command error: %v", err)
	}

	return re, nil
}

func (c *StreamClient) afterStartCommand() (*types.ResultEntry, error) {
	re, err := c.readPacketAndDecodeResultEntry()
	if err != nil {
		return nil, err
	}

	if err := re.GetError(); err != nil {
		return re, fmt.Errorf("got Result error code %d: %v", re.ErrorNum, err)
	}

	return re, nil
}

// reads all entries from the server and sends them to a channel
// sends the parsed FullL2Blocks with transactions to a channel
func (c *StreamClient) readAllFullL2BlocksToChannel() (err error) {
	readNewProto := true
	entryNum := uint64(0)
	parsedProto := interface{}(nil)
LOOP:
	for {
		select {
		default:
		case <-c.ctx.Done():
			log.Warn("[Datastream client] Context done - stopping")
			break LOOP
		}

		if c.stopReadingToChannel.Load() {
			break LOOP
		}

		if c.checkTimeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.checkTimeout))
		}

		if readNewProto {
			if parsedProto, entryNum, err = ReadParsedProto(c); err != nil {
				return err
			}
			readNewProto = false
		}
		c.lastWrittenTime.Store(time.Now().UnixNano())

		switch parsedProto := parsedProto.(type) {
		case *types.BookmarkProto:
			readNewProto = true
			continue
		case *types.BatchStart:
			c.currentFork = parsedProto.ForkId
		case *types.GerUpdate:
		case *types.BatchEnd:
		case *types.FullL2Block:
			parsedProto.ForkId = c.currentFork
			log.Trace("writing block to channel", "blockNumber", parsedProto.L2BlockNumber, "batchNumber", parsedProto.BatchNumber)
		default:
			return fmt.Errorf("unexpected entry type: %v", parsedProto)
		}
		select {
		case c.entryChan <- parsedProto:
			readNewProto = true
		default:
			time.Sleep(10 * time.Microsecond)
		}

		if c.header.TotalEntries == entryNum+1 {
			log.Trace("reached the end of the stream", "header_totalEntries", c.header.TotalEntries, "entryNum", entryNum)
			if err = c.sendStopCmd(); err != nil {
				return fmt.Errorf("failed to send the stop command: %v", err)
			}
			break LOOP
		}
	}

	return nil
}

func (c *StreamClient) tryReConnect() error {
	var err error
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Warn(fmt.Sprintf("failed to close the DS connection: %s", err))
			return err
		}
		c.conn = nil
	}
	if err = c.Start(); err != nil {
		log.Warn(fmt.Sprintf("failed to start the DS connection: %s", err))
	}

	return err
}

func (c *StreamClient) StopReadingToChannel() {
	c.stopReadingToChannel.Store(true)
}

type FileEntryIterator interface {
	NextFileEntry() (*types.FileEntry, error)
	GetEntryNumberLimit() uint64
}

func ReadParsedProto(iterator FileEntryIterator) (
	parsedEntry interface{},
	entryNum uint64,
	err error,
) {
	file, err := iterator.NextFileEntry()
	if err != nil {
		err = fmt.Errorf("read file entry error: %w", err)
		return
	}

	if file == nil {
		return
	}
	entryNum = file.EntryNum

	switch file.EntryType {
	case types.BookmarkEntryType:
		parsedEntry, err = types.UnmarshalBookmark(file.Data)
	case types.EntryTypeGerUpdate:
		parsedEntry, err = types.DecodeGerUpdateProto(file.Data)
	case types.EntryTypeBatchStart:
		parsedEntry, err = types.UnmarshalBatchStart(file.Data)
	case types.EntryTypeBatchEnd:
		parsedEntry, err = types.UnmarshalBatchEnd(file.Data)
	case types.EntryTypeL2Block:
		var l2Block *types.FullL2Block
		if l2Block, err = types.UnmarshalL2Block(file.Data); err != nil {
			return
		}

		txs := []types.L2TransactionProto{}

		var innerFile *types.FileEntry
		var l2Tx *types.L2TransactionProto
	LOOP:
		for {
			if innerFile, err = iterator.NextFileEntry(); err != nil {
				return
			}
			entryNum = innerFile.EntryNum
			if innerFile.IsL2Tx() {
				if l2Tx, err = types.UnmarshalTx(innerFile.Data); err != nil {
					return
				}
				txs = append(txs, *l2Tx)
			} else if innerFile.IsL2BlockEnd() {
				var l2BlockEnd *types.L2BlockEndProto
				if l2BlockEnd, err = types.UnmarshalL2BlockEnd(innerFile.Data); err != nil {
					return
				}
				if l2BlockEnd.GetBlockNumber() != l2Block.L2BlockNumber {
					err = fmt.Errorf("block end number (%d) not equal to block number (%d)", l2BlockEnd.GetBlockNumber(), l2Block.L2BlockNumber)
					return
				}
				break LOOP
			} else if innerFile.IsBookmark() {
				var bookmark *types.BookmarkProto
				if bookmark, err = types.UnmarshalBookmark(innerFile.Data); err != nil || bookmark == nil {
					return
				}
				if bookmark.BookmarkType() == datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK {
					break LOOP
				} else {
					err = fmt.Errorf("unexpected bookmark type inside block: %v", bookmark.Type())
					return
				}
			} else if innerFile.IsBatchEnd() {
				if _, err = types.UnmarshalBatchEnd(file.Data); err != nil {
					return
				}
				break LOOP
			} else {
				err = fmt.Errorf("unexpected entry type inside a block: %d", innerFile.EntryType)
				return
			}
			if entryNum == iterator.GetEntryNumberLimit() {
				break LOOP
			}
		}

		l2Block.L2Txs = txs
		parsedEntry = l2Block
		return
	case types.EntryTypeL2BlockEnd:
		log.Debug(fmt.Sprintf("retrieved EntryTypeL2BlockEnd: %+v", file))
		return
	case types.EntryTypeL2Tx:
		err = errors.New("unexpected L2 tx entry, found outside of block")
	default:
		err = fmt.Errorf("unexpected entry type: %d", file.EntryType)
	}

	return
}

// reads file bytes from socket and tries to parse them
// returns the parsed FileEntry
func (c *StreamClient) NextFileEntry() (file *types.FileEntry, err error) {
	// Read packet type
	packet, err := c.readBuffer(1)
	if err != nil {
		return file, fmt.Errorf("failed to read packet type: %v", err)
	}

	packetType := packet[0]
	// Check packet type
	if packetType == PtResult {
		// Read server result entry for the command
		r, err := c.readResultEntry(packet)
		if err != nil {
			return file, err
		}
		if err := r.GetError(); err != nil {
			return file, fmt.Errorf("got Result error code %d: %v", r.ErrorNum, err)
		}
		return file, nil
	} else if packetType != PtData && packetType != PtDataRsp {
		return file, fmt.Errorf("expected data packet type %d or %d and received %d", PtData, PtDataRsp, packetType)
	}

	// Read the rest of fixed size fields
	buffer, err := c.readBuffer(types.FileEntryMinSize - 1)
	if err != nil {
		return file, fmt.Errorf("error reading file bytes: %v", err)
	}

	if packetType != PtData {
		packet[0] = PtData
	}
	buffer = append(packet, buffer...)

	// Read variable field (data)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < types.FileEntryMinSize {
		return file, errors.New("error reading data entry: wrong data length")
	}

	// Read rest of the file data
	bufferAux, err := c.readBuffer(length - types.FileEntryMinSize)
	if err != nil {
		return file, fmt.Errorf("error reading file data bytes: %v", err)
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary data to data entry struct
	if file, err = types.DecodeFileEntry(buffer); err != nil {
		return file, fmt.Errorf("decode file entry error: %v", err)
	}

	if file.EntryType == types.EntryTypeNotFound {
		return file, ErrFileEntryNotFound
	}

	return
}

// reads header bytes from socket and tries to parse them
// returns the parsed HeaderEntry
func (c *StreamClient) readHeaderEntry() (h *types.HeaderEntry, err error) {

	// Read header stream bytes
	binaryHeader, err := c.readBuffer(types.HeaderSizePreEtrog)
	if err != nil {
		return h, fmt.Errorf("failed to read header bytes %v", err)
	}

	headLength := binary.BigEndian.Uint32(binaryHeader[1:5])
	if headLength == types.HeaderSize {
		// Read the rest of fixed size fields
		buffer, err := c.readBuffer(types.HeaderSize - types.HeaderSizePreEtrog)
		if err != nil {
			return h, fmt.Errorf("failed to read header bytes %v", err)
		}
		binaryHeader = append(binaryHeader, buffer...)
	}

	// Decode bytes stream to header entry struct
	if h, err = types.DecodeHeaderEntry(binaryHeader); err != nil {
		return h, fmt.Errorf("error decoding binary header: %v", err)
	}

	return
}

// reads result bytes and tries to parse them
// returns the parsed ResultEntry
func (c *StreamClient) readResultEntry(packet []byte) (re *types.ResultEntry, err error) {
	if len(packet) != 1 {
		return re, fmt.Errorf("expected packet size of 1, got: %d", len(packet))
	}

	// Read the rest of fixed size fields
	buffer, err := c.readBuffer(types.ResultEntryMinSize - 1)
	if err != nil {
		return re, fmt.Errorf("failed to read main result bytes %v", err)
	}
	buffer = append(packet, buffer...)

	// Read variable field (errStr)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < types.ResultEntryMinSize {
		return re, fmt.Errorf("%s Error reading result entry", c.id)
	}

	// read the rest of the result
	bufferAux, err := c.readBuffer(length - types.ResultEntryMinSize)
	if err != nil {
		return re, fmt.Errorf("failed to read result errStr bytes %v", err)
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary entry result
	if re, err = types.DecodeResultEntry(buffer); err != nil {
		return re, fmt.Errorf("decode result entry error: %v", err)
	}

	return re, nil
}

// readPacketAndDecodeResultEntry reads the packet from the connection and tries to decode the ResultEntry from it.
func (c *StreamClient) readPacketAndDecodeResultEntry() (*types.ResultEntry, error) {
	// Read packet
	packet, err := c.readBuffer(1)
	if err != nil {
		return nil, fmt.Errorf("read buffer error: %w", err)
	}

	// Read server result entry for the command
	r, err := c.readResultEntry(packet)
	if err != nil {
		return nil, fmt.Errorf("read result entry error: %w", err)
	}

	return r, nil
}

func (c *StreamClient) readBuffer(amount uint32) ([]byte, error) {
	c.resetReadTimeout()
	return readBuffer(c.conn, amount)
}

type writeType uint8

func (c *StreamClient) writeToConn(data interface{}) error {
	c.resetWriteTimeout()
	switch parsed := data.(type) {
	case []byte:
		return writeBytesToConn(c.conn, parsed)
	case uint32:
		return writeFullUint32ToConn(c.conn, parsed)
	case uint64:
		return writeFullUint64ToConn(c.conn, parsed)
	default:
		return errors.New("unexpected write type")
	}
}

func (c *StreamClient) resetWriteTimeout() {
	c.conn.SetWriteDeadline(time.Now().Add(c.checkTimeout))
}

func (c *StreamClient) resetReadTimeout() {
	c.conn.SetReadDeadline(time.Now().Add(c.checkTimeout))
}
