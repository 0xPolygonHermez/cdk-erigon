package client

import (
	"fmt"
)

const (
	// Commands
	CmdUnknown Command = iota
	CmdStart
	CmdStop
	CmdHeader
	CmdStartBookmark // CmdStartBookmark for the start from bookmark TCP client command
	CmdEntry         // CmdEntry for the get entry TCP client command
	CmdBookmark      // CmdBookmark for the get bookmark TCP client command
)

// sendHeaderCmd sends the header command to the server.
func (c *StreamClient) sendHeaderCmd() error {
	err := c.sendCommand(CmdHeader)
	if err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}

// sendBookmarkCmd sends either CmdStartBookmark or CmdBookmark for the provided bookmark value.
// In case streaming parameter is set to true, the CmdStartBookmark is sent, otherwise the CmdBookmark.
func (c *StreamClient) sendBookmarkCmd(bookmark []byte, streaming bool) error {
	// in case we want to stream the entries, CmdStartBookmark is sent, otherwise CmdBookmark command
	command := CmdStartBookmark
	if !streaming {
		command = CmdBookmark
	}

	// Send the command
	if err := c.sendCommand(command); err != nil {
		return err
	}

	// Send bookmark length
	if err := c.writeToConn(uint32(len(bookmark))); err != nil {
		return err
	}

	// Send the bookmark to retrieve
	return c.writeToConn(bookmark)
}

// sendStartCmd sends a start command to the server, indicating
// that the client wishes to start streaming from the given entry number.
func (c *StreamClient) sendStartCmd(from uint64) error {
	err := c.sendCommand(CmdStart)
	if err != nil {
		return err
	}

	// Send starting/from entry number
	return c.writeToConn(from)
}

// sendEntryCmd sends the get data stream entry by number command to a TCP connection
func (c *StreamClient) sendEntryCmd(entryNum uint64) error {
	// Send CmdEntry command
	if err := c.sendCommand(CmdEntry); err != nil {
		return err
	}

	// Send entry number
	return c.writeToConn(entryNum)
}

// sendHeaderCmd sends the header command to the server.
func (c *StreamClient) sendStopCmd() error {
	err := c.sendCommand(CmdStop)
	if err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}

func (c *StreamClient) sendCommand(cmd Command) error {

	// Send command
	if err := c.writeToConn(uint64(cmd)); err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	// Send stream type
	if err := c.writeToConn(uint64(c.streamType)); err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}
