// Copyright (C) 2017 Kale Blankenship. All rights reserved.
// This software may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details

package tftp // import "pack.ag/tftp"

import (
	"bytes"
	"errors"
	"io"
	"net"
	"slices"
	"strconv"
	"time"

	"pack.ag/tftp/netascii"
)

const (
	defaultPort              = "69"
	defaultMode              = ModeOctet
	defaultUDPNet            = "udp"
	defaultTCPNet            = "tcp"
	defaultTimeout           = time.Second * 60
	defaultBlksize           = 55
	defaultHdrsize           = sizeofHdr
	defaultPktsize           = defaultHdrsize + defaultBlksize
	defaultWindowsize        = 1
	defaultRetransmit        = 10
	defaultTimeOutMultiplier = 1
)

// All connections will use these options unless overridden.
var defaultOptions = map[string]string{
	// optTransferSize: "0", // Enable tsize
	// optMode: string(defaultMode),
}

// newConn starts listening on a system assigned port and returns an initialized conn
//
// udpNet is one of "udp", "udp4", or "udp6"
// addr is the address of the target server
// tcpConn is the TCP socket of an external application, if specified
func newConn(udpNet string, addr *net.UDPAddr, tcpConn *net.TCPConn, toMulti int) (*conn, error) {
	// Start listening, an empty UDPAddr will cause the system to assign a port
	netConn, err := net.ListenUDP(udpNet, &net.UDPAddr{})
	if err != nil {
		return nil, wrapError(err, "network listen failed")
	}

	c := &conn{
		log:               newLogger(addr.String()),
		remoteAddr:        addr,
		netConn:           netConn,
		blksize:           defaultBlksize,
		timeout:           defaultTimeout,
		windowsize:        defaultWindowsize,
		retransmit:        defaultRetransmit,
		mode:              defaultMode,
		tcpConn:           tcpConn,
		timeoutMultiplier: toMulti,
	}
	c.rx.buf = make([]byte, defaultPktsize)

	return c, nil
}

func newSinglePortConn(addr *net.UDPAddr, netConn *net.UDPConn, tcpConn *net.TCPConn, reqChan chan []byte, toMulti int) *conn {
	return &conn{
		log:               newLogger(addr.String()),
		remoteAddr:        addr,
		blksize:           defaultBlksize,
		timeout:           defaultTimeout,
		windowsize:        defaultWindowsize,
		retransmit:        defaultRetransmit,
		mode:              defaultMode,
		buf:               make([]byte, defaultPktsize),
		reqChan:           reqChan,
		netConn:           netConn,
		tcpConn:           tcpConn,
		timeoutMultiplier: toMulti,
	}
}

// newConnFromHost wraps newConn and looks up the target's address from a string
//
// This function is used by Client
func newConnFromHost(udpNet string, host string, port int, tcpConn *net.TCPConn, toMulti int) (*conn, error) {
	// Resolve server
	addr, err := net.ResolveUDPAddr(udpNet, host)
	if err != nil {
		return nil, wrapError(err, "address resolve failed")
	}

	// Use a specific udp port
	if port > 1023 {
		netConn, err := net.ListenUDP(udpNet, &net.UDPAddr{Port: port})
		if err != nil {
			return nil, wrapError(err, "network listen failed")
		}

		c := &conn{
			log:               newLogger(addr.String()),
			remoteAddr:        addr,
			netConn:           netConn,
			blksize:           defaultBlksize,
			timeout:           defaultTimeout,
			windowsize:        defaultWindowsize,
			retransmit:        defaultRetransmit,
			mode:              defaultMode,
			tcpConn:           tcpConn,
			timeoutMultiplier: toMulti,
		}
		c.rx.buf = make([]byte, defaultPktsize)

		return c, nil
	}

	return newConn(udpNet, addr, tcpConn, toMulti)
}

type dataBlock struct {
	block   uint16
	payload []byte
}

// TODO: add guard_time option (non-negotiable)
// conn handles TFTP read and write requests
type conn struct {
	log        *logger
	netConn    *net.UDPConn // Underlying UDP network connection
	tcpConn    *net.TCPConn // Underlying TCP network connection
	remoteAddr net.Addr     // Address of the remote server or client

	// Single Port Mode

	reqChan chan []byte
	timer   *time.Timer

	// Transfer type

	isClient bool // Whether or not we're the client, gets set by sendRequest
	isSender bool // Whether we're sending or receiving, gets set by writeSetup

	// Negotiable options

	blksize           uint8         // Size of DATA payloads
	timeout           time.Duration // How long to wait before resending packets
	timeoutMultiplier int
	windowsize        uint8        // Number of DATA packets between ACKs
	mode              TransferMode // octet or netascii
	tsize             *int64       // Size of the file being sent/received

	retransmit int // Number of times an individual datagram will be retransmitted on error

	// Track state of transfer

	optionsParsed bool   // Whether TFTP options have been parsed yet
	window        uint8  // Packets sent since last ACK
	block         uint16 // Current block #
	p             []byte // bytes to be read/written from/to file (depending on send/receive)
	n             int    // byte count read/written
	tries         int    // retry counter
	triesAck      int    // retry ack counter
	ackTimeout    bool
	unackBlocks   int
	err           error // error has occurreds
	closing       bool  // connection is closing
	done          bool  // the transfer is complete (or error occurred)
	duplicate     bool

	// Buffers

	buf   []byte       // incoming data from, sized to blksize + headers
	txBuf *ringBuffer  // buffers outgoing data, retaining windowsize * blksize
	rxBuf bytes.Buffer // buffer incoming data

	txWin      []dataBlock
	unackWin   []dataBlock
	ackPayload []byte

	rxWin      []dataBlock
	rxUnackWin map[uint8]uint16
	rxFirstw   uint8

	txTime    time.Time
	rxTime    time.Time
	rxTimeout time.Duration
	guardTime time.Duration

	// Datagrams

	tx datagram // Constructs outgoing datagrams
	rx datagram // Hold and parse current incoming datagram

	// reader/writer are rxBuf/txBuf, possibly wrapped by netascii reader/writer

	reader io.Reader
	writer io.Writer
}

// sendWriteRequest sends WRQ to server and negotiates transfer options
func (c *conn) sendWriteRequest(filename string, opts map[string]string) error {
	c.isSender = true

	// Build WRQ
	c.tx.writeWriteReq(filename, c.mode, opts)

	for state := c.sendRequest; state != nil; {
		state = state()
	}

	return c.err
}

// sendReadRequest send RRQ to server and negotiates transfer options
//
// If the server doesn't support options and responds with data, the data will be added
// to rxBuf.
func (c *conn) sendReadRequest(filename string, opts map[string]string) error {
	// Build RRQ
	c.tx.writeReadReq(filename, c.mode, opts)

	for state := c.sendRequest; state != nil; {
		state = state()
	}

	return c.err
}

// sendRequest() send WRQ/RRQ to the server and start waiting for the response
func (c *conn) sendRequest() stateType {
	// Set that we're a client
	c.isClient = true

	// Send request
	if err := c.writeToNet(c.fragmentRequest()); err != nil {
		c.err = wrapError(err, "writing request to network")
		return nil
	}

	// Store tx time to compute RTT on reception of OACK
	c.txTime = time.Now()

	return c.receiveResponse
}

// fragmentRequest() return true if the tx buffer is larger than the default packet size.
// Note that a wrq/rrq/oack is always sent using the defaultPktsize.
func (c *conn) fragmentRequest() bool {
	return (c.tx.offset > defaultPktsize)
}

// receiveResponse() receive the response to a WRQ/RRQ request from the server
func (c *conn) receiveResponse() stateType {
	if c.tries >= c.retransmit {
		c.err = wrapError(ErrMaxRetries, "receiving request response")
		return nil
	}
	c.tries++

	addr, err := c.readFromNet()
	if err != nil {
		c.log.debug("error getting %s response from %v", c.tx.opcode(), c.remoteAddr)
		c.log.debug("resending %s", c.tx.opcode())

		return c.sendRequest()
	}

	if err := c.rx.validate(); err != nil {
		c.log.debug("error validating response from %v: %v", c.remoteAddr, err)
		c.err = wrapError(err, "validating request response")
		return nil
	}

	if c.reqChan == nil {
		// Update address
		c.remoteAddr = addr
	}
	c.log.trace("Received response from %v: %v", addr, c.rx)

	c.tries = 0

	if c.isSender {
		// Set timeout to RTT
		c.timeout = time.Now().Sub(c.txTime)

		return c.handleWRQResponse
	}

	return c.handleRRQResponse
}

func (c *conn) handleWRQResponse() stateType {
	// Should have received OACK if server supports options, or ACK if not
	switch c.rx.opcode() {
	case opCodeOACK, opCodeACK:
		// Got OACK, parse options
		return c.writeSetup
	case opCodeERROR:
		// Received an error
		c.err = wrapError(c.remoteError(), "WRQ OACK response")
		return nil
	default:
		c.err = wrapError(&errUnexpectedDatagram{dg: c.rx.String()}, "WRQ OACK response")
		return nil
	}
}

func (c *conn) handleRRQResponse() stateType {
	// Should have received OACK if server supports options, or DATA if not
	switch c.rx.opcode() {
	case opCodeOACK:
		// Got OACK, parse options
		return c.readSetup
	case opCodeDATA:
		// Server doesn't support options,
		// write data to the buf so it's available for reading
		n, err := c.rxBuf.Write(c.rx.data())
		if err != nil {
			c.err = wrapError(err, "writing RRQ response data")
			return nil
		}
		c.block = c.rx.block()
		if uint8(n) < c.blksize {
			c.done = true
		}
		return c.readSetup
	case opCodeERROR:
		// Received an error
		c.err = wrapError(c.remoteError(), "RRQ OACK response")
		return nil
	default:
		c.err = wrapError(&errUnexpectedDatagram{dg: c.rx.String()}, "RRQ OACK response")
		return nil
	}
}

// Write implements io.Writer and wraps write().
//
// If mode is ModeNetASCII, wrap write() with netascii.EncodeWriter.
func (c *conn) Write(p []byte) (int, error) {
	// Can't write if an error has been sent/received
	if c.err != nil {
		return 0, wrapError(c.err, "checking conn err before Write")
	}

	c.p = p
	for state := c.startWrite; state != nil; {
		state = state()
	}

	return c.n, wrapError(c.err, "writing")
}

type stateType func() stateType

func (c *conn) startWrite() stateType {
	if !c.optionsParsed {
		// Options won't be parsed before first write so that API consumer
		// has opportunity to set tsize with ReadRequest.WriteSize()
		return c.writeSetup
	}
	return c.write
}

// writeSetup parses options and sets up buffers before
// first write.
func (c *conn) writeSetup() stateType {
	// Set that we're sending
	c.isSender = true

	ackOpts, err := c.parseOptions()
	if err != nil {
		return c.error(err, "parsing options")
	}

	// Set buf size
	if len(c.buf) != int(c.blksize) {
		c.buf = make([]byte, c.blksize)
	}

	// Init ringBuffer
	c.txBuf = newRingBuffer(int(c.windowsize), int(c.blksize))

	c.writer = c.txBuf
	if c.mode == ModeNetASCII {
		c.writer = netascii.NewWriter(c.writer)
	}

	// Init sender window
	c.txWin = make([]dataBlock, c.windowsize)

	// Init unACKed blocks window for sender and receiver
	c.unackWin = make([]dataBlock, c.windowsize)
	c.rxUnackWin = make(map[uint8]uint16)

	// Init ACK bitmask string
	// (1 : lost block - 0 : received block)
	bitlen := (1 + (c.windowsize-1)/8) * 8 // add padding
	c.ackPayload = make([]byte, bitlen)
	for i := range c.ackPayload {
		c.ackPayload[i] = 0x1
	}

	// Client setup is done, ready to send data
	if c.isClient {
		return nil
	}

	// Sending DATA ACKs when there are no options
	if len(ackOpts) == 0 {
		return c.write
	}

	// TODO: handle RRQ case
	// Send OACK
	return c.sendOACK(ackOpts)
}

func (c *conn) sendOACK(o options) stateType {
	return func() stateType {
		c.log.trace("Sending OACK to %s\n", c.remoteAddr)
		c.tx.writeOptionAck(o)
		if err := c.writeToNet(c.fragmentRequest()); err != nil {
			return c.error(err, "writing OACK")
		}

		return c.getAck
	}
}

func (c *conn) error(err error, desc string) stateType {
	return func() stateType {
		c.err = wrapError(err, desc)
		return nil
	}
}

// write writes data to txBuf and writes data to netConn in chunks of
// blksize, until the last chunk of <blksize, which signals transfer completion.
func (c *conn) write() stateType {
	// Copy to buffer
	read, err := c.writer.Write(c.p)
	if err != nil {
		c.n, c.err = read, wrapError(err, "writing data to txBuf before write")
		return nil
	}
	c.n = read

	return c.writeData
}

// writeData writes a single DATA datagram
func (c *conn) writeData() stateType {
	if c.closing && c.done {
		return nil
	}
	if c.txBuf.Len() < int(c.blksize) && !c.closing {
		return nil
	}

	var n int
	var err error

	// ACK not received or duplicate ACK, retransmit last window
	if c.ackTimeout {
		block := c.txWin[c.window].block
		payload := c.txWin[c.window].payload

		c.tx.writeData(c.window, block, payload)

		// If this is last block, move to get ack immediately
		if uint8(len(payload)) < c.blksize {
			c.window++
			c.done = true
			return c.getAck
		}

	} else {
		// received ACK, transmit new window
		if c.unackBlocks > 0 {
			// Retransmit not acked block
			unack_block := c.unackWin[c.window].block

			n, err = c.txBuf.Read(c.buf, int(c.window))
			if err != nil && err != io.EOF {
				c.err = wrapError(err, "reading data from txBuf before writing to network")
				return nil
			}
			c.tx.writeData(c.window, unack_block, c.buf[:n])

			// Copy last transmitted block to tx window
			c.txWin[c.window].block = unack_block
			n = copy(c.txWin[c.window].payload, c.buf[:n])

			// Decrement unacked blocks counter
			c.unackBlocks--

		} else {
			// Transmit new block
			c.block++

			n, err = c.txBuf.Read(c.buf, int(c.window))
			if err != nil && err != io.EOF {
				c.err = wrapError(err, "reading data from txBuf before writing to network")
				return nil
			}
			c.tx.writeData(c.window, c.block, c.buf[:n])

			// Copy last transmitted block to tx window
			c.txWin[c.window].block = c.block
			n = copy(c.txWin[c.window].payload, c.buf[:n])
		}
	}

	// Send tx datagram
	c.log.trace("Sending block %d with block number %d and %d bytes to %s\n", c.window, c.block, n, c.remoteAddr)
	err = c.writeToNet(false)
	if err != nil {
		c.err = wrapError(err, "writing data to network")
		return nil
	}

	// Update transmission time
	c.txTime = time.Now()

	// Increment the window
	c.window++

	// If this is last block, move to get ack immediately
	if !c.ackTimeout && uint8(n) < c.blksize {
		c.done = true
		return c.getAck
	}

	// Continue on if we haven't reached the windowsize
	if c.window < c.windowsize {
		return c.writeData
	}

	// Reset window
	c.window = 0

	return c.getAck
}

// Read implements io.Reader and wraps read()
//
// If mode is ModeNetASCII, read() is wrapped with netascii.ReadDecoder
func (c *conn) Read(p []byte) (int, error) {
	c.n = 0
	if c.err != nil {
		// Can't read if an error has been sent/received
		return 0, wrapError(c.err, "checking conn error before Read")
	}

	c.p = p
	for state := c.startRead; state != nil; {
		state = state()
	}
	return c.n, c.err
}

func (c *conn) startRead() stateType {
	if !c.optionsParsed {
		return c.readSetup
	}
	return c.read
}

// readSetup parses options and sets up buffers before
// first read.
func (c *conn) readSetup() stateType {
	c.reader = &c.rxBuf

	ackOpts, err := c.parseOptions()
	if err != nil {
		c.err = wrapError(err, "read setup")
		return nil
	}

	if c.mode == ModeNetASCII {
		c.reader = netascii.NewReader(c.reader)
	}

	// Set buf size
	if needed := int(c.blksize + defaultHdrsize); len(c.rx.buf) != needed {
		c.rx.buf = make([]byte, needed)
	}

	// If there are not options negotiated, send empty OACK
	// Client initiating with RRQ responds with ACK to an OACK
	if c.isClient {
		// TODO: handle RRQ case
		// c.log.trace("Sending ACK to %s\n", c.remoteAddr)
		// c.tx.writeAck(c.block)
	} else {
		c.log.trace("Sending OACK to %s\n", c.remoteAddr)
		c.tx.writeOptionAck(ackOpts)
	}

	// Send ACK/OACK
	if err := c.writeToNet(c.fragmentRequest()); err != nil {
		c.err = wrapError(err, "writing request to network")
		return nil
	}

	if c.isClient {
		return nil
	}

	return c.read
}

// read reads data from netConn until p is full or the connection is
// complete.
func (c *conn) read() stateType {
	if c.rxBuf.Len() >= len(c.p) || c.done {
		// Read buffered data into p
		n, err := c.reader.Read(c.p)
		c.n = n
		if err != nil && err != io.EOF { // Ignore EOF from bytes.Buffer
			c.err = wrapError(err, "reading from rxBuf after read")
		}
		// If done, signal that there's nothing more to read by io.EOF
		if c.done && c.rxBuf.Len() == 0 {
			c.err = io.EOF
		}
		return nil
	}

	// Read next datagram
	return c.readData
}

// readDatagram reads a single datagram into rx
func (c *conn) readData() stateType {
	if c.tries >= c.retransmit {
		c.log.debug("Max retries exceeded")
		c.sendError(ErrCodeNotDefined, "max retries reached")
		c.err = wrapError(ErrMaxRetries, "reading data")
		return nil
	}
	c.tries++

	c.log.trace("Waiting for DATA from %s\n", c.remoteAddr)

	oack := datagram{}
	if c.block == 0 {
		oack = c.tx
	}

	_, err := c.readFromNet()
	if err != nil {
		c.log.debug("error receiving block %d: %v", c.block+1, err)

		if c.block == 0 {
			// Retransmit an OACK
			c.log.trace("Resending %s", c.tx)

			c.tx = oack

			c.writeToNet(c.fragmentRequest())
		} else {
			// If received duplicate blocks and lost subsequent blocks of
			// the window: don't flag as lost already received blocks
			if c.duplicate && c.window < c.windowsize {
				for i := c.window; i < c.windowsize; i++ {
					if _, ok := c.rxUnackWin[i]; !ok {
						c.ackPayload[i] = 0
					}
				}

				c.duplicate = false
			}
			c.log.trace("Resending ACK %08b\n", c.ackPayload)
			if err := c.sendAck(); err != nil {
				c.log.debug("resending ACK %v", err)
			}
			c.window = 0
			c.tries += c.triesAck

			// Example
			// windowsize = 3
			// Received block 1 (window = 0), lost block 2 and received block 3 (window = 2)
			// Received duplicate block 1 (window = 0) and rxUnackWin = ["1" : 2]
			// Timeout (lost both block 2 and 3)
			// Send ackPayload = [010]
		}

		return c.readData
	}

	// validate datagram
	if err := c.rx.validate(); err != nil {
		c.err = wrapError(err, "validating read data")
		return nil
	}

	// Check for opcode
	switch op := c.rx.opcode(); op {
	case opCodeDATA:
	case opCodeERROR:
		// Received an error
		c.err = wrapError(c.remoteError(), "reading data")
		return nil
	default:
		c.err = wrapError(&errUnexpectedDatagram{dg: c.rx.String()}, "read data response")
		return nil
	}

	c.log.trace("Received block %d\n", c.rx.block())
	c.tries = 0

	return c.ackData
}

// ackData handles block sequence, windowing, and acknowledgements
func (c *conn) ackData() stateType {
	var n int
	var err error

	// Received a block of new window
	// Save index of the block in the window and time
	// For subsequent blocks of the window compute the expected time before the next one
	if c.window == 0 {
		c.rxFirstw = c.rx.window()
		c.rxTime = time.Now()
	} else {
		diff_window := c.rx.window() - c.rxFirstw
		rx_duration := (time.Now().Sub(c.rxTime)) / time.Duration(diff_window)

		c.rxTimeout = rx_duration*(time.Duration(c.windowsize)-time.Duration(c.rx.block())-1) + c.guardTime
	}

	// New block received
	// otherwise unacked block or duplicate
	if c.rx.block() > c.block {
		// In order block add to a temporary buffer if missed blocks in previous
		// windows otherwise write data to buffer
		if c.rx.window() == c.window {
			c.ackPayload[c.window] = 0x0
			c.window++

			if len(c.unackWin) > 0 {
				c.rxWin = append(c.rxWin, dataBlock{c.rx.block(), c.rx.data()})
			} else {
				n, err = c.rxBuf.Write(c.rx.data())
				if err != nil {
					c.err = wrapError(err, "writing to rxBuf after read")
					return nil
				}
			}
		} else {
			// Missed blocks in the current window
			// Add the block numbers to rxUnackWin window in increasing order
			// If some block was already in rxUnackWin updates the key
			n_unack := c.rx.block() - c.block - 1
			for i := n_unack; i > 0; i-- {
				c.rxUnackWin[c.rx.window()-uint8(i)] = c.rx.block() - i
			}

			c.window = c.rx.window() + 1
			c.rxWin = append(c.rxWin, dataBlock{c.rx.block(), c.rx.data()})
		}

		// Data block with highest block # received
		c.block = c.rx.block()

	} else {
		// Check if received block was lost in previous transmissions
		// otherwise is a duplicate
		var is_unacked bool
		var key_unacked uint8
		for k, v := range c.rxUnackWin {
			// If received block in rxUnackWin
			if v == c.rx.block() {
				is_unacked = true
				key_unacked = k
			}
		}

		if is_unacked {
			idx := slices.IndexFunc(c.rxWin, func(d dataBlock) bool {
				return d.block > c.rx.block()
			})

			c.rxWin = append(c.rxWin[:idx+1], c.rxWin[idx:]...)
			c.rxWin[idx] = dataBlock{c.rx.block(), c.rx.data()}

			delete(c.rxUnackWin, key_unacked)

			// If rxUnackWin is empty there are no out of order packets
			// write the whole rxWin to rxBuf
			// otherwise write only in order blocks
			if len(c.rxUnackWin) == 0 {
				for _, v := range c.rxWin {
					n, err = c.rxBuf.Write(v.payload)
					if err != nil {
						c.err = wrapError(err, "writing to rxBuf after read")
						return nil
					}
				}

				// Clear rxWin
				c.rxWin = nil
			} else {
				// Shift rxUnackWin greater than key_unacked
				temp_map := make(map[uint8]uint16)
				for k, v := range c.rxUnackWin {
					if k > key_unacked {
						temp_map[k-1] = v
					} else {
						temp_map[k] = v
					}
				}
				c.rxUnackWin = temp_map

				for c.rxUnackWin[0] > c.rxWin[0].block {
					idx := slices.IndexFunc(c.rxWin, func(d dataBlock) bool {
						return d.block > c.rxUnackWin[0]
					})

					for i := 0; i < idx; i++ {
						n, err = c.rxBuf.Write(c.rxWin[i].payload)
						if err != nil {
							c.err = wrapError(err, "writing to rxBuf after read")
							return nil
						}
					}

					c.rxWin = c.rxWin[idx:]
				}

				// Example:
				// rxWin = [ 2 3 4 5 9 ]
				// rxUnackWin = [ (0,1) (1,6) (2,7) (3,8) ]
				// 1 > 2 : false
				// rx.block() = 1
				// rx.window() = 0
				// rxWin = [ 1 2 3 4 5 9 ]
				// rxUnackWin = [ (0,6) (1,7) (2,8) ]
				// 6 > 1 : true
				//			idx = 5
				//			rxBuf.Write(rxWin[0:4].payload)
				//			rxWin = [ 9 ]
				// 6 > 9 : false
			}

		} else {
			c.duplicate = true
		}

		// Increase window
		// (in between lost blocks are already in rxUnackWin)
		c.ackPayload[c.window] = 0x0
		if c.rx.window() == c.window {
			c.window++
		} else {
			c.window = c.rx.window() + 1
		}
	}

	// Last block
	if n < int(c.blksize) {
		// Reveived last DATA, we're done
		c.done = true
	}

	// We haven't reached the window
	if c.window < c.windowsize && n >= int(c.blksize) {
		return c.read
	}

	// Reached the windowsize or final data, send ACK and reset window
	c.window = 0
	c.log.trace("Window %d reached, sending ACK for %d\n", c.windowsize, c.block)
	if err := c.sendAck(); err != nil {
		c.err = wrapError(err, "sending DATA ACK")
		return nil
	}

	// TODO:
	// reset timeout
	// the RTT is the time between instant in which sendAck e last ack sent: use this as timeout after sending ack

	return c.read
}

// Close flushes any remaining data to be transferred and closes netConn
func (c *conn) Close() error {
	c.log.debug("Closing connection to %s\n", c.remoteAddr)

	if c.reqChan == nil {
		defer func() {
			var err error
			// Close network even if another error occurs
			if c.netConn != nil {
				err = c.netConn.Close()
			}
			// Also close the tcp socket, if any
			if c.tcpConn != nil {
				err = c.tcpConn.Close()
			}
			if err != nil {
				c.log.debug("error closing network connection:", err)
			}
			if c.err == nil {
				c.err = err
			}
		}()
	}

	// Can't write if an error has been sent/received
	if c.err != nil && c.err != io.EOF {
		return wrapError(c.err, "checking conn err before Close")
	}

	// netasciiEnc needs to be flushed if it's in use
	if flusher, ok := c.writer.(interface {
		Flush() error
	}); ok {
		c.log.trace("flushing writer")
		if err := flusher.Flush(); err != nil {
			return wrapError(err, "flushing writer")
		}
	}

	// Write any remaining data, or 0 length DATA to end transfer
	if c.txBuf != nil {
		c.closing = true
		c.Write([]byte{})
	}

	if c.err == io.EOF {
		return nil
	}

	return c.err
}

// parseOACK parses the options from a datagram and returns the successfully
// negotiated options.
func (c *conn) parseOptions() (options, error) {
	ackOpts := make(map[string]string)

	// parse and set options
	for opt, val := range c.rx.options() {
		switch opt {
		case optBlocksize:
			size, err := strconv.ParseUint(val, 10, 8)
			if err != nil {
				return nil, &errParsingOption{option: opt, value: val}
			}
			c.blksize = uint8(size)
			ackOpts[opt] = val
		case optTimeout:
			seconds, err := strconv.ParseUint(val, 10, 8)
			if err != nil {
				return nil, &errParsingOption{option: opt, value: val}
			}
			c.timeout = time.Duration(c.timeoutMultiplier) * time.Second * time.Duration(seconds)
			ackOpts[opt] = val
		case optTransferSize:
			tsize, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, &errParsingOption{option: opt, value: val}
			}
			if c.isSender && c.tsize != nil {
				// We're sender, send tsize
				ackOpts[opt] = strconv.FormatInt(*c.tsize, 10)
				continue
			}
			c.tsize = &tsize
		case optWindowSize:
			size, err := strconv.ParseUint(val, 10, 8)
			if err != nil {
				return nil, &errParsingOption{option: opt, value: val}
			}
			c.windowsize = uint8(size)
			ackOpts[opt] = val
		case optMode:
			if val == string(ModeOctet) {
				c.mode = ModeOctet
			} else if val == string(ModeNetASCII) {
				c.mode = ModeNetASCII
			} else {
				return nil, &errParsingOption{option: opt, value: val}
			}
			ackOpts[opt] = val
		}
	}

	c.optionsParsed = true

	return ackOpts, nil
}

// sendError sends ERROR datagram to remote host
func (c *conn) sendError(code ErrorCode, msg string) {
	c.log.debug("Sending error code %s to %s: %s\n", code, c.remoteAddr, msg)

	// Check error message length
	if len(msg) > int((c.blksize - 1)) { // -1 for NULL terminator
		c.log.debug("error message is larger than blksize, truncating")
		msg = msg[:c.blksize-1]
	}

	// Send error
	c.tx.writeError(code, msg)
	if err := c.writeToNet(false); err != nil {
		c.log.debug("sending ERROR: %v", err)
	}
}

// sendAck sends ACK
func (c *conn) sendAck() error {
	c.tx.writeAck(c.ackPayload)

	c.log.trace("Sending ACK %08b to %s\n", c.ackPayload, c.remoteAddr)

	// Reset ACK bitmask
	for i := range c.ackPayload {
		c.ackPayload[i] = 0x1
	}

	return wrapError(c.writeToNet(false), "sending ACK")
}

// Retrieve lost blocks from ACK payload
//
// Bits set to 1 are lost blocks that the receiver didn't ack
// Use their indexes to retrieve block numbers from tx window
func (c *conn) getUnackBlocks(ack_p []byte) []uint16 {
	var blocks []uint16

	// Read each byte
	read_bits := 8
	for i := 0; i < len(ack_p); i++ {
		// Empty byte
		if ack_p[i] == 0 {
			continue
		}

		// Last byte, read up to c.windowsize bits
		if i == len(ack_p)-1 {
			if rem := int(c.windowsize) % read_bits; rem != 0 {
				read_bits = rem
			}
		}

		// Read every bit of the byte
		for j := 0; j < read_bits; j++ {
			// If equals to 1 get block number from txWin and update unackWin
			if (ack_p[i] & (1 << (7 - j))) != 0 {
				index := i*8 + j
				tx_data := c.txWin[index]

				c.txBuf.PushQueue(index)
				if idx := slices.IndexFunc(c.unackWin, func(d dataBlock) bool { return d.block == tx_data.block }); idx < 0 {
					// Not already in unackWin
					c.unackWin = append(c.unackWin, tx_data)
				}

				blocks = append(blocks, tx_data.block)
			} else {
				if len(c.unackWin) > 0 {
					// Got ack for a previously lost block
					index := i*8 + j
					tx_data := c.txWin[index]

					// Remove from unackWin
					c.unackWin = slices.DeleteFunc(c.unackWin, func(d dataBlock) bool {
						return d.block == tx_data.block
					})
				}
			}
		}
	}

	c.unackBlocks = len(c.unackWin)

	return blocks
}

// Return true if the receiver lost all packets.
//
// If all sent packets are lost, receiver timeouts and send an ack with only 1s
// treat it as a duplicate ack.
func (c *conn) windowLost(b []byte) bool {
	for _, v := range b {
		if v != 255 {
			return false
		}
	}
	return true
}

// getAck reads ACK, validates structure and checks for ERROR
//
// If the ACK is not received (timeout) it will rollback the transfer and
// retransmit the whole window.
// If the received ACK is for a previous block, indicating the receiver missed data.
// it will rollback the transfer to the ACK'd block and reset the window.
func (c *conn) getAck() stateType {
	c.tries++
	if c.tries > c.retransmit {
		c.log.debug("Max retries exceeded")
		c.sendError(ErrCodeNotDefined, "max retries reached")
		c.err = wrapError(ErrMaxRetries, "reading ack")
		return nil
	}

	c.log.trace("Waiting for ACK from %s\n", c.remoteAddr)
	sAddr, err := c.readFromNet()
	if err != nil {
		c.log.debug("Error waiting for ACK: %v", err)
		c.ackTimeout = true

		return c.writeData
	}

	c.ackTimeout = false

	// Send error to requests not from requesting client. May consider
	// ignoring entirely.
	// RFC1350:
	// "If a source TID does not match, the packet should be
	// discarded as erroneously sent from somewhere else.  An error packet
	// should be sent to the source of the incorrect packet, while not
	// disturbing the transfer."
	if c.reqChan == nil && sAddr.String() != c.remoteAddr.String() {
		c.log.err("Received unexpected datagram from %v, expected %v\n", sAddr, c.remoteAddr)
		go func() {
			var err datagram
			err.writeError(ErrCodeUnknownTransferID, "Unexpected TID")
			_, _ = c.netConn.WriteTo(err.bytes(), sAddr)
		}()

		return c.getAck // Read another datagram
	}

	// Validate received datagram
	if err := c.rx.validate(); err != nil {
		c.err = wrapError(err, "ACK validation failed")
		return nil
	}

	// Check opcode
	switch op := c.rx.opcode(); op {
	case opCodeOACK:
		c.log.trace("Received duplicate OACK. Resending last window.\n")
		return c.writeData
	case opCodeACK:
		ack_payload := c.rx.ack()

		if c.windowLost(ack_payload) {
			c.ackTimeout = true

			c.log.trace("Duplicate ACK. Resending last window.")
		} else {
			lost_blocks := c.getUnackBlocks(ack_payload)

			c.log.trace("Received ACK. Lost blocks: %d\n", lost_blocks)
		}

		// continue on
	case opCodeERROR:
		c.err = wrapError(c.remoteError(), "error receiving ACK")
		return nil
	default:
		c.err = wrapError(&errUnexpectedDatagram{c.rx.String()}, "error receiving ACK")
		return nil
	}

	// Reset done in case error on final send
	if len(c.unackWin) > 0 {
		c.done = false
	}

	c.tries = 0

	if c.tx.opcode() == opCodeOACK {
		return c.write
	}

	return c.writeData
}

// remoteError formats the error in rx, sets err and returns the error.
func (c *conn) remoteError() error {
	c.err = &errRemoteError{dg: c.rx.String()}
	return c.err
}

// readFromNet reads from netConn into buffer.
func (c *conn) readFromNet() (net.Addr, error) {
	// TODO: handle timeout update during reception of data blocks (server side)
	if c.reqChan != nil {
		// Setup timer
		if c.timer == nil {
			c.timer = time.NewTimer(c.timeout)
		} else {
			c.timer.Reset(c.timeout)
		}

		// Single port mode
		select {
		case c.rx.buf = <-c.reqChan:
			c.rx.offset = len(c.rx.buf)
			return nil, nil
		case <-c.timer.C:
			return nil, errors.New("timeout reading from channel")
		}
	}

	var err error
	var n int
	var addr net.Addr

	if c.tcpConn == nil {
		// Read from the UDP server socket
		if err = c.netConn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
			return nil, wrapError(err, "setting network read deadline")
		}
		n, addr, err = c.netConn.ReadFrom(c.rx.buf)
	} else {
		// Read from the TCP external socket
		if err = c.tcpConn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
			return nil, wrapError(err, "setting network read deadline")
		}
		n, err = c.tcpConn.Read(c.rx.buf)
		addr = c.tcpConn.RemoteAddr()
	}

	c.rx.offset = n
	if n > 0 && c.rx.opcode() == opCodeOACK {
		// Fragmented OACK received (last byte is always empty)
		for c.rx.buf[c.rx.offset-1] != 0x0 {
			buf := make([]byte, n)
			if c.tcpConn == nil {
				n, addr, err = c.netConn.ReadFrom(buf)
			} else {
				n, err = c.tcpConn.Read(buf)
				addr = c.tcpConn.RemoteAddr()
			}
			c.rx.buf = append(c.rx.buf, buf...)
			c.rx.offset += n
		}
	}

	return addr, err
}

// writeToNet writes tx to netConn.
// If fragment is true, the tx buffer is sent in multiple packets of default size.
// ( Fragmentation is required when defaultPktsize < WRQ/RRQ/OACK size)
func (c *conn) writeToNet(fragment bool) error {
	var err error

	if fragment {
		for i := 0; i < c.tx.offset; i += defaultPktsize {
			if c.tcpConn == nil {
				_, err = c.netConn.WriteTo(c.tx.getBytes(i, i+defaultPktsize), c.remoteAddr)
			} else {
				_, err = c.tcpConn.Write(c.tx.getBytes(i, i+defaultPktsize))
			}
		}

		return err
	}

	if c.tcpConn == nil {
		// Write to the UDP server socket
		if err = c.netConn.SetWriteDeadline(time.Now().Add(c.timeout * time.Duration(c.retransmit))); err != nil {
			return wrapError(err, "setting network write deadline")
		}
		_, err = c.netConn.WriteTo(c.tx.bytes(), c.remoteAddr)
	} else {
		// Write to the TCP external socket
		if err = c.tcpConn.SetWriteDeadline(time.Now().Add(c.timeout * time.Duration(c.retransmit))); err != nil {
			return wrapError(err, "setting network write deadline")
		}
		_, err = c.tcpConn.Write(c.tx.bytes())
		time.Sleep(1500 * time.Microsecond)
	}

	return err
}

// ringBuffer embeds a bytes.Buffer, adding the ability to store blocks in
// a queue.
type ringBuffer struct {
	bytes.Buffer
	lastSlot bool
	slots    int
	size     int
	current  int    // current slot to be read
	queue    []int  // queue of slots to read again
	buf      []byte // buffer of last sent window
}

// newRingBuffer initializes a new ringBuffer
func newRingBuffer(slots int, size int) *ringBuffer {
	return &ringBuffer{
		buf:   make([]byte, size*slots),
		slots: slots,
		size:  size,
	}
}

// Len returns bytes.Buffer.Len() + any buffer space in the queue
func (r *ringBuffer) Len() int {
	queuelen := len(r.queue) * r.size
	return r.Buffer.Len() + queuelen
}

// Read reads data from byte.Buffer if queue empty
// If queue not empty, data will be read from buf.
func (r *ringBuffer) Read(p []byte, window int) (int, error) {
	offset := window * r.size

	// Copy data from buf
	if len(r.queue) > 0 {
		offlen := r.queue[0] * r.size

		var n int
		if !r.lastSlot {
			n = copy(p, r.buf[offlen:offlen+r.size])
		} else {
			n = copy(p, r.buf[offlen:])
			r.lastSlot = false
		}

		// overwrite window slot
		n = copy(r.buf[offset:offset+n], p[:n])

		// Pop from queue
		r.PopQueue()

		return n, nil
	}

	// Read from Buffer and copy read data into current slot
	// (len(p) == c.blksize)
	n, err := r.Buffer.Read(p)
	n = copy(r.buf[offset:offset+n], p[:n])
	if n < r.size {
		r.lastSlot = true
	}

	// Increment current
	r.current++

	return n, err
}

// Append elementto queue
func (r *ringBuffer) PushQueue(slot int) {
	r.queue = append(r.queue, slot)
}

// Remove first element of the queue
func (r *ringBuffer) PopQueue() {
	if len(r.queue) > 0 {
		r.queue = r.queue[1:]
	}
}

// readerFunc is an adapter type to convert a function
// to a io.Reader
type readerFunc func([]byte) (int, error)

// Read implements io.Reader
func (f readerFunc) Read(p []byte) (int, error) {
	return f(p)
}

// writerFunc is an adapter type to convert a function
// to a io.Writer
type writerFunc func([]byte) (int, error)

// Write implements io.Writer
func (f writerFunc) Write(p []byte) (int, error) {
	return f(p)
}

func errorDefer(fn func() error, log *logger, msg string) {
	if err := fn(); err != nil {
		log.debug(msg+": %v", err)
	}
}
