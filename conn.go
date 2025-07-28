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
	defaultGuardTime         = time.Second * 5
	defaultBlksize           = 55
	defaultHdrsize           = sizeofHdr
	defaultPktsize           = defaultHdrsize + defaultBlksize
	defaultWindowsize        = 1
	defaultRetransmit        = 10
	defaultTimeoutMultiplier = 1
)

// All connections will use these options unless overridden.
var defaultOptions = map[string]string{
	// optTransferSize: "1", // Enable tsize
	// optMode: string(defaultMode),
}

// newConn starts listening on a system assigned port and returns an initialized conn
//
// udpNet is one of "udp", "udp4", or "udp6"
// addr is the address of the target server
// tcpConn is the TCP socket of an external application, if specified
func newConn(udpNet string, addr *net.UDPAddr, tcpConn *net.TCPConn) (*conn, error) {
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
		timeoutMultiplier: defaultTimeoutMultiplier,
		guardTime:         defaultGuardTime,
	}
	c.rx.buf = make([]byte, defaultPktsize)

	return c, nil
}

func newSinglePortConn(addr *net.UDPAddr, netConn *net.UDPConn, tcpConn *net.TCPConn, reqChan chan []byte) *conn {
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
		timeoutMultiplier: defaultTimeoutMultiplier,
		guardTime:         defaultGuardTime,
	}
}

// newConnFromHost wraps newConn and looks up the target's address from a string
//
// This function is used by Client
func newConnFromHost(udpNet string, host string, port int, tcpConn *net.TCPConn) (*conn, error) {
	// Resolve server
	addr, err := net.ResolveUDPAddr(udpNet, host)
	if err != nil {
		return nil, wrapError(err, "address resolve failed")
	}

	// TODO: remove this feature
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
			timeoutMultiplier: defaultTimeoutMultiplier,
			guardTime:         defaultGuardTime,
		}
		c.rx.buf = make([]byte, defaultPktsize)

		return c, nil
	}

	return newConn(udpNet, addr, tcpConn, toMulti)
}

// dataBlock mimics the structure of a DATA packet (without the opcode).
type dataBlock struct {
	window  uint8  // Index of a DATA packet inside the current window
	block   uint16 // Block number of a DATA packet
	payload []byte // Payload of a DATA packet
}

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
	timeoutMultiplier int           // Multiplier of timeout possible values (0-255 s)
	windowsize        uint8         // Number of DATA packets between ACKs
	mode              TransferMode  // octet or netascii
	tsize             *int64        // Size of the file being sent/received
	retransmit        int           // Number of times an individual datagram will be retransmitted on error

	// Track state of transfer

	optionsParsed bool   // Whether TFTP options have been parsed yet
	window        uint8  // Packets sent since last ACK
	block         uint16 // Highest block number transmitted/received
	p             []byte // bytes to be read/written from/to file (depending on send/receive)
	n             int    // byte count read/written
	tries         int    // retry counter
	triesAck      int    // retry ack counter
	ackTimeout    bool   // whether or not received ack before timeout
	unackBlocks   int    // Number of lost blocks in ACK payload
	err           error  // error has occurreds
	closing       bool   // connection is closing
	done          bool   // last packet was received
	adone         bool   // last packet received but still missing blocks
	duplicate     bool   // duplicate blocks received

	// Buffers

	buf   []byte        // data payload to transmit
	txBuf *windowBuffer // buffers outgoing data, retaining windowsize * blksize
	rxBuf bytes.Buffer  // buffer incoming data

	txWin    []uint16 // Block numbers of last transmitted window
	unackWin []uint16 // Block numbers of lost packets

	rxWin      []dataBlock // Out of order received DATA packets
	rxUnackWin []dataBlock // DATA packets not received (lost)
	rxFirstw   uint8       // Window number of first received block in the current window

	ackPayload []byte // Payload of ACK packets (1 = lost block; 0 = received block)

	// Timers

	txTime    time.Time     // Last transmission time
	rxTime    time.Time     // last reception time
	rxTimeout time.Duration // Expected time to wait before receiving the next packet of the current window
	guardTime time.Duration // Guard time added to the timout

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

	// Store TX time to compute RTT on reception of OACK
	c.txTime = time.Now()

	return c.receiveResponse
}

// fragmentRequest() return true if the tx buffer is larger than the default packet size.
//
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
		// Set timeout equals to RTT + guardTime
		c.timeout = time.Duration(time.Since(c.txTime).Seconds()) + c.guardTime

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
	c.txBuf = newWindowBuffer(int(c.windowsize), int(c.blksize))

	c.writer = c.txBuf
	if c.mode == ModeNetASCII {
		c.writer = netascii.NewWriter(c.writer)
	}

	// Init transmitted blocks window
	c.txWin = make([]uint16, c.windowsize)

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
// blksize, until the last chunk of size < blksize
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

// Reset the number of blocks transmitted in current window
// and update TX time.
// Set done and adone if last block sent.
func (c *conn) resetWindow(done bool) {
	// Last block
	c.done = done
	c.adone = done

	// Reset window
	c.window = 0

	// Store tx time to compute RTT on reception of OACK
	c.txTime = time.Now()
}

// writeData writes a single DATA datagram
func (c *conn) writeData() stateType {
	// Every blocks received
	if c.done && c.adone {
		return nil
	}

	var n int
	var err error

	// ACK not received or duplicate ACK, retransmit last window
	if c.ackTimeout {
		block := c.txWin[c.window]

		// Read bytes from last window
		n, err = c.txBuf.ReadFromWindow(c.buf, int(c.window))

		// Write bytes to datagram
		c.tx.writeData(c.window, block, c.buf[:n])

		// Decrement unacked blocks counter
		if c.unackBlocks > 0 {
			c.unackBlocks--
		}

		c.log.trace("Resending window %d with block number %d and %d bytes to %s\n", c.window, block, n, c.remoteAddr)
	} else {
		// received ACK packet, transmit new window.
		// First retransmit lost blocks (unackBlocks > 0) and then new ones.
		if c.unackBlocks > 0 {
			unack_block := c.unackWin[c.window]

			// Read bytes from last window
			n, err = c.txBuf.ReadFromWindow(c.buf, int(c.window))
			if err != nil && err != io.EOF {
				c.err = wrapError(err, "reading data from txBuf before writing to network")
				return nil
			}

			// Write bytes to datagram
			c.tx.writeData(c.window, unack_block, c.buf[:n])

			// Update txWin
			c.txWin[c.window] = unack_block

			// Decrement unacked blocks counter
			c.unackBlocks--

			c.log.trace("Sending window %d with block number %d and %d bytes to %s\n", c.window, unack_block, n, c.remoteAddr)

		} else {
			c.block++

			// Read new bytes from buffer
			n, err = c.txBuf.Read(c.buf, int(c.window))
			if err != nil && err != io.EOF {
				c.err = wrapError(err, "reading data from txBuf before writing to network")
				return nil
			}

			// Write bytes to datagram
			c.tx.writeData(c.window, c.block, c.buf[:n])

			// Update txWin
			c.txWin[c.window] = c.block

			c.log.trace("Sending window %d with block number %d and %d bytes to %s\n", c.window, c.block, n, c.remoteAddr)
		}
	}

	// Send tx datagram
	err = c.writeToNet(false)
	if err != nil {
		c.err = wrapError(err, "writing data to network")
		return nil
	}

	// If this is the last block or it was already transmitted
	// and it was the last retransmitted lost block, move to get ack immediately
	if (uint8(n) < c.blksize) || (c.done && c.unackBlocks == 0) {
		c.txWin = c.txWin[:c.window+1]

		c.resetWindow(true)

		return c.getAck
	}

	// Increment the window
	c.window++

	// Continue on if we haven't reached the windowsize
	if c.window < c.windowsize {
		return c.writeData
	}

	// Reset window
	c.resetWindow(false)

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

	// Init ACK bitstring with only 1s
	// To each bit corresponds a packet with the same window index
	bitlen := (1 + (c.windowsize-1)/8) * 8 // add padding
	c.ackPayload = make([]byte, bitlen)
	for i := range c.ackPayload {
		c.ackPayload[i] = 0x1
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

	// Update TX time
	c.txTime = time.Now()

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

// readData reads a single datagram into rx.
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
		c.log.debug("error receiving DATA from %s: %v", c.remoteAddr, err)

		if c.block == 0 {
			// Retransmit an OACK
			c.log.trace("Resending %s", c.tx)

			c.tx = oack

			c.writeToNet(c.fragmentRequest())
		} else {
			// If received duplicates and lost subsequent blocks of
			// the window, don't flag as lost already received ones.
			if c.duplicate {
				for i := c.window; i < c.windowsize; i++ {
					// Not already in rxUnackWin
					if idx := slices.IndexFunc(c.rxUnackWin, func(d dataBlock) bool { return d.window == i }); idx < 0 {
						c.ackPayload[i] = 0x0
					}
				}

				c.duplicate = false
			}

			if err := c.sendAck(); err != nil {
				c.log.debug("sending ACK %v", err)
			}

			c.window = 0
			c.tries += c.triesAck
		}

		return c.readData
	}

	// Received first block of a window (may not be in order).
	// Save window index of the block and reception time, and update
	// the timeout with the RTT + guardTime.
	if c.window == 0 {
		c.timeout = time.Duration(time.Since(c.txTime).Seconds()) + c.guardTime

		c.rxFirstw = c.rx.window()

		c.rxTime = time.Now()
	} else {
		// For successive blocks of the window compute the expected time before the next one.
		diff_window := float64(c.rx.window() - c.rxFirstw)
		rx_duration := time.Since(c.rxTime).Seconds() / diff_window

		// Number of lost blocks between last and current received block
		lost := float64(c.windowsize - uint8(c.rx.block()) - 1)

		// Expected time before next packet: rx duration * (no. of lost blocks)
		c.rxTimeout = time.Duration(rx_duration*lost*float64(time.Second)) + c.guardTime
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

	c.tries = 0

	c.log.trace("Received %s\n", c.rx.String())

	return c.ackData
}

// ackData handles block sequence, windowing, and acknowledgements
func (c *conn) ackData() stateType {
	var err error
	n := 0

	rx_window := c.rx.window()
	rx_block := c.rx.block()
	rx_data := c.rx.data()

	// New DATA packet received
	if rx_block > c.block {
		// Expected block received in order.
		if rx_window == c.window {
			c.ackPayload[c.window] = 0x0
			c.window++

			// There are missing blocks before this, store it to rxWin
			if len(c.rxUnackWin) > 0 {
				c.rxWin = append(c.rxWin, dataBlock{rx_window, rx_block, rx_data})

				n = len(rx_data)
			} else {
				// Every block in order, write to buffer
				n, err = c.rxBuf.Write(rx_data)
				if err != nil {
					c.err = wrapError(err, "writing to rxBuf after read")
					return nil
				}
			}
		} else {
			// Missed blocks in the current window
			n_unack := rx_block - c.block - 1

			if n_unack == 0 {
				// Missed all the unacked blocks
				c.log.trace("Missed unacked block(s)")
			} else {
				c.log.trace("Missed %d block(s)", n_unack)
			}

			// Add the block numbers to rxUnackWin window in increasing order
			for i := n_unack; i > 0; i-- {
				block := rx_block - i
				window := rx_window - uint8(i)
				c.rxUnackWin = append(c.rxUnackWin, dataBlock{window, block, nil})
			}

			// Update ACK payload and increment window
			c.ackPayload[rx_window] = 0x0
			c.window = rx_window + 1

			// Add received DATA packet to rxWin
			c.rxWin = append(c.rxWin, dataBlock{rx_window, rx_block, rx_data})
			n = len(rx_data)
		}

		// Data block with highest block # received
		c.block = rx_block

	} else {
		// Previously lost DATA packet.
		if idx_ruw := slices.IndexFunc(c.rxUnackWin, func(d dataBlock) bool { return d.block == rx_block }); idx_ruw >= 0 {
			// Retrieve the index where to insert this packet in rxWin
			idx_rw := slices.IndexFunc(c.rxWin, func(d dataBlock) bool {
				return d.block > c.rx.block()
			})

			// Update receiver windows
			c.rxWin = insertAt(c.rxWin, idx_rw, dataBlock{rx_window, rx_block, rx_data})
			c.rxUnackWin = removeAt(c.rxUnackWin, idx_ruw)

			// If there are no DATA packets lost write the whole rxWin to buffer.
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
				// There are still lost DATA packets.
				for true {
					// Check if some of the previously received DATA packets
					// can be written to buffer (are in order).
					if c.rxUnackWin[0].block > c.rxWin[0].block {
						idx := slices.IndexFunc(c.rxWin, func(d dataBlock) bool {
							return d.block > c.rxUnackWin[0].block
						})

						for i := 0; i < idx; i++ {
							n, err = c.rxBuf.Write(c.rxWin[i].payload)
							if err != nil {
								c.err = wrapError(err, "writing to rxBuf after read")
								return nil
							}
						}

						// Resize rxWin
						c.rxWin = c.rxWin[idx:]

						continue
					}

					if n == 0 {
						n = len(rx_data)
					}

					break
				}
			}

		} else {
			// Duplicate DATA packet.
			c.duplicate = true
		}

		// Increase window
		if rx_window == c.window {
			c.window++
		} else {
			c.window = rx_window + 1
		}
	}

	c.ackPayload[rx_window] = 0x0

	// Reveived last DATA packet, we're done (if there aren't lost ones)
	if n < int(c.blksize) {
		if len(c.rxUnackWin) == 0 {
			c.done = true
		}

		for i := c.window; i < c.windowsize; i++ {
			c.ackPayload[i] = 0x0
		}
	}

	// We haven't reached the window
	if c.window < c.windowsize && n >= int(c.blksize) {
		return c.read
	}

	// Reached the windowsize or final data, send ACK, reset window
	// and save TX time.
	c.log.trace("Window %d reached, sending ACK\n", c.window-1)
	if err := c.sendAck(); err != nil {
		c.err = wrapError(err, "sending DATA ACK")
		return nil
	}

	c.window = 0

	c.txTime = time.Now()

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

	c.log.trace("Sending %s to %s\n", c.tx.String(), c.remoteAddr)

	// Reset ACK bitstring
	for i := range c.ackPayload {
		c.ackPayload[i] = 0x1
	}

	return wrapError(c.writeToNet(false), "sending ACK")
}

// Retrieve lost blocks from ACK payload.
//
// Bits set to 1 are lost blocks that the receiver didn't ack.
// Use their indexes to retrieve block numbers from txWin.
func (c *conn) getUnackBlocks(ack_p []byte) []uint16 {
	var blocks []uint16

	// Read each byte
	read_bits := 8
	for i := 0; i < len(ack_p); i++ {
		// Last byte, read up to windowsize bits
		if i == len(ack_p)-1 {
			if rem := int(c.windowsize) % read_bits; rem != 0 {
				read_bits = rem
			}
		}

		// Read every bit of the current byte
		for j := 0; j < read_bits; j++ {
			index := i*8 + j

			// Last window may be smaller than windowsize
			last_window := len(c.txWin) - 1
			if c.done && uint8(index) > uint8(last_window) {
				break
			}

			tx_data := c.txWin[index]

			// If current bit equals to 1 get block number from txWin and update unackWin
			if (ack_p[i] & (1 << (7 - j))) != 0 {

				// Not already in unackWin, add it and update txBuf window.
				if idx := slices.IndexFunc(c.unackWin, func(b uint16) bool { return b == tx_data }); idx < 0 {
					c.unackWin = append(c.unackWin, tx_data)

					c.txBuf.AddSlotToWindow(index)
				}

				blocks = append(blocks, tx_data)
			} else {
				// Current bit equals to 0.
				// If current block was lost in previous transmissions,
				// remove it from txBuf window and unackWin.
				if idx := slices.IndexFunc(c.unackWin, func(b uint16) bool { return b == tx_data }); idx >= 0 {
					// Update txBuf window
					c.txBuf.RemoveSlotFromWindow(idx)

					c.unackWin = removeAt(c.unackWin, idx)
				}
			}
		}
	}

	// Update number of lost blocks.
	c.unackBlocks = len(c.unackWin)

	if c.unackBlocks > 0 {
		c.adone = false
	}

	return blocks
}

// Return true if the receiver lost all packets.
func (c *conn) windowLost(b []byte) bool {
	for _, v := range b {
		if v != 255 {
			return false
		}
	}
	return true
}

// getAck reads ACK, validates structure and checks for ERROR
func (c *conn) getAck() stateType {
	// Reset unackBlocks counter.
	c.unackBlocks = len(c.unackWin)

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
		c.adone = false

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
		c.ackTimeout = true
		c.adone = false
		return c.writeData
	case opCodeACK:
		// Set timeout equals to RTT + guardTime
		c.timeout = time.Duration(time.Since(c.txTime).Seconds()) + c.guardTime

		// Parse ACK bitstring
		ack_payload := c.rx.ack()
		if c.windowLost(ack_payload) {
			// All TX DATA packets were lost, retransmit last window
			// (as duplicate ACK case).
			c.ackTimeout = true
			c.adone = false

			c.log.trace("Received ACK, but no block was received. Resending last window.")
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
	timeout := c.timeout
	c.log.trace("timeout %d", timeout)

	// Expected time before next DATA packet of the window (server side).
	if !c.isClient && c.rxTimeout > 0 {
		timeout = c.rxTimeout
	}

	// TODO: remove, used only for testing with localhost --------------------
	if c.isClient {
		timeout += 10 * time.Second
	}
	c.log.trace("timeout %d", timeout)
	// TODO: remove, used only for testing with localhost --------------------

	// Server single port mode
	if c.reqChan != nil {
		// Setup timer
		if c.timer == nil {
			c.timer = time.NewTimer(timeout)
		} else {
			c.timer.Reset(timeout)
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
		if err = c.netConn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, wrapError(err, "setting network read deadline")
		}
		n, addr, err = c.netConn.ReadFrom(c.rx.buf)
	} else {
		// Read from the TCP external socket
		if err = c.tcpConn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
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

		// Waiting before next transmission avoids TCP
		// to read more bytes than it should.
		time.Sleep(1500 * time.Microsecond)
	}

	return err
}

// windowBuffer embeds a bytes.Buffer, adding the ability to read data
// from a window.
// Elements can be added to a specific index of the window or removed from it.
type windowBuffer struct {
	bytes.Buffer
	size      int    // Size of window slots
	lostSlots int    // Number of the left most elements in the window
	slotsLen  []int  // Length of each slot of the window
	buff      []byte // Window space
}

// newWindowBuffer initializes a new windowBuffer
func newWindowBuffer(windowsize int, slotsize int) *windowBuffer {
	return &windowBuffer{
		size:      slotsize,
		lostSlots: 0,
		slotsLen:  make([]int, windowsize),
		buff:      make([]byte, windowsize*slotsize),
	}
}

// Len returns bytes.Buffer.Len() + any window space up to lostSlots
func (w *windowBuffer) Len() int {
	window_len := 0
	for i := 0; i < w.lostSlots; i++ {
		window_len += w.slotsLen[i]
	}
	return w.Buffer.Len() + window_len
}

// RemoveSlotFromWindow overwrite the slot at index window with the
// slot at index window + 1.
func (w *windowBuffer) RemoveSlotFromWindow(window int) {
	// if w.lostSlots > 1 {
	if w.lostSlots > 1 {
		for i := window; i < w.lostSlots-1; i++ {
			n := w.updateWindow(i, i+1)
			w.slotsLen[i] = n
		}

		w.lostSlots--
	}
	// else {
	// 	w.lostSlots = 0
	// }
}

// AddSlotToWindow overwrite the slot at index lostSlots with
// the slot at index window.
func (w *windowBuffer) AddSlotToWindow(window int) {
	n := w.updateWindow(w.lostSlots, window)
	w.slotsLen[w.lostSlots] = n

	w.lostSlots++
}

func (w *windowBuffer) updateWindow(slots int, window int) int {
	dst_offset := slots * w.size
	dst_blen := dst_offset + w.slotsLen[window]

	src_offset := window * w.size
	src_blen := src_offset + w.slotsLen[window]

	n := copy(w.buff[dst_offset:dst_blen], w.buff[src_offset:src_blen])

	return n
}

func (w *windowBuffer) ReadFromWindow(p []byte, window int) (int, error) {
	if len(w.buff) == 0 {
		err := errors.New("Empty buffer")
		return 0, err
	}

	offset := window * w.size
	blen := offset + w.slotsLen[window]
	n := copy(p, w.buff[offset:blen])

	return n, nil
}

func (w *windowBuffer) Read(p []byte, window int) (int, error) {
	offset := window * w.size

	// Read from Buffer and copy read data into current slot
	// (len(p) == c.blksize)
	n, err := w.Buffer.Read(p)
	n = copy(w.buff[offset:offset+n], p[:n])
	w.slotsLen[window] = n

	return n, err
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

// removeAt removes an element of slice s at position index.
func removeAt[T dataBlock | uint16](s []T, index int) []T {
	ret := make([]T, 0, len(s)-1)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}

// insertAt adds element el to slice s at position index.
func insertAt(s []dataBlock, idx int, el dataBlock) []dataBlock {
	s = append(s[:idx+1], s[idx:]...)
	s[idx] = el
	return s
}
