// Copyright (C) 2017 Kale Blankenship. All rights reserved.
// This software may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details

package tftp // import "pack.ag/tftp"

import (
	"net"
	"sync"
	"time"
)

// Server contains the configuration to run a TFTP server.
//
// A ReadHandler, WriteHandler, or both can be registered to the server. If one
// of the handlers isn't registered, the server will return errors to clients
// attempting to use them.
type Server struct {
	log        *logger
	net        string       // UDP network
	addrStr    string       // UDP address string
	tcpAddrStr string       // TCP address string
	addr       *net.UDPAddr // UDP server address
	connMu     sync.RWMutex
	conn       *net.UDPConn
	tcpConn    *net.TCPConn // TCP connection socket
	close      chan struct{}

	singlePort bool

	dispatchChan chan *request
	reqDoneChan  chan string

	retransmit        int // Per-packet retransmission limit
	timeoutMultiplier int // Multiplier for the timeout entry

	rh ReadHandler
	wh WriteHandler
}

type request struct {
	addr *net.UDPAddr
	pkt  []byte
}

// NewServer returns a configured Server.
//
// Addr is the network address to listen on and is in the form "host:port".
// If a no host is given the server will listen on all interfaces.
//
// Any number of ServerOpts can be provided to configure optional values.
func NewServer(addr string, opts ...ServerOpt) (*Server, error) {
	s := &Server{
		log:               newLogger("server"),
		net:               defaultUDPNet,
		addrStr:           addr,
		retransmit:        defaultRetransmit,
		dispatchChan:      make(chan *request, 64),
		reqDoneChan:       make(chan string, 64),
		close:             make(chan struct{}),
		timeoutMultiplier: defaultTimeoutMultiplier,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	return s, nil
}

// Addr is the network address of the server. It is available
// after the server has been started.
func (s *Server) Addr() (*net.UDPAddr, error) {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	if s.conn == nil {
		return nil, ErrAddressNotAvailable
	}
	return s.conn.LocalAddr().(*net.UDPAddr), nil
}

// ReadHandler registers a ReadHandler for the server.
func (s *Server) ReadHandler(rh ReadHandler) {
	s.rh = rh
}

// WriteHandler registers a WriteHandler for the server.
func (s *Server) WriteHandler(wh WriteHandler) {
	s.wh = wh
}

// ListenAndServe starts a configured server.
func (s *Server) ListenAndServe() error {
	var conn *net.UDPConn
	if s.tcpAddrStr != "" {
		tcpServer, err := net.ResolveTCPAddr(defaultTCPNet, s.tcpAddrStr)
		if err != nil {
			return wrapError(err, "resolve TCP address failed")
		}
		s.tcpConn, err = net.DialTCP(defaultTCPNet, nil, tcpServer)
		if err != nil {
			return wrapError(err, "connecting to TCP address failed")
		}
	} else {
		addr, err := net.ResolveUDPAddr(s.net, s.addrStr)
		if err != nil {
			return wrapError(err, "resolving server address")
		}
		s.addr = addr

		conn, err = net.ListenUDP(s.net, s.addr)
		if err != nil {
			return wrapError(err, "opening network connection")
		}
	}

	return wrapError(s.Serve(conn), "serving tftp")
}

// Serve starts the server using an existing UDPConn.
func (s *Server) Serve(conn *net.UDPConn) error {
	if s.rh == nil && s.wh == nil {
		return ErrNoRegisteredHandlers
	}

	s.connMu.Lock()
	s.conn = conn
	s.connMu.Unlock()

	go s.connManager()

	s.connMu.RLock()
	defer s.connMu.RUnlock()
	buf := make([]byte, 65536) // Largest possible TFTP datagram
	offset := 0
	for {
		select {
		case <-s.close:
			return nil
		default:
			var err error
			var n int
			var addr *net.UDPAddr

			if s.tcpAddrStr == "" {
				// Read from UDP server
				conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				n, addr, err = conn.ReadFromUDP(buf[offset:])

			} else {
				// Read from TCP socket
				s.tcpConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				n, err = s.tcpConn.Read(buf[offset:])

				// Only needed for debug purposes
				tcpAddr := s.tcpConn.RemoteAddr().(*net.TCPAddr)
				addr = &net.UDPAddr{
					IP:   tcpAddr.IP,
					Port: tcpAddr.Port,
				}
			}

			// Fragmented WRQ/RRQ
			if n > 0 && (buf[0] == byte(opCodeRRQ) || buf[0] == byte(opCodeWRQ)) {
				if buf[offset+n-1] != 0x0 {
					offset += n
					s.log.trace("Incomplete request from %v: %d bytes received", addr, offset)
					continue
				}

				n += offset
				offset = 0
				s.log.trace("New request from %v: %d bytes received", addr, n)
			}

			if err != nil {
				if err, ok := err.(*net.OpError); ok && err.Timeout() {
					continue
				}
				return wrapError(err, "reading from conn")
			}

			if n < sizeofOpcode {
				continue // Must be at least 1 byte to read opcode
			}

			// Make a copy of the received data
			req := &request{
				addr: addr,
				pkt:  make([]byte, n),
			}
			copy(req.pkt, buf)
			s.dispatchChan <- req
		}
	}
}

func (s *Server) connManager() {
	reqMap := make(map[string]chan []byte)
	var reqChan chan []byte

	for {
		select {
		case req := <-s.dispatchChan:
			switch req.pkt[0] {
			case byte(opCodeRRQ): // RRQ
				if s.singlePort {
					reqChan = make(chan []byte, 64)
					reqMap[req.addr.String()] = reqChan
				}
				go s.dispatchReadRequest(req, reqChan)
			case byte(opCodeWRQ): // WRQ
				if s.singlePort {
					reqChan = make(chan []byte, 64)
					reqMap[req.addr.String()] = reqChan
				}
				go s.dispatchWriteRequest(req, reqChan)
			default:
				if s.singlePort {
					if reqChan, ok := reqMap[req.addr.String()]; ok {
						reqChan <- req.pkt
						break
					}
				}

				// RFC1350:
				// "If a source TID does not match, the packet should be
				// discarded as erroneously sent from somewhere else.  An error packet
				// should be sent to the source of the incorrect packet, while not
				// disturbing the transfer."
				dg := datagram{}
				dg.writeError(ErrCodeUnknownTransferID, "Unexpected TID")
				// Don't care about an error here, just a courtesy
				if s.conn != nil {
					_, _ = s.conn.WriteTo(dg.bytes(), req.addr)
				}
				s.log.debug("Unexpected datagram: %s", dg)
			}
		case addr := <-s.reqDoneChan:
			delete(reqMap, addr)
		case <-s.close:
			return
		}
	}
}

// Connected is true if the server has started serving.
func (s *Server) Connected() bool {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.conn != nil
}

// Close stops the server and closes the network connection.
func (s *Server) Close() error {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	close(s.close)
	return s.conn.Close()
}

// dispatchReadRequest dispatches the read handler, if it is registered.
// If a handler is not registered the server sends an error to the client.
func (s *Server) dispatchReadRequest(req *request, reqChan chan []byte) {
	// Check for handler
	if s.rh == nil {
		s.log.debug("No read handler registered.")
		var err datagram
		err.writeError(ErrCodeIllegalOperation, "Server does not support read requests.")
		_, _ = s.conn.WriteTo(err.bytes(), req.addr) // Ignore error
		return
	}

	c, closer, err := s.newConn(req, reqChan)
	if err != nil {
		return
	}
	defer errorDefer(closer, s.log, "error closing network connection in dispath")

	s.log.debug("New request from %v: %s", req.addr, c.rx)

	// Create request
	w := &readRequest{conn: c, name: c.rx.filename()}

	// execute handler
	s.rh.ServeTFTP(w)
}

// dispatchWriteRequest dispatches the write handler, if it is registered.
// If a handler is not registered the server sends an error to the client.
func (s *Server) dispatchWriteRequest(req *request, reqChan chan []byte) {
	// Check for handler
	if s.wh == nil {
		s.log.debug("No write handler registered.")
		var err datagram
		err.writeError(ErrCodeIllegalOperation, "Server does not support write requests.")
		_, _ = s.conn.WriteTo(err.bytes(), req.addr) // Ignore error
		return
	}

	c, closer, err := s.newConn(req, reqChan)
	if err != nil {
		return
	}
	defer errorDefer(closer, s.log, "error closing network connection in dispath")

	s.log.debug("New request from %v: %s", req.addr, c.rx)

	// Create request
	w := &writeRequest{conn: c, name: c.rx.filename()}
	s.wh.ReceiveTFTP(w)

	// No error occurred
	if !c.done {
		c.log.trace("Performing write setup")
		c.readSetup()
	}
}

func (s *Server) newConn(req *request, reqChan chan []byte) (*conn, func() error, error) {
	var c *conn
	var err error
	var dg datagram

	dg.setBytes(req.pkt)

	// Validate request datagram
	if err := dg.validate(); err != nil {
		s.log.debug("Error decoding new request: %v", err)
		return nil, nil, err
	}

	if s.singlePort {
		if s.tcpAddrStr != "" {
			c = newSinglePortConn(req.addr, s.conn, s.tcpConn, reqChan)
		} else {
			c = newSinglePortConn(req.addr, s.conn, nil, reqChan)
		}
	} else {
		// Use empty mode until request has been parsed.
		if s.tcpAddrStr != "" {
			c, err = newConn(s.net, req.addr, s.tcpConn)
		} else {
			c, err = newConn(s.net, req.addr, nil)
		}
		if err != nil {
			s.log.err("Received error opening connection for new request: %v", err)
			return nil, nil, err
		}
	}

	c.rx = dg
	// Set retransmit
	c.retransmit = s.retransmit

	c.timeoutMultiplier = s.timeoutMultiplier

	closer := func() error {
		err := c.Close()
		if s.singlePort {
			s.reqDoneChan <- req.addr.String()
		}
		return err
	}

	return c, closer, nil
}

// ServerOpt is a function that configures a Server.
type ServerOpt func(*Server) error

// ServerNet configures the network a server listens on.
// Must be one of: udp, udp4, udp6.
//
// Default: udp.
func ServerNet(net string) ServerOpt {
	return func(s *Server) error {
		if net != "udp" && net != "udp4" && net != "udp6" {
			return ErrInvalidNetwork
		}
		s.net = net
		return nil
	}
}

// ServerRetransmit configures the per-packet retransmission limit for all requests.
//
// Default: 5.
func ServerRetransmit(i int) ServerOpt {
	return func(s *Server) error {
		if i < 0 {
			return ErrInvalidRetransmit
		}
		s.retransmit = i
		return nil
	}
}

// ServerSinglePort enables the server to service all requests via a single port rather
// than the standard TFTP behavior of each client communicating on a separate port.
//
// This is an experimental feature.
//
// Default is disabled.
func ServerSinglePort(enable bool) ServerOpt {
	return func(s *Server) error {
		s.singlePort = enable
		return nil
	}
}

// ServerTcpForward forwards all incoming/outgoing packets to an external application
// listening on a tcp socket.
//
// Default is disabled (empty string)
func ServerTcpForward(tcpAddr string) ServerOpt {
	return func(s *Server) error {
		s.tcpAddrStr = tcpAddr
		return nil
	}
}

// ServerTimeoutMultiplier configures the multiplier of the timeout.
// Valid range is 1 to 255.
//
// Default: 1.
func ServerTimeoutMultiplier(multiplier int) ServerOpt {
	return func(s *Server) error {
		if multiplier < 0 {
			return ErrInvalidTimeOutMultiplier
		}
		s.timeoutMultiplier = multiplier
		return nil
	}
}
