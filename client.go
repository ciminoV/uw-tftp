// Copyright (C) 2017 Kale Blankenship. All rights reserved.
// This software may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details

package tftp // import "pack.ag/tftp"

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxFilenameLen = 45

// Client makes requests to a server.
type Client struct {
	log  *logger
	net  string // UDP network (ie, "udp", "udp4", "udp6")
	port int
	opts map[string]string // Map of TFTP options

	retransmit        int // Per-packet retransmission limit
	timeoutMultiplier int // Multiplier for the timeout entry

	tcpAddrStr string       // TCP address string
	tcpConn    *net.TCPConn // TCP connection socket
}

// NewClient returns a configured Client.
//
// Any number of ClientOpts can be provided to modify the default client behavior.
func NewClient(opts ...ClientOpt) (*Client, error) {
	// Copy default options into new map
	options := map[string]string{}
	for k, v := range defaultOptions {
		options[k] = v
	}

	c := &Client{
		log:               newLogger("client"),
		net:               defaultUDPNet,
		port:              -1,
		opts:              options,
		retransmit:        defaultRetransmit,
		timeoutMultiplier: defaultTimeoutMultiplier,
	}

	// Apply option functions to client
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return c, err
		}
	}

	if c.tcpAddrStr != "" {
		tcpClient, err := net.ResolveTCPAddr(defaultTCPNet, c.tcpAddrStr)
		if err != nil {
			return nil, wrapError(err, "resolve TCP address failed")
		}
		c.tcpConn, err = net.DialTCP(defaultTCPNet, nil, tcpClient)
		if err != nil {
			return nil, wrapError(err, "connecting to TCP address failed")
		}
	}

	return c, nil
}

// Get initiates a read request a server.
//
// URL is in the format tftp://[server]:[port]/[file]
func (c *Client) Get(url string) (*Response, error) {
	u, err := parseURL(url)
	if err != nil {
		return nil, err
	}

	// Create connection
	var conn *conn
	if c.tcpAddrStr == "" {
		conn, err = newConnFromHost(c.net, u.host, c.port, nil)
	} else {
		conn, err = newConnFromHost(c.net, u.host, c.port, c.tcpConn)
	}
	if err != nil {
		return nil, err
	}

	// Set retransmit
	conn.retransmit = c.retransmit

	conn.timeoutMultiplier = c.timeoutMultiplier

	if c.opts[optTimeout] != "" {
		timeout, err := strconv.ParseFloat(c.opts[optTimeout], 64)
		if err == nil {
			conn.timeout = time.Duration(c.timeoutMultiplier) * time.Duration(timeout) * time.Second
		}
	}

	// Initiate the request
	if err := conn.sendReadRequest(u.file, c.opts); err != nil {
		return nil, err
	}

	return &Response{conn: conn}, nil
}

// Put takes an io.Reader request a server.
//
// URL is in the format tftp://[server]:[port]/[file]
func (c *Client) Put(url string, r io.Reader, size int64) (err error) {
	u, err := parseURL(url)
	if err != nil {
		return err
	}

	// Create connection
	var conn *conn
	if c.tcpAddrStr != "" {
		conn, err = newConnFromHost(c.net, u.host, c.port, c.tcpConn)
	} else {
		conn, err = newConnFromHost(c.net, u.host, c.port, nil)
	}
	if err != nil {
		return err
	}
	defer func() {
		cErr := conn.Close()
		if err == nil {
			err = cErr
		}
	}()

	// Set retransmit
	conn.retransmit = c.retransmit

	conn.timeoutMultiplier = c.timeoutMultiplier

	if c.opts[optTimeout] != "" {
		timeout, err := strconv.ParseFloat(c.opts[optTimeout], 64)
		if err == nil {
			conn.timeout = time.Duration(c.timeoutMultiplier) * time.Duration(timeout) * time.Second
		}
	}

	// Check if tsize is enabled
	if _, ok := c.opts[optTransferSize]; ok {
		if size < 1 {
			// If size is <1, remove the option
			delete(c.opts, optTransferSize)
		} else {
			// Otherwise add the size as a string
			c.opts[optTransferSize] = fmt.Sprint(size)
		}
	}

	// Initiate the request
	if err := conn.sendWriteRequest(u.file, c.opts); err != nil {
		return err
	}

	// Write the data to the connections
	_, err = io.Copy(conn, r)

	return err
}

// parsedURL holds the result of parseURL
type parsedURL struct {
	host string
	file string
}

// parsedURL takes a string with the format "[server]:[port]/[file]"
// and splits it into host and file.
//
// If port is not specified, defaultPort will be used.
func parseURL(tftpURL string) (*parsedURL, error) {
	if tftpURL == "" {
		return nil, ErrInvalidURL
	}
	const kTftpPrefix = "tftp://"
	if !strings.HasPrefix(tftpURL, kTftpPrefix) {
		tftpURL = kTftpPrefix + tftpURL
	}
	u, err := url.Parse(tftpURL)
	if err != nil {
		return nil, err
	}

	file := u.RequestURI()
	if u.Fragment != "" {
		file = file + "#" + u.Fragment
	}
	p := &parsedURL{
		host: u.Hostname(),
		file: strings.TrimPrefix(file, "/"),
	}

	// Cut the filename if too long
	if len(p.file) > maxFilenameLen {
		p.file = p.file[:maxFilenameLen]
	}

	if p.host == "" {
		return nil, ErrInvalidHostIP
	}
	if isNumeric(p.host) {
		return nil, ErrInvalidHostIP
	}

	if p.file == "" {
		return nil, ErrInvalidFile
	}

	port := u.Port()
	if port == "" {
		port = defaultPort
	}
	if !isNumeric(port) {
		return nil, ErrInvalidHostIP
	}
	p.host = net.JoinHostPort(p.host, port)
	return p, nil
}

func isNumeric(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}

// Response is an io.Reader for receiving files from a TFTP server.
type Response struct {
	conn *conn
}

// Size returns the transfer size as indicated by the server in the tsize option.
//
// ErrSizeNotReceived will be returned if tsize option was not enabled.
func (r *Response) Size() (int64, error) {
	if r.conn.tsize == nil {
		return 0, ErrSizeNotReceived
	}
	return *r.conn.tsize, nil
}

func (r *Response) Read(p []byte) (int, error) {
	return r.conn.Read(p)
}

// ClientOpt is a function that configures a Client.
type ClientOpt func(*Client) error

// ClientMode configures the mode.
//
// Valid options are ModeNetASCII and ModeOctet. Default is ModeNetASCII.
func ClientMode(mode TransferMode) ClientOpt {
	return func(c *Client) error {
		if mode != ModeNetASCII && mode != ModeOctet {
			return ErrInvalidMode
		}
		c.opts[optMode] = string(mode)
		return nil
	}
}

// ClientBlocksize configures the number of data bytes that will be send in each datagram.
// Valid range is 8 to 247.
// The max value consider the 3B header plus a minum underwater header of 5B
//
// Default: 56.
func ClientBlocksize(size int) ClientOpt {
	return func(c *Client) error {
		if size < 8 || size > 247 {
			return ErrInvalidBlocksize
		}
		c.opts[optBlocksize] = strconv.Itoa(size)
		return nil
	}
}

// ClientTimeout configures the number of seconds to wait before resending an unacknowledged datagram.
// Valid range is 1 to 255.
//
// Default: 60.
func ClientTimeout(seconds int) ClientOpt {
	return func(c *Client) error {
		if seconds < 1 || seconds > 255 {
			return ErrInvalidTimeout
		}
		c.opts[optTimeout] = strconv.Itoa(seconds)
		return nil
	}
}

// ClientWindowsize configures the number of datagrams that will be transmitted before needing an acknowledgement.
//
// Default: 1.
func ClientWindowsize(window int) ClientOpt {
	return func(c *Client) error {
		if window < 1 || window > 255 {
			return ErrInvalidWindowsize
		}
		c.opts[optWindowSize] = strconv.Itoa(window)
		return nil
	}
}

// ClientTransferSize requests for the server to send the file size before sending.
//
// Default: enabled.
func ClientTransferSize(enable bool) ClientOpt {
	return func(c *Client) error {
		if enable {
			c.opts[optTransferSize] = "0"
		} else {
			delete(c.opts, optTransferSize)
		}
		return nil
	}
}

// ClientRetransmit configures the per-packet retransmission limit for all requests.
//
// Default: 10.
func ClientRetransmit(i int) ClientOpt {
	return func(c *Client) error {
		if i < 0 {
			return ErrInvalidRetransmit
		}
		c.retransmit = i
		return nil
	}
}

// ClientPort configures the udp port number for the client.
// (useless feature should be removed)
//
// Default: -1
func ClientPort(port int) ClientOpt {
	return func(c *Client) error {
		if port < 1024 {
			return ErrInvalidClientPort
		}
		c.port = port
		return nil
	}
}

// ClientTcpForward forwards all incoming/outgoing packets to an external application
// listening on a tcp socket.
//
// Default is disabled (empty string)
func ClientTcpForward(tcpAddr string) ClientOpt {
	return func(c *Client) error {
		c.tcpAddrStr = tcpAddr
		return nil
	}
}

// ClientTimeoutMultiplier configures the multiplier of the timeout.
// Valid range is 1 to 255.
//
// Default: 1.
func ClientTimeoutMultiplier(multiplier int) ClientOpt {
	return func(c *Client) error {
		if multiplier < 0 {
			return ErrInvalidTimeOutMultiplier
		}
		c.timeoutMultiplier = multiplier
		return nil
	}
}
