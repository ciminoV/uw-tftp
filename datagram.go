// Copyright (C) 2017 Kale Blankenship. All rights reserved.
// This software may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details

package tftp // import "pack.ag/tftp"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type opcode uint8

func (o opcode) String() string {
	name, ok := opcodeStrings[o]
	if ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN_OPCODE_%v", uint16(o))
}

// ErrorCode is a TFTP error code as defined in RFC 1350
type ErrorCode uint8

func (e ErrorCode) String() string {
	name, ok := errorStrings[e]
	if ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN_ERROR_%v", uint16(e))
}

const (
	opCodeRRQ   opcode = 0x1 // Read Request
	opCodeWRQ   opcode = 0x2 // Write Request
	opCodeDATA  opcode = 0x3 // Data
	opCodeACK   opcode = 0x4 // Acknowledgement
	opCodeERROR opcode = 0x5 // Error
	opCodeOACK  opcode = 0x6 // Option Acknowledgement

	// ErrCodeNotDefined - Not defined, see error message (if any).
	ErrCodeNotDefined ErrorCode = 0x0
	// ErrCodeFileNotFound - File not found.
	ErrCodeFileNotFound ErrorCode = 0x1
	// ErrCodeAccessViolation - Access violation.
	ErrCodeAccessViolation ErrorCode = 0x2
	// ErrCodeDiskFull - Disk full or allocation exceeded.
	ErrCodeDiskFull ErrorCode = 0x3
	// ErrCodeIllegalOperation - Illegal TFTP operation.
	ErrCodeIllegalOperation ErrorCode = 0x4
	// ErrCodeUnknownTransferID - Unknown transfer ID.
	ErrCodeUnknownTransferID ErrorCode = 0x5
	// ErrCodeFileAlreadyExists - File already exists.
	ErrCodeFileAlreadyExists ErrorCode = 0x6
	// ErrCodeNoSuchUser - No such user.
	ErrCodeNoSuchUser ErrorCode = 0x7

	// ModeNetASCII is the string for netascii transfer mode
	ModeNetASCII TransferMode = "NA"
	// ModeOctet is the string for octet/binary transfer mode
	ModeOctet TransferMode = "OT"

	optBlocksize    = "B"
	optTransferSize = "F"
	optWindowSize   = "W"
	optTimeout      = "T"
	optMode         = "M"

	sizeofOpcode  = 1 // Size of a opcode in bytes
	sizeofWindow  = 1 // Size of a window
	sizeofErrcode = 2 // Size of an error code in bytes
	sizeofBlock   = 2 // Size of a block number in bytes
	sizeofHdr     = sizeofOpcode + sizeofWindow + sizeofBlock
	sizeofErrHdr  = sizeofOpcode + sizeofErrcode
)

// TransferMode is a TFTP transer mode
type TransferMode string

var (
	errorStrings = map[ErrorCode]string{
		ErrCodeNotDefined:        "NOT_DEFINED",
		ErrCodeFileNotFound:      "FILE_NOT_FOUND",
		ErrCodeAccessViolation:   "ACCESS_VIOLATION",
		ErrCodeDiskFull:          "DISK_FULL",
		ErrCodeIllegalOperation:  "ILLEGAL_OPERATION",
		ErrCodeUnknownTransferID: "UNKNOWN_TRANSFER_ID",
		ErrCodeFileAlreadyExists: "FILE_ALREADY_EXISTS",
		ErrCodeNoSuchUser:        "NO_SUCH_USER",
	}
	opcodeStrings = map[opcode]string{
		opCodeRRQ:   "READ_REQUEST",
		opCodeWRQ:   "WRITE_REQUEST",
		opCodeDATA:  "DATA",
		opCodeACK:   "ACK",
		opCodeERROR: "ERROR",
		opCodeOACK:  "OPTION_ACK",
	}
)

type datagram struct {
	buf    []byte
	offset int
}

func (d datagram) String() string {
	if err := d.validate(); err != nil {
		return fmt.Sprintf("INVALID_DATAGRAM[Error: %q]", err.Error())
	}

	switch o := d.opcode(); o {
	case opCodeRRQ, opCodeWRQ:
		return fmt.Sprintf("%s[Filename: %q; Options: %s]", o, d.filename(), d.options())
	case opCodeDATA:
		return fmt.Sprintf("%s[Window: %d; Block: %d; Length: %d]", o, d.window(), d.block(), len(d.data()))
	case opCodeOACK:
		return fmt.Sprintf("%s[Options: %s]", o, d.options())
	case opCodeACK:
		return fmt.Sprintf("%s[Bitmask: %08b]", o, d.ack())
	case opCodeERROR:
		return fmt.Sprintf("%s[Code: %s; Message: %q]", o, d.errorCode(), d.errMsg())
	default:
		return o.String()
	}
}

// Sets the buffer from raw bytes
func (d *datagram) setBytes(b []byte) {
	d.buf = b
	d.offset = len(b)
}

// Returns the allocated bytes
func (d *datagram) bytes() []byte {
	return d.buf[:d.offset]
}

// Returns the allocated bytes within a range
func (d *datagram) getBytes(beg int, end int) []byte {
	if beg > end || d.offset < beg {
		return d.buf[:d.offset]
	}
	if d.offset < end && d.offset > beg {
		return d.buf[beg:d.offset]
	}

	return d.buf[beg:end]
}

// Resets the byte buffer.
// If requested size is larger than allocated the buffer is reallocated.
func (d *datagram) reset(size int) {
	if len(d.buf) < size {
		d.buf = make([]byte, size)
	}
	d.offset = 0
}

// DATAGRAM CONSTRUCTORS

// Write an ack packet
//
// Assume bitmask is padded
func (d *datagram) writeAck(bitmask []byte) {
	d.reset(sizeofOpcode + len(bitmask)/8)

	d.writeUint8(uint8(opCodeACK))
	d.writeBinaryString(bitmask)
}

// Write a data packet (block)
func (d *datagram) writeData(window uint8, block uint16, data []byte) {
	d.reset(sizeofOpcode + sizeofWindow + sizeofBlock + len(data))

	d.writeUint8(uint8(opCodeDATA))
	d.writeUint8(window)
	d.writeUint16(block)
	d.writeBytes(data)
}

func (d *datagram) writeError(code ErrorCode, msg string) {
	d.reset(sizeofOpcode + sizeofErrcode + len(msg) + 1)

	d.writeUint8(uint8(opCodeERROR))
	d.writeUint16(uint16(code))
	d.writeString(msg)
	d.writeNull()
}

func (d *datagram) writeOptionAck(options map[string]string) {
	optLen := 0
	for opt, val := range options {
		optLen += len(opt) + len(val) + 1
	}
	d.reset(sizeofOpcode + optLen)

	d.writeUint8(uint8(opCodeOACK))

	for opt, val := range options {
		d.writeOption(opt, val)
	}
}

func (d *datagram) writeReadReq(filename string, mode TransferMode, options map[string]string) {
	d.writeReq(opCodeRRQ, filename, mode, options)
}

func (d *datagram) writeWriteReq(filename string, mode TransferMode, options map[string]string) {
	d.writeReq(opCodeWRQ, filename, mode, options)
}

// Combines duplicate logic from RRQ and WRQ
func (d *datagram) writeReq(o opcode, filename string, mode TransferMode, options map[string]string) {
	optLen := 0
	for opt, val := range options {
		optLen += len(opt) + len(val) + 1
	}
	d.reset(sizeofOpcode + len(filename) + 1 + optLen)

	d.writeUint8(uint8(o))
	d.writeString(filename)
	d.writeNull()

	for opt, val := range options {
		d.writeOption(opt, val)
	}
}

// FIELD ACCESSORS

// Lost blocks from ACK datagram
func (d *datagram) ack() []byte {
	return d.buf[sizeofOpcode:d.offset]
}

// Window index from DATA datagram
func (d *datagram) window() uint8 {
	return d.buf[sizeofOpcode]
}

// Block # from DATA datagram
func (d *datagram) block() uint16 {
	return binary.BigEndian.Uint16(d.buf[sizeofOpcode+sizeofWindow : sizeofHdr])
}

// Data from DATA datagram
func (d *datagram) data() []byte {
	return d.buf[sizeofHdr:d.offset]
}

// ErrorCode from ERROR datagram
func (d *datagram) errorCode() ErrorCode {
	return ErrorCode(binary.BigEndian.Uint16(d.buf[sizeofOpcode:sizeofErrHdr]))
}

// ErrMsg from ERROR datagram
func (d *datagram) errMsg() string {
	end := d.offset - 1
	return string(d.buf[sizeofErrHdr:end])
}

// Filename from RRQ and WRQ datagrams
func (d *datagram) filename() string {
	offset := bytes.IndexByte(d.buf[sizeofOpcode:], 0x0) + sizeofOpcode
	return string(d.buf[sizeofOpcode:offset])
}

// Mode from RRQ and WRQ datagrams
func (d *datagram) mode() TransferMode {
	idx := bytes.IndexAny(d.bytes(), optMode)
	if idx < 0 {
		return TransferMode(defaultMode)
	}
	idx += 1 // skip the key string
	return TransferMode(d.bytes()[idx : idx+len(ModeOctet)])
}

// Opcode from all datagrams
func (d *datagram) opcode() opcode {
	return opcode(d.buf[0])
}

type options map[string]string

func (o options) String() string {
	opts := make([]string, 0, len(o))
	for k, v := range o {
		opts = append(opts, fmt.Sprintf("%q: %q", k, v))
	}

	return "{" + strings.Join(opts, "; ") + "}"
}

// options from RRQ and WRQ datagrams
func (d *datagram) options() options {
	options := make(options)

	optSlice := bytes.Split(d.buf[sizeofOpcode:d.offset-1], []byte{0x0}) // d.buf[2:d.offset-1] = file -> just before final NULL
	if op := d.opcode(); op == opCodeRRQ || op == opCodeWRQ {
		optSlice = optSlice[1:] // Remove filename
	}

	// Each option key is one character
	for i := 0; i < len(optSlice); i++ {
		if len(optSlice[i]) == 0 {
			continue
		}
		switch string(optSlice[i][0]) {
		case optBlocksize, optWindowSize, optTimeout:
			options[string(optSlice[i][0])] = fmt.Sprintf("%d", uint8(optSlice[i][1]))
			break
		case optTransferSize, optMode:
			options[string(optSlice[i][0])] = string(optSlice[i][1:])
			break
		default:
		}
	}
	return options
}

// BUFFER WRITING FUNCTIONS

// Write bytes
func (d *datagram) writeBytes(b []byte) {
	copy(d.buf[d.offset:], b)
	d.offset += len(b)
}

// Write null byte
func (d *datagram) writeNull() {
	d.buf[d.offset] = 0x0
	d.offset++
}

func (d *datagram) writeString(str string) {
	d.writeBytes([]byte(str))
}

// Write binary string
// Convert a binary string to bytes and write to datagram buffer
//
// Note: It assumes the string is padded (len(b) % 8 == 0)
func (d *datagram) writeBinaryString(b []byte) {
	bytelen := len(b) / 8
	var dst []byte = make([]byte, bytelen)
	var bitMask byte = 1

	// Store 8 elements of b in a single byte
	bitCounter := 0
	for i := 0; i < bytelen; i++ {
		for bit := 0; bit < 8; bit++ {
			dst[i] |= (b[bitCounter] & bitMask) << (7 - bit)
			bitCounter++
		}
	}

	// Write dst to datagram
	d.writeBytes(dst)
}

// Write uint16 using bigendian
func (d *datagram) writeUint16(i uint16) {
	binary.BigEndian.PutUint16(d.buf[d.offset:], i)
	d.offset += 2
}

// Write uint8
func (d *datagram) writeUint8(i uint8) {
	d.buf[d.offset] = i
	d.offset++
}

// Write rrq/wrq options
func (d *datagram) writeOption(o string, v string) {
	d.writeString(o)
	switch o {
	case optBlocksize, optWindowSize, optTimeout:
		size, _ := strconv.ParseUint(v, 10, 8)
		d.writeUint8(uint8(size))
		break
	default:
		d.writeString(v)
	}
	d.writeNull()
}

// Validate header
func (d *datagram) validate() error {
	switch {
	case d.offset < sizeofOpcode:
		return errors.New("Datagram has no opcode")
	case d.opcode() > 6:
		return errors.New("Invalid opcode")
	}

	switch d.opcode() {
	case opCodeRRQ, opCodeWRQ:
		switch {
		case len(d.filename()) < 1:
			return errors.New("No filename provided")
		case d.buf[d.offset-1] != 0x0: // End with NULL
			return fmt.Errorf("Corrupt %v datagram", d.opcode())
		default:
			switch d.mode() {
			case ModeNetASCII, ModeOctet:
				break
			default:
				return errors.New("Invalid transfer mode")
			}
		}
	case opCodeDATA:
		if d.offset < sizeofHdr {
			return errors.New("Corrupt block number")
		}
	case opCodeERROR:
		switch {
		case d.offset < sizeofErrHdr+1:
			return errors.New("Corrupt ERROR datagram")
		case d.buf[d.offset-1] != 0x0:
			return errors.New("Corrupt ERROR datagram")
		case bytes.Count(d.buf[sizeofErrHdr:d.offset], []byte{0x0}) > 1:
			return errors.New("Corrupt ERROR datagram")
		}
	case opCodeOACK:
		switch {
		case d.buf[d.offset-1] != 0x0:
			return errors.New("Corrupt OACK datagram")
		}
	}

	return nil
}
