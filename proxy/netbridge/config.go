package netbridge

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
)

const (
	NbMagic   = 0x4E425632 // "NBv2" LE
	NbVersion = 1

	NbAddrIPv4 = 0x04
	NbAddrIPv6 = 0x06

	NbProtoTCP = 6
	NbProtoUDP = 17

	NbErrVersion = 0x01
	NbErrToken   = 0x02

	NbTcpHeaderBaseSize = 36
	NbUdpReqHeaderSize  = 56
	NbUdpRespHeaderSize = 32
	NbErrorSize         = 8
)

// NbTcpHeader is the binary header sent by ProxyBridgeCore for TCP connections.
type NbTcpHeader struct {
	Magic       uint32
	Version     uint8
	AddrType    uint8
	Protocol    uint8
	ProcNameLen uint8
	DstPort     uint16
	SrcPort     uint16
	DstAddr     [16]byte
	Pid         uint32
	Token       uint32
	ProcName    string
}

// NbUdpReqHeader is the binary header for UDP requests (56 bytes fixed).
type NbUdpReqHeader struct {
	Magic      uint32
	Version    uint8
	AddrType   uint8
	Protocol   uint8
	Reserved   uint8
	DstPort    uint16
	SrcPort    uint16
	DstAddr    [16]byte
	SrcAddr    [16]byte
	Pid        uint32
	Token      uint32
	PayloadLen uint16
	Reserved2  uint16
}

// NbUdpRespHeader is the binary response header for UDP (32 bytes fixed).
type NbUdpRespHeader struct {
	Magic      uint32
	Version    uint8
	AddrType   uint8
	Reserved   [2]byte
	SrcPort    uint16
	Reserved2  uint16
	SrcAddr    [16]byte
	PayloadLen uint16
	Reserved3  uint16
}

// ParseTcpHeader reads a NetBridge TCP header from the connection.
func ParseTcpHeader(r io.Reader) (*NbTcpHeader, error) {
	base := make([]byte, NbTcpHeaderBaseSize)
	if _, err := io.ReadFull(r, base); err != nil {
		return nil, fmt.Errorf("read base header: %w", err)
	}

	h := &NbTcpHeader{
		Magic:       binary.LittleEndian.Uint32(base[0:4]),
		Version:     base[4],
		AddrType:    base[5],
		Protocol:    base[6],
		ProcNameLen: base[7],
		DstPort:     binary.LittleEndian.Uint16(base[8:10]),
		SrcPort:     binary.LittleEndian.Uint16(base[10:12]),
		Pid:         binary.LittleEndian.Uint32(base[28:32]),
		Token:       binary.LittleEndian.Uint32(base[32:36]),
	}
	copy(h.DstAddr[:], base[12:28])

	if h.ProcNameLen > 0 {
		nameBuf := make([]byte, h.ProcNameLen)
		if _, err := io.ReadFull(r, nameBuf); err != nil {
			return nil, fmt.Errorf("read proc_name: %w", err)
		}
		h.ProcName = string(nameBuf)
		total := NbTcpHeaderBaseSize + int(h.ProcNameLen)
		pad := (4 - total%4) % 4
		if pad > 0 {
			io.ReadFull(r, make([]byte, pad))
		}
	}

	return h, nil
}

// ParseUdpReqHeader parses a NetBridge UDP request header from data.
func ParseUdpReqHeader(data []byte) (*NbUdpReqHeader, []byte, error) {
	if len(data) < NbUdpReqHeaderSize {
		return nil, nil, fmt.Errorf("packet too short: %d < %d", len(data), NbUdpReqHeaderSize)
	}

	h := &NbUdpReqHeader{
		Magic:      binary.LittleEndian.Uint32(data[0:4]),
		Version:    data[4],
		AddrType:   data[5],
		Protocol:   data[6],
		DstPort:    binary.LittleEndian.Uint16(data[8:10]),
		SrcPort:    binary.LittleEndian.Uint16(data[10:12]),
		Pid:        binary.LittleEndian.Uint32(data[44:48]),
		Token:      binary.LittleEndian.Uint32(data[48:52]),
		PayloadLen: binary.LittleEndian.Uint16(data[52:54]),
	}
	copy(h.DstAddr[:], data[12:28])
	copy(h.SrcAddr[:], data[28:44])

	if int(h.PayloadLen) > len(data)-NbUdpReqHeaderSize {
		return nil, nil, fmt.Errorf("payload_len %d exceeds available data %d", h.PayloadLen, len(data)-NbUdpReqHeaderSize)
	}

	return h, data[NbUdpReqHeaderSize : NbUdpReqHeaderSize+int(h.PayloadLen)], nil
}

// BuildNbUdpRespHeader creates a UDP response header.
func BuildNbUdpRespHeader(addrType uint8, srcPort uint16, srcAddr [16]byte, payloadLen uint16) []byte {
	b := make([]byte, NbUdpRespHeaderSize)
	binary.LittleEndian.PutUint32(b[0:4], NbMagic)
	b[4] = NbVersion
	b[5] = addrType
	binary.LittleEndian.PutUint16(b[8:10], srcPort)
	copy(b[12:28], srcAddr[:])
	binary.LittleEndian.PutUint16(b[28:30], payloadLen)
	return b
}

// SendNbError sends an error packet on TCP.
func SendNbError(w io.Writer, code uint8) {
	b := make([]byte, NbErrorSize)
	binary.LittleEndian.PutUint32(b[0:4], NbMagic)
	b[4] = NbVersion
	b[5] = code
	w.Write(b)
}

// DstNetAddress converts the header's destination to an xray net.Address.
func (h *NbTcpHeader) DstNetAddress() xnet.Address {
	if h.AddrType == NbAddrIPv4 {
		return xnet.IPAddress(h.DstAddr[:4])
	}
	return xnet.IPAddress(h.DstAddr[:])
}

// Config is the internal configuration for netbridge inbound.
type Config struct {
	ListenAddress string
	ListenPort    uint32
	UDPPort       uint32
	Token         uint32
	UserLevel     uint32
}

// validateListenAddress ensures the listen address is loopback-only.
func (c *Config) validateListenAddress() error {
	ip := net.ParseIP(c.ListenAddress)
	if ip == nil {
		return fmt.Errorf("netbridge: invalid listen address %q", c.ListenAddress)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("netbridge: SECURITY VIOLATION — listen address %q is not loopback. "+
			"Only 127.0.0.1 or ::1 is allowed", c.ListenAddress)
	}
	return nil
}

// Unused import guard
var _ = time.Now
