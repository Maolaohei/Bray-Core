package netbridge

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// ===== TCP Header Tests =====

func TestParseTcpHeader_IPv4(t *testing.T) {
	buf := new(bytes.Buffer)
	// Base header: magic, version, addr_type, protocol, proc_name_len=0
	hdr := NbTcpHeader{
		Magic:       NbMagic,
		Version:     NbVersion,
		AddrType:    NbAddrIPv4,
		Protocol:    NbProtoTCP,
		ProcNameLen: 0,
		DstPort:     443,
		SrcPort:     54321,
		Pid:         1234,
		Token:       0xDEADBEEF,
	}
	hdr.DstAddr[0], hdr.DstAddr[1], hdr.DstAddr[2], hdr.DstAddr[3] = 1, 1, 1, 1

	// Write base header manually
	b := make([]byte, NbTcpHeaderBaseSize)
	binary.LittleEndian.PutUint32(b[0:4], hdr.Magic)
	b[4] = hdr.Version
	b[5] = hdr.AddrType
	b[6] = hdr.Protocol
	b[7] = hdr.ProcNameLen
	binary.LittleEndian.PutUint16(b[8:10], hdr.DstPort)
	binary.LittleEndian.PutUint16(b[10:12], hdr.SrcPort)
	copy(b[12:28], hdr.DstAddr[:])
	binary.LittleEndian.PutUint32(b[28:32], hdr.Pid)
	binary.LittleEndian.PutUint32(b[32:36], hdr.Token)
	buf.Write(b)

	parsed, err := ParseTcpHeader(buf)
	if err != nil {
		t.Fatalf("ParseTcpHeader: %v", err)
	}
	if parsed.Magic != NbMagic {
		t.Errorf("Magic = 0x%08X, want 0x%08X", parsed.Magic, NbMagic)
	}
	if parsed.Version != NbVersion {
		t.Errorf("Version = %d, want %d", parsed.Version, NbVersion)
	}
	if parsed.AddrType != NbAddrIPv4 {
		t.Errorf("AddrType = %d, want %d", parsed.AddrType, NbAddrIPv4)
	}
	if parsed.DstPort != 443 {
		t.Errorf("DstPort = %d, want 443", parsed.DstPort)
	}
	if parsed.Pid != 1234 {
		t.Errorf("Pid = %d, want 1234", parsed.Pid)
	}
	if parsed.Token != 0xDEADBEEF {
		t.Errorf("Token = 0x%08X, want 0xDEADBEEF", parsed.Token)
	}
}

func TestParseTcpHeader_IPv6(t *testing.T) {
	buf := new(bytes.Buffer)
	b := make([]byte, NbTcpHeaderBaseSize)
	binary.LittleEndian.PutUint32(b[0:4], NbMagic)
	b[4] = NbVersion
	b[5] = NbAddrIPv6
	b[6] = NbProtoTCP
	// IPv6 addr: 2001:db8::1
	b[12], b[13] = 0x20, 0x01
	b[14], b[15] = 0x0d, 0xb8
	b[27] = 0x01
	binary.LittleEndian.PutUint16(b[8:10], 8080)
	buf.Write(b)

	parsed, err := ParseTcpHeader(buf)
	if err != nil {
		t.Fatalf("ParseTcpHeader: %v", err)
	}
	if parsed.AddrType != NbAddrIPv6 {
		t.Errorf("AddrType = %d, want %d (IPv6)", parsed.AddrType, NbAddrIPv6)
	}
	if parsed.DstPort != 8080 {
		t.Errorf("DstPort = %d, want 8080", parsed.DstPort)
	}
	expectedIPv6 := []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	for i, v := range parsed.DstAddr {
		if v != expectedIPv6[i] {
			t.Errorf("DstAddr[%d] = 0x%02X, want 0x%02X", i, v, expectedIPv6[i])
		}
	}
}

func TestParseTcpHeader_WithProcName(t *testing.T) {
	buf := new(bytes.Buffer)
	name := "chrome.exe"
	b := make([]byte, NbTcpHeaderBaseSize)
	binary.LittleEndian.PutUint32(b[0:4], NbMagic)
	b[4] = NbVersion
	b[5] = NbAddrIPv4
	b[6] = NbProtoTCP
	b[7] = uint8(len(name))
	binary.LittleEndian.PutUint16(b[8:10], 443)
	binary.LittleEndian.PutUint16(b[10:12], 54321)
	binary.LittleEndian.PutUint32(b[28:32], 1234)
	binary.LittleEndian.PutUint32(b[32:36], 0xABCD)
	buf.Write(b)
	buf.WriteString(name)
	// Padding to 4-byte boundary: 36 + 10 = 46, pad to 48
	buf.Write([]byte{0, 0})

	parsed, err := ParseTcpHeader(buf)
	if err != nil {
		t.Fatalf("ParseTcpHeader: %v", err)
	}
	if parsed.ProcName != "chrome.exe" {
		t.Errorf("ProcName = %q, want %q", parsed.ProcName, "chrome.exe")
	}
}

func TestParseTcpHeader_TooShort(t *testing.T) {
	buf := bytes.NewReader([]byte{0x01, 0x02, 0x03})
	_, err := ParseTcpHeader(buf)
	if err == nil {
		t.Error("expected error for short buffer")
	}
}

func TestParseTcpHeader_InvalidMagic(t *testing.T) {
	buf := new(bytes.Buffer)
	b := make([]byte, NbTcpHeaderBaseSize)
	binary.LittleEndian.PutUint32(b[0:4], 0x12345678) // wrong magic
	buf.Write(b)

	parsed, err := ParseTcpHeader(buf)
	if err != nil {
		t.Fatalf("ParseTcpHeader: %v", err)
	}
	if parsed.Magic == NbMagic {
		t.Error("should not parse as valid magic")
	}
}

func TestDstNetAddress_IPv4(t *testing.T) {
	h := &NbTcpHeader{AddrType: NbAddrIPv4}
	h.DstAddr[0], h.DstAddr[1], h.DstAddr[2], h.DstAddr[3] = 8, 8, 8, 8
	addr := h.DstNetAddress()
	if addr == nil {
		t.Fatal("DstNetAddress returned nil")
	}
	if addr.Family().IsIPv6() {
		t.Error("expected IPv4 address")
	}
}

func TestDstNetAddress_IPv6(t *testing.T) {
	h := &NbTcpHeader{AddrType: NbAddrIPv6}
	h.DstAddr[0], h.DstAddr[1] = 0x20, 0x01
	addr := h.DstNetAddress()
	if addr == nil {
		t.Fatal("DstNetAddress returned nil")
	}
	if !addr.Family().IsIPv6() {
		t.Error("expected IPv6 address")
	}
}

// ===== UDP Header Tests =====

func TestParseUdpReqHeader(t *testing.T) {
	data := make([]byte, NbUdpReqHeaderSize+8)
	binary.LittleEndian.PutUint32(data[0:4], NbMagic)
	data[4] = NbVersion
	data[5] = NbAddrIPv4
	data[6] = NbProtoUDP
	binary.LittleEndian.PutUint16(data[8:10], 53)
	binary.LittleEndian.PutUint16(data[10:12], 12345)
	data[12], data[13], data[14], data[15] = 8, 8, 8, 8
	data[28], data[29], data[30], data[31] = 192, 168, 1, 1
	binary.LittleEndian.PutUint32(data[44:48], 9999)
	binary.LittleEndian.PutUint32(data[48:52], 0xCAFEBABE)
	binary.LittleEndian.PutUint16(data[52:54], 8)
	copy(data[56:], []byte("dnsquery"))

	hdr, payload, err := ParseUdpReqHeader(data)
	if err != nil {
		t.Fatalf("ParseUdpReqHeader: %v", err)
	}
	if hdr.Magic != NbMagic {
		t.Errorf("Magic = 0x%08X", hdr.Magic)
	}
	if hdr.DstPort != 53 {
		t.Errorf("DstPort = %d, want 53", hdr.DstPort)
	}
	if hdr.SrcPort != 12345 {
		t.Errorf("SrcPort = %d, want 12345", hdr.SrcPort)
	}
	if hdr.Pid != 9999 {
		t.Errorf("Pid = %d, want 9999", hdr.Pid)
	}
	if hdr.Token != 0xCAFEBABE {
		t.Errorf("Token = 0x%08X", hdr.Token)
	}
	if string(payload) != "dnsquery" {
		t.Errorf("payload = %q, want %q", string(payload), "dnsquery")
	}
}

func TestParseUdpReqHeader_TooShort(t *testing.T) {
	_, _, err := ParseUdpReqHeader([]byte{0x01, 0x02})
	if err == nil {
		t.Error("expected error for short data")
	}
}

func TestParseUdpReqHeader_PayloadLenExceedsData(t *testing.T) {
	data := make([]byte, NbUdpReqHeaderSize+5)
	binary.LittleEndian.PutUint16(data[52:54], 100) // payload_len=100, but only 5 bytes available
	_, _, err := ParseUdpReqHeader(data)
	if err == nil {
		t.Error("expected error for payload_len > available data")
	}
}

// ===== UDP Response Header Tests =====

func TestBuildNbUdpRespHeader(t *testing.T) {
	var srcAddr [16]byte
	srcAddr[0], srcAddr[1], srcAddr[2], srcAddr[3] = 8, 8, 8, 8

	hdr := BuildNbUdpRespHeader(NbAddrIPv4, 53, srcAddr, 42)
	if len(hdr) != NbUdpRespHeaderSize {
		t.Fatalf("len(hdr) = %d, want %d", len(hdr), NbUdpRespHeaderSize)
	}

	magic := binary.LittleEndian.Uint32(hdr[0:4])
	if magic != NbMagic {
		t.Errorf("magic = 0x%08X", magic)
	}
	if hdr[5] != NbAddrIPv4 {
		t.Errorf("addr_type = %d", hdr[5])
	}
	srcPort := binary.LittleEndian.Uint16(hdr[8:10])
	if srcPort != 53 {
		t.Errorf("src_port = %d, want 53", srcPort)
	}
	payloadLen := binary.LittleEndian.Uint16(hdr[28:30])
	if payloadLen != 42 {
		t.Errorf("payload_len = %d, want 42", payloadLen)
	}
}

// ===== Error Packet Tests =====

func TestSendNbError(t *testing.T) {
	var buf bytes.Buffer
	SendNbError(&buf, NbErrToken)
	data := buf.Bytes()

	if len(data) != NbErrorSize {
		t.Fatalf("len = %d, want %d", len(data), NbErrorSize)
	}
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != NbMagic {
		t.Errorf("magic = 0x%08X", magic)
	}
	if data[5] != NbErrToken {
		t.Errorf("error_code = %d, want %d", data[5], NbErrToken)
	}
}

// ===== Config Validation Tests =====

func TestConfig_DefaultValues(t *testing.T) {
	cfg := &Config{}
	if cfg.ListenAddress != "" {
		t.Errorf("ListenAddress = %q, want empty", cfg.ListenAddress)
	}
}

func TestConfig_ValidateLoopback_IPv4Loopback(t *testing.T) {
	cfg := &Config{ListenAddress: "127.0.0.1", ListenPort: 35000}
	if err := cfg.validateListenAddress(); err != nil {
		t.Errorf("validateListenAddress(%q) = %v, want nil", cfg.ListenAddress, err)
	}
}

func TestConfig_ValidateLoopback_IPv6Loopback(t *testing.T) {
	cfg := &Config{ListenAddress: "::1", ListenPort: 35000}
	if err := cfg.validateListenAddress(); err != nil {
		t.Errorf("validateListenAddress(%q) = %v, want nil", cfg.ListenAddress, err)
	}
}

// ===== Security Validation Tests =====

func TestConfig_ValidateLoopback_AllLoopbackAddresses(t *testing.T) {
	loopbacks := []string{"127.0.0.1", "::1", "127.0.0.2"}
	for _, addr := range loopbacks {
		cfg := &Config{ListenAddress: addr}
		if err := cfg.validateListenAddress(); err != nil {
			t.Errorf("validateListenAddress(%q) = %v, want nil", addr, err)
		}
	}
}

func TestConfig_ValidateLoopback_RejectsNonLoopback(t *testing.T) {
	bad := []string{"0.0.0.0", "192.168.1.1", "10.0.0.1", "172.16.0.1", "8.8.8.8", "::ffff:192.168.1.1"}
	for _, addr := range bad {
		cfg := &Config{ListenAddress: addr}
		err := cfg.validateListenAddress()
		if err == nil {
			t.Errorf("validateListenAddress(%q) = nil, want error (security violation)", addr)
		}
	}
}

func TestNetBridge_Init_NilConfig(t *testing.T) {
	n := &NetBridge{}
	err := n.Init(nil, nil)
	if err == nil {
		t.Error("expected error for nil config")
	}
}

func TestNetBridge_Init_NonLoopback(t *testing.T) {
	cfg := &Config{ListenAddress: "0.0.0.0", ListenPort: 35000}
	n := &NetBridge{}
	err := n.Init(cfg, nil)
	if err == nil {
		t.Error("expected error for non-loopback address")
	}
}

func TestNetBridge_Network(t *testing.T) {
	n := &NetBridge{}
	networks := n.Network()
	if len(networks) != 2 {
		t.Errorf("Network() returned %d networks, want 2", len(networks))
	}
}
