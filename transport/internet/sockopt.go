package internet

func isTCPSocket(network string) bool {
	switch network {
	case "tcp", "tcp4", "tcp6":
		return true
	default:
		return false
	}
}

func isUDPSocket(network string) bool {
	switch network {
	case "udp", "udp4", "udp6":
		return true
	default:
		return false
	}
}

// setQuickAck is a no-op by default. On Linux it is replaced in init()
// to enable TCP_QUICKACK on accepted connections, speeding up the TLS
// handshake by disabling delayed ACK for the first few data flights.
var setQuickAck = func(fd uintptr) {}

func (v *SocketConfig) ParseTFOValue() int {
	if v.Tfo == 0 {
		return -1 // disabled by default; some middleboxes drop SYN+data
	}
	tfo := int(v.Tfo)
	if tfo < 0 {
		tfo = 0
	}
	return tfo
}
