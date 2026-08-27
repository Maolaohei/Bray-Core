package congestion

import (
	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/congestion"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/brutal"
)

func UseBBR(conn *quic.Conn, profile bbr.Profile) {
	conn.SetCongestionControl(bbr.NewBbrSender(
		bbr.DefaultClock{},
		seedPacketSize(conn.InitialPacketSize(), bbr.GetInitialPacketSize(conn.RemoteAddr())),
		profile,
	))
}

// seedPacketSize picks the datagram size to seed a replacement congestion
// controller with, given the size QUIC itself starts at and the guess derived
// from the remote address.
//
// The seed must not exceed what QUIC actually starts at. If it does, the first
// path MTU probe can land between the two: QUIC sees an increase and reports
// it, but the controller sees a decrease, which it cannot represent. Taking the
// smaller of the two keeps the address-based guess as a floor for connections
// whose path we can't reason about, while never seeding above QUIC.
func seedPacketSize(quicSize, byAddr congestion.ByteCount) congestion.ByteCount {
	if quicSize <= 0 {
		return byAddr
	}
	return min(quicSize, byAddr)
}

func UseBrutal(conn *quic.Conn, tx uint64, disableLossCompensation bool) {
	conn.SetCongestionControl(brutal.NewBrutalSender(tx, disableLossCompensation))
}
