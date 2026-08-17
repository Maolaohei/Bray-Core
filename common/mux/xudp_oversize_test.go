package mux

// POC for the second, dispatch-independent XUDP DoS vector: a client
// declaring an oversized XUDP payload length (> buf.Size) in a ~20-byte
// frame used to tear down the whole Mux connection via reader.go:40 -> 
// handleXUDPSingle -> handleFrame -> run() -> done.Close().

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// TestXUDPOversizedPayloadKeepsMux: an oversized declared payload length
// must drop the datagram, not kill the multiplexed connection.
func TestXUDPOversizedPayloadKeepsMux(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, serverWriter := pipe.New()
	serverReader, clientWriter := pipe.New()
	link := &transport.Link{Reader: serverReader, Writer: serverWriter}

	// Any dispatcher: the oversized length is rejected in reader.go BEFORE
	// dispatch, so a healthy dispatcher is the right control (reuse
	// okayDispatcher from xudp_mux_dos_test.go).
	worker, err := NewServerWorker(ctx, okayDispatcher{}, link)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	frame := buf.New()
	target := net.UDPDestination(net.LocalHostIP, 9999)
	frame.UDP = &net.Destination{Network: net.Network_UDP, Address: target.Address, Port: target.Port}
	meta := FrameMetadata{
		SessionStatus: SessionStatusNew,
		Option:        OptionData,
		Target:        target,
		GlobalID:      [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	if err := meta.WriteTo(frame); err != nil {
		t.Fatal(err)
	}
	// Declared payload length > buf.Size (8192): the malformed datagram.
	oversized := buf.Size + 100
	frame.WriteByte(byte(oversized >> 8))
	frame.WriteByte(byte(oversized))

	if err := clientWriter.WriteMultiBuffer(buf.MultiBuffer{frame}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-worker.WaitClosed():
		t.Fatal("FAIL: oversized XUDP payload killed the whole Mux connection")
	case <-time.After(2 * time.Second):
		// Expected: connection survives (datagram dropped).
	}
}
