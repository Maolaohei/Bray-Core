package tcpinfo

import (
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet/quality"
)

func TestProfileNew(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	prof := NewProfile(conn, nil)
	if prof == nil {
		t.Fatal("NewProfile returned nil")
	}

	snap := prof.Snapshot()
	if snap == nil {
		t.Fatal("initial snapshot should not be nil")
	}
	if snap.Source != quality.SourceUnknown {
		t.Fatalf("initial source = %s, want unknown", snap.Source)
	}
	if snap.Confidence != 0 {
		t.Fatalf("initial confidence = %d, want 0", snap.Confidence)
	}
}

func TestProfileStartStop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	prof := NewProfile(conn, nil)
	prof.SetInterval(50 * time.Millisecond)
	prof.Start()
	defer prof.Stop()

	// Wait for at least one collection
	time.Sleep(150 * time.Millisecond)

	snap := prof.Snapshot()
	if snap == nil {
		t.Fatal("snapshot should not be nil after start")
	}
	// On Windows fallback, source should be "estimated"
	if snap.Source != quality.SourceEstimated {
		t.Logf("source = %s (may vary by platform)", snap.Source)
	}
}

func TestProfileDoubleStart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	prof := NewProfile(conn, nil)
	prof.Start()
	prof.Start() // should be safe
	prof.Stop()
}

func TestDebugInfo(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	prof := NewProfile(conn, nil)
	prof.SetInterval(50 * time.Millisecond)
	prof.Start()
	defer prof.Stop()

	time.Sleep(150 * time.Millisecond)

	info := prof.GetDebugInfo()
	if info.Network.Source == "" {
		t.Fatal("DebugInfo source should not be empty")
	}
	if info.Network.Age == "" {
		t.Fatal("DebugInfo age should not be empty")
	}
	t.Logf("DebugInfo: %+v", info.Network)
	t.Logf("Reason: %v", info.Network.Reason)
}
