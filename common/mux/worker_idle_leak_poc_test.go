package mux

// POC for XMUX 专项 M2 (MED goroutine leak claim): the monitor() idle-timeout
// path closes the worker via done.Close() but (allegedly) never interrupts the
// underlying link, so run()/fetchOutput() stay blocked on link.Reader and leak.
//
// Static analysis of common/mux/{server.go,client.go} monitor(): the idle
// branch calls CloseIfNoSessionAndIdle(...) then done.Close(). The actual
// common.Interrupt(link.Reader/Writer) lives in the `<-done.Wait()` branch,
// which the SAME monitor goroutine re-selects into on its next loop iteration
// (the closed done channel is always ready, and the ticker cannot fire again
// until the next interval). So run()/fetchOutput() are unblocked and the
// goroutines exit — i.e. M2 is very likely a false positive.
//
// This POC drives a REAL ClientWorker with a short keepalive period so the
// idle-close fires within a few seconds, then verifies with goroutine counting
// (goleak-style) that fetchOutput()+monitor() actually exit. If they leaked,
// the goroutine count would stay elevated (+2) after settle and the test
// fails, reproducing M2.

import (
	"runtime"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

func TestM2_ClientIdleCloseNoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	linkReader, linkWriter := pipe.New()
	c, err := NewClientWorker(transport.Link{Reader: linkReader, Writer: linkWriter}, ClientStrategy{KeepAlivePeriod: 1})
	if err != nil {
		t.Fatal(err)
	}
	// NewClientWorker spawns exactly 2 goroutines: fetchOutput + monitor.
	if spawned := runtime.NumGoroutine() - baseline; spawned < 2 {
		t.Fatalf("expected >=2 worker goroutines, saw +%d", spawned)
	}

	// Wait for idle-close (3 idle ticks of ~1s).
	deadline := time.Now().Add(15 * time.Second)
	for !c.Closed() {
		if time.Now().After(deadline) {
			t.Fatal("ClientWorker did not idle-close within deadline")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Give the monitor's done-branch a moment to interrupt fetchOutput, then
	// let goroutines settle. Retry once to absorb transient runtime churn.
	leaked := func() int { return runtime.NumGoroutine() - baseline }
	time.Sleep(2 * time.Second)
	if n := leaked(); n > 0 {
		time.Sleep(2 * time.Second)
		if n = leaked(); n > 0 {
			buf := make([]byte, 1<<20)
			stacks := buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutine leak after idle-close: +%d goroutines (M2 reproduced)\n%s", n, stacks)
		}
	}
}
