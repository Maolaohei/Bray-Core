package splithttp

// RED/regression for independent audit Finding-2: a mid-stream 404 (producer
// not ready yet) must be retried on the SAME segment — the old worker did
// `continue` which re-reserved a NEW seq, abandoning the 404'd one and
// deadlocking the stream once any production stall exceeded downsegPullWait.

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

func TestDownSegPullerMidStream404Recovery(t *testing.T) {
	h, base, client := newEndToEndServer(t)
	sid := h.config.GenerateSessionID()
	payload := bytes.Repeat([]byte{0x5C}, downsegSize*3)

	// Production: wait for the puller's segment GET to upsert the session,
	// then delay another 400ms (so first pulls 404) before writing + finalize.
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		deadline := time.Now().Add(5 * time.Second)
		for {
			v, ok := h.sessions.Load(sid)
			if ok {
				sess := v.(*httpSession)
				if !sess.enterDownsegMode() {
					return
				}
				time.Sleep(2500 * time.Millisecond) // exceed server 2s poll -> real 404s mid-stream
				prod := &httpServerConn{Instance: done.New(), sess: sess}
				if _, err := prod.Write(payload); err != nil {
					return
				}
				_ = prod.Close() // EOF
				return
			}
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	puller := NewDownSegPuller(ctx, client, base, sid, nil)
	defer puller.Close()

	var got bytes.Buffer
	buf := make([]byte, 64<<10)
	for {
		n, err := puller.Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	<-prodDone
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("mid-stream 404 recovery: got %d bytes want %d (Finding-2: segment abandoned on 404)", got.Len(), len(payload))
	}
}
