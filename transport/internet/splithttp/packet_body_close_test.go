package splithttp

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

func TestPooledBodyCloseIdempotent(t *testing.T) {
	raw := []byte("durable-payload-idempotent")
	body := acquireDurableBody(raw)
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(raw) {
		t.Fatalf("got %q", got)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	// Second Close must not double-put into the pool.
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}

	mb := buf.MultiBuffer{buf.FromBytes([]byte("packet-body"))}
	pb := acquirePacketBody(mb)
	got2, err := io.ReadAll(pb)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "packet-body" {
		t.Fatalf("got %q", got2)
	}
	if err := pb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pb.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFillPacketRequestWriteCL_Concurrent(t *testing.T) {
	cfg := &Config{Path: "/vless Splithttp"}
	var wg sync.WaitGroup
	errCh := make(chan error, 128)
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				n := (g*37 + i*13) % 4096
				data := make([]byte, n)
				for j := range data {
					data[j] = byte(j)
				}
				req := &http.Request{
					Method:     "POST",
					URL:        &url.URL{Scheme: "http", Host: "127.0.0.1", Path: "/sh"},
					Proto:      "HTTP/1.1",
					ProtoMajor: 1,
					ProtoMinor: 1,
					Host:       "127.0.0.1",
				}
				if err := cfg.FillPacketRequestBytes(req, "sess", "1", data); err != nil {
					errCh <- err
					return
				}
				if req.ContentLength != int64(n) {
					errCh <- errMismatch{want: n, got: int(req.ContentLength)}
					return
				}
				var b bytes.Buffer
				if err := req.Write(&b); err != nil {
					errCh <- err
					return
				}
				// Write already closed Body; second close must be safe if callers keep old pattern.
				if req.Body != nil {
					_ = req.Body.Close()
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

type errMismatch struct {
	want, got int
}

func (e errMismatch) Error() string {
	return "content length mismatch"
}
