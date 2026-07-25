package splithttp

import (
	"bytes"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

func TestFillPacketRequestWriteCL_MultiBufferConcurrent(t *testing.T) {
	cfg := &Config{Path: "/vless Splithttp"}
	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				n := (g*41 + i*17) % 2048
				data := make([]byte, n)
				mb := buf.MultiBuffer{buf.FromBytes(data)}
				req := &http.Request{
					Method:     "POST",
					URL:        &url.URL{Scheme: "http", Host: "127.0.0.1", Path: "/sh"},
					Proto:      "HTTP/1.1",
					ProtoMajor: 1,
					ProtoMinor: 1,
					Host:       "127.0.0.1",
				}
				if err := cfg.FillPacketRequest(req, "sess", "2", mb); err != nil {
					errCh <- err
					return
				}
				var b bytes.Buffer
				if err := req.Write(&b); err != nil {
					errCh <- err
					return
				}
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
