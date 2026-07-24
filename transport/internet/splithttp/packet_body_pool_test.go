package splithttp

import (
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

func TestAcquirePacketBodyPools(t *testing.T) {
	mb := buf.MultiBuffer{buf.New()}
	mb[0].Write([]byte("hi"))
	body := acquirePacketBody(mb)
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hi" {
		t.Fatalf("got %q", got)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	// Second acquire should succeed (pool reuse path).
	mb2 := buf.MultiBuffer{buf.New()}
	mb2[0].Write([]byte("ok"))
	body2 := acquirePacketBody(mb2)
	got2, err := io.ReadAll(body2)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "ok" {
		t.Fatalf("got %q", got2)
	}
	_ = body2.Close()
}

func TestAcquireDurableBody(t *testing.T) {
	raw := []byte("durable-payload")
	body := acquireDurableBody(raw)
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "durable-payload" {
		t.Fatalf("got %q", got)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	// Close must not free/alias-clear caller's durable bytes.
	if string(raw) != "durable-payload" {
		t.Fatalf("caller bytes mutated: %q", raw)
	}

	// Pool reuse path: second body over same durable source.
	body2 := acquireDurableBody(raw)
	got2, err := io.ReadAll(body2)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "durable-payload" {
		t.Fatalf("reuse got %q", got2)
	}
	_ = body2.Close()
}

func TestFillPacketRequestBytesBody(t *testing.T) {
	cfg := &Config{Path: "/sh"}
	req := &http.Request{
		URL: &url.URL{Scheme: "https", Host: "example.com", Path: "/sh"},
	}
	data := []byte("hello-bytes-path")
	if err := cfg.FillPacketRequestBytes(req, "sess1", "7", data); err != nil {
		t.Fatal(err)
	}
	if req.Body == nil {
		t.Fatal("expected body")
	}
	if req.ContentLength != int64(len(data)) {
		t.Fatalf("ContentLength=%d", req.ContentLength)
	}
	got, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello-bytes-path" {
		t.Fatalf("body %q", got)
	}
	if err := req.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello-bytes-path" {
		t.Fatalf("durable data freed/mutated: %q", data)
	}
}
