package buf_test

import (
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

func TestFromBytesShellPool(t *testing.T) {
	raw := []byte("hello-from-bytes")
	b1 := buf.FromBytes(raw)
	if string(b1.Bytes()) != "hello-from-bytes" {
		t.Fatalf("content: %q", b1.Bytes())
	}
	b1.Release()
	b2 := buf.FromBytes(raw)
	// Best-effort: same shell pointer is possible after Release; content must work.
	if string(b2.Bytes()) != "hello-from-bytes" {
		t.Fatalf("content after reuse: %q", b2.Bytes())
	}
	b2.Release()
	// Double-release must be safe (no panic).
	b2.Release()
}