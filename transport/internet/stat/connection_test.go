package stat

import "testing"

func TestCounterConnectionCloseNilUnderlyingConnection(t *testing.T) {
	conn := &CounterConnection{}

	if err := conn.Close(); err != nil {
		t.Fatalf("closing a failed dial wrapper: %v", err)
	}
}
