package dns

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIsContextDoneErr(t *testing.T) {
	if !isContextDoneErr(context.Canceled) {
		t.Fatal("Canceled")
	}
	if !isContextDoneErr(context.DeadlineExceeded) {
		t.Fatal("DeadlineExceeded")
	}
	if !isContextDoneErr(fmt.Errorf("Post %q: %w", "https://1.1.1.1/dns-query", context.Canceled)) {
		t.Fatal("wrapped canceled")
	}
	if !isContextDoneErr(errors.New("Post \"https://1.1.1.1/dns-query\": context canceled")) {
		t.Fatal("string canceled")
	}
	if isContextDoneErr(errors.New("connection reset")) {
		t.Fatal("reset must not match")
	}
	if isContextDoneErr(nil) {
		t.Fatal("nil")
	}
}
