package encoding

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestNewSeed_FormatAndUniqueness(t *testing.T) {
	s1 := NewSeed()
	if len(s1) != seedLength {
		t.Fatalf("seed length=%d want %d", len(s1), seedLength)
	}
	// Timestamp field is recent.
	ts := int64(binary.BigEndian.Uint32(s1[:4]))
	if d := time.Now().Unix() - ts; d < 0 || d > 5 {
		t.Fatalf("seed timestamp %d too far from now", ts)
	}
	// Nonce is non-zero (randomized).
	allZero := true
	for _, b := range s1[4:] {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Fatal("seed nonce is all zero")
	}
	// Two consecutive seeds differ.
	s2 := NewSeed()
	if string(s1) == string(s2) {
		t.Fatal("consecutive seeds must differ")
	}
}

func TestValidateSeed_WindowAndReplay(t *testing.T) {
	// Fresh seed passes.
	if err := ValidateSeed(NewSeed()); err != nil {
		t.Fatalf("fresh seed rejected: %v", err)
	}
	// Nil passes (legacy clients).
	if err := ValidateSeed(nil); err != nil {
		t.Fatalf("nil seed must be accepted: %v", err)
	}
	// Bad length rejected.
	if err := ValidateSeed([]byte{1, 2, 3}); err == nil {
		t.Fatal("short seed must be rejected")
	}
	// Old timestamp rejected.
	old := NewSeed()
	binary.BigEndian.PutUint32(old[:4], uint32(time.Now().Unix()-int64(seedWindow.Seconds())-10))
	if err := ValidateSeed(old); err == nil {
		t.Fatal("expired seed must be rejected")
	}
	// Future timestamp rejected.
	fut := NewSeed()
	binary.BigEndian.PutUint32(fut[:4], uint32(time.Now().Unix()+int64(seedWindow.Seconds())+10))
	if err := ValidateSeed(fut); err == nil {
		t.Fatal("future seed must be rejected")
	}
	// Replay rejected.
	seed := NewSeed()
	if err := ValidateSeed(seed); err != nil {
		t.Fatalf("first use rejected: %v", err)
	}
	if err := ValidateSeed(seed); err == nil {
		t.Fatal("replayed seed must be rejected")
	}
}
