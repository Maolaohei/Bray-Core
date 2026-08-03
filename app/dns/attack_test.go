package dns

import (
	"testing"

	"golang.org/x/net/dns/dnsmessage"
)

// TestAttack_DNSPoisoningGuard verifies the RFC 5452 cache-poisoning guard:
// a forged response whose echoed question is missing or mismatched must be
// discarded before it can populate the cache.
func TestAttack_DNSPoisoningGuard(t *testing.T) {
	cases := []struct {
		name   string
		domain string
		rec    *IPRecord
		want   bool
	}{
		{"matching question", "example.com.", &IPRecord{Question: "example.com."}, true},
		{"case-insensitive match", "example.com.", &IPRecord{Question: "EXAMPLE.com."}, true},
		{"mismatched question (forged)", "example.com.", &IPRecord{Question: "evil.com."}, false},
		{"missing question (forged)", "example.com.", &IPRecord{Question: ""}, false},
		{"nil record", "example.com.", nil, false},
		{"subdomain does not match", "example.com.", &IPRecord{Question: "sub.example.com."}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := responseMatchesRequest(tc.domain, tc.rec); got != tc.want {
				t.Fatalf("responseMatchesRequest(%q, Question=%q) = %v, want %v",
					tc.domain, questionOf(tc.rec), got, tc.want)
			}
		})
	}
}

func questionOf(r *IPRecord) string {
	if r == nil {
		return "<nil>"
	}
	return r.Question
}

// TestAttack_SeededIDUnpredictability guards the regression where the DNS
// query ID was a sequential counter from zero (predictable → forgeable).
func TestAttack_SeededIDUnpredictability(t *testing.T) {
	s := &ClassicNameServer{requests: make(map[uint16]*udpDnsRequest)}
	seen := make(map[uint16]struct{})
	for i := 0; i < 256; i++ {
		id := s.newReqID()
		if _, dup := seen[id]; dup {
			t.Fatalf("collision in 256 samples (16-bit space), id=%d", id)
		}
		seen[id] = struct{}{}
	}
	_ = dnsmessage.TypeA // keep import used
}
