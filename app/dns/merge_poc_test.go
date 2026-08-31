package dns

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	dns_feature "github.com/xtls/xray-core/features/dns"
	"golang.org/x/net/dns/dnsmessage"
)

// TestMerge_DualStackKeepsOtherFamilyIPs is a POC for D2: in dual-stack
// (IPv4Enable && IPv6Enable), merge() used to early-return as soon as the
// *second* family's getIPs() yielded errRecordNotFound (e.g. the AAAA query was
// lost/timed out, or the domain simply has no AAAA record), discarding the
// *first* family's already-resolved IPs. That turned a perfectly good A record
// into an NXDOMAIN for any dual-stack lookup where one family's response was
// missing.
//
// Before the fix: merge(dual, rec4=A-ips, rec6=nil) -> (nil, errRecordNotFound)
// After  the fix: merge(dual, rec4=A-ips, rec6=nil) -> (A-ips, nil)
// (and symmetrically when A fails but AAAA succeeds).
func TestMerge_DualStackKeepsOtherFamilyIPs(t *testing.T) {
	dual := dns_feature.IPOption{IPv4Enable: true, IPv6Enable: true}

	aIP := net.ParseIP("192.0.2.1")
	aaaaIP := net.ParseIP("2001:db8::1")

	// Case 1: A succeeds, AAAA query failed (rec6 nil). Pre-fix discarded A.
	rec4 := &IPRecord{IP: []net.IP{aIP}, Expire: time.Now().Add(time.Hour), RCode: dnsmessage.RCodeSuccess}
	var rec6 *IPRecord // nil -> getIPs() == errRecordNotFound
	ips, _, err := merge(dual, rec4, rec6)
	if err != nil {
		t.Fatalf("case1: merge returned error %v; expected the A IP to survive", err)
	}
	if len(ips) != 1 || !ips[0].Equal(aIP) {
		t.Fatalf("case1: A IP discarded when AAAA failed: got %v", ips)
	}

	// Case 2 (symmetric): AAAA succeeds, A query failed (rec4 nil). Pre-fix discarded AAAA.
	rec4b := (*IPRecord)(nil)
	rec6b := &IPRecord{IP: []net.IP{aaaaIP}, Expire: time.Now().Add(time.Hour), RCode: dnsmessage.RCodeSuccess}
	ips2, _, err2 := merge(dual, rec4b, rec6b)
	if err2 != nil {
		t.Fatalf("case2: merge returned error %v; expected the AAAA IP to survive", err2)
	}
	if len(ips2) != 1 || !ips2[0].Equal(aaaaIP) {
		t.Fatalf("case2: AAAA IP discarded when A failed: got %v", ips2)
	}

	// Case 3 (regression guard): BOTH families return NXDOMAIN (RCodeNameError,
	// e.g. notexist.google.com). Pre-fix with the naive allIPs-only merge this
	// returned a nil error (RCode 0) for the dual-stack lookup, so the caller
	// saw a "success" with no IPs instead of a NameError. It must surface the
	// NameError.
	rec4c := &IPRecord{Expire: time.Now().Add(time.Hour), RCode: dnsmessage.RCodeNameError}
	rec6c := &IPRecord{Expire: time.Now().Add(time.Hour), RCode: dnsmessage.RCodeNameError}
	ips3, _, err3 := merge(dual, rec4c, rec6c)
	if err3 == nil {
		t.Fatalf("case3: merge returned nil error for dual-stack NXDOMAIN; expected NameError")
	}
	if dns_feature.RCodeFromError(err3) != uint16(dnsmessage.RCodeNameError) {
		t.Fatalf("case3: merge returned error %v; expected NameError", err3)
	}
	if len(ips3) != 0 {
		t.Fatalf("case3: expected no IPs for NXDOMAIN, got %v", ips3)
	}
}
