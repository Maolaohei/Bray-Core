package dns

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xtls/xray-core/common/protocol/dns"
	dns_feature "github.com/xtls/xray-core/features/dns"
)

// TestPOC_OptResourceNoRace is a POC for D1: the OPT resource handed to
// buildReqMsgs used to be shallow-copied into msg.Additionals, so the message
// shared the pooled *OPTResource (and its Options slice) with any concurrent
// query. After releaseOptResource returned it to the pool, a parallel
// genEDNS0Options could reset/append its Options while PackMessage read them —
// a data race.
//
// Run with -race: pre-fix it fires (DATA RACE on OPTResource.Options), post-fix
// (buildReqMsgs deep-copies the OPT) it is clean.
func TestPOC_OptResourceNoRace(t *testing.T) {
	clientIP := []byte{192, 0, 2, 123}
	option := dns_feature.IPOption{IPv4Enable: true, IPv6Enable: true}

	var id atomic.Uint32
	reqIDGen := func() uint16 { return uint16(id.Add(1)) }

	var wg sync.WaitGroup
	// Builder goroutines: build a request (which shares the pool OPT) and pack it.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqOpts := genEDNS0Options(clientIP, 0)
			reqs, _ := buildReqMsgs("example.com.", option, reqIDGen, reqOpts)
			for _, r := range reqs {
				_, _ = dns.PackMessage(r.msg)
			}
		}()
	}
	// Concurrent mutators: keep allocating OPTs from the pool (as real queries
	// do), resetting/appending their Options — racing the packers pre-fix.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				o := genEDNS0Options(clientIP, 0)
				releaseOptResource(o)
			}
		}()
	}
	wg.Wait()
}
