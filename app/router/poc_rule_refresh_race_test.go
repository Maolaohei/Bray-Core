package router_test

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"github.com/xtls/xray-core/testing/mocks"
)

// TestRouterRuleRefreshDataRace is a POC for upstream #6673 / #6678:
// ReloadRules (API hot-reload, takes r.mu, writes r.rules) racing against
// PickRoute (reads r.rules with NO lock) is a data race in the pre-598bde74
// code. This test only asserts correctness of the reload result; run it under
// -race to observe the race detector fire. Fixed upstream by 598bde74 which
// swaps rules/balancers to atomic.Pointer.
func TestRouterRuleRefreshDataRace(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockDNS := mocks.NewDNSClient(mockCtl)
	mockOhm := mocks.NewOutboundManager(mockCtl)
	mockHs := mocks.NewOutboundHandlerSelector(mockCtl)

	initial := &Config{
		Rule: []*RoutingRule{
			{TargetTag: &RoutingRule_Tag{Tag: "v0"}, Networks: []net.Network{net.Network_TCP}},
		},
	}
	r := new(Router)
	common.Must(r.Init(context.TODO(), initial, mockDNS, &mockOutboundManager{
		Manager:         mockOhm,
		HandlerSelector: mockHs,
	}, nil))

	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
		Target: net.TCPDestination(net.DomainAddress("example.com"), 80),
	}})
	rc := routing_session.AsRoutingContext(ctx)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Continuous readers: no lock, iterate r.rules via PickRoute.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = r.PickRoute(rc)
			}
		}()
	}

	// Writer: repeatedly hot-reload the rule set (this is the AddRule API path
	// that takes r.mu and reassigns r.rules wholesale).
	for i := 0; i < 50; i++ {
		cfg := &Config{
			Rule: []*RoutingRule{
				{TargetTag: &RoutingRule_Tag{Tag: "v1"}, Networks: []net.Network{net.Network_TCP}},
				{TargetTag: &RoutingRule_Tag{Tag: "v2"}, Networks: []net.Network{net.Network_UDP}},
			},
		}
		if err := r.ReloadRules(cfg, false); err != nil {
			t.Fatalf("ReloadRules: %v", err)
		}
	}

	close(stop)
	wg.Wait()
}
