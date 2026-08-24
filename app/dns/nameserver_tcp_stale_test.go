package dns_test

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	dnssrv "github.com/miekg/dns"
	. "github.com/xtls/xray-core/app/dns"
	"github.com/xtls/xray-core/common"
	dns_feature "github.com/xtls/xray-core/features/dns"
)

// trackingListener records accepted conns so the test can simulate a remote
// DNS server dropping established connections (idle timeout / restart).
type trackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.conns = append(l.conns, c)
	l.mu.Unlock()
	return c, nil
}

func (l *trackingListener) killConns() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		c.Close()
	}
	l.conns = nil
}

// fixedAnswerHandler answers every A question with 1.2.3.4, keeping the
// connection open (normal resolver behaviour).
type fixedAnswerHandler struct{}

func (*fixedAnswerHandler) ServeDNS(w dnssrv.ResponseWriter, r *dnssrv.Msg) {
	ans := new(dnssrv.Msg)
	ans.Id = r.Id
	ans.Question = r.Question // RFC 5452 guard relies on the echoed question
	ans.RecursionAvailable = true
	for _, q := range r.Question {
		if q.Qtype == dnssrv.TypeA {
			rr, _ := dnssrv.NewRR(q.Name + " IN A 1.2.3.4")
			ans.Answer = append(ans.Answer, rr)
		}
	}
	_ = w.WriteMsg(ans)
}

// TestTCPNameServerStalePooledConnection proves the TCP name server survives
// reusing a pooled connection that the remote side closed after it was pooled
// (the classic idle-kill scenario): query 1 populates the pool, the server
// drops the established conn, query 2 (cache miss, different domain) must
// transparently recover instead of failing the whole lookup.
func TestTCPNameServerStalePooledConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("local TCP DNS harness; skipped in -short mode")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	tln := &trackingListener{Listener: ln}
	server := &dnssrv.Server{Listener: tln, Handler: &fixedAnswerHandler{}}
	go func() { _ = server.ActivateAndServe() }()
	defer server.Shutdown()
	t.Cleanup(func() { tln.killConns() })

	url, err := url.Parse("tcp+local://" + ln.Addr().String())
	common.Must(err)
	s, err := NewTCPLocalNameServer(url, false, false, 0, nil)
	common.Must(err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ips, _, err := s.QueryIP(ctx, "first.example.com", dns_feature.IPOption{IPv4Enable: true})
	cancel()
	if err != nil {
		t.Fatalf("query 1 (pool warm-up) failed: %v", err)
	}
	if len(ips) == 0 || !ips[0].Equal(net.IPv4(1, 2, 3, 4)) {
		t.Fatalf("query 1 unexpected answer: %v", ips)
	}

	// Simulate the remote end killing every established connection
	// (idle timeout, restart, LB drain). Give the kernel a moment to
	// deliver FIN/RST to the client-side pooled socket.
	tln.killConns()
	time.Sleep(200 * time.Millisecond)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	ips2, _, err := s.QueryIP(ctx2, "second.example.com", dns_feature.IPOption{IPv4Enable: true})
	cancel2()
	if err != nil {
		t.Fatalf("query 2 failed after remote dropped the pooled connection: %v", err)
	}
	if len(ips2) == 0 {
		t.Fatal("query 2 returned no IPs")
	}
}
