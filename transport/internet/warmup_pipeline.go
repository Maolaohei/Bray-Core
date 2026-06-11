package internet

import (
	"context"
	"errors"
	"sync"
	"time"

	common_errors "github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
)

var errNoIPsAvailable = errors.New("warmup: no IPs available")

// WarmupPipeline orchestrates the full warmup flow:
// DNS Warmup → Happy Eyeballs → PreConnect → Ready Connection
type WarmupPipeline struct {
	dnsResolver  DNSResolver
	dialFunc     func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error)
	sockopt      *SocketConfig
	domain       string
	port         net.Port

	// Results
	resolvedIPs  []net.IP
	bestConn     net.Conn
	err          error
	done         chan struct{}
	once         sync.Once
}

// DNSResolver resolves domain names to IP addresses.
type DNSResolver interface {
	LookupIP(domain string) ([]net.IP, error)
}

// WarmupResult holds the result of a warmup pipeline execution.
type WarmupResult struct {
	IPs  []net.IP
	Conn net.Conn
	Err  error
}

// NewWarmupPipeline creates a new warmup pipeline for a domain.
func NewWarmupPipeline(
	dnsResolver DNSResolver,
	dialFunc func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error),
	sockopt *SocketConfig,
	domain string,
	port net.Port,
) *WarmupPipeline {
	return &WarmupPipeline{
		dnsResolver: dnsResolver,
		dialFunc:    dialFunc,
		sockopt:     sockopt,
		domain:      domain,
		port:        port,
		done:        make(chan struct{}),
	}
}

// Run executes the full warmup pipeline in background.
// Results are available via Result() after Done() channel closes.
func (p *WarmupPipeline) Run(ctx context.Context) {
	go p.run(ctx)
}

func (p *WarmupPipeline) run(ctx context.Context) {
	defer p.once.Do(func() { close(p.done) })

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Step 1: DNS Warmup
	common_errors.LogDebug(ctx, "warmup pipeline: resolving ", p.domain)
	ips, err := p.dnsResolver.LookupIP(p.domain)
	if err != nil {
		common_errors.LogDebug(ctx, "warmup pipeline: DNS failed for ", p.domain, ": ", err)
		p.err = err
		return
	}
	if len(ips) == 0 {
		p.err = errNoIPsAvailable
		return
	}
	p.resolvedIPs = ips
	common_errors.LogDebug(ctx, "warmup pipeline: resolved ", p.domain, " to ", ips)

	// Step 2: Happy Eyeballs sorting (v3 uses scoreIPs, v2 uses sortIPs)
	prioritizeIPv6 := false
	if p.sockopt != nil && p.sockopt.HappyEyeballs != nil {
		prioritizeIPv6 = p.sockopt.HappyEyeballs.PrioritizeIpv6
	}

	sortedIPs := sortIPs(ips, prioritizeIPv6, 1)

	// Step 3: PreConnect - establish TCP + TLS/REALITY
	common_errors.LogDebug(ctx, "warmup pipeline: pre-connecting to ", p.domain)
	for _, ip := range sortedIPs {
		conn, err := p.dialFunc(ctx, net.Destination{
			Address: net.IPAddress(ip),
			Network: net.Network_TCP,
			Port:    p.port,
		}, p.sockopt)
		if err != nil {
			common_errors.LogDebug(ctx, "warmup pipeline: dial failed for ", ip, ": ", err)
			continue
		}
		// Connection established
		p.bestConn = conn
		common_errors.LogDebug(ctx, "warmup pipeline: connected to ", ip, " for ", p.domain)
		return
	}

	p.err = errNoIPsAvailable // all dials failed
}

// Done returns a channel that is closed when the pipeline completes.
func (p *WarmupPipeline) Done() <-chan struct{} {
	return p.done
}

// Result returns the warmup result. Must be called after Done().
func (p *WarmupPipeline) Result() WarmupResult {
	return WarmupResult{
		IPs:  p.resolvedIPs,
		Conn: p.bestConn,
		Err:  p.err,
	}
}

// WarmupManager manages warmup pipelines for multiple domains.
type WarmupManager struct {
	mu        sync.RWMutex
	pipelines map[string]*WarmupPipeline
	dnsResolver DNSResolver
	dialFunc    func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error)
	sockopt     *SocketConfig
}

// NewWarmupManager creates a new warmup manager.
func NewWarmupManager(
	dnsResolver DNSResolver,
	dialFunc func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error),
	sockopt *SocketConfig,
) *WarmupManager {
	return &WarmupManager{
		pipelines:  make(map[string]*WarmupPipeline),
		dnsResolver: dnsResolver,
		dialFunc:    dialFunc,
		sockopt:     sockopt,
	}
}

// WarmupDomain starts a warmup pipeline for a domain.
func (m *WarmupManager) WarmupDomain(ctx context.Context, domain string, port net.Port) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.pipelines[domain]; exists {
		return // already warming up
	}

	pipeline := NewWarmupPipeline(m.dnsResolver, m.dialFunc, m.sockopt, domain, port)
	m.pipelines[domain] = pipeline
	pipeline.Run(ctx)
}

// GetWarmConn returns a pre-established connection for a domain, or nil if not ready.
func (m *WarmupManager) GetWarmConn(domain string) net.Conn {
	m.mu.RLock()
	pipeline, exists := m.pipelines[domain]
	m.mu.RUnlock()

	if !exists {
		return nil
	}

	select {
	case <-pipeline.Done():
		result := pipeline.Result()
		return result.Conn
	default:
		return nil
	}
}

// WarmupFromOutbound extracts domains from outbound handlers and warms them up.
func (m *WarmupManager) WarmupFromOutbound(ctx context.Context, obm outbound.Manager) {
	domains := ExtractWarmupDomains(obm)
	if len(domains) == 0 {
		return
	}

	common_errors.LogDebug(ctx, "warmup manager: warming up ", len(domains), " domains from outbound config")

	// Use a semaphore to limit concurrent warmups
	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup

	for _, domain := range domains {
		wg.Add(1)
		go func(d string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			m.WarmupDomain(ctx, d, net.Port(443))
		}(domain)
	}

	wg.Wait()
}
