package internet

import (
	"context"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
)

type result struct {
	err   error
	conn  net.Conn
	index int
	rtt   time.Duration
}

func TcpRaceDial(ctx context.Context, src net.Address, ips []net.IP, port net.Port, sockopt *SocketConfig, domain string) (net.Conn, error) {
	if sockopt.HappyEyeballs.GetV3Enabled() {
		return tcpRaceDialV3(ctx, src, ips, port, sockopt, domain)
	}
	return tcpRaceDialV2(ctx, src, ips, port, sockopt, domain)
}

// tcpRaceDialV2 implements RFC 8305 (existing behavior).
func tcpRaceDialV2(ctx context.Context, src net.Address, ips []net.IP, port net.Port, sockopt *SocketConfig, domain string) (net.Conn, error) {
	if len(ips) < 2 {
		panic("at least 2 ips is required to race dial")
	}

	prioritizeIPv6 := sockopt.HappyEyeballs.PrioritizeIpv6
	interleave := sockopt.HappyEyeballs.Interleave
	tryDelayMs := time.Duration(sockopt.HappyEyeballs.TryDelayMs) * time.Millisecond
	maxConcurrentTry := sockopt.HappyEyeballs.MaxConcurrentTry

	ips = sortIPs(ips, prioritizeIPv6, interleave)
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resultCh := make(chan *result, len(ips))
	nextTryIndex := 0
	activeNum := uint32(0)
	timer := time.NewTimer(0)
	var winConn net.Conn
	errors.LogDebug(ctx, "happy eyeballs v2 racing dial for ", domain, " with IPs ", ips)
	for {
		select {
		case r := <-resultCh:
			activeNum--
			select {
			case <-ctx.Done():
				cancel()
				timer.Stop()
				if winConn != nil {
					winConn.Close()
				}
				if r.conn != nil {
					r.conn.Close()
				}
				if activeNum == 0 {
					return nil, ctx.Err()
				}
				continue
			default:
				if r.conn != nil {
					cancel()
					timer.Stop()
					if winConn == nil {
						winConn = r.conn
						errors.LogDebug(ctx, "happy eyeballs v2 established connection for ", domain, " with IP ", ips[r.index])
					} else {
						r.conn.Close()
					}
				}
				if winConn != nil && activeNum == 0 {
					return winConn, nil
				}
				if winConn != nil {
					continue
				}
				if nextTryIndex < len(ips) {
					timer.Reset(0)
					continue
				}
				if activeNum == 0 {
					errors.LogDebugInner(ctx, r.err, "happy eyeballs v2 no connection established for ", domain)
					return nil, r.err
				}
				timer.Stop()
				continue
			}

		case <-timer.C:
			if nextTryIndex == len(ips) || activeNum == maxConcurrentTry {
				panic("impossible situation")
			}
			go tcpTryDial(newCtx, src, sockopt, ips[nextTryIndex], port, nextTryIndex, resultCh, domain)
			activeNum++
			nextTryIndex++
			if nextTryIndex == len(ips) || activeNum == maxConcurrentTry {
				timer.Stop()
			} else {
				timer.Reset(tryDelayMs)
			}
			continue
		}
	}
}

// tcpRaceDialV3 implements Happy Eyeballs v3 draft with scoring, adaptive concurrency, and history.
func tcpRaceDialV3(ctx context.Context, src net.Address, ips []net.IP, port net.Port, sockopt *SocketConfig, domain string) (net.Conn, error) {
	if len(ips) < 2 {
		panic("at least 2 ips is required to race dial")
	}

	cfg := sockopt.HappyEyeballs
	prioritizeIPv6 := cfg.PrioritizeIpv6

	// Score and sort IPs by v3 priority
	// Reusable score buffer for this race (1 growth, then 0 alloc).
	var scoreBuf []HappyIPScore
	ipScores := scoreIPsInto(scoreBuf, ips, prioritizeIPv6, nil)

	// Set up try controller
	initialDelay := time.Duration(cfg.TryDelayMs) * time.Millisecond
	if initialDelay == 0 {
		initialDelay = 250 * time.Millisecond
	}
	maxConcurrent := int(cfg.MaxConcurrentTry)
	if maxConcurrent == 0 {
		maxConcurrent = 4
	}
	tryController := NewTryController(maxConcurrent, initialDelay)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resultCh := make(chan *result, len(ipScores))
	nextTryIndex := 0
	activeNum := int32(0)
	var winConn net.Conn
	var lastErr error
	errors.LogDebug(ctx, "happy eyeballs v3 racing dial for ", domain, " with scored IPs")

	// Launch first attempt immediately
	if len(ipScores) > 0 && tryController.CanTry() {
		tryController.OnStart()
		activeNum++
		go tcpTryDialV3(newCtx, src, sockopt, ipScores[0].IP, port, 0, resultCh, domain)
		nextTryIndex = 1
	}

	timer := time.NewTimer(tryController.GetDelay())
	defer timer.Stop()

	for {
		select {
		case r := <-resultCh:
			tryController.OnEnd()
			activeNum--

			select {
			case <-ctx.Done():
				cancel()
				if winConn != nil {
					winConn.Close()
				}
				if r.conn != nil {
					r.conn.Close()
				}
				if activeNum == 0 {
					return nil, ctx.Err()
				}
				continue
			default:
				if r.conn != nil {
					// Record success
					record := globalHappyIPDB.getByIP(ipScores[r.index].IP)
					if r.rtt > 0 {
						record.recordSuccess(r.rtt)
						tryController.OnSuccess(r.rtt)
					} else {
						record.recordSuccess(defaultSmoothedRTT)
						tryController.OnSuccess(defaultSmoothedRTT)
					}

					cancel()
					if winConn == nil {
						winConn = r.conn
						errors.LogDebug(ctx, "happy eyeballs v3 established connection for ", domain, " with IP ", ipScores[r.index].IP)
					} else {
						r.conn.Close()
					}
				} else {
					// Record failure
					record := globalHappyIPDB.getByIP(ipScores[r.index].IP)
					record.recordFail()
					tryController.OnFail()
					if r.err != nil {
						lastErr = r.err
					}
				}

				if winConn != nil && activeNum == 0 {
					return winConn, nil
				}
				if winConn != nil {
					continue
				}
				// Try launching more attempts
				for nextTryIndex < len(ipScores) && tryController.CanTry() {
					tryController.OnStart()
					activeNum++
					go tcpTryDialV3(newCtx, src, sockopt, ipScores[nextTryIndex].IP, port, nextTryIndex, resultCh, domain)
					nextTryIndex++
				}
				if winConn == nil && nextTryIndex >= len(ipScores) && activeNum == 0 {
					if lastErr != nil {
						errors.LogDebugInner(ctx, lastErr, "happy eyeballs v3 no connection established for ", domain)
						return nil, lastErr
					}
					return nil, errors.New("happy eyeballs v3: no connection established for ", domain)
				}
				continue
			}

		case <-timer.C:
			// Launch next attempt on timer
			if nextTryIndex < len(ipScores) && tryController.CanTry() {
				tryController.OnStart()
				activeNum++
				go tcpTryDialV3(newCtx, src, sockopt, ipScores[nextTryIndex].IP, port, nextTryIndex, resultCh, domain)
				nextTryIndex++
			}
			if nextTryIndex < len(ipScores) {
				timer.Reset(tryController.GetDelay())
			}
			continue
		}
	}
}

// sortIPScratch holds temporary family buckets for RFC 8305 interleave.
// Returned slice is always a fresh []net.IP of exact result length so callers
// may retain it without racing the pool.
type sortIPScratch struct {
	ip4 []net.IP
	ip6 []net.IP
	out []net.IP
}

var sortIPScratchPool = sync.Pool{
	New: func() any {
		return &sortIPScratch{
			ip4: make([]net.IP, 0, 16),
			ip6: make([]net.IP, 0, 16),
			out: make([]net.IP, 0, 16),
		}
	},
}

// sortIPs sort IPs according to rfc 8305.
func sortIPs(ips []net.IP, prioritizeIPv6 bool, interleave uint32) []net.IP {
	if len(ips) == 0 {
		return ips
	}
	if interleave == 0 {
		interleave = 1
	}
	sc := sortIPScratchPool.Get().(*sortIPScratch)
	ip4 := sc.ip4[:0]
	ip6 := sc.ip6[:0]
	if cap(ip4) < len(ips) {
		ip4 = make([]net.IP, 0, len(ips))
	}
	if cap(ip6) < len(ips) {
		ip6 = make([]net.IP, 0, len(ips))
	}
	for _, ip := range ips {
		// Prefer length checks over To4/To16 which may allocate for non-canonical forms.
		switch len(ip) {
		case net.IPv4len:
			ip4 = append(ip4, ip)
		case net.IPv6len:
			// IPv4-mapped IPv6 (::ffff:a.b.c.d) counts as v4 for interleave.
			if v4 := ip.To4(); v4 != nil {
				ip4 = append(ip4, v4)
			} else {
				ip6 = append(ip6, ip)
			}
		default:
			// Keep unparsable entries as-is in the IPv6 bucket so we never drop IPs.
			ip6 = append(ip6, ip)
		}
	}

	if len(ip4) == 0 || len(ip6) == 0 {
		// Clear refs before Put so pooled scratch does not pin IPs.
		for i := range ip4 {
			ip4[i] = nil
		}
		for i := range ip6 {
			ip6[i] = nil
		}
		sc.ip4 = ip4[:0]
		sc.ip6 = ip6[:0]
		sortIPScratchPool.Put(sc)
		return ips
	}

	out := sc.out[:0]
	if cap(out) < len(ips) {
		out = make([]net.IP, 0, len(ips))
	}
	consumeIP4 := 0
	consumeIP6 := 0
	consumeTurn := uint32(0)
	ip4turn := true
	if prioritizeIPv6 {
		ip4turn = false
	}
	for {
		if ip4turn {
			out = append(out, ip4[consumeIP4])
			consumeIP4++
			if consumeIP4 == len(ip4) {
				out = append(out, ip6[consumeIP6:]...)
				break
			}
			consumeTurn++
			if consumeTurn == interleave {
				ip4turn = false
				consumeTurn = uint32(0)
			}
		} else {
			out = append(out, ip6[consumeIP6])
			consumeIP6++
			if consumeIP6 == len(ip6) {
				out = append(out, ip4[consumeIP4:]...)
				break
			}
			consumeTurn++
			if consumeTurn == interleave {
				ip4turn = true
				consumeTurn = uint32(0)
			}
		}
	}

	// Caller-owned result: copy out of scratch so Put cannot race retainers.
	result := make([]net.IP, len(out))
	copy(result, out)
	for i := range ip4 {
		ip4[i] = nil
	}
	for i := range ip6 {
		ip6[i] = nil
	}
	for i := range out {
		out[i] = nil
	}
	sc.ip4 = ip4[:0]
	sc.ip6 = ip6[:0]
	sc.out = out[:0]
	if cap(sc.ip4) <= 128 && cap(sc.ip6) <= 128 && cap(sc.out) <= 128 {
		sortIPScratchPool.Put(sc)
	}
	return result
}

func tcpTryDial(ctx context.Context, src net.Address, sockopt *SocketConfig, ip net.IP, port net.Port, index int, resultCh chan<- *result, originalDomain string) {
	conn, err := effectiveSystemDialer.Dial(ctx, src, net.Destination{Address: net.IPAddress(ip), Network: net.Network_TCP, Port: port, OriginalDomain: originalDomain}, sockopt)
	select {
	case <-ctx.Done():
		if conn != nil {
			conn.Close()
		}
		resultCh <- &result{err: ctx.Err(), index: index}
		return
	default:
		if err != nil {
			resultCh <- &result{err: err, index: index}
			return
		}
		resultCh <- &result{conn: conn, index: index}
		return
	}
}

func tcpTryDialV3(ctx context.Context, src net.Address, sockopt *SocketConfig, ip net.IP, port net.Port, index int, resultCh chan<- *result, originalDomain string) {
	start := time.Now()
	conn, err := effectiveSystemDialer.Dial(ctx, src, net.Destination{Address: net.IPAddress(ip), Network: net.Network_TCP, Port: port, OriginalDomain: originalDomain}, sockopt)
	rtt := time.Since(start)
	select {
	case <-ctx.Done():
		if conn != nil {
			conn.Close()
		}
		resultCh <- &result{err: ctx.Err(), index: index}
		return
	default:
		if err != nil {
			resultCh <- &result{err: err, index: index, rtt: rtt}
			return
		}
		resultCh <- &result{conn: conn, index: index, rtt: rtt}
		return
	}
}
