package dns

import (
	"context"
	go_errors "errors"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/signal/pubsub"
	"github.com/xtls/xray-core/features/dns"
)

type CachedNameserver interface {
	getCacheController() *CacheController

	sendQuery(ctx context.Context, noResponseErrCh chan<- error, fqdn string, option dns.IPOption)
}

// queryIP is called from dns.Server->queryIPTimeout
func queryIP(ctx context.Context, s CachedNameserver, domain string, option dns.IPOption) ([]net.IP, uint32, error) {
	fqdn := Fqdn(domain)

	cache := s.getCacheController()
	if !cache.disableCache {
		if rec := cache.findRecords(fqdn); rec != nil {
			ips, ttl, err := merge(option, rec.A, rec.AAAA)
			if !go_errors.Is(err, errRecordNotFound) {
				if ttl > 0 {
					errors.LogDebugInner(ctx, err, cache.name, " cache HIT ", fqdn, " -> ", ips)
					log.Record(&log.DNSLog{Server: cache.name, Domain: fqdn, Result: ips, Status: log.DNSCacheHit, Elapsed: 0, Error: err})
					return ips, uint32(ttl), err
				}
				// serveExpiredTTL is stored as -graceSeconds (see NewCacheController).
				// getIPs() returns remaining TTL as ceil(Until(Expire)); once expired this is
				// negative (e.g. expired 10s ago => ttl≈-10). Grace 30s is stored as -30.
				// Allow stale when unlimited (0) or still within grace: serveExpiredTTL < ttl
				// (e.g. -30 < -10). Outside grace: -30 < -40 is false.
				if cache.allowServeStale(ttl) {
					errors.LogDebugInner(ctx, err, cache.name, " cache OPTIMISTE ", fqdn, " -> ", ips)
					log.Record(&log.DNSLog{Server: cache.name, Domain: fqdn, Result: ips, Status: log.DNSCacheOptimiste, Elapsed: 0, Error: err})
					go pull(ctx, s, fqdn, option)
					return ips, 1, err
				}
			}
		}
	} else {
		errors.LogDebug(ctx, "DNS cache is disabled. Querying IP for ", fqdn, " at ", cache.name)
	}

	return fetch(ctx, s, fqdn, option)
}

// allowServeStale reports whether an expired record with remainingTTL (from getIPs,
// negative once past Expire) may still be served under serve-stale policy.
func (c *CacheController) allowServeStale(remainingTTL int32) bool {
	if !c.serveStale {
		return false
	}
	// grace==0 is stored as 0 and means unlimited stale while entry is still present.
	if c.serveExpiredTTL == 0 {
		return true
	}
	// Within grace: stored -grace is still less than remainingTTL (e.g. -30 < -10).
	return c.serveExpiredTTL < remainingTTL
}

func pull(ctx context.Context, s CachedNameserver, fqdn string, option dns.IPOption) {
	nctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()

	fetch(nctx, s, fqdn, option)
}

func fetch(ctx context.Context, s CachedNameserver, fqdn string, option dns.IPOption) ([]net.IP, uint32, error) {
	key := fqdn
	switch {
	case option.IPv4Enable && option.IPv6Enable:
		key = key + "46"
	case option.IPv4Enable:
		key = key + "4"
	case option.IPv6Enable:
		key = key + "6"
	}

	v, _, _ := s.getCacheController().requestGroup.Do(key, func() (any, error) {
		// Leader-independent context: the merged flight must not fail every
		// follower just because the first caller's context was cancelled. Keep
		// the deadline (if any) so the query still times out, but sever the
		// cancellation coupling.
		fctx := context.WithoutCancel(ctx)
		if dl, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			fctx, cancel = context.WithDeadline(fctx, dl)
			defer cancel()
		}
		return doFetch(fctx, s, fqdn, option), nil
	})
	ret := v.(result)

	return ret.ips, ret.ttl, ret.error
}

type result struct {
	ips []net.IP
	ttl uint32
	error
}

func doFetch(ctx context.Context, s CachedNameserver, fqdn string, option dns.IPOption) result {
	sub4, sub6 := s.getCacheController().registerSubscribers(fqdn, option)
	defer closeSubscribers(sub4, sub6)

	noResponseErrCh := make(chan error, 2)
	onEvent := func(sub *pubsub.Subscriber) (*IPRecord, error) {
		if sub == nil {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-noResponseErrCh:
			return nil, err
		case msg := <-sub.Wait():
			sub.Close()
			return msg.(*IPRecord), nil // should panic
		}
	}

	start := time.Now()
	s.sendQuery(ctx, noResponseErrCh, fqdn, option)

	// Wait A and AAAA in parallel so dual-stack RTT is max(A,AAAA), not sum.
	var rec4, rec6 *IPRecord
	var err4, err6 error
	if sub4 != nil && sub6 != nil {
		type waitResult struct {
			rec *IPRecord
			err error
		}
		ch4 := make(chan waitResult, 1)
		ch6 := make(chan waitResult, 1)
		go func() {
			r, e := onEvent(sub4)
			ch4 <- waitResult{r, e}
		}()
		go func() {
			r, e := onEvent(sub6)
			ch6 <- waitResult{r, e}
		}()
		wr4 := <-ch4
		wr6 := <-ch6
		rec4, err4 = wr4.rec, wr4.err
		rec6, err6 = wr6.rec, wr6.err
	} else {
		rec4, err4 = onEvent(sub4)
		rec6, err6 = onEvent(sub6)
	}

	var errs []error
	if err4 != nil {
		errs = append(errs, err4)
	}
	if err6 != nil {
		errs = append(errs, err6)
	}

	ips, ttl, err := merge(option, rec4, rec6, errs...)
	var rTTL uint32
	if ttl > 0 {
		rTTL = uint32(ttl)
	} else if ttl == 0 && go_errors.Is(err, errRecordNotFound) {
		rTTL = 0
	} else { // edge case: where a fast rep's ttl expires during the rtt of a slower, parallel query
		rTTL = 1
	}

	log.Record(&log.DNSLog{Server: s.getCacheController().name, Domain: fqdn, Result: ips, Status: log.DNSQueried, Elapsed: time.Since(start), Error: err})
	return result{ips, rTTL, err}
}

func merge(option dns.IPOption, rec4 *IPRecord, rec6 *IPRecord, errs ...error) ([]net.IP, int32, error) {
	var allIPs []net.IP
	var rTTL int32 = dns.DefaultTTL

	mergeReq := option.IPv4Enable && option.IPv6Enable

	var err4, err6 error
	if option.IPv4Enable {
		ips, ttl, err := rec4.getIPs() // it's safe
		if !mergeReq {
			return ips, ttl, err
		}
		// In dual-stack we must not bail out on one family's failure (e.g. the
		// AAAA query was lost/timed out, or the domain simply has no AAAA): the
		// other family's already-resolved IPs are still valid. Only single-stack
		// may short-circuit here (DNS 专项 D2).
		err4 = err
		if len(ips) > 0 {
			if ttl < rTTL {
				rTTL = ttl
			}
			allIPs = append(allIPs, ips...)
		}
	}

	if option.IPv6Enable {
		ips, ttl, err := rec6.getIPs() // it's safe
		if !mergeReq {
			return ips, ttl, err
		}
		err6 = err
		if len(ips) > 0 {
			if ttl < rTTL {
				rTTL = ttl
			}
			allIPs = append(allIPs, ips...)
		}
	}

	if len(allIPs) > 0 {
		return allIPs, rTTL, nil
	}

	// No IPs collected. Prefer the per-family getIPs() errors (e.g. NXDOMAIN's
	// NameError, or ErrEmptyResponse for a family with no records) over the
	// transport-level errs, since a successful query can still carry a record
	// error. Only when no family reported a record error do we fall back to the
	// transport errs, and finally to a clean nil (DNS 专项 D2).
	var recErrs []error
	if err4 != nil {
		recErrs = append(recErrs, err4)
	}
	if err6 != nil {
		recErrs = append(recErrs, err6)
	}
	if len(recErrs) > 0 {
		if len(recErrs) == 2 && go_errors.Is(recErrs[0], recErrs[1]) {
			return nil, rTTL, recErrs[0]
		}
		return nil, rTTL, errors.Combine(recErrs...)
	}
	if len(errs) > 0 {
		if len(errs) == 2 && go_errors.Is(errs[0], errs[1]) {
			return nil, rTTL, errs[0]
		}
		return nil, rTTL, errors.Combine(errs...)
	}
	return nil, rTTL, nil
}
