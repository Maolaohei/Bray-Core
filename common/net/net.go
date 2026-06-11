// Package net is a drop-in replacement to Golang's net package, with some more functionalities.
package net // import "github.com/xtls/xray-core/common/net"

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// defines the maximum time an idle TCP session can survive in the tunnel, so
// it should be consistent across HTTP versions and with other transports.
const ConnIdleTimeout = 300 * time.Second

// consistent with quic-go
const QuicgoH3KeepAlivePeriod = 10 * time.Second

// ChromeH2KeepAlivePeriod is the idle timeout after which an HTTP/2
// health-check PING is sent before the next request (request-driven,
// not timer-driven). Go's http2.Transport pings only when the
// connection has been idle for this duration and a new request arrives.
// The PING confirms the peer is still alive; on failure the connection
// is closed and a new one is created.
// 30s is conservative: NAT timeouts are typically ≥60s, and normal
// traffic keeps the connection alive so PINGs rarely fire.
const ChromeH2KeepAlivePeriod = 30 * time.Second

var ErrNotLocal = errors.New("the source address is not from local machine.")

type localIPCacheEntry struct {
	addrs      []net.Addr
	lastUpdate time.Time
}

var localIPCache = atomic.Pointer[localIPCacheEntry]{}

func IsLocal(ip net.IP) (bool, error) {
	var addrs []net.Addr
	if entry := localIPCache.Load(); entry == nil || time.Since(entry.lastUpdate) > time.Minute {
		var err error
		addrs, err = net.InterfaceAddrs()
		if err != nil {
			return false, err
		}
		localIPCache.Store(&localIPCacheEntry{
			addrs:      addrs,
			lastUpdate: time.Now(),
		})
	} else {
		addrs = entry.addrs
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok {
			if ipnet.IP.Equal(ip) {
				return true, nil
			}
		}
	}
	return false, nil
}
