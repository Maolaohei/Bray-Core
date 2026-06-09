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

// consistent with chrome (reduced from 45s to 10s to prevent NAT
// timeout and TCP slow start reset during video buffering pauses.
// H2 PING frames every 10s keep the kernel from considering the
// connection idle, preserving the congestion window across segments.
const ChromeH2KeepAlivePeriod = 10 * time.Second

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
