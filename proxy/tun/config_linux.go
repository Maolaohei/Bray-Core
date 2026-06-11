//go:build linux && !android

package tun

import (
	"context"

	"github.com/vishvananda/netlink"
	"github.com/xtls/xray-core/common/errors"
)

func init() {
	// Override startNetlinkListener with the Linux netlink implementation.
	startNetlinkListener = func(updater *InterfaceUpdater) bool {
		ch := make(chan netlink.RouteUpdate, 16)
		if err := netlink.RouteSubscribe(ch, nil); err != nil {
			errors.LogInfo(context.Background(), "[tun] netlink route subscribe failed, falling back to polling: ", err)
			return false
		}

		go func() {
			for range ch {
				updater.debounceUpdate()
			}
		}()

		errors.LogInfo(context.Background(), "[tun] netlink route listener started")
		return true
	}
}
