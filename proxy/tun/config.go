package tun

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

type InterfaceUpdater struct {
	sync.Mutex

	tunIndex  int
	fixedName string
	iface     *net.Interface

	// debounceTimer merges rapid-fire netlink events into a single Update() call.
	debounceTimer *time.Timer
	quit          chan struct{}
}

var updater *InterfaceUpdater

func (updater *InterfaceUpdater) Get() *net.Interface {
	updater.Lock()
	defer updater.Unlock()

	return updater.iface
}

// Start begins background interface monitoring. On platforms with netlink
// support (Linux), it uses event-driven route watching for sub-second
// response to network switches. On other platforms it falls back to
// periodic polling every 10 minutes.
func (updater *InterfaceUpdater) Start() {
	updater.Update() // initial selection

	// Try platform-specific event listener; falls back to polling.
	if !startNetlinkListener(updater) {
		go updater.pollLoop()
	}
}

// startNetlinkListener is a platform-specific hook. The default returns
// false (not supported). On Linux it is replaced in init() to use netlink.
var startNetlinkListener = func(updater *InterfaceUpdater) bool {
	return false
}

func (updater *InterfaceUpdater) Update() {
	updater.Lock()
	defer updater.Unlock()

	got, err := findOutboundInterface(updater.tunIndex, updater.fixedName)
	if err != nil {
		errors.LogInfoInner(context.Background(), err, "[tun] failed to update interface")
		updater.iface = nil
		return
	}

	if got == nil {
		errors.LogInfo(context.Background(), "[tun] failed to update interface > got == nil")
		updater.iface = nil
		return
	}

	if updater.iface != nil && updater.iface.Index == got.Index && updater.iface.Name == got.Name {
		return
	}

	updater.iface = got
	errors.LogInfo(context.Background(), "[tun] update interface ", got.Name, " ", got.Index)
}

// pollLoop periodically refreshes the interface selection. Used as a
// fallback on platforms without netlink event support.
func (updater *InterfaceUpdater) pollLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			updater.Update()
		case <-updater.quit:
			return
		}
	}
}

func (updater *InterfaceUpdater) debounceUpdate() {
	updater.Lock()
	defer updater.Unlock()

	const debounceInterval = 500 * time.Millisecond
	if updater.debounceTimer != nil {
		updater.debounceTimer.Stop()
	}
	updater.debounceTimer = time.AfterFunc(debounceInterval, updater.Update)
}
