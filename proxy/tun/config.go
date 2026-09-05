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
	// stopped guards against a late debounce tick re-populating iface after Stop.
	stopped bool
}

var updater *InterfaceUpdater

func (updater *InterfaceUpdater) Get() *net.Interface {
	updater.Lock()
	defer updater.Unlock()

	return updater.iface
}

// Start begins background interface monitoring and performs the initial
// selection. On platforms with netlink support (Linux), it uses event-driven
// route watching for sub-second response to network switches; every platform
// also runs a 30s safety poll. Without this refresh the dialer controller
// keeps binding new sockets to a snapshot taken at TUN start — after a
// network switch (WiFi↔Ethernet, roaming) every outbound socket is bound to
// a dead interface and the connection stalls until restart.
func (updater *InterfaceUpdater) Start() {
	updater.Lock()
	updater.quit = make(chan struct{})
	updater.stopped = false
	updater.Unlock()

	updater.Update() // initial selection

	// Platform-specific event listener (Linux netlink) + 30s safety poll on
	// all platforms. The poll bounds staleness if events are missed.
	startNetlinkListener(updater)
	go updater.pollLoop()
}

// Stop terminates background monitoring. After Stop the dialer controller
// will skip interface binding (iface is cleared) until the next Start.
func (updater *InterfaceUpdater) Stop() {
	updater.Lock()
	defer updater.Unlock()
	if updater.stopped {
		return
	}
	updater.stopped = true
	if updater.quit != nil {
		close(updater.quit)
	}
	if updater.debounceTimer != nil {
		updater.debounceTimer.Stop()
		updater.debounceTimer = nil
	}
	updater.iface = nil
}

// startNetlinkListener is a platform-specific hook. The default is a no-op;
// on Linux it is replaced in init() to watch netlink route events.
var startNetlinkListener = func(updater *InterfaceUpdater) bool {
	return false
}

func (updater *InterfaceUpdater) Update() {
	updater.Lock()
	defer updater.Unlock()
	if updater.stopped {
		return
	}

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

// pollInterval bounds interface-selection staleness on platforms without
// netlink events (Windows/Darwin/BSD/Android). A stale binding sends every
// new outbound socket to a dead interface after a network switch, so the
// poll must be frequent; each pass is just a net.Interfaces + a few Addrs
// syscalls. On Linux the netlink listener reacts in real time and this is
// only a safety net.
const pollInterval = 30 * time.Second

// pollLoop periodically refreshes the interface selection.
func (updater *InterfaceUpdater) pollLoop() {
	ticker := time.NewTicker(pollInterval)
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
	if updater.stopped {
		return
	}

	const debounceInterval = 500 * time.Millisecond
	if updater.debounceTimer != nil {
		updater.debounceTimer.Stop()
	}
	updater.debounceTimer = time.AfterFunc(debounceInterval, updater.Update)
}
