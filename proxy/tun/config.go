package tun

import (
	"context"
	"net"
	"sort"
	"strings"
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

	if updater.iface != nil {
		iface, err := net.InterfaceByIndex(updater.iface.Index)
		if err == nil && iface.Name == updater.iface.Name {
			return
		}
	}

	updater.iface = nil

	interfaces, err := net.Interfaces()
	if err != nil {
		errors.LogInfoInner(context.Background(), err, "[tun] failed to update interface")
		return
	}

	var got *net.Interface
	if updater.fixedName != "" {
		for _, iface := range interfaces {
			if iface.Index == updater.tunIndex {
				continue
			}
			if iface.Name == updater.fixedName {
				got = &iface
				break
			}
		}
	} else {
		var ifs []struct {
			index int
			score int
		}
		for i, iface := range interfaces {
			if iface.Index == updater.tunIndex {
				continue
			}
			if strings.Contains(iface.Name, "vEthernet") {
				continue
			}
			if iface.Flags&net.FlagUp == 0 {
				continue
			}
			if iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil || len(addrs) == 0 {
				continue
			}
			ifs = append(ifs, struct {
				index int
				score int
			}{i, score(&iface, addrs)})
		}
		sort.Slice(ifs, func(i, j int) bool {
			if ifs[i].score != ifs[j].score {
				return ifs[i].score > ifs[j].score
			}

			return interfaces[ifs[i].index].Name < interfaces[ifs[j].index].Name
		})
		if len(ifs) > 0 {
			iface := interfaces[ifs[0].index]
			got = &iface
		}
	}

	if got == nil {
		errors.LogInfo(context.Background(), "[tun] failed to update interface > got == nil")
		return
	}

	updater.iface = got
	errors.LogInfo(context.Background(), "[tun] update interface ", got.Name, " ", got.Index)
}

func score(iface *net.Interface, addrs []net.Addr) int {
	score := 0

	name := strings.ToLower(iface.Name)
	if strings.Contains(name, "wlan") || strings.Contains(name, "wi-fi") {
		score += 2
	}

	for _, addr := range addrs {
		if strings.HasPrefix(addr.String(), "192.168.") {
			score += 1
			break
		}
	}

	return score
}

// pollLoop periodically refreshes the interface selection. Used as a
// fallback on platforms without netlink event support.
func (updater *InterfaceUpdater) pollLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		updater.Update()
	}
}

// debounceUpdate coalesces rapid netlink events so that a burst of
// route changes (e.g., old default deleted + new default added during
// a WiFi→cellular switch) triggers only one Update() call.
func (updater *InterfaceUpdater) debounceUpdate() {
	updater.Lock()
	defer updater.Unlock()

	const debounceInterval = 500 * time.Millisecond
	if updater.debounceTimer != nil {
		updater.debounceTimer.Stop()
	}
	updater.debounceTimer = time.AfterFunc(debounceInterval, updater.Update)
}
