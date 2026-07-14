package stats

import "sync"

// Optional lifecycle hooks for real stats.Manager implementations.
// Transport layers can bind observability without importing app/stats.
// NoopManager never invokes these (its Start/Close are empty).

var (
	hooksMu           sync.Mutex
	managerStartHooks []func(Manager)
	managerCloseHooks []func(Manager)
)

// OnManagerStart registers a callback invoked after Manager.Start succeeds
// (outside the manager lock). Safe to call from package init.
func OnManagerStart(fn func(Manager)) {
	if fn == nil {
		return
	}
	hooksMu.Lock()
	managerStartHooks = append(managerStartHooks, fn)
	hooksMu.Unlock()
}

// OnManagerClose registers a callback invoked on Manager.Close.
func OnManagerClose(fn func(Manager)) {
	if fn == nil {
		return
	}
	hooksMu.Lock()
	managerCloseHooks = append(managerCloseHooks, fn)
	hooksMu.Unlock()
}

// InvokeManagerStartHooks runs registered start hooks. Used by app/stats.
func InvokeManagerStartHooks(m Manager) {
	if m == nil {
		return
	}
	hooksMu.Lock()
	hooks := append([]func(Manager){}, managerStartHooks...)
	hooksMu.Unlock()
	for _, h := range hooks {
		h(m)
	}
}

// InvokeManagerCloseHooks runs registered close hooks. Used by app/stats.
func InvokeManagerCloseHooks(m Manager) {
	if m == nil {
		return
	}
	hooksMu.Lock()
	hooks := append([]func(Manager){}, managerCloseHooks...)
	hooksMu.Unlock()
	for _, h := range hooks {
		h(m)
	}
}
