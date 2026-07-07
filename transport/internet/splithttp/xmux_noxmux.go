//go:build noxmux

package splithttp

func init() {
	forceNewConnection = true
}
