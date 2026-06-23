package conf

import (
	"fmt"
	"net"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/proxy/netbridge"
	"google.golang.org/protobuf/proto"
)

// NetBridgeConfig is the JSON configuration for netbridge inbound.
type NetBridgeConfig struct {
	ListenAddress string `json:"listenAddress"`
	ListenPort    uint32 `json:"listenPort"`
	UDPPort       uint32 `json:"udpPort"`
	Token         uint32 `json:"token"`
	UserLevel     uint32 `json:"userLevel"`
}

// Build implements Buildable.
func (c *NetBridgeConfig) Build() (proto.Message, error) {
	cfg := &netbridge.Config{
		ListenAddress: c.ListenAddress,
		ListenPort:    c.ListenPort,
		UdpPort:       c.UDPPort,
		Token:         c.Token,
		UserLevel:     c.UserLevel,
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1"
	}
	if cfg.ListenPort == 0 {
		cfg.ListenPort = 35000
	}
	if cfg.UdpPort == 0 {
		cfg.UdpPort = 35001
	}

	// SECURITY: Enforce loopback-only binding
	if err := validateLoopback(cfg.ListenAddress); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validateLoopback ensures the address is 127.0.0.1 or ::1.
func validateLoopback(addr string) error {
	ip := net.ParseIP(addr)
	if ip == nil {
		return fmt.Errorf("netbridge: invalid listen address %q", addr)
	}
	if !ip.IsLoopback() {
		return errors.New("netbridge: SECURITY VIOLATION — listen address must be 127.0.0.1 or ::1, got ", addr)
	}
	return nil
}
