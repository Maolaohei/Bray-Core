package conf_test

import (
	"strings"
	"testing"

	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/tun"
)

func TestTunConfigDefaultNameAndDesc(t *testing.T) {
	cfg := &conf.TunConfig{}
	msg, err := cfg.Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	tc, ok := msg.(*tun.Config)
	if !ok {
		t.Fatalf("unexpected type %T", msg)
	}
	if !strings.HasPrefix(tc.Name, "utun") {
		t.Fatalf("Name=%q want utun prefix", tc.Name)
	}
	if tc.Name == "xray0" {
		t.Fatalf("Name unexpectedly stayed at legacy xray0")
	}
	if tc.Desc != "Wintun" {
		t.Fatalf("Desc=%q want Wintun", tc.Desc)
	}
	if tc.MTU != 1500 {
		t.Fatalf("MTU=%d want 1500", tc.MTU)
	}
}

func TestTunConfigExplicitNameAndDescPreserved(t *testing.T) {
	cfg := &conf.TunConfig{
		Name: "xray0",
		Desc: "CustomTunnel",
		MTU:  1400,
	}
	msg, err := cfg.Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	tc := msg.(*tun.Config)
	if tc.Name != "xray0" {
		t.Fatalf("Name=%q want xray0", tc.Name)
	}
	if tc.Desc != "CustomTunnel" {
		t.Fatalf("Desc=%q want CustomTunnel", tc.Desc)
	}
	if tc.MTU != 1400 {
		t.Fatalf("MTU=%d want 1400", tc.MTU)
	}
}

func TestGetAvailableTunNameFormat(t *testing.T) {
	name, err := conf.GetAvailableTunName()
	if err != nil {
		t.Fatalf("GetAvailableTunName: %v", err)
	}
	if !strings.HasPrefix(name, "utun") {
		t.Fatalf("name=%q want utun prefix", name)
	}
}
