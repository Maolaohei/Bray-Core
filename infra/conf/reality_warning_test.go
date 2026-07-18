package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/transport/internet/reality"
)

func TestREALITYConfigDefaultMinClientVerAndRiskyServerName(t *testing.T) {
	// Keep privateKey valid (32 bytes base64url).
	rc := &conf.REALITYConfig{
		Dest:        json.RawMessage(`"www.example.com:443"`),
		ServerNames: []string{"www.Microsoft.com", "cdn.example.cn"},
		PrivateKey:  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		ShortIds:    []string{"0123456789abcdef"},
	}
	msg, err := rc.Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	cfg, ok := msg.(*reality.Config)
	if !ok {
		t.Fatalf("unexpected type %T", msg)
	}
	if len(cfg.MinClientVer) != 3 || cfg.MinClientVer[0] != 26 || cfg.MinClientVer[1] != 3 || cfg.MinClientVer[2] != 27 {
		t.Fatalf("MinClientVer=%v want [26 3 27]", cfg.MinClientVer)
	}
}

func TestREALITYConfigExplicitMinClientVerPreserved(t *testing.T) {
	rc := &conf.REALITYConfig{
		Dest:         json.RawMessage(`"www.example.com:443"`),
		ServerNames:  []string{"www.example.com"},
		PrivateKey:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		ShortIds:     []string{"0123456789abcdef"},
		MinClientVer: "1.2.3",
	}
	msg, err := rc.Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	cfg := msg.(*reality.Config)
	if len(cfg.MinClientVer) != 3 || cfg.MinClientVer[0] != 1 || cfg.MinClientVer[1] != 2 || cfg.MinClientVer[2] != 3 {
		t.Fatalf("MinClientVer=%v want [1 2 3]", cfg.MinClientVer)
	}
}
