package conf

import (
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

// POC 验证：65458e91 (Config: Fix some issues #6640)
//
// 该修复前，validateOutboundTransportSecurity 读取的是 VLessOutboundConfig.Address /
// TrojanClientConfig.Address —— 这两个顶层字段在 VLESS/Trojan 出站配置中永远为 nil
// （真实地址位于 vnext[0].address / servers[0].address），导致
// requiresTransportSecurity(nil) 恒返回 false，"禁止公网明文出站"的校验形同虚设。
//
// 本测试锁定修复后的行为：公网地址必须被拒，私网地址仍然放行。

func pocAddr(s string) *Address {
	return &Address{Address: net.ParseAddress(s)}
}

// 无安全层的 sender：StreamSettings 为 nil，GetSecurityType() 返回 ""
func pocPlainSender() *proxyman.SenderConfig {
	return &proxyman.SenderConfig{}
}

// 带 TLS 的 sender：应当直接短路放行
func pocTLSSender() *proxyman.SenderConfig {
	return &proxyman.SenderConfig{
		StreamSettings: &internet.StreamConfig{SecurityType: "tls"},
	}
}

func TestPOC_VlessPublicAddrWithoutTLS_Rejected(t *testing.T) {
	cfg := &VLessOutboundConfig{
		Vnext: []*VLessOutboundVnext{{Address: pocAddr("1.2.3.4"), Port: 443}},
	}
	if err := validateOutboundTransportSecurity(cfg, pocPlainSender()); err == nil {
		t.Fatal("VLESS → 公网 IPv4 且未启用 TLS，必须被拒绝（修复前此处会漏判）")
	}
}

func TestPOC_TrojanPublicAddrWithoutTLS_Rejected(t *testing.T) {
	cfg := &TrojanClientConfig{
		Servers: []*TrojanServerTarget{{Address: pocAddr("1.2.3.4"), Port: 443}},
	}
	if err := validateOutboundTransportSecurity(cfg, pocPlainSender()); err == nil {
		t.Fatal("Trojan → 公网 IPv4 且未启用 TLS，必须被拒绝（修复前此处会漏判）")
	}
}

func TestPOC_PublicDomainWithoutTLS_Rejected(t *testing.T) {
	cfg := &VLessOutboundConfig{
		Vnext: []*VLessOutboundVnext{{Address: pocAddr("example.com"), Port: 443}},
	}
	if err := validateOutboundTransportSecurity(cfg, pocPlainSender()); err == nil {
		t.Fatal("VLESS → 公网域名且未启用 TLS，必须被拒绝（修复前此处会漏判）")
	}
}

func TestPOC_PrivateAddrWithoutTLS_Allowed(t *testing.T) {
	cases := []struct {
		name string
		cfg  any
	}{
		{"vless/192.168", &VLessOutboundConfig{
			Vnext: []*VLessOutboundVnext{{Address: pocAddr("192.168.1.1"), Port: 443}}}},
		{"vless/10.x", &VLessOutboundConfig{
			Vnext: []*VLessOutboundVnext{{Address: pocAddr("10.0.0.1"), Port: 443}}}},
		{"trojan/127.0.0.1", &TrojanClientConfig{
			Servers: []*TrojanServerTarget{{Address: pocAddr("127.0.0.1"), Port: 443}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateOutboundTransportSecurity(tc.cfg, pocPlainSender()); err != nil {
				t.Fatalf("私网地址未启用 TLS 应当放行，却被拒绝: %v", err)
			}
		})
	}
}

// 域名公私判定的边界：私有域名表收录的是 RFC 2606 保留 TLD
// (example/invalid/test/local) 与 lan/localhost/internal 等后缀，按"子域"匹配。
// 因此 host.example 私有，而 example.com 属于 .com 真实注册域 —— 是公网。
// 这条边界曾误伤过既有测试（把 example.com 当私有域），此处显式锁定。
func TestPOC_DomainPrivateBoundary(t *testing.T) {
	cases := []struct {
		domain   string
		isPublic bool
	}{
		{"host.example", false}, // RFC 2606 保留 TLD 的子域
		{"box.lan", false},      // 局域网后缀
		{"localhost", false},    // 回环
		{"a.internal", false},   // 内网后缀
		{"x.invalid", false},    // RFC 2606 保留 TLD
		{"example.com", true},   // IANA 持有的真实公网注册域，非 example TLD 子域
		{"cdn.cloudflare.com", true},
	}
	for _, tc := range cases {
		t.Run(tc.domain, func(t *testing.T) {
			cfg := &VLessOutboundConfig{
				Vnext: []*VLessOutboundVnext{{Address: pocAddr(tc.domain), Port: 443}},
			}
			err := validateOutboundTransportSecurity(cfg, pocPlainSender())
			rejected := err != nil
			if rejected != tc.isPublic {
				t.Fatalf("%s: 判定为%s(isPublic=%v) 但校验 rejected=%v，期望 rejected=%v",
					tc.domain, map[bool]string{true: "公网", false: "私网"}[tc.isPublic],
					tc.isPublic, rejected, tc.isPublic)
			}
		})
	}
}

func TestPOC_TLSSenderShortCircuits(t *testing.T) {
	cfg := &VLessOutboundConfig{
		Vnext: []*VLessOutboundVnext{{Address: pocAddr("1.2.3.4"), Port: 443}},
	}
	if err := validateOutboundTransportSecurity(cfg, pocTLSSender()); err != nil {
		t.Fatalf("已启用 TLS 的出站不应被拒绝: %v", err)
	}
}

// 回归保护：将来若有人把地址源改回顶层 Address 字段，这个测试会立刻失败。
func TestPOC_AddressSourceIsVnext(t *testing.T) {
	// 顶层 Address 故意留空，只有 vnext[0] 带公网地址。
	cfg := &VLessOutboundConfig{
		Vnext: []*VLessOutboundVnext{{Address: pocAddr("8.8.8.8"), Port: 443}},
	}
	if cfg.Address != nil {
		t.Fatal("测试前提不成立：顶层 Address 应为空")
	}
	if err := validateOutboundTransportSecurity(cfg, pocPlainSender()); err == nil {
		t.Fatal("地址源回退到顶层 Address 会导致校验失效")
	}
}
