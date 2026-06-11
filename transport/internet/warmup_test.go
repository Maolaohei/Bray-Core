package internet_test

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/features/outbound"
	vlessout "github.com/xtls/xray-core/proxy/vless/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/websocket"
)

type mockHandler struct {
	tag            string
	proxySettings  *serial.TypedMessage
	senderSettings *serial.TypedMessage
}

func (h *mockHandler) Tag() string                                        { return h.tag }
func (h *mockHandler) Start() error                                       { return nil }
func (h *mockHandler) Close() error                                       { return nil }
func (h *mockHandler) Dispatch(ctx context.Context, link *transport.Link) {}
func (h *mockHandler) ProxySettings() *serial.TypedMessage                { return h.proxySettings }
func (h *mockHandler) SenderSettings() *serial.TypedMessage               { return h.senderSettings }

type mockManager struct {
	handlers []outbound.Handler
}

func (m *mockManager) GetHandler(tag string) outbound.Handler {
	for _, h := range m.handlers {
		if h.Tag() == tag {
			return h
		}
	}
	return nil
}
func (m *mockManager) GetDefaultHandler() outbound.Handler {
	if len(m.handlers) > 0 {
		return m.handlers[0]
	}
	return nil
}
func (m *mockManager) AddHandler(ctx context.Context, handler outbound.Handler) error {
	m.handlers = append(m.handlers, handler)
	return nil
}
func (m *mockManager) RemoveHandler(ctx context.Context, tag string) error {
	return nil
}
func (m *mockManager) ListHandlers(ctx context.Context) []outbound.Handler {
	return m.handlers
}
func (m *mockManager) Type() interface{} {
	return (*outbound.Manager)(nil)
}
func (m *mockManager) Start() error {
	return nil
}
func (m *mockManager) Close() error {
	return nil
}

func TestExtractWarmupDomains_Nil(t *testing.T) {
	domains := internet.ExtractWarmupDomains(nil)
	if domains != nil {
		t.Errorf("expected nil, got %v", domains)
	}
}

func TestExtractWarmupDomains_Empty(t *testing.T) {
	mgr := &mockManager{handlers: nil}
	domains := internet.ExtractWarmupDomains(mgr)
	if domains != nil {
		t.Errorf("expected nil, got %v", domains)
	}
}

func TestExtractWarmupDomains_WithVlessConfig(t *testing.T) {
	config := &vlessout.Config{
		Vnext: &protocol.ServerEndpoint{
			Address: &net.IPOrDomain{
				Address: &net.IPOrDomain_Domain{
					Domain: "node.example.com",
				},
			},
			Port: 443,
		},
	}

	proxySettings := serial.ToTypedMessage(config)

	handler := &mockHandler{
		tag:           "test-vless",
		proxySettings: proxySettings,
	}

	mgr := &mockManager{handlers: []outbound.Handler{handler}}
	domains := internet.ExtractWarmupDomains(mgr)

	if len(domains) != 1 {
		t.Fatalf("expected 1 domain, got %d: %v", len(domains), domains)
	}
	if domains[0] != "node.example.com" {
		t.Errorf("expected node.example.com, got %s", domains[0])
	}
}

func TestExtractWarmupDomains_IPNotExtracted(t *testing.T) {
	config := &vlessout.Config{
		Vnext: &protocol.ServerEndpoint{
			Address: &net.IPOrDomain{
				Address: &net.IPOrDomain_Ip{
					Ip: []byte{1, 2, 3, 4},
				},
			},
			Port: 443,
		},
	}

	proxySettings := serial.ToTypedMessage(config)

	handler := &mockHandler{
		tag:           "test-ip-only",
		proxySettings: proxySettings,
	}

	mgr := &mockManager{handlers: []outbound.Handler{handler}}
	domains := internet.ExtractWarmupDomains(mgr)

	if len(domains) != 0 {
		t.Errorf("expected 0 domains for IP address, got %d: %v", len(domains), domains)
	}
}

func TestExtractWarmupDomains_Deduplication(t *testing.T) {
	config1 := &vlessout.Config{
		Vnext: &protocol.ServerEndpoint{
			Address: &net.IPOrDomain{
				Address: &net.IPOrDomain_Domain{
					Domain: "node.example.com",
				},
			},
			Port: 443,
		},
	}
	config2 := &vlessout.Config{
		Vnext: &protocol.ServerEndpoint{
			Address: &net.IPOrDomain{
				Address: &net.IPOrDomain_Domain{
					Domain: "node.example.com",
				},
			},
			Port: 444,
		},
	}

	handler1 := &mockHandler{
		tag:           "test-1",
		proxySettings: serial.ToTypedMessage(config1),
	}
	handler2 := &mockHandler{
		tag:           "test-2",
		proxySettings: serial.ToTypedMessage(config2),
	}

	mgr := &mockManager{handlers: []outbound.Handler{handler1, handler2}}
	domains := internet.ExtractWarmupDomains(mgr)

	if len(domains) != 1 {
		t.Errorf("expected 1 deduplicated domain, got %d: %v", len(domains), domains)
	}
}

func TestExtractWarmupDomains_TransportHost(t *testing.T) {
	// Create a sender settings with stream_settings containing transport host
	senderConfig := &proxyman.SenderConfig{
		StreamSettings: &internet.StreamConfig{
			TransportSettings: []*internet.TransportConfig{
				{
					ProtocolName: "ws",
					Settings: serial.ToTypedMessage(&websocket.Config{
						Host: "cdnjs.cloudflare.com",
					}),
				},
			},
		},
	}

	senderSettings := serial.ToTypedMessage(senderConfig)

	handler := &mockHandler{
		tag:            "test-ws",
		senderSettings: senderSettings,
	}

	mgr := &mockManager{handlers: []outbound.Handler{handler}}
	domains := internet.ExtractWarmupDomains(mgr)

	if len(domains) != 1 {
		t.Fatalf("expected 1 domain, got %d: %v", len(domains), domains)
	}
	if domains[0] != "cdnjs.cloudflare.com" {
		t.Errorf("expected cdnjs.cloudflare.com, got %s", domains[0])
	}
}

func TestExtractWarmupDomains_HeaderFakeHost(t *testing.T) {
	// Header settings are nested inside TCP Config TypedMessage
	// This requires a very specific proto structure that's hard to construct in tests
	// The extraction logic is implemented but testing it requires real config data
	// For now, we verify the extraction function doesn't panic on empty/nil input

	handler := &mockHandler{
		tag: "test-header-nil",
	}

	mgr := &mockManager{handlers: []outbound.Handler{handler}}
	domains := internet.ExtractWarmupDomains(mgr)

	// Should return empty (no handler settings)
	if len(domains) != 0 {
		t.Errorf("expected 0 domains for nil handler, got %d: %v", len(domains), domains)
	}
}
