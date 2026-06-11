package internet

import (
	"strings"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/features/outbound"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ExtractWarmupDomains extracts domains from outbound handlers that should be
// pre-resolved at startup for faster first connection.
//
// Priority:
//   - Node domains (proxy server addresses)
//   - REALITY dest/serverName
//
// Returns deduplicated list of domain strings.
func ExtractWarmupDomains(obm outbound.Manager) []string {
	if obm == nil {
		return nil
	}

	handlers := obm.ListHandlers(nil)
	if len(handlers) == 0 {
		return nil
	}

	domainSet := make(map[string]struct{}, 16)
	for _, handler := range handlers {
		extractDomainsFromHandler(handler, domainSet)
	}

	domains := make([]string, 0, len(domainSet))
	for d := range domainSet {
		domains = append(domains, d)
	}
	return domains
}

func extractDomainsFromHandler(handler outbound.Handler, domainSet map[string]struct{}) {
	if handler == nil {
		return
	}

	// 1. Extract from proxy settings (node domains)
	if ps := handler.ProxySettings(); ps != nil {
		extractDomainsFromProxySettings(ps, domainSet)
	}

	// 2. Extract from sender settings (REALITY dest/serverName)
	if ss := handler.SenderSettings(); ss != nil {
		extractDomainsFromSenderSettings(ss, domainSet)
	}
}

func extractDomainsFromProxySettings(ps *serial.TypedMessage, domainSet map[string]struct{}) {
	if ps == nil {
		return
	}

	msg, err := ps.GetInstance()
	if err != nil || msg == nil {
		return
	}

	// Extract server address from protocol configs
	serverAddr := extractServerAddress(msg)
	if serverAddr != "" && !isIP(serverAddr) {
		domainSet[serverAddr] = struct{}{}
	}
}

func extractServerAddress(msg interface{}) string {
	// VLESS outbound config: single Vnext field
	type hasSingleVnext interface {
		GetVnext() *protocol.ServerEndpoint
	}
	// VMess outbound config: slice of vnext
	type hasSliceVnext interface {
		GetVnext() []*protocol.ServerEndpoint
	}
	type hasServer interface {
		GetServer() string
	}

	if v, ok := msg.(hasSingleVnext); ok {
		if vnext := v.GetVnext(); vnext != nil {
			if addr := vnext.GetAddress(); addr != nil {
				return addr.GetDomain()
			}
		}
	}
	if v, ok := msg.(hasSliceVnext); ok {
		if vnext := v.GetVnext(); len(vnext) > 0 {
			if addr := vnext[0].GetAddress(); addr != nil {
				return addr.GetDomain()
			}
		}
	}
	if v, ok := msg.(hasServer); ok {
		return v.GetServer()
	}
	return ""
}

func extractDomainsFromSenderSettings(ss *serial.TypedMessage, domainSet map[string]struct{}) {
	if ss == nil {
		return
	}

	msg, err := ss.GetInstance()
	if err != nil || msg == nil {
		return
	}

	// Use protobuf reflection to extract fields without direct type imports
	pbMsg, ok := msg.(protoreflect.ProtoMessage)
	if !ok {
		return
	}

	protoreflectMsg := pbMsg.ProtoReflect()
	streamField := protoreflectMsg.Descriptor().Fields().ByName("stream_settings")
	if streamField == nil {
		return
	}
	if !protoreflectMsg.Has(streamField) {
		return
	}

	streamMsg := protoreflectMsg.Get(streamField).Message()
	if !streamMsg.IsValid() {
		return
	}

	// Extract security_type
	secTypeField := streamMsg.Descriptor().Fields().ByName("security_type")
	if secTypeField != nil && streamMsg.Has(secTypeField) {
		secType := streamMsg.Get(secTypeField).String()
		if secType == "reality" {
			extractRealityDomainsFromMsg(streamMsg, domainSet)
		}
	}

	// Extract transport host (CDN edge domains for Spider path simulation)
	extractTransportHostDomains(streamMsg, domainSet)

	// Extract header fakeHost (Spider simulation targets)
	extractHeaderFakeHostDomains(streamMsg, domainSet)
}

// extractTransportHostDomains extracts host domains from transport settings.
// Note: Transport host extraction requires proper proto unmarshaling of TypedMessage,
// which would cause circular dependencies. For now, this is a no-op.
// Transport hosts can be explicitly configured via dns.warmupDomains.
func extractTransportHostDomains(streamMsg protoreflect.Message, domainSet map[string]struct{}) {
	// Intentionally empty - extracting from TypedMessage bytes is unreliable
	// Use dns.warmupDomains config for explicit CDN domain warmup
}

// extractHeaderFakeHostDomains extracts fakeHost from header settings.
// Note: Header settings are nested inside transport TypedMessage.
// For now, this is a no-op due to the same circular dependency issue.
func extractHeaderFakeHostDomains(streamMsg protoreflect.Message, domainSet map[string]struct{}) {
	// Intentionally empty - extracting from TypedMessage bytes is unreliable
	// Use dns.warmupDomains config for explicit header host warmup
}

func extractRealityDomainsFromMsg(streamMsg protoreflect.Message, domainSet map[string]struct{}) {
	secSettingsField := streamMsg.Descriptor().Fields().ByName("security_settings")
	if secSettingsField == nil {
		return
	}
	if !streamMsg.Has(secSettingsField) {
		return
	}

	secSettingsList := streamMsg.Get(secSettingsField).List()
	for i := 0; i < secSettingsList.Len(); i++ {
		secSetting := secSettingsList.Get(i).Message()
		if !secSetting.IsValid() {
			continue
		}

		snField := secSetting.Descriptor().Fields().ByName("server_name")
		destField := secSetting.Descriptor().Fields().ByName("dest")
		serverNamesField := secSetting.Descriptor().Fields().ByName("server_names")

		if snField == nil && destField == nil {
			continue
		}

		if snField != nil && secSetting.Has(snField) {
			sn := secSetting.Get(snField).String()
			if sn != "" {
				domainSet[sn] = struct{}{}
			}
		}

		if destField != nil && secSetting.Has(destField) {
			dest := secSetting.Get(destField).String()
			if dest != "" {
				host := dest
				if idx := strings.LastIndex(dest, ":"); idx > 0 {
					host = dest[:idx]
				}
				if host != "" && !isIP(host) {
					domainSet[host] = struct{}{}
				}
			}
		}

		if serverNamesField != nil && secSetting.Has(serverNamesField) {
			namesList := secSetting.Get(serverNamesField).List()
			for j := 0; j < namesList.Len(); j++ {
				sn := namesList.Get(j).String()
				if sn != "" {
					domainSet[sn] = struct{}{}
				}
			}
		}
	}
}

func isIP(s string) bool {
	if len(s) == 0 {
		return false
	}
	return s[0] >= '0' && s[0] <= '9'
}
