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
// These are often CDN edge domains used by Spider path simulation.
func extractTransportHostDomains(streamMsg protoreflect.Message, domainSet map[string]struct{}) {
	tsField := streamMsg.Descriptor().Fields().ByName("transport_settings")
	if tsField == nil || !streamMsg.Has(tsField) {
		return
	}

	tsList := streamMsg.Get(tsField).List()
	for i := 0; i < tsList.Len(); i++ {
		tsMsg := tsList.Get(i).Message()
		if !tsMsg.IsValid() {
			continue
		}

		// Transport settings have a "settings" field which is a TypedMessage
		settingsField := tsMsg.Descriptor().Fields().ByName("settings")
		if settingsField == nil || !tsMsg.Has(settingsField) {
			continue
		}

		// The settings field contains a TypedMessage (type_url + value)
		// We can't easily unwrap it via reflection without circular deps
		// Instead, we'll use a heuristic approach on the raw bytes
		settingsVal := tsMsg.Get(settingsField)
		if !settingsVal.IsValid() {
			continue
		}

		// For TypedMessage, the value is in the "value" field
		settingsInnerMsg := settingsVal.Message()
		if !settingsInnerMsg.IsValid() {
			continue
		}

		valueField := settingsInnerMsg.Descriptor().Fields().ByName("value")
		if valueField == nil || !settingsInnerMsg.Has(valueField) {
			continue
		}

		valueBytes := settingsInnerMsg.Get(valueField).Bytes()
		if len(valueBytes) == 0 {
			continue
		}

		// Try to extract domain from the serialized bytes
		// This is a best-effort approach for transport host extraction
		extractDomainFromProtoBytes(valueBytes, domainSet)
	}
}

// extractDomainFromProtoBytes tries to extract domain strings from serialized proto bytes.
// This is a heuristic approach for cases where we can't use proper unmarshaling.
func extractDomainFromProtoBytes(data []byte, domainSet map[string]struct{}) {
	// Look for printable ASCII sequences that look like domains
	// This is a simple heuristic and may have false positives
	if len(data) == 0 {
		return
	}

	// Scan for domain-like patterns
	start := -1
	for i, b := range data {
		if b >= 32 && b <= 126 && b != ' ' {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 && i-start > 3 {
				s := string(data[start:i])
				if isDomainLike(s) {
					domainSet[s] = struct{}{}
				}
			}
			start = -1
		}
	}
	if start != -1 && len(data)-start > 3 {
		s := string(data[start:])
		if isDomainLike(s) {
			domainSet[s] = struct{}{}
		}
	}
}

// isDomainLike checks if a string looks like a domain name.
func isDomainLike(s string) bool {
	if len(s) < 4 || len(s) > 255 {
		return false
	}
	// Must contain at least one dot
	hasDot := false
	for _, c := range s {
		if c == '.' {
			hasDot = true
			break
		}
	}
	if !hasDot {
		return false
	}
	// Must start with alphanumeric
	if !((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= '0' && s[0] <= '9')) {
		return false
	}
	return true
}

// extractHeaderFakeHostDomains extracts fakeHost from HTTP header settings.
// These are Spider simulation target domains (e.g., cdnjs.cloudflare.com).
func extractHeaderFakeHostDomains(streamMsg protoreflect.Message, domainSet map[string]struct{}) {
	hsField := streamMsg.Descriptor().Fields().ByName("header_settings")
	if hsField == nil || !streamMsg.Has(hsField) {
		return
	}

	// header_settings is a TypedMessage
	hsMsg := streamMsg.Get(hsField).Message()
	if !hsMsg.IsValid() {
		return
	}

	// Extract value bytes
	valueField := hsMsg.Descriptor().Fields().ByName("value")
	if valueField == nil || !hsMsg.Has(valueField) {
		return
	}

	valueBytes := hsMsg.Get(valueField).Bytes()
	if len(valueBytes) == 0 {
		return
	}

	extractDomainFromProtoBytes(valueBytes, domainSet)
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
