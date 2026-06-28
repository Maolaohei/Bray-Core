package internet

import (
	"strings"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/features/outbound"
	"google.golang.org/protobuf/proto"
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
// For example, WebSocket "host" field and TCP header "domain" field.
func extractTransportHostDomains(streamMsg protoreflect.Message, domainSet map[string]struct{}) {
	tSettingsField := streamMsg.Descriptor().Fields().ByName("transport_settings")
	if tSettingsField == nil || !streamMsg.Has(tSettingsField) {
		return
	}

	tSettingsList := streamMsg.Get(tSettingsField).List()
	for i := 0; i < tSettingsList.Len(); i++ {
		tConfig := tSettingsList.Get(i).Message()
		if !tConfig.IsValid() {
			continue
		}

		settingsField := tConfig.Descriptor().Fields().ByName("settings")
		if settingsField == nil || !tConfig.Has(settingsField) {
			continue
		}

		settingsMsg := tConfig.Get(settingsField).Message()
		if !settingsMsg.IsValid() {
			continue
		}

		extractDomainsFromTypedMessage(settingsMsg, domainSet)
	}
}

// extractHeaderFakeHostDomains extracts fakeHost from TCP header settings.
func extractHeaderFakeHostDomains(streamMsg protoreflect.Message, domainSet map[string]struct{}) {
	tSettingsField := streamMsg.Descriptor().Fields().ByName("transport_settings")
	if tSettingsField == nil || !streamMsg.Has(tSettingsField) {
		return
	}

	tSettingsList := streamMsg.Get(tSettingsField).List()
	for i := 0; i < tSettingsList.Len(); i++ {
		tConfig := tSettingsList.Get(i).Message()
		if !tConfig.IsValid() {
			continue
		}

		protoNameField := tConfig.Descriptor().Fields().ByName("protocol_name")
		if protoNameField == nil || !tConfig.Has(protoNameField) {
			continue
		}
		protoName := tConfig.Get(protoNameField).String()
		if protoName != "tcp" {
			continue
		}

		settingsField := tConfig.Descriptor().Fields().ByName("settings")
		if settingsField == nil || !tConfig.Has(settingsField) {
			continue
		}

		settingsMsg := tConfig.Get(settingsField).Message()
		if !settingsMsg.IsValid() {
			continue
		}

		extractHeaderDomainsFromTCP(settingsMsg, domainSet)
	}
}

// extractDomainsFromTypedMessage deserializes a TypedMessage and looks for
// a "host" string field containing a domain.
func extractDomainsFromTypedMessage(settingsMsg protoreflect.Message, domainSet map[string]struct{}) {
	typeField := settingsMsg.Descriptor().Fields().ByName("type")
	valueField := settingsMsg.Descriptor().Fields().ByName("value")
	if typeField == nil || valueField == nil {
		return
	}
	if !settingsMsg.Has(typeField) || !settingsMsg.Has(valueField) {
		return
	}

	msgType := settingsMsg.Get(typeField).String()
	msgBytes := settingsMsg.Get(valueField).Bytes()

	instance, err := serial.GetInstance(msgType)
	if err != nil {
		return
	}

	pbMsg, ok := instance.(protoreflect.ProtoMessage)
	if !ok {
		return
	}

	if err := proto.Unmarshal(msgBytes, pbMsg); err != nil {
		return
	}

	innerMsg := pbMsg.ProtoReflect()

	// Look for "host" field (string) — common in WebSocket, HTTP/2, etc.
	hostField := innerMsg.Descriptor().Fields().ByName("host")
	if hostField != nil && innerMsg.Has(hostField) && hostField.Kind() == protoreflect.StringKind {
		host := innerMsg.Get(hostField).String()
		if host != "" && !isIP(host) {
			domainSet[host] = struct{}{}
		}
	}
}

// extractHeaderDomainsFromTCP extracts fake domains from TCP header settings.
func extractHeaderDomainsFromTCP(settingsMsg protoreflect.Message, domainSet map[string]struct{}) {
	typeField := settingsMsg.Descriptor().Fields().ByName("type")
	valueField := settingsMsg.Descriptor().Fields().ByName("value")
	if typeField == nil || valueField == nil {
		return
	}
	if !settingsMsg.Has(typeField) || !settingsMsg.Has(valueField) {
		return
	}

	msgType := settingsMsg.Get(typeField).String()
	msgBytes := settingsMsg.Get(valueField).Bytes()

	instance, err := serial.GetInstance(msgType)
	if err != nil {
		return
	}

	pbMsg, ok := instance.(protoreflect.ProtoMessage)
	if !ok {
		return
	}

	if err := proto.Unmarshal(msgBytes, pbMsg); err != nil {
		return
	}

	innerMsg := pbMsg.ProtoReflect()

	// TCP header settings have a "domain" field with a list of domain names
	domainField := innerMsg.Descriptor().Fields().ByName("domain")
	if domainField == nil || !innerMsg.Has(domainField) {
		return
	}
	if domainField.Kind() != protoreflect.MessageKind {
		return
	}

	domainList := innerMsg.Get(domainField).List()
	for i := 0; i < domainList.Len(); i++ {
		domainMsg := domainList.Get(i).Message()
		if !domainMsg.IsValid() {
			continue
		}
		// Each domain entry has a "domain" string field
		dField := domainMsg.Descriptor().Fields().ByName("domain")
		if dField != nil && domainMsg.Has(dField) {
			d := domainMsg.Get(dField).String()
			if d != "" && !isIP(d) {
				domainSet[d] = struct{}{}
			}
		}
	}
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
