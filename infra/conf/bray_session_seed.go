package conf

import (
	"encoding/json"
	"strings"

	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/proxy/vless"
	vinbound "github.com/xtls/xray-core/proxy/vless/inbound"
	voutbound "github.com/xtls/xray-core/proxy/vless/outbound"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"google.golang.org/protobuf/proto"
)

// injectBraySessionSeedFromVLESS wires VLESS account UUIDs into XHTTP local
// control header x-bray-session-uuid so session MAC is derived from the same
// UUID already configured for VLESS. Operators can still override with
// x-bray-session-secret (checked first by splithttp). Never overwrites an
// already-set secret or uuid seed header.
func injectBraySessionSeedFromVLESS(stream *internet.StreamConfig, proxyMsg proto.Message) {
	if stream == nil || proxyMsg == nil {
		return
	}
	seeds := collectVLESSUUIDSeeds(proxyMsg)
	if len(seeds) == 0 {
		return
	}
	applySessionUUIDSeeds(stream, seeds)
}

func collectVLESSUUIDSeeds(proxyMsg proto.Message) []string {
	switch cfg := proxyMsg.(type) {
	case *voutbound.Config:
		if cfg == nil {
			return nil
		}
		// Bray-Core VLESS outbound: single vnext endpoint with a single user.
		if rec := cfg.GetVnext(); rec != nil {
			if id := uuidFromProtocolUser(rec.GetUser()); id != "" {
				return []string{id}
			}
		}
		return nil
	case *vinbound.Config:
		if cfg == nil {
			return nil
		}
		out := make([]string, 0, len(cfg.GetUsers()))
		seen := make(map[string]struct{}, len(cfg.GetUsers()))
		for _, user := range cfg.GetUsers() {
			if id := uuidFromProtocolUser(user); id != "" {
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out
	default:
		return nil
	}
}

func uuidFromProtocolUser(user interface{ GetAccount() *serial.TypedMessage }) string {
	if user == nil {
		return ""
	}
	accMsg := user.GetAccount()
	if accMsg == nil {
		return ""
	}
	inst, err := accMsg.GetInstance()
	if err != nil || inst == nil {
		return ""
	}
	acc, ok := inst.(*vless.Account)
	if !ok || acc == nil {
		return ""
	}
	id := strings.TrimSpace(acc.GetId())
	if id == "" {
		return ""
	}
	// Normalize via parse so client/server casing matches.
	u, err := uuid.ParseString(id)
	if err != nil {
		return strings.ToLower(id)
	}
	return strings.ToLower(u.String())
}

func applySessionUUIDSeeds(stream *internet.StreamConfig, seeds []string) {
	if stream == nil || len(seeds) == 0 {
		return
	}
	joined := strings.Join(seeds, ",")
	for _, ts := range stream.TransportSettings {
		if ts == nil || ts.ProtocolName != "splithttp" || ts.Settings == nil {
			continue
		}
		inst, err := ts.Settings.GetInstance()
		if err != nil || inst == nil {
			continue
		}
		cfg, ok := inst.(*splithttp.Config)
		if !ok || cfg == nil {
			continue
		}
		if lookupBrayHeader(cfg.Headers, splithttp.BraySessionSecretHeader) != "" {
			// Explicit shared secret wins; leave alone.
			continue
		}
		if lookupBrayHeader(cfg.Headers, splithttp.BraySessionUUIDHeader) != "" {
			// Operator already set UUID seed(s).
			continue
		}
		if cfg.Headers == nil {
			cfg.Headers = make(map[string]string, 1)
		}
		cfg.Headers[splithttp.BraySessionUUIDHeader] = joined
		// Re-marshal into TypedMessage so runtime sees the seed.
		ts.Settings = serial.ToTypedMessage(cfg)

		// downloadSettings may carry a nested stream; seed it too when present.
		if cfg.DownloadSettings != nil {
			applySessionUUIDSeeds(cfg.DownloadSettings, seeds)
			// Nested mutation may have changed DownloadSettings; re-pack outer.
			ts.Settings = serial.ToTypedMessage(cfg)
		}
	}
}

func lookupBrayHeader(headers map[string]string, key string) string {
	if headers == nil {
		return ""
	}
	if v, ok := headers[key]; ok {
		if t := strings.TrimSpace(v); t != "" {
			return t
		}
	}
	for k, v := range headers {
		if strings.EqualFold(k, key) {
			if t := strings.TrimSpace(v); t != "" {
				return t
			}
		}
	}
	return ""
}

// extractVLESSUUIDFromRawSettings is a lightweight pre-Build helper used when
// only JSON settings are available (kept for tests).
func extractVLESSUUIDFromRawSettings(settings []byte, isOutbound bool) []string {
	if len(settings) == 0 {
		return nil
	}
	if isOutbound {
		var c VLessOutboundConfig
		if err := json.Unmarshal(settings, &c); err != nil {
			return nil
		}
		ids := make([]string, 0, 1)
		if c.Id != "" {
			if u, err := uuid.ParseString(c.Id); err == nil {
				ids = append(ids, strings.ToLower(u.String()))
			}
		}
		for _, rec := range c.Vnext {
			for _, raw := range rec.Users {
				var acc struct {
					Id string `json:"id"`
				}
				if json.Unmarshal(raw, &acc) == nil && acc.Id != "" {
					if u, err := uuid.ParseString(acc.Id); err == nil {
						ids = append(ids, strings.ToLower(u.String()))
					}
				}
			}
		}
		return uniqueStrings(ids)
	}
	var c VLessInboundConfig
	if err := json.Unmarshal(settings, &c); err != nil {
		return nil
	}
	users := c.Users
	if c.Clients != nil {
		users = c.Clients
	}
	ids := make([]string, 0, len(users))
	for _, raw := range users {
		var acc struct {
			Id string `json:"id"`
		}
		if json.Unmarshal(raw, &acc) == nil && acc.Id != "" {
			if u, err := uuid.ParseString(acc.Id); err == nil {
				ids = append(ids, strings.ToLower(u.String()))
			}
		}
	}
	return uniqueStrings(ids)
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
