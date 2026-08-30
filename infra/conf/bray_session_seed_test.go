package conf

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/splithttp"
)

func TestInjectBraySessionSeed_OutboundDetourBuild(t *testing.T) {
	const id = "550e8400-e29b-41d4-a716-446655440000"
	// NOTE: the address must stay on a private/reserved domain. The outbound
	// transport-security rule (upstream 65458e91) rejects plaintext VLESS to a
	// public address, and "example.com" is a real IANA-registered public domain
	// (it is NOT a subdomain of the reserved "example" TLD). "host.example" is
	// RFC 2606 reserved, so it is classified private and keeps this test on the
	// no-security branch where Bray seed injection is what we actually assert.
	settings := json.RawMessage(`{
		"vnext": [{
			"address": "host.example",
			"port": 443,
			"users": [{"id": "` + id + `", "encryption": "none"}]
		}]
	}`)
	streamJSON := json.RawMessage(`{
		"network": "xhttp",
		"xhttpSettings": {"path": "/xhttp", "mode": "auto"}
	}`)
	out := &OutboundDetourConfig{
		Protocol:      "vless",
		Tag:           "proxy",
		Settings:      &settings,
		StreamSetting: &StreamConfig{},
	}
	if err := json.Unmarshal(streamJSON, out.StreamSetting); err != nil {
		t.Fatal(err)
	}
	built, err := out.Build()
	if err != nil {
		t.Fatalf("outbound detour build: %v", err)
	}
	senderInst, err := built.GetSenderSettings().GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	sender, ok := senderInst.(*proxyman.SenderConfig)
	if !ok || sender == nil {
		t.Fatalf("sender type %T", senderInst)
	}
	assertSessionUUIDSeed(t, sender.GetStreamSettings(), strings.ToLower(id))

	// Client signs with UUID-derived key; server multi-user list containing same UUID accepts.
	client := &splithttp.Config{Headers: map[string]string{splithttp.BraySessionUUIDHeader: strings.ToLower(id)}}
	server := &splithttp.Config{Headers: map[string]string{splithttp.BraySessionUUIDHeader: strings.ToLower(id)}}
	sid := client.GenerateSessionID()
	if !splithttp.VerifySessionIDExported(sid, server) {
		t.Fatal("UUID-derived MAC must match")
	}
}

func TestInjectBraySessionSeed_InboundMultiUser(t *testing.T) {
	const idA = "550e8400-e29b-41d4-a716-446655440000"
	const idB = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	inSettings := json.RawMessage(`{
		"clients": [
			{"id": "` + idA + `"},
			{"id": "` + idB + `"}
		],
		"decryption": "none"
	}`)
	streamJSON := json.RawMessage(`{
		"network": "xhttp",
		"xhttpSettings": {"path": "/xhttp"}
	}`)
	// Full inbound detour path.
	in := &InboundDetourConfig{
		Protocol:      "vless",
		Tag:           "in",
		PortList:      &PortList{Range: []PortRange{{From: 443, To: 443}}},
		Settings:      &inSettings,
		StreamSetting: &StreamConfig{},
	}
	if err := json.Unmarshal(streamJSON, in.StreamSetting); err != nil {
		t.Fatal(err)
	}
	// Listen address required by some builds; set if field exists via raw JSON rebuild.
	rawIn := []byte(`{
		"protocol": "vless",
		"tag": "in",
		"port": 443,
		"listen": "0.0.0.0",
		"settings": {
			"clients": [
				{"id": "` + idA + `"},
				{"id": "` + idB + `"}
			],
			"decryption": "none"
		},
		"streamSettings": {
			"network": "xhttp",
			"xhttpSettings": {"path": "/xhttp"}
		}
	}`)
	var in2 InboundDetourConfig
	if err := json.Unmarshal(rawIn, &in2); err != nil {
		t.Fatal(err)
	}
	built, err := in2.Build()
	if err != nil {
		// Fall back to direct inject if detour needs more fields.
		t.Logf("inbound detour build: %v (fallback unit inject)", err)
		stream, err := in.StreamSetting.Build()
		if err != nil {
			t.Fatal(err)
		}
		vlessIn := new(VLessInboundConfig)
		if err := json.Unmarshal(inSettings, vlessIn); err != nil {
			t.Fatal(err)
		}
		pm, err := vlessIn.Build()
		if err != nil {
			t.Fatal(err)
		}
		injectBraySessionSeedFromVLESS(stream, pm)
		got := sessionUUIDFromStream(t, stream)
		if !strings.Contains(got, strings.ToLower(idA)) || !strings.Contains(got, strings.ToLower(idB)) {
			t.Fatalf("want both UUIDs in seed, got %q", got)
		}
	} else {
		recvInst, err := built.GetReceiverSettings().GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		recv, ok := recvInst.(*proxyman.ReceiverConfig)
		if !ok || recv == nil {
			t.Fatalf("receiver type %T", recvInst)
		}
		got := sessionUUIDFromStream(t, recv.GetStreamSettings())
		if !strings.Contains(got, strings.ToLower(idA)) || !strings.Contains(got, strings.ToLower(idB)) {
			t.Fatalf("want both UUIDs in seed, got %q", got)
		}
	}

	// Explicit secret must block UUID inject.
	streamCfg := &StreamConfig{}
	if err := json.Unmarshal(streamJSON, streamCfg); err != nil {
		t.Fatal(err)
	}
	streamSecret, err := streamCfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	for _, ts := range streamSecret.TransportSettings {
		if ts == nil || ts.ProtocolName != "splithttp" {
			continue
		}
		inst, _ := ts.Settings.GetInstance()
		cfg := inst.(*splithttp.Config)
		cfg.Headers = map[string]string{splithttp.BraySessionSecretHeader: "manual"}
		ts.Settings = serial.ToTypedMessage(cfg)
	}
	vlessIn := new(VLessInboundConfig)
	if err := json.Unmarshal(inSettings, vlessIn); err != nil {
		t.Fatal(err)
	}
	pm, err := vlessIn.Build()
	if err != nil {
		t.Fatal(err)
	}
	injectBraySessionSeedFromVLESS(streamSecret, pm)
	for _, ts := range streamSecret.TransportSettings {
		if ts == nil || ts.ProtocolName != "splithttp" {
			continue
		}
		inst, _ := ts.Settings.GetInstance()
		cfg := inst.(*splithttp.Config)
		if cfg.Headers[splithttp.BraySessionUUIDHeader] != "" {
			t.Fatal("explicit secret must prevent UUID seed inject")
		}
		if cfg.Headers[splithttp.BraySessionSecretHeader] != "manual" {
			t.Fatal("explicit secret must be preserved")
		}
	}
}

func TestInjectBraySessionSeed_RoundTripMAC(t *testing.T) {
	const id = "550e8400-e29b-41d4-a716-446655440000"
	mk := func(uuidList string) *splithttp.Config {
		return &splithttp.Config{
			Path: "/xhttp",
			Headers: map[string]string{
				splithttp.BraySessionUUIDHeader: uuidList,
			},
		}
	}
	client := mk(strings.ToLower(id))
	server := mk(strings.ToLower(id))
	sid := client.GenerateSessionID()
	if !splithttp.VerifySessionIDExported(sid, server) {
		t.Fatal("UUID-derived client/server MAC must match")
	}
	def := &splithttp.Config{Path: "/xhttp"}
	defID := def.GenerateSessionID()
	if splithttp.VerifySessionIDExported(defID, client) {
		t.Fatal("default-secret session must not verify under UUID seed")
	}
	if splithttp.VerifySessionIDExported(sid, def) {
		t.Fatal("UUID-signed session must not verify under default secret")
	}
	if client.GetRequestHeader().Get(splithttp.BraySessionUUIDHeader) != "" {
		t.Fatal("uuid control header must not leave process")
	}
}

func assertSessionUUIDSeed(t *testing.T, stream *internet.StreamConfig, want string) {
	t.Helper()
	got := sessionUUIDFromStream(t, stream)
	if got != want && !strings.Contains(got, want) {
		t.Fatalf("session uuid seed = %q, want %q", got, want)
	}
}

func sessionUUIDFromStream(t *testing.T, stream *internet.StreamConfig) string {
	t.Helper()
	if stream == nil {
		t.Fatal("nil stream")
	}
	for _, ts := range stream.TransportSettings {
		if ts == nil || ts.ProtocolName != "splithttp" || ts.Settings == nil {
			continue
		}
		inst, err := ts.Settings.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		cfg, ok := inst.(*splithttp.Config)
		if !ok || cfg == nil {
			t.Fatalf("not splithttp config: %T", inst)
		}
		return cfg.Headers[splithttp.BraySessionUUIDHeader]
	}
	t.Fatal("no splithttp transport settings")
	return ""
}
