package conf

import (
	"net/http"
	"strings"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/transport/internet/splithttp"
)

func TestSplitHTTPDefaultPaddingMethodMatchesBrayWire(t *testing.T) {
	built, err := (&SplitHTTPConfig{}).Build()
	if err != nil {
		t.Fatal(err)
	}
	config := built.(*splithttp.Config)
	if config.XPaddingMethod != "" {
		t.Fatalf("default non-obfs padding method=%q, want unset dual-accept policy", config.XPaddingMethod)
	}
	assertBrayPaddingAccepted(t, config, false)
	assertBrayPaddingAccepted(t, config, true)
}

func TestSplitHTTPExplicitRepeatXPaddingMatchesWire(t *testing.T) {
	built, err := (&SplitHTTPConfig{
		XPaddingBytes:  Int32Range{From: 1000, To: 1000},
		XPaddingMethod: string(splithttp.PaddingMethodRepeatX),
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	config := built.(*splithttp.Config)
	assertBrayPaddingAccepted(t, config, false)
	assertBrayPaddingAccepted(t, config, true)
}

func TestSplitHTTPObfsDefaultPaddingMethodRemainsRepeatX(t *testing.T) {
	built, err := (&SplitHTTPConfig{
		XPaddingObfsMode: true,
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	config := built.(*splithttp.Config)
	if config.XPaddingMethod != string(splithttp.PaddingMethodRepeatX) {
		t.Fatalf("default obfs padding method=%q, want repeat-x", config.XPaddingMethod)
	}
}

func TestSplitHTTPDefaultServerPaddingAcceptsLegacyRepeatX(t *testing.T) {
	built, err := (&SplitHTTPConfig{}).Build()
	if err != nil {
		t.Fatal(err)
	}
	server := built.(*splithttp.Config)
	window := server.GetNormalizedXPaddingBytes()
	from, to := splithttp.AcceptedPaddingRange(window.From, window.To)
	legacy := strings.Repeat("X", 100)
	if !server.IsPaddingValid(legacy, from, to, splithttp.PaddingMethod(server.XPaddingMethod)) {
		t.Fatal("default server must continue accepting legacy repeat-x padding")
	}
}

func assertBrayPaddingAccepted(t *testing.T, config *splithttp.Config, packet bool) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "https://example.test/xhttp", nil)
	if err != nil {
		t.Fatal(err)
	}
	if packet {
		if err := config.FillPacketRequest(req, "session", "0", buf.MergeBytes(nil, []byte("payload"))); err != nil {
			t.Fatal(err)
		}
		defer req.Body.Close()
	} else {
		config.FillStreamRequest(req, "session", "")
	}
	padding, _ := config.ExtractXPaddingFromRequest(req, config.XPaddingObfsMode)
	window := config.GetNormalizedXPaddingBytes()
	from, to := splithttp.AcceptedPaddingRange(window.From, window.To)
	if !config.IsPaddingValid(padding, from, to, splithttp.PaddingMethod(config.XPaddingMethod)) {
		t.Fatalf("packet=%t method=%q padding_len=%d accepted=[%d,%d]", packet, config.XPaddingMethod, len(padding), from, to)
	}
	if config.XPaddingMethod == string(splithttp.PaddingMethodRepeatX) && strings.Trim(padding, "X") != "" {
		t.Fatalf("explicit repeat-x emitted non-repeat padding")
	}
}
