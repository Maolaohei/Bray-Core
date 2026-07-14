package splithttp_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

func Test_GetNormalizedPath(t *testing.T) {
	tests := []struct {
		TestName           string
		Path               string
		SessionIDPlacement string
		SeqPlacement       string
		Expected           string
	}{
		{
			TestName: "default placement keeps trailing slash",
			Path:     "/sh",
			Expected: "/sh/",
		},
		{
			TestName: "query string is stripped",
			Path:     "/?world",
			Expected: "/",
		},
		{
			TestName:           "both off path drops trailing slash",
			Path:               "/stream",
			SessionIDPlacement: "query",
			SeqPlacement:       "query",
			Expected:           "/stream",
		},
		{
			TestName:           "both off path keeps file-like path",
			Path:               "/stream/filename.extension",
			SessionIDPlacement: "query",
			SeqPlacement:       "header",
			Expected:           "/stream/filename.extension",
		},
		{
			TestName:           "seq in path keeps trailing slash",
			Path:               "/stream",
			SessionIDPlacement: "query",
			Expected:           "/stream/",
		},
		{
			TestName:     "session in path keeps trailing slash",
			Path:         "/stream",
			SeqPlacement: "cookie",
			Expected:     "/stream/",
		},
		{
			TestName:           "existing trailing slash preserved",
			Path:               "/stream/",
			SessionIDPlacement: "query",
			SeqPlacement:       "query",
			Expected:           "/stream/",
		},
		{
			TestName:           "root unchanged",
			Path:               "/",
			SessionIDPlacement: "query",
			SeqPlacement:       "query",
			Expected:           "/",
		},
	}
	for _, test := range tests {
		t.Run(test.TestName, func(t *testing.T) {
			c := Config{
				Path:               test.Path,
				SessionIDPlacement: test.SessionIDPlacement,
				SeqPlacement:       test.SeqPlacement,
			}
			assert.Equal(t, test.Expected, c.GetNormalizedPath())
		})
	}
}

func TestXmuxBrowserDefaults(t *testing.T) {
	var m XmuxConfig
	c := m.GetNormalizedMaxConcurrency()
	// Process-stable ±10% jitter stays inside browser band (not fixed 8-16).
	if c.From < 4 || c.To > 32 || c.From > c.To {
		t.Fatalf("concurrency default out of band {%d,%d}", c.From, c.To)
	}
	n := m.GetNormalizedMaxConnections()
	if n.From < 1 || n.To > 8 || n.From > n.To {
		t.Fatalf("connections default out of band {%d,%d}", n.From, n.To)
	}
	r := m.GetNormalizedCMaxReuseTimes()
	if r.From < 32 || r.To > 256 || r.From > r.To {
		t.Fatalf("reuse default out of band {%d,%d}", r.From, r.To)
	}
	s := m.GetNormalizedHMaxReusableSecs()
	if s.From < 300 || s.To > 1800 || s.From > s.To {
		t.Fatalf("reusable secs default out of band {%d,%d}", s.From, s.To)
	}
	// Same process: defaults are stable (not re-rolled per call).
	c2 := m.GetNormalizedMaxConcurrency()
	if c2.From != c.From || c2.To != c.To {
		t.Fatalf("concurrency default not process-stable {%d,%d} vs {%d,%d}", c.From, c.To, c2.From, c2.To)
	}
	// Explicit zero-range still honored when set.
	m.MaxConcurrency = &RangeConfig{From: 0, To: 0}
	if m.GetNormalizedMaxConcurrency().From != 0 || m.GetNormalizedMaxConcurrency().To != 0 {
		t.Fatal("explicit zero concurrency must win")
	}
	// Explicit non-nil range wins over jittered defaults.
	m.MaxConnections = &RangeConfig{From: 3, To: 3}
	if got := m.GetNormalizedMaxConnections(); got.From != 3 || got.To != 3 {
		t.Fatalf("explicit connections must win, got {%d,%d}", got.From, got.To)
	}
}

func TestGetRequestHeaderStripsBrayControl(t *testing.T) {
	c := Config{
		Headers: map[string]string{
			"User-Agent":               "BrayTest/1.0",
			"X-Foo":                    "keep-me",
			"x-bray-mode-degrade":      "true",
			"x-bray-multi-endpoint":    "true",
			"X-Bray-Sticky-Mode":       "true",
			"x-bray-endpoints":         "1.2.3.4:443",
			"  x-bray-sticky-endpoint": "false",
		},
	}
	h := c.GetRequestHeader()
	for k := range h {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(k)), "x-bray-") {
			t.Fatalf("control header leaked on wire: %q", k)
		}
	}
	if h.Get("User-Agent") != "BrayTest/1.0" {
		t.Fatalf("User-Agent missing/changed: %q", h.Get("User-Agent"))
	}
	if h.Get("X-Foo") != "keep-me" {
		t.Fatalf("X-Foo missing: %q", h.Get("X-Foo"))
	}
	// Payload helper must still strip control keys.
	hp := c.GetRequestHeaderWithPayload([]byte("x"))
	for k := range hp {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(k)), "x-bray-") {
			t.Fatalf("control header leaked via payload path: %q", k)
		}
	}
}
