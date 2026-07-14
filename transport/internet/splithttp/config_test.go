package splithttp_test

import (
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
	if c.From != 8 || c.To != 16 {
		t.Fatalf("concurrency default = {%d,%d}", c.From, c.To)
	}
	n := m.GetNormalizedMaxConnections()
	if n.From != 2 || n.To != 4 {
		t.Fatalf("connections default = {%d,%d}", n.From, n.To)
	}
	r := m.GetNormalizedCMaxReuseTimes()
	if r.From != 64 || r.To != 128 {
		t.Fatalf("reuse default = {%d,%d}", r.From, r.To)
	}
	s := m.GetNormalizedHMaxReusableSecs()
	if s.From != 600 || s.To != 1200 {
		t.Fatalf("reusable secs default = {%d,%d}", s.From, s.To)
	}
	// Explicit zero-range still honored when set.
	m.MaxConcurrency = &RangeConfig{From: 0, To: 0}
	if m.GetNormalizedMaxConcurrency().From != 0 || m.GetNormalizedMaxConcurrency().To != 0 {
		t.Fatal("explicit zero concurrency must win")
	}
}
