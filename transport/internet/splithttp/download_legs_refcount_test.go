package splithttp

// B1 前置验证：多 GET 下载腿共享同一 session 时，session 必须活到最后一腿
// 结束（ref-count），而不是第一个完成的腿就把 session 拆掉（那样会杀掉
// 其它腿）。修复前行为：任一 GET 完成即 deleteSession。
//
// 流程：两个并发 GET（同 sessionId）→ 先完成的腿结束后 session 仍在
// （downloadLegs==1）→ 最后一腿结束后 session 删除。

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func refCountTestHandler(tb testing.TB) *requestHandler {
	tb.Helper()
	cfg := &Config{
		Path:                "/sh",
		SessionIDLength:     &RangeConfig{From: 16, To: 16},
		UplinkDataPlacement: PlacementBody,
		UplinkDataKey:       "x_data",
		XPaddingBytes: &RangeConfig{From: 16, To: 64},
		Mode:          "packet-up", // sessioned download requires packet-up/stream-up
		Headers: map[string]string{
			BraySessionSecretHeader: "testsecret",
		},
	}
	ln := &Listener{config: cfg, addConn: func(stat.Connection) {}}
	return &requestHandler{
		config:      cfg,
		host:        cfg.Host,
		path:        cfg.GetNormalizedPath(),
		ln:          ln,
		sessions:    sync.Map{},
		macVerifier: newSessionMacVerifier(cfg.sessionSecrets()),
		stopCh:      make(chan struct{}),
	}
}

// TestDownloadLegsRefcountMultiGET: 两个并发下载腿共享 session，第一腿结束后
// session 必须存活（downloadLegs 未归零），最后一腿结束才删除。
func TestDownloadLegsRefcountMultiGET(t *testing.T) {
	h := refCountTestHandler(t)

	sess := h.config.GenerateSessionID()

	startLeg := func() (func(), func(), *int) {
		ctx, cancel := context.WithCancel(context.Background())
		req := httptest.NewRequest("GET", "http://example.com"+h.path, nil)
		req = req.WithContext(ctx)
		req.RemoteAddr = "192.0.2.10:1234"
		// FillStreamRequest stamps Bray padding + session meta for us.
		h.config.FillStreamRequest(req, sess, "")
		// Debug: dump the headers + padding extract so we can see why 404.
		if v, _ := extractBrayDefaultXPadding(req); v == "" {
			t.Fatalf("padding not extractable from %v (produced headers: %v)", req.URL.RequestURI(), req.Header)
		}

		rec := httptest.NewRecorder()
		var wg sync.WaitGroup
		wg.Add(1)
		code := -1
		go func() {
			defer wg.Done()
			h.ServeHTTP(rec, req)
			code = rec.Code
		}()
		// cancel() ends this leg (GET download waits on request ctx).
		return func() { cancel(); wg.Wait() }, func() { cancel() }, &code
	}

	closeLeg1, _, code1 := startLeg()
	closeLeg2, _, code2 := startLeg()

	// Wait until both legs registered on the shared session.
	deadline := time.Now().Add(3 * time.Second)
	for {
		val, ok := h.sessions.Load(sess)
		if ok && val.(*httpSession).downloadLegs.Load() == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("legs did not both register: leg1 code=%d leg2 code=%d downloadLegs=%v",
				*code1, *code2, sessCount(h, sess))
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Close leg 1 first: session must survive (leg 2 still active).
	closeLeg1()
	val, ok := h.sessions.Load(sess)
	if !ok {
		t.Fatal("FAIL(regression of double-GET): session torn down after FIRST leg closed")
	}
	if n := val.(*httpSession).downloadLegs.Load(); n != 1 {
		t.Fatalf("expected downloadLegs==1 after one leg closed, got %d", n)
	}

	// Close last leg: session must be deleted.
	closeLeg2()
	if _, ok := h.sessions.Load(sess); ok {
		t.Fatal("session should be deleted after the LAST download leg closed")
	}
}

func sessCount(h *requestHandler, id string) int64 {
	val, ok := h.sessions.Load(id)
	if !ok {
		return -1
	}
	return val.(*httpSession).downloadLegs.Load()
}

var _ = atomic.Int64{} // keep import if refactored
var _ = net.LocalHostIP
