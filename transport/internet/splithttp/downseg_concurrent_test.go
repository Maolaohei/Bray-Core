package splithttp

// P1 隔离层验证：并发 PullSegment（不经 worker）——确认"并发 HTTP 段拉取"
// 本身是否可靠；若此层卡住则问题在服务端/HTTP 栈，若通过则问题在 worker 逻辑。

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

func TestConcurrentPullSegmentIsolation(t *testing.T) {
	withZeroDownsegJitter(t)
	h, base, client := newEndToEndServer(t)
	sid := h.config.GenerateSessionID()

	// 预生产 3 段 + finalize（先建 session：用一个同步 PullSegment 触发 upsert）
	if _, err := client.PullSegment(context.Background(), base, sid, "0"); err == nil {
		t.Fatal("expected 404/empty before production")
	} else if !isNotFoundOrEmpty(err) {
		t.Logf("pre-probe status: %v", err)
	}
	// session 已 upsert（真实 IP），生产写入 + finalize
	v, ok := h.sessions.Load(sid)
	if !ok {
		t.Fatal("session not created by pre-probe")
	}
	sess := v.(*httpSession)
	if !sess.enterDownsegMode() {
		t.Fatal("enterDownsegMode")
	}
	prod := &httpServerConn{Instance: done.New(), sess: sess}
	payload := bytes.Repeat([]byte{0x33}, downsegSize*3)
	if _, err := prod.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = prod.Close()

	// 并发拉 seq 0..5（0..2 有数据，3..5 空200/EOF）
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	results := make([]struct {
		seg []byte
		err error
	}, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			results[seq].seg, results[seq].err = client.PullSegment(ctx, base, sid, strconv.Itoa(seq))
		}(i)
	}
	wg.Wait()

	total := 0
	for i := 0; i < 6; i++ {
		if i < 3 {
			if results[i].err != nil || len(results[i].seg) != downsegSize {
				t.Fatalf("seq%d: err=%v len=%d want 256KiB", i, results[i].err, len(results[i].seg))
			}
			total += len(results[i].seg)
		} else {
			// EOF: 200 empty body
			if results[i].err != nil {
				t.Fatalf("seq%d err=%v (want empty-200 EOF)", i, results[i].err)
			}
			if len(results[i].seg) != 0 {
				t.Fatalf("seq%d len=%d want empty (EOF)", i, len(results[i].seg))
			}
		}
	}
	if total != downsegSize*3 {
		t.Fatalf("sum=%d want %d", total, downsegSize*3)
	}
}

func isNotFoundOrEmpty(err error) bool {
	return err == errSegNotFound || err == errSegGone
}

var _ = io.EOF
