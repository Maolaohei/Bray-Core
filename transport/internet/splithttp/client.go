package splithttp

import (
	"bytes"
	"context"
	"crypto/tls"
	stderrors "errors"
	"io"
	stdnet "net"
	"net/http"
	"net/url"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"golang.org/x/net/http2"
)

const maxBufferPoolCap = 64 * 1024

// defaultOpenStreamHeaderTimeout bounds how long OpenStream waits for response
// headers when the caller's ctx has no deadline. Without this, a blackholed H2
// stream (common under overloaded AI/SSE upstreams) blocks Dial forever and
// starves the whole SOCKS accept path. The long-lived stream still uses
// WithoutCancel after headers succeed.
const defaultOpenStreamHeaderTimeout = 20 * time.Second

var requestBuffPool = sync.Pool{
	New: func() any {
		b := new(bytes.Buffer)
		b.Grow(1024)
		return b
	},
}

// urlURLPool reuses request-local *url.URL shells for PostPacket path mutation.
// Base host/scheme fields are copied from the dialer's immutable packet URL.
var urlURLPool = sync.Pool{
	New: func() any {
		return new(url.URL)
	},
}

// reqBytesLocal holds H1 pipeline request wire snapshots for pooling.
// Store *reqBytesLocal so Put never captures a stack-local slice header.
type reqBytesLocal struct {
	b []byte
}

var reqBytesPool = sync.Pool{
	New: func() any {
		return &reqBytesLocal{b: make([]byte, 0, 2048)}
	},
}

func acquireReqBytes(src []byte) *reqBytesLocal {
	dl := reqBytesPool.Get().(*reqBytesLocal)
	if cap(dl.b) < len(src) {
		dl.b = make([]byte, len(src))
	} else {
		dl.b = dl.b[:len(src)]
	}
	copy(dl.b, src)
	return dl
}

func releaseReqBytes(dl *reqBytesLocal) {
	if dl == nil {
		return
	}
	if cap(dl.b) > maxBufferPoolCap {
		return
	}
	dl.b = dl.b[:0]
	reqBytesPool.Put(dl)
}

// interface to abstract between use of browser dialer, vs net/http
type DialerClient interface {
	IsClosed() bool

	// ctx, url, sessionId, body, uploadOnly
	OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error)

	// ctx, url, sessionId, seqStr, body, contentLength
	PostPacket(context.Context, string, string, string, buf.MultiBuffer) error
}

// implements splithttp.DialerClient in terms of direct network connections
type DefaultDialerClient struct {
	transportConfig *Config
	client          *http.Client
	closed          atomic.Bool
	httpVersion     string
	// pool of H1 upload conns, created using dialUploadConn (bounded)
	uploadRawPool  *h1ConnPool
	// hotH1 is the currently shared pipelined H1 upload conn (depth > 1).
	// Protected by hotH1Mu; activeUsers on the H1Conn tracks concurrent posts.
	hotH1Mu        sync.Mutex
	hotH1          *H1Conn
	// h1Dialing serializes concurrent first-dial so N parallel PostPacket
	// callers share one socket instead of N redundant dials.
	h1Dialing      bool
	hotH1Wait      *sync.Cond
	// packetURLBase caches the last PostPacket base URL for this dialer
	// (packet-up posts the same host/path every seq). Immutable after parse.
	packetURLRaw  atomic.Value // string
	packetURLBase atomic.Value // *url.URL
	dialUploadConn func(ctxInner context.Context) (net.Conn, error)
	// Callbacks are stored atomically because getHTTPClient may re-wire them
	// from concurrent Dial paths while OpenStream/PostPacket read them.
	// Values are function objects; nil means unset.
	onRTT        atomic.Value // func(time.Duration)
	onTTFB       atomic.Value // func(time.Duration)
	onNewConn    atomic.Value // func(net.Conn)
	onFatalError atomic.Value // func(error)

	// liveConns tracks raw TCP/QUIC sockets created for this dialer so MarkDead/Close
	// can force-close active H2 connections (CloseIdleConnections alone is not enough).
	liveMu    sync.Mutex
	liveConns map[stdnet.Conn]struct{}
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.closed.Load()
}

// SetOnRTT sets the callback for RTT measurement.
func (c *DefaultDialerClient) SetOnRTT(fn func(rtt time.Duration)) {
	if fn == nil {
		c.onRTT.Store((func(time.Duration))(nil))
		return
	}
	c.onRTT.Store(fn)
}

// SetOnTTFB sets the callback for Time-To-First-Byte measurement on stream open.
func (c *DefaultDialerClient) SetOnTTFB(fn func(ttfb time.Duration)) {
	if fn == nil {
		c.onTTFB.Store((func(time.Duration))(nil))
		return
	}
	c.onTTFB.Store(fn)
}

// SetOnNewConn sets the callback for new raw TCP connections.
func (c *DefaultDialerClient) SetOnNewConn(fn func(conn net.Conn)) {
	if fn == nil {
		c.onNewConn.Store((func(net.Conn))(nil))
		return
	}
	c.onNewConn.Store(fn)
}

// SetOnFatalError sets the callback for fatal connection errors (Fast Eviction).
func (c *DefaultDialerClient) SetOnFatalError(fn func(err error)) {
	if fn == nil {
		c.onFatalError.Store((func(error))(nil))
		return
	}
	c.onFatalError.Store(fn)
}

func (c *DefaultDialerClient) getOnRTT() func(time.Duration) {
	v := c.onRTT.Load()
	if v == nil {
		return nil
	}
	return v.(func(time.Duration))
}

func (c *DefaultDialerClient) getOnTTFB() func(time.Duration) {
	v := c.onTTFB.Load()
	if v == nil {
		return nil
	}
	return v.(func(time.Duration))
}

func (c *DefaultDialerClient) getOnNewConn() func(net.Conn) {
	v := c.onNewConn.Load()
	if v == nil {
		return nil
	}
	return v.(func(net.Conn))
}

func (c *DefaultDialerClient) getOnFatalError() func(error) {
	v := c.onFatalError.Load()
	if v == nil {
		return nil
	}
	return v.(func(error))
}

// isFatalConnError checks if an error indicates the H2/H3/H1 transport should be
// evicted from the XMUX pool. Only connection-level faults qualify.
//
// Bare io.EOF / UnexpectedEOF are intentionally NOT fatal: on multiplexed H2 a
// single stream can end with EOF while sibling streams are still healthy.
// Treating EOF as fatal used to MarkDead → forceCloseLiveConns and mid-transfer
// "断流" on concurrent stream-one/packet-up downloads.
func isFatalConnError(err error) bool {
	if err == nil {
		return false
	}
	// Soft stream-end signals: fail this request only, keep the pooled transport.
	if stderrors.Is(err, io.EOF) || stderrors.Is(err, io.ErrUnexpectedEOF) {
		return false
	}

	// Fast path: hard TCP/socket death (branch-predictor friendly)
	if stderrors.Is(err, stdnet.ErrClosed) ||
		stderrors.Is(err, syscall.EPIPE) ||
		stderrors.Is(err, syscall.ECONNRESET) {
		return true
	}

	// Slow path: TLS, HTTP/2, and third-party connection-level errors
	return isFatalConnErrorSlow(err)
}

// isFatalConnErrorSlow handles less common errors that still indicate connection death.
func isFatalConnErrorSlow(err error) bool {
	// TLS record-level error
	var tlsErr tls.RecordHeaderError
	if stderrors.As(err, &tlsErr) {
		return true
	}

	// String-based matching for TLS/x509/HTTP2 connection-level faults only.
	// Avoid stream-scoped phrases (REFUSED_STREAM, cancel, timeout) which must
	// not force-close the shared raw socket under other active streams.
	s := err.Error()
	return strings.Contains(s, "tls:") ||
		strings.Contains(s, "x509:") ||
		strings.Contains(s, "cipher suite") ||
		strings.Contains(s, "SSL_VERSION_OR_CIPHER_MISMATCH") ||
		strings.Contains(s, "RemoteCertificateNameMismatch") ||
		strings.Contains(s, "GOAWAY") ||
		strings.Contains(s, "connection shutdown") ||
		strings.Contains(s, "transport closed") ||
		strings.Contains(s, "server sent disconnect") ||
		strings.Contains(s, "client connection force closed") ||
		strings.Contains(s, "http2: Transport closing idle connection") ||
		strings.Contains(s, "http2: client connection force closed") ||
		strings.Contains(s, "http2: client connection lost")
}

func (c *DefaultDialerClient) markFatal(err error) {
	if !isFatalConnError(err) {
		return
	}
	c.closed.Store(true)
	if fn := c.getOnFatalError(); fn != nil {
		go fn(err)
	}
}

// trackConn records a live raw socket and returns a wrapper that untracks on Close.
// Used so MarkDead can force-close active H2 transports, not only idle ones.
func (c *DefaultDialerClient) trackConn(conn stdnet.Conn) stdnet.Conn {
	if conn == nil {
		return nil
	}
	tc := &trackedConn{Conn: conn}
	tc.onClose = func() {
		c.liveMu.Lock()
		delete(c.liveConns, tc)
		c.liveMu.Unlock()
	}
	c.liveMu.Lock()
	if c.liveConns == nil {
		c.liveConns = make(map[stdnet.Conn]struct{})
	}
	c.liveConns[tc] = struct{}{}
	c.liveMu.Unlock()
	return tc
}

// forceCloseLiveConns closes every tracked raw socket. Safe under concurrent dial.
func (c *DefaultDialerClient) forceCloseLiveConns() {
	c.liveMu.Lock()
	conns := make([]stdnet.Conn, 0, len(c.liveConns))
	for conn := range c.liveConns {
		conns = append(conns, conn)
	}
	c.liveConns = nil
	c.liveMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

type trackedConn struct {
	stdnet.Conn
	once    sync.Once
	onClose func()
}

func (t *trackedConn) Close() error {
	err := t.Conn.Close()
	t.once.Do(func() {
		if t.onClose != nil {
			t.onClose()
		}
	})
	return err
}

func (c *DefaultDialerClient) OpenStream(ctx context.Context, url string, sessionId string, body io.Reader, uploadOnly bool) (wrc io.ReadCloser, remoteAddr, localAddr net.Addr, err error) {
	t0 := time.Now()
	var addrMu sync.Mutex
	var gotRemote, gotLocal net.Addr
	var ttfbOnce sync.Once
	method := "GET" // stream-down
	if body != nil {
		method = c.transportConfig.GetNormalizedUplinkHTTPMethod() // stream-up/one
	}

	// Wait for response headers (not merely GotConn). Returning on GotConn alone
	// produced "dial succeeded then immediately EOF" when Do later failed or
	// returned a non-200 status.
	type streamResult struct {
		rc     io.ReadCloser
		remote net.Addr
		local  net.Addr
		err    error
	}
	resultCh := make(chan streamResult, 1)

	// I1: open is cancelable without tying the long-lived stream to Dial's ctx.
	// WithoutCancel keeps stream-one/stream-up alive after Dial returns; the child
	// cancel lets us abort in-flight Do/dial when the parent cancels during open.
	reqCtx, reqCancel := context.WithCancel(context.WithoutCancel(ctx))
	reqCtx = httptrace.WithClientTrace(reqCtx, &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			addrMu.Lock()
			gotRemote = connInfo.Conn.RemoteAddr()
			gotLocal = connInfo.Conn.LocalAddr()
			addrMu.Unlock()
			errors.LogDebug(ctx, "XHTTP stream: GotConn in ", time.Since(t0).Round(time.Millisecond),
				" (reused=", connInfo.Reused, ")")
		},
		GotFirstResponseByte: func() {
			// Prefer true first-byte latency when the transport fires the hook.
			ttfbOnce.Do(func() {
				if fn := c.getOnTTFB(); fn != nil {
					fn(time.Since(t0))
				}
			})
		},
	})

	req, err := http.NewRequestWithContext(reqCtx, method, url, body)
	if err != nil {
		reqCancel()
		errors.LogInfoInner(ctx, err, "failed to create HTTP request for "+url)
		return nil, nil, nil, err
	}
	c.transportConfig.FillStreamRequest(req, sessionId, "")

	go func() {
		resp, doErr := c.client.Do(req)
		if doErr != nil {
			// Connection-level faults evict the dialer for both download and upload-only.
			// Context cancel / deadline must not kill a healthy pooled client.
			if isFatalConnError(doErr) {
				c.markFatal(doErr)
			}
			errors.LogInfoInner(ctx, doErr, "failed to "+method+" "+url)
			common.Close(body)
			addrMu.Lock()
			r, l := gotRemote, gotLocal
			addrMu.Unlock()
			resultCh <- streamResult{remote: r, local: l, err: doErr}
			return
		}
		if resp.StatusCode != 200 {
			errors.LogInfo(ctx, "unexpected status ", resp.StatusCode)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			// Non-200: abort request body so the peer is not left half-open.
			common.Close(body)
			addrMu.Lock()
			r, l := gotRemote, gotLocal
			addrMu.Unlock()
			resultCh <- streamResult{
				remote: r,
				local:  l,
				err:    errors.New("unexpected status ", resp.StatusCode),
			}
			return
		}
		if uploadOnly {
			// stream-up: headers accepted; keep request body open for the pipe writer.
			// Drain the (usually empty/idle) response in the background.
			// Do NOT reqCancel here — cancel would abort the still-open request body.
			go func() {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
			addrMu.Lock()
			r, l := gotRemote, gotLocal
			addrMu.Unlock()
			// Fallback when httptrace.GotFirstResponseByte is unavailable
			// (custom RoundTripper / some H3 paths): headers accepted == TTFB.
			ttfbOnce.Do(func() {
				if fn := c.getOnTTFB(); fn != nil {
					fn(time.Since(t0))
				}
			})
			resultCh <- streamResult{remote: r, local: l}
			return
		}
		rc := &WaitReadCloser{wait: make(chan struct{})}
		// Pure download: cancel reqCtx when body closes so open-phase resources free.
		// stream-one (body != nil) must keep reqCtx alive for the bi-directional stream.
		if body == nil {
			rc.Set(&cancelOnClose{ReadCloser: resp.Body, cancel: reqCancel})
		} else {
			rc.Set(resp.Body)
		}
		addrMu.Lock()
		rr, ll := gotRemote, gotLocal
		addrMu.Unlock()
		ttfbOnce.Do(func() {
			if fn := c.getOnTTFB(); fn != nil {
				fn(time.Since(t0))
			}
		})
		resultCh <- streamResult{rc: rc, remote: rr, local: ll}
	}()

	// Bound header wait: if caller already set a deadline, honor it; otherwise
	// apply Bray default so blackholed H2 streams cannot pin Dial forever.
	// Always derive from ctx so cancel still aborts the wait.
	openWaitCtx := ctx
	var openWaitCancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		openWaitCtx, openWaitCancel = context.WithTimeout(ctx, defaultOpenStreamHeaderTimeout)
		defer openWaitCancel()
	}

	select {
	case <-openWaitCtx.Done():
		// Abort the in-flight round-trip (RST_STREAM / dial cancel). Do not leave
		// orphan Do/dial work holding H2 streams or TCP sockets after caller returns.
		reqCancel()
		go func() {
			res := <-resultCh
			if res.rc != nil {
				res.rc.Close()
			}
		}()
		addrMu.Lock()
		r, l := gotRemote, gotLocal
		addrMu.Unlock()
		errOut := openWaitCtx.Err()
		if errOut == nil {
			errOut = context.DeadlineExceeded
		}
		// Prefer caller's cancel reason when both fire.
		if ctx.Err() != nil {
			errOut = ctx.Err()
		}
		return nil, r, l, errOut
	case res := <-resultCh:
		if res.err != nil {
			reqCancel()
			return nil, res.remote, res.local, res.err
		}
		// Success: keep reqCtx alive for stream-one/stream-up. Pure download
		// cancels via cancelOnClose when the response body is closed.
		return res.rc, res.remote, res.local, nil
	}
}

// cancelOnClose cancels a request context when the response body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.once.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
	})
	return err
}

// resolvePacketURL returns a request-local *url.URL for PostPacket.
// The dialer reuses one immutable base for the common single-destination case;
// Path/Query mutations by FillPacketRequest only touch the returned copy.
func (c *DefaultDialerClient) resolvePacketURL(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, errors.New("empty packet-up URL")
	}
	if v := c.packetURLRaw.Load(); v != nil {
		if v.(string) == raw {
			if base := c.packetURLBase.Load(); base != nil {
				return cloneURL(base.(*url.URL)), nil
			}
		}
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	// Store immutable base; concurrent stores are fine (same raw wins).
	c.packetURLBase.Store(parsed)
	c.packetURLRaw.Store(raw)
	return cloneURL(parsed), nil
}

func cloneURL(base *url.URL) *url.URL {
	u := urlURLPool.Get().(*url.URL)
	*u = *base
	// Userinfo is a pointer; keep sharing the immutable base value.
	return u
}

func releaseURL(u *url.URL) {
	if u == nil {
		return
	}
	*u = url.URL{}
	urlURLPool.Put(u)
}

// newPacketRequest builds a POST/PUT packet-up request without re-parsing the
// base URL string on every seq. Context is attached once via NewRequestWithContext
// (avoids Request.WithContext's second heap clone of the whole Request).
func (c *DefaultDialerClient) newPacketRequest(ctx context.Context, method, rawURL string) (*http.Request, error) {
	u, err := c.resolvePacketURL(rawURL)
	if err != nil {
		return nil, err
	}
	// Build the request shell manually: no dummy URL parse, no NewRequest
	// method validation. Context is attached via one WithContext clone
	// (Request.ctx is unexported). Header/Body filled by FillPacketRequest.
	req := &http.Request{
		Method:     method,
		URL:        u,
		Host:       u.Host,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	return req.WithContext(context.WithoutCancel(ctx)), nil
}

func (c *DefaultDialerClient) PostPacket(ctx context.Context, url string, sessionId string, seqStr string, payload buf.MultiBuffer) error {
	return c.postPacket(ctx, url, sessionId, seqStr, func(req *http.Request) error {
		return c.transportConfig.FillPacketRequest(req, sessionId, seqStr, payload)
	})
}

// PostPacketBytes posts a durable []byte snapshot without MultiBuffer wrappers.
// Used by postPacketReliable on the packet-up hot path.
func (c *DefaultDialerClient) PostPacketBytes(ctx context.Context, url string, sessionId string, seqStr string, data []byte) error {
	return c.postPacket(ctx, url, sessionId, seqStr, func(req *http.Request) error {
		return c.transportConfig.FillPacketRequestBytes(req, sessionId, seqStr, data)
	})
}

func (c *DefaultDialerClient) postPacket(ctx context.Context, rawURL string, sessionId string, seqStr string, fill func(*http.Request) error) error {
	method := c.transportConfig.GetNormalizedUplinkHTTPMethod()
	req, err := c.newPacketRequest(ctx, method, rawURL)
	if err != nil {
		return err
	}
	// Fill may mutate req.URL.Path; recycle URL shell after the round-trip.
	defer releaseURL(req.URL)

	if err := fill(req); err != nil {
		if req.Header != nil {
			releaseHeaderMap(req.Header)
			req.Header = nil
		}
		if req.Body != nil {
			_ = req.Body.Close()
			req.Body = nil
		}
		return err
	}

	if c.httpVersion != "1.1" {
		rttFn := c.getOnRTT()
		var start time.Time
		if rttFn != nil {
			start = time.Now()
		}
		resp, err := c.client.Do(req)
		// Header map is request-local; recycle after transport finished with req.
		releaseHeaderMap(req.Header)
		req.Header = nil
		if err != nil {
			c.markFatal(err)
			return err
		}

		// Record RTT for RTT-aware scheduling (skip clock when unused).
		if rttFn != nil {
			rttFn(time.Since(start))
		}

		// Packet-up responses are empty (200 + CL 0); avoid generic Copy overhead.
		if resp.ContentLength != 0 {
			io.Copy(io.Discard, resp.Body)
		}
		resp.Body.Close()

		if resp.StatusCode != 200 {
			return errors.New("bad status code:", resp.Status)
		}
		return nil
	}

	// stringify the entire HTTP/1.1 request so it can be
	// safely retried. if instead req.Write is called multiple
	// times, the body is already drained after the first
	// request
	requestBuff := requestBuffPool.Get().(*bytes.Buffer)
	requestBuff.Reset()
	common.Must(req.Write(requestBuff))
	releaseHeaderMap(req.Header)
	req.Header = nil
	if req.Body != nil {
		_ = req.Body.Close()
		req.Body = nil
	}
	reqHold := acquireReqBytes(requestBuff.Bytes())
	reqBytes := reqHold.b
	if requestBuff.Cap() <= maxBufferPoolCap {
		requestBuff.Reset()
		requestBuffPool.Put(requestBuff)
	}
	defer releaseReqBytes(reqHold)

	start := time.Now()
	// Acquire a shared hot conn (pipeline depth > 1) or dial fresh.
	h1UploadConn, newConnection, err := c.acquireH1UploadConn(ctx)
	if err != nil {
		return err
	}
	postErr := h1UploadConn.pipelinePost(reqBytes)
	c.releaseH1UploadConn(h1UploadConn, newConnection, postErr)
	if postErr != nil {
		if newConnection || h1UploadConn.isDead() {
			c.markFatal(postErr)
		}
		// Pooled/shared write failure: try once more on a fresh dial so
		// transient closed-idle sockets do not fail the packet.
		if !newConnection {
			h2, _, err2 := c.acquireH1UploadConn(ctx)
			if err2 != nil {
				return err2
			}
			// Force new path: do not reuse the dead hot.
			postErr = h2.pipelinePost(reqBytes)
			c.releaseH1UploadConn(h2, true, postErr)
			if postErr != nil {
				c.markFatal(postErr)
				return postErr
			}
		} else {
			return postErr
		}
	}
	if fn := c.getOnRTT(); fn != nil {
		fn(time.Since(start))
	}
	return nil
}


// acquireH1UploadConn returns a shared pipelined H1 conn (hot) or dials a new one.
// Caller must releaseH1UploadConn when done.
// Concurrent first-dials wait on h1Dialing so parallel PostPacket shares one socket.
func (c *DefaultDialerClient) acquireH1UploadConn(ctx context.Context) (*H1Conn, bool, error) {
	c.hotH1Mu.Lock()
	if c.hotH1Wait == nil {
		c.hotH1Wait = sync.NewCond(&c.hotH1Mu)
	}
	for {
		// Prefer the shared hot conn so concurrent PostPacket can pipeline.
		if c.hotH1 != nil {
			h := c.hotH1
			if h.tryAcquireShared() {
				c.hotH1Mu.Unlock()
				return h, false, nil
			}
			// Dead hot: drop it.
			c.hotH1 = nil
		}
		if c.h1Dialing {
			c.hotH1Wait.Wait()
			continue
		}
		// Try idle pool first (may have leftover pipeline state from older path).
		if c.uploadRawPool != nil {
			if h := c.uploadRawPool.Get(); h != nil {
				if h.tryAcquireShared() {
					if c.hotH1 == nil {
						c.hotH1 = h
					}
					c.hotH1Mu.Unlock()
					return h, false, nil
				}
				_ = h.Close()
			}
		}
		// This caller becomes the single dialer; peers wait on h1Dialing.
		c.h1Dialing = true
		c.hotH1Mu.Unlock()

		newConn, err := c.dialUploadConn(context.WithoutCancel(ctx))
		c.hotH1Mu.Lock()
		c.h1Dialing = false
		if err != nil {
			c.hotH1Wait.Broadcast()
			c.hotH1Mu.Unlock()
			return nil, true, err
		}
		h := NewH1Conn(newConn)
		if !h.tryAcquireShared() {
			_ = h.Close()
			c.hotH1Wait.Broadcast()
			c.hotH1Mu.Unlock()
			return nil, true, stdnet.ErrClosed
		}
		// Prefer the socket we just dialed as the shared hot conn.
		if c.hotH1 == nil {
			c.hotH1 = h
			c.hotH1Wait.Broadcast()
			c.hotH1Mu.Unlock()
			return h, true, nil
		}
		// Unexpected: a hot appeared without going through h1Dialing.
		// Keep existing hot for sharing; drop our unused dial (we already
		// hold one activeUsers on h — close instead of pooling dirty).
		_ = h.Close()
		hot := c.hotH1
		if !hot.tryAcquireShared() {
			c.hotH1 = nil
			c.hotH1Wait.Broadcast()
			c.hotH1Mu.Unlock()
			return nil, true, stdnet.ErrClosed
		}
		c.hotH1Wait.Broadcast()
		c.hotH1Mu.Unlock()
		return hot, false, nil
	}
}

// releaseH1UploadConn drops a shared user and returns the conn to the idle pool
// when no users remain. On error the conn is closed and cleared from hot.
func (c *DefaultDialerClient) releaseH1UploadConn(h *H1Conn, newConnection bool, postErr error) {
	if h == nil {
		return
	}
	if postErr != nil || h.isDead() {
		c.hotH1Mu.Lock()
		if c.hotH1 == h {
			c.hotH1 = nil
		}
		c.hotH1Mu.Unlock()
		_ = h.Close()
		return
	}
	if h.releaseShared() {
		c.hotH1Mu.Lock()
		// Keep healthy hot for reuse; also park a spare in the idle pool.
		if c.hotH1 == h {
			// stay hot with 0 users; next acquire reuses without pool hop
			c.hotH1Mu.Unlock()
			return
		}
		c.hotH1Mu.Unlock()
		c.uploadRawPool.Put(h)
	}
}
// Close shuts down the underlying HTTP transport.
// For HTTP/2: force-closes tracked raw sockets (active streams included), then
// clears idle pool entries. CloseIdleConnections alone leaves busy H2 conns alive.
// For HTTP/1.1: closes idle connections after tracked sockets.
// For HTTP/3: closes immediately (QUIC handles graceful shutdown internally).
// For Happy Eyeballs: closes both H3 and H2 transports.
func (c *DefaultDialerClient) clearHotH1() {
	c.hotH1Mu.Lock()
	h := c.hotH1
	c.hotH1 = nil
	c.h1Dialing = false
	if c.hotH1Wait != nil {
		c.hotH1Wait.Broadcast()
	}
	c.hotH1Mu.Unlock()
	if h != nil {
		_ = h.Close()
	}
}

func (c *DefaultDialerClient) Close() error {
	c.closed.Store(true)
	c.clearHotH1()
	// Drain idle H1 upload pool so sockets do not linger after dialer close.
	if c.uploadRawPool != nil {
		for {
			h := c.uploadRawPool.Get()
			if h == nil {
				break
			}
			_ = h.Close()
		}
	}
	// Tear down live sockets first so blocked Read/Write on dead H2 streams unblock.
	c.forceCloseLiveConns()
	if c.client == nil || c.client.Transport == nil {
		return nil
	}
	transport := c.client.Transport
	switch t := transport.(type) {
	case *happyEyeballsTransport:
		t.Close()
	case *http3.Transport:
		t.Close()
	case *http2.Transport:
		t.CloseIdleConnections()
	case *http.Transport:
		t.CloseIdleConnections()
	}
	return nil
}

type WaitReadCloser struct {
	wait      chan struct{}
	rc        atomic.Value // stores io.ReadCloser
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

func (w *WaitReadCloser) Set(rc io.ReadCloser) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		rc.Close()
		return
	}
	w.rc.Store(rc)
	close(w.wait)
	w.mu.Unlock()
}

func (w *WaitReadCloser) Read(p []byte) (int, error) {
	if v := w.rc.Load(); v != nil {
		return v.(io.ReadCloser).Read(p)
	}
	select {
	case <-w.wait:
		if v := w.rc.Load(); v != nil {
			return v.(io.ReadCloser).Read(p)
		}
		return 0, io.ErrClosedPipe
	}
}

func (w *WaitReadCloser) Close() error {
	w.mu.Lock()
	alreadyClosed := w.closed
	w.closed = true
	v := w.rc.Load()
	// Unblock any reader waiting on Set() when close races with dial failure.
	if !alreadyClosed && v == nil {
		select {
		case <-w.wait:
		default:
			close(w.wait)
		}
	}
	w.mu.Unlock()

	if v != nil {
		w.closeOnce.Do(func() {
			v.(io.ReadCloser).Close()
		})
	}
	return nil
}

