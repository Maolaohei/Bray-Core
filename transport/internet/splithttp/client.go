package splithttp

import (
	"bytes"
	"context"
	"crypto/tls"
	stderrors "errors"
	"fmt"
	"io"
	stdnet "net"
	"net/http"
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

var requestBuffPool = sync.Pool{
	New: func() any {
		b := new(bytes.Buffer)
		b.Grow(1024)
		return b
	},
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

	select {
	case <-ctx.Done():
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
		return nil, r, l, ctx.Err()
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

func (c *DefaultDialerClient) PostPacket(ctx context.Context, url string, sessionId string, seqStr string, payload buf.MultiBuffer) error {
	method := c.transportConfig.GetNormalizedUplinkHTTPMethod()
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), method, url, nil)
	if err != nil {
		return err
	}
	c.transportConfig.FillPacketRequest(req, sessionId, seqStr, payload)

	if c.httpVersion != "1.1" {
		start := time.Now()
		resp, err := c.client.Do(req)
		if err != nil {
			c.markFatal(err)
			return err
		}

		// Record RTT for RTT-aware scheduling
		if fn := c.getOnRTT(); fn != nil {
			fn(time.Since(start))
		}

		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 {
			return errors.New("bad status code:", resp.Status)
		}
	} else {
		// stringify the entire HTTP/1.1 request so it can be
		// safely retried. if instead req.Write is called multiple
		// times, the body is already drained after the first
		// request
		requestBuff := requestBuffPool.Get().(*bytes.Buffer)
		requestBuff.Reset()
		common.Must(req.Write(requestBuff))
		defer func() {
			if requestBuff.Cap() <= maxBufferPoolCap {
				requestBuff.Reset()
				requestBuffPool.Put(requestBuff)
			}
		}()

		var h1UploadConn *H1Conn
		start := time.Now()

		for {
			h1UploadConn = c.uploadRawPool.Get()
			newConnection := h1UploadConn == nil
			if newConnection {
				newConn, err := c.dialUploadConn(context.WithoutCancel(ctx))
				if err != nil {
					return err
				}
				h1UploadConn = NewH1Conn(newConn)
			} else {

				// Drain any leftover unread responses before reuse (legacy pipeline depth).
				drainFailed := false
				for h1UploadConn.UnreadResponsesCount > 0 {
					resp, err := http.ReadResponse(h1UploadConn.RespBufReader, nil)
					if err != nil {
						_ = h1UploadConn.Close()
						c.markFatal(err)
						drainFailed = true
						break
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					h1UploadConn.UnreadResponsesCount--
					if resp.StatusCode != 200 {
						_ = h1UploadConn.Close()
						return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
					}
				}
				if drainFailed {
					// Drop dead pooled conn; try another (or dial fresh).
					continue
				}
			}

			_, err := h1UploadConn.Write(requestBuff.Bytes())
			// if the write failed, we try another connection from
			// the pool, until the write on a new connection fails.
			// failed writes to a pooled connection are normal when
			// the connection has been closed in the meantime.
			if err == nil {
				// Confirm this upload response immediately so failures are not
				// deferred until a later reuse that may never happen. Pipeline
				// depth stays at most 1 for packet-up reliability.
				resp, readErr := http.ReadResponse(h1UploadConn.RespBufReader, nil)
				if readErr != nil {
					_ = h1UploadConn.Close()
					if newConnection {
						c.markFatal(readErr)
						return readErr
					}
					c.markFatal(readErr)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					_ = h1UploadConn.Close()
					return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
				}
				if fn := c.getOnRTT(); fn != nil {
					fn(time.Since(start))
				}
				break
			} else if newConnection {
				return err
			} else {
				// Do not return a broken pooled connection; close and retry.
				_ = h1UploadConn.Close()
				c.markFatal(err)
			}
		}

		c.uploadRawPool.Put(h1UploadConn)
	}

	return nil
}

// Close shuts down the underlying HTTP transport.
// For HTTP/2: force-closes tracked raw sockets (active streams included), then
// clears idle pool entries. CloseIdleConnections alone leaves busy H2 conns alive.
// For HTTP/1.1: closes idle connections after tracked sockets.
// For HTTP/3: closes immediately (QUIC handles graceful shutdown internally).
// For Happy Eyeballs: closes both H3 and H2 transports.
func (c *DefaultDialerClient) Close() error {
	c.closed.Store(true)
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
