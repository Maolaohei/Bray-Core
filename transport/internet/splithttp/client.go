package splithttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/signal/done"
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
	// pool of net.Conn, created using dialUploadConn
	uploadRawPool  *sync.Pool
	dialUploadConn func(ctxInner context.Context) (net.Conn, error)
	// onRTT is called after each request completes with the measured RTT.
	// Used for RTT-aware scheduling in XmuxManager.
	onRTT func(rtt time.Duration)
	// onNewConn is called when a new raw TCP connection is established.
	// Used by TransportProfile to start TCP_INFO sampling.
	// The conn argument is the raw TCP socket (before TLS/REALITY wrapping).
	onNewConn func(conn net.Conn)
	// onFatalError is called when a fatal connection error is detected.
	// Used by Fast Eviction to immediately remove dead clients from the pool.
	onFatalError func(err error)
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.closed.Load()
}

// SetOnRTT sets the callback for RTT measurement.
func (c *DefaultDialerClient) SetOnRTT(fn func(rtt time.Duration)) {
	c.onRTT = fn
}

// SetOnNewConn sets the callback for new raw TCP connections.
func (c *DefaultDialerClient) SetOnNewConn(fn func(conn net.Conn)) {
	c.onNewConn = fn
}

// SetOnFatalError sets the callback for fatal connection errors (Fast Eviction).
func (c *DefaultDialerClient) SetOnFatalError(fn func(err error)) {
	c.onFatalError = fn
}

// isFatalConnError checks if an error indicates the connection is dead and should be evicted.
func isFatalConnError(err error) bool {
	if err == nil {
		return false
	}
	// Check for connection-level fatal errors
	errStr := err.Error()
	return strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "use of closed connection") ||
		strings.Contains(errStr, "GOAWAY") ||
		strings.Contains(errStr, "RST_STREAM") ||
		strings.Contains(errStr, "transport closed") ||
		strings.Contains(errStr, "unexpected EOF") ||
		strings.Contains(errStr, "RemoteCertificateNameMismatch")
}

func (c *DefaultDialerClient) OpenStream(ctx context.Context, url string, sessionId string, body io.Reader, uploadOnly bool) (wrc io.ReadCloser, remoteAddr, localAddr net.Addr, err error) {
	t0 := time.Now()
	gotConn := done.New()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			remoteAddr = connInfo.Conn.RemoteAddr()
			localAddr = connInfo.Conn.LocalAddr()
			errors.LogDebug(ctx, "XHTTP stream: GotConn in ", time.Since(t0).Round(time.Millisecond),
				" (reused=", connInfo.Reused, ")")
			gotConn.Close()
		},
	})

	method := "GET" // stream-down
	if body != nil {
		method = c.transportConfig.GetNormalizedUplinkHTTPMethod() // stream-up/one
	}
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), method, url, body)
	if err != nil {
		errors.LogInfoInner(ctx, err, "failed to create HTTP request for "+url)
		return nil, nil, nil, err
	}
	c.transportConfig.FillStreamRequest(req, sessionId, "")

	wrc = &WaitReadCloser{wait: make(chan struct{})}
	go func() {
		resp, err := c.client.Do(req)
		if err != nil {
			// Fast Eviction: trigger callback on fatal connection errors
			if c.onFatalError != nil && isFatalConnError(err) {
				go c.onFatalError(err)
			}
			if !uploadOnly { // stream-down is enough
				c.closed.Store(true)
				errors.LogInfoInner(ctx, err, "failed to "+method+" "+url)
			}
			gotConn.Close()
			common.Close(body)
			wrc.Close()
			return
		}
		if resp.StatusCode != 200 && !uploadOnly {
			errors.LogInfo(ctx, "unexpected status ", resp.StatusCode)
		}
		if resp.StatusCode != 200 || uploadOnly { // stream-up
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			common.Close(body)
			wrc.Close()
			return
		}
		wrc.(*WaitReadCloser).Set(resp.Body)
	}()

	<-gotConn.Wait()
	return
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
			c.closed.Store(true)
			return err
		}

		// Record RTT for RTT-aware scheduling
		if c.onRTT != nil {
			c.onRTT(time.Since(start))
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

		var uploadConn any
		var h1UploadConn *H1Conn

		for {
			uploadConn = c.uploadRawPool.Get()
			newConnection := uploadConn == nil
			if newConnection {
				newConn, err := c.dialUploadConn(context.WithoutCancel(ctx))
				if err != nil {
					return err
				}
				h1UploadConn = NewH1Conn(newConn)
				uploadConn = h1UploadConn
			} else {
				h1UploadConn = uploadConn.(*H1Conn)

				// TODO: Replace 0 here with a config value later
				// Or add some other condition for optimization purposes
				if h1UploadConn.UnreadResponsesCount > 0 {
					resp, err := http.ReadResponse(h1UploadConn.RespBufReader, req)
					if err != nil {
						c.closed.Store(true)
						return fmt.Errorf("error while reading response: %s", err.Error())
					}
					io.Copy(io.Discard, resp.Body)
					defer resp.Body.Close()
					if resp.StatusCode != 200 {
						return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
					}
				}
			}

			_, err := h1UploadConn.Write(requestBuff.Bytes())
			// if the write failed, we try another connection from
			// the pool, until the write on a new connection fails.
			// failed writes to a pooled connection are normal when
			// the connection has been closed in the meantime.
			if err == nil {
				break
			} else if newConnection {
				return err
			}
		}

		c.uploadRawPool.Put(uploadConn)
	}

	return nil
}

// Close shuts down the underlying HTTP transport.
// For HTTP/2: sends GOAWAY on idle connections, waits for active streams to finish.
// For HTTP/1.1: closes idle connections.
// For HTTP/3: closes immediately (QUIC handles graceful shutdown internally).
// For Happy Eyeballs: closes both H3 and H2 transports.
func (c *DefaultDialerClient) Close() error {
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
	w.closed = true
	v := w.rc.Load()
	w.mu.Unlock()

	if v != nil {
		w.closeOnce.Do(func() {
			v.(io.ReadCloser).Close()
		})
	}
	return nil
}
