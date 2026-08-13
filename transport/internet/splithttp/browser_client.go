package splithttp

import (
	"context"
	"io"
	"net/http"
	"net/url"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/browser_dialer"
	"github.com/xtls/xray-core/transport/internet/websocket"
)

// BrowserDialerClient implements splithttp.DialerClient in terms of browser dialer
type BrowserDialerClient struct {
	transportConfig *Config
}

func (c *BrowserDialerClient) IsClosed() bool {
	return false
}

// OpenStreamAsync: browser dialer streams open synchronously; wrap the
// result in an immediately-resolved future reader for interface parity.
func (c *BrowserDialerClient) OpenStreamAsync(ctx context.Context, base *url.URL, sessionId string, body io.Reader, uploadOnly bool, onReady func(remote, local net.Addr)) (io.ReadCloser, error) {
	rc, remote, local, err := c.OpenStream(ctx, base, sessionId, body, uploadOnly)
	if err != nil {
		return nil, err
	}
	if onReady != nil {
		onReady(remote, local)
	}
	return rc, nil
}

func (c *BrowserDialerClient) OpenStream(ctx context.Context, base *url.URL, sessionId string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	if body != nil {
		return nil, nil, nil, errors.New("bidirectional streaming for browser dialer not implemented yet")
	}
	if base == nil {
		return nil, nil, nil, errors.New("OpenStream: nil base URL")
	}

	// Request-local URL copy: FillStreamRequest may append session path segments.
	// Manual shell (no NewRequest parse): same pattern as DefaultDialerClient.
	u := *base
	request := &http.Request{
		Method: "GET",
		URL:    &u,
		Host:   u.Host,
	}

	c.transportConfig.FillStreamRequest(request, sessionId, "")

	conn, err := browser_dialer.DialGet(request.URL.String(), request.Header, request.Cookies())
	dummyAddr := &net.IPAddr{}
	if err != nil {
		return nil, dummyAddr, dummyAddr, err
	}

	return websocket.NewConnection(conn, dummyAddr, nil, 0), conn.RemoteAddr(), conn.LocalAddr(), nil
}

func (c *BrowserDialerClient) PostPacket(ctx context.Context, url string, sessionId string, seqStr string, payload buf.MultiBuffer) error {
	method := c.transportConfig.GetNormalizedUplinkHTTPMethod()
	request, err := http.NewRequest(method, url, nil)
	if err != nil {
		return err
	}

	err = c.transportConfig.FillPacketRequest(request, sessionId, seqStr, payload)
	if err != nil {
		return err
	}

	var bytes []byte
	if request.Body != nil {
		bytes, err = io.ReadAll(request.Body)
		if err != nil {
			return err
		}
	}

	err = browser_dialer.DialPacket(method, request.URL.String(), request.Header, request.Cookies(), bytes)
	if err != nil {
		return err
	}

	return nil
}
