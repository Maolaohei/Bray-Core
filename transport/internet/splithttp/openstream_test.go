package splithttp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestOpenStream_DownloadWaitsForHeadersAndReturnsBody(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("payload")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	rc, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(b) != "payload" {
		t.Fatalf("body=%q", string(b))
	}
}

func TestOpenStream_Non200ReturnsError(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 404,
			Body:       io.NopCloser(strings.NewReader("nope")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	rc, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err == nil {
		if rc != nil {
			rc.Close()
		}
		t.Fatal("expected non-200 error")
	}
	if c.IsClosed() {
		t.Fatal("non-200 must not mark connection fatal by itself")
	}
}

func TestOpenStream_DoErrorSurfacesAndFatalMarks(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, io.EOF
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	_, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if !c.IsClosed() {
		t.Fatal("EOF must mark dialer closed")
	}
}

func TestOpenStream_UploadOnlySuccessKeepsDialerOpen(t *testing.T) {
	bodySeen := make(chan struct{})
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		// stream-up: 200 with empty body; request body may still be open.
		go func() {
			if req.Body != nil {
				_, _ = io.Copy(io.Discard, req.Body)
			}
			close(bodySeen)
		}()
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	pr, pw := io.Pipe()
	rc, _, _, err := c.OpenStream(context.Background(), "http://example/u", "sid", pr, true)
	if err != nil {
		t.Fatalf("OpenStream uploadOnly: %v", err)
	}
	if rc != nil {
		t.Fatal("uploadOnly should not return a response body reader")
	}
	_, _ = pw.Write([]byte("up"))
	_ = pw.Close()
	select {
	case <-bodySeen:
	case <-time.After(2 * time.Second):
		t.Fatal("request body was not consumed")
	}
	if c.IsClosed() {
		t.Fatal("successful uploadOnly must not close dialer")
	}
}

func TestOpenStream_UploadOnlyFatalMarksClosed(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, io.EOF
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	pr, pw := io.Pipe()
	defer pw.Close()
	_, _, _, err := c.OpenStream(context.Background(), "http://example/u", "sid", pr, true)
	if err == nil {
		t.Fatal("expected fatal error")
	}
	if !c.IsClosed() {
		t.Fatal("uploadOnly fatal must mark dialer closed")
	}
}
