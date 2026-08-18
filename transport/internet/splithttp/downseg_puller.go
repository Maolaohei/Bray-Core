package splithttp

// Client side of downlink segmentation (Bray-paired M1): instead of one
// unbounded long GET for the downlink, the client pulls the downlink as a
// sequence of finalized 256KiB segments (GET+seq). This shapes the download
// as a set of short, browser-natural segment GETs (HLS/DASH style) rather
// than one infinite GET, and gives per-segment retry.
//
// Wire: each segment pull is a GET on the stream path whose meta token
// carries (sessionId, seq) and whose request declares the dseg marker header.
// The server (already implemented) answers:
//   200 + body           -> finalized segment payload
//   200 + empty body     -> stream finalized, no more segments (EOF)
//   410                  -> segment slid past (client advances)
//   404                  -> segment not yet produced (producer in flight):
//                            retry after a short wait
//
// Production is driven by a separate production leg (a sessioned dseg GET
// without seq) the dialer keeps open; this puller is purely the consumer.

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var (
	errSegGone     = errorsNew("downlink segment slid past (410)")
	errSegNotFound = errorsNew("downlink segment not produced yet (404)")
)

func errorsNew(s string) error { return &segError{s} }

type segError struct{ s string }

func (e *segError) Error() string { return e.s }

// PullSegment fetches one finalized downlink segment (200 body) or a
// 200-empty (EOF), distinguishing it from transient 404 and slipped 410.
func (c *DefaultDialerClient) PullSegment(ctx context.Context, base *url.URL, sessionId, seqStr string) ([]byte, error) {
	if base == nil {
		return nil, errorsNew("nil base URL")
	}
	u := new(url.URL)
	*u = *base
	req := &http.Request{
		Method:     http.MethodGet,
		URL:        u,
		Host:       u.Host,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	req = req.WithContext(ctx)
	// FillStreamRequest stamps padding + meta(sessionId, seq) (seq now
	// honored); the dseg marker header turns this into a segment pull on
	// the server.
	c.transportConfig.FillStreamRequest(req, sessionId, seqStr)
	req.Header.Set(downsegHeader, "1")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		body, rerr := io.ReadAll(resp.Body)
		if rerr != nil {
			return nil, rerr
		}
		return body, nil
	case http.StatusGone:
		return nil, errSegGone
	case http.StatusNotFound:
		return nil, errSegNotFound
	default:
		return nil, errorsNew("unexpected segment status " + strconv.Itoa(resp.StatusCode))
	}
}

// DownSegPuller is a sequential segment consumer: it pulls segments 0,1,2,...
// and yields their concatenated bytes (with gap retry) through Read. It is
// the M1 replacement for the long-GET download reader on the packet-up leg.
type DownSegPuller struct {
	client    *DefaultDialerClient
	base      *url.URL
	sessionId string
	ctx       context.Context

	// prod is the optional dseg production leg (GET without seq) that feeds
	// the server's segment cache; closed together with the puller to signal
	// EOF (finalize) on the server.
	prod io.Closer

	seq    uint64 // next segment to pull
	cur    []byte // current segment payload (being consumed)
	eof    bool
	closed bool
}

// NewDownSegPuller creates a segment puller for sessionId over base. prod
// (optional) is the production leg whose Close finalizes the stream.
func NewDownSegPuller(ctx context.Context, client *DefaultDialerClient, base *url.URL, sessionId string, prod io.Closer) *DownSegPuller {
	return &DownSegPuller{
		client:    client,
		base:      base,
		sessionId: sessionId,
		ctx:       ctx,
		prod:      prod,
	}
}

// Read returns the next bytes of the reconstructed downlink byte stream.
func (p *DownSegPuller) Read(b []byte) (int, error) {
	if p.closed {
		return 0, io.EOF
	}
	for len(p.cur) == 0 {
		if p.eof {
			return 0, io.EOF
		}
		seg, err := p.client.PullSegment(p.ctx, p.base, p.sessionId, strconv.FormatUint(p.seq, 10))
		switch {
		case err == nil:
			if len(seg) == 0 {
				// Empty 200 = EOF (server finalized, no more segments).
				p.eof = true
				return 0, io.EOF
			}
			p.cur = seg
			p.seq++
		case err == errSegGone:
			// Server slid past this segment: the client fell behind. Advance
			// and keep going (gaps are acceptable in a downlink stream the
			// upper layers tolerate; a misplaced read is still better than
			// hanging).
			p.seq++
		case err == errSegNotFound:
			// Producer still in flight: wait briefly and retry. If the
			// context is done, surface it.
			select {
			case <-p.ctx.Done():
				return 0, p.ctx.Err()
			case <-time.After(40 * time.Millisecond):
			}
		default:
			return 0, err
		}
	}
	n := copy(b, p.cur)
	p.cur = p.cur[n:]
	return n, nil
}

// Close marks the puller finished and, if present, closes the production leg
// (which finalizes the server-side stream / EOF).
func (p *DownSegPuller) Close() error {
	p.closed = true
	if p.prod != nil {
		return p.prod.Close()
	}
	return nil
}
