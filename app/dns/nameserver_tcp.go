package dns

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	go_errors "errors"
	"io"
	"net/url"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/common/protocol/dns"
	"github.com/xtls/xray-core/common/session"
	dns_feature "github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet"
)

// tcpConnPoolSize bounds the idle connection pool per TCP name server.
const tcpConnPoolSize = 4

// tcpPooledConnTTL caps how long a pooled connection may be reused.
// Remote resolvers routinely kill idle TCP DNS sessions after ~30-60s; a
// stale pooled conn otherwise turns the next lookup into a guaranteed
// failure. TTL is set below that window so pooled conns stay in the safe
// reuse range, and dead conns are additionally retried transparently.
const tcpPooledConnTTL = 30 * time.Second

// TCPNameServer implemented DNS over TCP (RFC7766).
type TCPNameServer struct {
	cacheController *CacheController
	destination     *net.Destination
	reqID           uint32
	dial            func(context.Context) (net.Conn, error)
	clientIP        net.IP
	connPool        chan *pooledConn
}

// pooledConn pairs a pooled connection with its admission time so getConn
// can drop entries that exceeded tcpPooledConnTTL instead of handing out
// sockets likely killed by the remote end's idle timeout.
type pooledConn struct {
	net.Conn
	pooledAt time.Time
}

// NewTCPNameServer creates DNS over TCP server object for remote resolving.
func NewTCPNameServer(
	url *url.URL,
	dispatcher routing.Dispatcher,
	disableCache bool, serveStale bool, serveExpiredTTL uint32,
	clientIP net.IP,
) (*TCPNameServer, error) {
	s, err := baseTCPNameServer(url, "TCP", disableCache, serveStale, serveExpiredTTL, clientIP)
	if err != nil {
		return nil, err
	}

	s.dial = func(ctx context.Context) (net.Conn, error) {
		link, err := dispatcher.Dispatch(toDnsContext(ctx, s.destination.String()), *s.destination)
		if err != nil {
			return nil, err
		}

		return cnc.NewConnection(
			cnc.ConnectionInputMulti(link.Writer),
			cnc.ConnectionOutputMulti(link.Reader),
		), nil
	}

	errors.LogInfo(context.Background(), "DNS: created TCP client initialized for ", url.String())
	return s, nil
}

// NewTCPLocalNameServer creates DNS over TCP client object for local resolving
func NewTCPLocalNameServer(url *url.URL, disableCache bool, serveStale bool, serveExpiredTTL uint32, clientIP net.IP) (*TCPNameServer, error) {
	s, err := baseTCPNameServer(url, "TCPL", disableCache, serveStale, serveExpiredTTL, clientIP)
	if err != nil {
		return nil, err
	}

	s.dial = func(ctx context.Context) (net.Conn, error) {
		return internet.DialSystem(ctx, *s.destination, nil)
	}

	errors.LogInfo(context.Background(), "DNS: created Local TCP client initialized for ", url.String())
	return s, nil
}

func baseTCPNameServer(url *url.URL, prefix string, disableCache bool, serveStale bool, serveExpiredTTL uint32, clientIP net.IP) (*TCPNameServer, error) {
	port := net.Port(53)
	if url.Port() != "" {
		var err error
		if port, err = net.PortFromString(url.Port()); err != nil {
			return nil, err
		}
	}
	dest := net.TCPDestination(net.ParseAddress(url.Hostname()), port)

	s := &TCPNameServer{
		cacheController: NewCacheController(prefix+"//"+dest.NetAddr(), disableCache, serveStale, serveExpiredTTL),
		destination:     &dest,
		clientIP:        clientIP,
		connPool:        make(chan *pooledConn, tcpConnPoolSize),
	}

	return s, nil
}

// Name implements Server.
func (s *TCPNameServer) Name() string {
	return s.cacheController.name
}

// IsDisableCache implements Server.
func (s *TCPNameServer) IsDisableCache() bool {
	return s.cacheController.disableCache
}

func (s *TCPNameServer) newReqID() uint16 {
	// RFC 5452: query IDs must be unpredictable. A sequential counter lets
	// an observer who captures one query predict the next ID and forge the
	// answer; draw fresh random IDs instead (same hardening as the UDP
	// name server). crypto/rand failure is effectively fatal; fall back to
	// a counter so resolution keeps working rather than stalling.
	var b [2]byte
	if _, err := rand.Read(b[:]); err == nil {
		return binary.BigEndian.Uint16(b[:])
	}
	return uint16(atomic.AddUint32(&s.reqID, 1))
}

// getCacheController implements CachedNameserver.
func (s *TCPNameServer) getCacheController() *CacheController {
	return s.cacheController
}

// getConn returns a pooled connection or dials a fresh one.
//
// Pooled entries older than tcpPooledConnTTL are discarded (remote resolvers
// kill idle sessions; a stale conn would fail the whole lookup). A freshly
// taken entry may still be dead if the remote closed it right after pooling;
// sendQuery therefore retries once on a pre-response transport failure —
// one wasted attempt, never a user-visible error.
func (s *TCPNameServer) getConn(ctx context.Context) (net.Conn, error) {
	for {
		select {
		case pc := <-s.connPool:
			if pc == nil || pc.Conn == nil {
				continue
			}
			if time.Since(pc.pooledAt) > tcpPooledConnTTL {
				pc.Conn.Close()
				continue
			}
			return pc.Conn, nil
		default:
		}
		return s.dial(ctx)
	}
}

func (s *TCPNameServer) putConn(conn net.Conn) {
	if conn == nil {
		return
	}
	select {
	case s.connPool <- &pooledConn{Conn: conn, pooledAt: time.Now()}:
	default:
		conn.Close()
	}
}

// isStaleConnErr reports whether err looks like a failure of a reused
// connection that the remote end had already closed (RST/FIN on first use).
// Only transport-level errors qualify — parse errors and DNS-level errors
// must not trigger a retry.
func isStaleConnErr(err error) bool {
	if err == nil {
		return false
	}
	if go_errors.Is(err, io.EOF) ||
		go_errors.Is(err, io.ErrUnexpectedEOF) ||
		go_errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "aborted") || // Windows WSAECONNABORTED
		strings.Contains(msg, "forcibly closed") // Windows WSAECONNRESET
}

// sendQuery implements CachedNameserver.
func (s *TCPNameServer) sendQuery(ctx context.Context, noResponseErrCh chan<- error, fqdn string, option dns_feature.IPOption) {
	errors.LogInfo(ctx, s.Name(), " querying DNS for: ", fqdn)

	reqs, err := buildReqMsgs(fqdn, option, s.newReqID, genEDNS0Options(s.clientIP, 0))
	if err != nil {
		errors.LogErrorInner(ctx, err, "failed to build dns query for ", fqdn)
		if noResponseErrCh != nil {
			if option.IPv4Enable {
				noResponseErrCh <- err
			}
			if option.IPv6Enable {
				noResponseErrCh <- err
			}
		}
		return
	}

	var deadline time.Time
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	} else {
		deadline = time.Now().Add(time.Second * 5)
	}

	for _, req := range reqs {
		go func(r *dnsRequest) {
			// Detach from parent cancel; keep deadline only.
			dnsCtx := context.WithoutCancel(ctx)

			if inbound := session.InboundFromContext(ctx); inbound != nil {
				dnsCtx = session.ContextWithInbound(dnsCtx, inbound)
			}

			dnsCtx = session.ContextWithContent(dnsCtx, &session.Content{
				Protocol:       "dns",
				SkipDNSResolve: true,
			})

			var cancel context.CancelFunc
			dnsCtx, cancel = context.WithDeadline(dnsCtx, deadline)
			defer cancel()
			defer releaseDnsRequest(r)

			b, err := dns.PackMessage(r.msg)
			if err != nil {
				errors.LogErrorInner(ctx, err, "failed to pack dns query")
				if noResponseErrCh != nil {
					noResponseErrCh <- err
				}
				return
			}
			if r.msg != nil {
				releaseMessage(r.msg)
				r.msg = nil
			}

			// Wire-format the query once so a stale-connection retry can
			// resend it byte-for-byte without re-packing.
			dnsReqBuf := buf.New()
			if err = binary.Write(dnsReqBuf, binary.BigEndian, uint16(b.Len())); err != nil {
				dnsReqBuf.Release()
				errors.LogErrorInner(ctx, err, "binary write failed")
				if noResponseErrCh != nil {
					noResponseErrCh <- err
				}
				return
			}
			if _, err = dnsReqBuf.Write(b.Bytes()); err != nil {
				dnsReqBuf.Release()
				errors.LogErrorInner(ctx, err, "buffer write failed")
				if noResponseErrCh != nil {
					noResponseErrCh <- err
				}
				return
			}
			b.Release()
			defer dnsReqBuf.Release()

			for attempt := 0; ; attempt++ {
				conn, err := s.getConn(dnsCtx)
				if err != nil {
					errors.LogErrorInner(ctx, err, "failed to dial namesever")
					if noResponseErrCh != nil {
						noResponseErrCh <- err
					}
					return
				}

				respBuf := buf.New()
				rec, transportFailure, xerr := exchangeTCP(conn, dnsReqBuf.Bytes(), respBuf)
				respBuf.Release()

				if xerr == nil {
					// Cache-poisoning guard: the echoed question must match the request.
					if !responseMatchesRequest(req.domain, rec) {
						errors.LogErrorInner(ctx, errors.New("question mismatch"), "DNS over TCP response discarded")
						xerr = errors.New("response question mismatch")
					} else {
						s.cacheController.updateRecord(r, rec)
						s.putConn(conn)
						break
					}
				}

				conn.Close()
				// A pooled connection the remote had already closed must not
				// fail the whole lookup: transparently retry once on a fresh
				// dial. Only pre-response transport failures qualify — parse
				// errors and protocol-level mismatches never retry.
				if attempt == 0 && transportFailure && isStaleConnErr(xerr) {
					errors.LogInfoInner(ctx, xerr, "discarding stale pooled TCP connection, retrying ", fqdn)
					continue
				}
				errors.LogErrorInner(ctx, xerr, "failed to query over TCP for ", fqdn)
				if noResponseErrCh != nil {
					noResponseErrCh <- xerr
				}
				return
			}
		}(req)
	}
}

// exchangeTCP runs one length-prefixed DNS-over-TCP exchange on conn.
// transportFailure reports that the exchange died before any response byte
// was consumed (write or first-byte read) — the signature of reusing a
// connection the remote end had already closed.
func exchangeTCP(conn net.Conn, wire []byte, resp *buf.Buffer) (rec *IPRecord, transportFailure bool, err error) {
	if _, err = conn.Write(wire); err != nil {
		return nil, true, err
	}
	n, err := resp.ReadFullFrom(conn, 2)
	if err != nil {
		return nil, n == 0, err
	}
	var length uint16
	if err = binary.Read(bytes.NewReader(resp.Bytes()), binary.BigEndian, &length); err != nil {
		return nil, false, err
	}
	resp.Clear()
	n, err = resp.ReadFullFrom(conn, int32(length))
	if err != nil {
		return nil, n == 0, err
	}
	rec, err = parseResponse(resp.Bytes())
	return rec, false, err
}

// QueryIP implements Server.
func (s *TCPNameServer) QueryIP(ctx context.Context, domain string, option dns_feature.IPOption) ([]net.IP, uint32, error) {
	return queryIP(ctx, s, domain, option)
}
