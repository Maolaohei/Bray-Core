package splithttp

import (
	"context"
	"crypto/sha256"
	gotls "crypto/tls"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/netip"
	"net/url"
	reflect "reflect"
	"runtime"

	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/hysteria/udphop"
	"github.com/xtls/xray-core/transport/internet/quality"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tcpinfo"
	"github.com/xtls/xray-core/transport/internet/tls"
	"github.com/xtls/xray-core/transport/pipe"
	"golang.org/x/net/http2"
)

// MuxKey uniquely identifies an XMUX connection pool.
// destIdentity is Network|Address|Port|OriginalDomain so two domains that
// resolve to the same IP never share a pool entry (github.com vs githubassets.com).
type MuxKey struct {
	destIdentity      string   // Network + address + port + OriginalDomain
	tlsServerName     string   // TLS ServerName (for non-Reality)
	realityServerName string   // REALITY disguise domain
	protocol          string   // "xhttp", "grpc", "tcp", etc.
	security          string   // "tls", "reality", "none"
	configHash        [32]byte // SHA256 of security config
}

func muxDestIdentity(dest net.Destination) string {
	// Stack-built identity "tcp|1.2.3.4|443|example.com" (IPv6 keeps the
	// bracketed form of ipv6Address.String()). Avoids the per-Dial
	// strings.Builder + net.IP.String() allocations on the global-lock hot
	// path; output is byte-for-byte identical to the previous builder form.
	var buf [384]byte
	b := buf[:0]
	switch dest.Network {
	case net.Network_TCP:
		b = append(b, "tcp|"...)
	case net.Network_UDP:
		b = append(b, "udp|"...)
	case net.Network_UNIX:
		b = append(b, "unix|"...)
	default:
		b = append(b, "unknown|"...)
	}
	if dest.Address != nil {
		switch dest.Address.Family() {
		case net.AddressFamilyIPv4:
			if a, ok := netip.AddrFromSlice(dest.Address.IP()); ok {
				b, _ = a.AppendText(b)
			} else {
				b = append(b, dest.Address.String()...)
			}
		case net.AddressFamilyIPv6:
			if a, ok := netip.AddrFromSlice(dest.Address.IP()); ok {
				b = append(b, '[')
				b, _ = a.AppendText(b)
				b = append(b, ']')
			} else {
				b = append(b, dest.Address.String()...)
			}
		default:
			b = append(b, dest.Address.Domain()...)
		}
	}
	b = append(b, '|')
	b = appendPort(b, uint16(dest.Port))
	b = append(b, '|')
	b = append(b, dest.OriginalDomain...)
	return string(b)
}

// appendPort formats p in decimal without allocation (strconv-free itoa).
func appendPort(b []byte, p uint16) []byte {
	if p == 0 {
		return append(b, '0')
	}
	var tmp [5]byte
	i := len(tmp)
	for p > 0 {
		i--
		tmp[i] = byte('0' + p%10)
		p /= 10
	}
	return append(b, tmp[i:]...)
}

// newMuxKey builds a MuxKey from destination and stream settings.
func newMuxKey(dest net.Destination, streamSettings *internet.MemoryStreamConfig) MuxKey {
	tlsCfg := tls.ConfigFromStreamSettings(streamSettings)
	realityCfg := reality.ConfigFromStreamSettings(streamSettings)
	transportCfg, _ := streamSettings.ProtocolSettings.(*Config)

	// TLS ServerName (for non-Reality)
	tlsServerName := ""
	if tlsCfg != nil && tlsCfg.ServerName != "" {
		tlsServerName = tlsCfg.ServerName
	}

	// REALITY disguise domain
	realityServerName := ""
	if realityCfg != nil && realityCfg.ServerName != "" {
		realityServerName = realityCfg.ServerName
	}

	// Security type
	security := "none"
	if realityCfg != nil {
		security = "reality"
	} else if tlsCfg != nil {
		security = "tls"
	}

	// Config fingerprint hash
	var configHash [32]byte
	h := sha256.New()
	if realityCfg != nil {
		h.Write([]byte("reality"))
		h.Write([]byte(realityCfg.ServerName))
		h.Write(realityCfg.PublicKey)
		h.Write(realityCfg.ShortId)
		h.Write([]byte(realityCfg.Fingerprint))
	} else if tlsCfg != nil {
		h.Write([]byte("tls"))
		h.Write([]byte(tlsCfg.ServerName))
		h.Write([]byte(tlsCfg.CipherSuites))
		h.Write([]byte(tlsCfg.Fingerprint))
		// ALPN: h2 vs http/1.1 have different connection semantics
		for _, alpn := range tlsCfg.NextProtocol {
			h.Write([]byte(alpn))
		}
	}
	if transportCfg != nil {
		h.Write([]byte(transportCfg.Mode))
		h.Write([]byte(transportCfg.Path))
	}
	copy(configHash[:], h.Sum(nil))

	// Protocol
	protocol := streamSettings.ProtocolName

	return MuxKey{
		destIdentity:      muxDestIdentity(dest),
		tlsServerName:     tlsServerName,
		realityServerName: realityServerName,
		protocol:          protocol,
		security:          security,
		configHash:        configHash,
	}
}

var (
	globalDialerMap    map[MuxKey]*XmuxManager
	globalDialerAccess sync.Mutex
	globalDialerQuit   chan struct{}
	globalDialerDone   chan struct{} // closed when cleanup goroutine exits
)

const (
	// globalMapIdleTimeout is how long a manager can be idle before being removed.
	// For small pools (<10 managers), 10 minutes is fine. For large pools,
	// clean up faster to reduce memory pressure.
	globalMapIdleTimeoutBase = 10 * time.Minute
	globalMapIdleTimeoutMin  = 3 * time.Minute
)

func getHTTPClient(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (DialerClient, *XmuxClient, error) {
	realityConfig := reality.ConfigFromStreamSettings(streamSettings)

	if false /* Bray-only: browser dialer disabled */ && realityConfig == nil {
		return &BrowserDialerClient{transportConfig: streamSettings.ProtocolSettings.(*Config)}, nil, nil
	}

	// Phase 1: Lookup or create XmuxManager under lock (no I/O)
	// Build the MuxKey before taking the global lock: newMuxKey is a pure
	// function of (dest, streamSettings) but costs ~1us (sha256 + string
	// building). Computing it inside the lock serialized every concurrent
	// Dial on the global mutex; with it outside, the locked section is only
	// a map lookup + nil check.
	key := newMuxKey(dest, streamSettings)

	globalDialerAccess.Lock()

	if globalDialerMap == nil {
		globalDialerMap = make(map[MuxKey]*XmuxManager)
		globalDialerQuit = make(chan struct{})
		globalDialerDone = make(chan struct{})
		go globalDialerCleanup(globalDialerDone)
	}

	xmuxManager, found := globalDialerMap[key]

	if !found {
		transportConfig := streamSettings.ProtocolSettings.(*Config)
		// Share the proto by pointer; never copy it by value (it carries an
		// internal mutex). Nil means all-defaults, handled by NewXmuxManager.
		xmuxConfig := transportConfig.Xmux

		// Build probe URL before starting XmuxManager background loops so
		// preConnectLoop/newXmuxClient never race a later probeURL write.
		tlsCfg := tls.ConfigFromStreamSettings(streamSettings)
		realityCfg := reality.ConfigFromStreamSettings(streamSettings)
		var probeScheme string
		if tlsCfg != nil || realityCfg != nil {
			probeScheme = "https"
		} else {
			probeScheme = "http"
		}
		probeHost := transportConfig.Host
		if probeHost == "" && tlsCfg != nil {
			probeHost = tlsCfg.ServerName
		}
		if probeHost == "" && realityCfg != nil {
			probeHost = realityCfg.ServerName
		}
		if probeHost == "" {
			probeHost = dest.ServerName()
		}
		if false /* Bray-only: browser dialer disabled */ && realityCfg == nil {
			if !(probeScheme == "http" && dest.Port == 80) && !(probeScheme == "https" && dest.Port == 443) {
				probeHost += ":" + dest.Port.String()
			}
		}
		probeURL := probeScheme + "://" + probeHost + transportConfig.GetNormalizedPath()

		xmuxManager = NewXmuxManager(xmuxConfig, func() XmuxConn {
			return createHTTPClient(dest, streamSettings)
		}, probeURL)

		globalDialerMap[key] = xmuxManager
	}

	globalDialerAccess.Unlock()

	// Phase 2: Get client outside lock (may involve network I/O)
	xmuxClient, err := xmuxManager.GetXmuxClient(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("XMUX: failed to get client: %w", err)
	}

	client, ok := xmuxClient.XmuxConn.(DialerClient)
	if !ok {
		return nil, nil, errors.New("XMUX: XmuxConn does not implement DialerClient")
	}

	// Set RTT callback on DefaultDialerClient for RTT-aware scheduling
	if dc, ok := client.(*DefaultDialerClient); ok {
		// Atomic profile reference - set by onNewConn, read by onRTT
		var activeProfile atomic.Value // stores interface{ FeedRTT(time.Duration) }

		dc.SetOnRTT(func(rtt time.Duration) {
			xmuxClient.UpdateRTT(rtt)
			// Feed RTT to Profile collector (no-op on Linux, estimated on Windows)
			if p := activeProfile.Load(); p != nil {
				p.(interface{ FeedRTT(time.Duration) }).FeedRTT(rtt)
			}
		})
		// Stream TTFB feeds manager-level pool health metrics (not per-client scheduling).
		dc.SetOnTTFB(func(ttfb time.Duration) {
			xmuxManager.RecordTTFB(ttfb)
		})

		// Wire up TransportProfile: raw TCP socket -> Profile -> UpdateQuality -> scoreClient
		dc.SetOnNewConn(func(rawConn net.Conn) {
			profile := tcpinfo.NewProfile(rawConn, nil)
			profile.OnUpdate(func(snap *quality.Snapshot) {
				q := int32(snap.Quality.Overall)
				conf := int32(snap.Confidence)
				var retrans int32
				if snap.Retrans.Valid {
					retrans = int32(snap.Retrans.Value)
				}
				var lossRate int64
				if snap.Loss.Valid {
					lossRate = int64(snap.Loss.Value * 10000)
				}
				xmuxClient.UpdateQuality(q, conf, retrans, lossRate)
			})
			profile.Start()
			xmuxClient.StartProfiling(profile)
			activeProfile.Store(profile)
		})

		// Fast Eviction: mark client as dead on fatal connection errors
		dc.SetOnFatalError(func(err error) {
			errors.LogInfo(ctx, "XMUX: Fast Eviction triggered, marking client dead: ", err)
			xmuxClient.MarkDead()
		})
	}

	return client, xmuxClient, nil
}

func decideHTTPVersion(tlsConfig *tls.Config, realityConfig *reality.Config) string {
	if realityConfig != nil {
		return "2"
	}
	if tlsConfig == nil {
		return "1.1"
	}
	if len(tlsConfig.NextProtocol) != 1 {
		return "2"
	}
	if tlsConfig.NextProtocol[0] == "http/1.1" {
		return "1.1"
	}
	if tlsConfig.NextProtocol[0] == "h3" {
		return "3"
	}
	return "2"
}

// destinationFromEndpoint parses host:port (or tcp:/udp: prefix) into a Destination
// retaining the network of the primary dial target when unspecified.
//
// IPv6-safe: uses SplitHostPort semantics. Bare hosts (no port) and bare/bracketed
// IPv6 without a port inherit primary.Port. Prefer "[2001:db8::1]:443" form for
// explicit IPv6 endpoints.
func destinationFromEndpoint(endpoint string, primary net.Destination) (net.Destination, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return net.Destination{}, errors.New("empty multi-endpoint")
	}

	lower := strings.ToLower(endpoint)
	hostport := endpoint
	if strings.HasPrefix(lower, "tcp:") {
		hostport = endpoint[4:]
	} else if strings.HasPrefix(lower, "udp:") {
		hostport = endpoint[4:]
	}

	// Bare hostname / bare IPv6 (no port) inherits primary port so operators can
	// list "cdn2.example.com" or "[2001:db8::1]" without repeating the port.
	if !strings.HasPrefix(lower, "unix:") {
		if _, _, err := net.SplitHostPort(hostport); err != nil {
			host := strings.TrimSpace(hostport)
			host = strings.TrimPrefix(host, "[")
			host = strings.TrimSuffix(host, "]")
			if host != "" && primary.Port != 0 {
				var hp string
				if strings.Contains(host, ":") {
					hp = "[" + host + "]:" + primary.Port.String()
				} else {
					hp = host + ":" + primary.Port.String()
				}
				if strings.HasPrefix(lower, "tcp:") {
					endpoint = "tcp:" + hp
				} else if strings.HasPrefix(lower, "udp:") {
					endpoint = "udp:" + hp
				} else {
					endpoint = hp
				}
			}
		}
	}

	d, err := net.ParseDestination(endpoint)
	if err != nil {
		// allow bare host:port without scheme
		d, err = net.ParseDestination("tcp:" + endpoint)
		if err != nil {
			return net.Destination{}, err
		}
	}
	if d.Network == net.Network_Unknown || d.Network == 0 {
		d.Network = primary.Network
	}
	if primary.Network != 0 && d.Network == net.Network_TCP && primary.Network == net.Network_UDP {
		// Keep primary network for H3/UDP when endpoint string omitted scheme.
		elower := strings.ToLower(endpoint)
		if !strings.HasPrefix(elower, "udp:") && !strings.HasPrefix(elower, "tcp:") {
			d.Network = primary.Network
		}
	}
	if d.Port == 0 {
		d.Port = primary.Port
	}
	return d, nil
}
func createHTTPClient(dest net.Destination, streamSettings *internet.MemoryStreamConfig) DialerClient {
	tlsConfig := tls.ConfigFromStreamSettings(streamSettings)
	realityConfig := reality.ConfigFromStreamSettings(streamSettings)

	httpVersion := decideHTTPVersion(tlsConfig, realityConfig)
	if httpVersion == "3" {
		dest.Network = net.Network_UDP // better to keep this line
	}

	var gotlsConfig *gotls.Config

	if tlsConfig != nil {
		gotlsConfig = tlsConfig.GetTLSConfig(tls.WithDestination(dest))
	}

	transportConfig := streamSettings.ProtocolSettings.(*Config)

	dc := &DefaultDialerClient{
		transportConfig: transportConfig,
		httpVersion:     httpVersion,
	}

	// Opt-in multi-endpoint race list (Wave-3). Default single dest unchanged.
	// Wave-5: sticky last-good endpoint reorders the race list per dial (TTL).
	var multiEndpoints []string
	var multiHeaders map[string]string
	if transportCfg := streamSettings.ProtocolSettings.(*Config); transportCfg != nil {
		multiHeaders = transportCfg.Headers
		if MultiEndpointEnabled(transportCfg.Headers) {
			multiEndpoints = BuildEndpointList(dest.NetAddr(), ParseExtraEndpoints(transportCfg.Headers))
		}
	}
	// Host/SNI for endpoint sticky key (aligned with mode sticky dest|host).
	stickyEPHost := ""
	if transportConfig != nil {
		stickyEPHost = transportConfig.Host
	}
	if stickyEPHost == "" && tlsConfig != nil {
		stickyEPHost = tlsConfig.ServerName
	}
	if stickyEPHost == "" && realityConfig != nil {
		stickyEPHost = realityConfig.ServerName
	}
	if stickyEPHost == "" {
		stickyEPHost = dest.ServerName()
	}
	multiPrimaryEP := ""
	if len(multiEndpoints) > 0 {
		multiPrimaryEP = multiEndpoints[0]
	}
	multiStickyKey := stickyEndpointKey(dest.NetAddr(), stickyEPHost)

	dialRawTCP := func(ctxInner context.Context, target net.Destination) (net.Conn, error) {
		conn, err := internet.DialSystem(ctxInner, target, streamSettings.SocketSettings)
		if err != nil {
			return nil, err
		}
		// ToT M3: outer-path socket tuning (Linux: BBR + larger buffers;
		// other platforms no-op). See tune_socket_linux.go.
		tuneOuterSocket(conn)
		return conn, nil
	}

	dialContext := func(ctxInner context.Context) (net.Conn, error) {
		t0 := time.Now()
		var rawConn net.Conn
		var err error
		if len(multiEndpoints) > 1 {
			raceList := multiEndpoints
			// Capture preferred sticky EP once for reorder + fail-invalidate (green-zone).
			stickyPreferred := ""
			if StickyEndpointEnabled(multiHeaders) {
				if se, ok := LookupStickyEndpoint(multiStickyKey); ok {
					stickyPreferred = se
					reordered := ApplyStickyEndpoints(raceList, se)
					if len(reordered) > 0 && reordered[0] != raceList[0] {
						recordEndpointStickyHit()
						errors.LogDebug(ctxInner, "XHTTP sticky endpoint: prefer ", reordered[0], " for ", multiStickyKey)
					}
					raceList = reordered
				}
			}
			var winner string
			rawConn, winner, err = RaceDialEndpoints(ctxInner, raceList, func(ctx context.Context, endpoint string) (net.Conn, error) {
				d, perr := destinationFromEndpoint(endpoint, dest)
				if perr != nil {
					return nil, perr
				}
				return dialRawTCP(ctx, d)
			})
			if err == nil {
				recordMultiEndpointRace(winner != "" && winner != multiPrimaryEP)
				if StickyEndpointEnabled(multiHeaders) && winner != "" {
					_, epTTL := StickyTTLFromHeaders(multiHeaders)
					RememberStickyEndpointTTL(multiStickyKey, winner, epTTL)
				}
			} else if StickyEndpointEnabled(multiHeaders) && stickyPreferred != "" {
				// Race failed while sticky was preferred: clear affinity so next dial
				// re-probes the full endpoint list (mirror NoteStickyModeFailure).
				NoteStickyEndpointFailure(multiStickyKey, stickyPreferred)
			}
		} else {
			rawConn, err = dialRawTCP(ctxInner, dest)
		}
		tcpDur := time.Since(t0)
		if err != nil {
			errors.LogDebug(ctxInner, "XHTTP dial: TCP failed in ", tcpDur.Round(time.Millisecond), ": ", err)
			return nil, err
		}

		// Track raw socket so MarkDead/Close can force-close active H2 connections.
		// Wrap first so onNewConn (TCP_INFO profiling) sees the same conn object.
		rawConn = dc.trackConn(rawConn)
		if fn := dc.getOnNewConn(); fn != nil {
			fn(rawConn)
		}

		conn := rawConn

		if streamSettings.TcpmaskManager != nil {
			newConn, err := streamSettings.TcpmaskManager.WrapConnClient(conn)
			if err != nil {
				conn.Close()
				return nil, errors.New("mask err").Base(err)
			}
			conn = newConn
		}

		if realityConfig != nil {
			t1 := time.Now()
			newConn, err := reality.UClient(conn, realityConfig, ctxInner, dest)
			realityDur := time.Since(t1)
			if err != nil {
				conn.Close()
				errors.LogDebug(ctxInner, "XHTTP dial: REALITY failed in ", realityDur.Round(time.Millisecond), ": ", err)
				return nil, err
			}
			conn = newConn
			return conn, nil
		}

		if gotlsConfig != nil {
			t1 := time.Now()
			if fingerprint := tls.GetFingerprint(tlsConfig.Fingerprint); fingerprint != nil {
				tlsConn := tls.UClient(conn, gotlsConfig, fingerprint)
				if uconn, ok := tlsConn.(*tls.UConn); ok {
					if err := uconn.HandshakeContext(ctxInner); err != nil {
						conn.Close()
						errors.LogDebug(ctxInner, "XHTTP dial: uTLS failed in ", time.Since(t1).Round(time.Millisecond), ": ", err)
						return nil, err
					}
				}
				conn = tlsConn
			} else {
				conn = tls.Client(conn, gotlsConfig)
			}
		}

		return conn, nil
	}

	var keepAlivePeriod time.Duration
	if streamSettings.ProtocolSettings.(*Config).Xmux != nil {
		keepAlivePeriod = time.Duration(streamSettings.ProtocolSettings.(*Config).Xmux.HKeepAlivePeriod) * time.Second
	}

	var transport http.RoundTripper

	if httpVersion == "3" {
		quicParams := streamSettings.QuicParams
		if quicParams == nil {
			quicParams = &internet.QuicParams{
				BbrProfile: string(bbr.ProfileStandard),
				UdpHop:     &internet.UdpHop{},
			}
		}

		quicConfig := &quic.Config{
			InitialStreamReceiveWindow:     quicParams.InitStreamReceiveWindow,
			MaxStreamReceiveWindow:         quicParams.MaxStreamReceiveWindow,
			InitialConnectionReceiveWindow: quicParams.InitConnReceiveWindow,
			MaxConnectionReceiveWindow:     quicParams.MaxConnReceiveWindow,
			MaxIdleTimeout:                 time.Duration(quicParams.MaxIdleTimeout) * time.Second,
			KeepAlivePeriod:                time.Duration(quicParams.KeepAlivePeriod) * time.Second,
			MaxIncomingStreams:             quicParams.MaxIncomingStreams,
			DisablePathMTUDiscovery:        quicParams.DisablePathMtuDiscovery || (runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin"),
		}
		if quicParams.MaxIdleTimeout == 0 {
			quicConfig.MaxIdleTimeout = net.ConnIdleTimeout
		}
		if quicParams.KeepAlivePeriod == 0 {
			if keepAlivePeriod == 0 {
				quicConfig.KeepAlivePeriod = net.QuicgoH3KeepAlivePeriod
			} else if keepAlivePeriod > 0 {
				quicConfig.KeepAlivePeriod = keepAlivePeriod
			}
		}
		if quicParams.MaxIncomingStreams == 0 {
			// these two are defaults of quic-go/http3. the default of quic-go (no
			// http3) is different, so it is hardcoded here for clarity.
			// https://github.com/quic-go/quic-go/blob/b8ea5c798155950fb5bbfdd06cad1939c9355878/http3/client.go#L36-L39
			quicConfig.MaxIncomingStreams = -1
		}

		h3Transport := &http3.Transport{
			QUICConfig:      quicConfig,
			TLSClientConfig: gotlsConfig,
			Dial: func(ctx context.Context, addr string, tlsCfg *gotls.Config, cfg *quic.Config) (*quic.Conn, error) {
				udpHopDialer := func(addr *net.UDPAddr) (net.PacketConn, error) {
					conn, err := internet.DialSystem(ctx, net.UDPDestination(net.IPAddress(addr.IP), net.Port(addr.Port)), streamSettings.SocketSettings)
					if err != nil {
						errors.LogInfoInner(context.Background(), err, "skip hop: failed to dial to dest")
						return nil, errors.New("")
					}

					var pktConn net.PacketConn

					switch c := conn.(type) {
					case *internet.PacketConnWrapper:
						pktConn = c.PacketConn
					case *cnc.Connection:
						pktConn = &internet.FakePacketConn{Conn: c}
					default:
						return nil, errors.New("unexpected connection type: ", reflect.TypeOf(c))
					}

					return pktConn, nil
				}

				var pktConn net.PacketConn
				var udpAddr *net.UDPAddr
				var index int

				if len(quicParams.UdpHop.Ports) > 0 {
					index = rand.Intn(len(quicParams.UdpHop.Ports))
					dest.Port = net.Port(quicParams.UdpHop.Ports[index])
				}

				raw, err := internet.DialSystem(ctx, dest, streamSettings.SocketSettings)
				if err != nil {
					return nil, errors.New("failed to dial to dest").Base(err)
				}
				switch c := raw.(type) {
				case *internet.PacketConnWrapper:
					pktConn = c.PacketConn
					udpAddr = raw.RemoteAddr().(*net.UDPAddr)
				case *cnc.Connection:
					pktConn = &internet.FakePacketConn{Conn: c}
					udpAddr = &net.UDPAddr{IP: c.RemoteAddr().(*net.TCPAddr).IP, Port: c.RemoteAddr().(*net.TCPAddr).Port}
				default:
					return nil, errors.New("unexpected connection type: ", reflect.TypeOf(c))
				}

				if len(quicParams.UdpHop.Ports) > 0 {
					pktConn = udphop.NewUDPHopPacketConn(udphop.ToAddrs(udpAddr.IP, quicParams.UdpHop.Ports), time.Duration(quicParams.UdpHop.IntervalMin)*time.Second, time.Duration(quicParams.UdpHop.IntervalMax)*time.Second, udpHopDialer, pktConn, index)
				}

				if streamSettings.UdpmaskManager != nil {
					newConn, err := streamSettings.UdpmaskManager.WrapPacketConnClient(pktConn)
					if err != nil {
						pktConn.Close()
						return nil, errors.New("mask err").Base(err)
					}
					pktConn = newConn
				}

				conn, err := quic.DialEarly(ctx, pktConn, udpAddr, tlsCfg, cfg)
				if err != nil {
					return nil, err
				}
				context.AfterFunc(conn.Context(), func() { pktConn.Close() })

				switch quicParams.Congestion {
				case "reno":
				case "", "bbr":
					congestion.UseBBR(conn, bbr.Profile(quicParams.BbrProfile))
				case "force-brutal":
					congestion.UseBrutal(conn, quicParams.BrutalUp)
				default:
					return nil, errors.New("unknown congestion algorithm: ", quicParams.Congestion)
				}

				return conn, nil
			},
		}

		// Happy Eyeballs: build an H2 fallback transport for racing.
		h2KeepAlive := keepAlivePeriod
		if h2KeepAlive == 0 {
			h2KeepAlive = net.ChromeH2KeepAlivePeriod
		}
		if h2KeepAlive < 0 {
			h2KeepAlive = 0
		}
		h2Transport := &http2.Transport{
			DialTLSContext: func(ctxInner context.Context, network string, addr string, cfg *gotls.Config) (net.Conn, error) {
				return dialContext(ctxInner)
			},
			IdleConnTimeout: net.ConnIdleTimeout,
			ReadIdleTimeout: h2KeepAlive,
			// Binary packet-up bodies must not be gzipped; skip Accept-Encoding work.
			DisableCompression: true,
			// 16KiB frame matches browser HTTP/2 SETTINGS (anti-fingerprint);
			// the 256KiB value is a recognizable non-browser machine marker.
			// (x/net's client SETTINGS are otherwise fixed: EnablePush=0 +
			// 4MiB initial window + MaxFrameSize; the window is not
			// configurable on the client side and is not a high-value
			// fingerprint.)
			MaxReadFrameSize: 16384,
		}

		transport = newHappyEyeballsTransport(h3Transport, h2Transport)

		// PostPacket uses HTTP/2 POST semantics (H3 POST is equivalent).
		dc.httpVersion = "2"
	} else if httpVersion == "2" {
		if keepAlivePeriod == 0 {
			keepAlivePeriod = net.ChromeH2KeepAlivePeriod
		}
		if keepAlivePeriod < 0 {
			keepAlivePeriod = 0
		}
		transport = &http2.Transport{
			DialTLSContext: func(ctxInner context.Context, network string, addr string, cfg *gotls.Config) (net.Conn, error) {
				return dialContext(ctxInner)
			},
			IdleConnTimeout: net.ConnIdleTimeout,
			ReadIdleTimeout: keepAlivePeriod,
			// Binary packet-up bodies must not be gzipped; skip Accept-Encoding work.
			DisableCompression: true,
			// 16KiB frame matches browser HTTP/2 SETTINGS (anti-fingerprint);
			// the 256KiB value is a recognizable non-browser machine marker.
			// (x/net's client SETTINGS are otherwise fixed: EnablePush=0 +
			// 4MiB initial window + MaxFrameSize; the window is not
			// configurable on the client side and is not a high-value
			// fingerprint.)
			MaxReadFrameSize: 16384,
		}
	} else {
		httpDialContext := func(ctxInner context.Context, network string, addr string) (net.Conn, error) {
			return dialContext(ctxInner)
		}

		transport = &http.Transport{
			DialTLSContext:  httpDialContext,
			DialContext:     httpDialContext,
			IdleConnTimeout: net.ConnIdleTimeout,
			// chunked transfer download with KeepAlives is buggy with
			// http.Client and our custom dial context.
			DisableKeepAlives: true,
		}
	}

	dc.client = &http.Client{
		Transport: transport,
	}
	dc.uploadRawPool = newH1ConnPool(defaultH1UploadPoolCap)
	dc.dialUploadConn = dialContext

	return dc
}

// globalDialerCleanup periodically removes idle XmuxManagers from the global map.
// Prevents memory and goroutine leaks when destinations change over time.
// Exits when globalDialerQuit is closed, cleaning up all remaining managers.
func globalDialerCleanup(done chan struct{}) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			globalDialerAccess.Lock()
			poolSize := len(globalDialerMap)
			// Dynamic timeout: large pools get cleaned up faster.
			// Linear interpolation from 10min (pool<=10) to 3min (pool>=100).
			timeout := globalMapIdleTimeoutBase
			if poolSize > 10 {
				reduction := time.Duration(float64(globalMapIdleTimeoutBase-globalMapIdleTimeoutMin) *
					float64(poolSize-10) / 90.0)
				if reduction > globalMapIdleTimeoutBase-globalMapIdleTimeoutMin {
					reduction = globalMapIdleTimeoutBase - globalMapIdleTimeoutMin
				}
				timeout = globalMapIdleTimeoutBase - reduction
			}
			for key, manager := range globalDialerMap {
				if manager.IdleFor() > timeout {
					delete(globalDialerMap, key)
					go manager.Close()
				}
			}
			globalDialerAccess.Unlock()
		case <-globalDialerQuit:
			globalDialerAccess.Lock()
			for _, manager := range globalDialerMap {
				manager.Close()
			}
			globalDialerMap = nil
			globalDialerAccess.Unlock()
			close(done)
			return
		}
	}
}

// ResetGlobalDialer closes the cleanup goroutine and resets the global state.
// Blocks until the cleanup goroutine finishes closing all managers.
// Intended for test cleanup - call after each test that uses Dial.
func ResetGlobalDialer() {
	globalDialerAccess.Lock()
	if globalDialerQuit != nil {
		close(globalDialerQuit)
		globalDialerQuit = nil
		done := globalDialerDone
		globalDialerDone = nil
		globalDialerAccess.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	globalDialerAccess.Unlock()
}

func init() {
	common.Must(internet.RegisterTransportDialer(protocolName, Dial))
}

func Dial(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
	tlsConfig := tls.ConfigFromStreamSettings(streamSettings)
	realityConfig := reality.ConfigFromStreamSettings(streamSettings)

	httpVersion := decideHTTPVersion(tlsConfig, realityConfig)
	if httpVersion == "3" {
		dest.Network = net.Network_UDP
	}

	transportConfiguration := streamSettings.ProtocolSettings.(*Config)
	// Wave-7: sticky TTL headers are per-entry at remember time (no process globals).
	stickyModeTTL, _ := StickyTTLFromHeaders(transportConfiguration.Headers)
	var requestURL url.URL

	if tlsConfig != nil || realityConfig != nil {
		requestURL.Scheme = "https"
	} else {
		requestURL.Scheme = "http"
	}
	requestURL.Host = transportConfiguration.Host
	if requestURL.Host == "" && tlsConfig != nil {
		requestURL.Host = tlsConfig.ServerName
	}
	if requestURL.Host == "" && realityConfig != nil {
		requestURL.Host = realityConfig.ServerName
	}
	if requestURL.Host == "" {
		requestURL.Host = dest.ServerName()
	}
	if realityConfig == nil {
		// Browser Dialer is disabled (Bray): always append the non-standard
		// port so downstream requests (incl. dseg PullSegment, which uses the
		// URL host via http.Client) resolve the real listener instead of the
		// scheme default (443 for https, 80 for http).
		if !(requestURL.Scheme == "http" && dest.Port == 80) && !(requestURL.Scheme == "https" && dest.Port == 443) {
			requestURL.Host += ":" + dest.Port.String()
		}
	}

	requestURL.Path = transportConfiguration.GetNormalizedPath()
	requestURL.RawQuery = transportConfiguration.GetNormalizedQuery()

	httpClient, xmuxClient, err := getHTTPClient(ctx, dest, streamSettings)
	if err != nil {
		return nil, err
	}

	if err := ValidateConfiguredMode(transportConfiguration.Mode); err != nil {
		return nil, err
	}

	// Browser dialer cannot open bi-dir stream bodies; prefer packet-up for auto.
	preferPacket := false /* Bray-only: browser dialer disabled */ && realityConfig == nil
	initialMode := ResolveInitialModeOpts(transportConfiguration.Mode, realityConfig != nil, transportConfiguration.DownloadSettings != nil, preferPacket)
	allowModeDegrade := ShouldAttemptModeDegrade(transportConfiguration.Mode, transportConfiguration.Headers)
	// Explicit stream modes under browser still fail closed unless degrade is on.
	modeCascade := BuildModeCascade(initialMode, allowModeDegrade)

	// Fail-closed before any stream open burns XMUX quota when packet-up is possible.
	for _, m := range modeCascade {
		if m == "packet-up" {
			if transportConfiguration.GetNormalizedScMaxEachPostBytes().From <= 0 {
				return nil, errors.New("`scMaxEachPostBytes` should be bigger than 0")
			}
			break
		}
	}

	// Wave-4 sticky last-good mode: prefer previously successful mode on this dest.
	stickyKey := stickyDestKey(dest.NetAddr(), requestURL.Host)
	if allowModeDegrade && StickyModeEnabled(transportConfiguration.Headers) {
		if sm, ok := LookupStickyMode(stickyKey); ok {
			before := modeCascade
			modeCascade = ApplyStickyMode(modeCascade, sm)
			if len(modeCascade) > 0 && (len(before) != len(modeCascade) || before[0] != modeCascade[0]) {
				recordStickyHit()
				errors.LogDebug(ctx, "XHTTP sticky mode: prefer ", modeCascade[0], " for ", stickyKey)
			}
		}
	}
	recordModeAttempt()

	// Prepare optional download leg once (shared across mode cascade attempts).
	requestURL2 := requestURL
	httpClient2 := httpClient
	xmuxClient2 := xmuxClient
	var dest2 net.Destination
	hasDownload := transportConfiguration.DownloadSettings != nil
	if hasDownload {
		globalDialerAccess.Lock()
		if streamSettings.DownloadSettings == nil {
			streamSettings.DownloadSettings = common.Must2(internet.ToMemoryStreamConfig(transportConfiguration.DownloadSettings))
			if streamSettings.SocketSettings != nil && streamSettings.SocketSettings.Penetrate {
				streamSettings.DownloadSettings.SocketSettings = streamSettings.SocketSettings
			}
		}
		globalDialerAccess.Unlock()
		memory2 := streamSettings.DownloadSettings
		if memory2.Destination == nil {
			return nil, errors.New("downloadSettings has nil Destination")
		}
		dest2 = *memory2.Destination
		tlsConfig2 := tls.ConfigFromStreamSettings(memory2)
		realityConfig2 := reality.ConfigFromStreamSettings(memory2)
		httpVersion2 := decideHTTPVersion(tlsConfig2, realityConfig2)
		if httpVersion2 == "3" {
			dest2.Network = net.Network_UDP
		}
		if tlsConfig2 != nil || realityConfig2 != nil {
			requestURL2.Scheme = "https"
		} else {
			requestURL2.Scheme = "http"
		}
		config2 := memory2.ProtocolSettings.(*Config)
		requestURL2.Host = config2.Host
		if requestURL2.Host == "" && tlsConfig2 != nil {
			requestURL2.Host = tlsConfig2.ServerName
		}
		if requestURL2.Host == "" && realityConfig2 != nil {
			requestURL2.Host = realityConfig2.ServerName
		}
		if requestURL2.Host == "" {
			requestURL2.Host = dest2.ServerName()
		}
		if false /* Bray-only: browser dialer disabled */ && realityConfig2 == nil {
			// For Browser Dialer's optimized IP and non-standard port
			if !(requestURL2.Scheme == "http" && dest2.Port == 80) && !(requestURL2.Scheme == "https" && dest2.Port == 443) {
				requestURL2.Host += ":" + dest2.Port.String()
			}
		}
		requestURL2.Path = config2.GetNormalizedPath()
		requestURL2.RawQuery = config2.GetNormalizedQuery()
		var getErr error
		httpClient2, xmuxClient2, getErr = getHTTPClient(ctx, dest2, memory2)
		if getErr != nil {
			return nil, fmt.Errorf("XMUX: failed to get download client: %w", getErr)
		}
	}

	// Wave-3: try mode cascade (stream-one -> stream-up -> packet-up) on open failure.
	// Each attempt gets a fresh pipe so partial writes never cross modes.
	// Fail-closed guard: session wire modes (stream-up/packet-up) require a MAC
	// secret to sign session IDs. Without one only stream-one is usable — skip
	// session modes instead of emitting unsigned requests the server rejects.
	{
		filtered := modeCascade[:0]
		for _, m := range modeCascade {
			if m == "stream-one" {
				filtered = append(filtered, m)
				continue
			}
			if transportConfiguration.sessionSecret() != nil {
				filtered = append(filtered, m)
			}
		}
		if len(filtered) == 0 {
			// Auto/empty mode degrades to stream-one (the only mode that does
			// not need a session secret); a locked session mode without a
			// secret is a configuration error and fails loudly.
			m := NormalizeXHTTPMode(transportConfiguration.Mode)
			if m == "" || m == "auto" {
				filtered = append(filtered, "stream-one")
			} else {
				return nil, errors.New("XHTTP: session wire modes require x-bray-session-secret (fail-closed); no mode available")
			}
		}
		modeCascade = filtered
	}
	var lastErr error
	for mi, mode := range modeCascade {
		sessionId := ""
		if mode != "stream-one" {
			sessionId = transportConfiguration.GenerateSessionID()
		}

		if xmuxClient != nil && !xmuxClient.Borrow() {
			return nil, errors.New("failed to borrow XMUX client for upload")
		}
		if xmuxClient2 != nil && xmuxClient2 != xmuxClient && !xmuxClient2.Borrow() {
			if xmuxClient != nil {
				xmuxClient.Release()
			}
			return nil, errors.New("failed to borrow XMUX client for download")
		}

		var closed atomic.Int32
		// ownedUploadXmux is the XMUX slot currently charged to this logical conn's upload side.
		// packet-up may rotate to a new client mid-session; onClose must Release the latest.
		ownedUploadXmux := xmuxClient
		var ownedUploadMu sync.Mutex

		reader, writer := io.Pipe()
		conn := splitConn{
			writer: writer,
			onClose: func() {
				if closed.Add(1) > 1 {
					return
				}
				ownedUploadMu.Lock()
				up := ownedUploadXmux
				ownedUploadXmux = nil
				ownedUploadMu.Unlock()
				if up != nil {
					up.Release()
				}
				if xmuxClient2 != nil && xmuxClient2 != xmuxClient {
					xmuxClient2.Release()
				}
			},
		}

		// Unified cleanup: close reader + conn on any error after pipe creation.
		// conn.Close() triggers onClose which Releases borrowed XMUX clients.
		cleanup := func() {
			reader.Close()
			conn.Close()
		}

		// tryMode opens streams for the current mode. Matches historical Dial semantics:
		//   stream-one: single bi-directional OpenStream on primary
		//   else: open download leg first (requestURL2/httpClient2; equals primary when no downloadSettings)
		//   stream-up: then open upload-only stream on primary and finish
		//   packet-up: continue with POST upload loop after download is open
		tryMode := func() (established bool, contPacket bool, openErr error) {
			if mode == "stream-one" {
				requestURL.Path = transportConfiguration.GetNormalizedPath()
				var oerr error
				conn.reader, conn.remoteAddr, conn.localAddr, oerr = httpClient.OpenStream(ctx, &requestURL, sessionId, reader, false)
				if oerr != nil {
					return false, false, oerr
				}
				// Count LeftRequests only after a successful open (Wave-7).
				if xmuxClient != nil {
					xmuxClient.LeftRequests.Add(-1)
					xmuxClient.NoteOpenSuccess()
				}
				return true, false, nil
			}

			// Non-stream-one: download leg first (httpClient2 may equal httpClient).
			{
				var oerr error
				if mode == "packet-up" {
					// M1 downlink segmentation (Bray-paired), marker-free:
					// replacing the single long GET download leg with
					// (a) a production leg — a plain async GET (the server
					// treats it as the producer once the session is in
					// segment mode) — and (b) a sequentially pulled
					// segment reader. Only the native dialer supports it;
					// the browser dialer falls back to legacy.
					// dseg segment pulls require HTTP/2 or HTTP/3 (multiplexed
					// connection): under HTTP/1.1 every segment pull would open a
					// fresh TCP+TLS handshake — a performance catastrophe AND a
					// glaring traffic shape (N short connections per download vs
					// one multiplexed session). Only the native dialer + H2/H3
					// may use it; H1/plaintext falls back to the legacy long-GET
					// download leg. This is the "H2/H3 gate" for segment mode.
					if transportConfiguration.downsegEnabled() {
						if dc, ok := httpClient2.(*DefaultDialerClient); ok && (dc.httpVersion == "2" || dc.httpVersion == "3") {
							// First a segment pull to enter the session into
							// segment mode (result irrelevant: empty/404 fine).
							_, _ = dc.PullSegment(ctx, &requestURL2, sessionId, "0")
							var prodLeg io.ReadCloser
							prodLeg, oerr = dc.OpenStreamAsync(ctx, &requestURL2, sessionId, nil, false, func(r, l net.Addr) {
								conn.remoteAddr, conn.localAddr = r, l
							})
							if oerr != nil {
								return false, false, oerr
							}
							conn.reader = NewDownSegPuller(ctx, dc, &requestURL2, sessionId, prodLeg)
						} else {
							conn.reader, oerr = httpClient2.OpenStreamAsync(ctx, &requestURL2, sessionId, nil, false, func(r, l net.Addr) {
								conn.remoteAddr, conn.localAddr = r, l
							})
						}
					} else {
						// B6: open the download leg asynchronously — packet-up's
						// upload loop can start immediately, saving one RTT on
						// TTFB. The future reader resolves (and fills the conn
						// addresses) on first Read; a failed GET surfaces as a
						// read error there, cascading into the normal teardown.
						conn.reader, oerr = httpClient2.OpenStreamAsync(ctx, &requestURL2, sessionId, nil, false, func(r, l net.Addr) {
							conn.remoteAddr, conn.localAddr = r, l
						})
					}
				} else {
					conn.reader, conn.remoteAddr, conn.localAddr, oerr = httpClient2.OpenStream(ctx, &requestURL2, sessionId, nil, false)
				}
				if oerr != nil {
					return false, false, oerr
				}
				if xmuxClient2 != nil {
					xmuxClient2.NoteOpenSuccess()
				} else if xmuxClient != nil {
					xmuxClient.NoteOpenSuccess()
				}
			}

			if mode == "stream-up" {
				// Green-zone: do not burn download LeftRequests until upload also succeeds.
				// Half-open (download OK, upload fail) previously leaked quota and could
				// exhaust XMUX MaxRequests during cascade retries.
				var upErr error
				_, _, _, upErr = httpClient.OpenStream(ctx, &requestURL, sessionId, reader, true)
				if upErr != nil {
					return false, false, upErr
				}
				if xmuxClient2 != nil {
					xmuxClient2.LeftRequests.Add(-1)
				}
				if xmuxClient != nil {
					xmuxClient.LeftRequests.Add(-1)
				}
				return true, false, nil
			}

			// packet-up / other non-stream-up: download open succeeded; count download quota.
			// Primary LeftRequests still decremented once on packet path after tryMode.
			if xmuxClient2 != nil {
				xmuxClient2.LeftRequests.Add(-1)
			}
			return false, true, nil
		}

		established, contPacket, openErr := tryMode()
		if openErr != nil {
			cleanup()
			lastErr = openErr
			// Header-wait timeout: accumulate toward MarkDead so a blackholed H2
			// (GotConn reused, headers never arrive) is rotated out of the pool.
			if openErr == context.DeadlineExceeded || stderrors.Is(openErr, context.DeadlineExceeded) {
				if xmuxClient != nil {
					xmuxClient.NoteOpenHeaderTimeout()
				}
				if xmuxClient2 != nil && xmuxClient2 != xmuxClient {
					xmuxClient2.NoteOpenHeaderTimeout()
				}
			}
			// Sticky: clear affinity when the sticky mode itself fails (Wave-7).
			if allowModeDegrade && StickyModeEnabled(transportConfiguration.Headers) {
				NoteStickyModeFailure(stickyKey, mode)
			}
			hasMoreModes := mi+1 < len(modeCascade) && IsDegradeEligibleError(openErr)
			// Fatal open + more modes: MarkDead and re-obtain clients so cascade
			// does not Borrow a dead XMUX session (Wave-7 P1).
			if ShouldRefreshXmuxBeforeCascade(openErr, hasMoreModes) {
				MaybeEvictXmuxAfterOpenFailure(xmuxClient, openErr)
				if xmuxClient2 != nil && xmuxClient2 != xmuxClient {
					MaybeEvictXmuxAfterOpenFailure(xmuxClient2, openErr)
				}
				if newHTTP, newXmux, gerr := getHTTPClient(ctx, dest, streamSettings); gerr == nil {
					httpClient, xmuxClient = newHTTP, newXmux
				} else {
					errors.LogInfoInner(ctx, gerr, "XHTTP cascade: failed to refresh primary XMUX after fatal open")
				}
				if hasDownload {
					memory2 := streamSettings.DownloadSettings
					if newHTTP2, newXmux2, gerr2 := getHTTPClient(ctx, dest2, memory2); gerr2 == nil {
						httpClient2, xmuxClient2 = newHTTP2, newXmux2
					} else {
						errors.LogInfoInner(ctx, gerr2, "XHTTP cascade: failed to refresh download XMUX after fatal open")
					}
				} else {
					httpClient2, xmuxClient2 = httpClient, xmuxClient
				}
			} else if !hasMoreModes {
				// Terminal failure: still drop broken sessions for the next Dial.
				MaybeEvictXmuxAfterOpenFailure(xmuxClient, openErr)
				if xmuxClient2 != nil && xmuxClient2 != xmuxClient {
					MaybeEvictXmuxAfterOpenFailure(xmuxClient2, openErr)
				}
			}
			// Non-fatal CDN mode rejects: keep client; cascade reuses same transport.
			if hasMoreModes {
				recordModeCascadeStep()
				errors.LogInfoInner(ctx, openErr, "XHTTP mode ", mode, " open failed; cascading to ", modeCascade[mi+1])
				// Green-zone: small inter-step jitter only on failed cascade path.
				// Preserve original openErr when cancel/deadline aborts the wait.
				if werr := WaitCascadeStepJitter(ctx); werr != nil {
					return nil, stderrors.Join(openErr, werr)
				}
				continue
			}
			return nil, openErr
		}
		if established {
			recordModeSuccess(mi > 0)
			if allowModeDegrade && StickyModeEnabled(transportConfiguration.Headers) {
				RememberStickyModeTTL(stickyKey, mode, stickyModeTTL)
			}
			return stat.Connection(&conn), nil
		}
		if !contPacket {
			cleanup()
			return nil, errors.New("XHTTP: unexpected mode open state")
		}

		// ---- packet-up path (success path for this attempt) ----
		scMaxEachPostBytes := transportConfiguration.GetNormalizedScMaxEachPostBytes()
		scMinPostsIntervalMs := transportConfiguration.GetNormalizedScMinPostsIntervalMs()

		// Validate config before burning XMUX LeftRequests (P2 review fix).
		if scMaxEachPostBytes.From <= 0 {
			cleanup()
			return nil, errors.New("`scMaxEachPostBytes` should be bigger than 0")
		}

		// Decrement LeftRequests once per Dial call (all modes).
		// stream-one/stream-down/stream-up: decremented above.
		// packet-up: decremented here.
		if xmuxClient != nil {
			xmuxClient.LeftRequests.Add(-1)
		}

		// Seed RTT once for both chunk size and in-flight window.
		// rtt==0 => keep configured post size + default window (cold start safe).
		var seedRTT time.Duration
		if xmuxClient != nil {
			seedRTT = xmuxClient.GetRTT()
		}
		configuredMax := scMaxEachPostBytes.rand()
		// Bray-only: RTT-aware chunk, hard-capped by scMaxEachPostBytes (server max).
		maxUploadSize := packetUploadChunkSize(configuredMax, seedRTT)
		uploadPipeReader, uploadPipeWriter := pipe.New(pipe.WithSizeLimit(max(0, maxUploadSize-buf.Size)))

		// Pre-compute URL string once to avoid per-packet allocation in upload loop.
		requestURLStr := requestURL.String()

		conn.writer = uploadWriter{
			uploadPipeWriter,
			maxUploadSize,
		}

		// Reliable packet-up: limited in-flight window (RTT-scaled, half-buffer capped).
		// Seq is assigned at launch so concurrent POSTs stay contiguous; each
		// POST still retries the same seq on transient failure. Server
		// upload_queue reorders by seq (scMaxBufferedPosts).
		uploadWindow := packetUploadWindow(transportConfiguration.GetNormalizedScMaxBufferedPosts(), seedRTT)
		go func() {
			var seq int64
			var lastWrite time.Time
			var clientMu sync.Mutex
			dynamicHTTPClient := httpClient
			dynamicXmuxClient := xmuxClient

			slots := make(chan struct{}, uploadWindow)
			var inflight sync.WaitGroup
			var failOnce sync.Once
			var uploadFailed atomic.Bool

			failUpload := func(err error, seqStr string) {
				failOnce.Do(func() {
					uploadFailed.Store(true)
					if err != nil {
						errors.LogInfoInner(ctx, err, "failed to send upload seq=", seqStr)
					}
					uploadPipeReader.Interrupt()
				})
			}

			waitInflight := func() { inflight.Wait() }
			defer waitInflight()

			// L4 failure granularity: rescue closure for postPacketReliable.
			// When the retry budget on the current outer connection is
			// exhausted, dial a FRESH XMUX client (same dest/config) and let
			// the seq replay there. Only if that also fails does the upload
			// session die — one bad H2/TCP connection no longer resets every
			// inner stream on the session.
			rescueClient := func(rctx context.Context) (DialerClient, error) {
				newHTTP, newXmux, err := getHTTPClient(rctx, dest, streamSettings)
				if err != nil {
					return nil, err
				}
				if newXmux == nil {
					return newHTTP, nil // non-XMUX transport: HTTP client alone is the fresh leg
				}
				if !newXmux.Borrow() {
					return nil, errors.New("XMUX: rescue borrow failed")
				}
				clientMu.Lock()
				prev := ownedUploadXmux
				ownedUploadXmux = newXmux
				clientMu.Unlock()
				if prev != nil {
					prev.Release()
				}
				if newHTTP != nil {
					dynamicHTTPClient = newHTTP
				}
				dynamicXmuxClient = newXmux
				return newHTTP, nil
			}

			for {
				if uploadFailed.Load() || ctx.Err() != nil {
					break
				}
				// Batch conn.Write into larger POSTs via the buffered pipe.
				remainder, err := uploadPipeReader.ReadMultiBuffer()
				if err != nil {
					break
				}

				for !remainder.IsEmpty() {
					if uploadFailed.Load() || ctx.Err() != nil {
						buf.ReleaseMulti(remainder)
						remainder = nil
						break
					}
					// L3a: small-packet flows (game heartbeats, DNS — avg
					// payload < 256B) cap the chunk at the minimum so each
					// POST flushes promptly and the server reorder buffer
					// sees tighter seq spacing; large-packet flows keep
					// the RTT-adaptive size. Pacing interval is untouched
					// (camouflage surface preserved).
					effMax := maxUploadSize
					if n := chunkAvgPacketSize(remainder); n > 0 && n < 256 {
						if effMax > packetUploadChunkMin {
							effMax = packetUploadChunkMin
						}
					}
					var chunk buf.MultiBuffer
					remainder, chunk = buf.SplitSize(remainder, effMax)
					if chunk.IsEmpty() {
						break
					}

					// Adaptive launch pacing: keep configured interval for
					// small/idle posts (camouflage); skip when more data is
					// already queued or this chunk is full-size so the window
					// can fill and hide RTT.
					configuredMs := int32(0)
					if scMinPostsIntervalMs.From > 0 {
						configuredMs = scMinPostsIntervalMs.rand()
					}
					hasBacklog := !remainder.IsEmpty()
					fullChunk := chunk.Len() >= maxUploadSize
					bulkChunk := chunk.Len() >= packetUploadBulkPaceBytes
					// Continuity: if we launched recently, treat as active flow even
					// for sub-bulk posts (e.g. many 1-4KiB writes in a tunnel burst).
					recentFlow := !lastWrite.IsZero() && time.Since(lastWrite) < 50*time.Millisecond
					paceMs := packetUploadLaunchIntervalMs(configuredMs, hasBacklog, fullChunk, bulkChunk, recentFlow)
					if paceMs > 0 {
						sleepDur := time.Duration(paceMs)*time.Millisecond - time.Since(lastWrite)
						if sleepDur > 0 {
							select {
							case <-time.After(sleepDur):
							case <-ctx.Done():
								buf.ReleaseMulti(chunk)
								buf.ReleaseMulti(remainder)
								return
							}
						}
					}
					lastWrite = time.Now()

					select {
					case slots <- struct{}{}:
					case <-ctx.Done():
						buf.ReleaseMulti(chunk)
						buf.ReleaseMulti(remainder)
						return
					}
					if uploadFailed.Load() {
						<-slots
						buf.ReleaseMulti(chunk)
						buf.ReleaseMulti(remainder)
						return
					}

					clientMu.Lock()
					if dynamicXmuxClient != nil && (dynamicXmuxClient.LeftRequests.Load() <= 0 ||
						(dynamicXmuxClient.UnreusableAt != time.Time{} && lastWrite.After(dynamicXmuxClient.UnreusableAt))) {
						oldClient := dynamicXmuxClient
						newHTTP, newXmux, err := getHTTPClient(ctx, dest, streamSettings)
						if err != nil {
							errors.LogInfo(ctx, "XMUX: failed to renew upload client, keeping old: ", err)
						} else if newXmux != nil && newXmux != oldClient {
							if newXmux.Borrow() {
								dynamicHTTPClient, dynamicXmuxClient = newHTTP, newXmux
								// Atomically swap ownership so onClose Releases exactly one slot.
								ownedUploadMu.Lock()
								prev := ownedUploadXmux
								if closed.Load() > 0 {
									// Conn already closed: drop the newly borrowed slot.
									ownedUploadMu.Unlock()
									newXmux.Release()
								} else {
									ownedUploadXmux = newXmux
									ownedUploadMu.Unlock()
									if prev != nil {
										prev.Release()
									}
								}
							}
							// Borrow failed: keep using oldClient.
						} else if newHTTP != nil {
							dynamicHTTPClient, dynamicXmuxClient = newHTTP, newXmux
						}
					}
					client := dynamicHTTPClient
					clientMu.Unlock()

					// Assign seq at launch (not after success) so concurrent
					// POSTs use contiguous numbers; retries reuse this seqStr.
					seqStr := formatSeqInt64(seq)
					seq++

					inflight.Add(1)
					go func(client DialerClient, seqStr string, chunk buf.MultiBuffer) {
						defer inflight.Done()
						defer func() { <-slots }()
						// postPacketReliable takes ownership of chunk (snapshot + release).
						if err := postPacketReliable(ctx, client, requestURLStr, sessionId, seqStr, chunk, rescueClient); err != nil {
							failUpload(err, seqStr)
						}
					}(client, seqStr, chunk)
				}
			}
		}()

		recordModeSuccess(mi > 0)
		if allowModeDegrade && StickyModeEnabled(transportConfiguration.Headers) {
			RememberStickyModeTTL(stickyKey, mode, stickyModeTTL)
		}
		return stat.Connection(&conn), nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("XHTTP: mode cascade exhausted")
}

// A wrapper around pipe that ensures the size limit is exactly honored.
//
// The MultiBuffer pipe accepts any single WriteMultiBuffer call even if that
// single MultiBuffer exceeds the size limit, and then starts blocking on the
// next WriteMultiBuffer call. This means that ReadMultiBuffer can return more
// bytes than the size limit. We work around this by splitting a potentially
// too large write up into multiple.
type uploadWriter struct {
	*pipe.Writer
	maxLen int32
}

func (w uploadWriter) Write(b []byte) (int, error) {
	/*
		capacity := int(w.maxLen - w.Len())
		if capacity > 0 && capacity < len(b) {
			b = b[:capacity]
		}
	*/

	// Split into pooled Buffers, then hand each one off to the pipe.
	// WriteMultiBuffer takes ownership, so never touch a Buffer after the
	// handoff (Len after release races packet-up postPacketReliable).
	//
	// Prefer a single NewWithSize buffer when the write fits one allocation
	// (common: app write <= scMaxEachPostBytes). Avoids MergeBytes' multi
	// 8KiB chunk path and the extra MultiBuffer slice growth for small posts.
	if len(b) == 0 {
		return 0, nil
	}
	if len(b) <= buf.Size {
		// Single standard buffer: zero intermediate MultiBuffer growth.
		chunk := buf.New()
		n, _ := chunk.Write(b)
		if err := w.WriteMultiBuffer(buf.MultiBuffer{chunk}); err != nil {
			return 0, err
		}
		return n, nil
	}
	// Larger than one Buffer: use size-class pool when possible for fewer pages.
	if len(b) <= int(w.maxLen) || w.maxLen <= 0 {
		chunk := buf.NewWithSize(int32(len(b)))
		n, _ := chunk.Write(b)
		if err := w.WriteMultiBuffer(buf.MultiBuffer{chunk}); err != nil {
			return 0, err
		}
		return n, nil
	}
	mb := buf.MergeBytes(nil, b)
	written := 0
	for len(mb) > 0 {
		chunk := mb[0]
		mb = mb[1:]
		n := int(chunk.Len())
		if err := w.WriteMultiBuffer(buf.MultiBuffer{chunk}); err != nil {
			if len(mb) > 0 {
				buf.ReleaseMulti(mb)
			}
			return written, err
		}
		written += n
	}
	return written, nil
}
