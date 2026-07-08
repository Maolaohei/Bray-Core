package splithttp

import (
	"context"
	"crypto/sha256"
	gotls "crypto/tls"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"net/url"
	reflect "reflect"
	"runtime"
	"strconv"
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
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/browser_dialer"
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

// MuxKey is a structured connection pool key that uniquely identifies
// a connection pool by: destination + SNI + protocol + security + config fingerprint.
// This prevents cross-domain reuse and ensures correct TLS SNI binding.
type MuxKey struct {
	dest       net.Destination
	sni        string // explicit SNI (TLS ServerName > OriginalDomain > Domain)
	protocol   string // "xhttp", "grpc", "tcp", etc.
	security   string // "tls", "reality", "none"
	configHash [32]byte // SHA256 of security config
}

// newMuxKey builds a MuxKey from destination and stream settings.
func newMuxKey(dest net.Destination, streamSettings *internet.MemoryStreamConfig) MuxKey {
	tlsCfg := tls.ConfigFromStreamSettings(streamSettings)
	realityCfg := reality.ConfigFromStreamSettings(streamSettings)
	transportCfg, _ := streamSettings.ProtocolSettings.(*Config)

	// SNI priority: TLS ServerName > Reality ServerName > OriginalDomain > Domain
	sni := dest.OriginalDomain
	if tlsCfg != nil && tlsCfg.ServerName != "" {
		sni = tlsCfg.ServerName
	} else if realityCfg != nil && realityCfg.ServerName != "" {
		sni = realityCfg.ServerName
	} else if sni == "" {
		sni = dest.Address.Domain()
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
		dest:       net.Destination{Address: dest.Address, Port: dest.Port, Network: dest.Network},
		sni:        sni,
		protocol:   protocol,
		security:   security,
		configHash: configHash,
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

func getHTTPClient(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (DialerClient, *XmuxClient) {
	realityConfig := reality.ConfigFromStreamSettings(streamSettings)

	if browser_dialer.HasBrowserDialer() && realityConfig == nil {
		return &BrowserDialerClient{transportConfig: streamSettings.ProtocolSettings.(*Config)}, nil
	}

	globalDialerAccess.Lock()
	defer globalDialerAccess.Unlock()

	if globalDialerMap == nil {
		globalDialerMap = make(map[MuxKey]*XmuxManager)
		globalDialerQuit = make(chan struct{})
		globalDialerDone = make(chan struct{})
		go globalDialerCleanup(globalDialerDone)
	}

	// Build structured MuxKey for connection pool
	key := newMuxKey(dest, streamSettings)

	xmuxManager, found := globalDialerMap[key]

	if !found {
		transportConfig := streamSettings.ProtocolSettings.(*Config)
		var xmuxConfig XmuxConfig
		if transportConfig.Xmux != nil {
			xmuxConfig = *transportConfig.Xmux
		}

		xmuxManager = NewXmuxManager(xmuxConfig, func() XmuxConn {
			return createHTTPClient(dest, streamSettings)
		})

		// Build probe URL for real TCP/TLS dial trigger.
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
		if browser_dialer.HasBrowserDialer() && realityCfg == nil {
			if !(probeScheme == "http" && dest.Port == 80) && !(probeScheme == "https" && dest.Port == 443) {
				probeHost += ":" + dest.Port.String()
			}
		}
		xmuxManager.probeURL = probeScheme + "://" + probeHost + transportConfig.GetNormalizedPath()

		globalDialerMap[key] = xmuxManager
	}

	xmuxClient := xmuxManager.GetXmuxClient(ctx)
	client := xmuxClient.XmuxConn.(DialerClient)

	// Set RTT callback on DefaultDialerClient for RTT-aware scheduling
	if dc, ok := client.(*DefaultDialerClient); ok {
		// Atomic profile reference — set by onNewConn, read by onRTT
		var activeProfile atomic.Value // stores interface{ FeedRTT(time.Duration) }

		dc.SetOnRTT(func(rtt time.Duration) {
			xmuxClient.UpdateRTT(rtt)
			// Feed RTT to Profile collector (no-op on Linux, estimated on Windows)
			if p := activeProfile.Load(); p != nil {
				p.(interface{ FeedRTT(time.Duration) }).FeedRTT(rtt)
			}
		})

		// Wire up TransportProfile: raw TCP socket → Profile → UpdateQuality → scoreClient
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

	return client, xmuxClient
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

	dialContext := func(ctxInner context.Context) (net.Conn, error) {
		t0 := time.Now()
		rawConn, err := internet.DialSystem(ctxInner, dest, streamSettings.SocketSettings)
		tcpDur := time.Since(t0)
		if err != nil {
			errors.LogDebug(ctxInner, "XHTTP dial: TCP failed in ", tcpDur.Round(time.Millisecond), ": ", err)
			return nil, err
		}

		// Notify profiling: raw TCP socket before TLS/REALITY wrapping
		if dc.onNewConn != nil {
			dc.onNewConn(rawConn)
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
	dc.uploadRawPool = &sync.Pool{}
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
			// Linear interpolation from 10min (pool≤10) to 3min (pool≥100).
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
// Intended for test cleanup — call after each test that uses Dial.
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
	if browser_dialer.HasBrowserDialer() && realityConfig == nil {
		// For Browser Dialer's optimized IP and non-standard port
		if !(requestURL.Scheme == "http" && dest.Port == 80) && !(requestURL.Scheme == "https" && dest.Port == 443) {
			requestURL.Host += ":" + dest.Port.String()
		}
	}

	requestURL.Path = transportConfiguration.GetNormalizedPath()
	requestURL.RawQuery = transportConfiguration.GetNormalizedQuery()

	httpClient, xmuxClient := getHTTPClient(ctx, dest, streamSettings)

	mode := transportConfiguration.Mode
	if mode == "" || mode == "auto" {
		mode = "packet-up"
		if realityConfig != nil {
			mode = "stream-one"
			if transportConfiguration.DownloadSettings != nil {
				mode = "stream-up"
			}
		}
	}

	sessionId := ""
	if mode != "stream-one" {
		sessionId = transportConfiguration.GenerateSessionID()
	}

	requestURL2 := requestURL
	httpClient2 := httpClient
	xmuxClient2 := xmuxClient
	if transportConfiguration.DownloadSettings != nil {
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
		dest2 := *memory2.Destination
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
		if browser_dialer.HasBrowserDialer() && realityConfig2 == nil {
			// For Browser Dialer's optimized IP and non-standard port
			if !(requestURL2.Scheme == "http" && dest2.Port == 80) && !(requestURL2.Scheme == "https" && dest2.Port == 443) {
				requestURL2.Host += ":" + dest2.Port.String()
			}
		}
		requestURL2.Path = config2.GetNormalizedPath()
		requestURL2.RawQuery = config2.GetNormalizedQuery()
		httpClient2, xmuxClient2 = getHTTPClient(ctx, dest2, memory2)
	}

	if xmuxClient != nil {
		xmuxClient.Borrow()
	}
	if xmuxClient2 != nil && xmuxClient2 != xmuxClient {
		xmuxClient2.Borrow()
	}
	var closed atomic.Int32

	reader, writer := io.Pipe()
	conn := splitConn{
		writer: writer,
		onClose: func() {
			if closed.Add(1) > 1 {
				return
			}
			if xmuxClient != nil {
				xmuxClient.Release()
			}
			if xmuxClient2 != nil && xmuxClient2 != xmuxClient {
				xmuxClient2.Release()
			}
		},
	}

	var err error
	if mode == "stream-one" {
		requestURL.Path = transportConfiguration.GetNormalizedPath()
		if xmuxClient != nil {
			xmuxClient.LeftRequests.Add(-1)
		}
		conn.reader, conn.remoteAddr, conn.localAddr, err = httpClient.OpenStream(ctx, requestURL.String(), sessionId, reader, false)
		if err != nil { // browser dialer only
			reader.Close()
			return nil, err
		}
		return stat.Connection(&conn), nil
	} else { // stream-down
		if xmuxClient2 != nil {
			xmuxClient2.LeftRequests.Add(-1)
		}
		conn.reader, conn.remoteAddr, conn.localAddr, err = httpClient2.OpenStream(ctx, requestURL2.String(), sessionId, nil, false)
		if err != nil { // browser dialer only
			reader.Close()
			return nil, err
		}
	}
	if mode == "stream-up" {
		if xmuxClient != nil {
			xmuxClient.LeftRequests.Add(-1)
		}
		_, _, _, err = httpClient.OpenStream(ctx, requestURL.String(), sessionId, reader, true)
		if err != nil { // browser dialer only
			reader.Close()
			conn.Close()
			return nil, err
		}
		return stat.Connection(&conn), nil
	}

	scMaxEachPostBytes := transportConfiguration.GetNormalizedScMaxEachPostBytes()
	scMinPostsIntervalMs := transportConfiguration.GetNormalizedScMinPostsIntervalMs()

	if scMaxEachPostBytes.From <= 0 {
		return nil, errors.New("`scMaxEachPostBytes` should be bigger than 0")
	}

	// Decrement LeftRequests once per Dial call (all modes).
	// stream-one/stream-down/stream-up: decremented above.
	// packet-up: decremented here.
	if xmuxClient != nil {
		xmuxClient.LeftRequests.Add(-1)
	}

	maxUploadSize := scMaxEachPostBytes.rand()
	// WithSizeLimit(0) will still allow single bytes to pass, and a lot of
	// code relies on this behavior. Subtract 1 so that together with
	// uploadWriter wrapper, exact size limits can be enforced
	// uploadPipeReader, uploadPipeWriter := pipe.New(pipe.WithSizeLimit(maxUploadSize - 1))
	uploadPipeReader, uploadPipeWriter := pipe.New(pipe.WithSizeLimit(max(0, maxUploadSize-buf.Size)))

	// Pre-compute URL string once to avoid per-packet allocation in upload loop.
	requestURLStr := requestURL.String()

	conn.writer = uploadWriter{
		uploadPipeWriter,
		maxUploadSize,
	}

	go func() {
		var seq int64
		var lastWrite time.Time

		dynamicHTTPClient := httpClient
		dynamicXmuxClient := xmuxClient
		for {
			// by offloading the uploads into a buffered pipe, multiple conn.Write
			// calls get automatically batched together into larger POST requests.
			// without batching, bandwidth is extremely limited.
			remainder, err := uploadPipeReader.ReadMultiBuffer()
			if err != nil {
				break
			}

			doSplit := atomic.Bool{}
			for doSplit.Store(true); doSplit.Load(); {
				var chunk buf.MultiBuffer
				remainder, chunk = buf.SplitSize(remainder, maxUploadSize)
				if chunk.IsEmpty() {
					break
				}

				wroteRequest := done.New()

				ctx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
					WroteRequest: func(httptrace.WroteRequestInfo) {
						wroteRequest.Close()
					},
				})

				seqStr := strconv.FormatInt(seq, 10)
				seq += 1

				if scMinPostsIntervalMs.From > 0 {
					sleepDur := time.Duration(scMinPostsIntervalMs.rand())*time.Millisecond - time.Since(lastWrite)
					if sleepDur > 0 {
						select {
						case <-time.After(sleepDur):
						case <-ctx.Done():
							return
						}
					}
				}

				lastWrite = time.Now()

				if dynamicXmuxClient != nil && (dynamicXmuxClient.LeftRequests.Load() <= 0 ||
					(dynamicXmuxClient.UnreusableAt != time.Time{} && lastWrite.After(dynamicXmuxClient.UnreusableAt))) {
					oldClient := dynamicXmuxClient
					dynamicHTTPClient, dynamicXmuxClient = getHTTPClient(ctx, dest, streamSettings)
					if oldClient != nil && oldClient != dynamicXmuxClient {
						oldClient.StopProfiling()
					}
				}

				go func(hClient DialerClient) {
					err := hClient.PostPacket(
						ctx,
						requestURLStr,
						sessionId,
						seqStr,
						chunk,
					)
					wroteRequest.Close()
					if err != nil {
						errors.LogInfoInner(ctx, err, "failed to send upload")
						uploadPipeReader.Interrupt()
						doSplit.Store(false)
					}
				}(dynamicHTTPClient)

				if _, ok := dynamicHTTPClient.(*DefaultDialerClient); ok {
					<-wroteRequest.Wait()
				}
			}
		}
	}()

	return stat.Connection(&conn), nil
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

	buffer := buf.MultiBufferContainer{}
	common.Must2(buffer.Write(b))

	var writed int
	for _, buff := range buffer.MultiBuffer {
		err := w.WriteMultiBuffer(buf.MultiBuffer{buff})
		if err != nil {
			return writed, err
		}
		writed += int(buff.Len())
	}
	return writed, nil
}
