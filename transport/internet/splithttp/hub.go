package splithttp

import (
	"context"
	gotls "crypto/tls"
	"encoding/base64"
	"io"
	"math"
	stdnet "net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goreality "github.com/Maolaohei/REALITY"
	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/bytespool"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	http_proto "github.com/xtls/xray-core/common/protocol/http"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
	"hash/fnv"
)

type requestHandler struct {
	config   *Config
	host     string
	path     string
	ln       *Listener
	sessions sync.Map
	// sessionLocks shard the upsert slow path by sessionId hash (M3):
	// different sessions no longer serialize on one mutex.
	sessionLocks    [16]sync.Mutex
	sessionN        atomic.Int64 // O(1) live session count (Bray-only)
	streamOneActive atomic.Int64 // live unauthenticated stream-one long-polls
	localAddr       net.Addr
	socketSettings  *internet.SocketConfig
	stopCh          chan struct{}
	cfDetected      atomic.Bool
	avgRTTNs        atomic.Int64        // EWMA of handler service time (ns) for adaptive session TTL
	macVerifier     *sessionMacVerifier // per-listener session MAC verifier with pooled HMAC instances
}

// sessionLock returns the shard mutex for a sessionId (M3).
func (h *requestHandler) sessionLock(sessionId string) *sync.Mutex {
	hsh := fnv.New32a()
	hsh.Write([]byte(sessionId))
	return &h.sessionLocks[hsh.Sum32()%uint32(len(h.sessionLocks))]
}

type httpSession struct {
	uploadQueue *uploadQueue
	// isFullyConnected is created lazily (H1): half-open sessions never
	// pay the ~96B hchan until a GET actually connects the session.
	isFullyConnected atomic.Pointer[done.Instance]
	// downloadLegs counts active stream-down download legs sharing this
	// session. A session may legitimately have several concurrent GET
	// download legs (multi-connection download sharing, e.g. downlink
	// segmentation): the session must live until the LAST leg finishes,
	// not fall to the first-returning defer as before (which tore it down
	// and killed the other legs).
	downloadLegs atomic.Int64
	// Downlink segmentation state (M1, Bray-paired). When downsegMode is
	// on, the session's downlink producer (httpServerConn.Write) writes
	// bytes into downseg instead of an HTTP response, and the client pulls
	// finalized segments with GET+seq. Legacy long-GET sessions keep
	// downsegMode off and stream directly (unchanged).
	downsegMode atomic.Int32
	downseg     atomic.Pointer[downSegCache]
	downsegOnce sync.Once
	// expiresAt is the unix-nano deadline for half-open sessions. Updated
	// on every upsert (atomic, lock-free); the single session sweeper
	// reaps expired entries. Fully-connected sessions are never reaped
	// (checked via isFullyConnected). Replaces the per-session timer +
	// goroutine (M1) and removes the post-connect timer Reset no-op (L3).
	expiresAt atomic.Int64
	remoteIP  string // remote IP that created this session (port ignored)
	closeOnce sync.Once
}

// fullyConnected returns the done.Instance, creating it on first use
// (H1 lazy upgrade). The sweeper must use the non-creating
// peekFullyConnected instead, so sweeping a half-open session does not
// allocate.
func (s *httpSession) fullyConnected() *done.Instance {
	if p := s.isFullyConnected.Load(); p != nil {
		return p
	}
	d := done.New()
	if s.isFullyConnected.CompareAndSwap(nil, d) {
		return d
	}
	return s.isFullyConnected.Load()
}

// peekFullyConnected returns the done.Instance without creating it.
func (s *httpSession) peekFullyConnected() *done.Instance {
	return s.isFullyConnected.Load()
}

// enterDownsegMode turns on downlink segmentation for this session (once),
// creating the segment cache. Returns whether the session is in segment mode
// after the call.
func (s *httpSession) enterDownsegMode() bool {
	s.downsegOnce.Do(func() {
		s.downseg.Store(newDownSegCache())
		s.downsegMode.Store(1)
	})
	return s.downsegMode.Load() == 1
}

// downsegAppend feeds downlink bytes into the segment cache. Only valid in
// segment mode (called from the downlink producer).
func (s *httpSession) downsegAppend(b []byte) {
	if c := s.downseg.Load(); c != nil {
		c.append(b)
	}
}

// downsegFinalize marks the downlink stream complete (EOF): finalizes the
// in-flight segment so the last segment becomes pullable.
func (s *httpSession) downsegFinalize() {
	if c := s.downseg.Load(); c != nil {
		c.finalize()
	}
}

// chunkSlicePool reuses the per-POST header/cookie chunk slices (L4).
// Pointer-pooled so the recycle path never boxes.
var chunkSlicePool = sync.Pool{
	New: func() any {
		s := make([]string, 0, 4)
		return &s
	},
}

// sessionSweepInterval bounds TTL precision; a half-open session may live
// up to one interval past its deadline (well under the 45-180s TTL scale).
const sessionSweepInterval = time.Second

// sessionSweeper is the single per-listener goroutine that reaps expired
// half-open sessions (M1: replaces one goroutine per session, removing the
// connection-establishment goroutine storm at scale).
func (h *requestHandler) sessionSweeper() {
	ticker := time.NewTicker(sessionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano()
			h.sessions.Range(func(key, value any) bool {
				s := value.(*httpSession)
				// Non-creating peek: sweeping must not allocate the
				// done.Instance (H1) — only fully-connected sessions
				// escape expiry.
				if d := s.peekFullyConnected(); d != nil {
					select {
					case <-d.Wait():
						return true // fully connected: no expiry
					default:
					}
				}
				if s.expiresAt.Load() <= now {
					h.deleteSession(key.(string), s)
					s.close()
				}
				return true
			})
		case <-h.stopCh:
			return
		}
	}
}

func (s *httpSession) close() {
	s.closeOnce.Do(func() { s.uploadQueue.Close() })
}

const maxSessionsPerHandler = 65536

// handleDownSegment serves a downlink-segment pull request (Bray-paired M1):
// reads finalized segment seq from the session's segment cache. The segment
// may still be in-flight (production in progress): poll briefly (up to
// downsegPullWait) for it to finalize, then answer 200+payload, 410 Gone
// (slid past) or 404 (not produced / stream over).
func (h *requestHandler) handleDownSegment(sess *httpSession, seqStr string, writer http.ResponseWriter) {
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	// Web-compliant no-cache so the segment is never stalely cached.
	writer.Header().Set("Cache-Control", "no-store")
	deadline := time.Now().Add(downsegPullWait)
	for {
		p, ok, gone := sess.downseg.Load().get(seq)
		if ok {
			if _, werr := writer.Write(p); werr == nil {
				if f, ok := writer.(http.Flusher); ok {
					f.Flush()
				}
			}
			return
		}
		if gone {
			writer.WriteHeader(http.StatusGone) // 410: slid past, client re-pull/abort
			return
		}
		if sess.downseg.Load().over() {
			// Stream finalized and the segment never appeared: end of data.
			// Signal EOF with an empty 200 body so the client can distinguish
			// it from a transient 404 (see PullSegment).
			return
		}
		if time.Now().After(deadline) {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// downsegPullWait bounds how long a segment pull waits for an in-flight
// segment to finalize before answering 404.
const downsegPullWait = 2 * time.Second

func (h *requestHandler) getSessionTtl() int32 {
	if h.cfDetected.Load() {
		return 75
	}
	base := h.config.GetNormalizedScSessionTtlSecs()
	// Stretch TTL when reverse-proxy / client path is slow so half-open
	// packet-up sessions are not reaped mid-burst.
	avg := time.Duration(h.avgRTTNs.Load())
	if avg >= 500*time.Millisecond {
		base += 30
	} else if avg >= 200*time.Millisecond {
		base += 15
	}
	if base > 180 {
		base = 180
	}
	return base
}

// updateAvgRTT updates the EWMA service-time estimate used for adaptive TTL.
func (h *requestHandler) updateAvgRTT(rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	newNs := int64(rtt)
	for {
		old := h.avgRTTNs.Load()
		var smoothed int64
		if old == 0 {
			smoothed = newNs
		} else {
			// EWMA: 80% old + 20% new
			smoothed = (old*8 + newNs*2) / 10
		}
		if h.avgRTTNs.CompareAndSwap(old, smoothed) {
			return
		}
	}
}

// sessionRemoteIP extracts host/IP without port so NAT source-port changes
// and XMUX multi-connection clients can keep the same logical session.
func sessionRemoteIP(remoteAddr string) string {
	if remoteAddr == "" {
		return ""
	}
	if host, _, err := stdnet.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}

func (h *requestHandler) sessionCount() int {
	n := h.sessionN.Load()
	if n < 0 {
		return 0
	}
	return int(n)
}

// deleteSession removes sessionId if still pointing at s and adjusts the counter.
func (h *requestHandler) deleteSession(sessionId string, s *httpSession) {
	if s == nil {
		return
	}
	if h.sessions.CompareAndDelete(sessionId, s) {
		h.sessionN.Add(-1)
	}
}

func (h *requestHandler) upsertSession(sessionId string, remoteAddr string) *httpSession {
	ttl := h.getSessionTtl()
	remoteIP := sessionRemoteIP(remoteAddr)

	// fast path
	currentSessionAny, ok := h.sessions.Load(sessionId)
	if ok {
		s := currentSessionAny.(*httpSession)
		// Bind by IP only. Port changes are normal under NAT / multi-conn XMUX.
		// Different IPs still force a replace to avoid cross-client session hijack.
		if s.remoteIP != "" && remoteIP != "" && s.remoteIP != remoteIP {
			errors.LogDebug(context.Background(),
				"XHTTP session reuse across different source IPs rejected",
			)
			// Fall through to slow path to create a new session.
		} else {
			s.expiresAt.Store(time.Now().Add(time.Duration(ttl) * time.Second).UnixNano())
			return s
		}
	}

	// slow path
	lock := h.sessionLock(sessionId)
	lock.Lock()
	defer lock.Unlock()

	currentSessionAny, ok = h.sessions.Load(sessionId)
	if ok {
		s := currentSessionAny.(*httpSession)
		if s.remoteIP != "" && remoteIP != "" && s.remoteIP != remoteIP {
			errors.LogDebug(context.Background(),
				"XHTTP session reuse across different source IPs rejected (slow path)",
			)
			// Old session belongs to a different client IP.
			// Close its uploadQueue and replace with a fresh session.
			h.deleteSession(sessionId, s)
			s.close()
		} else {
			s.expiresAt.Store(time.Now().Add(time.Duration(ttl) * time.Second).UnixNano())
			return s
		}
	}

	if h.sessionCount() >= maxSessionsPerHandler {
		// H3: before rejecting, reap the half-open session expiring
		// soonest — a probe/establishment flood recycles stale entries
		// instead of 503-ing real clients. Fully-connected sessions are
		// never reaped here (they have no expiry anyway). Full-table scan
		// only happens on this rare at-capacity path.
		var oldest *httpSession
		var oldestKey string
		oldestExp := int64(math.MaxInt64)
		h.sessions.Range(func(key, value any) bool {
			s := value.(*httpSession)
			if d := s.peekFullyConnected(); d != nil {
				select {
				case <-d.Wait():
					return true // fully connected: skip
				default:
				}
			}
			if e := s.expiresAt.Load(); e < oldestExp {
				oldestExp = e
				oldest = s
				oldestKey = key.(string)
			}
			return true
		})
		if oldest != nil {
			h.deleteSession(oldestKey, oldest)
			oldest.close()
		} else {
			errors.LogDebug(context.Background(),
				"XHTTP session limit reached and nothing to reap, max=", maxSessionsPerHandler,
			)
			return nil
		}
	}

	s := &httpSession{
		uploadQueue: NewUploadQueue(h.ln.config.GetNormalizedScMaxBufferedPosts()),
		remoteIP:    remoteIP,
	}
	s.expiresAt.Store(time.Now().Add(time.Duration(ttl) * time.Second).UnixNano())

	h.sessions.Store(sessionId, s)
	h.sessionN.Add(1)

	return s
}

// readBodyWithDeadline reads full body into buf with a hard read deadline via
// ResponseController.SetReadDeadline (HTTP/1 and HTTP/2 native support: the
// deadline is enforced by the connection/stream read path without spawning a
// goroutine or allocating a channel/context). When the underlying protocol
// does not support read deadlines (e.g. HTTP/3), it falls back to the
// goroutine-based readBodyWithTimeout so slow-body protection is never lost.
// The deadline is always cleared afterwards so it cannot leak into
// connection reuse.
func readBodyWithDeadline(writer http.ResponseWriter, request *http.Request, buf []byte, timeout time.Duration) (int, error) {
	if rc := http.NewResponseController(writer); rc != nil {
		if err := rc.SetReadDeadline(time.Now().Add(timeout)); err == nil {
			n, readErr := io.ReadFull(request.Body, buf)
			_ = rc.SetReadDeadline(time.Time{}) // clear: absolute deadline must not poison reuse
			return n, readErr
		}
	}
	return readBodyWithTimeout(request, buf, timeout)
}

// readBodyWithTimeout reads full body into buf with a hard deadline so a slow
// peer cannot pin a goroutine and a large allocation indefinitely.
func readBodyWithTimeout(request *http.Request, buf []byte, timeout time.Duration) (int, error) {
	readCtx, cancel := context.WithTimeout(request.Context(), timeout)
	defer cancel()
	type bodyResult struct {
		n   int
		err error
	}
	done := make(chan bodyResult, 1)
	go func() {
		n, err := io.ReadFull(request.Body, buf)
		done <- bodyResult{n, err}
	}()
	select {
	case r := <-done:
		return r.n, r.err
	case <-readCtx.Done():
		// The read goroutine is still blocked on request.Body (body reads do
		// not observe context cancellation). Abort the body so the goroutine
		// and its ≤ scMaxEachPostBytes buffer cannot linger indefinitely.
		if closer, ok := request.Body.(io.Closer); ok {
			_ = closer.Close()
		}
		return 0, readCtx.Err()
	}
}

func (h *requestHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	reqStart := time.Now()
	if len(h.host) > 0 && !internet.IsValidHTTPHost(request.Host, h.host) {
		errors.LogDebug(context.Background(), "failed to validate host")
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	if !strings.HasPrefix(request.URL.Path, h.path) {
		errors.LogDebug(context.Background(), "failed to validate path")
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	// Detect Cloudflare CDN once per listener. Only trust Cf-Ray (injected by
	// Cloudflare edge; plain clients never send it). The forgeable
	// "Server: cloudflare" request-header check was removed.
	if !h.cfDetected.Load() {
		if request.Header.Get("Cf-Ray") != "" {
			h.cfDetected.Store(true)
			errors.LogInfo(context.Background(), "Cloudflare CDN detected, session TTL set to 75s")
		}
	}

	h.config.WriteResponseHeader(writer, request.Method, request.Header)
	// Bray-only: default response X-Padding is a known Xray/XHTTP fingerprint.
	// Only stamp response padding when operator enabled obfs with custom placement.
	if h.config.XPaddingObfsMode {
		length := int(biasedRangeRand(h.config.GetNormalizedXPaddingBytes().From, h.config.GetNormalizedXPaddingBytes().To))
		config := XPaddingConfig{
			Length: length,
			Method: PaddingMethod(h.config.XPaddingMethod),
			Placement: XPaddingPlacement{
				Placement: h.config.XPaddingPlacement,
				Key:       h.config.XPaddingKey,
				Header:    h.config.XPaddingHeader,
			},
		}
		config.methodIdx = methodIndex(config.Method)
		h.config.ApplyXPaddingToResponse(writer, config)
	}

	if request.Method == "OPTIONS" {
		writer.WriteHeader(http.StatusOK)
		return
	}

	/*
		clientVer := []int{0, 0, 0}
		x_version := strings.Split(request.URL.Query().Get("x_version"), ".")
		for j := 0; j < 3 && len(x_version) > j; j++ {
			clientVer[j], _ = strconv.Atoi(x_version[j])
		}
	*/

	basePad := h.config.GetNormalizedXPaddingBytes()
	acceptFrom, acceptTo := AcceptedPaddingRange(basePad.From, basePad.To)
	paddingValue, _ := h.config.ExtractXPaddingFromRequest(request, h.config.XPaddingObfsMode)

	if !h.config.IsPaddingValid(paddingValue, acceptFrom, acceptTo, PaddingMethod(h.config.XPaddingMethod)) {
		errors.LogDebug(context.Background(), "invalid padding")
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	obfsPaddingAccepted := h.config.XPaddingObfsMode && paddingValue != ""

	sessionId, seqStr := h.config.ExtractMetaFromRequest(request, h.path)

	if len(sessionId) > 256 {
		errors.LogDebug(context.Background(), "sessionId too long")
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	// Bray-only: reject unsigned / forged session IDs before upsertSession.
	// Empty sessionId remains stream-one only (gated below).
	if sessionId != "" {
		macOK := false
		if h.macVerifier != nil {
			macOK = h.macVerifier.verify(sessionId)
		} else {
			macOK = verifySessionIDAny(sessionId, h.config.sessionSecrets())
		}
		if !macOK {
			errors.LogDebug(context.Background(), "invalid session MAC")
			writer.WriteHeader(http.StatusNotFound)
			return
		}
	}

	// Empty sessionId is stream-one shape only. Locked stream-up/packet-up reject it.
	// (auto/empty config still allow stream-one for compatibility.)
	if sessionId == "" && !ServerModeAllowsStreamOne(h.config.Mode) {
		errors.LogDebug(context.Background(), "sessionId required for mode")
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	var remoteAddr net.Addr
	var err error
	remoteAddr, err = net.ResolveTCPAddr("tcp", request.RemoteAddr)
	if err != nil {
		remoteAddr = &net.TCPAddr{
			IP:   []byte{0, 0, 0, 0},
			Port: 0,
		}
	}
	if request.ProtoMajor == 3 {
		if tcpAddr, ok := remoteAddr.(*net.TCPAddr); ok {
			remoteAddr = &net.UDPAddr{
				IP:   tcpAddr.IP,
				Port: tcpAddr.Port,
			}
		}
	}
	var trustedXFF []string
	if h.socketSettings != nil {
		trustedXFF = h.socketSettings.TrustedXForwardedFor
	}
	remoteAddr = http_proto.ApplyTrustedXForwardedFor(request.Header, trustedXFF, remoteAddr)

	var currentSession *httpSession
	if sessionId != "" {
		currentSession = h.upsertSession(sessionId, remoteAddr.String())
		if currentSession == nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
	scMaxEachPostBytes := int(h.ln.config.GetNormalizedScMaxEachPostBytes().To)
	isUplinkRequest := false

	switch request.Method {
	case "GET":
		isUplinkRequest = seqStr != ""
	default:
		isUplinkRequest = true
	}

	// Downlink segmentation (Bray-paired M1), marker-free:
	//   - a sessioned GET with seq is a segment pull (reuses the dead
	//     GET+seq path) and also enters the session into segment mode;
	//   - a sessioned GET without seq on an already-segment session is the
	//     production leg (httpServerConn.Write routes into the cache);
	//   - a sessioned GET without seq on a legacy session is the plain
	//     long-GET download leg (unchanged).
	// No extra header/label is sent on the wire: the segment pull differs
	// from a legacy GET only by its (tokenish) meta token carrying a seq,
	// which an observer cannot distinguish structurally.
	if request.Method == "GET" && sessionId != "" {
		if seqStr != "" {
			if !currentSession.enterDownsegMode() {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			h.handleDownSegment(currentSession, seqStr, writer)
			return
		}
		if currentSession.downsegMode.Load() == 1 {
			// Production leg: this sessioned GET is the downlink producer.
			// Build the splitConn and register it with the inbound (as the
			// legacy long-GET download leg does) so the upper layer's
			// dispatcher writes the proxied response through httpSC.Write,
			// which routes into the segment cache (segment mode). Close
			// finalizes the stream (EOF). Count as a download leg so the
			// session lives until this leg ends.
			currentSession.fullyConnected().Close()
			currentSession.downloadLegs.Add(1)
			defer func() {
				if currentSession.downloadLegs.Add(-1) == 0 {
					currentSession.close()
					h.deleteSession(sessionId, currentSession)
				}
			}()

			writer.WriteHeader(http.StatusOK)
			if f, ok := writer.(http.Flusher); ok {
				f.Flush()
			}
			httpSC := &httpServerConn{
				Instance:       done.New(),
				reader:         request.Body,
				ResponseWriter: writer,
				sess:           currentSession,
			}
			localAddr := h.localAddr
			if la, ok := request.Context().Value(http.LocalAddrContextKey).(net.Addr); ok && la != nil {
				localAddr = la
			}
			conn := splitConn{
				writer:     httpSC,
				reader:     currentSession.uploadQueue,
				remoteAddr: remoteAddr,
				localAddr:  localAddr,
			}
			h.ln.addConn(stat.Connection(&conn))
			defer conn.Close()
			select {
			case <-request.Context().Done():
			case <-httpSC.Wait():
			}
			return
		}
	}

	uplinkDataKey := h.config.UplinkDataKey

	if isUplinkRequest && sessionId != "" { // stream-up, packet-up
		if seqStr == "" {
			if !ServerModeAllowsStreamUp(h.config.Mode) {
				errors.LogDebug(context.Background(), "stream-up mode is not allowed")
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			httpSC := &httpServerConn{
				Instance:       done.New(),
				reader:         request.Body,
				ResponseWriter: writer,
			}
			err = currentSession.uploadQueue.Push(Packet{
				Reader: httpSC,
			})
			if err != nil {
				errors.LogInfoInner(context.Background(), err, "failed to upload (PushReader)")
				// Bray-only: do not leak stream-up conflict oracle (was 409).
				writer.WriteHeader(http.StatusNotFound)
			} else {
				// Bray-only: X-Accel-Buffering opt-in via local control header x-bray-x-accel=1.
				if h.config.Headers != nil {
					if v, ok := h.config.Headers["x-bray-x-accel"]; ok && (v == "1" || strings.EqualFold(v, "true")) {
						writer.Header().Set("X-Accel-Buffering", "no")
					}
				}
				writer.Header().Set("Cache-Control", "no-store")
				writer.WriteHeader(http.StatusOK)
				scStreamUpServerSecs := h.config.GetNormalizedScStreamUpServerSecs()
				// Bray-only: keep-alive padding on stream-up when client sent a
				// padding header from the pool (or legacy Referer still present
				// from mixed configs).
				hasPaddingMarker := hasBrayPaddingHeader(request) || request.Header.Get("Referer") != ""
				if (hasPaddingMarker || obfsPaddingAccepted) && scStreamUpServerSecs.To > 0 {
					go func() {
						bp := paddingBytePool.Get().(*[]byte)
						defer paddingBytePool.Put(bp)
						for {
							rb := h.config.GetNormalizedXPaddingBytes()
							n := int(biasedRangeRand(rb.From, rb.To))
							if n > len(*bp) {
								n = len(*bp)
							}
							_, err := httpSC.Write((*bp)[:n])
							if err != nil {
								break
							}
							sleepDur := time.Duration(biasedRangeRand(scStreamUpServerSecs.From, scStreamUpServerSecs.To)) * time.Second
							select {
							case <-time.After(sleepDur):
							case <-request.Context().Done():
								return
							}
						}
					}()
				}
				select {
				case <-request.Context().Done():
				case <-httpSC.Wait():
				}
			}
			httpSC.Close()
			return
		}

		if !ServerModeAllowsPacketUp(h.config.Mode) {
			errors.LogDebug(context.Background(), "packet-up mode is not allowed")
			writer.WriteHeader(http.StatusNotFound)
			return
		}

		dataPlacement := h.config.GetNormalizedUplinkDataPlacement()
		var headerPayload []byte
		// L4: reuse the per-POST chunk slices across requests (two
		// []string allocations per POST on the auto/header/cookie paths).
		sp := chunkSlicePool.Get().(*[]string)
		defer chunkSlicePool.Put(sp)
		if dataPlacement == PlacementAuto || dataPlacement == PlacementHeader {
			headerPayloadChunks := (*sp)[:0]
			for i := 0; true; i++ {
				chunk := request.Header.Get(uplinkDataKey + "-" + strconv.Itoa(i))
				if chunk == "" {
					break
				}
				headerPayloadChunks = append(headerPayloadChunks, chunk)
			}
			*sp = headerPayloadChunks
			headerPayloadEncoded := strings.Join(headerPayloadChunks, "")
			headerPayload, err = base64.RawURLEncoding.DecodeString(headerPayloadEncoded)
			if err != nil {
				errors.LogDebug(context.Background(), "invalid base64 in header payload")
				writer.WriteHeader(http.StatusNotFound)
				return
			}
		}

		var cookiePayload []byte
		if dataPlacement == PlacementAuto || dataPlacement == PlacementCookie {
			cookiePayloadChunks := (*sp)[:0]
			for i := 0; true; i++ {
				cookieName := uplinkDataKey + "_" + strconv.Itoa(i)
				if c, _ := request.Cookie(cookieName); c != nil {
					cookiePayloadChunks = append(cookiePayloadChunks, c.Value)
				} else {
					break
				}
			}
			*sp = cookiePayloadChunks
			cookiePayloadEncoded := strings.Join(cookiePayloadChunks, "")
			cookiePayload, err = base64.RawURLEncoding.DecodeString(cookiePayloadEncoded)
			if err != nil {
				errors.LogDebug(context.Background(), "invalid base64 in cookie payload")
				writer.WriteHeader(http.StatusNotFound)
				return
			}
		}

		var bodyPayload []byte
		bodyPooled := false // true when bodyPayload came from the postBodyPool
		if dataPlacement == PlacementAuto || dataPlacement == PlacementBody {
			var readErr error
			if request.ContentLength > int64(scMaxEachPostBytes) {
				errors.LogDebug(context.Background(), "upload exceeds scMaxEachPostBytes")
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			if request.ContentLength > 0 {
				bodyLen := int(request.ContentLength)
				// Allocate once for queue ownership (pooled; uploadQueue returns
				// it after consumption). Avoid pool+clone double copy on the
				// packet-up hot path (uploadQueue may retain the slice).
				bodyPayload = allocPostBody(bodyLen)
				bodyPooled = true
				// Slow-body guard: a peer may declare a large ContentLength and
				// drip bytes forever; cap the read with a deadline so a single
				// request cannot pin a goroutine + buffer indefinitely.
				_, readErr = readBodyWithDeadline(writer, request, bodyPayload, 30*time.Second)
			} else {
				// Chunked upload: bound both size and rate. ReadFull into a
				// fixed cap; io.ErrUnexpectedEOF merely means the body ended
				// before the cap (normal), not an error.
				bodyPayload = allocPostBody(scMaxEachPostBytes + 1)
				bodyPooled = true
				var n int
				n, readErr = readBodyWithDeadline(writer, request, bodyPayload, 30*time.Second)
				bodyPayload = bodyPayload[:n]
				if readErr == io.ErrUnexpectedEOF {
					readErr = nil
				}
			}
			if readErr != nil {
				if bodyPooled {
					freePostBody(bodyPayload)
				}
				errors.LogDebug(context.Background(), "failed to read body payload: ", readErr)
				writer.WriteHeader(http.StatusNotFound)
				return
			}
		}

		var payload []byte
		payloadPooled := false
		switch dataPlacement {
		case PlacementHeader:
			payload = headerPayload
		case PlacementCookie:
			payload = cookiePayload
		case PlacementBody:
			payload = bodyPayload
			payloadPooled = bodyPooled
		case PlacementAuto:
			totalLen := len(headerPayload) + len(cookiePayload) + len(bodyPayload)
			// Reject before allocating the merged buffer: a peer that drives
			// all three channels near scMaxEachPostBytes would otherwise force
			// a transient 3x+ peak allocation on every oversized POST.
			if totalLen > scMaxEachPostBytes {
				if bodyPooled {
					freePostBody(bodyPayload)
				}
				errors.LogDebug(context.Background(), "assembled payload exceeds scMaxEachPostBytes")
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			// Merged buffer is pooled; the body slice is consumed here and
			// returned immediately (its bytes were copied into payload).
			payload = allocPostBody(totalLen)
			payload = append(payload[:0], headerPayload...)
			payload = append(payload, cookiePayload...)
			payload = append(payload, bodyPayload...)
			if bodyPooled {
				freePostBody(bodyPayload)
			}
			payloadPooled = true
		}

		if len(payload) > scMaxEachPostBytes {
			if payloadPooled {
				freePostBody(payload)
			}
			errors.LogDebug(context.Background(), "assembled payload exceeds scMaxEachPostBytes")
			writer.WriteHeader(http.StatusNotFound)
			return
		}

		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			if payloadPooled {
				freePostBody(payload)
			}
			errors.LogDebug(context.Background(), "invalid packet-up seq")
			writer.WriteHeader(http.StatusNotFound)
			return
		}

		// payload is already uniquely owned (fresh decode / single body alloc /
		// PlacementAuto assemble). Do not clone again before enqueue.
		err = currentSession.uploadQueue.Push(Packet{
			Payload: payload,
			Seq:     seq,
			Pooled:  payloadPooled,
		})
		if err != nil {
			errors.LogDebug(context.Background(), "failed to upload (PushPayload)")
			// Bray-only: queue-full / closed -> 404, not 500 (no liveness oracle).
			writer.WriteHeader(http.StatusNotFound)
			return
		}

		if len(bodyPayload) == 0 {
			// Methods without a body are usually cached by default.
			writer.Header().Set("Cache-Control", "no-store")
		}

		h.updateAvgRTT(time.Since(reqStart))
		writer.WriteHeader(http.StatusOK)
	} else if request.Method == "GET" || sessionId == "" { // stream-down, stream-one
		// Locked stream-one: no sessioned download leg (that is stream-up/packet-up).
		if sessionId != "" && NormalizeXHTTPMode(h.config.Mode) == "stream-one" {
			errors.LogDebug(context.Background(), "sessioned download not allowed")
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		// Locked packet-up/stream-up without session already rejected above.
		// Locked packet-up with empty sessionId rejected by ServerModeAllowsStreamOne.
		if sessionId == "" {
			// Bray-only: unauthenticated stream-one long-polls are the biggest
			// probe/DoS surface (no MAC, no body bound). Cap concurrency and
			// lifetime per listener so fake streams cannot pin goroutines/fds.
			if n := h.streamOneActive.Add(1); n > streamOneMaxActive {
				h.streamOneActive.Add(-1)
				writer.WriteHeader(http.StatusTooManyRequests)
				return
			}
			defer h.streamOneActive.Add(-1)
		}
		if sessionId != "" {
			// A GET download leg is one of possibly several legs sharing
			// this session. Mark fully-connected (sweeper escapes it) and
			// account a leg; the session tears down only when the LAST leg
			// closes (referenced shared multi-GET download).
			currentSession.fullyConnected().Close()
			currentSession.downloadLegs.Add(1)
			defer func() {
				if currentSession.downloadLegs.Add(-1) == 0 {
					currentSession.close()
					h.deleteSession(sessionId, currentSession)
				}
			}()
		}

		// Bray-only: X-Accel-Buffering is a classic reverse-proxy fingerprint.
		// Only emit when operator opts in via local control header x-bray-x-accel=1.
		if h.config.Headers != nil {
			if v, ok := h.config.Headers["x-bray-x-accel"]; ok && (v == "1" || strings.EqualFold(v, "true")) {
				writer.Header().Set("X-Accel-Buffering", "no")
			}
		}
		// A web-compliant header telling all middleboxes to disable caching.
		// Should be able to prevent overloading the cache, or stop CDNs from
		// teeing the response stream into their cache, causing slowdowns.
		writer.Header().Set("Cache-Control", "no-store")

		// Bray-only: default text/event-stream is a classic XHTTP probe fingerprint.
		// Operators can force SSE with Headers path or by leaving NoSSEHeader=false
		// AND setting x-bray-sse=1 control header (never on wire; read from config.Headers).
		if !h.config.NoSSEHeader {
			if h.config.Headers != nil {
				if v, ok := h.config.Headers["x-bray-sse"]; ok && (v == "1" || strings.EqualFold(v, "true")) {
					writer.Header().Set("Content-Type", "text/event-stream")
				}
			}
		}

		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()

		httpSC := &httpServerConn{
			Instance:       done.New(),
			reader:         request.Body,
			ResponseWriter: writer,
			sess:           currentSession, // nil for stream-one; drives downseg split
		}
		localAddr := h.localAddr
		if la, ok := request.Context().Value(http.LocalAddrContextKey).(net.Addr); ok && la != nil {
			localAddr = la
		}
		conn := splitConn{
			writer:     httpSC,
			reader:     httpSC,
			remoteAddr: remoteAddr,
			localAddr:  localAddr,
		}
		if sessionId != "" { // if not stream-one
			conn.reader = currentSession.uploadQueue
		}

		if sessionId == "" {
			// Reap unauthenticated stream-one connections only when idle:
			// active transfers (long downloads, video playback) must never be
			// cut mid-stream. touch() resets the timer and the lastActive
			// stamp on every read/write; the callback double-checks the stamp
			// to close the Reset-vs-fire race at the deadline boundary.
			// A hard absolute cap (streamOneHardCapLifetime) still bounds
			// hostile long-lived connections regardless of activity.
			httpSC.lastActive.Store(time.Now().UnixNano())
			httpSC.hardCapTimer = time.AfterFunc(streamOneHardCapLifetime, func() {
				_ = httpSC.Close()
			})
			httpSC.idleTimer = time.AfterFunc(streamOneIdleLifetime, func() {
				if time.Since(time.Unix(0, httpSC.lastActive.Load())) < streamOneIdleLifetime {
					return // activity landed inside the race window; keep alive
				}
				_ = httpSC.Close()
			})
			defer func() {
				httpSC.hardCapTimer.Stop()
				httpSC.idleTimer.Stop()
			}()
		}

		h.ln.addConn(stat.Connection(&conn))

		// "A ResponseWriter may not be used after [Handler.ServeHTTP] has returned."
		select {
		case <-request.Context().Done():
		case <-httpSC.Wait():
		}

		conn.Close()
	} else {
		errors.LogDebug(context.Background(), "unsupported method")
		writer.WriteHeader(http.StatusNotFound)
	}
}

type httpServerConn struct {
	sync.Mutex
	*done.Instance
	reader io.Reader // request body (no need to Close)
	http.ResponseWriter
	// sess is the owning session; non-nil only for sessioned (packet-up /
	// stream-up) download legs. In segment mode the session's downlink
	// goes into its segment cache instead of this ResponseWriter.
	sess *httpSession
	// Downlink write aggregation: flushing every Write() forces one TCP
	// segment / H2 DATA frame per chunk, which inflates frame count on
	// high-chunk-rate tunnels. Bytes are buffered and flushed at a size
	// threshold or short interval instead. flushAt is zero when idle.
	writeBuf []byte
	flushAt  time.Time
	// idleTimer reaps idle unauthenticated stream-one long-polls; every
	// Read/Write activity resets it so live transfers are never cut.
	idleTimer *time.Timer
	// hardCapTimer bounds the absolute lifetime of an unauthenticated
	// stream-one connection regardless of activity (DoS backstop).
	hardCapTimer *time.Timer
	// lastActive is the last activity stamp (UnixNano) used to double-check
	// the reaper at its deadline boundary (Reset-vs-fire race).
	lastActive atomic.Int64
}

// streamFlushThreshold / streamFlushInterval bound downlink latency: a
// stream chunk is flushed within ~10ms even when far below the size threshold.
const (
	streamFlushThreshold = 16 << 10
	streamFlushInterval  = 10 * time.Millisecond
)

// streamOneMaxActive caps concurrent unauthenticated stream-one long-polls
// per listener (probe/DoS surface bound: no MAC, no body limit on that path).
const streamOneMaxActive = 2000

// streamOneIdleLifetime is how long an unauthenticated stream-one connection
// may sit idle before being reaped. Active transfers reset the timer on every
// read/write, so long downloads and video playback are never interrupted.
const streamOneIdleLifetime = 10 * time.Minute

// streamOneHardCapLifetime bounds the absolute lifetime of an unauthenticated
// stream-one connection even while active: an attacker feeding a byte every
// few minutes must still be reaped eventually (DoS backstop). Far beyond any
// real transfer, so genuine downloads/video are unaffected.
const streamOneHardCapLifetime = 4 * time.Hour

func (c *httpServerConn) Write(b []byte) (int, error) {
	// ServeHTTP may return while a handler goroutine is still writing
	// (echo/Copy helpers). Serialize Write+Flush against Close and the
	// HTTP server finishing the response, or Flush races ResponseWriter.
	c.Lock()
	defer c.Unlock()
	if c.Instance.Done() {
		return 0, io.ErrClosedPipe
	}
	c.touch() // write activity resets the stream-one idle reaper
	// Segment-mode session: feed downlink into the segment cache instead
	// of writing this (production-leg) HTTP response body. The client
	// pulls finalized segments with GET+seq.
	if c.sess != nil && c.sess.downsegMode.Load() == 1 {
		c.sess.downsegAppend(b)
		return len(b), nil
	}
	if len(b) == 0 {
		return 0, nil
	}
	if len(c.writeBuf) == 0 {
		// Isolated write (request/response or echo pattern): flush immediately
		// to keep first-byte latency. Aggregating here would add up to
		// streamFlushInterval of delay per round trip.
		n, err := c.ResponseWriter.Write(b)
		if err == nil {
			if f, ok := c.ResponseWriter.(http.Flusher); ok {
				f.Flush()
			}
		}
		return n, err
	}
	// Back-to-back writes: aggregate until the size threshold or short
	// interval, so high-chunk-rate tunnels do not emit one TCP segment /
	// H2 DATA frame per chunk.
	c.writeBuf = append(c.writeBuf, b...)
	now := time.Now()
	if len(c.writeBuf) >= streamFlushThreshold || now.Sub(c.flushAt) >= streamFlushInterval {
		if err := c.flushLocked(); err != nil {
			return 0, err
		}
		c.flushAt = time.Time{}
	} else if c.flushAt.IsZero() {
		c.flushAt = now
		// Guarantee delivery even when the writer goes quiet before the size
		// threshold: schedule a one-shot flush so buffered bytes are never
		// stranded (a passive observer would otherwise see a stall).
		time.AfterFunc(streamFlushInterval, c.flushPending)
	}
	return len(b), nil
}

// Read forwards the request body (upstream data) and resets the stream-one
// idle reaper on activity.
func (c *httpServerConn) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	if n > 0 {
		c.touch()
	}
	return n, err
}

// touch resets the stream-one idle reaper. Active transfers (downloads,
// video) never expire; only quiet long-polls are reaped. timer.Reset is
// concurrent-safe and the atomic stamp backs the deadline-boundary check.
func (c *httpServerConn) touch() {
	c.lastActive.Store(time.Now().UnixNano())
	if c.idleTimer != nil {
		c.idleTimer.Reset(streamOneIdleLifetime)
	}
}

// flushPending is the scheduled one-shot flush for quiet writers.
func (c *httpServerConn) flushPending() {
	c.Lock()
	defer c.Unlock()
	if c.Instance.Done() {
		return
	}
	if len(c.writeBuf) > 0 {
		_ = c.flushLocked()
		c.flushAt = time.Time{}
	}
}

func (c *httpServerConn) flushLocked() error {
	if len(c.writeBuf) == 0 {
		return nil
	}
	_, err := c.ResponseWriter.Write(c.writeBuf)
	c.writeBuf = c.writeBuf[:0] // keep capacity for the connection lifetime
	if err == nil {
		if f, ok := c.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
	return err
}

func (c *httpServerConn) Close() error {
	c.Lock()
	defer c.Unlock()
	segMode := c.sess != nil && c.sess.downsegMode.Load() == 1
	if !c.Instance.Done() {
		if segMode {
			// Downlink lives in the segment cache. EOF here finalizes the
			// in-flight segment so the last segment becomes pullable;
			// nothing goes to this leg's HTTP response body.
			if c.sess.downseg.Load() != nil {
				c.sess.downsegFinalize()
			}
		} else {
			// Deliver any buffered downlink bytes before the stream is torn down.
			_ = c.flushLocked()
		}
	}
	// M2: return the downlink aggregation buffer to the pool instead of
	// pinning up to streamFlushThreshold per connection for its lifetime.
	if cap(c.writeBuf) >= 2048 {
		bytespool.Free(c.writeBuf)
	}
	c.writeBuf = nil
	return c.Instance.Close()
}

type Listener struct {
	sync.Mutex
	server     http.Server
	h3server   *http3.Server
	listener   net.Listener
	h3listener http3.QUICListener
	config     *Config
	addConn    internet.ConnHandler
	handler    *requestHandler
	isH3       bool
}

func ListenXH(ctx context.Context, address net.Address, port net.Port, streamSettings *internet.MemoryStreamConfig, addConn internet.ConnHandler) (internet.Listener, error) {
	l := &Listener{
		addConn: addConn,
	}
	l.config = streamSettings.ProtocolSettings.(*Config)
	if l.config != nil {
		if streamSettings.SocketSettings == nil {
			streamSettings.SocketSettings = &internet.SocketConfig{}
		}
	}
	handler := &requestHandler{
		config: l.config,
		host:   l.config.Host,
		path:   l.config.GetNormalizedPath(),
		ln:     l,

		sessions:       sync.Map{},
		socketSettings: streamSettings.SocketSettings,
		stopCh:         make(chan struct{}),
		macVerifier:    newSessionMacVerifier(l.config.sessionSecrets()),
	}
	l.handler = handler
	// M1: single sweeper goroutine for all sessions (replaces one
	// goroutine per session). Terminates when the listener closes stopCh.
	go handler.sessionSweeper()
	tlsConfig := getTLSConfig(streamSettings)
	l.isH3 = len(tlsConfig.NextProtos) == 1 && tlsConfig.NextProtos[0] == "h3"

	var err error
	if port == net.Port(0) { // unix
		l.listener, err = internet.ListenSystem(ctx, &net.UnixAddr{
			Name: address.Domain(),
			Net:  "unix",
		}, streamSettings.SocketSettings)
		if err != nil {
			return nil, errors.New("failed to listen UNIX domain socket for XHTTP on ", address).Base(err)
		}
		errors.LogInfo(ctx, "listening UNIX domain socket for XHTTP on ", address)
	} else if l.isH3 { // quic
		Conn, err := internet.ListenSystemPacket(context.Background(), &net.UDPAddr{
			IP:   address.IP(),
			Port: int(port),
		}, streamSettings.SocketSettings)
		if err != nil {
			return nil, errors.New("failed to listen UDP for XHTTP/3 on ", address, ":", port).Base(err)
		}
		if streamSettings.UdpmaskManager != nil {
			newConn, err := streamSettings.UdpmaskManager.WrapPacketConnServer(Conn)
			if err != nil {
				Conn.Close()
				return nil, errors.New("mask err").Base(err)
			}
			Conn = newConn
		}

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
			MaxIncomingStreams:             quicParams.MaxIncomingStreams,
			DisablePathMTUDiscovery:        quicParams.DisablePathMtuDiscovery || (runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin"),
		}

		l.h3listener, err = quic.ListenEarly(Conn, tlsConfig, quicConfig)
		if err != nil {
			return nil, errors.New("failed to listen QUIC for XHTTP/3 on ", address, ":", port).Base(err)
		}
		l.h3listener = &QListener{
			QUICListener: l.h3listener,
			quicParams:   quicParams,
		}
		errors.LogInfo(ctx, "listening QUIC for XHTTP/3 on ", address, ":", port)

		handler.localAddr = l.h3listener.Addr()

		l.h3server = &http3.Server{
			Handler: handler,
		}
		go func() {
			if err := l.h3server.ServeListener(l.h3listener); err != nil {
				errors.LogErrorInner(ctx, err, "failed to serve HTTP/3 for XHTTP/3")
			}
		}()
	} else { // tcp
		l.listener, err = internet.ListenSystem(ctx, &net.TCPAddr{
			IP:   address.IP(),
			Port: int(port),
		}, streamSettings.SocketSettings)
		if err != nil {
			return nil, errors.New("failed to listen TCP for XHTTP on ", address, ":", port).Base(err)
		}
		errors.LogInfo(ctx, "listening TCP for XHTTP on ", address, ":", port)
	}

	if !l.isH3 && streamSettings.TcpmaskManager != nil {
		wrapped, err := streamSettings.TcpmaskManager.WrapListener(l.listener)
		if err != nil {
			l.listener.Close()
			return nil, errors.New("failed to wrap listener for TCP mask").Base(err)
		}
		l.listener = wrapped
	}

	// tcp/unix (h1/h2)
	if l.listener != nil {
		if config := tls.ConfigFromStreamSettings(streamSettings); config != nil {
			if tlsConfig := config.GetTLSConfig(); tlsConfig != nil {
				l.listener = gotls.NewListener(l.listener, tlsConfig)
			}
		}
		if config := reality.ConfigFromStreamSettings(streamSettings); config != nil {
			rc, err := config.GetREALITYConfig()
			if err != nil {
				// Fail closed like tcp/grpc hubs: a bad REALITY config must
				// not silently degrade to a plaintext listener.
				return nil, errors.New("invalid REALITY config").Base(err).AtError()
			}
			l.listener = goreality.NewListener(l.listener, rc)
		}

		handler.localAddr = l.listener.Addr()

		// server can handle both plaintext HTTP/1.1 and h2c
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		protocols.SetUnencryptedHTTP2(true)
		l.server = http.Server{
			Handler:           handler,
			ReadHeaderTimeout: time.Second * 4,
			MaxHeaderBytes:    l.config.GetNormalizedServerMaxHeaderBytes(),
			Protocols:         protocols,
			// Match client http2.Transport frame size so bulk packet-up DATA
			// frames are not stuck above the browser-like 16KiB SETTINGS ceiling.
			// Peer write size is limited by what THIS endpoint will read.
			HTTP2: &http.HTTP2Config{
				MaxReadFrameSize: 16384,
			},
		}
		go func() {
			if err := l.server.Serve(l.listener); err != nil {
				errors.LogErrorInner(ctx, err, "failed to serve HTTP for XHTTP")
			}
		}()
	}

	return l, err
}

// Addr implements net.Listener.Addr().
func (ln *Listener) Addr() net.Addr {
	if ln.h3listener != nil {
		return ln.h3listener.Addr()
	}
	if ln.listener != nil {
		return ln.listener.Addr()
	}
	return nil
}

// Close implements net.Listener.Close().
func (ln *Listener) Close() error {
	if ln.handler != nil {
		close(ln.handler.stopCh)
	}
	if ln.h3server != nil {
		if err := ln.h3server.Close(); err != nil {
			return err
		}
		if ln.h3listener != nil {
			return ln.h3listener.Close()
		}
		return nil
	} else if ln.listener != nil {
		return ln.listener.Close()
	}
	return errors.New("listener does not have an HTTP/3 server or a net.listener")
}

func getTLSConfig(streamSettings *internet.MemoryStreamConfig) *gotls.Config {
	config := tls.ConfigFromStreamSettings(streamSettings)
	if config == nil {
		return &gotls.Config{}
	}
	return config.GetTLSConfig()
}

func init() {
	common.Must(internet.RegisterTransportListener(protocolName, ListenXH))
}

type QListener struct {
	http3.QUICListener
	quicParams *internet.QuicParams
}

func (l *QListener) Accept(ctx context.Context) (*quic.Conn, error) {
	conn, err := l.QUICListener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	switch l.quicParams.Congestion {
	case "reno":
	case "", "bbr":
		congestion.UseBBR(conn, bbr.Profile(l.quicParams.BbrProfile))
	case "force-brutal":
		congestion.UseBrutal(conn, l.quicParams.BrutalUp)
	default:
		conn.CloseWithError(0, "")
		return nil, errors.New("unknown congestion algorithm: ", l.quicParams.Congestion)
	}
	return conn, nil
}
