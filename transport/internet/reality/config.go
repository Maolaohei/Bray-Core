package reality

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Maolaohei/REALITY"
	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
)

// keyLogCache caches key log file handles to avoid leaking file descriptors.
// Each handle is wrapped in a mutex so concurrent handshakes (which all share
// the same *os.File for a given path) serialize their writes — os.File is not
// safe for concurrent use and racing writes corrupt the key log and trip the
// race detector (REALITY 专项 R2). On Linux/Unix Go's os.File.Write is
// lock-free, so the shared offset races; on Windows Go serializes internally,
// but we wrap uniformly for correctness on every platform.
var keyLogCache = struct {
	sync.Mutex
	handles map[string]*lockedKeyLogWriter
}{
	handles: make(map[string]*lockedKeyLogWriter),
}

// lockedKeyLogWriter serializes Write calls onto a shared *os.File.
type lockedKeyLogWriter struct {
	mu sync.Mutex
	f  *os.File
}

func (w *lockedKeyLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Write(p)
}

func getKeyLogWriter(path string) *lockedKeyLogWriter {
	keyLogCache.Lock()
	defer keyLogCache.Unlock()
	if w, ok := keyLogCache.handles[path]; ok {
		return w
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil
	}
	w := &lockedKeyLogWriter{f: f}
	keyLogCache.handles[path] = w
	return w
}

func (c *Config) GetREALITYConfig() (*reality.Config, error) {
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
	}
	// Align with upstream Xray: when unset, require clients >= 26.3.27 so
	// protocol-capability mismatches fail closed instead of half-working.
	minClientVer := c.MinClientVer
	if len(minClientVer) == 0 {
		minClientVer = []byte{26, 3, 27}
	}

	// Replay protection: the REALITY library already treats an unset (0)
	// MaxTimeDiff as 90s but emits a startup WARNING and leaves the window
	// implicit. Set the same 90s here explicitly so logs stay clean and the
	// anti-replay window is deterministic. This is behavior-neutral versus the
	// library default (same accept/reject decision). The Bray config field is an
	// unsigned millisecond count, so operators tune the window with a positive
	// value; 0 always means the 90s default.
	maxTimeDiff := time.Duration(c.MaxTimeDiff) * time.Millisecond
	if c.MaxTimeDiff == 0 {
		maxTimeDiff = 90 * time.Second
	}

	config := &reality.Config{
		DialContext: dialer.DialContext,

		Show: c.Show,
		Type: c.Type,
		Dest: c.Dest,
		Xver: byte(c.Xver),

		PrivateKey:   c.PrivateKey,
		MinClientVer: minClientVer,
		MaxClientVer: c.MaxClientVer,
		MaxTimeDiff:  maxTimeDiff,

		NextProtos:             nil, // should be nil
		SessionTicketsDisabled: true,

		KeyLogWriter: KeyLogWriterFromConfig(c),
		CacheDir:     c.CacheDir,
	}

	if c.Mldsa65Seed != nil {
		if len(c.Mldsa65Seed) != 32 {
			// Length-mismatched seed would panic in NewKeyFromSeed; fail loudly.
			return nil, errors.New("REALITY: Mldsa65Seed must be exactly 32 bytes, got ", len(c.Mldsa65Seed))
		}
		_, key := mldsa65.NewKeyFromSeed((*[32]byte)(c.Mldsa65Seed))
		config.Mldsa65Key = key.Bytes()
	}
	if c.LimitFallbackUpload != nil {
		config.LimitFallbackUpload.AfterBytes = c.LimitFallbackUpload.AfterBytes
		config.LimitFallbackUpload.BytesPerSec = c.LimitFallbackUpload.BytesPerSec
		config.LimitFallbackUpload.BurstBytesPerSec = c.LimitFallbackUpload.BurstBytesPerSec
	}
	if c.LimitFallbackDownload != nil {
		config.LimitFallbackDownload.AfterBytes = c.LimitFallbackDownload.AfterBytes
		config.LimitFallbackDownload.BytesPerSec = c.LimitFallbackDownload.BytesPerSec
		config.LimitFallbackDownload.BurstBytesPerSec = c.LimitFallbackDownload.BurstBytesPerSec
	}
	config.ServerNames = make(map[string]bool)
	for _, serverName := range c.ServerNames {
		config.ServerNames[serverName] = true
	}
	config.ShortIds = make(map[[8]byte]bool)
	for _, shortId := range c.ShortIds {
		if len(shortId) != 8 {
			// Length-mismatched short_id would panic on the fixed-size cast.
			return nil, errors.New("REALITY: each short_id must be exactly 8 bytes, got ", len(shortId))
		}
		config.ShortIds[*(*[8]byte)(shortId)] = true
	}
	return config, nil
}

func KeyLogWriterFromConfig(c *Config) io.Writer {
	if len(c.MasterKeyLog) <= 0 || c.MasterKeyLog == "none" {
		return nil
	}

	if writer := getKeyLogWriter(c.MasterKeyLog); writer != nil {
		return writer
	}

	errors.LogErrorInner(context.Background(), errors.New("failed to open ", c.MasterKeyLog, " as master key log"), "")
	return nil
}

func ConfigFromStreamSettings(settings *internet.MemoryStreamConfig) *Config {
	if settings == nil {
		return nil
	}
	config, ok := settings.SecuritySettings.(*Config)
	if !ok {
		return nil
	}
	return config
}
