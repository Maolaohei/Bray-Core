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
var keyLogCache = struct {
	sync.Mutex
	handles map[string]*os.File
}{
	handles: make(map[string]*os.File),
}

func getKeyLogWriter(path string) *os.File {
	keyLogCache.Lock()
	defer keyLogCache.Unlock()
	if f, ok := keyLogCache.handles[path]; ok {
		return f
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil
	}
	keyLogCache.handles[path] = f
	return f
}

func (c *Config) GetREALITYConfig() *reality.Config {
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
		config.ShortIds[*(*[8]byte)(shortId)] = true
	}
	return config
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
