package reality

// POC for REALITY 专项 R2 (MED data race): getKeyLogWriter() caches and
// returns a single shared *os.File per MasterKeyLog path, and every concurrent
// handshake writes to that same file via config.KeyLogWriter. os.File is not
// safe for concurrent use; the shared offset field is raced and key-log lines
// get interleaved/corrupted. R2 only triggers when MasterKeyLog is set.
//
// This test drives the REAL KeyLogWriterFromConfig (which returns the shared
// writer) and hammers it with concurrent Write calls. Run with `go test -race`:
//   - pre-fix (raw *os.File shared across handshakes): DATA RACE -> FAIL
//   - post-fix (mutex-wrapped *lockedKeyLogWriter): no race -> PASS
// (CI does not compile this package under -race, so the race guard is verified
// locally with -race, consistent with the D1/M1 pattern.)

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestR2_KeyLogWriterConcurrentWriteNoRace(t *testing.T) {
	// The shared key-log handle is cached open for the process lifetime
	// (REALITY 专项 R6: keyLogCache.handles is never closed). Using t.TempDir()
	// would fail cleanup on Windows, where an open handle blocks deletion, so
	// we use a unique path under os.TempDir() and deliberately do not delete
	// it (the global cache keeps it open regardless).
	path := filepath.Join(os.TempDir(), fmt.Sprintf("r2-keylog-%d-%d.txt", os.Getpid(), time.Now().UnixNano()))
	c := &Config{MasterKeyLog: path}
	w := KeyLogWriterFromConfig(c)
	if w == nil {
		t.Fatal("expected a key log writer for MasterKeyLog")
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			line := fmt.Sprintf("CLIENT_RANDOM %02d %x %x\n", i, []byte{byte(i)}, []byte{byte(i)})
			for j := 0; j < 20; j++ {
				//nolint:errcheck // key-log write errors are non-fatal in REALITY
				_, _ = w.Write([]byte(line))
			}
		}(i)
	}
	wg.Wait()
}
