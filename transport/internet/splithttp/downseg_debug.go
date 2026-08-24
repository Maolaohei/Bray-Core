package splithttp

import (
	"fmt"
	"os"
	"time"
)

// dbgDownSeg enables temporary per-session downlink-segmentation diagnostics
// (trace of finalize / production-leg exits / puller outcomes). Enabled via
// the BRAY_DSEG_DEBUG environment variable (any non-empty value) so the
// dual-end e2e tests can opt in without shipping an API. Kept off otherwise;
// never routed through the structured logger so production logs stay clean
// even at debug level.
var dbgDownSeg = os.Getenv("BRAY_DSEG_DEBUG") != ""

// dbgDownSegTimeStart anchors dbgLog timestamps at process start so e2e log
// correlation stays simple (offsets, not wall clock).
var dbgDownSegTimeStart = time.Now()

// dbgLog emits one gated diagnostic line to stderr with a process-start
// offset timestamp. All [DBG*] trace points funnel through here: single
// format, single destination, no direct println scattered in handlers.
func dbgLog(args ...any) {
	prefix := "[" + time.Since(dbgDownSegTimeStart).Round(time.Millisecond).String() + "] "
	fmt.Fprintln(os.Stderr, append([]any{prefix}, args...)...)
}
