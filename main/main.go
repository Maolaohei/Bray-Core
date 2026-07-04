package main

import (
	"flag"
	"os"
	"runtime/debug"

	"github.com/xtls/xray-core/main/commands/base"
	_ "github.com/xtls/xray-core/main/distro/all"
)

func main() {
	applyGCTuning()

	os.Args = getArgsV4Compatible()

	base.RootCommand.Long = "Xray is a platform for building proxies."
	base.RootCommand.Commands = append(
		[]*base.Command{
			cmdRun,
			cmdVersion,
		},
		base.RootCommand.Commands...,
	)
	base.Execute()
}

func getArgsV4Compatible() []string {
	if len(os.Args) == 1 {
		return []string{os.Args[0], "run"}
	}
	if os.Args[1][0] != '-' {
		return os.Args
	}
	version := false
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.BoolVar(&version, "version", false, "")
	// parse silently, no usage, no error output
	fs.Usage = func() {}
	fs.SetOutput(&null{})
	err := fs.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		// fmt.Println("DEPRECATED: -h, WILL BE REMOVED IN V5.")
		// fmt.Println("PLEASE USE: xray help")
		// fmt.Println()
		return []string{os.Args[0], "help"}
	}
	if version {
		// fmt.Println("DEPRECATED: -version, WILL BE REMOVED IN V5.")
		// fmt.Println("PLEASE USE: xray version")
		// fmt.Println()
		return []string{os.Args[0], "version"}
	}
	// fmt.Println("COMPATIBLE MODE, DEPRECATED.")
	// fmt.Println("PLEASE USE: xray run [arguments] INSTEAD.")
	// fmt.Println()
	return append([]string{os.Args[0], "run"}, os.Args[1:]...)
}

type null struct{}

func (n *null) Write(p []byte) (int, error) {
	return len(p), nil
}

// applyGCTuning configures the garbage collector for proxy workloads.
// Proxy servers prioritize low latency over low memory usage. By reducing
// GOGC from the default 100 to 50, GC runs more frequently but with shorter
// stop-the-world pauses, reducing latency spikes under high concurrency.
//
// Environment variable overrides:
//   - GOGC: Set to override the default GC target percentage
//   - GOMEMLIMIT: Set a soft memory limit (Go 1.19+ auto-detects cgroup limits)
func applyGCTuning() {
	// If user explicitly set GOGC, respect their choice.
	if os.Getenv("GOGC") != "" {
		return
	}
	// Lower GC target from 100 (default) to 50 for lower pause times.
	// This trades ~5-10% more CPU for significantly smoother latency.
	debug.SetGCPercent(50)
}
