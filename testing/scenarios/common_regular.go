//go:build !coverage
// +build !coverage

package scenarios

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"
)

var testBinaryBuildMu sync.Mutex

func BuildXray() error {
	genTestBinaryPath()
	testBinaryBuildMu.Lock()
	defer testBinaryBuildMu.Unlock()
	if _, err := os.Stat(testBinaryPath); err == nil {
		return nil
	}

	fmt.Printf("Building Xray into path (%s)\n", testBinaryPath)
	cmd := exec.Command("go", "build", "-o="+testBinaryPath, GetSourcePath())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func RunXrayProtobuf(config []byte) *exec.Cmd {
	return RunXrayProtobufWithEnv(config, nil)
}

func RunXrayProtobufWithEnv(config []byte, env []string) *exec.Cmd {
	genTestBinaryPath()
	proc := exec.Command(testBinaryPath, "-config=stdin:", "-format=pb")
	proc.Stdin = bytes.NewBuffer(config)
	proc.Stderr = os.Stderr
	// Child xray processes print a version banner ("Xray ... Penetrates
	// Everything.") plus debug logs on startup, and are spawned fresh on
	// every `go test -bench` iteration. Forwarding their stdout to os.Stdout
	// interleaved those lines into the benchmark result stream, breaking
	// grep/benchstat sampling of "Benchmark..." rows (saw sporadic missing
	// samples / banner pollution on -count>1 runs). Route child stdout to
	// stderr so the benchmark stdout stays a clean channel: 2>&1 still shows
	// the logs when diagnosing, and `2>/dev/null` silently drops them.
	proc.Stdout = os.Stderr
	if len(env) > 0 {
		proc.Env = append(os.Environ(), env...)
	}

	return proc
}
