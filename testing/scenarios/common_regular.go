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
	proc.Stdout = os.Stdout
	if len(env) > 0 {
		proc.Env = append(os.Environ(), env...)
	}

	return proc
}
