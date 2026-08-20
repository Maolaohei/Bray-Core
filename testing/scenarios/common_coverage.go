//go:build coverage
// +build coverage

package scenarios

import (
	"bytes"
	"os"
	"os/exec"
	"sync"

	"github.com/xtls/xray-core/common/uuid"
)

var testBinaryBuildMu sync.Mutex

func BuildXray() error {
	genTestBinaryPath()
	testBinaryBuildMu.Lock()
	defer testBinaryBuildMu.Unlock()
	if _, err := os.Stat(testBinaryPath); err == nil {
		return nil
	}

	cmd := exec.Command("go", "test", "-tags", "coverage coveragemain", "-coverpkg", "github.com/xtls/xray-core/...", "-c", "-o", testBinaryPath, GetSourcePath())
	return cmd.Run()
}

func RunXrayProtobuf(config []byte) *exec.Cmd {
	genTestBinaryPath()

	covDir := os.Getenv("XRAY_COV")
	os.MkdirAll(covDir, os.ModeDir)
	randomID := uuid.New()
	profile := randomID.String() + ".out"
	proc := exec.Command(testBinaryPath, "-config=stdin:", "-format=pb", "-test.run", "TestRunMainForCoverage", "-test.coverprofile", profile, "-test.outputdir", covDir)
	proc.Stdin = bytes.NewBuffer(config)
	proc.Stderr = os.Stderr
	// Keep child stdout off the benchmark stream (same rationale as
	// common_regular.go): the xray banner would pollute go test -bench.
	proc.Stdout = os.Stderr

	return proc
}
