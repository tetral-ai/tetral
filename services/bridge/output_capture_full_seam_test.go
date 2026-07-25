package agentruntimebridge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"
	"golang.org/x/sys/unix"

	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
)

func newRealOutputCaptureScanner(t *testing.T) *SandboxDriverToolExecutor {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "report.txt"), []byte("captured body"), 0o644); err != nil {
		t.Fatalf("write full-seam output: %v", err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "stream.pipe"), 0o600); err != nil {
		t.Fatalf("mkfifo full-seam output: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, string([]byte{'b', 'a', 'd', 0xff})), []byte("bad"), 0o644); err != nil {
		t.Fatalf("write unrepresentable full-seam output: %v", err)
	}
	services := bridgeLocalOutputServices{root: root, helper: buildBridgeOutputCaptureHelper(t)}
	driver := sandboxdriver.NewDaytonaHelperExecutorForSandboxServices(func(context.Context, string) (sandboxdriver.DaytonaSandboxServices, error) {
		return sandboxdriver.DaytonaSandboxServices{Process: services}, nil
	})
	return NewSandboxDriverToolExecutor(driver)
}

func newOldHelperRootFailureOutputCaptureScanner() *SandboxDriverToolExecutor {
	driver := sandboxdriver.NewDaytonaHelperExecutorForSandboxServices(func(context.Context, string) (sandboxdriver.DaytonaSandboxServices, error) {
		return sandboxdriver.DaytonaSandboxServices{Process: oldHelperRootFailureProcess{}}, nil
	})
	return NewSandboxDriverToolExecutor(driver)
}

type oldHelperRootFailureProcess struct{}

func (oldHelperRootFailureProcess) ExecuteCommand(context.Context, string, ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	return &types.ExecuteResponse{ExitCode: 1}, nil
}

type bridgeLocalOutputServices struct {
	root   string
	helper string
}

func (s bridgeLocalOutputServices) ExecuteCommand(ctx context.Context, command string, _ ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	remote, maxBytes, maxEntries, maxNameBytes, err := parseBridgeOutputCaptureCommand(command)
	if err != nil {
		return nil, err
	}
	script := "mount -t tmpfs tmpfs /mnt && mkdir -p /mnt/session/outputs && mount --bind \"$1\" /mnt/session/outputs && exec \"$2\" __capture --path \"$3\" --max-bytes \"$4\" --max-entries \"$5\" --max-name-bytes \"$6\""
	cmd := exec.CommandContext(ctx, "unshare", "-Ur", "-m", "sh", "-c", script, "capture-test", s.root, s.helper, remote, strconv.FormatInt(maxBytes, 10), strconv.Itoa(maxEntries), strconv.FormatInt(maxNameBytes, 10))
	output, runErr := cmd.Output()
	if runErr == nil {
		return &types.ExecuteResponse{ExitCode: 0, Result: string(output)}, nil
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return &types.ExecuteResponse{ExitCode: exitErr.ExitCode(), Result: string(output)}, nil
	}
	return nil, runErr
}

func parseBridgeOutputCaptureCommand(command string) (string, int64, int, int64, error) {
	const pathMarker = " --path '"
	const maxMarker = "' --max-bytes "
	const entriesMarker = " --max-entries "
	const namesMarker = " --max-name-bytes "
	pathStart := strings.Index(command, pathMarker)
	if pathStart < 0 {
		return "", 0, 0, 0, fmt.Errorf("capture command lacks path: %q", command)
	}
	pathStart += len(pathMarker)
	pathEnd := strings.Index(command[pathStart:], maxMarker)
	if pathEnd < 0 {
		return "", 0, 0, 0, fmt.Errorf("capture command lacks max bytes: %q", command)
	}
	remote := command[pathStart : pathStart+pathEnd]
	bounds := command[pathStart+pathEnd+len(maxMarker):]
	entriesAt := strings.Index(bounds, entriesMarker)
	namesAt := strings.Index(bounds, namesMarker)
	if entriesAt < 0 || namesAt < 0 || namesAt <= entriesAt {
		return "", 0, 0, 0, fmt.Errorf("capture command lacks directory bounds: %q", command)
	}
	maxBytes, err := strconv.ParseInt(strings.TrimSpace(bounds[:entriesAt]), 10, 64)
	if err != nil {
		return "", 0, 0, 0, err
	}
	maxEntries, err := strconv.Atoi(strings.TrimSpace(bounds[entriesAt+len(entriesMarker) : namesAt]))
	if err != nil {
		return "", 0, 0, 0, err
	}
	maxNameBytes, err := strconv.ParseInt(strings.TrimSpace(bounds[namesAt+len(namesMarker):]), 10, 64)
	return remote, maxBytes, maxEntries, maxNameBytes, err
}

func buildBridgeOutputCaptureHelper(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve bridge output capture test source")
	}
	engineRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	binary := filepath.Join(t.TempDir(), "sandbox")
	cmd := exec.Command("go", "build", "-o", binary, "./internal/sandbox/helper/cmd/sandbox")
	cmd.Dir = engineRoot
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build sandbox helper: %v\n%s", err, output)
	}
	return binary
}
