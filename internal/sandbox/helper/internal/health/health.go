package health

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/patch"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	HelperFailureKind = protocol.ErrorKindHelperFailure
	RuntimeRoot       = "/tmp/tetral-runtime"
	FuseConfPath      = "/etc/fuse.conf"

	healthCommandDeadline = 5 * time.Second
	processTerminateGrace = 2 * time.Second
)

type Result struct {
	Status  string  `json:"status"`
	Version string  `json:"version"`
	Checks  []Check `json:"checks"`
}

type Check struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Detail  string `json:"detail,omitempty"`
	Message string `json:"message,omitempty"`
}

type Checker struct {
	Version         string
	RuntimeRoot     string
	FuseConfPath    string
	LookPath        func(string) (string, error)
	RunCommand      func(context.Context, string, ...string) (string, error)
	PatchSelfTest   func() error
	MkdirAll        func(string, os.FileMode) error
	Chmod           func(string, os.FileMode) error
	Stat            func(string) (os.FileInfo, error)
	ReadFile        func(string) ([]byte, error)
	CaptureSelfTest func() error
}

func NewChecker(version string) Checker {
	return Checker{
		Version:         version,
		RuntimeRoot:     RuntimeRoot,
		FuseConfPath:    FuseConfPath,
		LookPath:        exec.LookPath,
		RunCommand:      runCommand,
		PatchSelfTest:   patch.SelfTest,
		MkdirAll:        os.MkdirAll,
		Chmod:           os.Chmod,
		Stat:            os.Stat,
		ReadFile:        os.ReadFile,
		CaptureSelfTest: captureProcFDSelfTest,
	}
}

func (c Checker) Run(ctx context.Context) Result {
	c = c.withDefaults()
	result := Result{Status: "ok", Version: c.Version}
	result.Checks = append(result.Checks,
		c.checkExecutable("sandbox", false),
		c.checkRG(ctx),
		c.checkRuntimeRoot(),
		c.checkPatchEngine(),
		c.checkExecutable("rclone", false),
		c.checkFusermount3(),
		c.checkFuseConf(),
		c.checkCaptureProcFD(),
	)
	for _, check := range result.Checks {
		if !check.OK {
			result.Status = "error"
			break
		}
	}
	return result
}

func (c Checker) withDefaults() Checker {
	if c.Version == "" {
		c.Version = "dev"
	}
	if c.RuntimeRoot == "" {
		c.RuntimeRoot = RuntimeRoot
	}
	if c.FuseConfPath == "" {
		c.FuseConfPath = FuseConfPath
	}
	if c.LookPath == nil {
		c.LookPath = exec.LookPath
	}
	if c.RunCommand == nil {
		c.RunCommand = runCommand
	}
	if c.PatchSelfTest == nil {
		c.PatchSelfTest = patch.SelfTest
	}
	if c.MkdirAll == nil {
		c.MkdirAll = os.MkdirAll
	}
	if c.Chmod == nil {
		c.Chmod = os.Chmod
	}
	if c.Stat == nil {
		c.Stat = os.Stat
	}
	if c.ReadFile == nil {
		c.ReadFile = os.ReadFile
	}
	if c.CaptureSelfTest == nil {
		c.CaptureSelfTest = captureProcFDSelfTest
	}
	return c
}

func (c Checker) checkExecutable(name string, captureVersion bool) Check {
	path, err := c.LookPath(name)
	if err != nil {
		return Check{Name: name, OK: false, Message: name + " is not on PATH"}
	}
	check := Check{Name: name, OK: true, Detail: path}
	if captureVersion {
		return check
	}
	return check
}

func (c Checker) checkFusermount3() Check {
	path, err := c.LookPath("fusermount3")
	if err != nil {
		return Check{Name: "fusermount3", OK: false, Message: "fusermount3 is not on PATH"}
	}
	info, err := c.Stat(path)
	if err != nil {
		return Check{Name: "fusermount3", OK: false, Detail: path, Message: "fusermount3 is not statable"}
	}
	if info.Mode()&os.ModeSetuid == 0 {
		return Check{Name: "fusermount3", OK: false, Detail: path, Message: "fusermount3 is not setuid"}
	}
	return Check{Name: "fusermount3", OK: true, Detail: path}
}

func (c Checker) checkRG(ctx context.Context) Check {
	check := c.checkExecutable("rg", true)
	if !check.OK {
		return check
	}
	commandCtx, cancel := context.WithTimeout(ctx, healthCommandDeadline)
	defer cancel()
	output, err := c.RunCommand(commandCtx, "rg", "--version")
	if err != nil {
		return Check{Name: "rg", OK: false, Detail: check.Detail, Message: "rg --version failed"}
	}
	check.Detail = firstLine(output)
	return check
}

func (c Checker) checkRuntimeRoot() Check {
	root := filepath.Clean(c.RuntimeRoot)
	if err := c.MkdirAll(root, 0o700); err != nil {
		return Check{Name: "runtime_root", OK: false, Detail: root, Message: "create runtime root failed"}
	}
	if err := c.Chmod(root, 0o700); err != nil {
		return Check{Name: "runtime_root", OK: false, Detail: root, Message: "chmod runtime root failed"}
	}
	info, err := c.Stat(root)
	if err != nil {
		return Check{Name: "runtime_root", OK: false, Detail: root, Message: "stat runtime root failed"}
	}
	if !info.IsDir() {
		return Check{Name: "runtime_root", OK: false, Detail: root, Message: "runtime root is not a directory"}
	}
	if info.Mode().Perm() != 0o700 {
		return Check{Name: "runtime_root", OK: false, Detail: root, Message: "runtime root mode is not 0700"}
	}
	return Check{Name: "runtime_root", OK: true, Detail: root}
}

func (c Checker) checkPatchEngine() Check {
	if err := c.PatchSelfTest(); err != nil {
		return Check{Name: "apply_patch", OK: false, Message: "apply_patch self-test failed"}
	}
	return Check{Name: "apply_patch", OK: true, Detail: "self-test passed"}
}

func (c Checker) checkFuseConf() Check {
	body, err := c.ReadFile(c.FuseConfPath)
	if err != nil {
		return Check{Name: "fuse_conf", OK: false, Detail: c.FuseConfPath, Message: "/etc/fuse.conf is not readable"}
	}
	for _, line := range strings.Split(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "user_allow_other" {
			return Check{Name: "fuse_conf", OK: true, Detail: c.FuseConfPath}
		}
	}
	return Check{Name: "fuse_conf", OK: false, Detail: c.FuseConfPath, Message: "/etc/fuse.conf missing user_allow_other"}
}

func (c Checker) checkCaptureProcFD() Check {
	if err := c.CaptureSelfTest(); err != nil {
		return Check{Name: "capture_procfd", OK: false, Message: "/proc/self/fd capture self-test failed"}
	}
	return Check{Name: "capture_procfd", OK: true, Detail: "same-inode file and directory reopen passed"}
}

func captureProcFDSelfTest() error {
	root, err := os.MkdirTemp("", "sandbox-capture-health-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(root) }()

	file, err := os.CreateTemp(root, "file-*")
	if err != nil {
		return err
	}
	name := file.Name()
	if err := file.Close(); err != nil {
		return err
	}
	if err := verifyCaptureProcFD(name, false); err != nil {
		return err
	}

	childName := "child.txt"
	if err := os.WriteFile(filepath.Join(root, childName), []byte("child"), 0o600); err != nil {
		return err
	}
	return verifyCaptureProcFD(root, true)
}

func verifyCaptureProcFD(name string, directory bool) error {
	classifyFlags := unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if directory {
		classifyFlags |= unix.O_DIRECTORY
	}
	classifiedFD, err := unix.Open(name, classifyFlags, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(classifiedFD) }()

	readFlags := unix.O_RDONLY | unix.O_CLOEXEC
	if directory {
		readFlags |= unix.O_DIRECTORY
	}
	readFD, err := unix.Open(fmt.Sprintf("/proc/self/fd/%d", classifiedFD), readFlags, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(readFD) }()

	var classifiedStat unix.Stat_t
	if err := unix.Fstat(classifiedFD, &classifiedStat); err != nil {
		return err
	}
	var readStat unix.Stat_t
	if err := unix.Fstat(readFD, &readStat); err != nil {
		return err
	}
	if classifiedStat.Dev != readStat.Dev || classifiedStat.Ino != readStat.Ino {
		return fmt.Errorf("capture procfd reopened a different inode")
	}
	if directory {
		buffer := make([]byte, 4096)
		count, err := unix.ReadDirent(readFD, buffer)
		if err != nil {
			return err
		}
		_, _, names := unix.ParseDirent(buffer[:count], -1, nil)
		if !slices.Contains(names, "child.txt") {
			return fmt.Errorf("capture procfd directory enumeration missed child")
		}
	}
	return nil
}

func runCommand(ctx context.Context, name string, args ...string) (string, error) {
	command := exec.Command(name, args...) //nolint:gosec // helper health runs fixed command names.
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	var output bytes.Buffer
	command.Stdout = &output
	if err := command.Start(); err != nil {
		return "", err
	}
	done := make(chan error, 1)
	processDone := make(chan struct{})
	go func() {
		done <- command.Wait()
		close(processDone)
	}()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer signal.Stop(signalCh)
	select {
	case err := <-done:
		if err != nil {
			return "", err
		}
		return output.String(), nil
	case sig := <-signalCh:
		terminateProcessGroup(command.Process)
		select {
		case <-processDone:
		case <-time.After(processTerminateGrace):
			killProcessGroup(command.Process)
			<-processDone
		}
		exitForSignal(sig)
		return "", ctx.Err()
	case <-ctx.Done():
		terminateProcessGroup(command.Process)
		select {
		case <-processDone:
		case <-time.After(processTerminateGrace):
			killProcessGroup(command.Process)
			<-processDone
		}
		return "", ctx.Err()
	}
}

func terminateProcessGroup(process *os.Process) {
	if process == nil {
		return
	}
	_ = syscall.Kill(-process.Pid, syscall.SIGTERM)
}

func killProcessGroup(process *os.Process) {
	if process == nil {
		return
	}
	_ = syscall.Kill(-process.Pid, syscall.SIGKILL)
}

func exitForSignal(sig os.Signal) {
	if signalValue, ok := sig.(syscall.Signal); ok {
		os.Exit(128 + int(signalValue))
	}
	os.Exit(1)
}

func firstLine(output string) string {
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func Failed(result Result) bool {
	if result.Status == "error" {
		return true
	}
	for _, check := range result.Checks {
		if !check.OK {
			return true
		}
	}
	return false
}
