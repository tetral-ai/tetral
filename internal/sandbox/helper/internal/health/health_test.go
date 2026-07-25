package health

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCheckerRunVerifiesDraftBaseTemplateInvariants(t *testing.T) {
	root := t.TempDir()
	fuseConf := filepath.Join(root, "fuse.conf")
	if err := os.WriteFile(fuseConf, []byte("# ok\nuser_allow_other\n"), 0o600); err != nil {
		t.Fatalf("write fuse conf: %v", err)
	}
	runtimeRoot := filepath.Join(root, "runtime")
	checker := testChecker(runtimeRoot, fuseConf)
	checker.CaptureSelfTest = captureProcFDSelfTest

	result := checker.Run(context.Background())

	if Failed(result) {
		t.Fatalf("health result failed: %+v", result)
	}
	if result.Version != "test-version" {
		t.Fatalf("version = %q; want test-version", result.Version)
	}
	wantNames := []string{"sandbox", "rg", "runtime_root", "apply_patch", "rclone", "fusermount3", "fuse_conf", "capture_procfd"}
	if len(result.Checks) != len(wantNames) {
		t.Fatalf("checks = %+v; want %d checks", result.Checks, len(wantNames))
	}
	for index, want := range wantNames {
		if result.Checks[index].Name != want || !result.Checks[index].OK {
			t.Fatalf("check[%d] = %+v; want ok %s", index, result.Checks[index], want)
		}
	}
	info, err := os.Stat(runtimeRoot)
	if err != nil {
		t.Fatalf("stat runtime root: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("runtime root mode = %o; want 0700", info.Mode().Perm())
	}
}

func TestCaptureProcFDSelfTestRoundTripsSameInode(t *testing.T) {
	if err := captureProcFDSelfTest(); err != nil {
		t.Fatalf("capture procfd self-test: %v", err)
	}
}

func TestCheckerRunFailsWhenCaptureProcFDIsUnavailable(t *testing.T) {
	root := t.TempDir()
	fuseConf := filepath.Join(root, "fuse.conf")
	if err := os.WriteFile(fuseConf, []byte("user_allow_other\n"), 0o600); err != nil {
		t.Fatalf("write fuse conf: %v", err)
	}
	checker := testChecker(filepath.Join(root, "runtime"), fuseConf)
	checker.CaptureSelfTest = func() error { return errors.New("procfd unavailable") }

	result := checker.Run(context.Background())

	if got := failedCheck(result, "capture_procfd"); got == nil || got.OK || got.Message != "/proc/self/fd capture self-test failed" {
		t.Fatalf("capture_procfd check = %+v; want procfd failure", got)
	}
}

func TestCheckerRunFailsWhenRcloneIsMissing(t *testing.T) {
	root := t.TempDir()
	fuseConf := filepath.Join(root, "fuse.conf")
	if err := os.WriteFile(fuseConf, []byte("user_allow_other\n"), 0o600); err != nil {
		t.Fatalf("write fuse conf: %v", err)
	}
	checker := testChecker(filepath.Join(root, "runtime"), fuseConf)
	checker.LookPath = func(name string) (string, error) {
		if name == "rclone" {
			return "", errors.New("not found")
		}
		return "/usr/bin/" + name, nil
	}

	result := checker.Run(context.Background())

	if !Failed(result) {
		t.Fatalf("health result succeeded without rclone: %+v", result)
	}
	if result.Status != "error" {
		t.Fatalf("status = %q; want error", result.Status)
	}
	if got := failedCheck(result, "rclone"); got == nil || got.OK {
		t.Fatalf("rclone check = %+v; want failed check", got)
	}
}

func TestCheckerRunFailsWhenFusermount3IsNotSetuid(t *testing.T) {
	root := t.TempDir()
	fuseConf := filepath.Join(root, "fuse.conf")
	if err := os.WriteFile(fuseConf, []byte("user_allow_other\n"), 0o600); err != nil {
		t.Fatalf("write fuse conf: %v", err)
	}
	checker := testChecker(filepath.Join(root, "runtime"), fuseConf)
	checker.Stat = func(path string) (os.FileInfo, error) {
		if path == "/usr/bin/fusermount3" {
			return fakeHealthFileInfo{mode: 0o755}, nil
		}
		return os.Stat(path)
	}

	result := checker.Run(context.Background())

	if !Failed(result) {
		t.Fatalf("health result succeeded without setuid fusermount3: %+v", result)
	}
	if got := failedCheck(result, "fusermount3"); got == nil || got.OK || got.Message != "fusermount3 is not setuid" {
		t.Fatalf("fusermount3 check = %+v; want setuid failure", got)
	}
}

func TestCheckerRunFailsWhenFuseConfLacksUserAllowOther(t *testing.T) {
	root := t.TempDir()
	fuseConf := filepath.Join(root, "fuse.conf")
	if err := os.WriteFile(fuseConf, []byte("# missing\n"), 0o600); err != nil {
		t.Fatalf("write fuse conf: %v", err)
	}
	checker := testChecker(filepath.Join(root, "runtime"), fuseConf)

	result := checker.Run(context.Background())

	if got := failedCheck(result, "fuse_conf"); got == nil || got.OK {
		t.Fatalf("fuse_conf check = %+v; want failed check", got)
	}
}

func TestCheckerRunFailsWhenPatchSelfTestFails(t *testing.T) {
	root := t.TempDir()
	fuseConf := filepath.Join(root, "fuse.conf")
	if err := os.WriteFile(fuseConf, []byte("user_allow_other\n"), 0o600); err != nil {
		t.Fatalf("write fuse conf: %v", err)
	}
	checker := testChecker(filepath.Join(root, "runtime"), fuseConf)
	checker.PatchSelfTest = func() error { return errors.New("parse failed") }

	result := checker.Run(context.Background())

	if got := failedCheck(result, "apply_patch"); got == nil || got.OK {
		t.Fatalf("apply_patch check = %+v; want failed check", got)
	}
}

func TestRunCommandKillsHealthChildOnTimeout(t *testing.T) {
	scriptPath := filepath.Join(t.TempDir(), "rg")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nsleep 5\n"), 0o700); err != nil {
		t.Fatalf("write fake rg: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startedAt := time.Now()

	_, err := runCommand(ctx, scriptPath, "--version")
	elapsed := time.Since(startedAt)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runCommand error = %v; want DeadlineExceeded", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("runCommand elapsed = %s; want child killed near deadline", elapsed)
	}
}

func TestRunCommandSendsSIGTERMBeforeSIGKILL(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "rg")
	marker := filepath.Join(dir, "term.marker")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\ntrap 'echo term > \"$1\"; exit 0' TERM\nwhile :; do sleep 1; done\n"), 0o700); err != nil {
		t.Fatalf("write fake rg: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := runCommand(ctx, scriptPath, marker)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runCommand error = %v; want DeadlineExceeded", err)
	}
	body, readErr := os.ReadFile(marker)
	if readErr != nil {
		t.Fatalf("read SIGTERM marker: %v", readErr)
	}
	if strings.TrimSpace(string(body)) != "term" {
		t.Fatalf("SIGTERM marker = %q; want term", string(body))
	}
}

func testChecker(runtimeRoot string, fuseConf string) Checker {
	checker := NewChecker("test-version")
	checker.RuntimeRoot = runtimeRoot
	checker.FuseConfPath = fuseConf
	checker.LookPath = func(name string) (string, error) { return "/usr/bin/" + name, nil }
	checker.RunCommand = func(context.Context, string, ...string) (string, error) { return "ripgrep 14.1.0\n", nil }
	checker.CaptureSelfTest = func() error { return nil }
	checker.Stat = func(path string) (os.FileInfo, error) {
		if path == "/usr/bin/fusermount3" {
			return fakeHealthFileInfo{mode: os.ModeSetuid | 0o755}, nil
		}
		return os.Stat(path)
	}
	return checker
}

type fakeHealthFileInfo struct {
	mode os.FileMode
}

func (f fakeHealthFileInfo) Name() string       { return "fusermount3" }
func (f fakeHealthFileInfo) Size() int64        { return 0 }
func (f fakeHealthFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeHealthFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeHealthFileInfo) IsDir() bool        { return false }
func (f fakeHealthFileInfo) Sys() any           { return nil }

func failedCheck(result Result, name string) *Check {
	for index := range result.Checks {
		if result.Checks[index].Name == name {
			return &result.Checks[index]
		}
	}
	return nil
}
