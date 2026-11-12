package integration

import (
	"context"
	"errors"
	"net/http"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// commandProcess is one Engine command running as its built binary.
type commandProcess struct {
	name    string
	command *exec.Cmd
	output  lockedBuffer
	joined  chan error
	// ended is set once the test itself has ended the process.
	ended bool
}

// builtCommand builds the command of pkg, a path from the repository root,
// and returns its binary.
func builtCommand(t testing.TB, pkg string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), filepath.Base(pkg))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if output, err := buildCommand(ctx, binary, pkg).CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v: %s", pkg, err, output)
	}
	return binary
}

// startCommand starts binary as name with env. A process that exits before
// the test ends it fails the test; one still running when the test ends gets
// SIGTERM and must exit 0 within 20 seconds.
func startCommand(t testing.TB, name, binary string, env map[string]string) *commandProcess {
	t.Helper()
	process := &commandProcess{name: name, joined: make(chan error, 1)}
	process.command = exec.Command(binary) //nolint:gosec // The repository's own command, built by this test.
	process.command.Env = oidcChildEnvironment(env)
	process.command.Stdout, process.command.Stderr = &process.output, &process.output
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { process.joined <- process.command.Wait() }()
	t.Cleanup(func() {
		if !process.ended {
			process.terminate(t, 20*time.Second)
		}
	})
	return process
}

// ready waits until url answers 200.
func (p *commandProcess) ready(t testing.TB, url string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		select {
		case err := <-p.joined:
			p.ended = true
			t.Fatalf("%s exited at its start: %v: %s", p.name, err, p.output.String())
		default:
		}
		if response, err := client.Get(url); err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not ready within 30 seconds: %s", p.name, p.output.String())
		}
	}
}

// kill ends the process with SIGKILL: nothing of it runs afterwards.
func (p *commandProcess) kill(t testing.TB) {
	t.Helper()
	p.ended = true
	if err := p.command.Process.Kill(); err != nil {
		t.Fatalf("kill %s: %v", p.name, err)
	}
	<-p.joined
}

// terminate sends SIGTERM; the process must exit 0 within limit.
func (p *commandProcess) terminate(t testing.TB, limit time.Duration) {
	t.Helper()
	p.ended = true
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Errorf("%s had exited on its own before its SIGTERM: %v: %s", p.name, err, p.output.String())
		return
	}
	select {
	case err := <-p.joined:
		if err != nil {
			t.Errorf("%s after SIGTERM: %v: %s", p.name, err, p.output.String())
		}
	case <-time.After(limit):
		_ = p.command.Process.Kill()
		<-p.joined
		t.Errorf("%s did not exit within %s of SIGTERM", p.name, limit)
	}
}

// runToExit runs binary with env until it exits on its own and returns its
// exit status and output. A command still running after limit fails the
// test.
func runToExit(t testing.TB, binary string, env map[string]string, limit time.Duration, arguments ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	command := exec.CommandContext(ctx, binary, arguments...) //nolint:gosec // The repository's own command, built by this test.
	command.Env = oidcChildEnvironment(env)
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("%s was still running after %s: %s", filepath.Base(binary), limit, output)
	}
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatalf("run %s: %v", filepath.Base(binary), err)
	}
	return command.ProcessState.ExitCode(), string(output)
}
