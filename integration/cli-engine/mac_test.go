package joint

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A macOS machine runs on the Mac that TETRAL_MAC_SSH_DESTINATION names,
// every command in a login shell through SSH. The test's one forwarding
// session (macSession) makes each Engine endpoint that a Linux machine
// reaches on the Linux host's loopback reachable at the same URL on the
// Mac's loopback.

// macHost runs commands on the Mac.
type macHost struct {
	destination string
	key         string
}

func newMacHost(in inputs) macHost {
	return macHost{destination: in.macSSHDestination, key: in.macSSHKey}
}

func (h macHost) sshArgs(extra ...string) []string {
	return append([]string{"-i", h.key, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes"}, extra...)
}

// remote is the ssh command that runs script in a login shell on the Mac.
func (h macHost) remote(ctx context.Context, script string) *exec.Cmd {
	return exec.CommandContext(ctx, "ssh", h.sshArgs(h.destination, "zsh -lc "+shellQuote(script))...) //nolint:gosec // the harness's own command on the Mac
}

// output runs script on the Mac and returns its trimmed stdout, failing t
// at once with the command's output when ssh or the script fails.
func (h macHost) output(t *testing.T, script string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	out, err := h.remote(ctx, script).CombinedOutput()
	if err != nil {
		t.Fatalf("ssh %s %q: %v: %s", h.destination, script, err, out)
	}
	return strings.TrimSpace(string(out))
}

// copy copies local to remote on the Mac with scp and sets its mode to
// 0755.
func (h macHost) copy(t *testing.T, local, remote string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "scp", append(h.sshArgs(), local, h.destination+":"+remote)...) //nolint:gosec // the harness's own copy
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("scp %s to %s:%s: %v: %s", local, h.destination, remote, err, out)
	}
	h.output(t, "chmod 0755 "+shellQuote(remote))
}

// command runs argv on the Mac. The remote shell first writes its process
// ID to stderr (pidWriter), then reads one line of stdin per secret into its
// variable, then replaces itself with argv, which reads the rest of stdin.
func (h macHost) command(ctx context.Context, env, secrets map[string]string, stdin io.Reader, argv []string) *exec.Cmd {
	var script strings.Builder
	fmt.Fprintf(&script, "printf '%%s%%s\\n' %s \"$$\" >&2; ", shellQuote(pidMarker))
	var input []io.Reader
	for _, name := range sortedKeys(secrets) {
		fmt.Fprintf(&script, "IFS= read -r %s || exit 1; export %s; ", name, name)
		input = append(input, strings.NewReader(secrets[name]+"\n"))
	}
	for _, name := range sortedKeys(env) {
		fmt.Fprintf(&script, "export %s=%s; ", name, shellQuote(env[name]))
	}
	script.WriteString("exec")
	for _, arg := range argv {
		script.WriteString(" " + shellQuote(arg))
	}
	command := h.remote(ctx, script.String())
	if stdin != nil {
		input = append(input, stdin)
	}
	if len(input) > 0 {
		command.Stdin = io.MultiReader(input...)
	}
	return command
}

// signal sends sig to the remote process with kill on the Mac.
func (h macHost) signal(ctx context.Context, p *cliProcess, sig syscall.Signal) error {
	pid, err := p.stderr.remotePID(30 * time.Second)
	if err != nil {
		return err
	}
	name := map[syscall.Signal]string{syscall.SIGTERM: "TERM", syscall.SIGINT: "INT", syscall.SIGKILL: "KILL"}[sig]
	if out, err := h.remote(ctx, "kill -"+name+" "+pid).CombinedOutput(); err != nil {
		return fmt.Errorf("kill -%s %s on the Mac: %v: %s", name, pid, err, out)
	}
	return nil
}

func (h macHost) pid(p *cliProcess) string {
	pid, _ := p.stderr.remotePID(30 * time.Second)
	return pid
}

// macSession is the test's forwarding session: one -R forward per port.
type macSession struct {
	command *exec.Cmd
	output  lockedBuffer
	exited  chan struct{}
	err     error
}

// openMacSession opens the forwards of ports and waits until the Mac
// reaches each of them; a refused forward fails t at once.
func openMacSession(t *testing.T, host macHost, ports []string) *macSession {
	t.Helper()
	args := []string{"-N", "-o", "ExitOnForwardFailure=yes"}
	for _, port := range ports {
		args = append(args, "-R", port+":127.0.0.1:"+port)
	}
	s := &macSession{exited: make(chan struct{})}
	s.command = exec.Command("ssh", host.sshArgs(append(args, host.destination)...)...) //nolint:gosec // the harness's own forwarding session
	s.command.Stdout, s.command.Stderr = &s.output, &s.output
	if err := s.command.Start(); err != nil {
		t.Fatalf("start the SSH forwarding session: %v", err)
	}
	go func() {
		s.err = s.command.Wait()
		close(s.exited)
	}()
	t.Cleanup(func() {
		_ = s.command.Process.Kill()
		<-s.exited
	})
	deadline := time.Now().Add(30 * time.Second)
	for _, port := range ports {
		for {
			select {
			case <-s.exited:
				t.Fatalf("the SSH forwarding session ended: %v: %s", s.err, s.output.String())
			default:
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			err := host.remote(ctx, "curl -s -o /dev/null --max-time 2 http://127.0.0.1:"+port+"/").Run()
			cancel()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the Mac does not reach forwarded port %s within 30s: %s", port, s.output.String())
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	return s
}

// addMac starts the machine name of app on the Mac from TETRAL_CLI_BIN_MACOS
// in a fresh directory, opening the test's forwarding session first.
func (s *machines) addMac(name, app string) *machine {
	t := s.t
	t.Helper()
	host := newMacHost(s.in)
	if s.mac == nil {
		s.mac = openMacSession(t, host, []string{s.engine.edgePort})
	}
	dir := host.output(t, "mkdir -p ~/tetral-test-machines && mktemp -d ~/tetral-test-machines/machine.XXXXXXXX")
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("macOS machine %s kept at %s", name, dir)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = host.remote(ctx, "rm -rf "+shellQuote(dir)).Run()
	})
	copies := []string{s.in.cliBinMacOS}
	if s.needs.computerUse {
		copies = append(copies, s.in.mcpTestServerBinMacOS)
	}
	for _, local := range copies {
		host.copy(t, local, path.Join(dir, filepath.Base(local)))
	}
	s.record.command("macOS machine %s in a fresh directory on the Mac, binaries copied with scp", name)
	m := &machine{t: t, name: name, app: app, tetral: path.Join(dir, filepath.Base(s.in.cliBinMacOS)), home: path.Join(dir, "home"), host: host, record: s.record}
	m.version()
	return m
}

// shellQuote quotes value as one word of a POSIX shell or zsh.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
