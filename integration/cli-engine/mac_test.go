package joint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// laneChecked holds, per local artifact, whether its copy in the lane
// directory (TETRAL_MAC_LANE_DIR) had the same SHA-256 when this test
// process first used it.
var laneChecked sync.Map

// laneCopy fails t unless the lane directory's copy of local, named name,
// has local's SHA-256, checked once per artifact and test process.
func laneCopy(t *testing.T, host macHost, lane, name, local string) {
	t.Helper()
	if _, ok := laneChecked.Load(local); ok {
		return
	}
	data, err := os.ReadFile(local)
	if err != nil {
		t.Fatalf("read %s: %v", local, err)
	}
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	got, _, _ := strings.Cut(host.output(t, "shasum -a 256 "+shellQuote(path.Join(lane, name))), " ")
	if got != want {
		t.Fatalf("the lane copy %s/%s has SHA-256 %s; the local artifact %s has %s", lane, name, got, local, want)
	}
	laneChecked.Store(local, true)
}

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

// ownMac is the Mac on which a process of its own run made the run's
// directory, for removeOwnRun.
var ownMac atomic.Pointer[macHost]

// macRun is the run's Mac directory as the Mac's shell names it.
func macRun(host macHost) string {
	if ownRun {
		ownMac.Store(&host)
	}
	return "~/" + MacRoot(runID)
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
	name := map[syscall.Signal]string{syscall.SIGTERM: "TERM", syscall.SIGINT: "INT", syscall.SIGKILL: "KILL", syscall.SIGSTOP: "STOP", syscall.SIGCONT: "CONT"}[sig]
	if out, err := h.remote(ctx, "kill -"+name+" "+pid).CombinedOutput(); err != nil {
		return fmt.Errorf("kill -%s %s on the Mac: %v: %s", name, pid, err, out)
	}
	return nil
}

func (h macHost) pid(p *cliProcess) string {
	pid, _ := p.stderr.remotePID(30 * time.Second)
	return pid
}

// endOutlived ends p's remote process, by SIGTERM and after 15 seconds by
// SIGKILL, while its process ID still runs program; a reused ID does not.
func (h macHost) endOutlived(p *cliProcess, program string) (string, error) {
	pid, err := p.stderr.remotePID(time.Second)
	if err != nil {
		// The session ended before its shell wrote the ID, so it started
		// nothing.
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	script := fmt.Sprintf(`c=$(ps -o command= -p %[1]s) || exit 0
case "$c" in %[2]s*) ;; *) exit 0 ;; esac
kill -TERM %[1]s
for i in $(seq 15); do kill -0 %[1]s 2> /dev/null || { echo SIGTERM; exit 0; }; sleep 1; done
kill -KILL %[1]s && echo SIGKILL`, pid, shellQuote(program))
	out, err := h.remote(ctx, script).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("process %s on the Mac: %v: %s", pid, err, out)
	}
	return strings.TrimSpace(string(out)), nil
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
		done := s.record.timed("machine %s: SSH forwarding session", name)
		s.mac = openMacSession(t, host, []string{s.engine.edgePort, s.engine.gitProxyPort})
		done()
	}
	done := s.record.timed("machine %s: Mac directory", name)
	run := macRun(host)
	dir := host.output(t, "mkdir -p "+run+" && mktemp -d "+run+"/machine.XXXXXXXX")
	done()
	t.Cleanup(func() {
		defer s.record.timed("cleanup: unmount and remove Mac directory of %s", name)()
		if t.Failed() {
			t.Logf("macOS machine %s kept at %s", name, dir)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		// Shutdown leaves an App Server's drive mounts for the next App
		// Server to detach, and a test's machine has no next App Server.
		out, _ := host.remote(ctx, "zmodload zsh/datetime; a=$EPOCHREALTIME; mount | sed -n 's|^.* on \\("+dir+"/.*\\) (.*$|\\1|p' | while IFS= read -r point; do umount -f \"$point\"; done; b=$EPOCHREALTIME; rm -rf "+shellQuote(dir)+"; c=$EPOCHREALTIME; printf '%.1fs unmount, %.1fs rm -rf' $((b-a)) $((c-b))").Output()
		s.record.command("cleanup of Mac machine %s: %s", name, out)
	})
	return s.macMachine(host, name, app, dir)
}

// addMacOnSmallVolume is addOnSmallVolume on the Mac: a 7 GiB sparse APFS
// disk image attached in a fresh directory.
func (s *machines) addMacOnSmallVolume(name, app string) (*machine, string) {
	t := s.t
	t.Helper()
	host := newMacHost(s.in)
	if s.mac == nil {
		done := s.record.timed("machine %s: SSH forwarding session", name)
		s.mac = openMacSession(t, host, []string{s.engine.edgePort, s.engine.gitProxyPort})
		done()
	}
	done := s.record.timed("machine %s: Mac directory and 7 GiB disk image", name)
	run := macRun(host)
	dir := host.output(t, "mkdir -p "+run+" && mktemp -d "+run+"/volume.XXXXXXXX")
	volume := dir + "/volume"
	t.Cleanup(func() {
		defer s.record.timed("cleanup: unmount, detach and remove Mac disk image of %s", name)()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = host.remote(ctx, "mount | sed -n 's|^.* on \\("+volume+"/.*\\) (.*$|\\1|p' | while IFS= read -r point; do umount -f \"$point\"; done; hdiutil detach -force "+shellQuote(volume)+"; rm -rf "+shellQuote(dir)).Run()
	})
	host.output(t, "hdiutil create -quiet -size 7g -type SPARSE -fs APFS -volname tetral-joint "+shellQuote(dir+"/small.sparseimage")+
		" && hdiutil attach -quiet -nobrowse -mountpoint "+shellQuote(volume)+" "+shellQuote(dir+"/small.sparseimage")+" && mkdir "+shellQuote(volume+"/"+name)+" && echo ok")
	done()
	s.record.command("macOS machine %s on a 7 GiB sparse APFS disk image attached on the Mac", name)
	return s.macMachine(host, name, app, volume+"/"+name), volume
}

// addMacInMemory is addInMemory on the Mac: a 1 GiB RAM disk erased as
// APFS, mounted under /Volumes and detached at cleanup, also when the test
// failed, as it holds the Mac's memory.
func (s *machines) addMacInMemory(name, app string) *machine {
	t := s.t
	t.Helper()
	host := newMacHost(s.in)
	if s.mac == nil {
		done := s.record.timed("machine %s: SSH forwarding session", name)
		s.mac = openMacSession(t, host, []string{s.engine.edgePort, s.engine.gitProxyPort})
		done()
	}
	done := s.record.timed("machine %s: Mac RAM disk", name)
	device, volume, _ := strings.Cut(host.output(t, `d=$(hdiutil attach -nomount ram://2097152 2> /dev/null | awk '{print $1}') && n=`+RAMVolume(runID)+`${d##*/} && diskutil erasevolume APFS "$n" "$d" > /dev/null && chmod 755 "/Volumes/$n" && mkdir "/Volumes/$n/`+name+`" && echo "$d /Volumes/$n"`), " ")
	t.Cleanup(func() {
		defer s.record.timed("cleanup: unmount and detach Mac RAM disk of %s", name)()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = host.remote(ctx, "mount | sed -n 's|^.* on \\("+volume+"/.*\\) (.*$|\\1|p' | while IFS= read -r point; do umount -f \"$point\"; done; hdiutil detach -force "+shellQuote(device)+" > /dev/null 2>&1").Run()
	})
	done()
	s.record.command("macOS machine %s on a 1 GiB RAM disk attached on the Mac", name)
	return s.macMachine(host, name, app, volume+"/"+name)
}

// macMachine starts the machine name of app in the Mac directory dir,
// copying its binaries there.
func (s *machines) macMachine(host macHost, name, app, dir string) *machine {
	t := s.t
	t.Helper()
	copies := []string{s.in.cliBinMacOS, s.engine.github.caPath}
	if s.needs.computerUse {
		copies = append(copies, s.in.mcpTestServerBinMacOS)
	}
	lane := os.Getenv("TETRAL_MAC_LANE_DIR")
	for _, local := range copies {
		base := filepath.Base(local)
		if lane != "" && local != s.engine.github.caPath {
			done := s.record.timed("machine %s: cp %s from the lane directory", name, base)
			laneCopy(t, host, lane, base, local)
			host.output(t, "cp "+shellQuote(path.Join(lane, base))+" "+shellQuote(path.Join(dir, base)))
			done()
			continue
		}
		done := s.record.timed("machine %s: scp %s", name, base)
		host.copy(t, local, path.Join(dir, base))
		done()
	}
	if lane != "" {
		s.record.command("macOS machine %s in a fresh directory on the Mac, binaries copied from the lane directory %s after its SHA-256 matched the local artifact", name, lane)
	} else {
		s.record.command("macOS machine %s in a fresh directory on the Mac, binaries copied with scp", name)
	}
	m := &machine{t: t, name: name, app: app, tetral: path.Join(dir, filepath.Base(s.in.cliBinMacOS)), home: path.Join(dir, "home"), gitCA: path.Join(dir, filepath.Base(s.engine.github.caPath)), host: host, record: s.record}
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
