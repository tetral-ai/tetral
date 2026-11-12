// Layer 2: scripts/verify-cli.py as its own process, stopped by real signals,
// with a stand-in cargo.
package testinfra

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// What the fixture's test command reports of the run it was started in.
type cliVerificationLeft struct {
	Temporary, Memory string
	LeadsItsSession   bool `json:"leads_its_session"`
	Left              []int
}

func readCLIVerificationLeft(t *testing.T, path string) cliVerificationLeft {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var left cliVerificationLeft
	if err := json.Unmarshal(body, &left); err != nil {
		t.Fatal(err)
	}
	return left
}

// Whether a process that is not a child of this test still runs.
func cliVerificationRuns(t *testing.T, pid int) bool {
	t.Helper()
	output, _ := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
	state := strings.TrimSpace(string(output))
	return state != "" && !strings.HasPrefix(state, "Z")
}

func assertCLIVerificationRunsDirectoryIsEmpty(t *testing.T, result cliVerificationResult, kept ...string) {
	t.Helper()
	entries, err := os.ReadDir(result.runs)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	slices.Sort(kept)
	if !slices.Equal(names, kept) {
		t.Fatalf("the runs' directory holds %v, want %v", names, kept)
	}
}

func TestCLIVerificationClearsWhatItsTestsLeave(t *testing.T) {
	leave := filepath.Join(t.TempDir(), "left.json")
	result, command := prepareCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, "ordinary_test"}, "", "CLI_FIXTURE_LEAVE="+leave)
	output, err := command.CombinedOutput()
	result.output = string(output)
	if err != nil {
		t.Fatalf("verification failed: %v\n%s", err, output)
	}
	readCLIVerificationFixture(t, &result)
	left := readCLIVerificationLeft(t, leave)
	// The tests' temporary directory is the run's own, and their command
	// leads a session, by which the run finds what they started.
	if filepath.Dir(left.Temporary) != evalSymlinks(t, result.runs) || !left.LeadsItsSession {
		t.Fatalf("the tests ran in %q, leading their session: %v", left.Temporary, left.LeadsItsSession)
	}
	for _, pid := range left.Left {
		if cliVerificationRuns(t, pid) {
			t.Errorf("process %d outlived the run", pid)
		}
	}
	for _, directory := range []string{left.Temporary, left.Memory} {
		if _, err := os.Stat(directory); directory != "" && !os.IsNotExist(err) {
			t.Errorf("%s outlived the run: %v", directory, err)
		}
	}
	if len(result.report.Cleared) != 2 || len(result.report.Left) != 0 || !result.report.Success {
		t.Fatalf("report: cleared %v, left %v, success %v", result.report.Cleared, result.report.Left, result.report.Success)
	}
	assertCLIVerificationRunsDirectoryIsEmpty(t, result)
}

func TestCLIVerificationClearsAnEndedRunAndLeavesALiveOne(t *testing.T) {
	result, command := prepareCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, "ordinary_test"}, "")
	// Runs as a kill leaves them and a run that lives, each with a process
	// of its recorded session and one with a working directory of the run.
	// A session recorded on another boot, or whose leader's ID now names a
	// process started at another time, may be another's, and stays.
	type run struct {
		lock               *os.File
		inSession, working *exec.Cmd
		sessionKilled      bool
	}
	runs := map[string]*run{}
	for _, row := range []struct {
		name, boot, started string
		sessionKilled       bool
	}{
		{name: "run-ended", sessionKilled: true},
		{name: "run-live"},
		{name: "run-of-another-boot", boot: "another boot"},
		{name: "run-whose-leader-is-another", started: "Mon Jan  1 00:00:00 2001"},
	} {
		directory := filepath.Join(result.runs, row.name)
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		inSession := exec.Command("sleep", "300")
		inSession.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		working := exec.Command("sleep", "300")
		working.Dir = directory
		for _, process := range []*exec.Cmd{inSession, working} {
			if err := process.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = process.Process.Kill() })
		}
		lock, err := os.Create(filepath.Join(result.runs, row.name+".lock"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = lock.Close() })
		boot, started := row.boot, row.started
		if boot == "" {
			boot = cliVerificationBoot(t)
		}
		if started == "" {
			started = cliVerificationStarted(t, inSession.Process.Pid)
		}
		record, err := json.Marshal(map[string]any{"session": inSession.Process.Pid, "boot": boot, "started": started})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lock.Write(record); err != nil {
			t.Fatal(err)
		}
		runs[row.name] = &run{lock, inSession, working, row.sessionKilled}
	}
	if err := syscall.Flock(int(runs["run-live"].lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	output, err := command.CombinedOutput()
	result.output = string(output)
	if err != nil {
		t.Fatalf("verification failed: %v\n%s", err, output)
	}
	readCLIVerificationFixture(t, &result)

	ended := func(process *exec.Cmd) bool {
		done := make(chan struct{})
		go func() { _ = process.Wait(); close(done) }()
		select {
		case <-done:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}
	for name, run := range runs {
		live := name == "run-live"
		if ended(run.inSession) != run.sessionKilled {
			t.Errorf("%s: the process of its recorded session ended=%v", name, !run.sessionKilled)
		}
		if ended(run.working) == live {
			t.Errorf("%s: the process in its directory ended=%v", name, live)
		}
	}
	if len(result.report.ClearedOfEndedRuns) != 4 {
		t.Errorf("cleared of ended runs: %v", result.report.ClearedOfEndedRuns)
	}
	assertCLIVerificationRunsDirectoryIsEmpty(t, result, "run-live", "run-live.lock")
}

// The boot and a process's start as the script records them.
func cliVerificationBoot(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "linux" {
		body, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(body))
	}
	output, err := exec.Command("sysctl", "-n", "kern.boottime").Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(string(output), "}")[0]
}

func cliVerificationStarted(t *testing.T, pid int) string {
	t.Helper()
	output, err := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(output))
}

func TestCLIVerificationStoppedByASignalClearsItsRun(t *testing.T) {
	leave := filepath.Join(t.TempDir(), "left.json")
	result, command := prepareCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, "ordinary_test"}, "", "CLI_FIXTURE_LEAVE="+leave, "CLI_FIXTURE_HANG=1")
	var output strings.Builder
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	for start := time.Now(); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(leave); err == nil {
			break
		}
		if time.Since(start) > 20*time.Second {
			t.Fatalf("the tests never started: %s", output.String())
		}
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err := command.Wait()
	result.output = output.String()
	if command.ProcessState.ExitCode() != 1 {
		t.Fatalf("the stopped script ended %v\n%s", err, result.output)
	}
	readCLIVerificationFixture(t, &result)
	if result.report.Error != "stopped by SIGTERM" || result.report.Success {
		t.Fatalf("report: %+v", result.report)
	}
	left := readCLIVerificationLeft(t, leave)
	for _, pid := range left.Left {
		if cliVerificationRuns(t, pid) {
			t.Errorf("process %d outlived the stopped run", pid)
		}
	}
	if _, err := os.Stat(left.Temporary); !os.IsNotExist(err) {
		t.Errorf("%s outlived the stopped run: %v", left.Temporary, err)
	}
	assertCLIVerificationRunsDirectoryIsEmpty(t, result)
}

func evalSymlinks(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
