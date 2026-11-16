// Layer 2: scripts/verify-cli.py as its own process, stopped by real signals
// or at its limit, with a stand-in cargo.
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
	// A task parent the tests listed, and one they did not.
	Listed, Unlisted string
}

// Of the task parents the fixture's tests made, the one they listed went with
// the run, and the other stays.
func assertCLIVerificationRemovedTheListedTaskParent(t *testing.T, left cliVerificationLeft) {
	t.Helper()
	if _, err := os.Lstat(left.Listed); !os.IsNotExist(err) {
		t.Errorf("the listed task parent %s outlived the run: %v", left.Listed, err)
	}
	if _, err := os.Stat(left.Unlisted); err != nil {
		t.Errorf("the run removed the unlisted task parent %s: %v", left.Unlisted, err)
	}
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
	result, command := prepareCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, cliFullLedgerTest, "ordinary_test"}, "", "CLI_FIXTURE_LEAVE="+leave)
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
	assertCLIVerificationRemovedTheListedTaskParent(t, left)
	if len(result.report.Cleared) != 2 || len(result.report.Left) != 0 || !result.report.Success {
		t.Fatalf("report: cleared %v, left %v, success %v", result.report.Cleared, result.report.Left, result.report.Success)
	}
	assertCLIVerificationRunsDirectoryIsEmpty(t, result)
}

func TestCLIVerificationClearsAnEndedRunAndLeavesALiveOne(t *testing.T) {
	result, command := prepareCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, cliFullLedgerTest, "ordinary_test"}, "")
	// Runs as a kill leaves them and a run that lives, each with a process
	// of its recorded session and one with a working directory of the run.
	// A session recorded on another boot, or whose leader's ID now names a
	// process started at another time, may be another's, and stays.
	type run struct {
		lock               *os.File
		inSession, working *exec.Cmd
		sessionKilled      bool
		// A task parent its tests listed.
		taskParent string
	}
	runs := map[string]*run{}
	taskParents := t.TempDir()
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
		taskParent := filepath.Join(taskParents, row.name)
		if err := os.Mkdir(taskParent, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "task-parents"), []byte(taskParent+"\n"), 0o600); err != nil {
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
		runs[row.name] = &run{lock, inSession, working, row.sessionKilled, taskParent}
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
		if _, err := os.Stat(run.taskParent); os.IsNotExist(err) == live {
			t.Errorf("%s: its listed task parent stayed=%v", name, !live)
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

// A run stopped by a signal or at its limit, with its tests running, ends
// them and clears what they left.
func TestCLIVerificationStoppedByASignalClearsItsRun(t *testing.T) {
	rows := []struct {
		name, wantError string
		limit           bool
	}{
		{"SIGTERM", "stopped by SIGTERM", false},
		{"the run's limit", "stopped by the run's limit of 3 s", true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			leave := filepath.Join(t.TempDir(), "left.json")
			result, command := prepareCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, cliFullLedgerTest, "ordinary_test"}, "", "CLI_FIXTURE_LEAVE="+leave, "CLI_FIXTURE_HANG=1")
			if row.limit {
				command.Args = append(command.Args, "--run-limit", "3")
			}
			var output strings.Builder
			command.Stdout, command.Stderr = &output, &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			for start := time.Now(); ; time.Sleep(10 * time.Millisecond) {
				if body, err := os.ReadFile(leave); err == nil && json.Valid(body) {
					break
				}
				if time.Since(start) > 20*time.Second {
					t.Fatalf("the tests never started: %s", output.String())
				}
			}
			if !row.limit {
				if err := command.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			err := command.Wait()
			result.output = output.String()
			if command.ProcessState.ExitCode() != 1 {
				t.Fatalf("the stopped script ended %v\n%s", err, result.output)
			}
			readCLIVerificationFixture(t, &result)
			if result.report.Error != row.wantError || result.report.Success {
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
			assertCLIVerificationRemovedTheListedTaskParent(t, left)
			assertCLIVerificationRunsDirectoryIsEmpty(t, result)
		})
	}
}

// A run stopped by a signal or at its limit before its tests start ends the
// command that runs with its ordinary children, as a build script is of Cargo.
func TestCLIVerificationStoppedBeforeItsTestsEndsTheCommandWithItsChildren(t *testing.T) {
	rows := []struct {
		phase, wantError string
		limit            bool
	}{
		{"build", "stopped by SIGTERM", false},
		{"build", "stopped by the run's limit of 3 s", true},
		{"seams", "stopped by SIGTERM", false},
		{"reference-server", "stopped by SIGTERM", false},
		{"digest", "stopped by SIGTERM", false},
		{"list", "stopped by SIGTERM", false},
	}
	for _, row := range rows {
		t.Run(row.phase+" "+row.wantError, func(t *testing.T) {
			if row.phase == "digest" && runtime.GOOS != "linux" {
				t.Skip("the build with the host bwrap's digest runs on Linux only")
			}
			child := filepath.Join(t.TempDir(), "child")
			result, command := prepareCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, cliFullLedgerTest, "ordinary_test"}, "", "CLI_FIXTURE_CHILD="+row.phase, "CLI_FIXTURE_CHILD_PID="+child)
			if row.limit {
				command.Args = append(command.Args, "--run-limit", "3")
			}
			var output strings.Builder
			command.Stdout, command.Stderr = &output, &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			var pid int
			for start := time.Now(); pid == 0; time.Sleep(10 * time.Millisecond) {
				if body, err := os.ReadFile(child); err == nil {
					if pid, err = strconv.Atoi(string(body)); err != nil {
						t.Fatal(err)
					}
				}
				if time.Since(start) > 20*time.Second {
					t.Fatalf("the %s command never started its child: %s", row.phase, output.String())
				}
			}
			if !row.limit {
				if err := command.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			err := command.Wait()
			result.output = output.String()
			if command.ProcessState.ExitCode() != 1 {
				t.Fatalf("the stopped script ended %v\n%s", err, result.output)
			}
			readCLIVerificationFixture(t, &result)
			if result.report.Error != row.wantError || result.report.Success {
				t.Fatalf("report: %+v", result.report)
			}
			if cliVerificationRuns(t, pid) {
				t.Errorf("process %d, a child of the %s command, outlived the stopped run", pid, row.phase)
			}
		})
	}
}

func evalSymlinks(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
