package testinfra

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
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
	// A run as a kill leaves it and a run that lives, each with a process
	// of its recorded session and one with a working directory of the run.
	type run struct {
		lock               *os.File
		inSession, working *exec.Cmd
	}
	runs := map[string]run{}
	for _, name := range []string{"run-ended", "run-live"} {
		directory := filepath.Join(result.runs, name)
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
		lock, err := os.Create(filepath.Join(result.runs, name+".lock"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = lock.Close() })
		if _, err := lock.WriteString(strconv.Itoa(inSession.Process.Pid)); err != nil {
			t.Fatal(err)
		}
		runs[name] = run{lock, inSession, working}
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
	for _, process := range []*exec.Cmd{runs["run-ended"].inSession, runs["run-ended"].working} {
		if !ended(process) {
			t.Errorf("process %d of the ended run still runs", process.Process.Pid)
		}
	}
	for _, process := range []*exec.Cmd{runs["run-live"].inSession, runs["run-live"].working} {
		if ended(process) {
			t.Errorf("process %d of the live run was killed", process.Process.Pid)
		}
	}
	if len(result.report.ClearedOfEndedRuns) != 2 {
		t.Errorf("cleared of ended runs: %v", result.report.ClearedOfEndedRuns)
	}
	assertCLIVerificationRunsDirectoryIsEmpty(t, result, "run-live", "run-live.lock")
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
