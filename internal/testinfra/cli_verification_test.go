package testinfra

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	cliReleaseTest  = "a_release_build_holds_no_test_seam"
	cliCapacityTest = "five_thousand_tasks_on_one_stream_each_issued_after_the_previous_terminal_ack_are_all_admitted"
	// Run by a profile of its own on macOS, where its 4,030 Seatbelt launches
	// take minutes; on Linux the functional profile runs it.
	cliFullLedgerTest = "a_resume_that_resends_a_full_ledger_at_once_runs_no_instruction_again_and_keeps_its_stream"
)

var cliVerificationProfiles = []struct{ name, target, test string }{
	{"functional", "", ""},
	{"release", "release_seams", cliReleaseTest},
	{"task-capacity", "tool_admission", cliCapacityTest},
	{"full-ledger", "tool_admission", cliFullLedgerTest},
}

// The tests the functional profile leaves to a profile of their own on this
// system, and what its filter leaves out: the task-capacity test is compiled
// on Linux only, and the full-ledger test has a profile of its own on macOS.
func cliDedicatedTests() (dedicated, leftOut []string) {
	if runtime.GOOS == "linux" {
		return []string{cliReleaseTest, cliCapacityTest}, []string{cliReleaseTest, cliCapacityTest}
	}
	return []string{cliReleaseTest, cliFullLedgerTest}, []string{cliReleaseTest, cliCapacityTest, cliFullLedgerTest}
}

func TestCLIVerificationProfilesKeepDefaultCoverageAndIsolateDedicatedTests(t *testing.T) {
	// A name that only holds a dedicated test's name is an ordinary test:
	// the filters name a test by its whole name.
	ordinary := []string{"owner::tests::retains_acknowledged_entry", "resumed_stream_accepts_a_task", cliReleaseTest + "_other"}
	suite := func(names ...string) []string {
		selected := make([]string, len(names))
		for index, name := range names {
			selected[index] = "fixture-1 " + name
		}
		return selected
	}
	for _, profile := range cliVerificationProfiles {
		t.Run(profile.name, func(t *testing.T) {
			names := append(slices.Clone(ordinary), cliReleaseTest, cliCapacityTest, cliFullLedgerTest)
			_, leftOut := cliDedicatedTests()
			wantSelected := suite(ordinary...)
			if !slices.Contains(leftOut, cliFullLedgerTest) {
				wantSelected = append(wantSelected, suite(cliFullLedgerTest)...)
			}
			// The workspace's whole default scope, without the tests that
			// have a profile of their own.
			named := make([]string, len(leftOut))
			for index, name := range leftOut {
				named[index] = "test(=" + name + ")"
			}
			filter := "not (" + strings.Join(named, " | ") + ")"
			scope := []string{"--locked"}
			if profile.name != "functional" {
				names = []string{profile.test, profile.test + "_other", "unrelated_test"}
				wantSelected = suite(profile.test)
				filter = "test(=" + profile.test + ")"
				scope = []string{"--locked", "-p", "tetral-tests", "--test", profile.target}
			}
			var wantCommands [][]string
			for _, phase := range cliVerificationPhases(profile.name) {
				switch phase {
				case "build":
					wantCommands = append(wantCommands, []string{"cargo", "build", "--locked"})
				case "seams", "digest":
					target := map[string]string{"seams": "target/test-seams", "digest": "target/test-seams-bwrap-digest"}[phase]
					wantCommands = append(wantCommands, []string{"cargo", "build", "--locked", "-p", "tetral", "--features", "test-seams", "--target-dir", target})
				case "reference-server":
					wantCommands = append(wantCommands, []string{"go", "build", "-o", "../target/test-seams/tetral-reference", "./cmd/tetral-reference"})
				case "list":
					wantCommands = append(wantCommands, slices.Concat([]string{"cargo", "nextest", "list"}, scope, []string{"-E", filter, "--message-format", "json"}))
				case "test":
					wantCommands = append(wantCommands, slices.Concat([]string{"cargo", "nextest", "run"}, scope, []string{"-E", filter, "--no-fail-fast"}))
				}
			}
			result := runCLIVerificationFixture(t, profile.name, names, "")
			if result.err != nil || !result.report.Success {
				t.Fatalf("verification failed: %v\n%s", result.err, result.output)
			}
			if !slices.Equal(result.report.Selected, wantSelected) {
				t.Fatalf("selected tests = %v, want %v", result.report.Selected, wantSelected)
			}
			if len(result.calls) != len(wantCommands) || len(result.report.Commands) != len(wantCommands) {
				t.Fatalf("calls=%+v report=%+v, want %d commands", result.calls, result.report.Commands, len(wantCommands))
			}
			for i, want := range wantCommands {
				if result.calls[i].Program != want[0] || !slices.Equal(result.calls[i].Args, want[1:]) || !slices.Equal(result.report.Commands[i].Argv, want) {
					t.Fatalf("command %d: executed %v, recorded %v, want %v", i, result.calls[i].Args, result.report.Commands[i].Argv, want)
				}
				assertCLIVerificationCommandEvidence(t, result, i, 0)
				// The seams build shares the workspace's build directory;
				// the digest build compiles in the digest of the tests' bwrap.
				call := result.calls[i]
				wantBuildDirectory, wantDigest := "", ""
				switch call.Phase {
				case "seams":
					wantBuildDirectory = filepath.Join(result.root, "cli", "target")
				case "digest":
					digest := sha256.Sum256([]byte(cliVerificationBwrapFixture))
					wantDigest = hex.EncodeToString(digest[:])
				}
				if call.BuildDirectory != wantBuildDirectory || call.Digest != wantDigest {
					t.Fatalf("%s ran with build directory %q and digest %q", call.Phase, call.BuildDirectory, call.Digest)
				}
			}
			assertCLIVerificationKeepsTestResults(t, result)
		})
	}
}

func TestCLIVerificationRejectsMissingAndDuplicateSelections(t *testing.T) {
	for _, profile := range cliVerificationProfiles {
		t.Run(profile.name, func(t *testing.T) {
			required := []string{profile.test}
			if profile.name == "functional" {
				required, _ = cliDedicatedTests()
			}
			type selectionCase struct {
				name, want string
				names      []string
			}
			cases := []selectionCase{
				{name: "empty listing", want: "found 0"},
			}
			for index, name := range required {
				missing := append([]string{"ordinary_test"}, required[:index]...)
				missing = append(missing, required[index+1:]...)
				cases = append(cases,
					selectionCase{"missing " + name, "found 0", missing},
					selectionCase{"duplicate " + name, "found 2", append(slices.Clone(required), name, "ordinary_test")},
				)
			}
			if profile.name == "functional" {
				cases = append(cases, selectionCase{"no functional tests", "selected no tests", required})
			}
			// A filter that selects a test of another profile, as one
			// that the runner reads otherwise than this script would.
			cases = append(cases, selectionCase{"a filter that selects every test", "the filter of " + profile.name + " selected", append(slices.Clone(required), "ordinary_test")})
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					if strings.HasPrefix(tc.name, "a filter") {
						t.Setenv("CLI_FIXTURE_SELECT_ALL", "1")
					}
					result := runCLIVerificationFixture(t, profile.name, tc.names, "")
					if result.err == nil || result.report.Success || !strings.Contains(result.report.Error, tc.want) {
						t.Fatalf("selection was not rejected with %q: err=%v report=%+v output=%s", tc.want, result.err, result.report, result.output)
					}
					for i, call := range result.calls {
						if call.Phase == "test" {
							t.Fatal("executed tests after invalid selection")
						}
						assertCLIVerificationCommandEvidence(t, result, i, 0)
					}
				})
			}
		})
	}
}

func TestCLIVerificationRetainsFailedCommandEvidenceAndStops(t *testing.T) {
	for _, profile := range cliVerificationProfiles {
		for failedAt, phase := range cliVerificationPhases(profile.name) {
			t.Run(profile.name+"/"+phase, func(t *testing.T) {
				names := []string{cliReleaseTest, cliCapacityTest, cliFullLedgerTest, "ordinary_test"}
				result := runCLIVerificationFixture(t, profile.name, names, phase)
				if result.err == nil || result.report.Success || result.report.Error == "" {
					t.Fatalf("failed command was accepted: err=%v report=%+v output=%s", result.err, result.report, result.output)
				}
				if len(result.calls) != failedAt+1 || len(result.report.Commands) != failedAt+1 {
					t.Fatalf("runner continued after %s failed: calls=%+v report=%+v", phase, result.calls, result.report.Commands)
				}
				for i := range result.calls {
					wantExit := 0
					if i == failedAt {
						wantExit = 47
					}
					assertCLIVerificationCommandEvidence(t, result, i, wantExit)
				}
				if phase == "test" {
					assertCLIVerificationKeepsTestResults(t, result)
				}
			})
		}
	}
}

func TestCLIVerificationRefusesAnOlderBwrapBeforeAnyCommand(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the bwrap check runs on Linux only")
	}
	for _, tc := range []struct{ version, want string }{
		{"0.12.0", "need bubblewrap 0.13.0 or later"},
		{"unknown", "not a bubblewrap version"},
	} {
		t.Run(tc.version, func(t *testing.T) {
			t.Setenv("CLI_FIXTURE_BWRAP_VERSION", tc.version)
			result := runCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, cliFullLedgerTest, "ordinary_test"}, "")
			if result.err == nil || result.report.Success || !strings.Contains(result.report.Error, tc.want) {
				t.Fatalf("bwrap %s was not refused with %q: err=%v report=%+v output=%s", tc.version, tc.want, result.err, result.report, result.output)
			}
			if len(result.calls) != 0 || len(result.report.Commands) != 0 {
				t.Fatalf("cargo ran with bwrap %s: calls=%+v report=%+v", tc.version, result.calls, result.report.Commands)
			}
		})
	}
}

// The reference profile runs the reference control server's checks inside
// its own Go module and stops at the first that fails; its result names each
// test that go test reports.
func TestCLIVerificationReferenceProfileRunsItsGoChecksInsideTheirModule(t *testing.T) {
	want := [][]string{
		{"go", "vet", "./..."},
		{"golangci-lint", "run", "./..."},
		{"go", "test", "-json", "-race", "-count=1", "-timeout=5m", "./..."},
	}
	passed, failed := "pass", "fail"
	wantTests := []cliVerificationGoTest{{"fixture", "TestPasses", &passed}, {"fixture", "TestFails", &failed}}
	for failedAt := range len(want) + 1 {
		name := "all pass"
		if failedAt < len(want) {
			name = strings.Join(want[failedAt], " ") + " fails"
		}
		t.Run(name, func(t *testing.T) {
			fail := ""
			if failedAt < len(want) {
				fail = strings.Join(want[failedAt], " ")
			}
			root, evidence, command := prepareCLIVerificationReference(t, nil, "CLI_FIXTURE_FAIL="+fail)
			output, runErr := command.CombinedOutput()
			report := readCLIVerificationReference(t, evidence, output)
			ran := min(failedAt+1, len(want))
			if (runErr == nil) != (failedAt == len(want)) || report.Success != (failedAt == len(want)) || len(report.Commands) != ran {
				t.Fatalf("err=%v report=%+v, want %d commands and success only when none fails\n%s", runErr, report, ran, output)
			}
			if ranTests := ran == len(want); !slices.EqualFunc(report.Tests, map[bool][]cliVerificationGoTest{true: wantTests}[ranTests], sameCLIVerificationGoTest) {
				t.Errorf("tests %+v; want those go test reported", report.Tests)
			}
			body, err := os.ReadFile(filepath.Join(root, "calls.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(body)), "\n")
			if len(lines) != ran {
				t.Fatalf("%d commands ran, want %d: %s", len(lines), ran, body)
			}
			for index, line := range lines {
				var call struct {
					Argv []string
					CWD  string
				}
				if err := json.Unmarshal([]byte(line), &call); err != nil {
					t.Fatal(err)
				}
				wantExit := 0
				if index == failedAt {
					wantExit = 47
				}
				recorded := report.Commands[index]
				if !slices.Equal(call.Argv, want[index]) || call.CWD != filepath.Join(root, "cli", "reference") ||
					!slices.Equal(recorded.Argv, want[index]) || recorded.ExitCode == nil || *recorded.ExitCode != wantExit {
					t.Fatalf("command %d: ran %+v, recorded %+v; want %v in cli/reference with exit %d", index, call, recorded, want[index], wantExit)
				}
			}
		})
	}
}

// A stop of the reference profile, by a signal or at the run's limit, ends
// the whole process group of the command that runs, and its result keeps the
// tests reported so far, the running one without a result.
func TestCLIVerificationReferenceProfileStopEndsItsCommandsGroup(t *testing.T) {
	rows := []struct {
		name, wantError string
		limit           bool
	}{
		{"SIGTERM", "stopped by SIGTERM", false},
		{"the run's limit", "stopped by the run's limit of 1 s", true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			leave := filepath.Join(t.TempDir(), "left.json")
			var limit func([]byte) []byte
			if row.limit {
				limit = func(script []byte) []byte {
					const line = "REFERENCE_LIMIT_SECONDS = 480\n"
					if !bytes.Contains(script, []byte(line)) {
						t.Fatalf("the script has no line %q", line)
					}
					return bytes.Replace(script, []byte(line), []byte("REFERENCE_LIMIT_SECONDS = 1\n"), 1)
				}
			}
			_, evidence, command := prepareCLIVerificationReference(t, limit, "CLI_FIXTURE_FAIL=", "CLI_FIXTURE_HANG=1", "CLI_FIXTURE_LEAVE="+leave)
			var output strings.Builder
			command.Stdout, command.Stderr = &output, &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			for start := time.Now(); ; time.Sleep(10 * time.Millisecond) {
				if body, err := os.ReadFile(leave); err == nil && json.Valid(body) {
					break
				}
				if time.Since(start) > 5*time.Second {
					t.Fatalf("go test never started: %s", output.String())
				}
			}
			if !row.limit {
				if err := command.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			err := command.Wait()
			if command.ProcessState.ExitCode() != 1 {
				t.Fatalf("the stopped script ended %v\n%s", err, output.String())
			}
			report := readCLIVerificationReference(t, evidence, []byte(output.String()))
			if report.Error != row.wantError || report.Success {
				t.Fatalf("report: %+v", report)
			}
			if names := len(report.Tests); names != 3 || report.Tests[2].Name != "TestHangs" || report.Tests[2].Result != nil {
				t.Errorf("tests %+v; want the two that ended and the running one without a result", report.Tests)
			}
			for _, pid := range readCLIVerificationLeft(t, leave).Left {
				if cliVerificationRuns(t, pid) {
					t.Errorf("process %d of the stopped command's group outlived the run", pid)
				}
			}
		})
	}
}

// prepareCLIVerificationReference lays out a repository whose go and
// golangci-lint are the go fixture and returns the command of a reference
// run, its script changed by change when given.
func prepareCLIVerificationReference(t *testing.T, change func([]byte) []byte, env ...string) (root, evidence string, command *exec.Cmd) {
	t.Helper()
	root = t.TempDir()
	evidence = filepath.Join(root, "evidence")
	for _, directory := range []string{"scripts", "cli/reference", "bin"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	script, err := os.ReadFile(filepath.Join(testRepositoryRoot(t), "scripts/verify-cli.py"))
	if err != nil {
		t.Fatal(err)
	}
	if change != nil {
		script = change(script)
	}
	for path, body := range map[string][]byte{
		"scripts/verify-cli.py": script,
		"bin/go":                []byte(cliVerificationGoFixture),
		"bin/golangci-lint":     []byte(cliVerificationGoFixture),
	} {
		if err := os.WriteFile(filepath.Join(root, path), body, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	command = exec.CommandContext(ctx, "python3", filepath.Join(root, "scripts/verify-cli.py"), "reference", "--evidence-dir", evidence)
	command.Env = append(os.Environ(), append([]string{"PATH=" + filepath.Join(root, "bin") + ":" + os.Getenv("PATH"),
		"CLI_FIXTURE_CALLS=" + filepath.Join(root, "calls.jsonl")}, env...)...)
	return root, evidence, command
}

func readCLIVerificationReference(t *testing.T, evidence string, output []byte) cliVerificationReport {
	t.Helper()
	var report cliVerificationReport
	body, err := os.ReadFile(filepath.Join(evidence, "verification.json"))
	if err != nil {
		t.Fatalf("missing verification report: %v\n%s", err, output)
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	return report
}

func sameCLIVerificationGoTest(got, want cliVerificationGoTest) bool {
	return got.Package == want.Package && got.Name == want.Name &&
		(got.Result == nil) == (want.Result == nil) && (got.Result == nil || *got.Result == *want.Result)
}

func TestCLIVerificationWorkflowRequiresEverySelectedJobAndRetainsEvidence(t *testing.T) {
	workflow, err := parseYAMLFile(filepath.Join(testRepositoryRoot(t), ".github/workflows/cli-verification.yml"))
	if err != nil {
		t.Fatal(err)
	}
	jobs := mappingValue(workflow.Content[0], "jobs")
	// Both systems run their profiles through the verification script, so a
	// test selected on one is selected the same way on the other. The
	// task-capacity test is compiled on Linux only; the full-ledger test has
	// a job of its own on macOS.
	wantProfiles := map[string][]string{
		"linux": {"functional", "release", "task-capacity"},
		"macos": {"full-ledger", "functional", "release"},
	}
	for _, system := range []string{"linux", "macos"} {
		job := mappingValue(jobs, system)
		strategy := mappingValue(job, "strategy")
		if scalar(mappingValue(strategy, "fail-fast")) != "false" {
			t.Fatalf("%s: a failing CLI profile must not cancel the other profiles", system)
		}
		profiles := sequenceScalars(mappingValue(mappingValue(strategy, "matrix"), "profile"))
		slices.Sort(profiles)
		if !slices.Equal(profiles, wantProfiles[system]) {
			t.Fatalf("%s: CLI matrix must cover the system's verification profiles: %v", system, profiles)
		}
		if got := scalar(mappingValue(mappingValue(job, "env"), "CLI_PROFILE")); got != "${{ matrix.profile }}" {
			t.Fatalf("%s: CLI_PROFILE = %q", system, got)
		}
		steps := sequenceNodes(mappingValue(job, "steps"))
		position := func(id string) int {
			return slices.IndexFunc(steps, func(step *yaml.Node) bool { return scalar(mappingValue(step, "id")) == id })
		}
		if system == "macos" && (position("spotlight") != 0 || scalar(mappingValue(steps[0], "run")) != "sudo mdutil -a -i off") {
			t.Fatal("macos: Spotlight indexing must be off before the job builds")
		}
		nextest, verify := position("nextest"), position("verify")
		if nextest < 0 || verify < nextest {
			t.Fatalf("%s: cargo-nextest must be installed before the verification", system)
		}
		if got := scalar(mappingValue(steps[nextest], "run")); got != "./scripts/install-cargo-nextest.sh" {
			t.Fatalf("%s: cargo-nextest install = %q", system, got)
		}
		if got := scalar(mappingValue(steps[verify], "run")); got != `exec python3 scripts/verify-cli.py "$CLI_PROFILE" --evidence-dir "$CLI_EVIDENCE_DIR/verification"` {
			t.Fatalf("%s: verification = %q", system, got)
		}
		if system == "linux" {
			reference := position("reference")
			if reference < verify ||
				scalar(mappingValue(steps[reference], "if")) != "${{ !cancelled() && matrix.profile == 'functional' && steps.go.outcome == 'success' && steps.lint.outcome == 'success' }}" ||
				scalar(mappingValue(steps[reference], "run")) != `exec python3 scripts/verify-cli.py reference --evidence-dir "$CLI_EVIDENCE_DIR/reference"` {
				t.Fatal("linux: the functional job must check the reference control server after the tests, also after their failure")
			}
		}
		uploads := 0
		for index, step := range steps {
			if mappingValue(step, "continue-on-error") != nil {
				t.Fatalf("%s: step %q must fail its job when it fails", system, scalar(mappingValue(step, "name")))
			}
			if strings.HasPrefix(scalar(mappingValue(step, "uses")), "actions/upload-artifact@") {
				uploads++
				if index < verify {
					t.Fatalf("%s: CLI evidence must be uploaded after the verification", system)
				}
				if scalar(mappingValue(step, "if")) != "always()" {
					t.Fatalf("%s: CLI evidence upload must run after failure or cancellation", system)
				}
				with := mappingValue(step, "with")
				if scalar(mappingValue(with, "path")) != "${{ env.CLI_EVIDENCE_DIR }}" {
					t.Fatalf("%s: CLI artifact must retain the complete evidence directory", system)
				}
				want := map[string]string{"linux": "cli-", "macos": "cli-macos-"}[system] + "${{ matrix.profile }}-${{ github.run_attempt }}"
				if scalar(mappingValue(with, "name")) != want {
					t.Fatalf("%s: CLI artifact name = %q, want %q", system, scalar(mappingValue(with, "name")), want)
				}
			}
		}
		if uploads != 1 {
			t.Fatalf("%s: expected one evidence upload, got %d", system, uploads)
		}
	}
	results := mappingValue(jobs, "results")
	if scalar(mappingValue(results, "if")) != "always()" {
		t.Fatal("CLI results must aggregate even when a dependency fails or is skipped")
	}
	needs := sequenceScalars(mappingValue(results, "needs"))
	slices.Sort(needs)
	if !slices.Equal(needs, []string{"changed", "linux", "macos"}) {
		t.Fatalf("CLI results must wait for selection and every platform job: %v", needs)
	}
	// The deciding job's output selects both systems' jobs, and the gate
	// applies the shared rule to the jobs it needs under these names.
	changed := mappingValue(jobs, "changed")
	if got := scalar(mappingValue(mappingValue(changed, "outputs"), "cli")); got != "${{ steps.products.outputs.cli }}" {
		t.Fatalf("the deciding job's output cli = %q", got)
	}
	decide := sequenceNodes(mappingValue(changed, "steps"))
	if last := decide[len(decide)-1]; scalar(mappingValue(last, "id")) != "products" ||
		strings.Join(strings.Fields(scalar(mappingValue(last, "run"))), " ") != `go run ./internal/testinfra/cmd/tetral-ci-flow decide --event "$EVENT_NAME" --base "$BASE_SHA" --head "$HEAD_SHA" >> "$GITHUB_OUTPUT"` {
		t.Fatalf("the deciding step = %q", scalar(mappingValue(last, "run")))
	}
	for _, system := range []string{"linux", "macos"} {
		if got := scalar(mappingValue(mappingValue(jobs, system), "if")); got != "needs.changed.outputs.cli == 'true'" {
			t.Fatalf("%s runs if %q", system, got)
		}
	}
	steps := sequenceNodes(mappingValue(results, "steps"))
	rule := steps[slices.IndexFunc(steps, func(step *yaml.Node) bool {
		return strings.Contains(scalar(mappingValue(step, "run")), "tetral-ci-flow needs")
	})]
	if got := scalar(mappingValue(mappingValue(rule, "env"), "TETRAL_CI_NEEDS")); got != "${{ toJSON(needs) }}" {
		t.Fatalf("the gate reads its needed jobs from %q", got)
	}
	duration := steps[len(steps)-1]
	if scalar(mappingValue(duration, "run")) != `go run ./internal/testinfra/cmd/tetral-ci-flow duration --gate "CLI results"` ||
		scalar(mappingValue(duration, "if")) != "${{ !cancelled() }}" {
		t.Fatalf("the gate's last step must measure the run also after a failed job: %q if %q", scalar(mappingValue(duration, "run")), scalar(mappingValue(duration, "if")))
	}
	for name, want := range map[string]string{
		"GH_TOKEN":              "${{ github.token }}",
		"TETRAL_CI_REPOSITORY":  "${{ github.repository }}",
		"TETRAL_CI_RUN_ID":      "${{ github.run_id }}",
		"TETRAL_CI_RUN_ATTEMPT": "${{ github.run_attempt }}",
	} {
		if got := scalar(mappingValue(mappingValue(duration, "env"), name)); got != want {
			t.Fatalf("the measurement's %s = %q, want %q", name, got, want)
		}
	}
	if scalar(mappingValue(mappingValue(workflow.Content[0], "permissions"), "actions")) != "read" {
		t.Fatal("the measurement reads the run's jobs, which needs actions: read")
	}
	// The rule itself has its table in package ciflow; these rows hold the
	// workflow's job and output names to it.
	needed := func(selected, changed, linux, macos string) string {
		return fmt.Sprintf(`{"changed":{"result":%q,"outputs":{"cli":%q}},"linux":{"result":%q,"outputs":{}},"macos":{"result":%q,"outputs":{}}}`, changed, selected, linux, macos)
	}
	for _, row := range []struct {
		name, needs string
		passes      bool
	}{
		{"selected and passed", needed("true", "success", "success", "success"), true},
		{"not selected and skipped", needed("false", "success", "skipped", "skipped"), true},
		{"a system failed", needed("true", "success", "success", "failure"), false},
		{"selected and skipped", needed("true", "success", "skipped", "success"), false},
		{"the deciding job failed", needed("", "failure", "skipped", "skipped"), false},
	} {
		t.Run(row.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			command := exec.CommandContext(ctx, "bash", "-c", scalar(mappingValue(rule, "run")))
			command.Dir = testRepositoryRoot(t)
			command.Env = append(os.Environ(), "TETRAL_CI_NEEDS="+row.needs)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil || (err == nil) != row.passes {
				t.Fatalf("passed=%v want=%v context=%v output=%s", err == nil, row.passes, ctx.Err(), output)
			}
		})
	}
}

func TestCLIVerificationRunsOnPullRequestUpdatesAndDispatchOnly(t *testing.T) {
	workflow, err := parseYAMLFile(filepath.Join(testRepositoryRoot(t), ".github/workflows/cli-verification.yml"))
	if err != nil {
		t.Fatal(err)
	}
	triggers := mappingValue(workflow.Content[0], "on")
	pullRequest := mappingValue(triggers, "pull_request")
	if triggers == nil || len(triggers.Content) != 4 || pullRequest == nil || mappingValue(triggers, "workflow_dispatch") == nil {
		t.Fatal("CLI Verification must run on pull requests and manual dispatch only")
	}
	if types := sequenceScalars(mappingValue(pullRequest, "types")); !slices.Equal(types, []string{"opened", "synchronize", "reopened"}) {
		t.Fatalf("CLI Verification must run only on opened, synchronize and reopened pull requests and on manual dispatch: %v", types)
	}
}

type cliVerificationCommand struct {
	Name     string
	Argv     []string
	ExitCode *int     `json:"exit_code"`
	Seconds  *float64 `json:"seconds"`
}

type cliVerificationReport struct {
	Profile, Revision, Platform, Architecture, Error string
	Success                                          bool
	Selected                                         []string `json:"selected_tests"`
	Commands                                         []cliVerificationCommand
	Tests                                            []cliVerificationGoTest
	Cleared                                          []string
	ClearedOfEndedRuns                               []string `json:"cleared_of_ended_runs"`
	Left                                             []string
}

// A test of the reference profile's go test, with its result once it has one.
type cliVerificationGoTest struct {
	Package, Name string
	Result        *string
}

type cliVerificationCall struct {
	Phase, Program, CWD, Acceptance string
	Args                            []string
	BuildDirectory                  string `json:"build_directory"`
	Digest                          string
}

// The commands of a profile in order: the workspace's build, the programs
// the tests start, the listing and the tests. The build with the host
// bwrap's digest is Linux's.
func cliVerificationPhases(profile string) []string {
	switch {
	case profile == "release":
		return []string{"list", "test"}
	case profile == "task-capacity" || profile == "full-ledger":
		return []string{"seams", "reference-server", "list", "test"}
	case runtime.GOOS == "linux":
		return []string{"build", "seams", "reference-server", "digest", "list", "test"}
	}
	return []string{"build", "seams", "reference-server", "list", "test"}
}

type cliVerificationResult struct {
	profile, root, evidence string
	// Where the script keeps its runs' directories and locks.
	runs   string
	report cliVerificationReport
	calls  []cliVerificationCall
	err    error
	output string
}

func runCLIVerificationFixture(t *testing.T, profile string, names []string, fail string) cliVerificationResult {
	t.Helper()
	result, command := prepareCLIVerificationFixture(t, profile, names, fail)
	output, err := command.CombinedOutput()
	result.err, result.output = err, string(output)
	if command.ProcessState == nil || !command.ProcessState.Exited() {
		t.Fatalf("verification did not exit: %v\n%s", err, output)
	}
	readCLIVerificationFixture(t, &result)
	return result
}

// The fixture's files and the script's command, not yet started, with the
// directory of the script's runs inside the fixture.
func prepareCLIVerificationFixture(t *testing.T, profile string, names []string, fail string, environment ...string) (cliVerificationResult, *exec.Cmd) {
	t.Helper()
	result := cliVerificationResult{profile: profile, root: t.TempDir()}
	result.evidence = filepath.Join(result.root, "evidence")
	// Its name is also the name of the runs' directory in memory, which a
	// script that failed to clear its run leaves.
	result.runs = filepath.Join(result.root, "tetral-cli-runs-fixture-"+filepath.Base(result.root)+"-"+strings.ReplaceAll(t.Name(), "/", "-"))
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("/dev/shm", filepath.Base(result.runs))) })
	for _, directory := range []string{"scripts", "cli/reference", "bin"} {
		if err := os.MkdirAll(filepath.Join(result.root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	script, err := os.ReadFile(filepath.Join(testRepositoryRoot(t), "scripts/verify-cli.py"))
	if err != nil {
		t.Fatal(err)
	}
	// The script checks the fixture's bwrap instead of the host's.
	systemPath := []byte(`TEST_SYSTEM_PATH = "/usr/bin:/bin"`)
	if bytes.Count(script, systemPath) != 1 {
		t.Fatalf("verify-cli.py must define %s once", systemPath)
	}
	script = bytes.Replace(script, systemPath, []byte(`TEST_SYSTEM_PATH = "`+filepath.Join(result.root, "bin")+`"`), 1)
	for path, body := range map[string][]byte{"scripts/verify-cli.py": script, "bin/cargo": []byte(cliVerificationCargoFixture), "bin/go": []byte(cliVerificationCargoFixture), "bin/bwrap": []byte(cliVerificationBwrapFixture)} {
		if err := os.WriteFile(filepath.Join(result.root, path), body, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	var listing strings.Builder
	for _, name := range names {
		listing.WriteString(name + "\n")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, "python3", filepath.Join(result.root, "scripts/verify-cli.py"), profile, "--evidence-dir", result.evidence)
	command.Env = append(os.Environ(), "PATH="+filepath.Join(result.root, "bin")+":"+os.Getenv("PATH"),
		"CLI_FIXTURE_CALLS="+filepath.Join(result.root, "calls.jsonl"), "CLI_FIXTURE_LISTING="+listing.String(), "CLI_FIXTURE_FAIL="+fail,
		"GITHUB_SHA="+strings.Repeat("a", 40), "TETRAL_ACCEPTANCE_DIR=/must-not-inherit", "TETRAL_CLI_RUNS="+result.runs)
	command.Env = append(command.Env, environment...)
	return result, command
}

// Reads the report and the fixture's calls of a script that has ended.
func readCLIVerificationFixture(t *testing.T, result *cliVerificationResult) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(result.evidence, "verification.json"))
	if err != nil {
		t.Fatalf("missing verification report: %v\n%s", err, result.output)
	}
	if err := json.Unmarshal(body, &result.report); err != nil {
		t.Fatal(err)
	}
	if result.report.Profile != result.profile || result.report.Revision != strings.Repeat("a", 40) || result.report.Platform == "" || result.report.Architecture == "" {
		t.Fatalf("report lacks revision/platform identity: %+v", result.report)
	}
	body, err = os.ReadFile(filepath.Join(result.root, "calls.jsonl"))
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var call cliVerificationCall
		if err := json.Unmarshal([]byte(line), &call); err != nil {
			t.Fatal(err)
		}
		directory := map[string]string{"cargo": "cli", "go": "cli/reference"}[call.Program]
		if directory == "" || call.CWD != filepath.Join(result.root, directory) || call.Acceptance != filepath.Join(result.evidence, "acceptance") {
			t.Fatalf("cargo ran outside its workspace or lost its acceptance destination: %+v", call)
		}
		result.calls = append(result.calls, call)
	}
}

func assertCLIVerificationCommandEvidence(t *testing.T, result cliVerificationResult, index, wantExit int) {
	t.Helper()
	if index >= len(result.report.Commands) {
		t.Fatalf("missing command %d from report: %+v", index, result.report)
	}
	command := result.report.Commands[index]
	call := result.calls[index]
	if command.Name != call.Phase || !slices.Equal(command.Argv, append([]string{call.Program}, call.Args...)) || command.ExitCode == nil || *command.ExitCode != wantExit || command.Seconds == nil || *command.Seconds < 0 {
		t.Fatalf("lost command identity, exit or timing: call=%+v report=%+v want exit=%d", call, command, wantExit)
	}
	body, err := os.ReadFile(filepath.Join(result.evidence, call.Phase+".log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{call.Phase + " stdout marker", call.Phase + " stderr marker"} {
		if !strings.Contains(string(body), marker) {
			t.Fatalf("%s log lost %q: %s", call.Phase, marker, body)
		}
	}
}

// The run's results file, which cargo-nextest writes also when a test
// failed, is kept with the evidence.
func assertCLIVerificationKeepsTestResults(t *testing.T, result cliVerificationResult) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(result.evidence, "junit.xml"))
	if err != nil || string(body) != "<testsuites/>\n" {
		t.Fatalf("the run's results were not kept: %q, %v", body, err)
	}
}

// A Go tool of a fixture: it records its command line and directory, and
// fails when CLI_FIXTURE_FAIL is that command line.
const cliVerificationGoFixture = `#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import time

argv = [os.path.basename(sys.argv[0]), *sys.argv[1:]]
with open(os.environ["CLI_FIXTURE_CALLS"], "a") as transcript:
    transcript.write(json.dumps({"argv": argv, "cwd": os.getcwd()}) + "\n")
if "-json" in argv:
    # As go test -json reports: one test that passes and one that fails.
    for name, result in [("TestPasses", "pass"), ("TestFails", "fail")]:
        print(json.dumps({"Action": "run", "Package": "fixture", "Test": name}))
        print(json.dumps({"Action": "output", "Package": "fixture", "Test": name, "Output": name + " output\n"}))
        print(json.dumps({"Action": result, "Package": "fixture", "Test": name, "Elapsed": 0.5}), flush=True)
if argv[1] == "test" and "CLI_FIXTURE_HANG" in os.environ:
    # A test binary of the command's process group that outlives it.
    print(json.dumps({"Action": "run", "Package": "fixture", "Test": "TestHangs"}), flush=True)
    binary = subprocess.Popen(["sleep", "300"])
    with open(os.environ["CLI_FIXTURE_LEAVE"], "w") as leave:
        json.dump({"left": [os.getpid(), binary.pid]}, leave)
    time.sleep(300)
sys.exit(47 if os.environ["CLI_FIXTURE_FAIL"] == " ".join(argv) else 0)
`

// The bwrap of a fixture, of the version CLI_FIXTURE_BWRAP_VERSION names.
const cliVerificationBwrapFixture = "#!/bin/sh\necho \"bubblewrap ${CLI_FIXTURE_BWRAP_VERSION:-0.13.0}\"\n"

// The cargo of a fixture, and its go, whose one command is the reference
// control server's build.
const cliVerificationCargoFixture = `#!/usr/bin/env python3
import json
import os
from pathlib import Path
import re
import sys

program = os.path.basename(sys.argv[0])
args = sys.argv[1:]
if "--version" in args:
    print("cargo fixture")
    sys.exit(0)
if program == "go":
    phase = "reference-server"
elif args[0] == "build":
    phase = {"--locked": "build", "target/test-seams": "seams", "target/test-seams-bwrap-digest": "digest"}[args[-1]]
else:
    phase = {"list": "list", "run": "test"}[args[1]]
acceptance = os.environ.get("TETRAL_ACCEPTANCE_DIR", "")
assert Path(acceptance).is_dir(), "acceptance destination was not created"
with open(os.environ["CLI_FIXTURE_CALLS"], "a") as transcript:
    transcript.write(json.dumps({
        "phase": phase, "program": program, "args": args, "cwd": os.getcwd(), "acceptance": acceptance,
        "build_directory": os.environ.get("CARGO_BUILD_BUILD_DIR", ""),
        "digest": os.environ.get("CODEX_BWRAP_SHA256", ""),
    }) + "\n")
print(phase + " stdout marker", flush=True)
print(phase + " stderr marker", file=sys.stderr, flush=True)
if phase == "list":
    # As cargo-nextest lists: every test of the scope by binary, with
    # whether the filter selects it. A filter names tests by their whole
    # names, all of them or all but them; the nth test of one name is in the
    # nth binary.
    expression = args[args.index("-E") + 1]
    named = re.findall(r"test\(=([^)]+)\)", expression)
    suites = {}
    for name in os.environ["CLI_FIXTURE_LISTING"].split():
        binary = next(f"fixture-{n}" for n in range(1, 99) if name not in suites.get(f"fixture-{n}", {}))
        matches = (name in named) != expression.startswith("not ") or "CLI_FIXTURE_SELECT_ALL" in os.environ
        suites.setdefault(binary, {})[name] = {
            "ignored": False,
            "filter-match": {"status": "matches" if matches else "mismatch"},
        }
    print(json.dumps({"rust-suites": {binary: {"testcases": cases} for binary, cases in suites.items()}}), flush=True)
if phase == "test":
    results = Path("target/nextest/default")
    results.mkdir(parents=True)
    (results / "junit.xml").write_text("<testsuites/>\n")
    if "CLI_FIXTURE_LEAVE" in os.environ:
        # One process in the tests' session, and one that left the session
        # and kept a working directory of the run.
        import subprocess
        temporary = Path(os.environ["TMPDIR"])
        (temporary / "elsewhere").mkdir()
        # A directory a test took every permission from.
        (temporary / "closed" / "inside").mkdir(parents=True)
        (temporary / "closed").chmod(0)
        quiet = {"stdin": subprocess.DEVNULL, "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
        stayed = subprocess.Popen(["sleep", "300"], cwd="/", **quiet)
        moved = subprocess.Popen(["sleep", "300"], cwd=temporary / "elsewhere", start_new_session=True, **quiet)
        Path(os.environ["CLI_FIXTURE_LEAVE"]).write_text(json.dumps({
            "temporary": str(temporary), "memory": os.environ.get("TETRAL_TESTS_MEMORY_DIR", ""),
            "leads_its_session": os.getsid(0) == os.getpid(), "left": [stayed.pid, moved.pid],
        }))
    if "CLI_FIXTURE_HANG" in os.environ:
        import time
        time.sleep(300)
sys.exit(47 if os.environ["CLI_FIXTURE_FAIL"] == phase else 0)
`
