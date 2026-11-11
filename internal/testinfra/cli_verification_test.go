package testinfra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	cliReleaseTest  = "a_release_build_holds_no_test_seam"
	cliCapacityTest = "five_thousand_tasks_on_one_stream_each_issued_after_the_previous_terminal_ack_are_all_admitted"
)

var cliVerificationProfiles = []struct{ name, target, test string }{
	{"functional", "", ""},
	{"release", "release_seams", cliReleaseTest},
	{"task-capacity", "tool_admission", cliCapacityTest},
}

func TestCLIVerificationProfilesKeepDefaultCoverageAndIsolateDedicatedTests(t *testing.T) {
	ordinary := []string{"owner::tests::retains_acknowledged_entry", "resumed_stream_accepts_a_task", "crates/sdk/src/lib.rs - Client (line 7)"}
	for _, profile := range cliVerificationProfiles {
		t.Run(profile.name, func(t *testing.T) {
			names := append(slices.Clone(ordinary), cliReleaseTest, cliCapacityTest)
			wantSelected := ordinary
			// Plain cargo test retains its default unit, integration and doctest
			// scope. Only the two independently owned tests may be skipped.
			wantCommands := [][]string{
				{"cargo", "build", "--locked"},
				{"cargo", "test", "--locked", "--", "--list"},
				{"cargo", "test", "--locked", "--no-fail-fast", "--", "--skip", cliReleaseTest, "--skip", cliCapacityTest},
			}
			if profile.name != "functional" {
				// A substring collision is safe for the dedicated job because
				// its execution uses an exact selector, not the functional skip.
				names = []string{profile.test, profile.test + "_other", "unrelated_test"}
				wantSelected = []string{profile.test}
				wantCommands = [][]string{
					{"cargo", "test", "--locked", "-p", "tetral-tests", "--test", profile.target, "--", "--list"},
					{"cargo", "test", "--locked", "-p", "tetral-tests", "--test", profile.target, profile.test, "--", "--exact"},
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
				if !slices.Equal(result.calls[i].Args, want[1:]) || !slices.Equal(result.report.Commands[i].Argv, want) {
					t.Fatalf("command %d: executed %v, recorded %v, want %v", i, result.calls[i].Args, result.report.Commands[i].Argv, want)
				}
				assertCLIVerificationCommandEvidence(t, result, i, 0)
			}
		})
	}
}

func TestCLIVerificationRejectsMissingDuplicateAndShadowedSelections(t *testing.T) {
	for _, profile := range cliVerificationProfiles {
		t.Run(profile.name, func(t *testing.T) {
			required := []string{profile.test}
			if profile.name == "functional" {
				required = []string{cliReleaseTest, cliCapacityTest}
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
				for _, name := range required {
					cases = append(cases, selectionCase{"shadowed " + name, "would also omit", append(slices.Clone(required), name+"_extra", "ordinary_test")})
				}
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
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
		phases := []string{"list", "test"}
		if profile.name == "functional" {
			phases = append([]string{"build"}, phases...)
		}
		for failedAt, phase := range phases {
			t.Run(profile.name+"/"+phase, func(t *testing.T) {
				names := []string{cliReleaseTest, cliCapacityTest, "ordinary_test"}
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
			result := runCLIVerificationFixture(t, "functional", []string{cliReleaseTest, cliCapacityTest, "ordinary_test"}, "")
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
// its own Go module and stops at the first that fails.
func TestCLIVerificationReferenceProfileRunsItsGoChecksInsideTheirModule(t *testing.T) {
	want := [][]string{
		{"go", "vet", "./..."},
		{"golangci-lint", "run", "./..."},
		{"go", "test", "-race", "-count=1", "-timeout=5m", "./..."},
	}
	for failedAt := range len(want) + 1 {
		name := "all pass"
		if failedAt < len(want) {
			name = strings.Join(want[failedAt], " ") + " fails"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			evidence := filepath.Join(root, "evidence")
			for _, directory := range []string{"scripts", "cli/reference", "bin"} {
				if err := os.MkdirAll(filepath.Join(root, directory), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			script, err := os.ReadFile(filepath.Join(testRepositoryRoot(t), "scripts/verify-cli.py"))
			if err != nil {
				t.Fatal(err)
			}
			fail := ""
			if failedAt < len(want) {
				fail = strings.Join(want[failedAt], " ")
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
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", filepath.Join(root, "scripts/verify-cli.py"), "reference", "--evidence-dir", evidence)
			command.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+":"+os.Getenv("PATH"),
				"CLI_FIXTURE_CALLS="+filepath.Join(root, "calls.jsonl"), "CLI_FIXTURE_FAIL="+fail)
			output, runErr := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("verification timed out: %s", output)
			}
			var report cliVerificationReport
			body, err := os.ReadFile(filepath.Join(evidence, "verification.json"))
			if err != nil {
				t.Fatalf("missing verification report: %v\n%s", err, output)
			}
			if err := json.Unmarshal(body, &report); err != nil {
				t.Fatal(err)
			}
			ran := min(failedAt+1, len(want))
			if (runErr == nil) != (failedAt == len(want)) || report.Success != (failedAt == len(want)) || len(report.Commands) != ran {
				t.Fatalf("err=%v report=%+v, want %d commands and success only when none fails\n%s", runErr, report, ran, output)
			}
			body, err = os.ReadFile(filepath.Join(root, "calls.jsonl"))
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

func TestCLIVerificationWorkflowRequiresEverySelectedJobAndRetainsEvidence(t *testing.T) {
	workflow, err := parseYAMLFile(filepath.Join(testRepositoryRoot(t), ".github/workflows/cli-verification.yml"))
	if err != nil {
		t.Fatal(err)
	}
	jobs := mappingValue(workflow.Content[0], "jobs")
	linux := mappingValue(jobs, "linux")
	strategy := mappingValue(linux, "strategy")
	if scalar(mappingValue(strategy, "fail-fast")) != "false" {
		t.Fatal("a failing CLI profile must not cancel the other profiles")
	}
	profiles := sequenceScalars(mappingValue(mappingValue(strategy, "matrix"), "profile"))
	slices.Sort(profiles)
	if !slices.Equal(profiles, []string{"functional", "release", "task-capacity"}) {
		t.Fatalf("CLI matrix must cover all three verification profiles: %v", profiles)
	}
	uploads := 0
	for _, step := range sequenceNodes(mappingValue(linux, "steps")) {
		if strings.HasPrefix(scalar(mappingValue(step, "uses")), "actions/upload-artifact@") {
			uploads++
			if scalar(mappingValue(step, "if")) != "always()" {
				t.Fatal("CLI evidence upload must run after failure or cancellation")
			}
			if scalar(mappingValue(mappingValue(step, "with"), "path")) != "${{ env.CLI_EVIDENCE_DIR }}" {
				t.Fatal("CLI artifact must retain the complete evidence directory")
			}
		}
	}
	if uploads == 0 {
		t.Fatal("CLI verification has no evidence upload")
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
	steps := sequenceNodes(mappingValue(results, "steps"))
	if len(steps) != 1 {
		t.Fatalf("expected one result aggregation step, got %d", len(steps))
	}
	step := steps[0]
	for name, want := range map[string]string{
		"CHANGED_RESULT": "${{ needs.changed.result }}",
		"CLI_CHANGED":    "${{ needs.changed.outputs.cli }}",
		"LINUX_RESULT":   "${{ needs.linux.result }}",
		"MACOS_RESULT":   "${{ needs.macos.result }}",
	} {
		if got := scalar(mappingValue(mappingValue(step, "env"), name)); got != want {
			t.Fatalf("result input %s = %q, want %q", name, got, want)
		}
	}
	script := scalar(mappingValue(step, "run"))
	outcomes := []string{"success", "failure", "cancelled", "skipped"}
	for _, selectionResult := range outcomes {
		for _, selected := range []string{"true", "false", ""} {
			jobPasses := func(result string) bool {
				return selected == "true" && result == "success" || selected == "false" && (result == "skipped" || result == "success")
			}
			for _, linuxResult := range outcomes {
				for _, macosResult := range outcomes {
					t.Run(selectionResult+"/selected="+selected+"/linux="+linuxResult+"/macos="+macosResult, func(t *testing.T) {
						ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
						defer cancel()
						command := exec.CommandContext(ctx, "bash", "-c", script)
						command.Env = append(os.Environ(), "CHANGED_RESULT="+selectionResult, "CLI_CHANGED="+selected, "LINUX_RESULT="+linuxResult, "MACOS_RESULT="+macosResult)
						output, err := command.CombinedOutput()
						wantSuccess := selectionResult == "success" && jobPasses(linuxResult) && jobPasses(macosResult)
						if ctx.Err() != nil || (err == nil) != wantSuccess {
							t.Fatalf("aggregation success=%v want=%v context=%v output=%s", err == nil, wantSuccess, ctx.Err(), output)
						}
					})
				}
			}
		}
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
}

type cliVerificationCall struct {
	Phase, CWD, Acceptance string
	Args                   []string
}

type cliVerificationResult struct {
	root, evidence string
	report         cliVerificationReport
	calls          []cliVerificationCall
	err            error
	output         string
}

func runCLIVerificationFixture(t *testing.T, profile string, names []string, fail string) cliVerificationResult {
	t.Helper()
	result := cliVerificationResult{root: t.TempDir()}
	result.evidence = filepath.Join(result.root, "evidence")
	for _, directory := range []string{"scripts", "cli", "bin"} {
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
	for path, body := range map[string][]byte{"scripts/verify-cli.py": script, "bin/cargo": []byte(cliVerificationCargoFixture), "bin/bwrap": []byte(cliVerificationBwrapFixture)} {
		if err := os.WriteFile(filepath.Join(result.root, path), body, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	var listing strings.Builder
	for _, name := range names {
		listing.WriteString(name + ": test\n")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "python3", filepath.Join(result.root, "scripts/verify-cli.py"), profile, "--evidence-dir", result.evidence)
	command.Env = append(os.Environ(), "PATH="+filepath.Join(result.root, "bin")+":"+os.Getenv("PATH"),
		"CLI_FIXTURE_CALLS="+filepath.Join(result.root, "calls.jsonl"), "CLI_FIXTURE_LISTING="+listing.String(), "CLI_FIXTURE_FAIL="+fail,
		"GITHUB_SHA="+strings.Repeat("a", 40), "TETRAL_ACCEPTANCE_DIR=/must-not-inherit")
	output, err := command.CombinedOutput()
	result.err, result.output = err, string(output)
	if ctx.Err() != nil {
		t.Fatalf("verification timed out: %s", output)
	}
	body, err := os.ReadFile(filepath.Join(result.evidence, "verification.json"))
	if err != nil {
		t.Fatalf("missing verification report: %v\n%s", err, output)
	}
	if err := json.Unmarshal(body, &result.report); err != nil {
		t.Fatal(err)
	}
	if result.report.Profile != profile || result.report.Revision != strings.Repeat("a", 40) || result.report.Platform == "" || result.report.Architecture == "" {
		t.Fatalf("report lacks revision/platform identity: %+v", result.report)
	}
	body, err = os.ReadFile(filepath.Join(result.root, "calls.jsonl"))
	if errors.Is(err, fs.ErrNotExist) {
		return result
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var call cliVerificationCall
		if err := json.Unmarshal([]byte(line), &call); err != nil {
			t.Fatal(err)
		}
		if call.CWD != filepath.Join(result.root, "cli") || call.Acceptance != filepath.Join(result.evidence, "acceptance") {
			t.Fatalf("cargo ran outside its workspace or lost its acceptance destination: %+v", call)
		}
		result.calls = append(result.calls, call)
	}
	return result
}

func assertCLIVerificationCommandEvidence(t *testing.T, result cliVerificationResult, index, wantExit int) {
	t.Helper()
	if index >= len(result.report.Commands) {
		t.Fatalf("missing command %d from report: %+v", index, result.report)
	}
	command := result.report.Commands[index]
	call := result.calls[index]
	if command.Name != call.Phase || !slices.Equal(command.Argv, append([]string{"cargo"}, call.Args...)) || command.ExitCode == nil || *command.ExitCode != wantExit || command.Seconds == nil || *command.Seconds < 0 {
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

// A Go tool of a fixture: it records its command line and directory, and
// fails when CLI_FIXTURE_FAIL is that command line.
const cliVerificationGoFixture = `#!/usr/bin/env python3
import json
import os
import sys

argv = [os.path.basename(sys.argv[0]), *sys.argv[1:]]
with open(os.environ["CLI_FIXTURE_CALLS"], "a") as transcript:
    transcript.write(json.dumps({"argv": argv, "cwd": os.getcwd()}) + "\n")
sys.exit(47 if os.environ["CLI_FIXTURE_FAIL"] == " ".join(argv) else 0)
`

// The bwrap of a fixture, of the version CLI_FIXTURE_BWRAP_VERSION names.
const cliVerificationBwrapFixture = "#!/bin/sh\necho \"bubblewrap ${CLI_FIXTURE_BWRAP_VERSION:-0.13.0}\"\n"

const cliVerificationCargoFixture = `#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
if "--version" in args:
    print("cargo fixture")
    sys.exit(0)
phase = "build" if args[0] == "build" else "list" if "--list" in args else "test"
acceptance = os.environ.get("TETRAL_ACCEPTANCE_DIR", "")
assert Path(acceptance).is_dir(), "acceptance destination was not created"
with open(os.environ["CLI_FIXTURE_CALLS"], "a") as transcript:
    transcript.write(json.dumps({"phase": phase, "args": args, "cwd": os.getcwd(), "acceptance": acceptance}) + "\n")
print(phase + " stdout marker", flush=True)
print(phase + " stderr marker", file=sys.stderr, flush=True)
if phase == "list":
    print(os.environ["CLI_FIXTURE_LISTING"], end="", flush=True)
sys.exit(47 if os.environ["CLI_FIXTURE_FAIL"] == phase else 0)
`
