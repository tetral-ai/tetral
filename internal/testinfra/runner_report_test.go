package testinfra

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGoCommandsApplyIntegrationPackageWatchdog(t *testing.T) {
	// Force tool lookup to fail after the native worker constructs its command;
	// this command-contract test must not execute the selected compositions.
	t.Setenv("PATH", t.TempDir())
	tests := []struct {
		name    string
		profile Profile
		pkg     string
		timeout string
	}{
		{name: "full integration", profile: ProfileFull, pkg: "github.com/tetral-ai/tetral/integration", timeout: "-timeout=25m"},
		{name: "affected integration", profile: ProfileAffected, pkg: "github.com/tetral-ai/tetral/integration", timeout: "-timeout=25m"},
		{name: "unrelated package", profile: ProfileFull, pkg: "github.com/tetral-ai/tetral/services/bridge", timeout: "-timeout=20m"},
		{name: "integration subpackage", profile: ProfileFull, pkg: "github.com/tetral-ai/tetral/integration/static", timeout: "-timeout=20m"},
		{name: "fast integration", profile: ProfileFast, pkg: "github.com/tetral-ai/tetral/integration"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selection := Selection{Group: "go", Packages: []string{test.pkg}, Tests: []string{"TestSelected"}}
			want := []string{"go", "test", "-json", "-count=1"}
			if test.timeout != "" {
				want = append(want, "-race", test.timeout)
			}
			serial, err := commandsForSelection(Plan{Profile: test.profile}, selection, t.TempDir(), t.TempDir(), DependencyAuditChanged)
			if err != nil || len(serial) != 1 {
				t.Fatalf("serial commands = %v/%v; want one command", serial, err)
			}
			if !slices.Equal(serial[0].Arguments, append(slices.Clone(want), test.pkg)) {
				t.Fatalf("serial command = %v; want %v", serial[0].Arguments, append(slices.Clone(want), test.pkg))
			}
			worker, err := executeGoSelections(context.Background(), test.profile, []Selection{selection}, RunOptions{
				Root: t.TempDir(), OutputDir: t.TempDir(), MaxWorkers: 1,
			}, &dependencyManager{})
			if err == nil || len(worker) != 1 || worker[0].Status != "apparatus-failed" {
				t.Fatalf("worker result = %v/%v; want tool lookup failure", worker, err)
			}
			want = append(want, "-run", "^(?:TestSelected)$", test.pkg)
			if !slices.Equal(worker[0].Command, want) {
				t.Fatalf("worker command = %v; want %v", worker[0].Command, want)
			}
		})
	}

	t.Run("serial multiple packages", func(t *testing.T) {
		packages := []string{"github.com/tetral-ai/tetral/services/bridge", "github.com/tetral-ai/tetral/integration"}
		commands, err := commandsForSelection(Plan{Profile: ProfileFull}, Selection{Group: "go", Packages: packages}, t.TempDir(), t.TempDir(), DependencyAuditChanged)
		if err != nil || len(commands) != 1 {
			t.Fatalf("serial commands = %v/%v; want one command", commands, err)
		}
		want := append([]string{"go", "test", "-json", "-count=1", "-race", "-timeout=25m"}, packages...)
		if !slices.Equal(commands[0].Arguments, want) {
			t.Fatalf("serial multi-package command = %v; want %v", commands[0].Arguments, want)
		}
	})
}

func TestCoverageInstallsCrossLanguageDependenciesBeforeGoTests(t *testing.T) {
	commands, err := commandsForSelection(
		Plan{},
		Selection{Group: "coverage"},
		t.TempDir(),
		t.TempDir(),
		DependencyAuditChanged,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) < 3 {
		t.Fatalf("coverage commands = %v; want dependency setup before tests", commands)
	}
	wantInstall := []string{"bun", "install", "--frozen-lockfile"}
	if commands[0].WorkingDir != "services/agent-runtime" || !slices.Equal(commands[0].Arguments, wantInstall) {
		t.Fatalf("first coverage command = %v in %q; want Runtime dependency install", commands[0].Arguments, commands[0].WorkingDir)
	}
	if commands[1].WorkingDir != "services/gateway" || !slices.Equal(commands[1].Arguments, wantInstall) {
		t.Fatalf("second coverage command = %v in %q; want Gateway dependency install", commands[1].Arguments, commands[1].WorkingDir)
	}
	if !slices.Equal(commands[2].Arguments[:2], []string{"go", "test"}) {
		t.Fatalf("third coverage command = %v; want Go tests after dependency setup", commands[2].Arguments)
	}

	// Without an explicit budget go test stops every package binary after ten
	// minutes. Coverage runs the integration package as one sequential binary,
	// so its budget must leave twofold headroom over that package's run time.
	// Coverage times are not in the repository, so this checks the package's
	// summed Race time from the shard weights: lower than a coverage run, and
	// growing with the package.
	var budget time.Duration
	for _, argument := range commands[2].Arguments {
		value, ok := strings.CutPrefix(argument, "-timeout=")
		if !ok {
			continue
		}
		if budget != 0 {
			t.Fatalf("coverage Go command %v names more than one timeout", commands[2].Arguments)
		}
		budget, err = time.ParseDuration(value)
		if err != nil || budget <= 0 {
			t.Fatalf("coverage Go timeout %q is malformed: %v", value, err)
		}
	}
	if budget == 0 {
		t.Fatalf("coverage Go command %v has no whole-package budget", commands[2].Arguments)
	}
	weights, err := loadGoShardWeights()
	if err != nil {
		t.Fatal(err)
	}
	measured := time.Duration(weights.Packages["github.com/tetral-ai/tetral/integration"].ElapsedMS) * time.Millisecond
	if measured == 0 || budget < 2*measured {
		t.Fatalf("coverage Go budget %s does not leave twofold headroom over the integration package's Race time %s", budget, measured)
	}

	// The job must outlast the Go budget so a stuck package reports through
	// go test's watchdog, with time left for dependency setup, compilation and
	// the Bun coverage commands that share the job.
	workflow, err := parseYAMLFile(filepath.Join(testRepositoryRoot(t), ".github", "workflows", "scheduled-verification.yml"))
	if err != nil {
		t.Fatal(err)
	}
	minutes, err := strconv.Atoi(scalar(mappingValue(mappingValue(mappingValue(workflow.Content[0], "jobs"), "coverage"), "timeout-minutes")))
	if err != nil {
		t.Fatalf("scheduled coverage job has no numeric timeout-minutes: %v", err)
	}
	if jobLimit := time.Duration(minutes) * time.Minute; jobLimit < budget+15*time.Minute {
		t.Fatalf("scheduled coverage job limit %s leaves less than 15 minutes around the Go budget %s", jobLimit, budget)
	}
}

func TestSecuritySelectionAppliesDependencyAuditPolicy(t *testing.T) {
	tests := []struct {
		name      string
		mode      DependencyAuditMode
		paths     []string
		wantAudit bool
	}{
		{name: "changed unrelated", mode: DependencyAuditChanged, paths: []string{"services/bridge/runtime_delivery.go"}},
		{name: "changed package manifest", mode: DependencyAuditChanged, paths: []string{"services/gateway/package.json"}, wantAudit: true},
		{name: "changed lockfile", mode: DependencyAuditChanged, paths: []string{"services/agent-runtime/bun.lock"}, wantAudit: true},
		{name: "changed audit runner", mode: DependencyAuditChanged, paths: []string{"scripts/run-bun-audit.sh"}, wantAudit: true},
		{name: "changed audit plumbing", mode: DependencyAuditChanged, paths: []string{"internal/testinfra/runner.go"}, wantAudit: true},
		{name: "always", mode: DependencyAuditAlways, wantAudit: true},
		{name: "never", mode: DependencyAuditNever, paths: []string{"services/gateway/package.json"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := Plan{Revision: Revision{ChangedPaths: test.paths}}
			commands, err := commandsForSelection(plan, Selection{Group: "security"}, t.TempDir(), t.TempDir(), test.mode)
			if err != nil {
				t.Fatal(err)
			}
			var audits [][]string
			staticChecks := 0
			runtimeInstalls := 0
			gatewayInstalls := 0
			for _, command := range commands {
				if len(command.Arguments) > 0 && command.Arguments[0] == "./scripts/run-bun-audit.sh" {
					audits = append(audits, command.Arguments[1:])
				}
				if slices.Equal(command.Arguments, []string{"bun", "install", "--frozen-lockfile"}) {
					switch command.WorkingDir {
					case "services/agent-runtime":
						runtimeInstalls++
					case "services/gateway":
						gatewayInstalls++
					}
				}
				if slices.Equal(command.Arguments[:min(3, len(command.Arguments))], []string{"go", "test", "./integration/static"}) {
					staticChecks++
				}
			}
			// One audit names both packages, so one package's findings cannot
			// stop the other package's audit.
			var wantAudits [][]string
			wantGatewayInstalls := 0
			if test.wantAudit {
				wantAudits = [][]string{{"services/agent-runtime", "services/gateway"}}
				wantGatewayInstalls = 1
			}
			if !slices.EqualFunc(audits, wantAudits, slices.Equal[[]string]) {
				t.Fatalf("online audits = %v; want %v", audits, wantAudits)
			}
			if runtimeInstalls != 1 {
				t.Fatalf("Runtime installs = %d; want 1 for deterministic boundary checks", runtimeInstalls)
			}
			if gatewayInstalls != wantGatewayInstalls {
				t.Fatalf("Gateway installs = %d; want %d", gatewayInstalls, wantGatewayInstalls)
			}
			if staticChecks != 1 {
				t.Fatalf("static security checks = %d; want 1", staticChecks)
			}
		})
	}
}

func TestDependencyAuditModeRejectsUnknownPolicy(t *testing.T) {
	if _, err := ParseDependencyAuditMode("sometimes"); err == nil {
		t.Fatal("unknown dependency audit policy was accepted")
	}
}

func TestRunnerAnchorsRelativeEvidencePathsAtRepositoryRoot(t *testing.T) {
	root := t.TempDir()
	result, err := Execute(context.Background(), Plan{}, RunOptions{Root: root, OutputDir: ".test-results/example"})
	if err != nil || result.Status != "pass" {
		t.Fatalf("empty evidence run = %s/%v", result.Status, err)
	}
	if _, err := os.Stat(filepath.Join(root, ".test-results", "example", "result.json")); err != nil {
		t.Fatalf("relative evidence output was not rooted at the repository: %v", err)
	}
}

func TestStepWithoutStructuredArtifactUsesVisibleDiagnosticName(t *testing.T) {
	output := t.TempDir()
	manager := &dependencyManager{environment: os.Environ()}
	step, err := runStep(context.Background(), t.TempDir(), "protocol", commandSpec{Arguments: []string{"go", "version"}}, manager, output)
	if err != nil || step.Status != "pass" {
		t.Fatalf("diagnostic step = %s/%v", step.Status, err)
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || strings.HasPrefix(entries[0].Name(), ".") || !strings.HasSuffix(entries[0].Name(), ".log") {
		t.Fatalf("diagnostic files = %v; want one visible log", entries)
	}
}

func TestFailedStepPrintsDiagnosticExcerpt(t *testing.T) {
	goReport := strings.Join([]string{
		`{"Action":"run","Package":"example/pkg","Test":"TestPassing"}`,
		`{"Action":"output","Package":"example/pkg","Test":"TestPassing","Output":"    passing_test.go:3: passing noise\n"}`,
		`{"Action":"pass","Package":"example/pkg","Test":"TestPassing"}`,
		`{"Action":"run","Package":"example/pkg","Test":"TestBroken"}`,
		`{"Action":"output","Package":"example/pkg","Test":"TestBroken","Output":"    broken_test.go:9: deadline: context canceled\n"}`,
		`{"Action":"output","Package":"example/pkg","Test":"TestBroken","Output":"--- FAIL: TestBroken (0.08s)\n"}`,
		`{"Action":"fail","Package":"example/pkg","Test":"TestBroken"}`,
		`{"Action":"output","Package":"example/pkg","Output":"FAIL\texample/pkg\t0.1s\n"}`,
		`{"Action":"fail","Package":"example/pkg"}`,
	}, "\n")
	skipReport := strings.Join([]string{
		`{"Action":"run","Package":"example/pkg","Test":"TestNeedsFixture"}`,
		`{"Action":"output","Package":"example/pkg","Test":"TestNeedsFixture","Output":"    fixture_test.go:3: NATS fixture is not provisioned\n"}`,
		`{"Action":"output","Package":"example/pkg","Test":"TestNeedsFixture","Output":"--- SKIP: TestNeedsFixture (0.00s)\n"}`,
		`{"Action":"skip","Package":"example/pkg","Test":"TestNeedsFixture"}`,
		`{"Action":"run","Package":"example/pkg","Test":"TestPassing"}`,
		`{"Action":"output","Package":"example/pkg","Test":"TestPassing","Output":"    passing_test.go:3: passing noise\n"}`,
		`{"Action":"pass","Package":"example/pkg","Test":"TestPassing"}`,
		`{"Action":"pass","Package":"example/pkg"}`,
	}, "\n")
	longLine := "advisory 100 " + strings.Repeat("x", 987)
	var longLog strings.Builder
	for line := 1; line < 100; line++ {
		fmt.Fprintf(&longLog, "advisory %03d\n", line)
	}
	longLog.WriteString(longLine + "\n")
	// printf spells the command's own summary entry, so the command line in
	// the excerpt header does not contain it.
	const commandWrite = "step command wrote the summary"
	for _, tc := range []struct {
		name, kind, diagnostic string
		rejectSkip             bool
		want, absent           []string
	}{
		{
			name: "go test JSON", kind: "go-json", diagnostic: goReport,
			want:   []string{"    broken_test.go:9: deadline: context canceled", "--- FAIL: TestBroken (0.08s)", "FAIL\texample/pkg\t0.1s"},
			absent: []string{"passing noise", `"Action"`},
		},
		{
			name: "rejected Go test skip", kind: "go-json", diagnostic: skipReport, rejectSkip: true,
			want:   []string{"    fixture_test.go:3: NATS fixture is not provisioned", "--- SKIP: TestNeedsFixture (0.00s)"},
			absent: []string{"passing noise"},
		},
		{
			name: "plain log beyond the caps", diagnostic: longLog.String(),
			want:   []string{"advisory 001", "advisory 020", "... 20 lines omitted ...", "advisory 041", longLine[:300] + " ..."},
			absent: []string{"advisory 021", "advisory 040", longLine[:301]},
		},
		{
			name: "carriage return and control characters", diagnostic: "advisory text\r::warning::forged annotation\nbell\a nul\x00 kept\ttab\n",
			want:   []string{"::warning::forged annotation", "bell nul kept\ttab"},
			absent: []string{"\r", "\a", "\x00"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			fixture := filepath.Join(directory, "diagnostic")
			writeTestFile(t, directory, "diagnostic", tc.diagnostic)
			summary := filepath.Join(directory, "summary.md")
			t.Setenv("GITHUB_STEP_SUMMARY", summary)
			stdout, err := os.Create(filepath.Join(directory, "stdout"))
			if err != nil {
				t.Fatal(err)
			}
			saved := os.Stdout
			os.Stdout = stdout
			t.Cleanup(func() { os.Stdout = saved })
			spec := commandSpec{
				Arguments:  []string{"sh", "-c", `cat "$0"; if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then printf 'step %s wrote the summary\n' command >> "$GITHUB_STEP_SUMMARY"; fi; exit 1`, fixture},
				Kind:       tc.kind,
				RejectSkip: tc.rejectSkip,
			}
			if tc.kind != "" {
				spec.Artifact = "go.jsonl"
			}
			if _, err := runStep(context.Background(), directory, "go", spec, &dependencyManager{environment: os.Environ()}, t.TempDir()); err == nil {
				t.Fatal("failing step passed")
			}
			for name, path := range map[string]string{"job log": stdout.Name(), "job summary": summary} {
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, line := range tc.want {
					if !strings.Contains(string(body), line) {
						t.Errorf("%s lacks %q:\n%s", name, line, body)
					}
				}
				for _, line := range append(tc.absent, commandWrite) {
					if strings.Contains(string(body), line) {
						t.Errorf("%s contains %q:\n%s", name, line, body)
					}
				}
			}
			// The runner reads a bare carriage return as a line end, so every
			// line after the two header lines must carry the excerpt prefix.
			log, err := os.ReadFile(stdout.Name())
			if err != nil {
				t.Fatal(err)
			}
			printed := strings.Split(strings.TrimSuffix(strings.ReplaceAll(string(log), "\r", "\n"), "\n"), "\n")
			for _, line := range printed[min(2, len(printed)):] {
				if !strings.HasPrefix(line, "  | ") {
					t.Errorf("job log line %q lacks the excerpt prefix", line)
				}
			}
		})
	}
}

func TestEvidenceRecordsStepIntervalAndRunnerHost(t *testing.T) {
	before := time.Now()
	manager := &dependencyManager{environment: os.Environ()}
	step, err := runStep(context.Background(), t.TempDir(), "protocol", commandSpec{Arguments: []string{"go", "version"}}, manager, t.TempDir())
	after := time.Now()
	if err != nil {
		t.Fatal(err)
	}
	for name, at := range map[string]time.Time{"start": step.StartedAt, "end": step.FinishedAt} {
		if at.Before(before) || at.After(after) {
			t.Fatalf("step %s %s is outside the command's run [%s, %s]", name, at, before, after)
		}
	}

	t.Setenv("ImageOS", "ubuntu24")
	t.Setenv("ImageVersion", "20261004.327.1")
	if image := executionEnvelopeFromEnvironment().RunnerImage; image != "ubuntu24/20261004.327.1" {
		t.Fatalf("runner image = %q", image)
	}
}

func TestEvidenceRecordsGoCacheRestore(t *testing.T) {
	const key = "go-race-v1-Linux-go1.26.9-cc0123456789ab-sum-head"
	measured := GoCacheRestore{Key: key, Elapsed: 38 * time.Second, RestoredBytes: 6_400_000_000, DiskFreeBefore: 22_000_000_000, DiskFreeAfter: 15_600_000_000}
	for _, tc := range []struct {
		name, key, matched, outcome, match string
	}{
		{name: "job without the Go build cache"},
		{name: "no entry", key: key, outcome: "success", match: "miss"},
		{name: "an older entry by restore key", key: key, matched: "go-race-v1-Linux-go1.26.9-cc0123456789ab-sum-base", outcome: "success", match: "prefix"},
		{name: "the tested commit's entry", key: key, matched: strings.ToLower(key), outcome: "success", match: "exact"},
		{name: "too little free disk to restore", key: key, outcome: "skipped", match: "skipped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TETRAL_CI_GO_CACHE_KEY", tc.key)
			t.Setenv("TETRAL_CI_GO_CACHE_MATCHED_KEY", tc.matched)
			t.Setenv("TETRAL_CI_GO_CACHE_RESTORE_OUTCOME", tc.outcome)
			t.Setenv("TETRAL_CI_GO_CACHE_RESTORE_NS", "38000000000")
			t.Setenv("TETRAL_CI_GO_CACHE_RESTORED_BYTES", "6400000000")
			t.Setenv("TETRAL_CI_GO_CACHE_DISK_FREE_BEFORE", "22000000000")
			t.Setenv("TETRAL_CI_GO_CACHE_DISK_FREE_AFTER", "15600000000")
			got := executionEnvelopeFromEnvironment().GoCache
			if tc.key == "" {
				if got != nil {
					t.Fatalf("Go build cache restore = %+v; want none", got)
				}
				return
			}
			want := measured
			want.MatchedKey, want.Match = tc.matched, tc.match
			if got == nil || *got != want {
				t.Fatalf("Go build cache restore = %+v; want %+v", got, want)
			}
		})
	}
}

func TestProcessAndStructuredReportProduceOneDecisiveVerdict(t *testing.T) {
	processFailure := errors.New("exit status 1")
	testFailure := errors.New("test failed")
	if err := reconcileProcessAndReport(processFailure, testFailure, true); !errors.Is(err, testFailure) {
		t.Fatalf("complete failure report verdict = %v; want test failure", err)
	}
	if err := reconcileProcessAndReport(processFailure, invalidReport("truncated"), true); err == nil {
		t.Fatal("failed process with malformed report was accepted")
	} else {
		var malformed *reportError
		if !errors.As(err, &malformed) {
			t.Fatalf("malformed report verdict = %T; want apparatus failure", err)
		}
	}
	if err := reconcileProcessAndReport(processFailure, nil, true); err == nil {
		t.Fatal("failed process with passing report was accepted")
	} else {
		var malformed *reportError
		if !errors.As(err, &malformed) {
			t.Fatalf("incoherent report verdict = %T; want apparatus failure", err)
		}
	}
	if err := reconcileProcessAndReport(nil, testFailure, true); !errors.Is(err, testFailure) {
		t.Fatalf("zero process exit with failing report verdict = %v; want test failure", err)
	}
}

func TestGoJSONReconcilesSelectedRunnableUniverse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go.jsonl")
	writeReport(t, path, `{"Action":"run","Package":"example.test/p","Test":"TestOne"}
{"Action":"pass","Package":"example.test/p","Test":"TestOne"}
{"Action":"pass","Package":"example.test/p"}
`)
	if _, err := inspectGoJSON(path, true, []string{"example.test/p"}, []string{"TestOne"}); err != nil {
		t.Fatalf("complete report: %v", err)
	}
	writeReport(t, path, `{"Action":"run","Package":"example.test/p","Test":"TestOne"}
{"Action":"pass","Package":"example.test/p","Test":"TestOne"}
{"Action":"run","Package":"example.test/p","Test":"TestOne"}
{"Action":"pass","Package":"example.test/p","Test":"TestOne"}
{"Action":"pass","Package":"example.test/p"}
`)
	if _, err := inspectGoJSON(path, true, []string{"example.test/p"}, []string{"TestOne"}); err == nil {
		t.Fatal("duplicate runnable lifecycle was accepted")
	}
	writeReport(t, path, `{"Action":"run","Package":"example.test/p","Test":"TestOne"}
{"Action":"pass","Package":"example.test/p","Test":"TestOne"}
{"Action":"pass","Package":"example.test/p"}
`)
	if _, err := inspectGoJSON(path, true, []string{"example.test/p"}, []string{"TestOne", "TestOmitted"}); err == nil {
		t.Fatal("omitted selected runnable was accepted")
	}
	writeReport(t, path, `{"Action":"skip","Package":"example.test/p","Test":"TestOne"}
{"Action":"pass","Package":"example.test/p"}
`)
	if _, err := inspectGoJSON(path, true, []string{"example.test/p"}, []string{"TestOne"}); err == nil {
		t.Fatal("unexpected native skip was accepted")
	}
	writeReport(t, path, "not-json\n")
	if _, err := inspectGoJSON(path, true, []string{"example.test/p"}, []string{"TestOne"}); err == nil {
		t.Fatal("malformed Go JSON report was accepted")
	}
}

func TestGoJSONAcceptsExplicitNoTestPackageOnlyForCompileSelection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go.jsonl")
	writeReport(t, path, `{"Action":"start","Package":"example.test/compile"}
{"Action":"output","Package":"example.test/compile","Output":"?   example.test/compile [no test files]\n"}
{"Action":"skip","Package":"example.test/compile"}
`)
	if _, err := inspectGoJSON(path, true, []string{"example.test/compile"}, nil); err != nil {
		t.Fatalf("compile-only package report rejected: %v", err)
	}
	if _, err := inspectGoJSON(path, true, []string{"example.test/compile"}, []string{"TestExpected"}); err == nil {
		t.Fatal("no-test package report satisfied an expected runnable")
	}
}

func TestJUnitReconcilesSelectedFileUniverse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bun.xml")
	writeReport(t, path, `<?xml version="1.0"?><testsuites tests="1" failures="0" skipped="0"><testsuite file="pkg/one.test.ts" tests="1" failures="0" skipped="0"><testcase name="one"/></testsuite></testsuites>`)
	if _, err := inspectJUnit(path, true, []string{"pkg/one.test.ts"}); err != nil {
		t.Fatalf("complete report: %v", err)
	}
	if _, err := inspectJUnit(path, true, []string{"pkg/one.test.ts", "pkg/two.test.ts"}); err == nil {
		t.Fatal("omitted selected Bun file was accepted")
	}
	writeReport(t, path, `<?xml version="1.0"?><testsuites><testsuite file="pkg/one.test.ts"></testsuite></testsuites>`)
	if _, err := inspectJUnit(path, true, []string{"pkg/one.test.ts"}); err == nil {
		t.Fatal("empty JUnit report was accepted")
	}
	writeReport(t, path, `<?xml version="1.0"?><testsuites tests="2"><testsuite file="pkg/one.test.ts" tests="1"><testcase name="one"/></testsuite></testsuites>`)
	if _, err := inspectJUnit(path, true, []string{"pkg/one.test.ts"}); err == nil {
		t.Fatal("inconsistent JUnit totals were accepted")
	}
	writeReport(t, path, `<?xml version="1.0"?><testsuite file="pkg/one.test.ts" tests="1" failures="0" skipped="0"><testcase name="one"/></testsuite>`)
	if _, err := inspectJUnit(path, true, []string{"pkg/one.test.ts"}); err != nil {
		t.Fatalf("single-suite report: %v", err)
	}
	writeReport(t, path, `<?xml version="1.0"?><not-junit/>`)
	if _, err := inspectJUnit(path, true, nil); err == nil {
		t.Fatal("unsupported JUnit root was accepted")
	}
}

func writeReport(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
