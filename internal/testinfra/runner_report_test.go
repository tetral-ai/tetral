package testinfra

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

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
}

func TestSecuritySelectionAppliesDependencyAuditPolicy(t *testing.T) {
	tests := []struct {
		name       string
		mode       DependencyAuditMode
		paths      []string
		wantAudits int
	}{
		{name: "changed unrelated", mode: DependencyAuditChanged, paths: []string{"services/bridge/runtime_delivery.go"}},
		{name: "changed package manifest", mode: DependencyAuditChanged, paths: []string{"services/gateway/package.json"}, wantAudits: 2},
		{name: "changed lockfile", mode: DependencyAuditChanged, paths: []string{"services/agent-runtime/bun.lock"}, wantAudits: 2},
		{name: "changed audit runner", mode: DependencyAuditChanged, paths: []string{"scripts/run-bun-audit.sh"}, wantAudits: 2},
		{name: "always", mode: DependencyAuditAlways, wantAudits: 2},
		{name: "never", mode: DependencyAuditNever, paths: []string{"services/gateway/package.json"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := Plan{Revision: Revision{ChangedPaths: test.paths}}
			commands, err := commandsForSelection(plan, Selection{Group: "security"}, t.TempDir(), t.TempDir(), test.mode)
			if err != nil {
				t.Fatal(err)
			}
			audits := 0
			staticChecks := 0
			for _, command := range commands {
				if len(command.Arguments) > 0 && command.Arguments[0] == "./scripts/run-bun-audit.sh" {
					audits++
				}
				if slices.Equal(command.Arguments[:min(3, len(command.Arguments))], []string{"go", "test", "./integration/static"}) {
					staticChecks++
				}
			}
			if audits != test.wantAudits {
				t.Fatalf("online audits = %d; want %d", audits, test.wantAudits)
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
