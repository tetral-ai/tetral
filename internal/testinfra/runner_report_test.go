package testinfra

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

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
