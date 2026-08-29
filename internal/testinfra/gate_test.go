package testinfra

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyMergeGateAcceptsOnlyCompleteExactExecution(t *testing.T) {
	want := gateFixtureExpectation()
	root := t.TempDir()
	for _, producer := range want.Producers {
		writeGateFixture(t, root, gateFixtureResult(want, producer))
	}
	verdict, err := VerifyMergeGate(root, want)
	if err != nil || verdict.Status != "pass" || len(verdict.Producers) != len(want.Producers) {
		t.Fatalf("verdict = %#v error = %v", verdict, err)
	}
}

func TestVerifyMergeGateRejectsEveryNonAcceptedResultClass(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Result)
	}{
		{name: "failure", mutate: func(result *Result) { result.Status = "fail" }},
		{name: "timeout", mutate: func(result *Result) { result.Status = "timed-out" }},
		{name: "cancelled", mutate: func(result *Result) { result.Status = "cancelled" }},
		{name: "unexpected skip", mutate: func(result *Result) { result.Status = "skipped" }},
		{name: "apparatus", mutate: func(result *Result) { result.Status = "apparatus-failed" }},
		{name: "stale head", mutate: func(result *Result) { result.Execution.EventHeadSHA = strings.Repeat("1", 40) }},
		{name: "wrong merge", mutate: func(result *Result) { result.Execution.TestMergeSHA = strings.Repeat("2", 40) }},
		{name: "wrong check carrier", mutate: func(result *Result) { result.Execution.RequiredCheckSHA = strings.Repeat("3", 40) }},
		{name: "wrong run", mutate: func(result *Result) { result.Execution.RunID = "42" }},
		{name: "wrong attempt", mutate: func(result *Result) { result.Execution.RunAttempt = "2" }},
		{name: "wrong workflow source", mutate: func(result *Result) { result.Execution.WorkflowSourceSHA = strings.Repeat("4", 40) }},
		{name: "wrong job", mutate: func(result *Result) { result.Execution.Job = "other-job" }},
		{name: "undeclared not applicable", mutate: func(result *Result) { result.Status = "not-applicable" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := gateFixtureExpectation()
			root := t.TempDir()
			for index, producer := range want.Producers {
				result := gateFixtureResult(want, producer)
				if index == 0 {
					test.mutate(&result)
				}
				writeGateFixture(t, root, result)
			}
			if _, err := VerifyMergeGate(root, want); err == nil {
				t.Fatal("non-accepted result passed")
			}
		})
	}
}

func TestVerifyMergeGateAcceptsDeclaredNotApplicable(t *testing.T) {
	want := gateFixtureExpectation()
	root := t.TempDir()
	for _, producer := range want.Producers {
		result := gateFixtureResult(want, producer)
		if producer == "repository" {
			result.Status = "not-applicable"
			result.Plan.Excluded = []Exclusion{{Group: producer, Disposition: "not-applicable", Reason: "declared by selection plan"}}
		}
		writeGateFixture(t, root, result)
	}
	if _, err := VerifyMergeGate(root, want); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyMergeGateRejectsMissingMalformedDuplicateAndUnexpectedArtifacts(t *testing.T) {
	want := gateFixtureExpectation()
	t.Run("missing", func(t *testing.T) {
		root := t.TempDir()
		writeGateFixture(t, root, gateFixtureResult(want, want.Producers[0]))
		if _, err := VerifyMergeGate(root, want); err == nil {
			t.Fatal("missing result passed")
		}
	})
	t.Run("malformed", func(t *testing.T) {
		root := t.TempDir()
		directory := filepath.Join(root, "broken")
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "result.json"), []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyMergeGate(root, want); err == nil {
			t.Fatal("malformed result passed")
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		root := t.TempDir()
		result := gateFixtureResult(want, want.Producers[0])
		writeGateFixture(t, root, result)
		result.Plan.CreatedAt = result.Plan.CreatedAt.Add(1)
		writeGateFixture(t, root, result)
		if _, err := VerifyMergeGate(root, want); err == nil {
			t.Fatal("duplicate result passed")
		}
	})
	t.Run("unexpected", func(t *testing.T) {
		root := t.TempDir()
		writeGateFixture(t, root, gateFixtureResult(want, "unknown"))
		if _, err := VerifyMergeGate(root, want); err == nil {
			t.Fatal("unexpected result passed")
		}
	})
}

func gateFixtureExpectation() GateExpectation {
	return GateExpectation{
		Repository: "tetral-ai/tetral", EventHeadSHA: strings.Repeat("a", 40), EventBaseSHA: strings.Repeat("b", 40),
		TestMergeSHA: strings.Repeat("c", 40), RequiredCheckSHA: strings.Repeat("d", 40), WorkflowSourceSHA: strings.Repeat("e", 40),
		Workflow: "Pull Request Verification", RunID: "123", RunAttempt: "1", Producers: []string{"repository", "go-0"},
		ProducerJobs: map[string]string{"repository": "repository-integrity", "go-0": "go-race"},
	}
}

func gateFixtureResult(want GateExpectation, producer string) Result {
	return Result{
		Status: "pass", Plan: Plan{Revision: Revision{Head: want.TestMergeSHA}},
		Execution: ExecutionEnvelope{Repository: want.Repository, EventHeadSHA: want.EventHeadSHA, EventBaseSHA: want.EventBaseSHA,
			TestMergeSHA: want.TestMergeSHA, RequiredCheckSHA: want.RequiredCheckSHA, WorkflowSourceSHA: want.WorkflowSourceSHA,
			Workflow: want.Workflow, RunID: want.RunID, RunAttempt: want.RunAttempt, Job: want.ProducerJobs[producer], Producer: producer},
	}
}

func writeGateFixture(t *testing.T, root string, result Result) {
	t.Helper()
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, result.Execution.Producer+"-"+string(rune(len(mustReadDirectories(t, root))+'a')))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "result.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustReadDirectories(t *testing.T, root string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}
