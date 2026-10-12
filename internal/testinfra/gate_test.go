package testinfra

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
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

func TestVerifyMergeGateAcceptsLatestEvidenceAcrossRerunAttempts(t *testing.T) {
	want := gateFixtureExpectation()
	want.RunAttempt = "2"
	root := t.TempDir()

	// go-0 failed in attempt 1 and passed in attempt 2; the others, go-1
	// among them, are carried over from attempt 1.
	for _, producer := range want.Producers {
		carried := gateFixtureResult(want, producer)
		carried.Execution.RunAttempt = "1"
		if producer == "go-0" {
			carried.Status = "fail"
		}
		writeGateFixture(t, root, carried)
	}
	writeGateFixture(t, root, gateFixtureResult(want, "go-0"))

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

func TestVerifyMergeGateChecksGoShardPartition(t *testing.T) {
	selection := func(name string, tests ...string) Selection {
		return Selection{Group: "go", Packages: []string{name}, Tests: tests}
	}
	tests := []struct {
		name        string
		expectation func(*GateExpectation)
		mutate      func(map[string]*Result)
		// want is part of the failure the row must raise; empty means pass.
		want string
	}{
		{name: "the shards of one run beside go-static"},
		{name: "a test in no shard", want: "union of the shards does not match", mutate: func(results map[string]*Result) {
			results["go-1"].Plan.Selections = []Selection{selection("p/b", "TestB")}
		}},
		{name: "a test in two shards", want: "p/a TestA1 runs in both go-0 and go-1", mutate: func(results map[string]*Result) {
			results["go-1"].Plan.Selections[0] = selection("p/a", "TestA1", "TestA2")
		}},
		{name: "a missing zero-test package", want: "union of the shards does not match", mutate: func(results map[string]*Result) {
			results["go-0"].Plan.Selections = results["go-0"].Plan.Selections[:1]
		}},
		{name: "a duplicated zero-test package", want: "zero-test package p/zero is selected 2 times", mutate: func(results map[string]*Result) {
			results["go-1"].Plan.Selections = append(results["go-1"].Plan.Selections, selection("p/zero"))
		}},
		{name: "a Go selection without exactly one package", want: "go-1 holds a Go selection with 2 packages", mutate: func(results map[string]*Result) {
			results["go-1"].Plan.Selections = append(results["go-1"].Plan.Selections, Selection{Group: "go", Packages: []string{"p/x", "p/y"}})
		}},
		{name: "a duplicate shard index", want: "go-0 and go-1 both ran shard 0", mutate: func(results map[string]*Result) { results["go-1"].Plan.GoSelection.ShardIndex = 0 }},
		{name: "a shard index outside the count", want: "go-1 has shard index 2 outside 0..1", mutate: func(results map[string]*Result) { results["go-1"].Plan.GoSelection.ShardIndex = 2 }},
		{name: "a shard count other than the Go shard list's", want: "go-0 is shard 0 of 3; want 2 shards", mutate: func(results map[string]*Result) {
			results["go-0"].Plan.GoSelection.ShardCount, results["go-1"].Plan.GoSelection.ShardCount = 3, 3
		}},
		{name: "a digest mismatch", want: "does not match the Go selection go-0 recorded", mutate: func(results map[string]*Result) {
			results["go-0"].Plan.GoSelection.Digest = strings.Repeat("0", 64)
		}},
		{name: "different exclusions", want: "go-1 carries different Go exclusions than go-0", mutate: func(results map[string]*Result) { results["go-1"].Plan.Excluded = nil }},
		{name: "a shard without an identity", want: "go-1 records no Go shard identity", mutate: func(results map[string]*Result) { results["go-1"].Plan.GoSelection = nil }},
		{name: "an expectation without Go shards", want: "gate has no Go shards", expectation: func(want *GateExpectation) { want.GoShards = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := gateFixtureExpectation()
			results := map[string]*Result{}
			for _, producer := range want.Producers {
				result := gateFixtureResult(want, producer)
				results[producer] = &result
			}
			if test.expectation != nil {
				test.expectation(&want)
			}
			if test.mutate != nil {
				test.mutate(results)
			}
			root := t.TempDir()
			for _, producer := range want.Producers {
				writeGateFixture(t, root, *results[producer])
			}
			verdict, err := VerifyMergeGate(root, want)
			if test.want == "" {
				if err != nil {
					t.Fatalf("verdict = %#v error = %v", verdict, err)
				}
				return
			}
			if err == nil || !strings.Contains(strings.Join(verdict.Failures, "\n"), test.want) {
				t.Fatalf("verdict = %#v error = %v; want a failure naming %q", verdict, err, test.want)
			}
		})
	}
}

func gateFixtureExpectation() GateExpectation {
	return GateExpectation{
		Repository: "tetral-ai/tetral", EventHeadSHA: strings.Repeat("a", 40), EventBaseSHA: strings.Repeat("b", 40),
		TestMergeSHA: strings.Repeat("c", 40), RequiredCheckSHA: strings.Repeat("d", 40), WorkflowSourceSHA: strings.Repeat("e", 40),
		Workflow: "Pull Request Verification", RunID: "123", RunAttempt: "1", Producers: []string{"repository", "go-static", "go-0", "go-1"},
		ProducerJobs: map[string]string{"repository": "repository-sandbox-security-runtime", "go-static": "go-static-analysis", "go-0": "go-race", "go-1": "go-race"},
		GoShards:     []string{"go-0", "go-1"},
	}
}

// The fixture's Go selection is split into two shards: p/a by test, p/b and
// the zero-test package p/zero whole, and both carry the same exclusion.
var (
	gateFixtureGoSelection = []Selection{
		{Group: "go", Packages: []string{"p/a"}, Tests: []string{"TestA1", "TestA2"}},
		{Group: "go", Packages: []string{"p/b"}, Tests: []string{"TestB"}},
		{Group: "go", Packages: []string{"p/zero"}},
	}
	gateFixtureGoExclusions = []Exclusion{{Group: "go", Package: "p/a", Runnable: "TestRoot", Disposition: "delegated", Reason: "fixture"}}
	gateFixtureGoShards     = map[string][]Selection{
		"go-0": {{Group: "go", Packages: []string{"p/a"}, Tests: []string{"TestA1"}}, {Group: "go", Packages: []string{"p/zero"}}},
		"go-1": {{Group: "go", Packages: []string{"p/a"}, Tests: []string{"TestA2"}}, {Group: "go", Packages: []string{"p/b"}, Tests: []string{"TestB"}}},
	}
)

func gateFixtureResult(want GateExpectation, producer string) Result {
	result := Result{
		Status: "pass", Plan: Plan{Revision: Revision{Head: want.TestMergeSHA}},
		Execution: ExecutionEnvelope{Repository: want.Repository, EventHeadSHA: want.EventHeadSHA, EventBaseSHA: want.EventBaseSHA,
			TestMergeSHA: want.TestMergeSHA, RequiredCheckSHA: want.RequiredCheckSHA, WorkflowSourceSHA: want.WorkflowSourceSHA,
			Workflow: want.Workflow, RunID: want.RunID, RunAttempt: want.RunAttempt, Job: want.ProducerJobs[producer], Producer: producer},
	}
	if shard, ok := gateFixtureGoShards[producer]; ok {
		digest, err := goSelectionDigest(gateFixtureGoSelection, gateFixtureGoExclusions)
		if err != nil {
			panic(err)
		}
		result.Plan.Selections = slices.Clone(shard)
		result.Plan.Excluded = slices.Clone(gateFixtureGoExclusions)
		result.Plan.GoSelection = &GoShardIdentity{Digest: digest, ShardIndex: slices.Index(want.GoShards, producer), ShardCount: len(want.GoShards)}
	}
	return result
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
