package testinfra

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

type GateExpectation struct {
	Repository        string
	EventHeadSHA      string
	EventBaseSHA      string
	TestMergeSHA      string
	RequiredCheckSHA  string
	WorkflowSourceSHA string
	Workflow          string
	RunID             string
	RunAttempt        string
	Producers         []string
	ProducerJobs      map[string]string
	// GoShards lists the producers whose results are the Go selection's
	// shards; their results must partition that selection exactly once.
	GoShards []string
}

type GateVerdict struct {
	Status    string   `json:"status"`
	Producers []string `json:"producers"`
	Failures  []string `json:"failures,omitempty"`
}

// VerifyMergeGate accepts the latest result from every expected producer for
// the exact current execution envelope. GitHub preserves successful jobs when
// only failed jobs are rerun, so a complete verdict may span run attempts.
func VerifyMergeGate(root string, expectation GateExpectation) (GateVerdict, error) {
	if err := validateGateExpectation(expectation); err != nil {
		return GateVerdict{Status: "fail", Failures: []string{err.Error()}}, err
	}
	wanted := map[string]bool{}
	for _, producer := range expectation.Producers {
		if producer == "" || wanted[producer] {
			return GateVerdict{Status: "fail"}, fmt.Errorf("invalid duplicate gate producer %q", producer)
		}
		wanted[producer] = true
	}
	currentAttempt, _ := strconv.ParseUint(expectation.RunAttempt, 10, 32)
	type attemptedResult struct {
		attempt uint64
		result  Result
	}
	results := map[string]attemptedResult{}
	seenAttempts := map[string]map[uint64]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "result.json" {
			return nil
		}
		// Gate artifacts are downloaded beneath a runner-owned directory.
		//nolint:gosec
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		var result Result
		if unmarshalErr := json.Unmarshal(body, &result); unmarshalErr != nil {
			return fmt.Errorf("malformed result artifact %s: %w", filepath.Base(filepath.Dir(path)), unmarshalErr)
		}
		producer := result.Execution.Producer
		if !wanted[producer] {
			return fmt.Errorf("unexpected result producer %q", producer)
		}
		attempt, parseErr := strconv.ParseUint(result.Execution.RunAttempt, 10, 32)
		if parseErr != nil || attempt == 0 || attempt > currentAttempt {
			return fmt.Errorf("result producer %q has invalid run attempt", producer)
		}
		if reason := gateResultIdentityFailure(result, expectation); reason != "" {
			return fmt.Errorf("result producer %q has %s", producer, reason)
		}
		if seenAttempts[producer] == nil {
			seenAttempts[producer] = map[uint64]bool{}
		}
		if seenAttempts[producer][attempt] {
			return fmt.Errorf("duplicate result producer %q for run attempt %d", producer, attempt)
		}
		seenAttempts[producer][attempt] = true
		if existing, exists := results[producer]; !exists || attempt > existing.attempt {
			results[producer] = attemptedResult{attempt: attempt, result: result}
		}
		return nil
	})
	if err != nil {
		return GateVerdict{Status: "fail", Failures: []string{err.Error()}}, err
	}
	verdict := GateVerdict{Status: "pass"}
	for _, producer := range expectation.Producers {
		attempted, exists := results[producer]
		if !exists {
			verdict.Failures = append(verdict.Failures, "missing result: "+producer)
			continue
		}
		verdict.Producers = append(verdict.Producers, producer)
		if reason := gateResultFailure(attempted.result, expectation); reason != "" {
			verdict.Failures = append(verdict.Failures, producer+": "+reason)
		}
	}
	shards := map[string]Result{}
	for _, producer := range expectation.GoShards {
		if attempted, exists := results[producer]; exists {
			shards[producer] = attempted.result
		}
	}
	if len(shards) == len(expectation.GoShards) {
		verdict.Failures = append(verdict.Failures, goShardPartitionFailures(expectation.GoShards, shards)...)
	}
	sort.Strings(verdict.Producers)
	if len(verdict.Failures) > 0 {
		verdict.Status = "fail"
		return verdict, fmt.Errorf("merge gate rejected %d result(s)", len(verdict.Failures))
	}
	return verdict, nil
}

func validateGateExpectation(value GateExpectation) error {
	for name, item := range map[string]string{
		"repository": value.Repository, "event head": value.EventHeadSHA,
		"event base": value.EventBaseSHA, "test merge": value.TestMergeSHA,
		"required check": value.RequiredCheckSHA, "workflow source": value.WorkflowSourceSHA,
		"workflow": value.Workflow, "run ID": value.RunID, "run attempt": value.RunAttempt,
	} {
		if strings.TrimSpace(item) == "" {
			return fmt.Errorf("gate expectation is missing %s", name)
		}
	}
	if _, err := strconv.ParseUint(value.RunID, 10, 64); err != nil {
		return fmt.Errorf("gate run ID is malformed")
	}
	if attempt, err := strconv.ParseUint(value.RunAttempt, 10, 32); err != nil || attempt == 0 {
		return fmt.Errorf("gate run attempt is malformed")
	}
	if len(value.Producers) == 0 {
		return fmt.Errorf("gate has no expected producers")
	}
	if len(value.GoShards) == 0 {
		return fmt.Errorf("gate has no Go shards")
	}
	seen := map[string]bool{}
	for _, shard := range value.GoShards {
		if !slices.Contains(value.Producers, shard) || seen[shard] {
			return fmt.Errorf("gate Go shard %q is not one expected producer", shard)
		}
		seen[shard] = true
	}
	return nil
}

// goShardPartitionFailures checks that the Go shard results, one per listed
// producer, partition one complete Go selection exactly once. It trusts each
// result's recorded identity and digest instead of planning again.
func goShardPartitionFailures(producers []string, results map[string]Result) []string {
	var failures []string
	fail := func(format string, arguments ...any) {
		failures = append(failures, "go shards: "+fmt.Sprintf(format, arguments...))
	}
	indexes := map[int]string{}
	owners := map[[2]string]string{}
	selections := map[string]int{}
	var zeroTest []string
	var union []Selection
	exclusions, exclusionsFrom := "", ""
	for _, producer := range producers {
		result := results[producer]
		identity := result.Plan.GoSelection
		if identity == nil {
			fail("%s records no Go shard identity", producer)
			continue
		}
		if identity.ShardCount != len(producers) {
			fail("%s is shard %d of %d; want %d shards", producer, identity.ShardIndex, identity.ShardCount, len(producers))
		} else if identity.ShardIndex < 0 || identity.ShardIndex >= identity.ShardCount {
			fail("%s has shard index %d outside 0..%d", producer, identity.ShardIndex, identity.ShardCount-1)
		} else if other, duplicate := indexes[identity.ShardIndex]; duplicate {
			fail("%s and %s both ran shard %d", other, producer, identity.ShardIndex)
		} else {
			indexes[identity.ShardIndex] = producer
		}
		for _, selection := range result.Plan.Selections {
			if selection.Group != "go" {
				continue
			}
			if len(selection.Packages) != 1 {
				fail("%s holds a Go selection with %d packages", producer, len(selection.Packages))
				continue
			}
			name := selection.Packages[0]
			selections[name]++
			if len(selection.Tests) == 0 {
				zeroTest = append(zeroTest, name)
			}
			for _, test := range selection.Tests {
				key := [2]string{name, test}
				if other, duplicate := owners[key]; duplicate {
					fail("%s %s runs in both %s and %s", name, test, other, producer)
				}
				owners[key] = producer
			}
			union = append(union, selection)
		}
		// Without selections the digest covers only the exclusions and
		// cannot fail.
		shardExclusions, _ := goSelectionDigest(nil, result.Plan.Excluded)
		if exclusionsFrom == "" {
			exclusions, exclusionsFrom = shardExclusions, producer
		} else if shardExclusions != exclusions {
			fail("%s carries different Go exclusions than %s", producer, exclusionsFrom)
		}
	}
	for _, name := range zeroTest {
		if selections[name] != 1 {
			fail("zero-test package %s is selected %d times", name, selections[name])
		}
	}
	if len(failures) > 0 {
		return failures
	}
	digest, err := goSelectionDigest(union, results[producers[0]].Plan.Excluded)
	if err != nil {
		fail("%v", err)
		return failures
	}
	for _, producer := range producers {
		if results[producer].Plan.GoSelection.Digest != digest {
			fail("the union of the shards does not match the Go selection %s recorded", producer)
		}
	}
	return failures
}

func gateResultFailure(result Result, want GateExpectation) string {
	if result.Status != "pass" && result.Status != "not-applicable" {
		return "evidence status is " + result.Status
	}
	if result.Status == "not-applicable" {
		if len(result.Plan.Selections) != 0 || !hasDeclaredNotApplicable(result.Plan.Excluded) {
			return "not-applicable evidence has no declared selection disposition"
		}
	}
	if result.Plan.Revision.WorktreeDirty {
		return "tested worktree was dirty"
	}
	return gateResultIdentityFailure(result, want)
}

func gateResultIdentityFailure(result Result, want GateExpectation) string {
	pairs := [][3]string{
		{"repository", result.Execution.Repository, want.Repository},
		{"event_head_sha", result.Execution.EventHeadSHA, want.EventHeadSHA},
		{"event_base_sha", result.Execution.EventBaseSHA, want.EventBaseSHA},
		{"test_merge_sha", result.Execution.TestMergeSHA, want.TestMergeSHA},
		{"required_check_sha", result.Execution.RequiredCheckSHA, want.RequiredCheckSHA},
		{"workflow_source_sha", result.Execution.WorkflowSourceSHA, want.WorkflowSourceSHA},
		{"workflow", result.Execution.Workflow, want.Workflow},
		{"run_id", result.Execution.RunID, want.RunID},
		{"plan_head", result.Plan.Revision.Head, want.TestMergeSHA},
	}
	for _, pair := range pairs {
		if pair[1] != pair[2] {
			return pair[0] + " mismatch"
		}
	}
	if expectedJob := want.ProducerJobs[result.Execution.Producer]; expectedJob != "" && result.Execution.Job != expectedJob {
		return "job mismatch"
	}
	return ""
}

func hasDeclaredNotApplicable(exclusions []Exclusion) bool {
	for _, exclusion := range exclusions {
		if exclusion.Disposition == "not-applicable" && exclusion.Reason != "" {
			return true
		}
	}
	return false
}
