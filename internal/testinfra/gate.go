package testinfra

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
}

type GateVerdict struct {
	Status    string   `json:"status"`
	Producers []string `json:"producers"`
	Failures  []string `json:"failures,omitempty"`
}

// VerifyMergeGate accepts only one passing result from every expected producer
// for the exact current execution envelope. It never retries or reinterprets a
// failed producer.
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
	results := map[string]Result{}
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
		if _, exists := results[producer]; exists {
			return fmt.Errorf("duplicate result producer %q", producer)
		}
		results[producer] = result
		return nil
	})
	if err != nil {
		return GateVerdict{Status: "fail", Failures: []string{err.Error()}}, err
	}
	verdict := GateVerdict{Status: "pass"}
	for _, producer := range expectation.Producers {
		result, exists := results[producer]
		if !exists {
			verdict.Failures = append(verdict.Failures, "missing result: "+producer)
			continue
		}
		verdict.Producers = append(verdict.Producers, producer)
		if reason := gateResultFailure(result, expectation); reason != "" {
			verdict.Failures = append(verdict.Failures, producer+": "+reason)
		}
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
	return nil
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
	pairs := [][3]string{
		{"repository", result.Execution.Repository, want.Repository},
		{"event_head_sha", result.Execution.EventHeadSHA, want.EventHeadSHA},
		{"event_base_sha", result.Execution.EventBaseSHA, want.EventBaseSHA},
		{"test_merge_sha", result.Execution.TestMergeSHA, want.TestMergeSHA},
		{"required_check_sha", result.Execution.RequiredCheckSHA, want.RequiredCheckSHA},
		{"workflow_source_sha", result.Execution.WorkflowSourceSHA, want.WorkflowSourceSHA},
		{"workflow", result.Execution.Workflow, want.Workflow},
		{"run_id", result.Execution.RunID, want.RunID},
		{"run_attempt", result.Execution.RunAttempt, want.RunAttempt},
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
