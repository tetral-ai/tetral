package testinfra

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var pullRequestJobs = map[string]string{
	"repository-integrity":   "Repository Integrity",
	"go-static-analysis":     "Go Static Analysis",
	"go-race":                "Go Race (shard ${{ matrix.shard }})",
	"agent-runtime":          "Agent Runtime",
	"provider-gateway":       "Provider Gateway",
	"protocol-sdk":           "Protocol and SDK Compatibility",
	"deployment-definitions": "Deployment Definitions",
	"sandbox-image":          "Sandbox Image",
	"dependency-security":    "Dependency Security",
	"merge-gate":             "Merge Gate",
}

func VerifyPullRequestWorkflow(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "pull-request-verification.yml"))
	if err != nil {
		return err
	}
	workflow := document.Content[0]
	if scalar(mappingValue(workflow, "name")) != "Pull Request Verification" {
		return fmt.Errorf("pull request workflow has the wrong public name")
	}
	triggers := mappingValue(workflow, "on")
	if triggers == nil || mappingValue(triggers, "pull_request") == nil || mappingValue(triggers, "pull_request_target") != nil {
		return fmt.Errorf("pull request workflow has an unsafe or missing trigger")
	}
	pullRequest := mappingValue(triggers, "pull_request")
	if pullRequest.Kind == workflowYAMLMapping && (mappingValue(pullRequest, "paths") != nil || mappingValue(pullRequest, "paths-ignore") != nil) {
		return fmt.Errorf("pull request workflow must not use path filters")
	}
	permissions := mappingValue(workflow, "permissions")
	if !readOnlyPermissions(permissions, []string{"actions", "checks", "contents", "pull-requests"}) {
		return fmt.Errorf("pull request permissions are not read-only contents")
	}
	concurrency := mappingValue(workflow, "concurrency")
	group := scalar(mappingValue(concurrency, "group"))
	if !strings.Contains(group, "pull_request.number") || !strings.Contains(group, "pull_request.head.sha") || scalar(mappingValue(concurrency, "cancel-in-progress")) != "true" {
		return fmt.Errorf("pull request concurrency is not keyed by PR and exact head")
	}
	if strings.Contains(allScalars(workflow), "secrets.") {
		return fmt.Errorf("pull request workflow references secrets")
	}
	jobs := mappingValue(workflow, "jobs")
	if jobs == nil || jobs.Kind != workflowYAMLMapping {
		return fmt.Errorf("pull request workflow has no jobs")
	}
	for id, name := range pullRequestJobs {
		job := mappingValue(jobs, id)
		if job == nil || scalar(mappingValue(job, "name")) != name {
			return fmt.Errorf("pull request job %q is missing or misnamed", id)
		}
	}
	gate := mappingValue(jobs, "merge-gate")
	if scalar(mappingValue(gate, "if")) != "always()" {
		return fmt.Errorf("merge Gate is not unconditional")
	}
	expectedNeeds := []string{"repository-integrity", "go-static-analysis", "go-race", "agent-runtime", "provider-gateway", "protocol-sdk", "deployment-definitions", "sandbox-image", "dependency-security"}
	if !sameStrings(sequenceScalars(mappingValue(gate, "needs")), expectedNeeds) {
		return fmt.Errorf("merge Gate dependency set is incomplete")
	}
	gateEnvironment := mappingValue(gate, "env")
	if scalar(mappingValue(gateEnvironment, "GH_TOKEN")) != "${{ github.token }}" ||
		scalar(mappingValue(gateEnvironment, "TETRAL_CI_NEEDS")) != "${{ toJSON(needs) }}" {
		return fmt.Errorf("merge Gate is missing live read-only reconciliation inputs")
	}
	producers, err := workflowEvidenceProducers(jobs)
	if err != nil {
		return err
	}
	expectedProducers, err := LoadPRProducers()
	if err != nil {
		return err
	}
	if !sameStrings(producers, expectedProducers) {
		return fmt.Errorf("workflow producers do not match the PR producer inventory")
	}
	return nil
}

func VerifyLegacyShadowSidecar(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "actions", "legacy-shadow-sidecar", "action.yml"))
	if err != nil {
		return err
	}
	action := document.Content[0]
	steps := sequenceNodes(mappingValue(mappingValue(action, "runs"), "steps"))
	if len(steps) != 2 {
		return fmt.Errorf("legacy shadow sidecar must contain exactly record and upload steps")
	}
	for _, step := range steps {
		if scalar(mappingValue(step, "continue-on-error")) != "true" {
			return fmt.Errorf("legacy shadow sidecar can influence the authoritative verdict")
		}
	}
	if scalar(mappingValue(steps[1], "if")) != "always()" {
		return fmt.Errorf("legacy shadow upload is not attempted after metadata failure")
	}
	return nil
}

func readOnlyPermissions(permissions *workflowYAMLNode, names []string) bool {
	if permissions == nil || permissions.Kind != workflowYAMLMapping || len(permissions.Content) != len(names)*2 {
		return false
	}
	for _, name := range names {
		if scalar(mappingValue(permissions, name)) != "read" {
			return false
		}
	}
	return true
}

func VerifyWorkflowSkeletons(root string) error {
	wanted := map[string]struct {
		file        string
		trigger     string
		concurrency string
	}{
		"Pull Request Verification": {"pull-request-verification.yml", "pull_request", "pr-verification-"},
		"Main Branch Verification":  {"main-branch-verification.yml", "push", "main-verification-"},
		"Scheduled Verification":    {"scheduled-verification.yml", "schedule", "scheduled-verification-"},
	}
	for name, item := range wanted {
		document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", item.file))
		if err != nil {
			return err
		}
		workflow := document.Content[0]
		if scalar(mappingValue(workflow, "name")) != name || mappingValue(mappingValue(workflow, "on"), item.trigger) == nil {
			return fmt.Errorf("workflow %q has the wrong name or trigger", name)
		}
		if !strings.Contains(scalar(mappingValue(mappingValue(workflow, "concurrency"), "group")), item.concurrency) {
			return fmt.Errorf("workflow %q has the wrong concurrency namespace", name)
		}
	}
	return nil
}

func workflowEvidenceProducers(jobs *workflowYAMLNode) ([]string, error) {
	var producers []string
	for index := 0; index+1 < len(jobs.Content); index += 2 {
		jobID, job := jobs.Content[index].Value, jobs.Content[index+1]
		if jobID == "merge-gate" {
			continue
		}
		steps := mappingValue(job, "steps")
		found := ""
		for _, step := range sequenceNodes(steps) {
			if scalar(mappingValue(step, "uses")) != "./.github/actions/run-test-evidence" {
				continue
			}
			found = scalar(mappingValue(mappingValue(step, "with"), "producer"))
		}
		if found == "" {
			return nil, fmt.Errorf("evidence job %q has no repository runner producer", jobID)
		}
		if jobID == "go-race" {
			shards := sequenceScalars(mappingValue(mappingValue(mappingValue(job, "strategy"), "matrix"), "shard"))
			if !sameStrings(shards, []string{"0", "1", "2", "3"}) || found != "go-${{ matrix.shard }}" {
				return nil, fmt.Errorf("go Race does not own the exact four shards")
			}
			for _, shard := range shards {
				producers = append(producers, "go-"+shard)
			}
			continue
		}
		producers = append(producers, found)
	}
	return producers, nil
}

func parseYAMLFile(path string) (*workflowYAMLNode, error) {
	// The path is a repository-owned workflow selected by the policy owner.
	//nolint:gosec
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	document, err := parseWorkflowYAML(body, filepath.Base(path))
	if err != nil {
		return nil, err
	}
	if len(document.Content) != 1 || document.Content[0].Kind != workflowYAMLMapping {
		return nil, fmt.Errorf("%s is not a workflow mapping", filepath.Base(path))
	}
	return document, nil
}

func mappingValue(mapping *workflowYAMLNode, key string) *workflowYAMLNode {
	if mapping == nil || mapping.Kind != workflowYAMLMapping {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func sequenceNodes(sequence *workflowYAMLNode) []*workflowYAMLNode {
	if sequence == nil || sequence.Kind != workflowYAMLSequence {
		return nil
	}
	return sequence.Content
}

func sequenceScalars(sequence *workflowYAMLNode) []string {
	var values []string
	for _, node := range sequenceNodes(sequence) {
		values = append(values, scalar(node))
	}
	return values
}

func scalar(node *workflowYAMLNode) string {
	if node == nil || node.Kind != workflowYAMLScalar {
		return ""
	}
	return node.Value
}

func allScalars(node *workflowYAMLNode) string {
	if node == nil {
		return ""
	}
	value := node.Value
	for _, child := range node.Content {
		value += "\n" + allScalars(child)
	}
	return value
}

func sameStrings(left, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return strings.Join(left, "\x00") == strings.Join(right, "\x00")
}
