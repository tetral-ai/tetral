package testinfra

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
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

const pinnedSetupBunAction = "oven-sh/setup-bun@0c5077e51419868618aeaa5fe8019c62421857d6"

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
	environment := mappingValue(workflow, "env")
	if scalar(mappingValue(environment, "TETRAL_CI_TEST_MERGE_SHA")) != "${{ github.sha }}" ||
		scalar(mappingValue(environment, "TETRAL_CI_REQUIRED_CHECK_SHA")) != "${{ github.event.pull_request.head.sha }}" {
		return fmt.Errorf("pull request workflow conflates the tested merge revision with the required-check carrier")
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
		if !jobChecksOutFullHistory(job) {
			return fmt.Errorf("pull request job %q does not check out complete repository history", id)
		}
	}
	if !goRacePreparesIntegrationHost(mappingValue(jobs, "go-race")) {
		return fmt.Errorf("go Race does not prepare its integration host")
	}
	gate := mappingValue(jobs, "merge-gate")
	if scalar(mappingValue(gate, "if")) != "always()" {
		return fmt.Errorf("merge Gate is not unconditional")
	}
	if !mergeGateDownloadsAllRunAttempts(gate) {
		return fmt.Errorf("merge Gate does not reconcile evidence across rerun attempts")
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

func mergeGateDownloadsAllRunAttempts(job *yaml.Node) bool {
	for _, step := range sequenceNodes(mappingValue(job, "steps")) {
		if !strings.HasPrefix(scalar(mappingValue(step, "uses")), "actions/download-artifact@") {
			continue
		}
		return scalar(mappingValue(mappingValue(step, "with"), "pattern")) == "pr-evidence-*-${{ github.run_id }}-*"
	}
	return false
}

func VerifyMainBranchWorkflow(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "main-branch-verification.yml"))
	if err != nil {
		return err
	}
	jobs := mappingValue(document.Content[0], "jobs")
	integrated := mappingValue(jobs, "integrated-correctness")
	if integrated == nil || !jobEvidenceInputEquals(integrated, "group", "all") {
		return fmt.Errorf("main integrated correctness does not run the full evidence group")
	}
	if !jobEvidenceInputEquals(integrated, "needs-go-test-host", "true") {
		return fmt.Errorf("main integrated correctness does not prepare its Go integration host")
	}
	coverage := mappingValue(jobs, "coverage")
	if !jobRunContainsAll(coverage,
		"apparmor_restrict_unprivileged_userns=0",
		"unshare -Ur -m true",
	) {
		return fmt.Errorf("main coverage does not prepare its Go integration host")
	}
	return nil
}

func VerifyScheduledWorkflow(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "scheduled-verification.yml"))
	if err != nil {
		return err
	}
	jobs := mappingValue(document.Content[0], "jobs")
	job := mappingValue(jobs, "concurrency-history")
	if job == nil {
		return fmt.Errorf("scheduled concurrency history job is missing")
	}
	setupIndex := -1
	runIndex := -1
	for index, step := range sequenceNodes(mappingValue(job, "steps")) {
		if scalar(mappingValue(step, "uses")) == pinnedSetupBunAction &&
			scalar(mappingValue(mappingValue(step, "with"), "bun-version-file")) == "services/gateway/package.json" {
			setupIndex = index
		}
		if strings.Contains(scalar(mappingValue(step, "run")), "go run ./internal/testinfra/cmd/tetral-scheduled") {
			runIndex = index
		}
	}
	if setupIndex < 0 || runIndex < 0 || setupIndex >= runIndex {
		return fmt.Errorf("scheduled concurrency history must install the pinned Bun version before running its repository runner")
	}
	return nil
}

func jobChecksOutFullHistory(job *workflowYAMLNode) bool {
	for _, step := range sequenceNodes(mappingValue(job, "steps")) {
		if scalar(mappingValue(step, "uses")) != "actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10" {
			continue
		}
		inputs := mappingValue(step, "with")
		return scalar(mappingValue(inputs, "fetch-depth")) == "0" && scalar(mappingValue(inputs, "persist-credentials")) == "false"
	}
	return false
}

func goRacePreparesIntegrationHost(job *workflowYAMLNode) bool {
	return jobEvidenceInputEquals(job, "needs-go-test-host", "true")
}

func jobRunContainsAll(job *workflowYAMLNode, fragments ...string) bool {
	for _, step := range sequenceNodes(mappingValue(job, "steps")) {
		run := scalar(mappingValue(step, "run"))
		matched := true
		for _, fragment := range fragments {
			matched = matched && strings.Contains(run, fragment)
		}
		if matched {
			return true
		}
	}
	return false
}

func jobEvidenceInputEquals(job *workflowYAMLNode, name, value string) bool {
	for _, step := range sequenceNodes(mappingValue(job, "steps")) {
		if scalar(mappingValue(step, "uses")) != "./.github/actions/run-test-evidence" {
			continue
		}
		return scalar(mappingValue(mappingValue(step, "with"), name)) == value
	}
	return false
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
			if !jobEvidenceInputEquals(job, "shard-count", fmt.Sprintf("%d", len(shards))) {
				return nil, fmt.Errorf("go Race shard count does not match its matrix cardinality")
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
