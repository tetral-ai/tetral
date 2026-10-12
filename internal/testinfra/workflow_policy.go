package testinfra

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

var pullRequestJobs = map[string]string{
	"repository-sandbox-security-runtime": "Repository, Sandbox Image, Security and Runtime",
	"go-static-analysis":                  "Go Static Analysis",
	"go-race":                             "Go Race (shard ${{ matrix.shard }})",
	"provider-gateway":                    "Provider Gateway",
	"deployment-protocol":                 "Deployment and Protocol",
	"merge-gate":                          "Merge Gate",
}

const (
	pinnedSetupBunAction        = "oven-sh/setup-bun@0c5077e51419868618aeaa5fe8019c62421857d6"
	evidenceAction              = "./.github/actions/run-test-evidence"
	followOnEvidenceCondition   = "${{ !cancelled() }}"
	evidenceStepBackstopMinutes = 3 // setup before the run, the 1 min kill-after and the upload
	sharedJobOverheadMinutes    = 3
)

func VerifyPullRequestWorkflow(root string) error {
	inventory, err := LoadPRProducers()
	if err != nil {
		return err
	}
	return verifyPullRequestWorkflow(root, inventory)
}

func verifyPullRequestWorkflow(root string, inventory PRProducerInventory) error {
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
	evidence, err := readPullRequestEvidence(jobs, inventory)
	if err != nil {
		return err
	}
	if !evidence.groupInputEquals("deployment", "needs-bun", "true") {
		return fmt.Errorf("deployment verification does not install Bun for the owning configuration parsers")
	}
	if !evidence.groupInputEquals("security", "dependency-audit", string(DependencyAuditChanged)) {
		return fmt.Errorf("pull request dependency security does not audit changed dependency graphs")
	}
	gate := mappingValue(jobs, "merge-gate")
	if scalar(mappingValue(gate, "if")) != "always()" {
		return fmt.Errorf("merge Gate is not unconditional")
	}
	if !mergeGateDownloadsAllRunAttempts(gate) {
		return fmt.Errorf("merge Gate does not reconcile evidence across rerun attempts")
	}
	if !sameStrings(sequenceScalars(mappingValue(gate, "needs")), PRUpstreamJobs()) {
		return fmt.Errorf("merge Gate dependency set is incomplete")
	}
	gateEnvironment := mappingValue(gate, "env")
	if scalar(mappingValue(gateEnvironment, "GH_TOKEN")) != "${{ github.token }}" ||
		scalar(mappingValue(gateEnvironment, "TETRAL_CI_NEEDS")) != "${{ toJSON(needs) }}" {
		return fmt.Errorf("merge Gate is missing live read-only reconciliation inputs")
	}
	if !sameStrings(evidence.producers, inventory.Producers) {
		return fmt.Errorf("workflow producers do not match the PR producer inventory")
	}
	if !sameStrings(evidence.goShards, inventory.GoShards) {
		return fmt.Errorf("go Race producers do not match the PR inventory's Go shards")
	}
	return nil
}

func mergeGateDownloadsAllRunAttempts(job *workflowYAMLNode) bool {
	for _, step := range sequenceNodes(mappingValue(job, "steps")) {
		if !strings.HasPrefix(scalar(mappingValue(step, "uses")), "actions/download-artifact@") {
			continue
		}
		return scalar(mappingValue(mappingValue(step, "with"), "pattern")) == "pr-evidence-*-${{ github.run_id }}-*"
	}
	return false
}

func VerifyMainBranchWorkflow(root string) error {
	inventory, err := LoadPRProducers()
	if err != nil {
		return err
	}
	return verifyMainBranchWorkflow(root, inventory)
}

func verifyMainBranchWorkflow(root string, inventory PRProducerInventory) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "main-branch-verification.yml"))
	if err != nil {
		return err
	}
	jobs := mappingValue(document.Content[0], "jobs")
	if mappingValue(jobs, "integrated-correctness") != nil {
		return fmt.Errorf("main verification retains the serial integrated evidence job")
	}
	if mappingValue(jobs, "coverage") != nil {
		return fmt.Errorf("main verification runs report-only coverage on every push")
	}
	verification := mappingValue(jobs, "verification")
	if verification == nil || scalar(mappingValue(verification, "name")) != "${{ matrix.name }}" {
		return fmt.Errorf("main verification matrix is missing")
	}
	if scalar(mappingValue(mappingValue(verification, "strategy"), "fail-fast")) != "false" {
		return fmt.Errorf("main verification matrix is not fail-independent")
	}
	include := mappingValue(mappingValue(mappingValue(verification, "strategy"), "matrix"), "include")
	type matrixEntry struct {
		name, producer, bun, buf, helm, lint string
	}
	wantGroups := map[string]matrixEntry{
		"repository":    {name: "Repository Integrity", producer: "repository", bun: "false", buf: "false", helm: "false", lint: "false"},
		"go-static":     {name: "Go Static Analysis", producer: "go-static", bun: "false", buf: "false", helm: "false", lint: "true"},
		"runtime":       {name: "Agent Runtime", producer: "runtime", bun: "true", buf: "false", helm: "false", lint: "false"},
		"gateway":       {name: "Provider Gateway", producer: "gateway", bun: "true", buf: "false", helm: "false", lint: "false"},
		"protocol":      {name: "Protocol and SDK Compatibility", producer: "protocol", bun: "true", buf: "true", helm: "false", lint: "false"},
		"deployment":    {name: "Deployment Definitions", producer: "deployment", bun: "true", buf: "false", helm: "true", lint: "false"},
		"sandbox-image": {name: "Sandbox Image", producer: "sandbox-image", bun: "false", buf: "false", helm: "false", lint: "false"},
		"security":      {name: "Dependency Security", producer: "security", bun: "true", buf: "false", helm: "false", lint: "false"},
	}
	gotGroups := map[string]matrixEntry{}
	for _, entry := range sequenceNodes(include) {
		group := scalar(mappingValue(entry, "group"))
		got := matrixEntry{
			name: scalar(mappingValue(entry, "name")), producer: scalar(mappingValue(entry, "producer")),
			bun: scalar(mappingValue(entry, "needs_bun")), buf: scalar(mappingValue(entry, "needs_buf")),
			helm: scalar(mappingValue(entry, "needs_helm")), lint: scalar(mappingValue(entry, "needs_lint")),
		}
		if group == "" || got.producer == "" || gotGroups[group].producer != "" {
			return fmt.Errorf("main verification matrix contains an invalid or duplicate evidence group %q", group)
		}
		gotGroups[group] = got
	}
	if len(gotGroups) != len(wantGroups) {
		return fmt.Errorf("main verification matrix has %d evidence groups; want %d", len(gotGroups), len(wantGroups))
	}
	for group, want := range wantGroups {
		if got := gotGroups[group]; got != want {
			return fmt.Errorf("main verification group %q = %+v; want %+v", group, got, want)
		}
	}
	if !jobEvidenceInputEquals(verification, "group", "${{ matrix.group }}") ||
		!jobEvidenceInputEquals(verification, "producer", "${{ matrix.producer }}") ||
		!jobEvidenceInputEquals(verification, "needs-bun", "${{ matrix.needs_bun }}") ||
		!jobEvidenceInputEquals(verification, "needs-buf", "${{ matrix.needs_buf }}") ||
		!jobEvidenceInputEquals(verification, "needs-helm", "${{ matrix.needs_helm }}") ||
		!jobEvidenceInputEquals(verification, "needs-lint", "${{ matrix.needs_lint }}") ||
		!jobEvidenceInputEquals(verification, "dependency-audit", string(DependencyAuditNever)) {
		return fmt.Errorf("main verification matrix does not preserve evidence identity or offline audit policy")
	}
	race := mappingValue(jobs, "go-race")
	shards := sequenceScalars(mappingValue(mappingValue(mappingValue(race, "strategy"), "matrix"), "shard"))
	if !sameStrings(shards, goShardMatrix(inventory)) ||
		!jobEvidenceInputEquals(race, "producer", "go-${{ matrix.shard }}") ||
		!jobEvidenceInputEquals(race, "shard-count", strconv.Itoa(len(inventory.GoShards))) ||
		!goRacePreparesIntegrationHost(race) {
		return fmt.Errorf("main Go Race does not own the %d complete integration-ready shards go_shards lists", len(inventory.GoShards))
	}
	if !jobEvidenceInputEquals(race, "dependency-audit", string(DependencyAuditNever)) {
		return fmt.Errorf("main Go Race repeats the online dependency audit")
	}
	if !jobChecksOutFullHistory(verification) || !jobChecksOutFullHistory(race) {
		return fmt.Errorf("main parallel verification does not check out complete repository history")
	}
	return nil
}

func VerifyScheduledWorkflow(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "scheduled-verification.yml"))
	if err != nil {
		return err
	}
	workflow := document.Content[0]
	schedules := mappingValue(mappingValue(workflow, "on"), "schedule")
	var crons []string
	for _, schedule := range sequenceNodes(schedules) {
		crons = append(crons, scalar(mappingValue(schedule, "cron")))
	}
	if !sameStrings(crons, []string{"41 6 * * *", "17 7 * * 1"}) {
		return fmt.Errorf("scheduled verification must declare one daily health run and one weekly concurrency run")
	}
	jobs := mappingValue(workflow, "jobs")
	job := mappingValue(jobs, "concurrency-history")
	if job == nil {
		return fmt.Errorf("scheduled concurrency history job is missing")
	}
	if scalar(mappingValue(job, "if")) != "github.event_name == 'workflow_dispatch' || github.event.schedule == '17 7 * * 1'" {
		return fmt.Errorf("scheduled concurrency history is not limited to manual and weekly runs")
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
	const manualOrDaily = "github.event_name == 'workflow_dispatch' || github.event.schedule == '41 6 * * *'"
	health := mappingValue(jobs, "compatibility-health")
	if health == nil || scalar(mappingValue(health, "if")) != manualOrDaily {
		return fmt.Errorf("scheduled compatibility health is not limited to manual and daily runs")
	}
	if !jobEvidenceInputEquals(health, "dependency-audit", string(DependencyAuditNever)) {
		return fmt.Errorf("scheduled compatibility health runs the online dependency audit that dependency advisories owns")
	}
	advisories := mappingValue(jobs, "dependency-advisories")
	if advisories == nil || scalar(mappingValue(advisories, "if")) != manualOrDaily {
		return fmt.Errorf("scheduled dependency advisories is not limited to manual and daily runs")
	}
	if !jobEvidenceInputEquals(advisories, "group", "security") ||
		!jobEvidenceInputEquals(advisories, "dependency-audit", string(DependencyAuditAlways)) {
		return fmt.Errorf("scheduled dependency advisories does not always run the online dependency audit")
	}
	coverage := mappingValue(jobs, "coverage")
	if coverage == nil || scalar(mappingValue(coverage, "if")) != manualOrDaily {
		return fmt.Errorf("scheduled coverage is not limited to manual and daily runs")
	}
	if !jobRunContainsAll(coverage,
		"apparmor_restrict_unprivileged_userns=0",
		"unshare -Ur -m true",
	) {
		return fmt.Errorf("scheduled coverage does not prepare its Go integration host")
	}
	if !coveragePreparesRenderedTransport(coverage) {
		return fmt.Errorf("scheduled coverage does not install pinned Helm before rendered transport fixtures")
	}
	return nil
}

const (
	// CI Benchmark's produce job writes side A's Go build cache entry into the
	// dispatched branch's scope, never into main's.
	benchmarkProduceGuard = "${{ inputs.produce-cache && github.ref != 'refs/heads/main' }}"
	// Both sides start together once produce succeeded or was skipped; after a
	// failed or cancelled produce, B would measure a cache miss.
	benchmarkSideGuard      = "${{ !cancelled() && needs.resolve.result == 'success' && contains(fromJSON('[\"success\",\"skipped\"]'), needs.produce.result) }}"
	benchmarkGoShardsScript = `set -euo pipefail
for side in a b; do
  sha="$A_SHA"
  if [[ "$side" == b ]]; then
    sha="$B_SHA"
  fi
  if ! count="$(git show "${sha}:internal/testinfra/pr_producers.json" | jq -e '.go_shards | arrays | length | select(. > 0)')"; then
    echo "::error::$sha lists no go_shards in internal/testinfra/pr_producers.json"
    exit 1
  fi
  echo "${side}-shard-count=$count" >> "$GITHUB_OUTPUT"
  echo "${side}-shards=$(jq -cn --argjson count "$count" '[range($count)]')" >> "$GITHUB_OUTPUT"
  echo "Side ${side^^} runs $count Go Race shards"
done
`
)

// VerifyBenchmarkWorkflow keeps CI Benchmark a read-only measurement whose two
// sides run Pull Request Verification's Go Race evidence on their own commits,
// each with its commit's go_shards. Its only cache writer is the guarded
// produce job.
func VerifyBenchmarkWorkflow(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "ci-benchmark.yml"))
	if err != nil {
		return err
	}
	workflow := document.Content[0]
	triggers := mappingValue(workflow, "on")
	if triggers == nil || triggers.Kind != workflowYAMLMapping || len(triggers.Content) != 2 || mappingValue(triggers, "workflow_dispatch") == nil {
		return fmt.Errorf("benchmark workflow must be triggered by workflow_dispatch only")
	}
	if !readOnlyPermissions(mappingValue(workflow, "permissions"), []string{"actions", "contents"}) {
		return fmt.Errorf("benchmark permissions are not exactly read-only actions and contents")
	}
	if strings.Contains(allScalars(workflow), "secrets.") {
		return fmt.Errorf("benchmark workflow references secrets")
	}
	if scalar(mappingValue(workflow, "cache-mode")) != "read" {
		return fmt.Errorf("benchmark workflow does not restrict the Actions cache to read")
	}
	jobs := mappingValue(workflow, "jobs")
	if jobs == nil || jobs.Kind != workflowYAMLMapping {
		return fmt.Errorf("benchmark workflow has no jobs")
	}
	for index := 0; index+1 < len(jobs.Content); index += 2 {
		jobID, job := jobs.Content[index].Value, jobs.Content[index+1]
		if mappingValue(job, "permissions") != nil {
			return fmt.Errorf("benchmark job %q overrides the workflow permissions", jobID)
		}
		if jobID == "produce" {
			if scalar(mappingValue(job, "if")) != benchmarkProduceGuard || scalar(mappingValue(job, "cache-mode")) != "write" {
				return fmt.Errorf("benchmark produce job must write the cache only when produce-cache is set off main")
			}
		} else if mode := mappingValue(job, "cache-mode"); mode != nil && scalar(mode) != "read" {
			return fmt.Errorf("benchmark job %q has write-capable cache access", jobID)
		}
		if !jobChecksOutFullHistory(job) {
			return fmt.Errorf("benchmark job %q does not check out complete history without persisted credentials", jobID)
		}
		for _, step := range sequenceNodes(mappingValue(job, "steps")) {
			uses := scalar(mappingValue(step, "uses"))
			// A local action must come from the job's own checkout: "$/" would
			// resolve to the running commit and run side B's action for side A.
			if uses != "" && !strings.Contains(uses, "@") && !strings.HasPrefix(uses, "./") {
				return fmt.Errorf("benchmark job %q uses local action %q outside its checkout", jobID, uses)
			}
		}
	}
	produceInput := mappingValue(mappingValue(mappingValue(triggers, "workflow_dispatch"), "inputs"), "produce-cache")
	if mappingValue(jobs, "produce") == nil || scalar(mappingValue(produceInput, "type")) != "boolean" || scalar(mappingValue(produceInput, "default")) != "false" {
		return fmt.Errorf("benchmark workflow must produce the cache only on an explicit produce-cache input")
	}
	pullRequest, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "pull-request-verification.yml"))
	if err != nil {
		return err
	}
	pullRequestRace := mappingValue(mappingValue(pullRequest.Content[0], "jobs"), "go-race")
	wantInputs := evidenceStepInputs(pullRequestRace)
	if wantInputs == nil {
		return fmt.Errorf("pull request Go Race has no evidence step")
	}
	resolve := mappingValue(jobs, "resolve")
	shardStep := stepWithID(sequenceNodes(mappingValue(resolve, "steps")), "shards")
	shardEnvironment := mappingValue(shardStep, "env")
	if scalar(mappingValue(shardStep, "run")) != benchmarkGoShardsScript ||
		scalar(mappingValue(shardEnvironment, "A_SHA")) != "${{ steps.commits.outputs.base }}" ||
		scalar(mappingValue(shardEnvironment, "B_SHA")) != "${{ steps.commits.outputs.head }}" {
		return fmt.Errorf("benchmark resolve job does not read each side's go_shards from its own commit")
	}
	prefixes := map[string]bool{}
	for _, side := range []string{"a", "b"} {
		jobID := side + "-go-race"
		job := mappingValue(jobs, jobID)
		if !slices.Equal(sequenceScalars(mappingValue(job, "needs")), []string{"resolve", "produce"}) || scalar(mappingValue(job, "if")) != benchmarkSideGuard {
			return fmt.Errorf("benchmark job %q does not wait for produce", jobID)
		}
		inputs := evidenceStepInputs(job)
		if inputs == nil {
			return fmt.Errorf("benchmark job %q has no evidence step from its checkout", jobID)
		}
		prefix := inputs["artifact-prefix"]
		if prefix == "" || prefixes[prefix] {
			return fmt.Errorf("benchmark job %q needs its own artifact prefix", jobID)
		}
		prefixes[prefix] = true
		for _, output := range []string{side + "-shards", side + "-shard-count"} {
			if scalar(mappingValue(mappingValue(resolve, "outputs"), output)) != "${{ steps.shards.outputs."+output+" }}" {
				return fmt.Errorf("benchmark resolve job output %q does not come from its go_shards step", output)
			}
		}
		if scalar(mappingValue(mappingValue(mappingValue(job, "strategy"), "matrix"), "shard")) != "${{ fromJSON(needs.resolve.outputs."+side+"-shards) }}" {
			return fmt.Errorf("benchmark job %q does not run the Go shards its commit lists", jobID)
		}
		if inputs["shard-count"] != "${{ needs.resolve.outputs."+side+"-shard-count }}" {
			return fmt.Errorf("benchmark job %q does not pass the Go shard count its commit lists", jobID)
		}
		want := maps.Clone(wantInputs)
		for _, name := range []string{"artifact-prefix", "shard-count"} {
			delete(inputs, name)
			delete(want, name)
		}
		if !maps.Equal(inputs, want) {
			return fmt.Errorf("benchmark job %q Go evidence inputs differ from pull request Go Race", jobID)
		}
	}
	return nil
}

const (
	goBuildCacheKey         = "${{ steps.go-cache-key.outputs.key }}"
	goBuildCacheRestoreKeys = "${{ steps.go-cache-key.outputs.modules-prefix }}\n${{ steps.go-cache-key.outputs.compiler-prefix }}\n${{ steps.go-cache-key.outputs.toolchain-prefix }}\n"
	goBuildCacheMiss        = "steps.lookup.outputs.cache-hit != 'true'"
	goBuildCacheInput       = "inputs.go-cache == 'true'"
	// The restore is skipped on a runner with less than 25 GB free disk, or
	// when the measurement failed.
	goBuildCacheRestoreGuard = goBuildCacheInput + " && fromJSON(steps.go-cache-before.outputs.disk-free || '0') >= 25000000000"
)

// goBuildCacheWriter names a job allowed to save to the Actions cache, the
// commit it checks out (empty: the triggering one) and the exact step that
// runs the repository's build script in it.
type goBuildCacheWriter struct {
	file, job string
	ref       string
	build     string
	buildEnv  map[string]string
}

// VerifyGoBuildCacheWorkflows keeps the Go build cache main-owned. Only the Go
// Build Cache job on main and CI Benchmark's guarded produce job save it, each
// from the repository's build script after a lookup of the exact key; every
// consumer restores the same paths under the same key read-only; a pull
// request job restores a Go cache in its first evidence step only; pull request
// workflows only read the Actions cache; and no other job can write it.
func VerifyGoBuildCacheWorkflows(root string) error {
	workflows := filepath.Join(root, ".github", "workflows")
	action, err := parseYAMLFile(filepath.Join(root, ".github", "actions", "run-test-evidence", "action.yml"))
	if err != nil {
		return err
	}
	consumerSteps := sequenceNodes(mappingValue(mappingValue(action.Content[0], "runs"), "steps"))
	keyStep := stepWithID(consumerSteps, "go-cache-key")
	restoreIndex := slices.IndexFunc(consumerSteps, func(step *workflowYAMLNode) bool {
		return strings.HasPrefix(scalar(mappingValue(step, "uses")), "actions/cache/restore@")
	})
	if keyStep == nil || restoreIndex < 0 {
		return fmt.Errorf("evidence action does not restore the Go build cache")
	}
	restore := consumerSteps[restoreIndex]
	restoreInputs := mappingValue(restore, "with")
	paths := scalar(mappingValue(restoreInputs, "path"))
	if scalar(mappingValue(keyStep, "if")) != goBuildCacheInput || scalar(mappingValue(restore, "if")) != goBuildCacheRestoreGuard {
		return fmt.Errorf("evidence action restores the Go build cache without its go-cache input and free-disk guard")
	}
	if scalar(mappingValue(restoreInputs, "key")) != goBuildCacheKey || scalar(mappingValue(restoreInputs, "restore-keys")) != goBuildCacheRestoreKeys {
		return fmt.Errorf("evidence action does not fall back from the commit's key to its go.sum, compiler and Go version prefixes in order")
	}
	for index, step := range consumerSteps {
		uses := scalar(mappingValue(step, "uses"))
		if strings.HasPrefix(uses, "actions/setup-go@") && scalar(mappingValue(mappingValue(step, "with"), "cache")) != "${{ inputs.go-cache == 'false' }}" {
			return fmt.Errorf("evidence action keeps setup-go's own cache beside the Go build cache")
		}
		if scalar(mappingValue(step, "name")) == "Install buf" && index < restoreIndex {
			return fmt.Errorf("evidence action installs buf before restoring the Go build cache")
		}
	}

	writers := []goBuildCacheWriter{
		{file: "go-build-cache.yml", job: "build", build: "./scripts/build-go-cache.sh"},
		{
			file: "ci-benchmark.yml", job: "produce", ref: "${{ needs.resolve.outputs.base }}",
			build:    "git show \"${HEAD_SHA}:scripts/build-go-cache.sh\" > \"$RUNNER_TEMP/build-go-cache.sh\"\nbash \"$RUNNER_TEMP/build-go-cache.sh\"\n",
			buildEnv: map[string]string{"HEAD_SHA": "${{ needs.resolve.outputs.head }}"},
		},
	}
	for _, writer := range writers {
		document, err := parseYAMLFile(filepath.Join(workflows, writer.file))
		if err != nil {
			return err
		}
		job := mappingValue(mappingValue(document.Content[0], "jobs"), writer.job)
		if writer.file == "go-build-cache.yml" {
			if err := verifyGoBuildCacheProducer(document.Content[0], job); err != nil {
				return err
			}
		}
		if err := verifyGoBuildCacheWriterSteps(job, writer, keyStep, paths); err != nil {
			return fmt.Errorf("%s: %w", writer.file, err)
		}
	}

	consumers := []struct {
		file, job string
		readOnly  bool
	}{
		{file: "pull-request-verification.yml", job: "go-race"},
		{file: "pull-request-verification.yml", job: "deployment-protocol"},
		{file: "main-branch-verification.yml", job: "go-race", readOnly: true},
	}
	for _, consumer := range consumers {
		document, err := parseYAMLFile(filepath.Join(workflows, consumer.file))
		if err != nil {
			return err
		}
		job := mappingValue(mappingValue(document.Content[0], "jobs"), consumer.job)
		if !jobEvidenceInputEquals(job, "go-cache", "true") {
			return fmt.Errorf("%s job %q does not restore the Go build cache", consumer.file, consumer.job)
		}
		if consumer.readOnly && scalar(mappingValue(job, "cache-mode")) != "read" {
			return fmt.Errorf("%s job %q restores the Go build cache with write access", consumer.file, consumer.job)
		}
	}
	// A second restore in one job fails to extract over the read-only module
	// cache the first one left.
	pullRequest, err := parseYAMLFile(filepath.Join(workflows, "pull-request-verification.yml"))
	if err != nil {
		return err
	}
	jobs := mappingValue(pullRequest.Content[0], "jobs")
	for index := 0; jobs != nil && index+1 < len(jobs.Content); index += 2 {
		jobID, steps := jobs.Content[index].Value, evidenceSteps(jobs.Content[index+1])
		for position := 1; position < len(steps); position++ {
			if scalar(mappingValue(mappingValue(steps[position], "with"), "go-cache")) != "none" {
				return fmt.Errorf("pull-request-verification.yml job %q restores a Go cache again in evidence step %q; want go-cache: none", jobID, scalar(mappingValue(steps[position], "name")))
			}
		}
	}
	return verifyActionsCacheWriters(root, writers)
}

func verifyGoBuildCacheProducer(workflow, job *workflowYAMLNode) error {
	triggers := mappingValue(workflow, "on")
	push := mappingValue(triggers, "push")
	if triggers == nil || triggers.Kind != workflowYAMLMapping || len(triggers.Content) != 6 ||
		push == nil || len(push.Content) != 2 || !slices.Equal(sequenceScalars(mappingValue(push, "branches")), []string{"main"}) ||
		len(sequenceNodes(mappingValue(triggers, "schedule"))) != 1 || mappingValue(triggers, "workflow_dispatch") == nil {
		return fmt.Errorf("go build cache producer must run on pushes to main, a daily schedule and dispatch only")
	}
	if !readOnlyPermissions(mappingValue(workflow, "permissions"), []string{"contents"}) || mappingValue(job, "permissions") != nil {
		return fmt.Errorf("go build cache producer permissions are not exactly read-only contents")
	}
	concurrency := mappingValue(workflow, "concurrency")
	if scalar(mappingValue(concurrency, "group")) != "go-build-cache-${{ github.ref }}" || scalar(mappingValue(concurrency, "cancel-in-progress")) != "false" {
		return fmt.Errorf("go build cache producer runs must queue per ref without cancelling an upload")
	}
	if scalar(mappingValue(job, "if")) != "github.ref == 'refs/heads/main'" {
		return fmt.Errorf("go build cache producer job is not limited to main")
	}
	if scalar(mappingValue(job, "cache-mode")) != "write" {
		return fmt.Errorf("go build cache producer job does not hold cache-mode write")
	}
	return nil
}

// verifyGoBuildCacheWriterSteps requires exactly: checkout of the writer's
// commit without persisted credentials, setup-go without its own cache, the
// consumers' key step, a lookup of the exact key, then the build and the save
// only on a miss.
func verifyGoBuildCacheWriterSteps(job *workflowYAMLNode, writer goBuildCacheWriter, keyStep *workflowYAMLNode, paths string) error {
	steps := sequenceNodes(mappingValue(job, "steps"))
	if len(steps) != 6 {
		return fmt.Errorf("cache writer must run exactly checkout, setup-go, the key, the lookup, the build and the save")
	}
	checkout, setup, key, lookup, build, save := steps[0], steps[1], steps[2], steps[3], steps[4], steps[5]
	lookupInputs, saveInputs := mappingValue(lookup, "with"), mappingValue(save, "with")
	// Entries match on a hash of the path list as written, so any difference
	// from the consumers' list makes every restore miss.
	if scalar(mappingValue(lookupInputs, "path")) != paths || scalar(mappingValue(saveInputs, "path")) != paths {
		return fmt.Errorf("cache writer paths differ from the evidence action's restore paths")
	}
	checkoutInputs := mappingValue(checkout, "with")
	if !strings.HasPrefix(scalar(mappingValue(checkout, "uses")), "actions/checkout@") ||
		scalar(mappingValue(checkoutInputs, "ref")) != writer.ref || scalar(mappingValue(checkoutInputs, "persist-credentials")) != "false" {
		return fmt.Errorf("cache writer does not check out its own build commit without persisted credentials")
	}
	if !strings.HasPrefix(scalar(mappingValue(setup, "uses")), "actions/setup-go@") || scalar(mappingValue(mappingValue(setup, "with"), "cache")) != "false" {
		return fmt.Errorf("cache writer keeps setup-go's own cache")
	}
	if scalar(mappingValue(key, "id")) != "go-cache-key" || goBuildCacheKeyStepText(key) != goBuildCacheKeyStepText(keyStep) {
		return fmt.Errorf("cache writer key step differs from the evidence action's")
	}
	if scalar(mappingValue(lookup, "id")) != "lookup" || !strings.HasPrefix(scalar(mappingValue(lookup, "uses")), "actions/cache/restore@") ||
		scalar(mappingValue(lookupInputs, "lookup-only")) != "true" || scalar(mappingValue(lookupInputs, "key")) != goBuildCacheKey ||
		mappingValue(lookupInputs, "restore-keys") != nil {
		return fmt.Errorf("cache writer does not look up exactly its own key without downloading")
	}
	buildEnvironment := map[string]string{}
	environment := mappingValue(build, "env")
	for index := 0; environment != nil && index+1 < len(environment.Content); index += 2 {
		buildEnvironment[environment.Content[index].Value] = scalar(environment.Content[index+1])
	}
	if scalar(mappingValue(build, "run")) != writer.build || scalar(mappingValue(build, "if")) != goBuildCacheMiss || !maps.Equal(buildEnvironment, writer.buildEnv) {
		return fmt.Errorf("cache writer does not build exactly with the repository's build script on a miss")
	}
	if !strings.HasPrefix(scalar(mappingValue(save, "uses")), "actions/cache/save@") || scalar(mappingValue(save, "if")) != goBuildCacheMiss ||
		scalar(mappingValue(saveInputs, "key")) != goBuildCacheKey {
		return fmt.Errorf("cache writer does not save under its own key on a miss")
	}
	return nil
}

// Steps compute the same key when they run the same program over the same
// inputs; their conditions and names may differ.
func goBuildCacheKeyStepText(step *workflowYAMLNode) string {
	environment := mappingValue(step, "env")
	var variables []string
	for index := 0; environment != nil && index+1 < len(environment.Content); index += 2 {
		variables = append(variables, environment.Content[index].Value+"="+scalar(environment.Content[index+1]))
	}
	return scalar(mappingValue(step, "shell")) + "\x00" + strings.Join(variables, "\x00") + "\x00" + scalar(mappingValue(step, "run"))
}

// verifyActionsCacheWriters scans every workflow and composite action: only
// the listed writer jobs may save to the Actions cache or hold write-capable
// cache access, nothing uses actions/cache (whose post step saves), and every
// workflow a pull request triggers reads the cache only.
func verifyActionsCacheWriters(root string, writers []goBuildCacheWriter) error {
	writeCapable := func(mode *workflowYAMLNode) bool {
		return mode != nil && scalar(mode) != "read" && scalar(mode) != "none"
	}
	checkSteps := func(source string, steps []*workflowYAMLNode, writer bool) error {
		for _, step := range steps {
			uses := scalar(mappingValue(step, "uses"))
			if strings.HasPrefix(uses, "actions/cache@") {
				return fmt.Errorf("%s uses actions/cache, which saves in a post step; restore with actions/cache/restore", source)
			}
			if strings.HasPrefix(uses, "actions/cache/save@") && !writer {
				return fmt.Errorf("%s saves to the Actions cache", source)
			}
		}
		return nil
	}
	for _, directory := range []string{filepath.Join(root, ".github", "workflows"), filepath.Join(root, ".github", "actions")} {
		err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || (filepath.Ext(path) != ".yml" && filepath.Ext(path) != ".yaml") {
				return nil
			}
			document, err := parseYAMLFile(path)
			if err != nil {
				return err
			}
			file, top := entry.Name(), document.Content[0]
			if err := checkSteps(file, sequenceNodes(mappingValue(mappingValue(top, "runs"), "steps")), false); err != nil {
				return err
			}
			mode := mappingValue(top, "cache-mode")
			if writeCapable(mode) {
				return fmt.Errorf("%s grants every job write-capable cache access", file)
			}
			triggers, pullRequest := mappingValue(top, "on"), false
			for _, event := range []string{"pull_request", "pull_request_target"} {
				pullRequest = pullRequest || mappingValue(triggers, event) != nil || scalar(triggers) == event || slices.Contains(sequenceScalars(triggers), event)
			}
			if pullRequest && scalar(mode) != "read" {
				return fmt.Errorf("%s runs on pull requests without cache-mode read", file)
			}
			jobs := mappingValue(top, "jobs")
			for index := 0; jobs != nil && index+1 < len(jobs.Content); index += 2 {
				jobID, job := jobs.Content[index].Value, jobs.Content[index+1]
				writer := slices.ContainsFunc(writers, func(writer goBuildCacheWriter) bool {
					return writer.file == file && writer.job == jobID
				})
				jobMode := mappingValue(job, "cache-mode")
				if pullRequest && jobMode != nil {
					return fmt.Errorf("%s job %q overrides the pull request cache-mode", file, jobID)
				}
				if writeCapable(jobMode) && !writer {
					return fmt.Errorf("%s job %q has write-capable cache access", file, jobID)
				}
				if err := checkSteps(fmt.Sprintf("%s job %q", file, jobID), sequenceNodes(mappingValue(job, "steps")), writer); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func stepWithID(steps []*workflowYAMLNode, id string) *workflowYAMLNode {
	for _, step := range steps {
		if scalar(mappingValue(step, "id")) == id {
			return step
		}
	}
	return nil
}

func evidenceSteps(job *workflowYAMLNode) []*workflowYAMLNode {
	var steps []*workflowYAMLNode
	for _, step := range sequenceNodes(mappingValue(job, "steps")) {
		if scalar(mappingValue(step, "uses")) == evidenceAction {
			steps = append(steps, step)
		}
	}
	return steps
}

func stepInputs(step *workflowYAMLNode) map[string]string {
	inputs := map[string]string{}
	with := mappingValue(step, "with")
	for index := 0; with != nil && index+1 < len(with.Content); index += 2 {
		inputs[with.Content[index].Value] = scalar(with.Content[index+1])
	}
	return inputs
}

// evidenceStepInputs reads the job's first evidence step only.
func evidenceStepInputs(job *workflowYAMLNode) map[string]string {
	steps := evidenceSteps(job)
	if len(steps) == 0 {
		return nil
	}
	return stepInputs(steps[0])
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

func goShardMatrix(inventory PRProducerInventory) []string {
	matrix := make([]string, len(inventory.GoShards))
	for index := range matrix {
		matrix[index] = strconv.Itoa(index)
	}
	return matrix
}

func goRacePreparesIntegrationHost(job *workflowYAMLNode) bool {
	return jobEvidenceInputEquals(job, "needs-go-test-host", "true") &&
		jobEvidenceInputEquals(job, "needs-helm", "true")
}

func coveragePreparesRenderedTransport(job *workflowYAMLNode) bool {
	ready := false
	for _, step := range sequenceNodes(mappingValue(job, "steps")) {
		if strings.HasPrefix(scalar(mappingValue(step, "uses")), "azure/setup-helm@") &&
			scalar(mappingValue(mappingValue(step, "with"), "version")) == "v4.2.0" &&
			mappingValue(step, "if") == nil {
			ready = true
		}
		if strings.Contains(scalar(mappingValue(step, "run")), "go run ./internal/testinfra/cmd/tetral-coverage") {
			return ready
		}
	}
	return false
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
	inputs := evidenceStepInputs(job)
	return inputs != nil && inputs[name] == value
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
		"CI Benchmark":              {"ci-benchmark.yml", "workflow_dispatch", "ci-benchmark-"},
		"Go Build Cache":            {"go-build-cache.yml", "push", "go-build-cache-"},
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

type pullRequestEvidence struct {
	producers, goShards []string
	groups              map[string][]map[string]string
}

func readPullRequestEvidence(jobs *workflowYAMLNode, inventory PRProducerInventory) (pullRequestEvidence, error) {
	evidence := pullRequestEvidence{groups: map[string][]map[string]string{}}
	producerJobs := PRProducerJobs(inventory)
	for index := 0; index+1 < len(jobs.Content); index += 2 {
		jobID, job := jobs.Content[index].Value, jobs.Content[index+1]
		if jobID == "merge-gate" {
			continue
		}
		steps := evidenceSteps(job)
		if len(steps) == 0 {
			return pullRequestEvidence{}, fmt.Errorf("evidence job %q has no repository runner producer", jobID)
		}
		if len(steps) > 1 {
			if err := verifySharedJobLimits(jobID, job, steps); err != nil {
				return pullRequestEvidence{}, err
			}
		}
		for position, step := range steps {
			if position > 0 && scalar(mappingValue(step, "if")) != followOnEvidenceCondition {
				return pullRequestEvidence{}, fmt.Errorf("pull request job %q evidence step %q must run with if: %s", jobID, scalar(mappingValue(step, "name")), followOnEvidenceCondition)
			}
			inputs := stepInputs(step)
			if inputs["producer"] == "" {
				return pullRequestEvidence{}, fmt.Errorf("evidence job %q has no repository runner producer", jobID)
			}
			evidence.groups[inputs["group"]] = append(evidence.groups[inputs["group"]], inputs)
			producers := []string{inputs["producer"]}
			if jobID == "go-race" {
				shards := sequenceScalars(mappingValue(mappingValue(mappingValue(job, "strategy"), "matrix"), "shard"))
				if !sameStrings(shards, goShardMatrix(inventory)) || inputs["producer"] != "go-${{ matrix.shard }}" {
					return pullRequestEvidence{}, fmt.Errorf("go Race does not own the %d shards go_shards lists", len(inventory.GoShards))
				}
				if inputs["shard-count"] != strconv.Itoa(len(shards)) {
					return pullRequestEvidence{}, fmt.Errorf("go Race shard count does not match its matrix cardinality")
				}
				producers = nil
				for _, shard := range shards {
					producers = append(producers, "go-"+shard)
				}
				evidence.goShards = append(evidence.goShards, producers...)
			}
			for _, producer := range producers {
				if producerJobs[producer] != jobID {
					return pullRequestEvidence{}, fmt.Errorf("producer %q runs in job %q, but Merge Gate expects its result from job %q", producer, jobID, producerJobs[producer])
				}
			}
			evidence.producers = append(evidence.producers, producers...)
		}
	}
	return evidence, nil
}

// A hanging group's run stops at time-limit-minutes and still uploads its
// result; the step and job timeouts are only backstops above it.
func verifySharedJobLimits(jobID string, job *workflowYAMLNode, steps []*workflowYAMLNode) error {
	sum := 0
	for _, step := range steps {
		run, runErr := strconv.Atoi(scalar(mappingValue(mappingValue(step, "with"), "time-limit-minutes")))
		limit, limitErr := strconv.Atoi(scalar(mappingValue(step, "timeout-minutes")))
		if runErr != nil || limitErr != nil || run <= 0 || limit < run+evidenceStepBackstopMinutes {
			return fmt.Errorf("pull request job %q evidence step %q must set time-limit-minutes and a timeout-minutes at least %d minutes above it", jobID, scalar(mappingValue(step, "name")), evidenceStepBackstopMinutes)
		}
		sum += limit
	}
	if limit, err := strconv.Atoi(scalar(mappingValue(job, "timeout-minutes"))); err != nil || limit < sum+sharedJobOverheadMinutes {
		return fmt.Errorf("pull request job %q timeout-minutes must be at least its evidence steps' %d plus %d minutes", jobID, sum, sharedJobOverheadMinutes)
	}
	return nil
}

// groupInputEquals is false when no evidence step runs the group.
func (evidence pullRequestEvidence) groupInputEquals(group, name, value string) bool {
	for _, inputs := range evidence.groups[group] {
		if inputs[name] != value {
			return false
		}
	}
	return len(evidence.groups[group]) > 0
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
