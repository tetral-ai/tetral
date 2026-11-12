package testinfra

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestVerificationWorkflowStructure(t *testing.T) {
	root := testRepositoryRoot(t)
	if err := VerifyWorkflowSkeletons(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyMainBranchWorkflow(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyScheduledWorkflow(root); err != nil {
		t.Fatal(err)
	}
}

func TestVerificationWorkflowPolicy(t *testing.T) {
	root := testRepositoryRoot(t)
	const pullRequest, main, scheduled = "pull-request-verification.yml", "main-branch-verification.yml", "scheduled-verification.yml"
	verifiers := map[string]func(string, PRProducerInventory) error{
		pullRequest: verifyPullRequestWorkflow,
		main:        verifyMainBranchWorkflow,
		scheduled:   func(root string, _ PRProducerInventory) error { return VerifyScheduledWorkflow(root) },
	}
	committed, err := LoadPRProducers()
	if err != nil {
		t.Fatal(err)
	}
	withGoShards := func(count int) PRProducerInventory {
		inventory := PRProducerInventory{GoShards: goShardProducers(count)}
		for _, producer := range committed.Producers {
			if !slices.Contains(committed.GoShards, producer) {
				inventory.Producers = append(inventory.Producers, producer)
			}
		}
		inventory.Producers = append(inventory.Producers, inventory.GoShards...)
		return inventory
	}
	type edit struct{ old, new string }
	shardHelm := "          shard-count: '5'\n          needs-helm: 'true'"
	sixShards := []edit{{"        shard: [0, 1, 2, 3, 4]\n", "        shard: [0, 1, 2, 3, 4, 5]\n"}, {"          shard-count: '5'\n", "          shard-count: '6'\n"}}
	pullRequestBun := "          producer: deployment\n          artifact-prefix: pr-evidence\n          needs-bun: 'true'"
	mainBun := "            producer: deployment\n            needs_bun: 'true'"
	disable := func(input string) string { return strings.Replace(input, "'true'", "'false'", 1) }
	sandboxStep := "      - name: Verify the local Sandbox image\n        if: ${{ !cancelled() }}\n        timeout-minutes: 23\n        uses: ./.github/actions/run-test-evidence\n        with:\n          group: sandbox-image\n          producer: sandbox-image\n          artifact-prefix: pr-evidence\n          time-limit-minutes: 20\n          go-cache: none\n"
	for _, tc := range []struct {
		name, file string
		edits      []edit
		want       string
		// goShards, when set, replaces the inventory's go_shards with that
		// many shards for the edited workflow; an empty want accepts it.
		goShards int
	}{
		{"pull request Go shard without Helm", pullRequest, []edit{{shardHelm, disable(shardHelm)}}, "go Race does not prepare its integration host", 0},
		{"main Go shard without Helm", main, []edit{{shardHelm, disable(shardHelm)}}, "main Go Race does not own the 5 complete integration-ready shards go_shards lists", 0},
		{"pull request Go Race follows six go_shards", pullRequest, sixShards, "", 6},
		{"main Go Race follows six go_shards", main, sixShards, "", 6},
		{"pull request Go shard count differs from its matrix size", pullRequest, []edit{{"          shard-count: '5'\n", "          shard-count: '4'\n"}}, "go Race shard count does not match its matrix cardinality", 0},
		{"main Go Race runs fewer shards than go_shards", main, []edit{{"        shard: [0, 1, 2, 3, 4]\n", "        shard: [0, 1, 2, 3]\n"}}, "main Go Race does not own the 5 complete", 0},
		{"main Go shard count is not go_shards'", main, []edit{{"          shard-count: '5'\n", "          shard-count: '4'\n"}}, "main Go Race does not own the 5 complete", 0},
		{"coverage with unpinned Helm", scheduled, []edit{{"version: v4.2.0", "version: latest"}}, "does not install pinned Helm", 0},
		{"pull request deployment without Bun", pullRequest, []edit{{pullRequestBun, disable(pullRequestBun)}}, "deployment verification does not install Bun", 0},
		{"main deployment without Bun", main, []edit{{mainBun, disable(mainBun)}}, `main verification group "deployment"`, 0},
		{"later security step without the changed-graph audit", pullRequest, []edit{{"          dependency-audit: changed\n", "          dependency-audit: never\n"}}, "does not audit changed dependency graphs", 0},
		{"later evidence step skipped after an earlier group fails", pullRequest, []edit{{"      - name: Verify dependencies and redaction boundaries\n        if: ${{ !cancelled() }}\n", "      - name: Verify dependencies and redaction boundaries\n"}}, `evidence step "Verify dependencies and redaction boundaries" must run with if: ${{ !cancelled() }}`, 0},
		{"shared job's evidence step without its run limit", pullRequest, []edit{{"          needs-bun: 'true'\n          time-limit-minutes: 20\n          go-cache: none\n\n  go-static-analysis:", "          needs-bun: 'true'\n          go-cache: none\n\n  go-static-analysis:"}}, `evidence step "Verify the Runtime service" must set time-limit-minutes and a timeout-minutes at least 3 minutes above it`, 0},
		{"shared job limit below its steps' limits", pullRequest, []edit{{"    timeout-minutes: 44\n", "    timeout-minutes: 41\n"}}, `job "deployment-protocol" timeout-minutes must be at least its evidence steps' 41 plus 3 minutes`, 0},
		{"group removed from its job", pullRequest, []edit{{sandboxStep, ""}}, "workflow producers do not match the PR producer inventory", 0},
		{"producers swapped between jobs", pullRequest, []edit{
			{"          group: security\n          producer: security\n", "          group: security\n          producer: gateway\n"},
			{"          group: gateway\n          producer: gateway\n", "          group: gateway\n          producer: security\n"},
		}, `producer "gateway" runs in job "repository-sandbox-security-runtime", but Merge Gate expects its result from job "provider-gateway"`, 0},
		{"Merge Gate does not wait for a job", pullRequest, []edit{{"      - deployment-protocol\n", ""}}, "merge Gate dependency set is incomplete", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(root, ".github", "workflows", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			fixture := t.TempDir()
			if err := os.MkdirAll(filepath.Join(fixture, ".github", "workflows"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, fixture, ".github/workflows/"+tc.file, string(body))
			if err := verifiers[tc.file](fixture, committed); err != nil {
				t.Fatalf("positive control: %v", err)
			}
			changed := string(body)
			for _, change := range tc.edits {
				next := strings.Replace(changed, change.old, change.new, 1)
				if next == changed {
					t.Fatalf("%s does not contain the text this case changes", tc.file)
				}
				changed = next
			}
			writeTestFile(t, fixture, ".github/workflows/"+tc.file, changed)
			inventory := committed
			if tc.goShards > 0 {
				inventory = withGoShards(tc.goShards)
			}
			err = verifiers[tc.file](fixture, inventory)
			if tc.want == "" && err != nil {
				t.Fatalf("%s rejected with %d go_shards: %v", tc.file, tc.goShards, err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("%s policy error = %v; want %q", tc.file, err, tc.want)
			}
		})
	}
}

func TestCoveragePrerequisitesIncludeProductionTransportImages(t *testing.T) {
	plan, err := CoveragePlan(testRepositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, dependency := range []string{"postgresql", "minio", "nats", "bun-workspaces", "docker", "envoy", "bun-image"} {
		if !contains(plan.Dependencies, dependency) {
			t.Errorf("coverage omitted %s", dependency)
		}
	}
}

// workflowEdit is a seeded defect: a change of one parsed file below .github.
type workflowEdit struct {
	file   string
	change func(t *testing.T, workflow *workflowYAMLNode)
}

func setIn(file, value string, path ...string) workflowEdit {
	return workflowEdit{file, func(t *testing.T, workflow *workflowYAMLNode) { yamlSet(t, workflow, value, path...) }}
}

func putIn(file, text string, path ...string) workflowEdit {
	return workflowEdit{file, func(t *testing.T, workflow *workflowYAMLNode) { yamlPut(t, workflow, text, path...) }}
}

func removeIn(file string, path ...string) workflowEdit {
	return workflowEdit{file, func(t *testing.T, workflow *workflowYAMLNode) { yamlRemove(t, workflow, path...) }}
}

func extendIn(file, text string, path ...string) workflowEdit {
	return workflowEdit{file, func(t *testing.T, workflow *workflowYAMLNode) { yamlExtend(t, workflow, text, path...) }}
}

func appendIn(file, text string, path ...string) workflowEdit {
	return workflowEdit{file, func(t *testing.T, workflow *workflowYAMLNode) { yamlAppend(t, workflow, text, path...) }}
}

// editedWorkflows copies the repository's .github tree and applies the edits.
func editedWorkflows(t *testing.T, edits []workflowEdit) string {
	t.Helper()
	fixture := t.TempDir()
	if err := os.CopyFS(filepath.Join(fixture, ".github"), os.DirFS(filepath.Join(testRepositoryRoot(t), ".github"))); err != nil {
		t.Fatal(err)
	}
	for _, edit := range edits {
		editWorkflowFile(t, fixture, ".github/"+edit.file, edit.change)
	}
	return fixture
}

func TestBenchmarkWorkflowPolicy(t *testing.T) {
	const benchmark, pullRequest = "workflows/ci-benchmark.yml", "workflows/pull-request-verification.yml"
	checkout, evidence := "uses^=actions/checkout@", "with.group=go"
	tests := []struct {
		name string
		edit workflowEdit
		want string
	}{
		{name: "second trigger", edit: setIn(benchmark, "", "on", "push"), want: "workflow_dispatch only"},
		{name: "write permission", edit: setIn(benchmark, "write", "permissions", "actions"), want: "exactly read-only"},
		{name: "extra permission", edit: setIn(benchmark, "read", "permissions", "pull-requests"), want: "exactly read-only"},
		{name: "job permission override", edit: putIn(benchmark, "contents: read", "jobs", "compare", "permissions"), want: "overrides the workflow permissions"},
		{name: "secret reference", edit: setIn(benchmark, "${{ secrets.GITHUB_TOKEN }}", "jobs", "compare", "env", "GH_TOKEN"), want: "references secrets"},
		{name: "workflow cache write", edit: setIn(benchmark, "write", "cache-mode"), want: "restrict the Actions cache"},
		{name: "job cache write", edit: setIn(benchmark, "write-only", "jobs", "resolve", "cache-mode"), want: "write-capable cache access"},
		{name: "shallow side checkout", edit: setIn(benchmark, "1", "jobs", "b-go-race", "steps", checkout, "with", "fetch-depth"), want: "complete history"},
		{name: "persisted credentials", edit: setIn(benchmark, "true", "jobs", "a-go-race", "steps", checkout, "with", "persist-credentials"), want: "complete history"},
		{name: "running-commit action", edit: setIn(benchmark, "$/.github/actions/run-test-evidence", "jobs", "a-go-race", "steps", evidence, "uses"), want: "outside its checkout"},
		{name: "drifted Go input", edit: setIn(benchmark, "false", "jobs", "b-go-race", "steps", evidence, "with", "needs-buf"), want: "inputs differ"},
		{name: "pull request input the benchmark lacks", edit: setIn(pullRequest, "never", "jobs", "go-race", "steps", evidence, "with", "dependency-audit"), want: "inputs differ"},
		{name: "shared artifact prefix", edit: setIn(benchmark, "bench-a", "jobs", "b-go-race", "steps", evidence, "with", "artifact-prefix"), want: "own artifact prefix"},
		{name: "a side's matrix is not its commit's", edit: putIn(benchmark, "[0, 1, 2, 3]", "jobs", "b-go-race", "strategy", "matrix", "shard"), want: `"b-go-race" does not run the Go shards its commit lists`},
		{name: "a side's count is not its commit's", edit: setIn(benchmark, "4", "jobs", "b-go-race", "steps", evidence, "with", "shard-count"), want: `"b-go-race" does not pass the Go shard count its commit lists`},
		{name: "a side's shard list taken from the other side", edit: setIn(benchmark, "${{ steps.shards.outputs.b-shards }}", "jobs", "resolve", "outputs", "a-shards"), want: `resolve job output "a-shards" does not come from its go_shards step`},
		{name: "a side's shards read from the other side's commit", edit: setIn(benchmark, "${{ steps.commits.outputs.head }}", "jobs", "resolve", "steps", "id=shards", "env", "A_SHA"), want: "does not read each side's go_shards from its own commit"},
		{name: "side B's shards read from the base commit", edit: setIn(benchmark, "${{ steps.commits.outputs.base }}", "jobs", "resolve", "steps", "id=shards", "env", "B_SHA"), want: "does not read each side's go_shards from its own commit"},
		{name: "another script reads the go_shards", edit: extendIn(benchmark, "cat internal/testinfra/pr_producers.json\n", "jobs", "resolve", "steps", "id=shards", "run"), want: "does not read each side's go_shards from its own commit"},
		{name: "cache producer on main", edit: setIn(benchmark, "${{ inputs.produce-cache }}", "jobs", "produce", "if"), want: "only when produce-cache is set off main"},
		{name: "cache produced by default", edit: setIn(benchmark, "true", "on", "workflow_dispatch", "inputs", "produce-cache", "default"), want: "explicit produce-cache input"},
		{name: "side that a skipped produce skips", edit: removeIn(benchmark, "jobs", "b-go-race", "if"), want: "does not wait for produce"},
		{name: "side that starts before the cache is produced", edit: putIn(benchmark, "resolve", "jobs", "b-go-race", "needs"), want: "does not wait for produce"},
	}
	if err := VerifyBenchmarkWorkflow(testRepositoryRoot(t)); err != nil {
		t.Fatalf("repository benchmark workflow rejected: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyBenchmarkWorkflow(editedWorkflows(t, []workflowEdit{tc.edit}))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("benchmark workflow policy error = %v; want %q", err, tc.want)
			}
		})
	}
}

func TestGoBuildCacheWorkflowPolicy(t *testing.T) {
	const (
		action    = "actions/run-test-evidence/action.yml"
		producer  = "workflows/go-build-cache.yml"
		benchmark = "workflows/ci-benchmark.yml"
		pull      = "workflows/pull-request-verification.yml"
		main      = "workflows/main-branch-verification.yml"
		scheduled = "workflows/scheduled-verification.yml"
		vulncheck = "workflows/engine-vulncheck.yml"
	)
	const checkout, setupGo, save = "uses^=actions/checkout@", "uses^=actions/setup-go@", "uses^=actions/cache/save@"
	cacheStep := func(uses string) workflowEdit {
		return appendIn(scheduled, "{uses: "+uses+"@55cc8345863c7cc4c66a329aec7e433d2d1c52a9, with: {path: x, key: y}}", "jobs", "coverage", "steps")
	}
	// The buf install moves in front of the step that computes the cache key.
	bufFirst := workflowEdit{action, func(t *testing.T, workflow *workflowYAMLNode) {
		steps := yamlAt(t, workflow, "runs", "steps")
		buf := yamlAt(t, steps, "run^=go install github.com/bufbuild/buf/")
		yamlRemove(t, workflow, "runs", "steps", "run^=go install github.com/bufbuild/buf/")
		steps.Content = append([]*workflowYAMLNode{buf}, steps.Content...)
	}}
	inventory, err := LoadPRProducers()
	if err != nil {
		t.Fatal(err)
	}
	sharedJob := PRProducerJobs(inventory)["runtime"]
	tests := []struct {
		name string
		edit workflowEdit
		want string
	}{
		{name: "producer on pull requests", edit: setIn(producer, "", "on", "pull_request"), want: "pushes to main, a daily schedule and dispatch only"},
		{name: "producer write permission", edit: setIn(producer, "write", "permissions", "contents"), want: "read-only contents"},
		{name: "producer cancels an upload", edit: setIn(producer, "true", "concurrency", "cancel-in-progress"), want: "without cancelling an upload"},
		{name: "producer off main", edit: removeIn(producer, "jobs", "build", "if"), want: "limited to main"},
		{name: "producer without job write access", edit: removeIn(producer, "jobs", "build", "cache-mode"), want: "does not hold cache-mode write"},
		{name: "producer restores setup-go's cache", edit: setIn(producer, "true", "jobs", "build", "steps", setupGo, "with", "cache"), want: "keeps setup-go's own cache"},
		{name: "producer builds outside the build script", edit: setIn(producer, "go test -race -count=1 -run '^$' -exec /bin/true ./...", "jobs", "build", "steps", "run=./scripts/build-go-cache.sh", "run"), want: "repository's build script"},
		{name: "producer downloads before deciding", edit: removeIn(producer, "jobs", "build", "steps", "id=lookup", "with", "lookup-only"), want: "without downloading"},
		{name: "producer persists credentials", edit: setIn(producer, "true", "jobs", "build", "steps", checkout, "with", "persist-credentials"), want: "go-build-cache.yml: cache writer does not check out its own build commit"},
		{name: "benchmark produce builds the head", edit: setIn(benchmark, "${{ needs.resolve.outputs.head }}", "jobs", "produce", "steps", checkout, "with", "ref"), want: "ci-benchmark.yml: cache writer does not check out its own build commit"},
		{name: "producer runs another step", edit: appendIn(producer, "{name: Run the tests, run: go test ./...}", "jobs", "build", "steps"), want: "must run exactly"},
		{name: "producer saves after a hit", edit: removeIn(producer, "jobs", "build", "steps", save, "if"), want: "does not save under its own key on a miss"},
		{name: "benchmark produce builds the base's script", edit: setIn(benchmark, "${{ needs.resolve.outputs.base }}", "jobs", "produce", "steps", "env.HEAD_SHA^=${{", "env", "HEAD_SHA"), want: "repository's build script"},
		{name: "benchmark produce key differs", edit: extendIn(benchmark, "echo changed\n", "jobs", "produce", "steps", "id=go-cache-key", "run"), want: "ci-benchmark.yml: cache writer key step differs"},
		{name: "consumer key differs", edit: extendIn(action, "echo changed\n", "runs", "steps", "id=go-cache-key", "run"), want: "go-build-cache.yml: cache writer key step differs"},
		{name: "consumer paths differ", edit: setIn(action, "~/go/pkg/mod\n~/.cache/go-build\n~/go/bin\n", "runs", "steps", "id=go-cache", "with", "path"), want: "paths differ"},
		{name: "consumer restore keys out of order", edit: setIn(action, "${{ steps.go-cache-key.outputs.compiler-prefix }}\n${{ steps.go-cache-key.outputs.modules-prefix }}\n${{ steps.go-cache-key.outputs.toolchain-prefix }}\n", "runs", "steps", "id=go-cache", "with", "restore-keys"), want: "in order"},
		{name: "consumer restores whatever the free disk", edit: setIn(action, "inputs.go-cache == 'true'", "runs", "steps", "id=go-cache", "if"), want: "free-disk guard"},
		{name: "consumer keeps setup-go's cache", edit: removeIn(action, "runs", "steps", setupGo, "with", "cache"), want: "beside the Go build cache"},
		{name: "consumer restores after buf is installed", edit: bufFirst, want: "installs buf before"},
		{name: "pull request Go Race without the cache", edit: removeIn(pull, "jobs", "go-race", "steps", "with.group=go", "with", "go-cache"), want: `job "go-race" does not restore the Go build cache`},
		{name: "second Go build cache restore in one job", edit: setIn(pull, "true", "jobs", "deployment-protocol", "steps", "with.group=protocol", "with", "go-cache"), want: `job "deployment-protocol" restores a Go cache again in evidence step`},
		{name: "later evidence step restores setup-go's cache", edit: removeIn(pull, "jobs", sharedJob, "steps", "with.group=runtime", "with", "go-cache"), want: `job "` + sharedJob + `" restores a Go cache again in evidence step`},
		{name: "main Go Race restores with write access", edit: removeIn(main, "jobs", "go-race", "cache-mode"), want: "with write access"},
		{name: "pull request workflow may write", edit: removeIn(pull, "cache-mode"), want: "pull-request-verification.yml runs on pull requests without cache-mode read"},
		{name: "pull request job override", edit: setIn(pull, "write", "jobs", "provider-gateway", "cache-mode"), want: "overrides the pull request cache-mode"},
		{name: "vulnerability scan may write", edit: removeIn(vulncheck, "cache-mode"), want: "engine-vulncheck.yml runs on pull requests without cache-mode read"},
		{name: "workflow-wide write access", edit: setIn(main, "write", "cache-mode"), want: "grants every job write-capable cache access"},
		{name: "another job with write access", edit: setIn(scheduled, "write-only", "jobs", "coverage", "cache-mode"), want: `job "coverage" has write-capable cache access`},
		{name: "another job saves", edit: cacheStep("actions/cache/save"), want: "saves to the Actions cache"},
		{name: "a job uses the saving cache action", edit: cacheStep("actions/cache"), want: "saves in a post step"},
	}
	if err := VerifyGoBuildCacheWorkflows(testRepositoryRoot(t)); err != nil {
		t.Fatalf("repository Go build cache workflows rejected: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyGoBuildCacheWorkflows(editedWorkflows(t, []workflowEdit{tc.edit}))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Go build cache policy error = %v; want %q", err, tc.want)
			}
		})
	}
}
