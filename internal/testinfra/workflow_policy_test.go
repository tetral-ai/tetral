package testinfra

import (
	"os"
	"path/filepath"
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

func TestGoWorkflowsRejectMissingTransportRenderer(t *testing.T) {
	root := testRepositoryRoot(t)
	for _, tc := range []struct {
		file   string
		verify func(string) error
	}{
		{"pull-request-verification.yml", VerifyPullRequestWorkflow},
		{"main-branch-verification.yml", VerifyMainBranchWorkflow},
	} {
		t.Run(tc.file, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(root, ".github", "workflows", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			fixture := t.TempDir()
			if err := os.MkdirAll(filepath.Join(fixture, ".github", "workflows"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, fixture, ".github/workflows/"+tc.file, string(body))
			if err := tc.verify(fixture); err != nil {
				t.Fatalf("positive control: %v", err)
			}
			changed := strings.Replace(string(body), "          shard-count: '4'\n          needs-helm: 'true'", "          shard-count: '4'\n          needs-helm: 'false'", 1)
			if changed == string(body) {
				t.Fatal("missing Go shard renderer input")
			}
			writeTestFile(t, fixture, ".github/workflows/"+tc.file, changed)
			if err := tc.verify(fixture); err == nil {
				t.Fatal("Go workflow accepted missing transport renderer")
			}
			if tc.file == "main-branch-verification.yml" {
				changed = strings.Replace(string(body), "version: v4.2.0", "version: latest", 1)
				writeTestFile(t, fixture, ".github/workflows/"+tc.file, changed)
				if err := tc.verify(fixture); err == nil {
					t.Fatal("coverage accepted an unpinned transport renderer")
				}
			}
			bunInput := "          producer: deployment\n          artifact-prefix: pr-evidence\n          needs-bun: 'true'"
			if tc.file == "main-branch-verification.yml" {
				bunInput = "            producer: deployment\n            needs_bun: 'true'"
			}
			changed = strings.Replace(string(body), bunInput, strings.Replace(bunInput, "'true'", "'false'", 1), 1)
			if changed == string(body) {
				t.Fatal("missing deployment Bun input")
			}
			writeTestFile(t, fixture, ".github/workflows/"+tc.file, changed)
			if err := tc.verify(fixture); err == nil {
				t.Fatal("deployment workflow accepted missing configuration parser runtime")
			}
		})
	}
}

func TestCoveragePrerequisitesIncludeProductionTransportImages(t *testing.T) {
	plan, err := CoveragePlan(testRepositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, dependency := range []string{"postgresql", "minio", "nats", "sdk", "bun-workspaces", "docker", "envoy", "bun-image"} {
		if !contains(plan.Dependencies, dependency) {
			t.Errorf("coverage omitted %s", dependency)
		}
	}
}

func TestBenchmarkWorkflowPolicy(t *testing.T) {
	root := testRepositoryRoot(t)
	read := func(name string) string {
		body, err := os.ReadFile(filepath.Join(root, ".github", "workflows", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	const benchmark, pullRequest = "ci-benchmark.yml", "pull-request-verification.yml"
	sideA := "          ref: ${{ needs.resolve.outputs.base }}\n          fetch-depth: 0\n          persist-credentials: false\n      - name: Verify all Go packages under Race\n        uses: ./.github/actions/run-test-evidence\n"
	sideBInputs := "          artifact-prefix: bench-b\n          shard-index: ${{ matrix.shard }}\n          shard-count: '4'\n          needs-helm: 'true'\n          needs-bun: 'true'\n          needs-buf: 'true'\n"
	tests := []struct {
		name, file, old, new, want string
	}{
		{name: "repository workflows"},
		{name: "second trigger", file: benchmark, old: "on:\n  workflow_dispatch:\n", new: "on:\n  push:\n  workflow_dispatch:\n", want: "workflow_dispatch only"},
		{name: "write permission", file: benchmark, old: "  actions: read\n", new: "  actions: write\n", want: "exactly read-only"},
		{name: "extra permission", file: benchmark, old: "  contents: read\n", new: "  contents: read\n  pull-requests: read\n", want: "exactly read-only"},
		{name: "job permission override", file: benchmark, old: "    name: Compare A and B\n", new: "    name: Compare A and B\n    permissions:\n      contents: read\n", want: "overrides the workflow permissions"},
		{name: "secret reference", file: benchmark, old: "GH_TOKEN: ${{ github.token }}", new: "GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}", want: "references secrets"},
		{name: "workflow cache write", file: benchmark, old: "cache-mode: read\n", new: "cache-mode: write\n", want: "restrict the Actions cache"},
		{name: "job cache write", file: benchmark, old: "    name: Resolve Commits\n", new: "    name: Resolve Commits\n    cache-mode: write-only\n", want: "write-capable cache access"},
		{name: "shallow side checkout", file: benchmark, old: "ref: ${{ needs.resolve.outputs.head }}\n          fetch-depth: 0\n", new: "ref: ${{ needs.resolve.outputs.head }}\n          fetch-depth: 1\n", want: "complete history"},
		{name: "persisted credentials", file: benchmark, old: sideA, new: strings.Replace(sideA, "persist-credentials: false", "persist-credentials: true", 1), want: "complete history"},
		{name: "running-commit action", file: benchmark, old: sideA, new: strings.Replace(sideA, "uses: ./", "uses: $/", 1), want: "outside its checkout"},
		{name: "drifted Go input", file: benchmark, old: sideBInputs, new: strings.Replace(sideBInputs, "needs-buf: 'true'", "needs-buf: 'false'", 1), want: "inputs differ"},
		{name: "pull request input the benchmark lacks", file: pullRequest, old: "          go-cache: 'true'\n\n  agent-runtime:", new: "          go-cache: 'true'\n          dependency-audit: never\n\n  agent-runtime:", want: "inputs differ"},
		{name: "shared artifact prefix", file: benchmark, old: "artifact-prefix: bench-b", new: "artifact-prefix: bench-a", want: "own artifact prefix"},
		{name: "fewer shards", file: benchmark, old: "        shard: [0, 1, 2, 3]\n    env:\n      TETRAL_CI_TEST_MERGE_SHA: ${{ needs.resolve.outputs.head }}\n", new: "        shard: [0, 1, 2]\n    env:\n      TETRAL_CI_TEST_MERGE_SHA: ${{ needs.resolve.outputs.head }}\n", want: "shards differ"},
		{name: "cache producer on main", file: benchmark, old: "if: ${{ inputs.produce-cache && github.ref != 'refs/heads/main' }}", new: "if: ${{ inputs.produce-cache }}", want: "only when produce-cache is set off main"},
		{name: "cache produced by default", file: benchmark, old: "        type: boolean\n        required: false\n        default: false\n", new: "        type: boolean\n        required: false\n        default: true\n", want: "explicit produce-cache input"},
		{name: "side that a skipped produce skips", file: benchmark, old: "    name: B Go Race (shard ${{ matrix.shard }})\n    needs: [resolve, produce]\n    if: ${{ !cancelled() && ", new: "    name: B Go Race (shard ${{ matrix.shard }})\n    needs: [resolve, produce]\n    if: ${{ ", want: "does not wait for produce"},
		{name: "side that starts before the cache is produced", file: benchmark, old: "    name: B Go Race (shard ${{ matrix.shard }})\n    needs: [resolve, produce]\n", new: "    name: B Go Race (shard ${{ matrix.shard }})\n    needs: resolve\n", want: "does not wait for produce"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := map[string]string{benchmark: read(benchmark), pullRequest: read(pullRequest)}
			if tc.file != "" {
				changed := strings.Replace(files[tc.file], tc.old, tc.new, 1)
				if changed == files[tc.file] {
					t.Fatalf("%s does not contain the text this case changes", tc.file)
				}
				files[tc.file] = changed
			}
			fixture := t.TempDir()
			if err := os.MkdirAll(filepath.Join(fixture, ".github", "workflows"), 0o700); err != nil {
				t.Fatal(err)
			}
			for name, body := range files {
				writeTestFile(t, fixture, ".github/workflows/"+name, body)
			}
			err := VerifyBenchmarkWorkflow(fixture)
			if tc.file == "" && err != nil {
				t.Fatalf("repository benchmark workflow rejected: %v", err)
			}
			if tc.file != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("benchmark workflow policy error = %v; want %q", err, tc.want)
			}
		})
	}
}

func TestGoBuildCacheWorkflowPolicy(t *testing.T) {
	root := testRepositoryRoot(t)
	const (
		action    = "actions/run-test-evidence/action.yml"
		producer  = "workflows/go-build-cache.yml"
		benchmark = "workflows/ci-benchmark.yml"
		pull      = "workflows/pull-request-verification.yml"
		main      = "workflows/main-branch-verification.yml"
		vulncheck = "workflows/engine-vulncheck.yml"
	)
	type edit struct{ file, old, new string }
	installBuf := "    - name: Install buf\n      if: inputs.needs-buf == 'true'\n      shell: bash\n      run: go install github.com/bufbuild/buf/cmd/buf@v1.70.0\n\n"
	coverage := "    name: Report-only Coverage\n    runs-on: ubuntu-latest\n    timeout-minutes: 80\n    steps:\n"
	cacheStep := func(uses string) string {
		return coverage + "      - uses: " + uses + "@55cc8345863c7cc4c66a329aec7e433d2d1c52a9\n        with:\n          path: x\n          key: y\n"
	}
	tests := []struct {
		name  string
		edits []edit
		want  string
	}{
		{name: "repository workflows"},
		{name: "producer on pull requests", edits: []edit{{producer, "on:\n  push:\n", "on:\n  pull_request:\n  push:\n"}}, want: "pushes to main, a daily schedule and dispatch only"},
		{name: "producer write permission", edits: []edit{{producer, "permissions:\n  contents: read\n", "permissions:\n  contents: write\n"}}, want: "read-only contents"},
		{name: "producer cancels an upload", edits: []edit{{producer, "  cancel-in-progress: false\n", "  cancel-in-progress: true\n"}}, want: "without cancelling an upload"},
		{name: "producer off main", edits: []edit{{producer, "    if: github.ref == 'refs/heads/main'\n", ""}}, want: "limited to main"},
		{name: "producer without job write access", edits: []edit{{producer, "    cache-mode: write\n", ""}}, want: "does not hold cache-mode write"},
		{name: "producer restores setup-go's cache", edits: []edit{{producer, "          cache: false\n", "          cache: true\n"}}, want: "keeps setup-go's own cache"},
		{name: "producer builds outside the build script", edits: []edit{{producer, "run: ./scripts/build-go-cache.sh", "run: go test -race -count=1 -run '^$' -exec /bin/true ./..."}}, want: "repository's build script"},
		{name: "producer downloads before deciding", edits: []edit{{producer, "          lookup-only: true\n", ""}}, want: "without downloading"},
		{name: "producer persists credentials", edits: []edit{{producer, "          persist-credentials: false\n", "          persist-credentials: true\n"}}, want: "go-build-cache.yml: cache writer does not check out its own build commit"},
		{name: "benchmark produce builds the head", edits: []edit{{benchmark, "ref: ${{ needs.resolve.outputs.base }}\n          fetch-depth: 0\n          persist-credentials: false\n      - name: Set up Go without its own cache", "ref: ${{ needs.resolve.outputs.head }}\n          fetch-depth: 0\n          persist-credentials: false\n      - name: Set up Go without its own cache"}}, want: "ci-benchmark.yml: cache writer does not check out its own build commit"},
		{name: "producer runs another step", edits: []edit{{producer, "      - name: Save the entry\n", "      - name: Run the tests\n        run: go test ./...\n      - name: Save the entry\n"}}, want: "must run exactly"},
		{name: "producer saves after a hit", edits: []edit{{producer, "        if: steps.lookup.outputs.cache-hit != 'true'\n        uses: actions/cache/save@", "        uses: actions/cache/save@"}}, want: "does not save under its own key on a miss"},
		{name: "benchmark produce builds the base's script", edits: []edit{{benchmark, "HEAD_SHA: ${{ needs.resolve.outputs.head }}", "HEAD_SHA: ${{ needs.resolve.outputs.base }}"}}, want: "repository's build script"},
		{name: "benchmark produce key differs", edits: []edit{{benchmark, "cut -c 1-12", "cut -c 1-16"}}, want: "ci-benchmark.yml: cache writer key step differs"},
		{name: "consumer key differs", edits: []edit{{action, "cut -c 1-12", "cut -c 1-16"}}, want: "go-build-cache.yml: cache writer key step differs"},
		{name: "consumer paths differ", edits: []edit{{action, "          ~/.cache/go-build\n          ~/go/pkg/mod\n", "          ~/go/pkg/mod\n          ~/.cache/go-build\n"}}, want: "paths differ"},
		{name: "consumer restore keys out of order", edits: []edit{{action, "-key.outputs.modules-prefix }}\n          ${{ steps.go-cache-key.outputs.compiler-prefix }}", "-key.outputs.compiler-prefix }}\n          ${{ steps.go-cache-key.outputs.modules-prefix }}"}}, want: "in order"},
		{name: "consumer restores whatever the free disk", edits: []edit{{action, "      if: inputs.go-cache == 'true' && fromJSON(steps.go-cache-before.outputs.disk-free || '0') >= 25000000000\n", "      if: inputs.go-cache == 'true'\n"}}, want: "free-disk guard"},
		{name: "consumer keeps setup-go's cache", edits: []edit{{action, "        cache: ${{ inputs.go-cache != 'true' }}\n", ""}}, want: "beside the Go build cache"},
		{name: "consumer restores after buf is installed", edits: []edit{{action, installBuf, ""}, {action, "    - name: Compute the Go build cache key\n", installBuf + "    - name: Compute the Go build cache key\n"}}, want: "installs buf before"},
		{name: "pull request Go Race without the cache", edits: []edit{{pull, "          go-cache: 'true'\n\n  agent-runtime:", "\n  agent-runtime:"}}, want: "job \"go-race\" does not restore the Go build cache"},
		{name: "main Go Race restores with write access", edits: []edit{{main, "    timeout-minutes: 35\n    cache-mode: read\n", "    timeout-minutes: 35\n"}}, want: "with write access"},
		{name: "pull request workflow may write", edits: []edit{{pull, "cache-mode: read\n", ""}}, want: "pull-request-verification.yml runs on pull requests without cache-mode read"},
		{name: "pull request job override", edits: []edit{{pull, "    name: Agent Runtime\n", "    name: Agent Runtime\n    cache-mode: write\n"}}, want: "overrides the pull request cache-mode"},
		{name: "vulnerability scan may write", edits: []edit{{vulncheck, "cache-mode: read\n", ""}}, want: "engine-vulncheck.yml runs on pull requests without cache-mode read"},
		{name: "workflow-wide write access", edits: []edit{{main, "permissions:\n  contents: read\n", "permissions:\n  contents: read\n\ncache-mode: write\n"}}, want: "grants every job write-capable cache access"},
		{name: "another job with write access", edits: []edit{{main, coverage, strings.Replace(coverage, "    steps:\n", "    cache-mode: write-only\n    steps:\n", 1)}}, want: "job \"coverage\" has write-capable cache access"},
		{name: "another job saves", edits: []edit{{main, coverage, cacheStep("actions/cache/save")}}, want: "saves to the Actions cache"},
		{name: "a job uses the saving cache action", edits: []edit{{main, coverage, cacheStep("actions/cache")}}, want: "saves in a post step"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := t.TempDir()
			if err := os.CopyFS(filepath.Join(fixture, ".github"), os.DirFS(filepath.Join(root, ".github"))); err != nil {
				t.Fatal(err)
			}
			for _, change := range tc.edits {
				path := filepath.Join(fixture, ".github", change.file)
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				changed := strings.Replace(string(body), change.old, change.new, 1)
				if changed == string(body) {
					t.Fatalf("%s does not contain the text this case changes", change.file)
				}
				writeTestFile(t, fixture, ".github/"+change.file, changed)
			}
			err := VerifyGoBuildCacheWorkflows(fixture)
			if tc.edits == nil && err != nil {
				t.Fatalf("repository Go build cache workflows rejected: %v", err)
			}
			if tc.edits != nil && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("Go build cache policy error = %v; want %q", err, tc.want)
			}
		})
	}
}
