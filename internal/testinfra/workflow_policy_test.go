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
		{name: "pull request input the benchmark lacks", file: pullRequest, old: "          needs-go-test-host: 'true'\n\n  agent-runtime:", new: "          needs-go-test-host: 'true'\n          dependency-audit: never\n\n  agent-runtime:", want: "inputs differ"},
		{name: "shared artifact prefix", file: benchmark, old: "artifact-prefix: bench-b", new: "artifact-prefix: bench-a", want: "own artifact prefix"},
		{name: "fewer shards", file: benchmark, old: "    name: B Go Race (shard ${{ matrix.shard }})\n    needs: resolve\n    runs-on: ubuntu-latest\n    timeout-minutes: 35\n    strategy:\n      fail-fast: false\n      matrix:\n        shard: [0, 1, 2, 3]\n", new: "    name: B Go Race (shard ${{ matrix.shard }})\n    needs: resolve\n    runs-on: ubuntu-latest\n    timeout-minutes: 35\n    strategy:\n      fail-fast: false\n      matrix:\n        shard: [0, 1, 2]\n", want: "shards differ"},
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
