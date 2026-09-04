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

func TestScheduledWorkflowRejectsMissingBunSetup(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := t.TempDir()
	source := filepath.Join(root, ".github", "workflows", "scheduled-verification.yml")
	body, err := os.ReadFile(source) //nolint:gosec // source is a repository-owned workflow fixture.
	if err != nil {
		t.Fatal(err)
	}
	const setup = `      - name: Set up Bun
        uses: oven-sh/setup-bun@0c5077e51419868618aeaa5fe8019c62421857d6
        with:
          bun-version-file: services/gateway/package.json
`
	mutated := strings.Replace(string(body), setup, "", 1)
	if mutated == string(body) {
		t.Fatal("scheduled workflow fixture did not contain the pinned Bun setup")
	}
	destination := filepath.Join(fixture, ".github", "workflows")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(destination, "scheduled-verification.yml")
	if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil { //nolint:gosec // fixture path is rooted in t.TempDir.
		t.Fatal(err)
	}
	if err := VerifyScheduledWorkflow(fixture); err == nil {
		t.Fatal("scheduled concurrency history without Bun setup passed")
	}
}

func TestWorkflowsRejectWrongDependencyAuditCadence(t *testing.T) {
	tests := []struct {
		name     string
		workflow string
		old      string
		new      string
		verify   func(string) error
	}{
		{
			name:     "pull request skips changed audit",
			workflow: "pull-request-verification.yml",
			old:      "          dependency-audit: changed\n",
			new:      "          dependency-audit: never\n",
			verify:   VerifyPullRequestWorkflow,
		},
		{
			name:     "main repeats online audit",
			workflow: "main-branch-verification.yml",
			old:      "          dependency-audit: never\n",
			new:      "          dependency-audit: always\n",
			verify:   VerifyMainBranchWorkflow,
		},
		{
			name:     "scheduled audit becomes changed-only",
			workflow: "scheduled-verification.yml",
			old:      "          dependency-audit: always\n",
			new:      "          dependency-audit: changed\n",
			verify:   VerifyScheduledWorkflow,
		},
	}
	root := testRepositoryRoot(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := copyWorkflowFixture(t, root, test.workflow)
			path := filepath.Join(fixture, ".github", "workflows", test.workflow)
			body, err := os.ReadFile(path) //nolint:gosec // fixture path is rooted in t.TempDir.
			if err != nil {
				t.Fatal(err)
			}
			mutated := strings.Replace(string(body), test.old, test.new, 1)
			if mutated == string(body) {
				t.Fatal("workflow fixture did not contain the expected audit policy")
			}
			if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil { //nolint:gosec // fixture path is rooted in t.TempDir.
				t.Fatal(err)
			}
			if err := test.verify(fixture); err == nil {
				t.Fatal("wrong dependency audit cadence passed")
			}
		})
	}
}

func TestScheduledWorkflowRejectsMissingDailyAudit(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := copyWorkflowFixture(t, root, "scheduled-verification.yml")
	path := filepath.Join(fixture, ".github", "workflows", "scheduled-verification.yml")
	body, err := os.ReadFile(path) //nolint:gosec // fixture path is rooted in t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(body), "    - cron: '41 6 * * *'\n", "", 1)
	if mutated == string(body) {
		t.Fatal("scheduled workflow fixture did not contain the daily audit")
	}
	if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil { //nolint:gosec // fixture path is rooted in t.TempDir.
		t.Fatal(err)
	}
	if err := VerifyScheduledWorkflow(fixture); err == nil {
		t.Fatal("scheduled workflow without daily dependency audit passed")
	}
}

func TestPullRequestWorkflowRejectsMismatchedRaceShardCount(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := copyPullRequestWorkflowFixture(t, root)
	path := filepath.Join(fixture, ".github", "workflows", "pull-request-verification.yml")
	body, err := os.ReadFile(path) //nolint:gosec // fixture path is rooted in t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "          shard-count: '4'\n", "          shard-count: '5'\n", 1))
	if err := os.WriteFile(path, body, 0o600); err != nil { //nolint:gosec // fixture path is rooted in t.TempDir.
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(fixture); err == nil {
		t.Fatal("mismatched Race shard count passed")
	}
}

func TestPullRequestWorkflowRejectsCurrentAttemptOnlyEvidence(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := copyPullRequestWorkflowFixture(t, root)
	path := filepath.Join(fixture, ".github", "workflows", "pull-request-verification.yml")
	body, err := os.ReadFile(path) //nolint:gosec // path is rooted in t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(
		string(body),
		"pr-evidence-*-${{ github.run_id }}-*",
		"pr-evidence-*-${{ github.run_id }}-${{ github.run_attempt }}",
		1,
	))
	if err := os.WriteFile(path, body, 0o600); err != nil { //nolint:gosec // path is rooted in t.TempDir.
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(fixture); err == nil {
		t.Fatal("current-attempt-only Merge Gate evidence passed")
	}
}

func TestMainWorkflowRejectsUnpreparedGoIntegrationHost(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := t.TempDir()
	source := filepath.Join(root, ".github", "workflows", "main-branch-verification.yml")
	body, err := os.ReadFile(source) //nolint:gosec // source is a repository-owned workflow fixture.
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "          needs-go-test-host: 'true'\n", "", 1))
	destination := filepath.Join(fixture, ".github", "workflows")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(destination, "main-branch-verification.yml")
	if err := os.WriteFile(path, body, 0o600); err != nil { //nolint:gosec // fixture path is rooted in t.TempDir.
		t.Fatal(err)
	}
	if err := VerifyMainBranchWorkflow(fixture); err == nil {
		t.Fatal("unprepared main Go integration host passed")
	}
}

func TestMainCoverageRejectsUnpreparedGoIntegrationHost(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := t.TempDir()
	source := filepath.Join(root, ".github", "workflows", "main-branch-verification.yml")
	body, err := os.ReadFile(source) //nolint:gosec // source is a repository-owned workflow fixture.
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "          unshare -Ur -m true\n", "", 1))
	destination := filepath.Join(fixture, ".github", "workflows")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(destination, "main-branch-verification.yml")
	if err := os.WriteFile(path, body, 0o600); err != nil { //nolint:gosec // fixture path is rooted in t.TempDir.
		t.Fatal(err)
	}
	if err := VerifyMainBranchWorkflow(fixture); err == nil {
		t.Fatal("unprepared main coverage Go integration host passed")
	}
}

func TestPullRequestWorkflowRejectsMissingRaceShard(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := t.TempDir()
	source := filepath.Join(root, ".github", "workflows", "pull-request-verification.yml")
	// The source is a repository-owned workflow fixture.
	//nolint:gosec
	body, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "shard: [0, 1, 2, 3]", "shard: [0, 1, 2]", 1))
	destination := filepath.Join(fixture, ".github", "workflows")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(destination, "pull-request-verification.yml")
	// fixturePath is rooted in t.TempDir and uses a fixed filename.
	//nolint:gosec
	if err := os.WriteFile(fixturePath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(fixture); err == nil {
		t.Fatal("missing required Race shard passed")
	}
}

func TestPullRequestWorkflowRejectsShallowCheckout(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := copyPullRequestWorkflowFixture(t, root)
	path := filepath.Join(fixture, ".github", "workflows", "pull-request-verification.yml")
	body, err := os.ReadFile(path) //nolint:gosec // fixture path is rooted in t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "          fetch-depth: 0\n", "", 1))
	// path is rooted in t.TempDir and uses a fixed repository-owned filename.
	//nolint:gosec
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(fixture); err == nil {
		t.Fatal("shallow checkout passed")
	}
}

func TestPullRequestWorkflowRejectsUnpreparedGoIntegrationHost(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := copyPullRequestWorkflowFixture(t, root)
	path := filepath.Join(fixture, ".github", "workflows", "pull-request-verification.yml")
	body, err := os.ReadFile(path) //nolint:gosec // fixture path is rooted in t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "          needs-go-test-host: 'true'\n", "", 1))
	// path is rooted in t.TempDir and uses a fixed repository-owned filename.
	//nolint:gosec
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(fixture); err == nil {
		t.Fatal("unprepared Go integration host passed")
	}
}

func TestPullRequestWorkflowRejectsMergeRevisionAsRequiredCheckCarrier(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := copyPullRequestWorkflowFixture(t, root)
	path := filepath.Join(fixture, ".github", "workflows", "pull-request-verification.yml")
	body, err := os.ReadFile(path) //nolint:gosec // fixture path is rooted in t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(
		string(body),
		"TETRAL_CI_REQUIRED_CHECK_SHA: ${{ github.event.pull_request.head.sha }}",
		"TETRAL_CI_REQUIRED_CHECK_SHA: ${{ github.sha }}",
		1,
	))
	// path is rooted in t.TempDir and uses a fixed repository-owned filename.
	//nolint:gosec
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(fixture); err == nil {
		t.Fatal("merge revision passed as the required-check carrier")
	}
}

func copyPullRequestWorkflowFixture(t *testing.T, root string) string {
	t.Helper()
	return copyWorkflowFixture(t, root, "pull-request-verification.yml")
}

func copyWorkflowFixture(t *testing.T, root, name string) string {
	t.Helper()
	fixture := t.TempDir()
	source := filepath.Join(root, ".github", "workflows", name)
	body, err := os.ReadFile(source) //nolint:gosec // source is a repository-owned workflow fixture.
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(fixture, ".github", "workflows")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	// destination is rooted in t.TempDir and the filename is fixed.
	//nolint:gosec
	if err := os.WriteFile(filepath.Join(destination, name), body, 0o600); err != nil {
		t.Fatal(err)
	}
	return fixture
}
