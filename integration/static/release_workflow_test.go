package static

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

type releaseWorkflow struct {
	Name        string                        `yaml:"name"`
	On          map[string]any                `yaml:"on"`
	Permissions map[string]string             `yaml:"permissions"`
	Concurrency releaseWorkflowConcurrency    `yaml:"concurrency"`
	Jobs        map[string]releaseWorkflowJob `yaml:"jobs"`
}

type releaseWorkflowConcurrency struct {
	Group            string `yaml:"group"`
	CancelInProgress bool   `yaml:"cancel-in-progress"`
}

type releaseWorkflowJob struct {
	If          string            `yaml:"if"`
	Environment string            `yaml:"environment"`
	Permissions map[string]string `yaml:"permissions"`
	Steps       []map[string]any  `yaml:"steps"`
}

func TestReleaseWorkflowSeparatesReadAndProtectedWriteAuthority(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	path := filepath.Join(root, ".github", "workflows", "engine-release.yml")
	body, err := os.ReadFile(path) //nolint:gosec // Repository-owned workflow under test.
	if err != nil {
		t.Fatal(err)
	}
	var workflow releaseWorkflow
	if err := testinfra.DecodeWorkflowYAML(body, path, &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Name != "Release" || len(workflow.On) != 1 || workflow.On["workflow_dispatch"] == nil {
		t.Fatalf("release triggers = %#v; want workflow_dispatch only", workflow.On)
	}
	if workflow.Permissions["contents"] != "read" || workflow.Permissions["packages"] != "read" || workflow.Concurrency.CancelInProgress {
		t.Fatalf("release default authority = permissions:%v concurrency:%+v", workflow.Permissions, workflow.Concurrency)
	}
	if workflow.Concurrency.Group != "tetral-release-version-owner" {
		t.Fatalf("release concurrency owner = %q; want one global version owner", workflow.Concurrency.Group)
	}
	writeJobs := []string{"reserve", "candidate-images", "finalize-candidate", "record-rehearsal", "promote", "retire-candidate", "cleanup-candidates"}
	for _, name := range writeJobs {
		job, ok := workflow.Jobs[name]
		if !ok || job.Environment != "release" {
			t.Fatalf("write job %q is absent or outside the release environment", name)
		}
		if !jobUsesLocalAction(job, "./.github/actions/verify-release-authority") {
			t.Fatalf("write job %q does not re-read GitHub authority before mutation", name)
		}
	}
	if workflow.Jobs["state"].Environment != "" {
		t.Fatal("read-only state reconstruction must not cross the release environment")
	}
	if workflow.Jobs["promote"].Permissions["contents"] != "write" || workflow.Jobs["promote"].Permissions["packages"] != "write" {
		t.Fatal("promotion lacks its explicit job-scoped Git and package authority")
	}
	if workflow.Jobs["candidate-images"].Permissions["contents"] != "read" || workflow.Jobs["candidate-images"].Permissions["packages"] != "write" {
		t.Fatal("candidate image builder must receive package write without contents write")
	}
	requireWorkflowActionsUseFullSHAs(t, "engine-release.yml", string(body))
}

func TestReleaseWorkflowBuildsCandidateOnceAndPromotesRecordedDigests(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	body, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "engine-release.yml")) //nolint:gosec // Repository-owned workflow under test.
	if err != nil {
		t.Fatal(err)
	}
	var workflow releaseWorkflow
	if err := testinfra.DecodeWorkflowYAML(body, "engine-release.yml", &workflow); err != nil {
		t.Fatal(err)
	}
	candidate := marshalWorkflowJob(t, workflow.Jobs["candidate-images"])
	finalize := marshalWorkflowJob(t, workflow.Jobs["finalize-candidate"])
	promote := marshalWorkflowJob(t, workflow.Jobs["promote"])
	for _, token := range []string{"docker/build-push-action@", `"platforms":"linux/amd64"`} {
		if !strings.Contains(candidate, token) {
			t.Fatalf("candidate build is missing %q", token)
		}
	}
	for _, token := range []string{"helm package", "release-oci-record.sh publish helm-candidate", "release-oci-record.sh publish candidate", "render_command"} {
		if !strings.Contains(finalize, token) {
			t.Fatalf("candidate finalizer is missing %q", token)
		}
	}
	for _, forbidden := range []string{"docker/build-push-action@", "run: docker build ", "helm package"} {
		if strings.Contains(promote, forbidden) {
			t.Fatalf("promotion rebuilds or repackages through %q", forbidden)
		}
	}
	for _, required := range []string{"docker buildx imagetools create", "candidate_manifest_digest", "helm push", "publish authorization", "fetch authorization", "validate-authorization", "promotion-plan", "reconstruct_release pre-images", "reconstruct_release pre-chart", "reconstruct_release pre-tag", "reconstruct_release pre-github-release", "git/tags", "gh release create", "gh release upload"} {
		if !strings.Contains(promote, required) {
			t.Fatalf("promotion is missing digest-preserving step %q", required)
		}
	}
	text := string(body)
	for _, required := range []string{"github.workflow_sha", "refs/heads/main", "rehearsal_values_digest", "rehearsal_render_digest", "release-oci-record.sh fetch", "release-github-deployment.sh", "release-state.sh", "candidate-$ARTIFACT_VERSION", "rehearsal-$ARTIFACT_VERSION", "authorization-$VERSION", "oras repo tags \"$RELEASE_METADATA_REPOSITORY\" --format json", "require_exclusive_candidate_tag"} {
		if !strings.Contains(text, required) {
			t.Fatalf("release workflow is missing immutable boundary %q", required)
		}
	}
	if strings.Count(text, `^v0\.1\.0-alpha\.[0-9]+$`) != 2 || strings.Count(text, `^reservation-0\.1\.0-alpha\.[0-9]+$`) != 2 {
		t.Fatal("candidate and promotion must exclude the historical alpha.rc tag from numeric monotonicity")
	}
	for _, forbidden := range []string{"oras blob fetch \"$RELEASE_METADATA_REPOSITORY\"", "--output json", "git merge-base --is-ancestor", "test -z \"$(git tag"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("release workflow retains unverified or non-resumable operation %q", forbidden)
		}
	}
	if !strings.Contains(text, `if [[ "$MODE" = candidate ]]`) || !strings.Contains(text, `test "$SOURCE_COMMIT" = "$WORKFLOW_SHA"`) {
		t.Fatal("candidate creation does not require the exact current main source")
	}
	if strings.Count(text, `test "$SOURCE_COMMIT" = "$(git rev-parse origin/main)"`) != 0 {
		t.Fatal("post-candidate release modes still require the source to remain the main tip")
	}
	for _, forbidden := range []string{"push:\n    tags:", ":latest", "0.1.0-alpha\n", "DAYTONA_API_KEY", "daytona_release_smoke", "external-smoke", "secrets."} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("release workflow contains retired or mutable surface %q", forbidden)
		}
	}
}

func TestBaseImageMaintenanceIsManualImmutableAndSeparate(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	body, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "mirror-base-images.yml")) //nolint:gosec // Repository-owned workflow under test.
	if err != nil {
		t.Fatal(err)
	}
	var workflow releaseWorkflow
	if err := testinfra.DecodeWorkflowYAML(body, "mirror-base-images.yml", &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Name != "Base Image Maintenance" || len(workflow.On) != 1 || workflow.On["workflow_dispatch"] == nil {
		t.Fatalf("base-image triggers = %#v", workflow.On)
	}
	text := string(body)
	for _, required := range []string{"repository@sha256:digest", "linux/amd64", "source_identity", "selected_digest", "TARGET_REPOSITORY: ${{ inputs.target_repository }}", "--prefer-index=false", "target_digest", "internal/release/base_images.json"} {
		if !strings.Contains(text, required) {
			t.Fatalf("base-image maintenance is missing %q", required)
		}
	}
	for _, forbidden := range []string{"fallback", "DAYTONA", "PROVIDER", "contents: write", "engine-release"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("base-image maintenance contains forbidden authority %q", forbidden)
		}
	}
	requireWorkflowActionsUseFullSHAs(t, "mirror-base-images.yml", text)
}

func TestReleaseAuthorityUsesPaginatedFactsWithoutFabricatedTokenCapabilities(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	for _, relative := range []string{"scripts/verify-release-github-authority.sh", "scripts/release-github-deployment.sh", "scripts/release-oci-record.sh", "scripts/release-state.sh"} {
		body, err := os.ReadFile(filepath.Join(root, relative)) //nolint:gosec // Repository-owned release adapter.
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		if strings.Contains(text, "repository_token_can_read") || strings.Contains(text, "repository_token_can_write") {
			t.Fatalf("%s fabricates package-token capability", relative)
		}
	}
	authority, err := os.ReadFile(filepath.Join(root, "scripts", "verify-release-github-authority.sh")) //nolint:gosec // Repository-owned release adapter.
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"--paginate --slurp", "DECLARED_PACKAGE_PERMISSION", "readback_complete:true", "declared_job_permission"} {
		if !strings.Contains(string(authority), required) {
			t.Fatalf("release authority readback is missing %q", required)
		}
	}
	oci, err := os.ReadFile(filepath.Join(root, "scripts", "release-oci-record.sh")) //nolint:gosec // Repository-owned release adapter.
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"--to-oci-layout", "validate-layout", "--from-oci-layout"} {
		if !strings.Contains(string(oci), required) {
			t.Fatalf("release OCI adapter is missing %q", required)
		}
	}
}

func TestReleaseSurfacesContainNoMovingOrUnnumberedIdentity(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	paths := []string{
		"README.md", "docs/bootstrap.md", "deploy/helm/tetral/README.md", "deploy/helm/tetral/Chart.yaml",
		"deploy/helm/tetral/values.yaml", "deploy/kubernetes", "services/api/k8s", "services/auth/k8s",
		"services/bridge/k8s", "services/cleanup/k8s", "services/event-stream/k8s", "services/gateway/k8s",
		"services/git-proxy/k8s", "services/queue/k8s", "services/sandbox/k8s", "services/agent-runtime/k8s",
	}
	for _, relative := range paths {
		info, err := os.Stat(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		if info.IsDir() {
			err = filepath.Walk(filepath.Join(root, relative), func(path string, info os.FileInfo, walkErr error) error {
				if walkErr == nil && !info.IsDir() && (strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".md")) {
					checkReleaseSurface(t, path)
				}
				return walkErr
			})
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		checkReleaseSurface(t, filepath.Join(root, relative))
	}
}

func TestReleaseSurfaceIdentityValidationAppliesToAnyOwnedRepository(t *testing.T) {
	for _, invalid := range []string{
		"ghcr.io/tetral-ai/new-service",
		"ghcr.io/tetral-ai/new-service:latest",
		"ghcr.io/tetral-ai/new-service:main",
		"ghcr.io/tetral-ai/new-service:0.1.0-alpha",
	} {
		if err := validateReleaseSurfaceIdentity(invalid); err == nil {
			t.Fatalf("moving release identity %q passed", invalid)
		}
	}
	for _, valid := range []string{
		"ghcr.io/tetral-ai/new-service:0.0.0-dev",
		"ghcr.io/tetral-ai/new-service:0.1.0-alpha.12",
		"ghcr.io/tetral-ai/new-service@sha256:" + strings.Repeat("a", 64),
		"ghcr.io/tetral-ai/new-service:<platform version>",
		"oci://ghcr.io/tetral-ai/charts/tetral --version 0.1.0-alpha.12",
	} {
		if err := validateReleaseSurfaceIdentity(valid); err != nil {
			t.Fatalf("fixed release identity %q failed: %v", valid, err)
		}
	}
}

func jobUsesLocalAction(job releaseWorkflowJob, action string) bool {
	for _, step := range job.Steps {
		if step["uses"] == action {
			return true
		}
	}
	return false
}

func marshalWorkflowJob(t *testing.T, job releaseWorkflowJob) string {
	t.Helper()
	body, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func checkReleaseSurface(t *testing.T, path string) {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // Repository-owned release surface under test.
	if err != nil {
		t.Fatal(err)
	}
	if err := validateReleaseSurfaceIdentity(string(body)); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

var ownedOCIReference = regexp.MustCompile(`ghcr\.io/tetral-ai/[a-z0-9][a-z0-9._/-]*(?::[^\s\x60\"'<>\[\]{}(),]+|@[^\s\x60\"'<>\[\]{}(),]+)?`)
var digestPlaceholder = regexp.MustCompile(`@sha256:<[a-z0-9-]+>`)
var numberedAlphaTag = regexp.MustCompile(`^0\.1\.0-alpha\.[1-9][0-9]*$`)
var immutableDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func validateReleaseSurfaceIdentity(text string) error {
	text = strings.ReplaceAll(text, "<platform version>", "0.1.0-alpha.1")
	text = digestPlaceholder.ReplaceAllString(text, "@sha256:"+strings.Repeat("a", 64))
	for _, location := range ownedOCIReference.FindAllStringIndex(text, -1) {
		reference := text[location[0]:location[1]]
		if before, digest, found := strings.Cut(reference, "@"); found {
			if before == "" || !immutableDigest.MatchString(digest) {
				return fmt.Errorf("owned OCI reference %q is not digest-addressed", reference)
			}
			continue
		}
		lastSlash := strings.LastIndex(reference, "/")
		colon := strings.LastIndex(reference, ":")
		if colon <= lastSlash {
			if location[0] >= len("oci://") && text[location[0]-len("oci://"):location[0]] == "oci://" {
				continue
			}
			return fmt.Errorf("owned OCI reference %q has no explicit tag or digest", reference)
		}
		tag := reference[colon+1:]
		if tag != "0.0.0-dev" && !numberedAlphaTag.MatchString(tag) {
			return fmt.Errorf("owned OCI reference %q uses a moving or unnumbered tag", reference)
		}
	}
	return nil
}
