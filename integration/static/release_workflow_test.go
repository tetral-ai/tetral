package static

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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
	if err := yaml.Unmarshal(body, &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Name != "Release" || len(workflow.On) != 1 || workflow.On["workflow_dispatch"] == nil {
		t.Fatalf("release triggers = %#v; want workflow_dispatch only", workflow.On)
	}
	if workflow.Permissions["contents"] != "read" || workflow.Permissions["packages"] != "read" || workflow.Concurrency.CancelInProgress {
		t.Fatalf("release default authority = permissions:%v concurrency:%+v", workflow.Permissions, workflow.Concurrency)
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
	if err := yaml.Unmarshal(body, &workflow); err != nil {
		t.Fatal(err)
	}
	candidate := marshalWorkflowJob(t, workflow.Jobs["candidate-images"])
	finalize := marshalWorkflowJob(t, workflow.Jobs["finalize-candidate"])
	promote := marshalWorkflowJob(t, workflow.Jobs["promote"])
	for _, token := range []string{"docker/build-push-action@", "platforms: linux/amd64", "Build candidate exactly once"} {
		if !strings.Contains(candidate, token) {
			t.Fatalf("candidate build is missing %q", token)
		}
	}
	for _, token := range []string{"helm package", "artifact --kind helm-candidate", "artifact --kind candidate"} {
		if !strings.Contains(finalize, token) {
			t.Fatalf("candidate finalizer is missing %q", token)
		}
	}
	for _, forbidden := range []string{"docker/build-push-action@", "run: docker build ", "helm package"} {
		if strings.Contains(promote, forbidden) {
			t.Fatalf("promotion rebuilds or repackages through %q", forbidden)
		}
	}
	for _, required := range []string{"docker buildx imagetools create", "candidate_manifest_digest", "helm push", "validate-authorization", "git/tags", "gh release create"} {
		if !strings.Contains(promote, required) {
			t.Fatalf("promotion is missing digest-preserving step %q", required)
		}
	}
	text := string(body)
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
	if err := yaml.Unmarshal(body, &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Name != "Base Image Maintenance" || len(workflow.On) != 1 || workflow.On["workflow_dispatch"] == nil {
		t.Fatalf("base-image triggers = %#v", workflow.On)
	}
	text := string(body)
	for _, required := range []string{"repository@sha256:digest", "linux/amd64", "Mirror without overwrite", "source_identity", "Update the owning Dockerfile"} {
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
	body, err := yaml.Marshal(job)
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
	text := string(body)
	for _, forbidden := range []string{"ghcr.io/tetral-ai/tetral:latest", "ghcr.io/tetral-ai/gateway:latest", "ghcr.io/tetral-ai/agent-runtime:latest", "ghcr.io/tetral-ai/sandbox:latest", ":0.1.0-alpha"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("%s contains moving or unnumbered release identity %q", path, forbidden)
		}
	}
}
