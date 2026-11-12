package static

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
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

func TestReleaseCLIDoesNotDependOnDatabaseRuntime(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", `{{.ImportPath}}|{{join .Imports ","}}`, "./internal/release/cmd/tetral-release")
	command.Dir = finalArchitectureEngineRoot(t)
	body, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	const identityPackage = "github.com/tetral-ai/tetral/internal/schemaidentity"
	const storagePackage = "github.com/tetral-ai/tetral/internal/storage"
	foundIdentity := false
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		name, imports, _ := strings.Cut(line, "|")
		if name == identityPackage {
			foundIdentity = true
			if imports != "" {
				t.Fatalf("shared schema identities must remain dependency-free; imports = %s", imports)
			}
		}
		if name == storagePackage || strings.HasPrefix(name, storagePackage+"/") ||
			name == "database/sql" || strings.HasPrefix(name, "database/sql/") ||
			strings.HasPrefix(name, "github.com/jackc/") || name == "github.com/lib/pq" {
			t.Fatalf("release CLI pulls database runtime dependency %s; read schemaidentity instead", name)
		}
	}
	if !foundIdentity {
		t.Fatal("release CLI must consume the shared schema identity owner")
	}
}

// The workflows that publish release artifacts start only by hand, are
// never cancelled half way, and hold write authority only in the jobs that
// run in the protected release environment. A release builds its images
// once; later transitions publish the recorded digests.
func TestReleaseWorkflowsHoldWriteAuthorityOnlyInTheReleaseEnvironment(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	for _, row := range []struct {
		file   string
		builds int
	}{
		{file: "engine-release.yml", builds: 1},
		{file: "mirror-base-images.yml", builds: 0},
	} {
		t.Run(row.file, func(t *testing.T) {
			path := filepath.Join(root, ".github", "workflows", row.file)
			body, err := os.ReadFile(path) //nolint:gosec // Repository-owned workflow under test.
			if err != nil {
				t.Fatal(err)
			}
			var workflow releaseWorkflow
			if err := testinfra.DecodeWorkflowYAML(body, path, &workflow); err != nil {
				t.Fatal(err)
			}
			if len(workflow.On) != 1 || workflow.On["workflow_dispatch"] == nil {
				t.Fatalf("triggers = %v; want workflow_dispatch only", slices.Sorted(maps.Keys(workflow.On)))
			}
			if workflow.Concurrency.Group == "" || workflow.Concurrency.CancelInProgress {
				t.Fatalf("concurrency = %+v; want one group that never cancels a run in progress", workflow.Concurrency)
			}
			builds := 0
			for name, job := range workflow.Jobs {
				permissions := job.Permissions
				if permissions == nil {
					permissions = workflow.Permissions
				}
				writes := false
				for _, access := range permissions {
					writes = writes || access == "write"
				}
				if writes != (job.Environment == "release") {
					t.Errorf("job %s: write authority %t, environment %q; write authority belongs to the release environment alone", name, writes, job.Environment)
				}
				for _, step := range job.Steps {
					if uses, _ := step["uses"].(string); strings.HasPrefix(uses, "docker/build-push-action@") {
						builds++
					}
				}
			}
			if builds != row.builds {
				t.Errorf("image build steps = %d; want %d", builds, row.builds)
			}
		})
	}
}

func TestReleaseSurfacesContainNoMovingOrUnnumberedIdentity(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	paths := []string{
		"README.md", "docs/bootstrap.md", "deploy/helm/tetral/README.md", "deploy/helm/tetral/Chart.yaml",
		"deploy/helm/tetral/values.yaml", "deploy/kubernetes", "services/api/k8s", "services/auth/k8s",
		"services/bridge/k8s", "services/cleanup/k8s", "services/event-stream/k8s", "services/gateway/k8s",
		"services/git-proxy/k8s", "services/queue/k8s", "services/sandbox/k8s", "services/agent-runtime/k8s",
		"services/job-runner/k8s", "services/web-connector/k8s",
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
