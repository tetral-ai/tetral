package helper_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHelperHealthChecksDraftBaseTemplateInvariants(t *testing.T) {
	source := readRepoFile(t, "internal", "health", "health.go")
	for _, token := range []string{
		`checkExecutable("sandbox"`,
		`checkRG(ctx)`,
		`checkRuntimeRoot()`,
		`checkPatchEngine()`,
		`checkExecutable("rclone"`,
		`checkFusermount3()`,
		`checkFuseConf()`,
		`user_allow_other`,
	} {
		if !strings.Contains(source, token) {
			t.Fatalf("helper health contract missing %q", token)
		}
	}
}

func TestHelperDoesNotImportServiceOrProviderBoundaries(t *testing.T) {
	root := helperRoot(t)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path) //nolint:gosec // repository-local source walk.
		if err != nil {
			return err
		}
		source := string(body)
		for _, forbidden := range []string{
			"github.com/daytonaio",
			"k8s.io/",
			"github.com/tetral-ai/tetral/internal/storage",
			"github.com/tetral-ai/tetral/internal/sandbox/driver",
			"github.com/tetral-ai/tetral/services/",
			`"daytona"`,
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s contains forbidden helper boundary token %q", path, forbidden)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk helper source: %v", err)
	}
}

func TestMakefileBuildsStaticSandboxHelperArtifact(t *testing.T) {
	makefilePath := filepath.Join(helperRoot(t), "..", "..", "..", "Makefile")
	body, err := os.ReadFile(makefilePath) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	source := string(body)
	for _, token := range []string{
		"build-sandbox-helper:",
		"install-sandbox-helper:",
		"CGO_ENABLED=0",
		"GOOS=linux",
		"./internal/sandbox/helper/cmd/sandbox",
		"SANDBOX_HELPER_INSTALL_PATH ?= /usr/local/bin/sandbox",
		"install -D -m 0755",
	} {
		if !strings.Contains(source, token) {
			t.Fatalf("Makefile sandbox helper build/install contract missing %q", token)
		}
	}
}

func TestHelperPackageLayoutMatchesContract(t *testing.T) {
	for _, dir := range []string{
		"bound",
		"execute",
		"task",
		"filetool",
		"patch",
		"search",
		"media",
		"health",
	} {
		if info, err := os.Stat(filepath.Join(helperRoot(t), "internal", dir)); err != nil || !info.IsDir() {
			t.Fatalf("helper contract package internal/%s is missing", dir)
		}
	}
}

func TestTaskHelperDoesNotProduceHelperFailureKind(t *testing.T) {
	source := readRepoFile(t, "internal", "task", "exec.go")
	for _, forbidden := range []string{
		`errorResponse("helper_failure"`,
		`Kind: "helper_failure"`,
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("command helper emits reserved helper_failure kind via %q", forbidden)
		}
	}
}

func TestSandboxServicePreparesBaseDirectoriesBeforeResourceSteps(t *testing.T) {
	serviceSource, err := os.ReadFile(filepath.Join(helperRoot(t), "..", "service.go")) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read service.go: %v", err)
	}
	source := string(serviceSource)
	health := strings.Index(source, "CheckBaseTemplateHealth(ctx, handle)")
	prepareResources := strings.Index(source, "prepareResources(ctx, setup, handle)")
	baseDirectories := strings.Index(source, "PrepareBaseDirectories(ctx, handle)")
	prepareSessionResources := strings.Index(source, "prepareSessionResources(ctx, setup, handle)")
	if health < 0 || prepareResources < 0 || baseDirectories < 0 || prepareSessionResources < 0 {
		t.Fatalf("service.go missing health/base-directory/resource lifecycle calls")
	}
	if health >= prepareResources || prepareResources >= baseDirectories || baseDirectories >= prepareSessionResources {
		t.Fatalf("base-template/base-directory order invalid: health=%d prepareResources=%d baseDirectories=%d prepareSessionResources=%d",
			health, prepareResources, baseDirectories, prepareSessionResources)
	}
}

func readRepoFile(t *testing.T, parts ...string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(append([]string{helperRoot(t)}, parts...)...)) //nolint:gosec // repository-local source path.
	if err != nil {
		t.Fatalf("read helper file %v: %v", parts, err)
	}
	return string(body)
}

func helperRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return cwd
}
