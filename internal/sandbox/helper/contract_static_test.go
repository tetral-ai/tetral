package helper_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

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
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", "./...")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list Helper dependency closure: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		for _, forbidden := range []string{
			"github.com/daytonaio/",
			"github.com/tetral-ai/tetral/internal/sandbox/driver",
			"github.com/tetral-ai/tetral/services/",
		} {
			if strings.HasPrefix(dependency, forbidden) {
				t.Fatalf("Helper dependency closure contains forbidden boundary %q", dependency)
			}
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
