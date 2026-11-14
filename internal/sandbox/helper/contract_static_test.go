package helper_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The helper is copied into a Sandbox and runs there alone: nothing it links
// is a provider's SDK, a Kubernetes client, the database or a service.
func TestHelperDoesNotImportServiceOrProviderBoundaries(t *testing.T) {
	root := helperRoot(t)
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", "./...")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list Helper dependency closure: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		for _, forbidden := range []string{
			"github.com/daytonaio/",
			"k8s.io/",
			"github.com/tetral-ai/tetral/internal/storage",
			"github.com/tetral-ai/tetral/internal/sandbox/driver",
			"github.com/tetral-ai/tetral/services/",
		} {
			if strings.HasPrefix(dependency, forbidden) {
				t.Fatalf("Helper dependency closure contains forbidden boundary %q", dependency)
			}
		}
	}
}

func helperRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return cwd
}
