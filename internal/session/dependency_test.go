package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionPersistenceFilesStayOutOfServiceDependencies(t *testing.T) {
	paths := sessionProductionFilesForDependencyTest(t)
	for _, path := range paths {
		if sessionServiceSideFileForDependencyTest(path) {
			continue
		}
		source := readSessionSourceForDependencyTest(t, path)
		for _, forbidden := range []string{
			"github.com/tetral-ai/tetral/internal/agent",
			"github.com/tetral-ai/tetral/internal/environment",
			"github.com/tetral-ai/tetral/internal/files",
			"github.com/tetral-ai/tetral/internal/memory",
			"github.com/tetral-ai/tetral/internal/sandbox",
			"github.com/tetral-ai/tetral/internal/vault",
			"github.com/tetral-ai/tetral/internal/" + "worker",
			"github.com/tetral-ai/tetral/internal/kubernetes",
			"github.com/tetral-ai/tetral/internal/anthropic",
			"github.com/tetral-ai/tetral/internal/claude",
			"github.com/cloudflare",
			"github.com/e2b",
			"github.com/modal",
			"net/http",
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s must not import service-side or execution dependency %q", path, forbidden)
			}
		}
		for _, forbidden := range []string{
			"AgentResolver",
			"ResolveAgentConfig",
			"SessionManager",
			"Worker",
			"Kubernetes",
			"Anthropic",
			"Claude",
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s must not expose %s after Session/Agent decoupling", path, forbidden)
			}
		}
	}
}

func sessionServiceSideFileForDependencyTest(path string) bool {
	switch filepath.Base(path) {
	case "api.go", "service.go", "validation.go":
		return true
	default:
		return false
	}
}

func sessionProductionFilesForDependencyTest(t *testing.T) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk session package: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no production Go files found for session dependency boundary test")
	}
	return paths
}

func readSessionSourceForDependencyTest(t *testing.T, name string) string {
	t.Helper()
	source, err := os.ReadFile(filepath.Clean(name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(source)
}
