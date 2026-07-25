package agent

import (
	"go/build"
	"strings"
	"testing"
)

func TestAgentPackageDoesNotImportForbiddenControlPlanePackages(t *testing.T) {
	pkg, err := build.Default.Import("github.com/tetral-ai/tetral/internal/agent", "", 0)
	if err != nil {
		t.Fatalf("import internal/agent: %v", err)
	}

	visited := map[string]bool{}
	var walk func(importPath string)
	walk = func(importPath string) {
		if visited[importPath] {
			return
		}
		visited[importPath] = true
		p, err := build.Default.Import(importPath, pkg.Dir, 0)
		if err != nil {
			// External (stdlib or vendored) packages without .go files
			// in the GOPATH layout end up here. Treat as opaque leaves.
			return
		}
		for _, dep := range p.Imports {
			walk(dep)
		}
	}
	for _, dep := range pkg.Imports {
		walk(dep)
	}

	forbidden := []string{
		"github.com/tetral-ai/tetral/internal/session",
		"github.com/tetral-ai/tetral/internal/vault",
		"github.com/tetral-ai/tetral/internal/skill",
	}
	for dep := range visited {
		for _, prefix := range forbidden {
			if strings.HasPrefix(dep, prefix) {
				t.Errorf("internal/agent imports forbidden package: %s", dep)
			}
		}
	}
}
