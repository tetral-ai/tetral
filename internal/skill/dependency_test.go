package skill_test

import (
	"go/build"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestSkillIsTheOnlyYAMLConsumer pins the YAML parser confinement rule:
// production use of gopkg.in/yaml.v3 must remain inside the Skill
// package/frontmatter parser package. The one test-only exception performs
// object-level YAML parsing to validate Helm chart equivalence. Other Engine
// packages must continue to follow engine/CLAUDE.md's stdlib-first dependency
// rule.
//
// The test walks every Go source file under engine/ and asserts that
// no file outside `internal/skill` or the exact chart-equivalence test imports
// `gopkg.in/yaml.v3` directly.
func TestSkillIsTheOnlyYAMLConsumer(t *testing.T) {
	const yamlImport = `"gopkg.in/yaml.v3"`
	const allowedDir = "internal/skill"
	const allowedChartTest = "deploy/helm/chart_test.go"

	engineRoot := engineRootDir(t)
	violations := []string{}
	err := filepath.WalkDir(engineRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			name := d.Name()
			if name == "vendor" || name == "testdata" || name == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, relErr := filepath.Rel(engineRoot, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, allowedDir+"/") || rel == allowedChartTest {
			return nil
		}
		body, readErr := os.ReadFile(path) //nolint:gosec // engine source path, walked from root
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(body), yamlImport) {
			violations = append(violations, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk engine sources: %v", err)
	}
	for _, file := range violations {
		t.Errorf("file %s imports forbidden gopkg.in/yaml.v3 outside internal/skill", file)
	}
}

// TestSkillPackageImportsAreConfined pins the package-level rule: the
// internal/skill package imports yaml.v3 (legitimate) but does NOT
// pull in any other surprise third-party dependency. Engine's
// dependency budget is tight; this test catches accidental sprawl.
func TestSkillPackageImportsAreConfined(t *testing.T) {
	pkg, err := build.Default.Import("github.com/tetral-ai/tetral/internal/skill", "", 0)
	if err != nil {
		t.Fatalf("import internal/skill: %v", err)
	}
	allowed := map[string]bool{
		"gopkg.in/yaml.v3":               true,
		"github.com/jackc/pgx/v5/pgconn": true,
	}
	for _, dep := range pkg.Imports {
		if isStdlibImport(dep) {
			continue
		}
		if strings.HasPrefix(dep, "github.com/tetral-ai/tetral/") {
			continue
		}
		if !allowed[dep] {
			t.Errorf("internal/skill imports unexpected third-party dep: %s", dep)
		}
	}
}

// engineRootDir returns the absolute path of the `engine/` module
// root, located by reflecting on this test file.
func engineRootDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// thisFile lives at engine/internal/skill/dependency_test.go.
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

// isStdlibImport returns true for canonical stdlib import paths. Go's
// standard library has no dot in the first path segment, while every
// third-party module uses `host/path` or `gopkg.in/...`.
func isStdlibImport(path string) bool {
	first := path
	if i := strings.Index(path, "/"); i >= 0 {
		first = path[:i]
	}
	return !strings.Contains(first, ".")
}
