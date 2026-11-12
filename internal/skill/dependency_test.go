package skill_test

import (
	"go/build"
	"strings"
	"testing"
)

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
