package memory_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryNormDependencyConfinedToInternalMemory(t *testing.T) {
	root := "../.."
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return parseErr
		}
		for _, imported := range file.Imports {
			filePath := filepath.ToSlash(path)
			if strings.Trim(imported.Path.Value, `"`) == "golang.org/x/text/unicode/norm" &&
				!strings.HasPrefix(filePath, "../../internal/pathvalidation/") {
				t.Errorf("unicode/norm import outside shared path validation helper: %s", filePath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
