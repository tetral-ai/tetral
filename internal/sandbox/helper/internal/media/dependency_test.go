package media_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPDFCPUDependencyIsConfinedToAuthorizedPackages(t *testing.T) {
	root := engineRootDir(t)
	allowedDirs := []string{
		filepath.Join(root, "internal", "sandbox", "helper", "internal", "media"),
		filepath.Join(root, "internal", "files", "pdfcount"),
	}
	violations, err := pdfCPUImportViolations(root, allowedDirs)
	if err != nil {
		t.Fatalf("scan engine imports: %v", err)
	}
	for _, path := range violations {
		t.Errorf("%s imports pdfcpu outside an authorized package", path)
	}
}

func TestXImageVersionMeetsWebPSecurityFloor(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(engineRootDir(t), "go.mod")) //nolint:gosec // repository-local dependency manifest.
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "golang.org/x/image" {
			if fields[1] != "v0.43.0" {
				t.Fatalf("golang.org/x/image version = %s; want v0.43.0 security floor for GO-2026-5061", fields[1])
			}
			return
		}
	}
	t.Fatal("go.mod is missing golang.org/x/image")
}

func pdfCPUImportViolations(root string, allowedDirs []string) ([]string, error) {
	var violations []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "testdata") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			importPath := strings.Trim(spec.Path.Value, `"`)
			if importPath != "github.com/pdfcpu/pdfcpu" && !strings.HasPrefix(importPath, "github.com/pdfcpu/pdfcpu/") {
				continue
			}
			authorized := false
			for _, allowedDir := range allowedDirs {
				if filepath.Clean(filepath.Dir(path)) == filepath.Clean(allowedDir) {
					authorized = true
					break
				}
			}
			if !authorized {
				relativePath, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				violations = append(violations, relativePath)
			}
		}
		return nil
	})
	return violations, err
}

func TestPDFCPUConfinementFindsProductionImportOutsideAllowedPackage(t *testing.T) {
	root := t.TempDir()
	helperDir := filepath.Join(root, "internal", "sandbox", "helper", "internal", "media")
	filesDir := filepath.Join(root, "internal", "files", "pdfcount")
	for path, source := range map[string]string{
		filepath.Join(helperDir, "pdf.go"):                 `package media; import "github.com/pdfcpu/pdfcpu/pkg/api"`,
		filepath.Join(filesDir, "count.go"):                `package pdfcount; import "github.com/pdfcpu/pdfcpu/pkg/api"`,
		filepath.Join(root, "internal", "other", "pdf.go"): `package other; import "github.com/pdfcpu/pdfcpu/pkg/api"`,
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create fixture directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
	}

	violations, err := pdfCPUImportViolations(root, []string{helperDir, filesDir})
	if err != nil {
		t.Fatalf("scan fixture: %v", err)
	}
	if len(violations) != 1 || violations[0] != filepath.Join("internal", "other", "pdf.go") {
		t.Fatalf("violations = %v; want the production import outside helper media", violations)
	}
}

func engineRootDir(t *testing.T) string {
	t.Helper()
	_, path, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(path), "..", "..", "..", "..", ".."))
}
