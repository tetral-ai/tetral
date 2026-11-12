package media_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

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

func engineRootDir(t *testing.T) string {
	t.Helper()
	_, path, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(path), "..", "..", "..", "..", ".."))
}
