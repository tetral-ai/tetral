package internalgrpc_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestGRPCAndProtobufVersionFloors(t *testing.T) {
	mod := readGoMod(t)
	for _, requirement := range []struct {
		module string
		min    semver
	}{
		{module: "google.golang.org/grpc", min: semver{1, 79, 3}},
		{module: "google.golang.org/protobuf", min: semver{1, 33, 0}},
	} {
		got := findVersion(mod, requirement.module)
		if got == nil {
			t.Fatalf("go.mod missing %s", requirement.module)
			continue
		}
		if got.lessThan(requirement.min) {
			t.Fatalf("%s = v%d.%d.%d; want >= v%d.%d.%d", requirement.module, got.major, got.minor, got.patch, requirement.min.major, requirement.min.minor, requirement.min.patch)
		}
	}
}

type semver struct {
	major, minor, patch int
}

func (a semver) lessThan(b semver) bool {
	if a.major != b.major {
		return a.major < b.major
	}
	if a.minor != b.minor {
		return a.minor < b.minor
	}
	return a.patch < b.patch
}

func findVersion(modText string, module string) *semver {
	for _, line := range strings.Split(modText, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 || fields[0] != module {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(fields[1], "v"), ".")
		if len(parts) < 3 {
			return nil
		}
		major, ok1 := atoi(parts[0])
		minor, ok2 := atoi(parts[1])
		patch, ok3 := atoi(parts[2])
		if !ok1 || !ok2 || !ok3 {
			return nil
		}
		return &semver{major: major, minor: minor, patch: patch}
	}
	return nil
}

func atoi(value string) (int, bool) {
	total := 0
	if value == "" {
		return 0, false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, false
		}
		total = total*10 + int(r-'0')
	}
	return total, true
}

func readGoMod(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	body, err := os.ReadFile(filepath.Join(root, "go.mod")) //nolint:gosec // source tree path
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	return string(body)
}
