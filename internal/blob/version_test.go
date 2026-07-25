package blob_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestAWSSDKVersionPinning enforces the minimum safe versions for the AWS SDK
// for Go v2 dependencies. service/s3
// >= v1.97.3 and aws/protocol/eventstream >= v1.7.8 close
// GHSA-xmrv-pmrh-hhx2 (the 2026 EventStream decoder DoS advisory).
//
// The check parses go.mod for direct/indirect requires of those two
// modules and refuses to ship with any pinned version older than the
// floor.
func TestAWSSDKVersionPinning(t *testing.T) {
	mod := readEngineGoMod(t)
	requirements := []struct {
		path string
		min  semver
	}{
		{"github.com/aws/aws-sdk-go-v2/service/s3", semver{1, 97, 3}},
		{"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream", semver{1, 7, 8}},
	}
	for _, req := range requirements {
		got := findModuleVersion(mod, req.path)
		if got == nil {
			t.Errorf("go.mod is missing required dep %s", req.path)
			continue
		}
		if got.lessThan(req.min) {
			t.Errorf("%s pinned at v%d.%d.%d; need >= v%d.%d.%d to address GHSA-xmrv-pmrh-hhx2",
				req.path, got.major, got.minor, got.patch, req.min.major, req.min.minor, req.min.patch)
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

// findModuleVersion scans the go.mod text for an exact module path
// and returns its parsed version. Returns nil when the module is
// absent.
func findModuleVersion(modText, modulePath string) *semver {
	for _, line := range strings.Split(modText, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		// Lines look like `<path> v1.2.3` or `<path> v1.2.3 // indirect`.
		if fields[0] != modulePath {
			continue
		}
		v := strings.TrimPrefix(fields[1], "v")
		parts := strings.Split(v, ".")
		if len(parts) < 3 {
			continue
		}
		major, ok1 := atoiSafe(parts[0])
		minor, ok2 := atoiSafe(parts[1])
		patch, ok3 := atoiSafe(parts[2])
		if !ok1 || !ok2 || !ok3 {
			continue
		}
		return &semver{major: major, minor: minor, patch: patch}
	}
	return nil
}

func atoiSafe(s string) (int, bool) {
	n := 0
	if s == "" {
		return 0, false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	return n, true
}

func readEngineGoMod(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	body, err := os.ReadFile(filepath.Join(root, "go.mod")) //nolint:gosec // engine source path
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	return string(body)
}
