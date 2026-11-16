package media_test

import (
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/mod/semver"
)

// golang.org/x/image below v0.43.0 decodes WebP with GO-2026-5061.
func TestXImageVersionMeetsWebPSecurityFloor(t *testing.T) {
	output, err := exec.Command("go", "list", "-m", "-f", "{{.Version}}", "golang.org/x/image").Output()
	if err != nil {
		t.Fatalf("go list -m golang.org/x/image: %v", err)
	}
	if version := strings.TrimSpace(string(output)); semver.Compare(version, "v0.43.0") < 0 {
		t.Fatalf("golang.org/x/image is %s; want v0.43.0 or later, which fixes GO-2026-5061", version)
	}
}
