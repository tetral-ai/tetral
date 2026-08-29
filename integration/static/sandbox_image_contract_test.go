package static

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The projection mount passes --vfs-cache-min-free-space, which Ubuntu's
// rclone (v1.60.1) does not have. Building the sandbox image from the
// distribution package makes every resource projection fail with rclone's
// generic "mount not ready" once its daemon-wait expires, with no other
// diagnostic anywhere. The image therefore installs rclone from upstream,
// and this gate keeps the two in step.
func TestSandboxImageInstallsUpstreamRclone(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	dockerfile, err := os.ReadFile(filepath.Join(engineRoot, "Dockerfile.sandbox"))
	if err != nil {
		t.Fatalf("read Dockerfile.sandbox: %v", err)
	}
	text := string(dockerfile)

	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), "\\"))
		if trimmed == "rclone" {
			t.Fatal("Dockerfile.sandbox installs rclone from the distribution; " +
				"the archived version predates --vfs-cache-min-free-space")
		}
	}
	if !strings.Contains(text, "github.com/rclone/rclone/releases/download/v${RCLONE_RELEASE}") {
		t.Fatal("Dockerfile.sandbox does not install rclone from the project's official release archive")
	}
	if !strings.Contains(text, "sha256sum -c -") {
		t.Fatal("the upstream rclone download is not checksum-verified")
	}
	if !strings.Contains(text, "--vfs-cache-min-free-space") {
		t.Fatal("the image build does not assert the mount flag the projection requires")
	}
	if strings.Contains(text, "ARG RCLONE_VERSION") {
		t.Fatal("RCLONE_VERSION collides with rclone's own --version flag; use another name")
	}
}
