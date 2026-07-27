package resourceprojection

import (
	"strings"
	"testing"
)

// Daytona executes the mount script as the runtime user, and the staging root
// lives under root-owned /mnt, so its creation must be one of the script's
// sudo'd steps like every other privileged line.
func TestMountBindVerifyCommandCreatesStagingRootWithSudo(t *testing.T) {
	script := MountBindVerifyCommand(Plan{
		ResourcePrefix: "workspaces/ws_test/sessions/sesn_test/resources/",
	}, MountBindVerifyCommandConfig{
		Bucket:                "tetral-files",
		RcloneVFSCacheMaxSize: "2G",
		RcloneVFSMinFree:      "1G",
	})
	if !strings.Contains(script, "sudo install -d -m 0755 \"$STAGING\"\n") {
		t.Fatalf("mount script does not sudo the staging root install:\n%s", script)
	}
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "install -d -m 0755 \"$STAGING\"") {
			t.Fatalf("mount script installs the staging root unprivileged:\n%s", script)
		}
	}
}
