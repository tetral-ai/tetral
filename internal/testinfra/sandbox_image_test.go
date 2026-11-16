package testinfra

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The sandbox image's rclone step unpacks the downloaded archive only when
// it matches the pinned SHA-256. The step runs under bash as Docker runs it,
// with a stub curl that serves a stand-in archive and its absolute paths
// moved into the test's directory.
func TestTheSandboxImageUnpacksRcloneOnlyWhenItsArchiveMatchesItsPin(t *testing.T) {
	dockerfile, err := os.ReadFile(filepath.Join(repositoryRootForTest(t), "Dockerfile.sandbox"))
	if err != nil {
		t.Fatal(err)
	}
	args := map[string]string{}
	for _, match := range regexp.MustCompile(`(?m)^ARG (RCLONE_\w+)=(\S+)$`).FindAllStringSubmatch(string(dockerfile), -1) {
		args[match[1]] = match[2]
	}
	release, pin := args["RCLONE_RELEASE"], args["RCLONE_ARCHIVE_SHA256"]
	var step string
	for _, run := range dockerfileStageRuns(string(dockerfile), "") {
		if strings.Contains(run, "${RCLONE_RELEASE}") {
			step = run
		}
	}
	if release == "" || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(pin) || step == "" {
		t.Fatalf("the final stage pins rclone %q at %q and installs it with %q; want a release, a SHA-256 and a step", release, pin, step)
	}
	for _, row := range []struct {
		name    string
		matches bool
	}{
		{"an archive with the pinned SHA-256 is installed", true},
		{"an archive with another SHA-256 is not unpacked", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, sub := range []string{"tmp", "bin", "stub"} {
				if err := os.Mkdir(filepath.Join(dir, sub), 0o750); err != nil {
					t.Fatal(err)
				}
			}
			archive := filepath.Join(dir, "rclone.zip")
			writeRcloneArchive(t, archive, release)
			sum := pin
			if row.matches {
				body, err := os.ReadFile(archive) //nolint:gosec // The archive this test wrote.
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(body)
				sum = hex.EncodeToString(digest[:])
			}
			curl := "#!/bin/bash\nwhile [ $# -gt 0 ]; do if [ \"$1\" = -o ]; then cp \"$RCLONE_STAND_IN\" \"$2\"; exit; fi; shift; done; exit 1\n"
			if err := os.WriteFile(filepath.Join(dir, "stub", "curl"), []byte(curl), 0o755); err != nil { //nolint:gosec // The stub curl must be executable.
				t.Fatal(err)
			}
			script := strings.NewReplacer("/tmp/", dir+"/tmp/", "/usr/bin", dir+"/bin").Replace(step)
			command := exec.Command("bash", "--noprofile", "--norc", "-e", "-c", script) //nolint:gosec // The repository's own step under test.
			command.Env = append(os.Environ(), "RCLONE_RELEASE="+release, "RCLONE_ARCHIVE_SHA256="+sum, "RCLONE_STAND_IN="+archive,
				"PATH="+filepath.Join(dir, "stub")+string(os.PathListSeparator)+filepath.Join(dir, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
			output, err := command.CombinedOutput()
			_, statErr := os.Stat(filepath.Join(dir, "bin", "rclone"))
			if installed := statErr == nil; row.matches != (err == nil) || row.matches != installed {
				t.Fatalf("the step ended with %v and installed rclone: %t; want success and an install only with the pinned archive\n%s", err, installed, output)
			}
		})
	}
}

// writeRcloneArchive writes a release archive of release whose rclone
// answers the step's checks.
func writeRcloneArchive(t *testing.T, path, release string) {
	t.Helper()
	file, err := os.Create(path) //nolint:gosec // A path in the test's directory.
	if err != nil {
		t.Fatal(err)
	}
	archive := zip.NewWriter(file)
	header := &zip.FileHeader{Name: "rclone-v" + release + "-linux-amd64/rclone", Method: zip.Deflate}
	header.SetMode(0o755)
	entry, err := archive.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("#!/bin/bash\ncase \"$1\" in version) echo rclone v" + release + ";; mount) echo --vfs-cache-min-free-space;; esac\n")); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
