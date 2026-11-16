package integration

import (
	"context"
	"os/exec"
)

// buildCommand is the go build of pkg, a path from the repository root, into
// binary, with the race detector when the tests run with it. The build then
// finds its packages in the cache the test run filled; a race build beside
// tests compiled without the detector compiles the standard library and
// every dependency again, which outlasts the builds' time limits.
func buildCommand(ctx context.Context, binary, pkg string) *exec.Cmd {
	arguments := []string{"build"}
	if raceDetector {
		arguments = append(arguments, "-race")
	}
	//nolint:gosec // G204: a repository-owned command and a private output path.
	command := exec.CommandContext(ctx, "go", append(arguments, "-o", binary, pkg)...)
	command.Dir = ".."
	return command
}
