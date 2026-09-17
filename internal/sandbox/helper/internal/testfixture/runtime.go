// Package testfixture provides runtime isolation for helper test executables.
// It is imported only by tests, never by the production helper.
package testfixture

import (
	"fmt"
	"os"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/runtimepath"
)

const RuntimeRootEnv = "TETRAL_HELPER_TEST_RUNTIME_ROOT"

// Run owns one short runtime directory for a test executable and its reexecs.
// Reexec children inherit the directory; only the creating parent removes it.
// The short path also leaves room for detached-task Unix socket names.
func Run(run func() int) int {
	if inherited := os.Getenv(RuntimeRootEnv); inherited != "" {
		runtimepath.SetForTesting(inherited)
		return run()
	}
	root, err := os.MkdirTemp("/tmp", "tht-*")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create helper test runtime: %v\n", err)
		return 1
	}
	if err := os.Setenv(RuntimeRootEnv, root); err != nil {
		_ = os.RemoveAll(root)
		return 1
	}
	runtimepath.SetForTesting(root)
	code := run()
	if err := os.RemoveAll(root); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "remove helper test runtime: %v\n", err)
		return 1
	}
	return code
}
