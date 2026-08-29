//go:build !linux

package testinfra

import "os"

func currentProcessIdentity() (int, string) {
	return os.Getpid(), "unsupported"
}

func processIdentityAlive(_ int, _ string) bool {
	// Fail closed on hosts without a stable process-start identity. Normal
	// teardown still removes containers owned by the current invocation.
	return true
}

func processStartIdentity(_ int) string {
	return "unsupported"
}
