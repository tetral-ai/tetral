// Package runtimeidentity owns the named Linux identity used for every Agent
// Tool effect inside a prepared Sandbox. The Driver uses it while preparing
// runtime-owned paths and Git configuration; the root Helper uses it after
// opening the protected Tool payload and before dispatching the Tool.
package runtimeidentity

import "os"

const (
	User  = "daytona"
	Home  = "/home/daytona"
	Shell = "/bin/bash"
)

// IsReservedEnvironmentKey reports whether a Tool command must receive the
// Helper-owned value (or absence) instead of an inherited or caller value.
func IsReservedEnvironmentKey(key string) bool {
	switch key {
	case "HOME", "USER", "LOGNAME", "SUDO_USER", "SUDO_UID", "SUDO_GID", "SUDO_COMMAND":
		return true
	default:
		return false
	}
}

// NormalizeProcessEnvironment completes the root Helper's identity transition
// after its irreversible credential drop. File Tools observe this process
// environment directly; command Tools reserve the same values again when
// composing their final child environment.
func NormalizeProcessEnvironment() error {
	for _, key := range []string{"SUDO_USER", "SUDO_UID", "SUDO_GID", "SUDO_COMMAND"} {
		if err := os.Unsetenv(key); err != nil {
			return err
		}
	}
	for key, value := range map[string]string{
		"HOME":    Home,
		"USER":    User,
		"LOGNAME": User,
	} {
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return nil
}
