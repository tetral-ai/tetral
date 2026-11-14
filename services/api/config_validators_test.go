package tetralapi

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
)

// TestValidateVaultKeyRejectionsAreConfigErrors proves the vault-key validator
// surfaces a workload.ConfigError naming ENGINE_VAULT_KEY so startup logs a
// config_error with the key's name. The empty and wrong-length cases both
// carry static text with no key material.
func TestValidateVaultKeyRejectionsAreConfigErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		key  string
	}{
		{name: "empty", key: ""},
		{name: "wrong length", key: "short"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateVaultKey(test.key)
			if err == nil {
				t.Fatalf("ValidateVaultKey(%q) returned nil; want a rejection", test.key)
			}
			configErr, ok := workload.AsConfigError(err)
			if !ok {
				t.Fatalf("ValidateVaultKey(%q) returned %T; want a workload.ConfigError", test.key, err)
			}
			if !strings.Contains(configErr.Error(), "ENGINE_VAULT_KEY") {
				t.Fatalf("config error %q does not name ENGINE_VAULT_KEY", configErr.Error())
			}
		})
	}
}

// TestEnsureDataDirRejectionIsConfigError proves a loosened data directory is a
// config error naming ENGINE_DATA_DIR, not a dependency failure.
func TestEnsureDataDirRejectionIsConfigError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(dir, 0755); err != nil { //nolint:gosec // G302: deliberately group/other accessible to drive the rejection path under test.
		t.Fatalf("chmod 0755: %v", err)
	}
	err := EnsureDataDir(dir)
	if err == nil {
		t.Fatal("EnsureDataDir accepted a group/other accessible data directory")
	}
	configErr, ok := workload.AsConfigError(err)
	if !ok {
		t.Fatalf("EnsureDataDir returned %T; want a workload.ConfigError", err)
	}
	if !strings.Contains(configErr.Error(), "ENGINE_DATA_DIR") {
		t.Fatalf("config error %q does not name ENGINE_DATA_DIR", configErr.Error())
	}
}

// A data directory that cannot be created fails with the operating system's
// error, which becomes the startup record's cause, without the configured path.
func TestEnsureDataDirFailureDoesNotNameThePath(t *testing.T) {
	file := filepath.Join(t.TempDir(), "secret-shaped-segment")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	err := EnsureDataDir(filepath.Join(file, "data"))
	if err == nil || err.Error() != "create data dir: mkdir: not a directory" {
		t.Fatalf("EnsureDataDir error = %v; want the operating system's error without the path", err)
	}
	if !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("EnsureDataDir error %v does not keep the operating system's error in its chain", err)
	}
}

// TestLoadPositiveIntRejectionsAreConfigErrors proves the positive-integer
// loaders fail config validation with a ConfigError naming the offending env key.
func TestLoadPositiveIntRejectionsAreConfigErrors(t *testing.T) {
	// The message names the key and never the value, which may be anything.
	const value = "-1 credential-startup-sentinel"
	env := envMapValidators{"TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST": value}
	_, err := loadPositiveInt(env, "TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST", 32)
	if configErr, ok := workload.AsConfigError(err); !ok || configErr.Error() != "TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST must be a positive integer" {
		t.Fatalf("loadPositiveInt(%q) = %v; want the ConfigError naming the key only", value, err)
	}

	env64 := envMapValidators{"TETRAL_SESSION_EVENT_BODY_BYTE_CAP": value}
	_, err = loadPositiveInt64(env64, "TETRAL_SESSION_EVENT_BODY_BYTE_CAP", 1<<20)
	if configErr, ok := workload.AsConfigError(err); !ok || configErr.Error() != "TETRAL_SESSION_EVENT_BODY_BYTE_CAP must be a positive integer" {
		t.Fatalf("loadPositiveInt64(%q) = %v; want the ConfigError naming the key only", value, err)
	}
}

// TestDriveUploadBudgetFloor proves the upload budget refuses a per-minute
// amount below 65,536 bytes, the least a request counts.
func TestDriveUploadBudgetFloor(t *testing.T) {
	if got, err := loadDriveUploadBudgetFromEnv(envMapValidators{envDriveUploadBudgetBytesPerMinute: "65536"}); err != nil || got != 65536 {
		t.Fatalf("65536 = %d, %v; want accepted", got, err)
	}
	_, err := loadDriveUploadBudgetFromEnv(envMapValidators{envDriveUploadBudgetBytesPerMinute: "65535"})
	if _, ok := workload.AsConfigError(err); !ok {
		t.Fatalf("65535: %v; want a ConfigError", err)
	}
}

type envMapValidators map[string]string

func (m envMapValidators) Getenv(key string) string { return m[key] }
