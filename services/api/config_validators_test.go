package tetralapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
)

// TestValidateVaultKeyRejectionsAreConfigErrors proves the vault-key validator
// surfaces a workload.ConfigError naming ENGINE_VAULT_KEY so startup logs the
// safe key name rather than a class-only line. The empty and wrong-length cases
// both carry static text with no key material.
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
// config error naming ENGINE_DATA_DIR, not a class-only dependency failure.
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

// TestLoadPositiveIntRejectionsAreConfigErrors proves the positive-integer
// loaders fail config validation with a ConfigError naming the offending env key.
func TestLoadPositiveIntRejectionsAreConfigErrors(t *testing.T) {
	env := envMapValidators{"TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST": "-1"}
	_, err := loadPositiveInt(env, "TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST", 32)
	if err == nil {
		t.Fatal("loadPositiveInt accepted a negative value")
	}
	configErr, ok := workload.AsConfigError(err)
	if !ok {
		t.Fatalf("loadPositiveInt returned %T; want a workload.ConfigError", err)
	}
	if !strings.Contains(configErr.Error(), "TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST") {
		t.Fatalf("config error %q does not name the env key", configErr.Error())
	}

	env64 := envMapValidators{"TETRAL_SESSION_EVENT_BODY_BYTE_CAP": "0"}
	_, err = loadPositiveInt64(env64, "TETRAL_SESSION_EVENT_BODY_BYTE_CAP", 1<<20)
	if err == nil {
		t.Fatal("loadPositiveInt64 accepted a zero value")
	}
	if _, ok := workload.AsConfigError(err); !ok {
		t.Fatalf("loadPositiveInt64 returned %T; want a workload.ConfigError", err)
	}
}

type envMapValidators map[string]string

func (m envMapValidators) Getenv(key string) string { return m[key] }
