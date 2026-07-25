package tetralapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/workload"
)

// TestAsStartupConfigErrorConvertsForeignConfigErrors is the non-DB unit proof
// (Criterion A.3) that the single shared helper re-expresses a foreign
// config-validation error — blob.ConfigError — as a
// workload.ConfigError carrying the underlying safe static message. The foreign
// types are structurally unmatchable by workload.AsConfigError, so without the
// helper they would degrade to class-only startup_error.
func TestAsStartupConfigErrorConvertsForeignConfigErrors(t *testing.T) {
	for _, test := range []struct {
		name    string
		foreign error
		wantKey string
	}{
		{
			name:    "blob config error",
			foreign: &blob.ConfigError{Message: "blob: " + blob.EnvRegion + " is required"},
			wantKey: blob.EnvRegion,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			converted := asStartupConfigError(test.foreign)
			configErr, ok := workload.AsConfigError(converted)
			if !ok {
				t.Fatalf("asStartupConfigError(%T) yielded %T; want a workload.ConfigError", test.foreign, converted)
			}
			if configErr.Message != test.foreign.Error() {
				t.Fatalf("converted message = %q; want the underlying safe message %q", configErr.Message, test.foreign.Error())
			}
			if !strings.Contains(configErr.Message, test.wantKey) {
				t.Fatalf("converted message %q does not name %q", configErr.Message, test.wantKey)
			}
		})
	}
}

// TestAsStartupConfigErrorPassesThroughExistingConfigError proves the helper
// preserves an already-typed workload.ConfigError rather than double-wrapping it.
func TestAsStartupConfigErrorPassesThroughExistingConfigError(t *testing.T) {
	original := workload.NewConfigError("ENGINE_VAULT_KEY must be 64 hex characters")
	converted := asStartupConfigError(original)
	configErr, ok := workload.AsConfigError(converted)
	if !ok {
		t.Fatalf("asStartupConfigError dropped the ConfigError classification: %T", converted)
	}
	if configErr.Message != original.Message {
		t.Fatalf("converted message = %q; want %q", configErr.Message, original.Message)
	}
}

// TestAsStartupConfigErrorReturnsNilForNil keeps the helper safe to apply at a
// validation return without changing the nil-success path.
func TestAsStartupConfigErrorReturnsNilForNil(t *testing.T) {
	if err := asStartupConfigError(nil); err != nil {
		t.Fatalf("asStartupConfigError(nil) = %v; want nil", err)
	}
}

// TestValidateVaultKeyRejectsNonHexKeyWithoutLeak (encryptor no-leak lock)
// proves a 64-character-but-non-hex ENGINE_VAULT_KEY is rejected up front by
// ValidateVaultKey as a workload.ConfigError naming ENGINE_VAULT_KEY, with the
// raw hex.DecodeString error and the key material never echoed. Before the fix
// ValidateVaultKey only checked length, so a non-hex 64-char key passed
// validation and failed later inside encryption.NewAES256GCMEncryptor as
// "invalid hex key: ..." which echoes the offending byte.
func TestValidateVaultKeyRejectsNonHexKeyWithoutLeak(t *testing.T) {
	// 64 characters (length check passes), but contains non-hex bytes ('z', '_')
	// with an embedded secret-shaped sentinel in the value position. hex decode
	// must fail; the rejection must never echo this material.
	nonHexKey := "tetralsk_zz_vault_sentinel_0000000000000000000000000000000000000"
	if len(nonHexKey) != 64 {
		t.Fatalf("test setup: nonHexKey length = %d; want 64", len(nonHexKey))
	}
	err := ValidateVaultKey(nonHexKey)
	if err == nil {
		t.Fatal("ValidateVaultKey accepted a 64-character non-hex key")
	}
	configErr, ok := workload.AsConfigError(err)
	if !ok {
		t.Fatalf("ValidateVaultKey returned %T; want a workload.ConfigError", err)
	}
	if !strings.Contains(configErr.Error(), "ENGINE_VAULT_KEY") {
		t.Fatalf("config error %q does not name ENGINE_VAULT_KEY", configErr.Error())
	}
	for _, leaked := range []string{nonHexKey, "tetralsk_zz_vault_sentinel", "invalid hex key"} {
		if strings.Contains(configErr.Error(), leaked) {
			t.Fatalf("config error %q leaked key material or raw hex error fragment %q", configErr.Error(), leaked)
		}
	}
}

// TestValidateVaultKeyAcceptsValidHexKey is the additive-only lock: every
// previously-valid 64-hex key still passes after the hex check is added.
func TestValidateVaultKeyAcceptsValidHexKey(t *testing.T) {
	validKey := strings.Repeat("0123456789abcdef", 4)
	if len(validKey) != 64 {
		t.Fatalf("test setup: validKey length = %d; want 64", len(validKey))
	}
	if err := ValidateVaultKey(validKey); err != nil {
		t.Fatalf("ValidateVaultKey rejected a valid 64-hex key: %v", err)
	}
}

// TestEnsureDataDirRejectionDropsConfiguredPath (P1 L1 + L2) proves the data-dir
// mode rejection names ENGINE_DATA_DIR and the offending mode but never embeds
// the configured path value. Before the fix the message interpolated the raw
// path, leaking a secret-shaped sentinel placed in the value position.
func TestEnsureDataDirRejectionDropsConfiguredPath(t *testing.T) {
	parent := t.TempDir()
	// The leaf directory name carries a secret-shaped sentinel in the value
	// position. EnsureDataDir's mode rejection must not echo it.
	leaf := filepath.Join(parent, "tetral_sk_data_dir_path_sentinel")
	if err := os.Mkdir(leaf, 0o700); err != nil {
		t.Fatalf("mkdir leaf: %v", err)
	}
	if err := os.Chmod(leaf, 0o755); err != nil { //nolint:gosec // G302: deliberately group/other accessible to drive the rejection path under test.
		t.Fatalf("chmod 0755: %v", err)
	}
	err := EnsureDataDir(leaf)
	if err == nil {
		t.Fatal("EnsureDataDir accepted a group/other accessible data directory")
	}
	configErr, ok := workload.AsConfigError(err)
	if !ok {
		t.Fatalf("EnsureDataDir returned %T; want a workload.ConfigError", err)
	}
	message := configErr.Error()
	// L2: still classified config_error, still names ENGINE_DATA_DIR + the mode.
	if !strings.Contains(message, "ENGINE_DATA_DIR") {
		t.Fatalf("config error %q does not name ENGINE_DATA_DIR", message)
	}
	if !strings.Contains(message, "0755") {
		t.Fatalf("config error %q does not name the offending mode 0755", message)
	}
	// L1: the configured path / sentinel must be absent.
	for _, leaked := range []string{leaf, "tetral_sk_data_dir_path_sentinel", parent} {
		if strings.Contains(message, leaked) {
			t.Fatalf("config error %q leaked the configured path fragment %q", message, leaked)
		}
	}
}
