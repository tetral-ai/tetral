package auth

import (
	"context"
	"strings"

	"github.com/tetral-ai/tetral/internal/workspace"
)

// MinBootstrapKeyBytes is the minimum byte length required for the
// ENGINE_API_KEY environment variable. The strength floor keeps the
// bootstrap key at least as strong as generated API keys.
const MinBootstrapKeyBytes = 32

// ValidateBootstrapKey enforces the strength floor for
// ENGINE_API_KEY. Returns *WeakBootstrapKeyError when the value is
// blank, all-whitespace, or shorter than MinBootstrapKeyBytes after
// trimming. Engine startup calls this before serving requests so an
// underpowered env value never authenticates traffic.
//
// The check uses byte length rather than rune count because the
// underlying entropy is byte-level: an operator who configures a
// 32-byte secret has 32 bytes regardless of which bytes those are.
func ValidateBootstrapKey(envKey string) error {
	if envKey == "" {
		return &WeakBootstrapKeyError{Reason: "value is empty"}
	}
	trimmed := strings.TrimSpace(envKey)
	if trimmed == "" {
		return &WeakBootstrapKeyError{Reason: "value is whitespace-only"}
	}
	if len(trimmed) < MinBootstrapKeyBytes {
		return &WeakBootstrapKeyError{Reason: "value is shorter than the 32-byte minimum"}
	}
	return nil
}

// RefreshBootstrap validates the env value, then refreshes the
// bootstrap api_keys row for workspaceID through the store. Engine
// startup composes ValidateBootstrapKey + RefreshBootstrap so a
// missing/weak ENGINE_API_KEY fails fast before any network traffic
// is accepted. Standard workspace-managed keys are not touched.
func RefreshBootstrap(ctx context.Context, store *APIKeyStore, workspaceID workspace.ID, envKey string) error {
	if err := ValidateBootstrapKey(envKey); err != nil {
		return err
	}
	return store.UpsertBootstrap(ctx, workspaceID, envKey)
}
