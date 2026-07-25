package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Canonicalize returns the canonical JSON bytes and the lowercase-hex
// SHA-256 hash of those bytes for cfg. The bytes written to
// agent_versions.config_json and the input to agent_versions.config_hash
// come from the same emitter, so stored bytes and hash stay consistent.
//
// Determinism: Go's encoding/json marshals struct fields in
// declaration order, and storedAgentConfig + RawArray + StringMap (the
// only types reachable here) all have deterministic field/key ordering.
// RawArray entries are canonicalized by Agent request decoding and
// ValidateAndCanonicalizeConfig before Canonicalize runs; Canonicalize
// does not re-marshal them.
//
// Errors: only json.Marshal failure surfaces. Service callers pass it
// through to the downstream HTTP layer, which maps it to a generic
// api_error.
func Canonicalize(cfg AgentConfig) ([]byte, string, error) {
	cfg = cfg.Normalize()
	bytes, err := json.Marshal(storedAgentConfigFromAgentConfig(cfg))
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(bytes)
	return bytes, hex.EncodeToString(sum[:]), nil
}

type storedAgentConfig struct {
	Name         string       `json:"name"`
	Model        string       `json:"model"`
	ApprovalMode ApprovalMode `json:"approval_mode"`
	System       *string      `json:"system"`
	Description  *string      `json:"description"`
	Tools        RawArray     `json:"tools"`
	MCPServers   RawArray     `json:"mcp_servers"`
	Skills       RawArray     `json:"skills"`
	Metadata     StringMap    `json:"metadata"`
}

func storedAgentConfigFromAgentConfig(cfg AgentConfig) storedAgentConfig {
	cfg = cfg.Normalize()
	return storedAgentConfig{
		Name:         cfg.Name,
		Model:        cfg.Model,
		ApprovalMode: cfg.ApprovalMode,
		System:       cfg.System,
		Description:  cfg.Description,
		Tools:        cfg.Tools,
		MCPServers:   cfg.MCPServers,
		Skills:       cfg.Skills,
		Metadata:     cfg.Metadata,
	}
}

// CanonicalHashOfBytes is the symmetric helper for situations where
// the caller already has bytes (e.g. a stored config_json row read out
// of agent_versions). Used by Service no-op detection to fall back to
// recomputed hashes when an older row's stored config_hash is empty.
func CanonicalHashOfBytes(bytes []byte) string {
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}
