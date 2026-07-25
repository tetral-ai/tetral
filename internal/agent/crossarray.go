package agent

import (
	"encoding/json"
	"fmt"
)

// ValidateCrossArray enforces the runtime-agnostic checks that span
// fields of the resolved Agent body:
//
//  1. every tools[].mcp_toolset.mcp_server_name must reference an
//     existing mcp_servers[].name;
//  2. mcp_servers[].name values are unique;
//  3. a given tools[].mcp_toolset.mcp_server_name is referenced at
//     most once.
//
// Runs on the Agent-owned config after HTTP decoding and Agent
// canonicalization. The check inspects RawArray entries by peeking at
// their JSON `type` discriminator and the well-known reference fields,
// without importing any runtime package.
//
// Declaring an mcp_server without a matching mcp_toolset is allowed: the server
// is stored but does not imply runtime enablement in this phase.
func ValidateCrossArray(cfg AgentConfig) error {
	serverNames := map[string]struct{}{}
	for i, raw := range cfg.MCPServers {
		var entry struct {
			Type string `json:"type"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			// Malformed entries reach here only if an Agent entry path
			// bypassed canonicalization. Surface a runtime-agnostic
			// error rather than crash.
			return &ValidationError{Message: fmt.Sprintf("mcp_servers[%d] is not a valid object", i)}
		}
		if entry.Name == "" {
			continue
		}
		if _, dup := serverNames[entry.Name]; dup {
			return &ValidationError{Message: fmt.Sprintf("mcp_servers[%d].name %q is duplicated", i, entry.Name)}
		}
		serverNames[entry.Name] = struct{}{}
	}

	referenced := map[string]struct{}{}
	for i, raw := range cfg.Tools {
		var entry struct {
			Type          string `json:"type"`
			MCPServerName string `json:"mcp_server_name"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return &ValidationError{Message: fmt.Sprintf("tools[%d] is not a valid object", i)}
		}
		if entry.Type != "mcp_toolset" {
			continue
		}
		name := entry.MCPServerName
		if name == "" {
			continue
		}
		if _, ok := serverNames[name]; !ok {
			return &ValidationError{Message: fmt.Sprintf("tools[%d].mcp_server_name references unknown mcp server %q", i, name)}
		}
		if _, dup := referenced[name]; dup {
			return &ValidationError{Message: fmt.Sprintf("tools[%d].mcp_server_name %q is referenced more than once", i, name)}
		}
		referenced[name] = struct{}{}
	}

	// A given skill_id may appear at most once in skills[]. Agent
	// canonicalization has already normalized each entry into a canonical
	// {"skill_id":"...","version":"..."} object by the time this runs,
	// so collecting the skill_id field is sufficient.
	skillIDs := map[string]struct{}{}
	for i, raw := range cfg.Skills {
		var entry struct {
			SkillID string `json:"skill_id"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return &ValidationError{Message: fmt.Sprintf("skills[%d] is not a valid object", i)}
		}
		if entry.SkillID == "" {
			// Empty skill_id should already have been rejected by
			// Agent canonicalization; surface a runtime-agnostic error
			// if a future entry path forgets the per-entry check.
			return &ValidationError{Message: fmt.Sprintf("skills[%d].skill_id is missing", i)}
		}
		if _, dup := skillIDs[entry.SkillID]; dup {
			return &ValidationError{Message: fmt.Sprintf("skills[%d].skill_id %q is duplicated", i, entry.SkillID)}
		}
		skillIDs[entry.SkillID] = struct{}{}
	}
	return nil
}
