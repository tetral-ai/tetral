package agent_test

import "encoding/json"

// marshalSkillEntry produces the canonical JSON for one skills[]
// entry. Keys are emitted in declaration order via a typed struct.
func marshalSkillEntry(entry map[string]string) ([]byte, error) {
	type canonical struct {
		Type    string `json:"type"`
		SkillID string `json:"skill_id"`
		Version string `json:"version"`
	}
	c := canonical{Type: "custom", SkillID: entry["skill_id"], Version: entry["version"]}
	if c.Version == "" {
		c.Version = "latest"
	}
	return json.Marshal(c)
}
