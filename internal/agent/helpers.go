package agent

import (
	"database/sql"
	"encoding/json"
	"time"
)

// agentFromStoredConfig reconstructs an Agent from its persisted
// version/config_json/timestamps. Used by Get and version reads where
// the caller already knows the agent ID and has loaded the stored
// config bytes verbatim.
func agentFromStoredConfig(agentID string, versionID string, version int, configJSON string, archivedAt sql.NullTime, createdAt time.Time, updatedAt time.Time) (*Agent, error) {
	return finalizeAgent(&Agent{ID: agentID, VersionID: versionID, Version: version}, configJSON, archivedAt, createdAt, updatedAt)
}

// finalizeAgent decodes config_json into the Agent's AgentConfig,
// normalizes it, and normalizes created_at/updated_at to UTC. The
// helper centralizes the "stored row → Agent value" projection so PG
// store code does not duplicate parsing logic.
func finalizeAgent(agent *Agent, configJSON string, archivedAt sql.NullTime, createdAt time.Time, updatedAt time.Time) (*Agent, error) {
	agent.Type = "agent"

	if err := json.Unmarshal([]byte(configJSON), &agent.AgentConfig); err != nil {
		return nil, err
	}
	agent.AgentConfig = agent.Normalize()

	agent.CreatedAt = createdAt.UTC()
	agent.UpdatedAt = updatedAt.UTC()
	if archivedAt.Valid {
		t := archivedAt.Time.UTC()
		agent.ArchivedAt = &t
	}

	return agent, nil
}
